package jsconsumer

import (
	"fmt"
	"strings"
	"time"

	"github.com/entireio/go-nuts/backoff"
)

// Schedule is a durable consumer's retry timing as plain values, and
// [Schedule.Validate] is the single implementation of the arithmetic that
// says whether it hangs together.
//
// It takes values rather than a live consumer or a [RetryConfig] on purpose.
// The same arithmetic has to run in three places — [NewRetry] at construction,
// a fleet CI lint over rendered NACK Consumer CRs before merge, and (when
// bind-only mode lands) the library at startup against the durable's ACTUAL
// server-side config — and three implementations of it would drift. ENT-1535
// was a consumer whose real redelivery envelope was 34h22m while its config
// advertised 17h45m; duplicated timing arithmetic is precisely how that
// happens. Compile this function into the checker instead of restating it.
//
// Validate is pure: no NATS connection, no I/O, no clock. Zero-valued fields
// are "not known / not constrained" and their checks are skipped, so a caller
// that knows only the server-side ladder still gets every check that ladder
// supports.
type Schedule struct {
	// NakDelay, Factor and MaxDelay describe the CLIENT-side ladder the
	// library applies with NakWithDelay (see backoff.Policy). Leave NakDelay
	// zero when the server owns redelivery.
	NakDelay time.Duration
	Factor   float64
	MaxDelay time.Duration

	// ServerBackOff is the SERVER-side ladder from the consumer's own config
	// (jetstream.ConsumerConfig.BackOff, or a NACK Consumer CR's backOff).
	// Leave it empty when the client owns redelivery. Setting both this and
	// NakDelay is the ENT-1535 defect itself and is reported as a violation.
	ServerBackOff []time.Duration

	// MaxDeliver is the consumer's delivery cap, AckWait its redelivery
	// timeout. Zero means unknown.
	MaxDeliver int
	AckWait    time.Duration

	// CaptureReserve is how many deliveries are held back to retry a failed
	// dead-letter capture, so the ladder's terminal branch is reached at
	// MaxDeliver-CaptureReserve.
	CaptureReserve int

	// MaxTimeToDeadLetter bounds the cumulative scheduled wait to that
	// terminal branch. Zero leaves it unbounded.
	MaxTimeToDeadLetter time.Duration

	// RecoverBy is the delivery by which a transient failure is expected to
	// clear, and FloorAge the stall threshold a floor-age breaker would act
	// on. Both are client-side expectations; zero means not applicable.
	RecoverBy int
	FloorAge  time.Duration

	// StreamMaxAge is the stream's retention window. A ladder that outlives it
	// cannot reach its own terminal branch — the message is discarded first.
	// Zero means unknown or unlimited.
	StreamMaxAge time.Duration
}

// Violation is one way a [Schedule] does not hang together.
type Violation struct {
	// Field names the setting to change, in the caller's own vocabulary where
	// they differ (a CR's backOff is this type's ServerBackOff).
	Field string
	// Problem states what is wrong and what it costs, in one sentence.
	Problem string
}

func (v Violation) String() string { return v.Field + ": " + v.Problem }

// Ladder reports the client-side ladder as a backoff.Policy.
func (s Schedule) Ladder() backoff.Policy {
	return backoff.Policy{NakDelay: s.NakDelay, Factor: s.Factor, MaxDelay: s.MaxDelay, MaxDeliver: s.MaxDeliver}
}

// DeadLetterDelivery is the delivery on which the terminal branch is reached:
// the delivery cap less whatever is reserved for retrying a failed capture.
func (s Schedule) DeadLetterDelivery() int { return s.MaxDeliver - s.CaptureReserve }

// RungBefore is the scheduled wait served before delivery number n (n >= 2),
// from whichever mechanism actually schedules this consumer's redeliveries, in
// precedence order:
//
//  1. ServerBackOff — the durable's ladder. JetStream repeats the LAST rung
//     once the array runs out, so a short list under a larger maxDeliver is
//     not a short ladder.
//  2. NakDelay — a client-scheduled ladder, for modelling a legacy consumer
//     that Naks with its own delays. Nothing in this package produces one.
//  3. AckWait — NO ladder at all. This is not "no wait": with an empty
//     BackOff the broker still redelivers, on the acknowledgement timeout, so
//     the real ladder is AckWait repeated. Treating that case as zero made
//     every duration check pass vacuously — an AckWait of 1h with MaxDeliver 6
//     scored as an instant schedule while really taking four hours to reach
//     the dead-letter branch.
//
// Every timing check goes through here rather than reading the fields
// directly, because each time one didn't, it silently measured a ladder that
// was not the one running.
func (s Schedule) RungBefore(n int) time.Duration {
	switch {
	case n < 2:
		return 0
	case len(s.ServerBackOff) > 0:
		return s.ServerBackOff[min(n-2, len(s.ServerBackOff)-1)]
	case s.NakDelay > 0:
		return s.Ladder().DelayFor(n - 1)
	default:
		return s.AckWait
	}
}

// CumulativeTo is the total scheduled wait a message has served by the time it
// is delivered for the nth time.
func (s Schedule) CumulativeTo(n int) time.Duration {
	var total time.Duration
	for i := 2; i <= n; i++ {
		total += s.RungBefore(i)
	}
	return total
}

// LongestRung is the largest wait this schedule ever serves before the
// terminal branch.
func (s Schedule) LongestRung() time.Duration {
	var longest time.Duration
	for i := 2; i <= s.DeadLetterDelivery(); i++ {
		longest = max(longest, s.RungBefore(i))
	}
	return longest
}

// TimeToDeadLetter is the cumulative scheduled wait before the terminal branch
// runs — the number a stall SLA is actually about. It counts scheduled waits
// only; handler time and a lost delivery's AckWait sit on top.
func (s Schedule) TimeToDeadLetter() time.Duration {
	return s.CumulativeTo(s.DeadLetterDelivery())
}

// Validate reports every way the schedule does not hang together, most
// structural first. An empty result means it is coherent.
//
//nolint:gocognit // one flat list of independent checks; splitting it would scatter the arithmetic this type exists to centralize
func (s Schedule) Validate() []Violation {
	var vs []Violation
	add := func(field, format string, args ...any) {
		vs = append(vs, Violation{Field: field, Problem: fmt.Sprintf(format, args...)})
	}

	// The ENT-1535 defect itself: two schedulers, so neither is the real one.
	if s.NakDelay > 0 && len(s.ServerBackOff) > 0 {
		add("ServerBackOff", "set with a client-side NakDelay (%s): the server stretches each NAK delay by the BackOff increments, so redeliveries follow neither ladder and the configured one becomes dead config", s.NakDelay)
	}
	if s.NakDelay < 0 {
		add("NakDelay", "must not be negative, got %s", s.NakDelay)
	}
	if s.Factor > 1 && s.MaxDelay <= 0 {
		add("MaxDelay", "required with a growing Factor (%.1f): an uncapped rung cannot be checked against any bound", s.Factor)
	}
	for i, d := range s.ServerBackOff {
		if d <= 0 {
			add("ServerBackOff", "rung %d must be positive, got %s", i, d)
		}
	}
	if s.MaxTimeToDeadLetter < 0 {
		add("MaxTimeToDeadLetter", "must not be negative, got %s", s.MaxTimeToDeadLetter)
	}
	if s.MaxDeliver < 0 {
		add("MaxDeliver", "must not be negative, got %d", s.MaxDeliver)
	}
	if s.CaptureReserve < 0 {
		add("CaptureReserve", "must not be negative, got %d", s.CaptureReserve)
	}
	if s.MaxDeliver > 0 && s.CaptureReserve >= s.MaxDeliver {
		add("CaptureReserve", "%d leaves no deliveries for the handler out of MaxDeliver %d", s.CaptureReserve, s.MaxDeliver)
	}

	// A server ladder must have somewhere to apply every rung.
	if n := len(s.ServerBackOff); n > 0 && s.MaxDeliver > 0 && n > s.MaxDeliver {
		add("ServerBackOff", "has %d rungs but MaxDeliver is %d: there are only %d redeliveries to schedule", n, s.MaxDeliver, s.MaxDeliver)
	}
	// JetStream applies BackOff[0] in place of AckWait on the first delivery,
	// so a disagreement means the first redelivery lands on neither value.
	if len(s.ServerBackOff) > 0 && s.AckWait > 0 && s.AckWait != s.ServerBackOff[0] {
		add("AckWait", "%s disagrees with ServerBackOff[0] (%s): the server applies the first rung in place of AckWait, so the first redelivery follows neither", s.AckWait, s.ServerBackOff[0])
	}

	if s.MaxDeliver <= 0 || s.DeadLetterDelivery() < 1 {
		return vs // nothing below can be computed
	}
	ttl := s.TimeToDeadLetter()

	if s.MaxTimeToDeadLetter > 0 && ttl > s.MaxTimeToDeadLetter {
		add("MaxTimeToDeadLetter", "the ladder takes %s to reach the dead-letter branch (delivery %d), past the %s bound: shorten the rungs, lower MaxDeliver, or raise the bound", ttl, s.DeadLetterDelivery(), s.MaxTimeToDeadLetter)
	}
	// A ladder that outlives retention never reaches its own terminal branch.
	if s.StreamMaxAge > 0 && ttl >= s.StreamMaxAge {
		add("StreamMaxAge", "the ladder takes %s to reach the dead-letter branch but the stream discards after %s: the message is gone before it can be captured", ttl, s.StreamMaxAge)
	}

	if s.RecoverBy < 0 {
		add("RecoverBy", "must not be negative, got %d", s.RecoverBy)
	}
	if s.RecoverBy > s.DeadLetterDelivery() {
		add("RecoverBy", "%d exceeds the ladder's %d deliveries: recovery is expected after the ladder has already given up", s.RecoverBy, s.DeadLetterDelivery())
	}
	if s.FloorAge > 0 {
		// The ladder and the breaker threshold are one schedule: if the wait
		// to the expected recovery reaches the threshold, the breaker can
		// quarantine a transient that was still going to succeed.
		if s.RecoverBy > 0 && s.RecoverBy <= s.DeadLetterDelivery() {
			if cum := s.CumulativeTo(s.RecoverBy); cum >= s.FloorAge {
				add("RecoverBy", "the ladder reaches delivery %d after %s, at or past FloorAge (%s): a transient still inside its recovery window could be dead-lettered — shorten the ladder, lower RecoverBy, or raise FloorAge", s.RecoverBy, cum, s.FloorAge)
			}
		}
		// A threshold the ladder never reaches is a breaker that can never
		// fire — configured, named in every doc and dashboard, and inert. The
		// ENT-1535 disease in miniature, so it is a violation rather than a
		// quiet no-op. The breaker's last chance is the dead-letter delivery
		// itself, since Settle weighs quarantine before exhaustion.
		//
		// Note the weaker case this does NOT reject: a threshold above
		// CumulativeTo(DeadLetterDelivery-1) leaves the breaker able to fire
		// only on the very delivery exhaustion would have handled anyway, so
		// it accelerates nothing. That is a judgement about whether the
		// breaker earns its keep on a given ladder, not a broken config, and
		// on a tight bounded ladder it is the normal outcome.
		if ttl := s.TimeToDeadLetter(); ttl > 0 && s.FloorAge > ttl {
			add("FloorAge", "%s is longer than the %s this ladder takes to dead-letter a message: the breaker could never fire — shorten FloorAge, lengthen the ladder, or drop the monitor",
				s.FloorAge, ttl)
		}
		// A breaker only ever fires on a delivery, so a rung longer than the
		// threshold leaves the floor pinned for that rung after it arms.
		if rung := s.LongestRung(); rung > s.FloorAge {
			add("FloorAge", "the largest ladder rung (%s) exceeds FloorAge (%s): the breaker acts on a delivery, so the floor would stay pinned for up to FloorAge+%s", rung, s.FloorAge, rung)
		}
	}
	return vs
}

// Err folds Validate's result into a single error, or nil when the schedule is
// coherent — the form [NewRetry] and a bind-time check want, where fleet CI
// wants the individual violations.
func (s Schedule) Err() error {
	vs := s.Validate()
	if len(vs) == 0 {
		return nil
	}
	parts := make([]string, 0, len(vs))
	for _, v := range vs {
		parts = append(parts, v.String())
	}
	return fmt.Errorf("jsconsumer: incoherent retry schedule: %s", strings.Join(parts, "; "))
}
