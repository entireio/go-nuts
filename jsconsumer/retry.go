package jsconsumer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/entireio/go-nuts/backoff"
	"github.com/entireio/go-nuts/natsmsg"
)

const (
	// DefaultMinDeliveries is the number of deliveries a message must already
	// have burned before the breaker will dead-letter it, when
	// RetryConfig.MinDeliveries is zero. The forensics found events that
	// recovered on attempt 4 and none that recovered on 5 or later, so a
	// message is never dead-lettered on the delivery that first failed —
	// however long the floor has been stalled, it gets at least one retry.
	DefaultMinDeliveries = 2
	// DefaultCaptureReserve is how many deliveries are held back for retrying
	// a failed dead-letter capture when RetryConfig.CaptureReserve is zero.
	// One spare delivery turns "the DLQ was briefly unreachable at exactly
	// the wrong moment" from a stranded message into a retry.
	DefaultCaptureReserve = 1
	// NoCaptureReserve is the explicit RetryConfig.CaptureReserve value for
	// "spend every delivery on the handler". The ladder then exhausts on the
	// broker's own final delivery, so a capture that fails there strands the
	// message ([OutcomeStranded]) — acceptable only where the whole delivery
	// budget is needed and a stranded message is alerted on.
	NoCaptureReserve = -1

	// dlqAckTimeout bounds the DoubleAck confirming a dead-lettered message is
	// settled. Without a bound a NATS blip would hang the handler on the one
	// path that must not hang: the message is already captured, and all that
	// remains is releasing the ack floor.
	dlqAckTimeout = 15 * time.Second

	// maxTrackedFailures caps the per-message failure clocks a Retry holds. A
	// consumer failing more distinct messages than this at once is having a
	// systemic outage, not a poison-message incident, and the breaker being
	// late for some of them is the least of it.
	maxTrackedFailures = 4096
)

// stallSignal is the TRIGGER half of the breaker, kept as an interface so the
// action half never depends on how a stall is detected. [FloorMonitor] is the
// client-side implementation; JetStream could plausibly grow a native
// equivalent (a MaxPendingAge-style consumer option), and slotting that in
// should not touch the capture-and-settle path below — the server will never
// know our DLQ topology, so the action stays ours whatever the trigger becomes.
type stallSignal interface {
	// FloorStall reports the ack floor, how long it has been stalled, and
	// whether it is stalled at all.
	FloorStall() (floor uint64, age time.Duration, stalled bool)
	// FloorAge is the threshold past which the stall is worth acting on.
	FloorAge() time.Duration
}

var _ stallSignal = (*FloorMonitor)(nil)

// Outcome is what [Retry.Settle] did with a failed delivery.
type Outcome string

const (
	// OutcomeRetried means the delivery was Nak'd for redelivery on the
	// ladder.
	OutcomeRetried Outcome = "retried"
	// OutcomeDeadLettered means the raw message and its headers were captured
	// to the DLQ subject and the original Acked, releasing the ack floor.
	OutcomeDeadLettered Outcome = "dead_lettered"
	// OutcomeStranded means the capture failed with no deliveries left to
	// retry it: the broker will drop a Nak at this point, so nothing in the
	// system will touch this message again. It is unsettled, it is pinning the
	// ack floor, and it needs an operator — the non-lossy break-glass in
	// runbooks/nats-poison-ref-event.md. Alert on this; it is the one outcome
	// no automation clears.
	OutcomeStranded Outcome = "stranded"
)

// Cause explains an [OutcomeDeadLettered] settlement; it is empty on a retry.
type Cause string

const (
	// CauseFloorAge is the circuit breaker: this message was pinning the
	// durable's ack floor and the floor had been stalled past FloorAge.
	CauseFloorAge Cause = "floor_age"
	// CauseExhausted is the ordinary terminal branch: the delivery cap was
	// reached (ENT-1492's dead-letter-then-Ack, replacing the unsettled Term).
	CauseExhausted Cause = "max_deliver"
	// CausePermanent is a caller-declared unprocessable message, dead-lettered
	// by [Retry.DeadLetter] without consuming the ladder.
	CausePermanent Cause = "permanent"
)

// Settlement reports what [Retry.Settle] chose and the state it chose it
// from, for the caller's metrics and log lines. FloorAge and Floor are zero
// when no ack-floor observation was available (the breaker is disabled, or
// [Start] has not run yet).
type Settlement struct {
	Outcome   Outcome
	Cause     Cause
	Delivered int           // this message's delivery count, 1-based
	Floor     uint64        // last observed ack floor (stream sequence)
	FloorAge  time.Duration // how long the floor had been stalled there
	// FailingFor is how long this message has been failing, MEASURED from the
	// first failure this process observed — the message-local half of the
	// breaker's condition. It is not inferred from the delivery count, which
	// JetStream also increments on AckWait expiry (see Retry.noteFailure), and
	// it restarts on process restart.
	FailingFor time.Duration
	// BreakerTripped reports that the quarantine condition was met for this
	// message, whether or not the breaker acted on it — so an adopter running
	// [BreakerObserve] can count what enforcing WOULD have dead-lettered.
	BreakerTripped bool
	// Captured reports that the raw message reached the DLQ. It is the field
	// that separates the two strandings: with Captured the data is safe and
	// only the original needs clearing, without it nothing was preserved and
	// the stream copy is all that remains.
	Captured bool
}

// BreakerMode is whether a [Retry] acts on its [FloorMonitor]'s stall signal.
// The zero value observes; dead-lettering on a stall requires saying so.
type BreakerMode int

const (
	// BreakerObserve evaluates the quarantine condition and reports it —
	// through Settlement.BreakerTripped, OnSettle and a log line — without
	// touching the message, which stays on the ladder. This is the zero value:
	// handing a Retry a monitor buys the measurement, never an automatic
	// disposition you did not ask for.
	//
	// It is a faithful dry run, not a looser one: a trip consumes the same
	// one-per-stall-window budget it would consume when enforcing, so the
	// counts an adopter collects here are the counts enforcing would produce
	// — not the far larger number of messages that merely satisfied the
	// conditions.
	BreakerObserve BreakerMode = iota
	// BreakerEnforce dead-letters and Acks a message that meets the condition.
	//
	// EXPERIMENTAL — not for production. Every serious defect found in this
	// package so far has been in this path, and all of them came from the same
	// root: acting on a client-side inference about which message is holding a
	// consumer-global floor. Elapsed failure time inferred from a delivery
	// count that JetStream also increments on AckWait expiry; a blocker
	// identified by a sequence arithmetic that a filtered consumer defeats; a
	// conjunction that would have drained a whole consumer into the DLQ during
	// an entiredb outage. Each was fixed, and each was found late.
	//
	// Meanwhile the boring half of this package covers the incident: a ladder
	// bounded by MaxTimeToDeadLetter reaches the dead-letter branch inside the
	// SLA on its own, with no inference at all. entire-search's post-#173
	// consumer has run clean on exactly that shape, with no delivery ever
	// exceeding attempt 4. The argument this mechanism was built on — that
	// configured ladders lie — was true, and is now fixed at its root by this
	// package owning the schedule outright, not by a breaker.
	//
	// So the intended path is a multi-week [BreakerObserve] soak measuring
	// real trip counts, and then a decision. DELETING this mode is an
	// acceptable, expected outcome of that decision if bounded ladders prove
	// sufficient: it must not survive as dormant complexity for want of anyone
	// willing to remove it. The residual case it uniquely covers — one poison
	// message stalling a consumer whose ladder is deliberately long for other
	// failure classes — is real but narrow, and worth its cost only if the
	// soak shows it happening.
	BreakerEnforce
)

func (m BreakerMode) String() string {
	if m == BreakerEnforce {
		return "enforce"
	}
	return "observe"
}

// RetryConfig describes a [Retry]: the one redelivery ladder, the floor-age
// circuit breaker, and the dead-letter capture they both terminate into.
type RetryConfig struct {
	// MaxDeliver is the consumer's delivery cap, and must equal the
	// consumer's Config.EffectiveMaxDeliver() — the two are cross-checked,
	// because a value that disagrees with the broker either never reaches the
	// dead-letter branch or reaches it early (COR-762).
	//
	// The redelivery SCHEDULE is not here, and not this package's: the server
	// owns it, through the consumer's own BackOff (Config.BackOff, or a NACK
	// Consumer CR's backOff). A failed delivery is plain-Nak'd and the server
	// decides when it comes back. What this type owns is where the retries
	// END — capture to a DLQ, then settle — plus the expectations below, which
	// Config.validate checks against whatever ladder the server is actually
	// running.
	MaxDeliver int

	// Monitor arms the circuit breaker: once the [FloorMonitor] reports the
	// durable's ack floor stalled past its FloorAge, and the message being
	// settled is un-settled work that has itself been failing that long,
	// Settle dead-letters it — well before the ladder would. See
	// [Retry.Settle] for why that is a conjunction and not an attempt to name
	// the blocker. The same monitor should be on the Config, so one poll loop
	// serves both (Start uses this one when Config.FloorMonitor is nil).
	//
	// Nil leaves Settle purely message-local — the ladder and exhaustion, no
	// consumer-global inference. An adopter that wants the stall signal
	// without the automatic disposition builds a FloorMonitor, leaves this
	// nil, and calls [Retry.DeadLetter] on its own judgement.
	Monitor *FloorMonitor
	// MinDeliveries is how many deliveries a message must have had before the
	// breaker may dead-letter it — a second guard alongside the measured
	// failure time, so a message is never quarantined on the delivery that
	// first failed. Zero uses DefaultMinDeliveries.
	MinDeliveries int
	// MaxTimeToDeadLetter bounds the ladder end to end: [Start] rejects a
	// consumer whose BackOff takes longer than this to reach the dead-letter
	// delivery (MaxDeliver less CaptureReserve). Zero leaves it unbounded.
	//
	// This is the guarantee ENT-1535 was actually missing, expressed as
	// config a deployment can assert on rather than arithmetic someone has to
	// redo by hand. The 2026-08-06 stall came from a consumer whose ladder
	// summed to ~34h with nothing anywhere saying so; a service that sets this
	// to its stall SLA cannot ship that consumer at all. Note the bound covers
	// the SCHEDULED waits — handler time and a lost delivery's AckWait sit on
	// top — so leave headroom rather than setting it exactly at the SLA.
	MaxTimeToDeadLetter time.Duration
	// RecoverBy is the delivery by which a TRANSIENT failure is expected to
	// have cleared — a domain fact the library cannot know and therefore takes
	// as input. Construction rejects a ladder whose cumulative wait to that
	// delivery reaches the monitor's FloorAge, because the breaker would then
	// be able to quarantine a message that was still inside its own recovery
	// window: the ladder and the threshold are one schedule, not two
	// independent knobs, and this is where they are checked against each other.
	//
	// Zero uses MinDeliveries, which only pins the guarantee already made
	// elsewhere. Set it to the real envelope — ENT-1535's forensics measured
	// recoveries at attempt 4 and none at 5 or later — to have the ladder
	// checked against it.
	RecoverBy int
	// Breaker is whether Monitor's stall signal is acted on or only reported.
	// The zero value is [BreakerObserve]: supplying a Monitor never enables an
	// automatic disposition by itself, because a rule that Acks messages on a
	// consumer-global heuristic should be something a service opted into in
	// writing. Set [BreakerEnforce] when the trip rate has been measured.
	Breaker BreakerMode

	// CaptureReserve is how many of the consumer's deliveries are held back
	// for retrying a FAILED dead-letter capture. The ladder exhausts at
	// Backoff.MaxDeliver-CaptureReserve, so the first capture attempt still
	// has that many deliveries behind it; without a reserve, a capture that
	// fails on the broker's final delivery has nowhere to go — the Nak is
	// dropped, and the message is stranded for an operator (OutcomeStranded).
	// Zero uses DefaultCaptureReserve; explicit 0 is spelled
	// [NoCaptureReserve] and accepts that risk.
	CaptureReserve int

	// DLQSubject is where the raw message and its headers are republished
	// before the original is Acked. Required: without a capture subject there
	// is no non-lossy way to give up on a message, and this package never
	// drops one.
	DLQSubject string
	// Publisher performs that republish — the same jetstream.JetStream handle
	// the service already publishes with. Required.
	Publisher natsmsg.DLQPublisher

	// OnSettle observes every settlement, for the caller's metrics. Domain
	// metrics stay with the caller, as everywhere else in this module; the
	// Settlement carries what a counter needs (outcome, cause, floor age).
	OnSettle func(ctx context.Context, msg jetstream.Msg, s Settlement)

	// Name prefixes the Retry's own log lines and Logger receives them. Both
	// are optional: [Start] fills them from Config.Name and Config.Logger when
	// unset, so an adopter configures them in one place.
	Name   string
	Logger *slog.Logger
}

// Retry is the single retry mechanism a jsconsumer-hosted handler uses to
// settle a failed delivery. It owns the redelivery ladder and the dead-letter
// capture that ladder terminates into, and — behind an opt-in that is
// currently experimental — a floor-age circuit breaker.
//
// # Two halves, with very different maturity
//
// The settled half is the schedule: one library-owned ladder, bounded end to
// end by RetryConfig.MaxTimeToDeadLetter, terminating in capture-then-Ack.
// That is what a consumer should adopt today, and on its own it is enough to
// dead-letter a poison message inside a stall SLA — no inference about which
// message is at fault, nothing to soak.
//
// The experimental half is the breaker: RetryConfig.Monitor plus
// [BreakerEnforce]. Leave Monitor nil and none of it exists — no failure
// clocks, no quarantine budget, no consumer-global judgement. Attach a monitor
// with the default [BreakerObserve] and it measures without acting, which is
// the supported way to run it. See [BreakerEnforce] for why enforcing is not
// yet a production posture and why removing it is an acceptable outcome.
//
// # Who owns the schedule
//
// The SERVER does. A durable's BackOff ladder — set through Config.BackOff
// today, through a NACK Consumer CR once Track D lands — is the single source
// of truth for when a failed delivery comes back, and this type plain-Naks
// into it. It does not run a schedule of its own.
//
// That is not a detail, it is the ENT-1535 finding. A JetStream consumer has
// two possible redelivery schedulers, and setting both does not pick one: the
// server stretches each client NakWithDelay by the BackOff increments, so
// redeliveries land on neither schedule and the configured ladder becomes dead
// config that still reads as authoritative. entire-search's search-indexer-refs
// advertised a 5m/10m/30m/1h/4h/12h ladder while the forensics measured a real
// 34h22m envelope, and nothing in the system said so. Having exactly one
// scheduler is what prevents that, and the one that composes with declarative
// fleet management is the server's.
//
// So this type owns where retries END, not when they happen: capture to a DLQ,
// then settle. Its client-side expectations about the ladder — how long the
// whole thing may take, when a transient should have recovered — are checked
// against the ladder the consumer actually runs, by [Config] at Start and by
// [Schedule] wherever else the arithmetic is needed.
//
// # What the breaker measures
//
// Floor-stationary age, not message stream-age. The two diverge badly under
// backlog: the same forensics measured 35 minutes of receipt→first-delivery
// lag on a healthy message, which a stream-age trigger would read as a stall.
// The ack floor only stops advancing when a message genuinely will not
// settle, which is the condition worth breaking on.
//
// The signal itself lives in [FloorMonitor], deliberately: ack-floor state is
// consumer-global and this decision is message-local, so the observation is a
// separate, independently usable thing and RetryConfig.Monitor is what opts
// into acting on it. Leave that nil and Settle is purely message-local — the
// ladder and exhaustion, nothing inferred about the consumer as a whole.
//
// Acting on it is still a judgement over consumer-global state, and one limit
// remains that this package cannot resolve from inside: a stall held by
// another replica's message is indistinguishable from one this process could
// clear, so enforcing may quarantine a message that was failing alongside the
// blocker rather than being it. Two things bound that. The message goes to
// the DLQ with full provenance and is replayable; and the breaker claims at
// most one message per stall window, so a wrong guess costs one message per
// FloorAge rather than a drained consumer (see spendQuarantineBudgetLocked).
// [BreakerObserve] measures the rate against real traffic first.
//
// # Non-goal: a handler that dies on the poison message
//
// If the handler panics, OOMs, or otherwise takes the process down instead of
// returning, this package never sees the failure. Settle is the only entry
// point to every disposition it owns — ladder, exhaustion and breaker alike —
// so a message that kills its handler is not retried late or quarantined
// late; it is not settled at all, by any of them. That is a property of the
// callback contract, not of how elapsed failure time is computed, and it
// would be equally true of a breaker that inferred time from the delivery
// count.
//
// This is deliberate, and it is consistent with the module's existing posture
// that a panicking loop is fatal rather than something to recover and carry
// on from (see nuts.ShutdownGroup). Swallowing a panic here to dead-letter
// the message would hide the bug that caused it and leave the handler's own
// state unreconciled — the wrong trade for a library that does not know what
// the handler was in the middle of.
//
// The failure stays loud rather than silent. [FloorMonitor] polls
// independently of message flow, so it keeps reporting the stall for as long
// as the process lives; the ack-floor monitor still pages; and a crash-looping
// pod is itself an alerting surface. What does not happen is automatic
// remediation, and that is the intended boundary: a consumer whose handler
// cannot survive its input needs the crash fixed, not the evidence Acked away.
//
// A related and much milder case: because the failure clock is per-process,
// frequent restarts for unrelated reasons (a rollout, an OOM elsewhere) reset
// it, so the breaker is late by however much of the window was lost. Late is
// the safe direction, and it is the same conservatism [FloorMonitor] applies
// to the stall itself.
//
// # Trigger and action
//
// The two halves are kept apart. The trigger — "the floor has been stalled
// this long" — is behind the stallSignal interface, because JetStream could
// plausibly grow a native equivalent one day. The action — capture to a DLQ,
// then settle — never will be native: the server does not know our DLQ
// topology. A future native trigger slots in without touching the capture
// path.
//
// A Retry belongs to exactly one durable consumer; give each consumer its
// own.
//
// # Worst-case pin
//
// The breaker acts on a delivery, so the floor can stay pinned for up to
// FloorAge + the largest ladder rung + AckWait. Construction rejects a rung
// longer than FloorAge, which bounds it at 2×FloorAge + AckWait; pick rungs
// well under FloorAge to leave headroom.
//
// The ladder's own reach is a second, independent bound, and the one that
// applies with the breaker in observe-only mode or disabled: the DLQ is
// reached at delivery Backoff.MaxDeliver-CaptureReserve, so budget
// (MaxDeliver-CaptureReserve-1) rungs of wall-clock for it.
//
// # Wiring
//
// The first adopter, entire-search's search-indexer-refs on repo_refs_v1,
// wires it like this — a flat ladder, so the whole envelope is one number:
//
//	floor, err := jsconsumer.NewFloorMonitor(jsconsumer.FloorMonitorConfig{
//		FloorAge: 20 * time.Minute, // must exceed the wait to RecoverBy below
//	})
//	if err != nil {
//		return err
//	}
//	retry, err := jsconsumer.NewRetry(jsconsumer.RetryConfig{
//		MaxDeliver: 6,                 // must match the consumer's own
//		DLQSubject: "repo.refs.dlq",   // captured into repo_refs_dlq_v1
//		Publisher:  js,                // the handle the service already publishes with
//		OnSettle:   metrics.RefEventSettled,
//		// Expectations about the SERVER's ladder, checked against it at Start.
//		MaxTimeToDeadLetter: 25 * time.Minute,
//		RecoverBy:           4, // forensics measured recoveries at attempt 4
//		// Breaker left at its BreakerObserve zero value: the monitor measures
//		// what enforcing WOULD do, and nothing is dead-lettered on its say-so.
//		Monitor: floor,
//	})
//	if err != nil {
//		return err
//	}
//	ladder := []time.Duration{5 * time.Minute, 5 * time.Minute, 5 * time.Minute, 5 * time.Minute, 5 * time.Minute}
//	cfg := jsconsumer.Config{
//		Stream: "repo_refs_v1", Durable: "search-indexer-refs",
//		Name: "search-indexer-refs", MaxDeliver: 6,
//		AckWait: ladder[0], // the server applies rung 0 in place of AckWait
//		BackOff: ladder,    // the one schedule; Retry plain-Naks into it
//		Retry:   retry,
//	}
//
//	handle := func(ctx context.Context, span trace.Span, msg jetstream.Msg, ev RefEvent) {
//		switch err := index(ctx, ev); {
//		case err == nil:
//			_ = msg.Ack()
//		case errors.Is(err, errUnprocessable):
//			_, _ = retry.DeadLetter(ctx, msg, err.Error()) // no redelivery can fix it
//		default:
//			_, _ = retry.Settle(ctx, msg, err.Error()) // Nak, or the DLQ
//		}
//	}
//
// Setting the ladder here is the interim step. The app owns its consumer
// today, so it declares the ladder it wants and Start writes it. When the
// durable moves to a Consumer CR under Track D, the CR mirrors a server config
// that already matches, Config.BackOff goes away, and nothing else changes —
// one migration, not two.
//
// The ladder is what carries that configuration: the DLQ is reached on
// delivery 5 of 6 — four 5-minute rungs, ~20 minutes — with the sixth held
// back to retry a failed capture, and MaxTimeToDeadLetter refusing at Start
// any ladder that would take longer. That is inside the 30-minute SLA the
// ack-floor monitor is written against, with no breaker involved. Note the
// server repeats the last rung once the array runs out, so a short list is not
// a short ladder — [Schedule.TimeToDeadLetter] accounts for that, and it is
// the number to reason about.
//
// FloorAge is 20 minutes here rather than the 15-minute default for a reason
// the checks will otherwise find for you: three 5-minute rungs put delivery 4
// at exactly 15 minutes, so a 15-minute threshold could quarantine a
// transient still inside the recovery window RecoverBy declares. The
// validation is deliberately strict about this — run NewRetry once locally
// rather than discovering the arithmetic in CI.
type Retry struct {
	cfg RetryConfig
	// failureTTL is how long a message's failure clock is kept after its last
	// observed failure: a full ladder plus the stall window, so a message
	// still riding the ladder is never forgotten mid-flight.
	failureTTL time.Duration

	mu     sync.Mutex
	name   string
	logger *slog.Logger
	// monitor is cfg.Monitor behind the trigger interface, so nothing below
	// depends on the stall coming from a client-side poll. Nil when the
	// breaker is not armed.
	monitor stallSignal
	// clock reads the current time; a package-internal seam so tests can drive
	// the failure clock without sleeping.
	clock func() time.Time
	// failing is when this process first, and last, saw each stream sequence
	// fail — the measured basis for the breaker's message-local condition.
	failing map[uint64]failureRecord
	// quarantined is when the breaker last claimed a message, and the ack
	// floor it was stalled at, which together ration the breaker to one
	// message per stall window. See spendQuarantineBudgetLocked.
	quarantined      time.Time
	quarantinedFloor uint64
	haveQuarantined  bool
}

type failureRecord struct{ first, last time.Time }

// NewRetry validates cfg, applies its defaults, and returns the mechanism.
// The validation is the point: every rejection here is a configuration that
// would otherwise run, look configured, and quietly not do what it says.
func NewRetry(cfg RetryConfig) (*Retry, error) {
	if cfg.Publisher == nil {
		return nil, errors.New("jsconsumer: RetryConfig.Publisher is required: dead-letter capture is the only terminal branch, so there is no configuration in which a message may be dropped")
	}
	if cfg.DLQSubject == "" {
		return nil, errors.New("jsconsumer: RetryConfig.DLQSubject is required: see Publisher")
	}
	if cfg.MaxDeliver <= 0 {
		return nil, fmt.Errorf("jsconsumer: RetryConfig.MaxDeliver must be positive, got %d: without a finite cap the dead-letter branch would never run", cfg.MaxDeliver)
	}
	if cfg.MinDeliveries < 0 {
		return nil, fmt.Errorf("jsconsumer: RetryConfig.MinDeliveries must not be negative, got %d", cfg.MinDeliveries)
	}
	if cfg.MinDeliveries == 0 {
		cfg.MinDeliveries = DefaultMinDeliveries
	}
	switch {
	case cfg.CaptureReserve == 0:
		cfg.CaptureReserve = DefaultCaptureReserve
	case cfg.CaptureReserve == NoCaptureReserve:
		cfg.CaptureReserve = 0
	case cfg.CaptureReserve < 0:
		return nil, fmt.Errorf("jsconsumer: RetryConfig.CaptureReserve must be -1 (NoCaptureReserve), 0 (default), or positive, got %d", cfg.CaptureReserve)
	}
	if cfg.MinDeliveries > cfg.MaxDeliver-cfg.CaptureReserve {
		return nil, fmt.Errorf("jsconsumer: RetryConfig.MinDeliveries (%d) exceeds the ladder's %d deliveries (MaxDeliver %d less CaptureReserve %d): the breaker could never fire before the ladder exhausted", cfg.MinDeliveries, cfg.MaxDeliver-cfg.CaptureReserve, cfg.MaxDeliver, cfg.CaptureReserve)
	}
	switch cfg.Breaker {
	case BreakerObserve:
	case BreakerEnforce:
		if cfg.Monitor == nil {
			return nil, errors.New("jsconsumer: RetryConfig.Breaker is BreakerEnforce but Monitor is nil: there is no stall signal to enforce on, so the breaker would silently never fire — supply the FloorMonitor here (Config.FloorMonitor alone drives the poll, not this mechanism's disposition)")
		}
	default:
		return nil, fmt.Errorf("jsconsumer: RetryConfig.Breaker is %d, want BreakerObserve or BreakerEnforce", cfg.Breaker)
	}
	if cfg.RecoverBy == 0 {
		cfg.RecoverBy = cfg.MinDeliveries
	}
	if cfg.RecoverBy > cfg.MaxDeliver-cfg.CaptureReserve {
		return nil, fmt.Errorf("jsconsumer: RetryConfig.RecoverBy (%d) exceeds the ladder's %d deliveries: recovery is expected after the ladder has already given up", cfg.RecoverBy, cfg.MaxDeliver-cfg.CaptureReserve)
	}
	// Everything that needs the LADDER — cumulative time to dead-letter,
	// recovery envelope versus breaker threshold — is checked by
	// Config.validate, which knows the server ladder this consumer runs. There
	// is nothing to check against here.
	//
	// The failure clock outlives any plausible ladder: it is swept by TTL only
	// to reclaim memory from messages that recovered, and holding one too long
	// costs nothing but a map entry.
	ttl := time.Hour
	if cfg.Monitor != nil {
		ttl = max(ttl, 4*cfg.Monitor.FloorAge())
	}
	r := &Retry{cfg: cfg, failureTTL: ttl, clock: time.Now}
	if cfg.Monitor != nil {
		r.monitor = cfg.Monitor
	}
	return r, nil
}

// expectations is the client side of the schedule: what this Retry assumes
// about a ladder it does not own. [Config.validate] fills in the ladder the
// server is actually running and validates the whole thing; a future
// bind-only mode fills it in from the durable's live ConsumerInfo instead.
// Same arithmetic, different source — see [Schedule].
func (r *Retry) expectations() Schedule {
	s := Schedule{
		MaxDeliver:          r.cfg.MaxDeliver,
		CaptureReserve:      r.cfg.CaptureReserve,
		MaxTimeToDeadLetter: r.cfg.MaxTimeToDeadLetter,
		RecoverBy:           r.cfg.RecoverBy,
	}
	if r.monitor != nil {
		s.FloorAge = r.monitor.FloorAge()
	}
	return s
}

// cumulativeDelay is the total scheduled wait a message has served by the time
// it is delivered for the nth time: the rungs 1..n-1. Used at construction to
// check the ladder against the breaker threshold. It is NOT used at runtime to
// judge a message — see [Retry.noteFailure] for why elapsed failure time is
// measured rather than inferred from a delivery count.
func cumulativeDelay(p backoff.Policy, n int) time.Duration {
	var total time.Duration
	for i := 1; i < n; i++ {
		total += p.DelayFor(i)
	}
	return total
}

// MaxDeliver reports the delivery cap this mechanism is built for; [Config]
// cross-checks it against the consumer's own.
func (r *Retry) MaxDeliver() int { return r.cfg.MaxDeliver }

// Monitor reports the ack-floor signal driving this mechanism's circuit
// breaker, or nil when Settle is purely message-local. [Start] polls it when
// Config.FloorMonitor is unset.
func (r *Retry) Monitor() *FloorMonitor { return r.cfg.Monitor }

// ladderSpent reports whether the handler's share of the delivery budget is
// used up, so the next step is the DLQ. It stops CaptureReserve deliveries
// short of the broker's own cap, leaving those to retry a failed capture.
// A delivered count of 0 (no metadata) is never exhaustion — the safe default
// is another retry, matching backoff.IsFinalDelivery.
func (r *Retry) ladderSpent(delivered int) bool {
	return delivered > 0 && delivered >= r.cfg.MaxDeliver-r.cfg.CaptureReserve
}

// attach binds the host's log identity, when the RetryConfig left it unset.
// Called by [Start] on every attempt, including [Run]'s recreates.
func (r *Retry) attach(name string, logger *slog.Logger) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.name = firstNonEmpty(r.cfg.Name, name)
	if r.cfg.Logger != nil {
		r.logger = r.cfg.Logger
	} else {
		r.logger = logger
	}
}

func (r *Retry) log() (*slog.Logger, string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	logger, name := r.logger, r.name
	if logger == nil {
		if r.cfg.Logger != nil {
			logger = r.cfg.Logger
		} else {
			logger = slog.Default()
		}
	}
	if name == "" {
		name = firstNonEmpty(r.cfg.Name, "jsconsumer")
	}
	return logger, name
}

// noteFailure records that this process has seen the message at stream
// sequence seq fail, and reports how long it has been failing — measured from
// the first failure this process observed, never inferred.
//
// The inference this replaces was wrong in the one direction that matters.
// Summing the ladder rungs a message "must have" served assumes every
// redelivery came from a NakWithDelay, and JetStream also increments
// NumDelivered on AckWait expiry: a handler that panics, a pod killed
// mid-flight, an eviction under MaxAckPending. Four crash-deliveries can land
// in milliseconds, and the ladder arithmetic would score them as three rungs
// — dead-lettering a message that had barely arrived, on the strength of a
// stall it had nothing to do with. Measuring beats inferring.
//
// The clock is per-process, so it resets on restart and does not follow a
// message across replicas. Both make the breaker LATE, which is the safe
// direction and the same conservatism [FloorMonitor] applies to the stall
// itself: this can be late, never early.
func (r *Retry) noteFailure(seq uint64, now time.Time) time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failing == nil {
		r.failing = make(map[uint64]failureRecord)
	}
	rec, seen := r.failing[seq]
	if !seen {
		rec.first = now
	}
	rec.last = now
	r.failing[seq] = rec
	if len(r.failing) > maxTrackedFailures {
		r.sweepFailuresLocked(now)
	}
	return now.Sub(rec.first)
}

// forgetFailure drops a message's clock once it has been settled for good.
func (r *Retry) forgetFailure(seq uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.failing, seq)
}

// sweepFailuresLocked drops clocks for messages that have stopped coming
// back. A message that recovers is simply never settled again, so nothing
// else would remove its entry; anything untouched for longer than a full
// ladder plus the stall window has either succeeded or moved to another
// replica. If the sweep does not free enough, the oldest-touched entries go
// too — losing a clock only costs the breaker some lateness.
func (r *Retry) sweepFailuresLocked(now time.Time) {
	for seq, rec := range r.failing {
		if now.Sub(rec.last) > r.failureTTL {
			delete(r.failing, seq)
		}
	}
	for len(r.failing) > maxTrackedFailures*3/4 {
		var oldestSeq uint64
		var oldest time.Time
		for seq, rec := range r.failing {
			if oldest.IsZero() || rec.last.Before(oldest) {
				oldestSeq, oldest = seq, rec.last
			}
		}
		delete(r.failing, oldestSeq)
	}
}

// shouldQuarantine reports whether msg may be dead-lettered on the strength of
// a stalled ack floor, along with the stall it was judged against.
//
// # Why this is a conjunction and not an identification
//
// The obvious rule — "dead-letter the message pinning the floor" — needs the
// blocker's identity, and JetStream's consumer info does not carry it. The
// two candidate inferences both fail against a live server:
//
//   - AckFloor.Stream+1 is the blocker only when the floor was last computed
//     by the server's pending scan. After the consumer fully drains, the floor
//     jumps to the delivered high-water mark, and a poison message arriving
//     behind non-matching subjects then sits well above floor+1 (measured:
//     floor 2, blocker at stream 6). A filtered consumer hits this routinely.
//   - AckFloor.Consumer+1 is worse. The server only recomputes that floor when
//     the message AT the floor is acked, so it pins to the blocker's FIRST
//     delivery and goes stale the moment it is redelivered (measured: floor
//     stuck at 1 while the blocker's redeliveries ran 4, 5, 6). It matches on
//     delivery 1 and never again — so any MinDeliveries above 1 disables the
//     breaker entirely.
//
// So this does not guess which message is responsible. It requires three
// facts that are each independently true, and acts only when all three hold:
//
//  1. the consumer is genuinely stuck — the monitor reports the ack floor
//     stalled past FloorAge (consumer-global, advisory);
//  2. this message is part of what is stuck — its stream sequence is above
//     the ack floor, so it is un-settled (exact, and free of any assumption
//     about filters or gaps);
//  3. this message has itself been failing at least as long as the stall —
//     from its own delivery count against the ladder (message-local, exact).
//
// A message satisfying all three has burned FloorAge of retries, on its own,
// while the consumer demonstrably made no progress. Whether or not it is the
// head-of-line blocker, it is not a message that is about to recover: the
// ENT-1535 forensics found recoveries at attempt 4 and none at 5 or later.
// If it IS the blocker the floor is released; if it is not, a message that
// has been failing for a quarter of an hour is captured to the DLQ with full
// provenance and can be replayed.
func (r *Retry) shouldQuarantine(msg jetstream.Msg, delivered int, failingFor time.Duration, now time.Time) (ok bool, floor uint64, age time.Duration) {
	mon := r.monitor
	if mon == nil {
		return false, 0, 0
	}
	floor, age, stalled := mon.FloorStall()
	if !stalled || age < mon.FloorAge() {
		return false, floor, age
	}
	meta, err := msg.Metadata()
	if err != nil || meta == nil {
		return false, floor, age
	}
	if meta.Sequence.Stream <= floor {
		return false, floor, age // already settled past; not part of the stall
	}
	if delivered < r.cfg.MinDeliveries {
		return false, floor, age
	}
	if failingFor < mon.FloorAge() {
		return false, floor, age
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.spendQuarantineBudgetLocked(floor, mon.FloorAge(), now), floor, age
}

// spendQuarantineBudgetLocked rations the breaker to one message per stall
// window, and is what makes it a poison-message remedy rather than a mass
// dead-lettering engine.
//
// Without it, a systemic outage — entiredb down for an hour — trips every one
// of the conditions for EVERY message in flight: the floor is stalled, they
// are all above it, and they have all been failing longer than the threshold.
// The breaker would empty the consumer into the DLQ. The floor+1 rule this
// replaced had selectivity for free by only ever matching one message; the
// conjunction has to buy it back explicitly.
//
// One claim per window, and the window resets when the ack floor MOVES. A
// blocker that really was holding the floor is therefore cleared, the floor
// advances, and the next one becomes eligible immediately — successive
// floor-holders drain at the rate the stall clock allows (a few per hour at
// the default threshold). A claim that did NOT move the floor was aimed at
// the wrong message, and the next attempt waits out a full window rather than
// working through the backlog one Ack at a time.
//
// It also bounds the blast radius of a false positive to one message per
// FloorAge, whatever the cause.
func (r *Retry) spendQuarantineBudgetLocked(floor uint64, window time.Duration, now time.Time) bool {
	switch {
	case !r.haveQuarantined, floor != r.quarantinedFloor, now.Sub(r.quarantined) >= window:
	default:
		return false
	}
	r.haveQuarantined = true
	r.quarantined = now
	r.quarantinedFloor = floor
	return true
}

// Settle applies the mechanism to a delivery whose handler failed
// transiently, and is the only disposition call such a handler makes. It
// picks exactly one of:
//
//   - the circuit breaker — the consumer's ack floor has been stalled past
//     FloorAge and this message has itself been failing that long (see
//     shouldQuarantine): capture to the DLQ and Ack, well before the ladder
//     would have run out (Cause
//     CauseFloorAge). Under [BreakerObserve] the condition is reported on
//     the Settlement and the message stays on the ladder;
//   - exhaustion — the ladder is spent (delivery MaxDeliver-CaptureReserve or
//     later): capture to the DLQ and Ack (Cause CauseExhausted);
//   - the ladder — NakWithDelay for redelivery (Outcome OutcomeRetried).
//
// reason is recorded on the captured copy's Nats-Dlq-Reason header, prefixed
// with the cause, so the DLQ says why a message is there and not just that it
// is.
//
// # When the capture itself fails
//
// A capture failure is never allowed to become a drop, and the Settlement
// says which of two very different things happened:
//
//   - Deliveries remain (CaptureReserve is doing its job): the original is
//     Nak'd, the error returned is settle:dlq_publish_failed, and the
//     Settlement reports OutcomeRetried with the Cause of the terminal branch
//     that could not complete — so the caller's metrics show a DLQ path being
//     reached and failing rather than one sitting idle. The next delivery
//     retries the capture.
//   - No deliveries remain: a Nak here is dropped by the broker, so nothing
//     will retry. The Settlement reports [OutcomeStranded] and the message is
//     left unsettled, pinning the ack floor so the monitor fires — ENT-1492's
//     deliberate posture for a degraded DLQ path, and the one state this
//     package cannot resolve on its own. It is reported as what it is rather
//     than dressed up as a retry.
//
// An Ack that fails after a successful capture (settle:dlq_ack_failed) leaves
// the original to redeliver and be captured again; a duplicate in the DLQ
// beats a lost message, and the Settlement reports OutcomeRetried.
func (r *Retry) Settle(ctx context.Context, msg jetstream.Msg, reason string) (Settlement, error) {
	delivered := backoff.NumDelivered(msg)
	now := r.clock()
	// With no stall signal there is no breaker, so none of its bookkeeping
	// runs: no failure clocks, no map, no quarantine budget. A consumer on
	// the bounded-ladder configuration pays nothing for a mechanism it has
	// not opted into.
	var failingFor time.Duration
	if r.monitor != nil {
		failingFor = r.noteFailure(streamSeq(msg), now)
	}
	quarantine, floor, age := r.shouldQuarantine(msg, delivered, failingFor, now)

	s := Settlement{
		Delivered:      delivered,
		Floor:          floor,
		FloorAge:       age,
		FailingFor:     failingFor,
		BreakerTripped: quarantine,
	}
	switch {
	case quarantine && r.cfg.Breaker == BreakerEnforce:
		s.Cause = CauseFloorAge
	case r.ladderSpent(delivered):
		s.Cause = CauseExhausted
	default:
		// Includes the observe-only trip: the condition is on the Settlement
		// and in the log, but the message keeps its remaining deliveries.
		if quarantine {
			logger, name := r.log()
			logger.WarnContext(ctx, name+": breaker would dead-letter this message (observe only)",
				slog.String("reason", reason),
				slog.Int("delivered", delivered),
				slog.Duration("failing_for", s.FailingFor),
				slog.Uint64("ack_floor", floor),
				slog.Duration("floor_stalled_for", age))
		}
		return r.nak(ctx, msg, s)
	}
	return r.terminate(ctx, msg, s, reason)
}

// terminate runs the capture-then-settle sequence shared by [Settle]'s two
// terminal branches and [Retry.DeadLetter], and classifies every way it can
// fall short. s must already carry its Cause.
func (r *Retry) terminate(ctx context.Context, msg jetstream.Msg, s Settlement, reason string) (Settlement, error) {
	logger, name := r.log()
	spent := s.Delivered > 0 && s.Delivered >= r.cfg.MaxDeliver

	if err := r.capture(ctx, msg, s.Cause, reason); err != nil {
		if spent {
			// Out of deliveries: a Nak is dropped by the broker, so saying
			// "retried" would be a lie. Leave it unsettled and name the state.
			s.Outcome = OutcomeStranded
			logger.ErrorContext(ctx, name+": message stranded; dead-letter capture failed with no deliveries left",
				slog.Any("error", err),
				slog.String("cause", string(s.Cause)),
				slog.String("reason", reason),
				slog.Int("delivered", s.Delivered),
				slog.Uint64("ack_floor", s.Floor))
			r.observe(ctx, msg, s)
			return s, fmt.Errorf("settle:stranded: %w", err)
		}
		// Deliveries remain — fall back to the ladder so the capture is
		// retried. Preserve the cause: the terminal branch was reached and
		// could not complete.
		naked, nakErr := r.nak(ctx, msg, s)
		naked.Cause = s.Cause
		return naked, errors.Join(err, nakErr)
	}
	s.Captured = true

	// DoubleAck, not Ack: a plain Ack is fire-and-forget, so a lost one leaves
	// the floor pinned with nothing reporting it. Waiting for the server's
	// confirmation is what makes the settlement a fact rather than a hope, and
	// it costs one round-trip on a path that only runs when a message is being
	// given up on.
	ackCtx, cancel := context.WithTimeout(ctx, dlqAckTimeout)
	defer cancel()
	if err := msg.DoubleAck(ackCtx); err != nil {
		if spent {
			// The copy is safe in the DLQ, but the original is unsettled and
			// no further delivery is coming: it will pin the ack floor until
			// the stream's retention expires it. Captured distinguishes this
			// from a stranding with nothing captured — the data is recoverable,
			// the consumer is not.
			s.Outcome = OutcomeStranded
			logger.ErrorContext(ctx, name+": message stranded; captured to the DLQ but the ack failed with no deliveries left",
				slog.Any("error", err),
				slog.String("cause", string(s.Cause)),
				slog.Int("delivered", s.Delivered),
				slog.Uint64("ack_floor", s.Floor))
			r.observe(ctx, msg, s)
			return s, fmt.Errorf("settle:dlq_ack_failed: %w", err)
		}
		// A delivery remains: the message redelivers and is captured again.
		// A duplicate in the DLQ beats a pinned floor.
		s.Outcome = OutcomeRetried
		r.observe(ctx, msg, s)
		return s, fmt.Errorf("settle:dlq_ack_failed: %w", err)
	}

	s.Outcome = OutcomeDeadLettered
	r.forgetFailure(streamSeq(msg))
	logger.WarnContext(ctx, name+": dead-lettered",
		slog.String("cause", string(s.Cause)),
		slog.String("reason", reason),
		slog.Int("delivered", s.Delivered),
		slog.Duration("failing_for", s.FailingFor),
		slog.Uint64("ack_floor", s.Floor),
		slog.Duration("floor_stalled_for", s.FloorAge))
	r.observe(ctx, msg, s)
	return s, nil
}

// DeadLetter captures msg to the DLQ and Acks it immediately, for a failure
// the handler knows no redelivery can fix — an unprocessable payload, a
// reference that will never resolve. It is the non-lossy replacement for a
// bare Term: same terminal effect on the consumer, but the raw message and
// its headers survive in the DLQ.
//
// As in [Settle], a capture failure Naks rather than dropping, and returns
// the error.
//
// This is also the seam for an adopter that wants the stall signal without
// the library's automatic disposition: build a [FloorMonitor], leave
// RetryConfig.Monitor nil, and call DeadLetter from your own quarantine rule.
func (r *Retry) DeadLetter(ctx context.Context, msg jetstream.Msg, reason string) (Settlement, error) {
	var floor uint64
	var age, failingFor time.Duration
	if r.monitor != nil {
		floor, age, _ = r.monitor.FloorStall()
		failingFor = r.noteFailure(streamSeq(msg), r.clock())
	}
	delivered := backoff.NumDelivered(msg)
	return r.terminate(ctx, msg, Settlement{
		Cause:      CausePermanent,
		Delivered:  delivered,
		FailingFor: failingFor,
		Floor:      floor,
		FloorAge:   age,
	}, reason)
}

// streamSeq is the message's stream sequence, or 0 when metadata is
// unavailable (a synthetic message). All such messages share the zero key,
// which is harmless: they cannot satisfy the breaker's above-the-floor
// condition anyway.
func streamSeq(msg jetstream.Msg) uint64 {
	if meta, err := msg.Metadata(); err == nil && meta != nil {
		return meta.Sequence.Stream
	}
	return 0
}

// capture republishes the raw message and headers to the DLQ subject, tagging
// the reason with its cause.
func (r *Retry) capture(ctx context.Context, msg jetstream.Msg, cause Cause, reason string) error {
	if err := natsmsg.DeadLetter(ctx, r.cfg.Publisher, r.cfg.DLQSubject, msg, string(cause)+": "+reason); err != nil {
		logger, name := r.log()
		logger.ErrorContext(ctx, name+": settle:dlq_publish_failed",
			slog.Any("error", err),
			slog.String("dlq_subject", r.cfg.DLQSubject),
			slog.String("cause", string(cause)))
		return fmt.Errorf("settle:dlq_publish_failed: %w", err)
	}
	return nil
}

// nak schedules the next delivery on the ladder and completes s.
func (r *Retry) nak(ctx context.Context, msg jetstream.Msg, s Settlement) (Settlement, error) {
	s.Outcome = OutcomeRetried
	// Plain Nak: the server owns the redelivery schedule (Config.BackOff, or
	// the consumer's CR), so naming a delay here would be this process
	// second-guessing the one source of truth — and the server would stretch
	// it by the BackOff increments anyway, which is the ENT-1535 defect.
	err := msg.Nak()
	r.observe(ctx, msg, s)
	if err != nil {
		return s, fmt.Errorf("settle:nak_failed: %w", err)
	}
	return s, nil
}

func (r *Retry) observe(ctx context.Context, msg jetstream.Msg, s Settlement) {
	if r.cfg.OnSettle != nil {
		r.cfg.OnSettle(ctx, msg, s)
	}
}
