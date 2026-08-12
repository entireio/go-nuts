package jsconsumer

import (
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// fields returns the Field of every violation, for order-insensitive
// assertions about which checks fired.
func fields(vs []Violation) []string {
	out := make([]string, 0, len(vs))
	for _, v := range vs {
		out = append(out, v.Field)
	}
	return out
}

func hasField(vs []Violation, field string) bool {
	for _, v := range vs {
		if v.Field == field {
			return true
		}
	}
	return false
}

// TestScheduleAcceptsTheBoundedLadder: the shape Track A actually ships — a
// flat client ladder, no server BackOff, bounded end to end.
func TestScheduleAcceptsTheBoundedLadder(t *testing.T) {
	s := Schedule{
		NakDelay: 5 * time.Minute, MaxDeliver: 6, CaptureReserve: 1,
		MaxTimeToDeadLetter: 25 * time.Minute, RecoverBy: 4, FloorAge: 20 * time.Minute,
	}
	if vs := s.Validate(); len(vs) != 0 {
		t.Fatalf("the shipping schedule reported violations: %v", fields(vs))
	}
	if got := s.TimeToDeadLetter(); got != 20*time.Minute {
		t.Errorf("TimeToDeadLetter = %s, want 20m0s (four 5-minute rungs to delivery 5)", got)
	}
	if got := s.DeadLetterDelivery(); got != 5 {
		t.Errorf("DeadLetterDelivery = %d, want 5", got)
	}
	if s.Err() != nil {
		t.Errorf("Err = %v, want nil", s.Err())
	}
}

// TestScheduleCatchesTheENT1535Consumer is the regression that matters: the
// production consumer whose real envelope was 34h22m while its config
// advertised 17h45m. Both defects are visible from config alone — two
// schedulers fighting, and a ladder far past any sane bound — so a lint that
// runs this function at merge time refuses that consumer before it ships.
func TestScheduleCatchesTheENT1535Consumer(t *testing.T) {
	s := Schedule{
		// The handler's own ladder...
		NakDelay: 30 * time.Second, Factor: 4, MaxDelay: 24 * time.Hour,
		// ...fighting the consumer's configured one.
		ServerBackOff: []time.Duration{
			5 * time.Minute, 10 * time.Minute, 30 * time.Minute,
			time.Hour, 4 * time.Hour, 12 * time.Hour,
		},
		MaxDeliver: 7, AckWait: 5 * time.Minute,
		MaxTimeToDeadLetter: 30 * time.Minute,
	}
	vs := s.Validate()
	if !hasField(vs, "ServerBackOff") {
		t.Errorf("two competing ladders not reported; got %v", fields(vs))
	}
	if !hasField(vs, "MaxTimeToDeadLetter") {
		t.Errorf("a 17h45m ladder under a 30m bound not reported; got %v", fields(vs))
	}
	if err := s.Err(); err == nil || !strings.Contains(err.Error(), "dead config") {
		t.Errorf("Err = %v, want it to name the dead-config failure", err)
	}
}

// TestScheduleChecksServerSideConfig covers the checks a rendered NACK
// Consumer CR needs and a client-owned ladder never exercises.
func TestScheduleChecksServerSideConfig(t *testing.T) {
	tests := []struct {
		name  string
		sched Schedule
		want  string
	}{
		{"ackWait disagrees with the first rung", Schedule{
			ServerBackOff: []time.Duration{time.Minute, 2 * time.Minute},
			MaxDeliver:    5, AckWait: 30 * time.Second,
		}, "AckWait"},
		{"more rungs than redeliveries", Schedule{
			ServerBackOff: []time.Duration{time.Minute, time.Minute, time.Minute},
			MaxDeliver:    2, AckWait: time.Minute,
		}, "ServerBackOff"},
		{"non-positive rung", Schedule{
			ServerBackOff: []time.Duration{time.Minute, 0},
			MaxDeliver:    5, AckWait: time.Minute,
		}, "ServerBackOff"},
		{"ladder outlives stream retention", Schedule{
			NakDelay: 2 * time.Hour, MaxDeliver: 5, CaptureReserve: 1,
			StreamMaxAge: 6 * time.Hour,
		}, "StreamMaxAge"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if vs := tt.sched.Validate(); !hasField(vs, tt.want) {
				t.Fatalf("Validate() reported %v, want a %s violation", fields(vs), tt.want)
			}
		})
	}
}

// TestScheduleReportsEveryViolation: a merge-time lint should show an author
// all of it at once, not the first thing that happens to be checked.
func TestScheduleReportsEveryViolation(t *testing.T) {
	s := Schedule{
		NakDelay: 30 * time.Minute, MaxDeliver: 6, CaptureReserve: 1,
		MaxTimeToDeadLetter: 25 * time.Minute,
		RecoverBy:           4, FloorAge: 20 * time.Minute,
		StreamMaxAge: time.Hour,
	}
	vs := s.Validate()
	if len(vs) < 3 {
		t.Fatalf("reported %d violations (%v), want every independent failure", len(vs), fields(vs))
	}
	for _, want := range []string{"MaxTimeToDeadLetter", "StreamMaxAge", "RecoverBy", "FloorAge"} {
		if !hasField(vs, want) {
			t.Errorf("missing a %s violation; got %v", want, fields(vs))
		}
	}
}

// TestScheduleZeroFieldsSkipTheirChecks: an unknown value must not become a
// false violation, or a caller that knows only part of a config (fleet CI
// reading a CR with no client-side expectations) could not use this at all.
func TestScheduleZeroFieldsSkipTheirChecks(t *testing.T) {
	s := Schedule{NakDelay: 12 * time.Hour, MaxDeliver: 6, CaptureReserve: 1}
	if vs := s.Validate(); len(vs) != 0 {
		t.Fatalf("an unbounded-but-unconstrained schedule reported %v", fields(vs))
	}
	// Naming a bound is what turns the same ladder into a violation.
	s.MaxTimeToDeadLetter = time.Hour
	if vs := s.Validate(); !hasField(vs, "MaxTimeToDeadLetter") {
		t.Fatalf("adding a bound did not surface the ladder; got %v", fields(vs))
	}
}

// TestNewRetryUsesTheSharedSchedule pins that the library's construction-time
// checks and the exported function are the same arithmetic, not two copies —
// the drift that produced ENT-1535 in the first place.
func TestConfigUsesTheSharedSchedule(t *testing.T) {
	r, err := NewRetry(RetryConfig{
		MaxDeliver:          6,
		DLQSubject:          "repo.refs.dlq",
		Publisher:           &fakeDLQ{},
		MaxTimeToDeadLetter: 25 * time.Minute,
	})
	if err != nil {
		t.Fatalf("NewRetry: %v", err)
	}
	ladder := []time.Duration{30 * time.Minute, 30 * time.Minute, 30 * time.Minute, 30 * time.Minute, 30 * time.Minute}
	cfg := Config{
		Stream: "s_v1", Durable: "d", Name: "c",
		AckWait: 30 * time.Minute, MaxDeliver: 6, BackOff: ladder, Retry: r,
	}
	startErr := cfg.validate(nil, func(jetstream.Msg) {})
	if startErr == nil {
		t.Fatal("Start accepted a ladder past the Retry's bound")
	}
	// The same consumer, expressed as plain values, must fail the same way —
	// one implementation, whether it runs here, in fleet CI, or at bind time.
	if vs := cfg.schedule().Validate(); !hasField(vs, "MaxTimeToDeadLetter") {
		t.Fatalf("Schedule.Validate reported %v, want the violation Start raised", fields(vs))
	}
	if !strings.Contains(startErr.Error(), "MaxTimeToDeadLetter") {
		t.Errorf("Start err = %v, want it to carry the schedule violation", startErr)
	}
}

// TestScheduleRepeatsTheLastServerRung: JetStream reuses the final backOff
// entry once the array runs out, so a short array under a larger maxDeliver is
// not a short ladder. Summing only the listed rungs under-counts the real time
// to the terminal branch — and would pass a CR that actually breaches its
// bound, which is the whole job of this check.
func TestScheduleRepeatsTheLastServerRung(t *testing.T) {
	s := Schedule{
		// Three listed rungs, but the terminal branch is delivery 6, so five
		// rungs are served: 5m + 10m + 30m + 30m + 30m. Summing only what is
		// listed gives 45m; the real wait is 1h45m.
		ServerBackOff: []time.Duration{5 * time.Minute, 10 * time.Minute, 30 * time.Minute},
		MaxDeliver:    7, CaptureReserve: 1, AckWait: 5 * time.Minute,
	}
	if got, want := s.TimeToDeadLetter(), time.Hour+45*time.Minute; got != want {
		t.Fatalf("TimeToDeadLetter = %s, want %s (the last rung repeats)", got, want)
	}
	s.MaxTimeToDeadLetter = time.Hour
	if vs := s.Validate(); !hasField(vs, "MaxTimeToDeadLetter") {
		t.Errorf("a ladder that really takes 1h45m passed a 1h bound; got %v", fields(vs))
	}
}

// TestScheduleServerLadderBoundaries walks the edges of the repeat-tail rule,
// where an off-by-one silently changes the number fleet CI gates on.
func TestScheduleServerLadderBoundaries(t *testing.T) {
	m := time.Minute
	tests := []struct {
		name       string
		backOff    []time.Duration
		maxDeliver int
		reserve    int
		want       time.Duration
	}{
		// Exactly enough rungs: nothing repeats.
		{"array length == deliveries before the branch", []time.Duration{m, 2 * m, 3 * m, 4 * m}, 6, 1, 10 * m},
		// One short: the last rung covers the final gap.
		{"array one short", []time.Duration{m, 2 * m, 3 * m}, 6, 1, 9 * m},
		// Degenerate: a single rung is the whole ladder.
		{"single-rung array", []time.Duration{7 * m}, 6, 1, 28 * m},
		// Longer than needed: the tail is never reached.
		{"array longer than the branch", []time.Duration{m, m, m, m, m, m, m, m}, 4, 1, 2 * m},
		// No reserve: the branch is the broker's own cap.
		{"no capture reserve", []time.Duration{2 * m}, 4, 0, 6 * m},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := Schedule{
				ServerBackOff: tt.backOff, MaxDeliver: tt.maxDeliver,
				CaptureReserve: tt.reserve, AckWait: tt.backOff[0],
			}
			if got := s.TimeToDeadLetter(); got != tt.want {
				t.Fatalf("TimeToDeadLetter = %s, want %s (branch at delivery %d)", got, tt.want, s.DeadLetterDelivery())
			}
		})
	}
}
