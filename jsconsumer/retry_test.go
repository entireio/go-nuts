package jsconsumer

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/trace"

	"github.com/entireio/go-nuts/backoff"
	"github.com/entireio/go-nuts/natsmsg"
	"github.com/entireio/go-nuts/natsmsg/natsmsgtest"
)

// fakeDLQ records dead-letter captures, or fails them on demand so the
// never-drop path can be exercised.
type fakeDLQ struct {
	mu   sync.Mutex
	msgs []*nats.Msg
	err  error
}

func (f *fakeDLQ) PublishMsg(_ context.Context, m *nats.Msg, _ ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	f.msgs = append(f.msgs, m)
	return &jetstream.PubAck{Stream: "repo_refs_dlq_v1", Sequence: uint64(len(f.msgs))}, nil
}

func (f *fakeDLQ) captured() []*nats.Msg {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*nats.Msg(nil), f.msgs...)
}

var _ natsmsg.DLQPublisher = (*fakeDLQ)(nil)

// testRetryConfig is the shape of the first adopter's config
// (search-indexer-refs): a flat 5-minute rung well under the 15-minute
// breaker threshold, MaxDeliver 6, capture to the ref-event DLQ. The two
// terminal boundaries land apart, which is what lets the tests tell them
// apart: the breaker becomes eligible on delivery 4 (three 5-minute rungs
// served = the 15-minute threshold) and the ladder is spent on delivery 5
// (MaxDeliver 6 less one reserved for a failed capture).
const (
	breakerDelivery   = 4
	exhaustedDelivery = 5
	finalDelivery     = 6
)

func testRetryConfig(pub natsmsg.DLQPublisher) RetryConfig {
	return RetryConfig{
		MaxDeliver: 6,
		DLQSubject: "repo.refs.dlq",
		Publisher:  pub,
		Name:       "search-indexer-refs",
		Monitor:    mustFloorMonitor(FloorMonitorConfig{Name: "search-indexer-refs"}),
		Breaker:    BreakerEnforce,
	}
}

// testLadder is the SERVER-side ladder the test consumer runs: flat 5 minutes,
// so the terminal branch is reached on delivery 5 after 20 minutes.
func testLadder() []time.Duration {
	return []time.Duration{5 * time.Minute, 5 * time.Minute, 5 * time.Minute, 5 * time.Minute, 5 * time.Minute}
}

func mustFloorMonitor(cfg FloorMonitorConfig) *FloorMonitor {
	m, err := NewFloorMonitor(cfg)
	if err != nil {
		panic(err)
	}
	return m
}

func newTestRetry(t *testing.T, mutate func(*RetryConfig)) (*Retry, *fakeDLQ) {
	t.Helper()
	pub := &fakeDLQ{}
	cfg := testRetryConfig(pub)
	if mutate != nil {
		mutate(&cfg)
	}
	r, err := NewRetry(cfg)
	if err != nil {
		t.Fatalf("NewRetry: %v", err)
	}
	return r, pub
}

// armFloor puts the Retry in the state the poll loop would have built up over
// a real stall: ack floor pinned at seq with the consumer delivered past it,
// for age. The delivered mark matters — a floor sitting still on a caught-up
// consumer is not a stall (see TestObserveFloorIgnoresACaughtUpConsumer).
func armFloor(r *Retry, seq uint64, age time.Duration) {
	armStall(r.cfg.Monitor, seq, age)
}

// armStall drives the monitor to the state a real poll loop would have built
// up: floor pinned at seq, stalled for age. It sets the state rather than
// replaying observations so a test can re-arm the same floor with a different
// age; observeFloor's own transitions are covered by the TestObserveFloor*
// cases.
func armStall(m *FloorMonitor, seq uint64, age time.Duration) {
	now := time.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.clock = func() time.Time { return now }
	m.stalled, m.floor, m.floorSince, m.stallLogged = true, seq, now.Add(-age), false
}

// armFailing seeds the measured failure clock: this process first saw stream
// sequence seq fail `age` ago. The breaker measures rather than infers, so a
// test that wants a trip has to say how long the message has really been
// failing — a delivery count alone no longer implies it.
func armFailing(r *Retry, seq uint64, age time.Duration) {
	now := time.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.clock = func() time.Time { return now }
	if r.failing == nil {
		r.failing = make(map[uint64]failureRecord)
	}
	r.failing[seq] = failureRecord{first: now.Add(-age), last: now.Add(-age)}
}

// deliveredMsg is a message at stream sequence seq on its nth delivery.
func deliveredMsg(seq uint64, n int) *natsmsgtest.FakeMsg {
	return &natsmsgtest.FakeMsg{
		SubjectVal: "repo.refs.update",
		DataVal:    []byte("ref-event"),
		HeadersVal: nats.Header{"X-Entire-Repo": []string{"01KWHDFJ0C575CRVF18DPVSQW0"}},
		Meta: &jetstream.MsgMetadata{
			Sequence:     jetstream.SequencePair{Stream: seq, Consumer: seq},
			NumDelivered: uint64(n),
		},
	}
}

// TestNewRetryRejectsUnworkableConfig: every rejection here is a
// configuration that would otherwise start, read as configured, and quietly
// not do what it says — the ENT-1535 failure mode, one level up.
func TestNewRetryRejectsUnworkableConfig(t *testing.T) {
	pub := &fakeDLQ{}
	tests := []struct {
		name string
		cfg  RetryConfig
		want string
	}{
		{"no publisher", func() RetryConfig { c := testRetryConfig(pub); c.Publisher = nil; return c }(), "Publisher"},
		{"no dlq subject", func() RetryConfig { c := testRetryConfig(pub); c.DLQSubject = ""; return c }(), "DLQSubject"},
		{"unlimited max deliver", func() RetryConfig {
			c := testRetryConfig(pub)
			c.MaxDeliver = backoff.UnlimitedMaxDeliver
			return c
		}(), "MaxDeliver"},
		{"recovery expected after the ladder gives up", func() RetryConfig {
			c := testRetryConfig(pub)
			c.RecoverBy = 6
			return c
		}(), "RecoverBy"},
		{"negative min deliveries", func() RetryConfig {
			c := testRetryConfig(pub)
			c.MinDeliveries = -1
			return c
		}(), "MinDeliveries"},
		{"min deliveries past the cap", func() RetryConfig {
			c := testRetryConfig(pub)
			c.MinDeliveries = 9
			return c
		}(), "MinDeliveries"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewRetry(tt.cfg)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("NewRetry() err = %v, want error containing %q", err, tt.want)
			}
		})
	}
}

// TestFloorMonitorDefaults: the zero values are the sized ones from
// ENT-1535's forensics.
func TestFloorMonitorDefaults(t *testing.T) {
	m := mustFloorMonitor(FloorMonitorConfig{})
	if m.FloorAge() != DefaultFloorAge {
		t.Errorf("FloorAge = %s, want %s", m.FloorAge(), DefaultFloorAge)
	}
	// FloorAge/10 is 90s, clamped to the 30s ceiling.
	if got := m.pollInterval(); got != maxFloorPoll {
		t.Errorf("pollInterval = %s, want %s", got, maxFloorPoll)
	}
}

// TestNewFloorMonitorRejectsUnworkableConfig: a stall that is never sampled
// cannot be resolved, so a poll slower than the threshold is a construction
// error rather than a monitor that silently never reports.
func TestNewFloorMonitorRejectsUnworkableConfig(t *testing.T) {
	tests := map[string]FloorMonitorConfig{
		"poll slower than the window": {Poll: 20 * time.Minute},
		"negative poll":               {Poll: -time.Second},
		"negative floor age":          {FloorAge: -time.Second},
	}
	for name, cfg := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := NewFloorMonitor(cfg); err == nil {
				t.Fatal("NewFloorMonitor succeeded, want an error")
			}
		})
	}
}

// TestNewRetryDefaults covers the constructor's defaults and, in its second
// half, what omitting the monitor buys: Settle then decides from delivery
// metadata alone — no consumer-global inference and no ack-floor poll — so a
// rung longer than any breaker threshold is legal. That is the composition an
// adopter uses when it wants to own the quarantine judgement itself.
func TestNewRetryDefaults(t *testing.T) {
	r, _ := newTestRetry(t, nil)
	if r.cfg.MinDeliveries != DefaultMinDeliveries {
		t.Errorf("MinDeliveries = %d, want %d", r.cfg.MinDeliveries, DefaultMinDeliveries)
	}
	if r.cfg.CaptureReserve != DefaultCaptureReserve {
		t.Errorf("CaptureReserve = %d, want %d", r.cfg.CaptureReserve, DefaultCaptureReserve)
	}
	if r.Monitor() == nil {
		t.Error("Monitor() nil, want the configured monitor")
	}

	// No monitor: a rung longer than any threshold is legal, because nothing
	// has to fit inside a breaker window.
	off, err := NewRetry(func() RetryConfig {
		c := testRetryConfig(&fakeDLQ{})
		c.Monitor, c.Breaker = nil, BreakerObserve
		return c
	}())
	if err != nil {
		t.Fatalf("NewRetry(no monitor): %v", err)
	}
	if off.Monitor() != nil {
		t.Error("Monitor() non-nil without one configured")
	}
	msg := deliveredMsg(101, 2)
	armStall(r.cfg.Monitor, 100, time.Hour) // a stall on someone else's monitor
	if _, err := off.Settle(t.Context(), msg, "transient"); err != nil {
		t.Fatalf("Settle: %v", err)
	}
	if msg.Acked {
		t.Error("a monitor-less Retry acted on consumer-global state")
	}
}

// TestSettleRetriesOnTheLadder: the ordinary path — no floor observation yet,
// deliveries left — disposes of nothing, leaving the delivery for the server's
// ladder to redeliver on AckWait expiry.
func TestSettleRetriesOnTheLadder(t *testing.T) {
	r, pub := newTestRetry(t, nil)
	msg := deliveredMsg(2774437, 2)

	s, err := r.Settle(t.Context(), msg, "checkpoint metadata not ready")
	if err != nil {
		t.Fatalf("Settle: %v", err)
	}
	if s.Outcome != OutcomeRetried || s.Cause != "" {
		t.Errorf("Settlement = %+v, want retried with no cause", s)
	}
	// Nothing is disposed: the server's ladder redelivers on AckWait expiry,
	// and a Nak here would ask for immediate redelivery instead, skipping it.
	if msg.Naks != 0 || len(msg.NakDelays) != 0 || msg.Acked || msg.Termed {
		t.Errorf("message disposed (naks=%d/%d ack=%v term=%v); want it left for the ladder",
			msg.Naks, len(msg.NakDelays), msg.Acked, msg.Termed)
	}
	if msg.Acked || msg.Termed {
		t.Errorf("message acked=%v termed=%v on a retry, want neither", msg.Acked, msg.Termed)
	}
	if len(pub.captured()) != 0 {
		t.Error("captured to the DLQ on a retry")
	}
}

// TestSettleBreakerDeadLettersThroughAStall is the ENT-1535 remedy, and the
// regression test for the filtered-consumer defect: the message sits at a
// stream sequence far above the ack floor, exactly as a poison event does when
// non-matching subjects sit between it and the floor. An identification rule
// keyed on floor+1 misses it — with unmatched sequences below the blocker and
// nothing acked beneath them, floor+1 names a message this consumer never
// receives — while the conjunction used here does not depend on naming the
// blocker at all. See shouldQuarantine for the mechanism; the specific floor
// and blocker sequences once quoted here could not be reproduced.
func TestSettleBreakerDeadLettersThroughAStall(t *testing.T) {
	var got Settlement
	r, pub := newTestRetry(t, func(c *RetryConfig) {
		c.OnSettle = func(_ context.Context, _ jetstream.Msg, s Settlement) { got = s }
	})
	armFloor(r, 2774436, 16*time.Minute)
	armFailing(r, 2774600, 16*time.Minute)
	msg := deliveredMsg(2774600, breakerDelivery) // 164 sequences above the floor

	s, err := r.Settle(t.Context(), msg, "re-queuing: update")
	if err != nil {
		t.Fatalf("Settle: %v", err)
	}
	if s.Outcome != OutcomeDeadLettered || s.Cause != CauseFloorAge {
		t.Fatalf("Settlement = %+v, want dead_lettered/floor_age", s)
	}
	if !s.BreakerTripped || !s.Captured {
		t.Errorf("Settlement = %+v, want the trip and the capture both reported", s)
	}
	if s.FailingFor != 16*time.Minute {
		t.Errorf("FailingFor = %s, want the 16m0s actually observed", s.FailingFor)
	}
	if !msg.Acked {
		t.Error("blocker not Acked; the ack floor stays pinned")
	}
	if msg.Termed {
		t.Error("blocker Termed; a Term settles it but leaves no record of what was discarded")
	}
	if msg.Naks != 0 || len(msg.NakDelays) != 0 {
		t.Errorf("blocker also naked (%d plain, %d delayed), want none", msg.Naks, len(msg.NakDelays))
	}
	if s.Floor != 2774436 || s.FloorAge != 16*time.Minute {
		t.Errorf("Settlement floor state = %d/%s, want 2774436/16m0s", s.Floor, s.FloorAge)
	}
	if got != s {
		t.Errorf("OnSettle saw %+v, want %+v", got, s)
	}

	captured := pub.captured()
	if len(captured) != 1 {
		t.Fatalf("captured %d messages, want 1", len(captured))
	}
	c := captured[0]
	if c.Subject != "repo.refs.dlq" {
		t.Errorf("DLQ subject = %q, want repo.refs.dlq", c.Subject)
	}
	if string(c.Data) != "ref-event" {
		t.Errorf("DLQ payload = %q, want the raw message", c.Data)
	}
	if c.Header.Get("X-Entire-Repo") != "01KWHDFJ0C575CRVF18DPVSQW0" {
		t.Error("DLQ copy lost the original headers")
	}
	if reason := c.Header.Get(natsmsg.DLQReasonHeader); !strings.HasPrefix(reason, string(CauseFloorAge)+": ") {
		t.Errorf("DLQ reason = %q, want it prefixed with the cause", reason)
	}
	if c.Header.Get(natsmsg.DLQStreamSeqHeader) != "2774600" {
		t.Errorf("DLQ stream seq = %q, want 2774600", c.Header.Get(natsmsg.DLQStreamSeqHeader))
	}
}

// TestBreakerIsIndependentOfTheGapToTheFloor pins the property directly: the
// distance between the message and the ack floor must not change the outcome,
// because that distance is a function of the consumer's filter and of whether
// the server last recomputed the floor from a pending scan or a drain.
func TestBreakerIsIndependentOfTheGapToTheFloor(t *testing.T) {
	for _, seq := range []uint64{100 + 1, 100 + 2, 100 + 164, 100 + 100000} {
		r, pub := newTestRetry(t, nil)
		armFloor(r, 100, 20*time.Minute)
		armFailing(r, seq, 20*time.Minute)
		msg := deliveredMsg(seq, breakerDelivery)
		if _, err := r.Settle(t.Context(), msg, "transient"); err != nil {
			t.Fatalf("Settle(seq %d): %v", seq, err)
		}
		if !msg.Acked || len(pub.captured()) != 1 {
			t.Errorf("seq %d (floor+%d) not dead-lettered; the breaker is gap-sensitive", seq, seq-100)
		}
	}
}

// logCapture is a thread-safe slog.Handler that counts messages, so a test
// can assert a log line is emitted once per stationary period rather than
// once per delivery.
type logCapture struct {
	mu   sync.Mutex
	msgs []string
}

func (h *logCapture) Enabled(context.Context, slog.Level) bool { return true }

func (h *logCapture) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	h.msgs = append(h.msgs, r.Message)
	h.mu.Unlock()
	return nil
}

func (h *logCapture) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *logCapture) WithGroup(string) slog.Handler      { return h }

func (h *logCapture) snapshot() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.Clone(h.msgs)
}

func (h *logCapture) count(msg string) int {
	n := 0
	for _, m := range h.snapshot() {
		if m == msg {
			n++
		}
	}
	return n
}

// TestSettleBreakerHoldsOff covers every way the quarantine conjunction must
// fail closed. Each case would take an event away from a ladder it can still
// recover on — ENT-1535's forensics found recoveries as late as attempt 4.
func TestSettleBreakerHoldsOff(t *testing.T) {
	tests := []struct {
		name       string
		floorSeq   uint64
		age        time.Duration
		msgSeq     uint64
		attempt    int
		failingFor time.Duration
	}{
		{"floor stalled but not yet past the threshold", 2774436, 14 * time.Minute, 2774437, breakerDelivery, 30 * time.Minute},
		{"message already settled below the floor", 2774436, 30 * time.Minute, 2774400, breakerDelivery, 30 * time.Minute},
		{"message has not itself been failing long enough", 2774436, 30 * time.Minute, 2774437, breakerDelivery, 14 * time.Minute},
		{"message on its first delivery has had no retry at all", 2774436, 30 * time.Minute, 2774437, 1, 30 * time.Minute},
		{"deliveries burned fast by crashes, not by the ladder", 2774436, 30 * time.Minute, 2774437, breakerDelivery, 68 * time.Millisecond},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, pub := newTestRetry(t, nil)
			armFloor(r, tt.floorSeq, tt.age)
			armFailing(r, tt.msgSeq, tt.failingFor)
			msg := deliveredMsg(tt.msgSeq, tt.attempt)

			s, err := r.Settle(t.Context(), msg, "transient")
			if err != nil {
				t.Fatalf("Settle: %v", err)
			}
			if s.Outcome != OutcomeRetried {
				t.Fatalf("Settlement = %+v, want retried", s)
			}
			if s.BreakerTripped {
				t.Error("Settlement reports a trip the conjunction should have refused")
			}
			if msg.Acked || msg.Termed {
				t.Errorf("message acked=%v termed=%v, want it left on the ladder", msg.Acked, msg.Termed)
			}
			if len(pub.captured()) != 0 {
				t.Error("dead-lettered a message the breaker must hold off on")
			}
		})
	}
}

// TestBreakerMeasuresFailureTimeRatherThanInferringIt is the regression test
// for the delivery-count inference. JetStream increments NumDelivered on
// AckWait expiry too — a panicking handler, a killed pod, an eviction under
// MaxAckPending — so a message can reach delivery 4 in milliseconds without
// ever having served a ladder rung. Scoring those as three 5-minute rungs
// would dead-letter a message that had barely arrived, on the strength of a
// stall it had nothing to do with, and would break the "late, never early"
// guarantee outright.
func TestBreakerMeasuresFailureTimeRatherThanInferringIt(t *testing.T) {
	r, pub := newTestRetry(t, nil)
	base := time.Now()
	now := base
	r.clock = func() time.Time { return now }
	armStall(r.cfg.Monitor, 2774436, 30*time.Minute) // an OLD stall, from something else

	// Four crash-deliveries of a freshly arrived message, 68ms apart in total.
	msg := deliveredMsg(2774437, 0)
	for n := 1; n <= 4; n++ {
		now = base.Add(time.Duration(n) * 17 * time.Millisecond)
		msg = deliveredMsg(2774437, n)
		s, err := r.Settle(t.Context(), msg, "handler panicked")
		if err != nil {
			t.Fatalf("Settle(delivery %d): %v", n, err)
		}
		if s.BreakerTripped {
			t.Fatalf("delivery %d: breaker tripped on a message failing for %s; the ladder inference would have scored it %s",
				n, s.FailingFor, 3*5*time.Minute)
		}
	}
	if msg.Acked || len(pub.captured()) != 0 {
		t.Fatal("a message failing for 68ms was dead-lettered during an unrelated stall")
	}

	// Same message, same delivery count, once it really has been failing that
	// long: now it is a legitimate quarantine.
	now = base.Add(16 * time.Minute)
	aged := deliveredMsg(2774437, 4)
	s, err := r.Settle(t.Context(), aged, "still failing")
	if err != nil {
		t.Fatalf("Settle: %v", err)
	}
	if !s.BreakerTripped || !aged.Acked {
		t.Errorf("Settlement = %+v; want the trip once the measured clock reached the threshold", s)
	}
}

// TestBreakerIsSelectiveUnderAMassTransient is the scenario that separates a
// poison-message remedy from a mass dead-lettering engine, and the reason the
// breaker rations itself.
//
// entiredb is down for an hour. Every message in flight fails, the floor is
// stalled, and every one of them satisfies the breaker's conditions: above
// the floor, failing longer than the threshold, past MinDeliveries. Left
// unrationed the breaker would empty the consumer into the DLQ — which is
// precisely the outcome a short ladder would produce, and therefore precisely
// what the breaker exists to avoid. It must claim ONE message per stall
// window instead.
func TestBreakerIsSelectiveUnderAMassTransient(t *testing.T) {
	r, pub := newTestRetry(t, nil)
	base := time.Now()
	now := base
	r.clock = func() time.Time { return now }
	mon := r.cfg.Monitor
	floor := uint64(1000)
	armStall(mon, floor, 30*time.Minute)

	// 200 distinct messages, all failing for well over the threshold.
	settleAll := func() int {
		acked := 0
		for i := range uint64(200) {
			seq := floor + 1 + i
			armFailing(r, seq, 40*time.Minute)
			r.clock = func() time.Time { return now }
			msg := deliveredMsg(seq, breakerDelivery)
			if _, err := r.Settle(t.Context(), msg, "entiredb unavailable"); err != nil {
				t.Fatalf("Settle(seq %d): %v", seq, err)
			}
			if msg.Acked {
				acked++
			}
		}
		return acked
	}

	if n := settleAll(); n != 1 {
		t.Fatalf("%d of 200 messages dead-lettered in one stall window, want exactly 1", n)
	}

	// The claim did not move the floor — it was aimed at a message that was
	// not the blocker. The next window opens one FloorAge later, not sooner.
	now = base.Add(mon.FloorAge() - time.Second)
	armStall(mon, floor, 30*time.Minute)
	r.clock = func() time.Time { return now }
	if n := settleAll(); n != 0 {
		t.Errorf("%d further dead-letters inside the same window, want 0", n)
	}
	now = base.Add(mon.FloorAge() + time.Second)
	armStall(mon, floor, 30*time.Minute)
	r.clock = func() time.Time { return now }
	if n := settleAll(); n != 1 {
		t.Errorf("%d dead-letters in the next window, want exactly 1", n)
	}

	if got := len(pub.captured()); got != 2 {
		t.Errorf("captured %d messages across two windows, want 2", got)
	}
}

// TestBreakerClearsSuccessiveFloorHolders: the flip side of the rationing.
// When a claim DOES move the ack floor it was the blocker, so the next one
// must not have to wait out a window — successive floor-holders drain as fast
// as the stall clock re-arms, which is what makes the breaker a remedy rather
// than a rate limiter.
func TestBreakerClearsSuccessiveFloorHolders(t *testing.T) {
	r, pub := newTestRetry(t, nil)
	now := time.Now()
	r.clock = func() time.Time { return now }
	mon := r.cfg.Monitor

	for i, floor := range []uint64{1000, 1001, 1002} {
		armStall(mon, floor, 30*time.Minute) // floor advanced after the last claim
		seq := floor + 1
		armFailing(r, seq, 40*time.Minute)
		r.clock = func() time.Time { return now }
		msg := deliveredMsg(seq, breakerDelivery)
		if _, err := r.Settle(t.Context(), msg, "poison"); err != nil {
			t.Fatalf("Settle: %v", err)
		}
		if !msg.Acked {
			t.Fatalf("blocker %d not dead-lettered; a moved floor must re-arm the breaker at once", i+1)
		}
	}
	if got := len(pub.captured()); got != 3 {
		t.Errorf("captured %d successive floor-holders, want 3", got)
	}
}

// TestDocumentedAdopterConfigIsValid keeps the wiring example in the Retry
// doc honest. A documented configuration that its own validation would reject
// is worse than no example, and the checks here are strict enough that the
// arithmetic is easy to get wrong by one rung.
func TestDocumentedAdopterConfigIsValid(t *testing.T) {
	floor, err := NewFloorMonitor(FloorMonitorConfig{FloorAge: 20 * time.Minute})
	if err != nil {
		t.Fatalf("NewFloorMonitor: %v", err)
	}
	r, err := NewRetry(RetryConfig{
		MaxDeliver:          6,
		DLQSubject:          "repo.refs.dlq",
		Publisher:           &fakeDLQ{},
		MaxTimeToDeadLetter: 25 * time.Minute,
		RecoverBy:           4,
		Monitor:             floor,
	})
	if err != nil {
		t.Fatalf("the documented adopter config does not validate: %v", err)
	}
	if r.cfg.Breaker != BreakerObserve {
		t.Error("the documented config enforces; the example must stay on the supported observe mode")
	}
	cfg := Config{
		Stream: "repo_refs_v1", Durable: "search-indexer-refs", Name: "search-indexer-refs",
		AckWait: 5 * time.Minute, MaxDeliver: 6, BackOff: testLadder(), Retry: r,
	}
	if err := cfg.schedule().Err(); err != nil {
		t.Fatalf("the documented consumer config does not validate: %v", err)
	}
	// The server ladder carries the SLA: capture on delivery 5, four rungs.
	if got := cfg.schedule().TimeToDeadLetter(); got != 20*time.Minute {
		t.Errorf("scheduled time to dead-letter = %s, want 20m0s", got)
	}
}

// TestMaxTimeToDeadLetterBoundsTheLadder: the guarantee ENT-1535 was missing,
// as config a deployment can assert on. The 2026-08-06 ladder summed to ~34h
// with nothing anywhere saying so; a service that sets this cannot ship that
// consumer at all.
func TestMaxTimeToDeadLetterBoundsTheLadder(t *testing.T) {
	newCfg := func(ladder []time.Duration) Config {
		r, _ := newTestRetry(t, func(c *RetryConfig) {
			c.Monitor, c.Breaker = nil, BreakerObserve
			c.MaxTimeToDeadLetter = 30 * time.Minute
		})
		return Config{
			Stream: "repo_refs_v1", Durable: "d", Name: "c",
			AckWait: ladder[0], MaxDeliver: 6, BackOff: ladder, Retry: r,
		}
	}
	// The ENT-1535 ladder: reaches the dead-letter branch hours past the bound.
	runaway := []time.Duration{5 * time.Minute, 10 * time.Minute, 30 * time.Minute, time.Hour, 4 * time.Hour}
	_, err := Start(t.Context(), nil, newCfg(runaway), func(jetstream.Msg) {})
	if err == nil || !strings.Contains(err.Error(), "MaxTimeToDeadLetter") {
		t.Fatalf("Start(runaway ladder) err = %v, want a MaxTimeToDeadLetter rejection", err)
	}
	// A bounded one gets past the schedule check (and fails later, on the nil conn).
	if _, err := Start(t.Context(), nil, newCfg(testLadder()), func(jetstream.Msg) {}); err == nil ||
		strings.Contains(err.Error(), "MaxTimeToDeadLetter") {
		t.Fatalf("Start(bounded ladder) err = %v, want the schedule accepted", err)
	}
}

// TestNewRetryRejectsEnforceWithoutAMonitor: enforcing with no stall signal
// is a breaker that can never fire, which reads as "configured" and behaves
// as absent — the ENT-1535 disease. Reject it at construction.
func TestNewRetryRejectsEnforceWithoutAMonitor(t *testing.T) {
	_, err := NewRetry(func() RetryConfig {
		c := testRetryConfig(&fakeDLQ{})
		c.Monitor = nil // Breaker stays BreakerEnforce
		return c
	}())
	if err == nil || !strings.Contains(err.Error(), "Monitor is nil") {
		t.Fatalf("NewRetry(enforce, no monitor) err = %v, want a rejection", err)
	}

	// And an unrecognised mode is a typo, not a default.
	if _, err := NewRetry(func() RetryConfig {
		c := testRetryConfig(&fakeDLQ{})
		c.Breaker = BreakerMode(7)
		return c
	}()); err == nil || !strings.Contains(err.Error(), "Breaker is 7") {
		t.Fatalf("NewRetry(unknown mode) err = %v, want a rejection", err)
	}
}

// TestSettleBreakerNeedsAFloorObservation: with no reading yet — a process
// that just started, or a poll that cannot reach the server — the breaker
// stays out of the way. It may be late; it may never be early.
func TestSettleBreakerNeedsAFloorObservation(t *testing.T) {
	r, pub := newTestRetry(t, nil)
	if _, _, ok := r.cfg.Monitor.FloorStall(); ok {
		t.Fatal("FloorStall reports an observation before any poll")
	}
	msg := deliveredMsg(2774437, breakerDelivery) // ladder deliveries left, so only the breaker could act
	if _, err := r.Settle(t.Context(), msg, "transient"); err != nil {
		t.Fatalf("Settle: %v", err)
	}
	if len(pub.captured()) != 0 || msg.Acked {
		t.Error("breaker fired with no ack-floor observation")
	}
}

// TestObserveFloorRestartsTheClockWhenTheFloorMoves: an advancing floor —
// however slowly — is not a stall, and re-observing the same stalled floor
// must not restart the clock either.
func TestObserveFloorRestartsTheClockWhenTheFloorMoves(t *testing.T) {
	r, _ := newTestRetry(t, nil)
	base := time.Now()
	now := base
	r.cfg.Monitor.clock = func() time.Time { return now }

	r.cfg.Monitor.observeFloor(100, 200, base)
	now = base.Add(10 * time.Minute)
	r.cfg.Monitor.observeFloor(100, 200, now) // same stall: the clock keeps running
	if _, age, _ := r.cfg.Monitor.FloorStall(); age != 10*time.Minute {
		t.Fatalf("age after re-observing the same floor = %s, want 10m0s", age)
	}

	r.cfg.Monitor.observeFloor(101, 200, now) // floor advanced: clock restarts
	floor, age, ok := r.cfg.Monitor.FloorStall()
	if !ok || floor != 101 || age != 0 {
		t.Fatalf("after the floor moved: floor=%d age=%s ok=%v, want 101/0s/true", floor, age, ok)
	}

	// And the breaker is disarmed again for the new floor.
	msg := deliveredMsg(102, 2)
	if _, err := r.Settle(t.Context(), msg, "transient"); err != nil {
		t.Fatalf("Settle: %v", err)
	}
	if msg.Acked {
		t.Error("breaker fired against a floor that had just moved")
	}
}

// TestObserveFloorIgnoresACaughtUpConsumer is the false positive that makes
// the difference between a poison remedy and a message shredder: a quiet
// consumer's floor sits still because NOTHING ARRIVED, not because anything
// is stuck. Crediting the next message to arrive with that idle time would
// dead-letter it on its second delivery. The stall clock therefore runs only
// while the consumer has delivered past its own floor, and starts when the
// gap opens — not when the floor last moved.
func TestObserveFloorIgnoresACaughtUpConsumer(t *testing.T) {
	r, pub := newTestRetry(t, nil)
	base := time.Now()
	now := base
	r.cfg.Monitor.clock = func() time.Time { return now }

	// Four idle hours, fully caught up: floor stationary, but not stalled.
	r.cfg.Monitor.observeFloor(100, 100, base)
	now = base.Add(4 * time.Hour)
	r.cfg.Monitor.observeFloor(100, 100, now)
	if _, age, stalled := r.cfg.Monitor.FloorStall(); stalled {
		t.Fatalf("a caught-up consumer reported as stalled for %s", age)
	}

	// A message finally arrives and fails its way to breaker eligibility. The
	// breaker must still not fire: the stall clock starts now, not four hours ago.
	r.cfg.Monitor.observeFloor(100, 101, now)
	msg := deliveredMsg(101, breakerDelivery)
	if _, err := r.Settle(t.Context(), msg, "transient"); err != nil {
		t.Fatalf("Settle: %v", err)
	}
	if msg.Acked || len(pub.captured()) != 0 {
		t.Fatal("breaker dead-lettered a message credited with the consumer's idle time")
	}

	// Once the gap has genuinely persisted past the threshold, it fires.
	now = now.Add(16 * time.Minute)
	armFailing(r, 101, 16*time.Minute)
	blocker := deliveredMsg(101, breakerDelivery)
	if _, err := r.Settle(t.Context(), blocker, "re-queuing: update"); err != nil {
		t.Fatalf("Settle: %v", err)
	}
	if !blocker.Acked || len(pub.captured()) != 1 {
		t.Error("breaker did not fire on a stall that really had persisted")
	}
}

// TestObserveFloorClearsTheStallWhenTheConsumerCatchesUp: a stall that
// resolves must disarm the breaker, not leave it primed on a stale clock.
func TestObserveFloorClearsTheStallWhenTheConsumerCatchesUp(t *testing.T) {
	r, _ := newTestRetry(t, nil)
	base := time.Now()
	r.cfg.Monitor.clock = func() time.Time { return base }

	r.cfg.Monitor.observeFloor(100, 200, base.Add(-30*time.Minute))
	if _, _, stalled := r.cfg.Monitor.FloorStall(); !stalled {
		t.Fatal("floor not reported as stalled")
	}
	r.cfg.Monitor.observeFloor(200, 200, base) // drained
	if _, _, stalled := r.cfg.Monitor.FloorStall(); stalled {
		t.Fatal("stall survived the consumer catching up")
	}
}

// TestSettleDeadLettersOnExhaustion: with the breaker never triggered, the
// ladder still terminates into the DLQ + Ack (ENT-1492), never a bare Term —
// and it does so at MaxDeliver-CaptureReserve, keeping the reserved delivery
// in hand in case the capture fails.
func TestSettleDeadLettersOnExhaustion(t *testing.T) {
	r, pub := newTestRetry(t, func(c *RetryConfig) { c.Monitor, c.Breaker = nil, BreakerObserve })

	// One delivery short of the ladder's end is still the handler's.
	early := deliveredMsg(2774437, exhaustedDelivery-1)
	if _, err := r.Settle(t.Context(), early, "still not ready"); err != nil {
		t.Fatalf("Settle: %v", err)
	}
	if early.Acked || len(pub.captured()) != 0 {
		t.Error("captured before the ladder was spent")
	}

	// Logical exhaustion: MaxDeliver less one delivery reserved for capture.
	msg := deliveredMsg(2774437, exhaustedDelivery)

	s, err := r.Settle(t.Context(), msg, "still not ready")
	if err != nil {
		t.Fatalf("Settle: %v", err)
	}
	if s.Outcome != OutcomeDeadLettered || s.Cause != CauseExhausted {
		t.Fatalf("Settlement = %+v, want dead_lettered/max_deliver", s)
	}
	if !msg.Acked || msg.Termed {
		t.Errorf("final delivery acked=%v termed=%v, want acked only", msg.Acked, msg.Termed)
	}
	if len(pub.captured()) != 1 {
		t.Fatalf("captured %d messages, want 1", len(pub.captured()))
	}
}

// TestSettleNeverDropsOnCaptureFailure: a failed DLQ publish must not become
// a lost message. The original is left untouched for the server's ladder to
// redeliver, the error surfaces as settle:dlq_publish_failed, and the
// Settlement still names the terminal branch that could not complete.
func TestSettleNeverDropsOnCaptureFailure(t *testing.T) {
	r, pub := newTestRetry(t, nil)
	pub.err = errors.New("no responders")
	armFloor(r, 2774436, 20*time.Minute)
	armFailing(r, 2774437, 20*time.Minute)
	msg := deliveredMsg(2774437, breakerDelivery)

	s, err := r.Settle(t.Context(), msg, "re-queuing: update")
	if err == nil || !strings.Contains(err.Error(), "settle:dlq_publish_failed") {
		t.Fatalf("Settle err = %v, want settle:dlq_publish_failed", err)
	}
	if msg.Acked || msg.Termed {
		t.Fatalf("message acked=%v termed=%v after a failed capture, want it unsettled", msg.Acked, msg.Termed)
	}
	if msg.Naks != 0 || len(msg.NakDelays) != 0 {
		t.Errorf("message naked (%d/%d); want it left for the ladder to redeliver", msg.Naks, len(msg.NakDelays))
	}
	if s.Outcome != OutcomeRetried || s.Cause != CauseFloorAge {
		t.Errorf("Settlement = %+v, want retried but still attributed to floor_age", s)
	}
}

// TestSettleStrandsWhenCaptureFailsWithNoDeliveriesLeft: at the broker's own
// cap nothing redelivers, so nothing will retry the capture — calling that
// "retried" would tell the adopter's metrics the opposite of the truth. The
// message is left unsettled (still pinning the floor, so the monitor fires)
// and reported as what it is: stranded, needing the break-glass runbook.
func TestSettleStrandsWhenCaptureFailsWithNoDeliveriesLeft(t *testing.T) {
	var got Settlement
	r, _ := newTestRetry(t, func(c *RetryConfig) {
		c.OnSettle = func(_ context.Context, _ jetstream.Msg, s Settlement) { got = s }
	})
	pub, ok := r.cfg.Publisher.(*fakeDLQ)
	if !ok {
		t.Fatal("publisher is not the fake")
	}
	pub.err = errors.New("dlq stream unavailable")
	msg := deliveredMsg(2774437, finalDelivery) // the broker's own cap: nothing left

	s, err := r.Settle(t.Context(), msg, "still not ready")
	if err == nil || !strings.Contains(err.Error(), "settle:stranded") {
		t.Fatalf("Settle err = %v, want settle:stranded", err)
	}
	if s.Outcome != OutcomeStranded {
		t.Fatalf("Outcome = %q, want %q — nothing redelivers past the cap, so this is not a retry", s.Outcome, OutcomeStranded)
	}
	if msg.Acked || msg.Termed || msg.Naks != 0 || len(msg.NakDelays) != 0 {
		t.Errorf("stranded message disposed (ack=%v term=%v naks=%d/%d), want it left unsettled",
			msg.Acked, msg.Termed, msg.Naks, len(msg.NakDelays))
	}
	if got.Outcome != OutcomeStranded {
		t.Error("OnSettle did not see the stranded outcome; the adopter cannot alert on it")
	}
}

// TestCaptureReserveRetriesAFailedCapture: the reserve exists so a DLQ blip
// at exactly the wrong moment is a retry rather than a stranding. The logically
// exhausting delivery captures, fails, and is left for the ladder with the
// reserved delivery still in hand.
func TestCaptureReserveRetriesAFailedCapture(t *testing.T) {
	r, pub := newTestRetry(t, nil)
	pub.err = errors.New("dlq stream unavailable")
	msg := deliveredMsg(2774437, exhaustedDelivery) // logical exhaustion, one delivery reserved

	s, err := r.Settle(t.Context(), msg, "still not ready")
	if err == nil || !strings.Contains(err.Error(), "settle:dlq_publish_failed") {
		t.Fatalf("Settle err = %v, want settle:dlq_publish_failed", err)
	}
	if s.Outcome != OutcomeRetried || s.Cause != CauseExhausted {
		t.Fatalf("Settlement = %+v, want retried but attributed to max_deliver", s)
	}
	if msg.Naks != 0 || len(msg.NakDelays) != 0 {
		t.Errorf("message naked (%d/%d); the reserved delivery arrives via the ladder", msg.Naks, len(msg.NakDelays))
	}
	if msg.Acked || msg.Termed {
		t.Error("message disposed despite a failed capture")
	}
}

// TestNoCaptureReserveSpendsEveryDelivery: the explicit opt-out gives the
// handler the whole budget and accepts the stranding risk at the cap.
func TestNoCaptureReserveSpendsEveryDelivery(t *testing.T) {
	r, pub := newTestRetry(t, func(c *RetryConfig) { c.CaptureReserve = NoCaptureReserve })
	if _, err := r.Settle(t.Context(), deliveredMsg(1, finalDelivery-1), "transient"); err != nil {
		t.Fatalf("Settle: %v", err)
	}
	if len(pub.captured()) != 0 {
		t.Error("captured before the broker's cap with no reserve; the handler should have every delivery")
	}
	if _, err := r.Settle(t.Context(), deliveredMsg(1, finalDelivery), "transient"); err != nil {
		t.Fatalf("Settle: %v", err)
	}
	if len(pub.captured()) != 1 {
		t.Error("did not capture on the broker's final delivery")
	}
}

// TestBreakerObserveOnly is the rollout position: evaluate and report, act on
// nothing. An adopter runs this against real traffic to prove the trip rate
// before letting the library Ack messages on its own.
func TestBreakerObserveOnly(t *testing.T) {
	logs := &logCapture{}
	r, pub := newTestRetry(t, func(c *RetryConfig) {
		c.Breaker = BreakerObserve
		c.Logger = slog.New(logs)
	})
	armFloor(r, 2774436, 20*time.Minute)
	armFailing(r, 2774437, 20*time.Minute)
	msg := deliveredMsg(2774437, breakerDelivery)

	s, err := r.Settle(t.Context(), msg, "re-queuing: update")
	if err != nil {
		t.Fatalf("Settle: %v", err)
	}
	if !s.BreakerTripped {
		t.Error("Settlement did not report the trip; there is nothing to count in observe mode")
	}
	if s.Outcome != OutcomeRetried || s.Cause != "" {
		t.Fatalf("Settlement = %+v, want the message left on the ladder", s)
	}
	if msg.Acked || len(pub.captured()) != 0 {
		t.Error("observe-only breaker acted on the message")
	}
	if logs.count("search-indexer-refs: breaker would dead-letter this message (observe only)") != 1 {
		t.Errorf("no observe-only log line; saw %v", logs.snapshot())
	}

	// Observing is a faithful dry run: the trip consumed the same
	// one-per-window budget enforcing would have, so the count an adopter
	// collects is what enforcing would do — not the far larger number of
	// messages that merely satisfied the conditions.
	trips := 0
	for i := range uint64(50) {
		seq := 2774438 + i
		armFailing(r, seq, 20*time.Minute)
		other := deliveredMsg(seq, breakerDelivery)
		s, err := r.Settle(t.Context(), other, "transient")
		if err != nil {
			t.Fatalf("Settle: %v", err)
		}
		if s.BreakerTripped {
			trips++
		}
	}
	if trips != 0 {
		t.Errorf("%d further trips reported inside one stall window; observe mode would over-report what enforcing does", trips)
	}
}

// TestSettleReportsAckFailureAfterCapture: the copy is safe in the DLQ but
// the original is unsettled, so it will redeliver and be captured again. A
// duplicate in the DLQ beats a lost message or a permanently pinned floor —
// the caller just has to hear about it (settle:dlq_ack_failed).
func TestSettleReportsAckFailureAfterCapture(t *testing.T) {
	r, pub := newTestRetry(t, nil)
	armFloor(r, 2774436, 20*time.Minute)
	armFailing(r, 2774437, 20*time.Minute)
	msg := deliveredMsg(2774437, breakerDelivery)
	msg.AckErr = errors.New("connection closed")

	s, err := r.Settle(t.Context(), msg, "re-queuing: update")
	if err == nil || !strings.Contains(err.Error(), "settle:dlq_ack_failed") {
		t.Fatalf("Settle err = %v, want settle:dlq_ack_failed", err)
	}
	if len(pub.captured()) != 1 {
		t.Errorf("captured %d messages, want the copy to have landed first", len(pub.captured()))
	}
	if s.Outcome != OutcomeRetried {
		t.Errorf("Outcome = %q, want the message reported as still coming back, not dead-lettered", s.Outcome)
	}
	if msg.Termed {
		t.Error("message Termed after a failed Ack")
	}
}

// TestTerminalAckIsConfirmed: the dead-letter path settles with DoubleAck, not
// a fire-and-forget Ack. A lost plain Ack would leave the floor pinned with
// nothing reporting it — the settlement has to be a fact, not a hope.
func TestTerminalAckIsConfirmed(t *testing.T) {
	r, _ := newTestRetry(t, nil)
	msg := deliveredMsg(2774437, exhaustedDelivery)
	if _, err := r.Settle(t.Context(), msg, "still not ready"); err != nil {
		t.Fatalf("Settle: %v", err)
	}
	if msg.DoubleAcks != 1 {
		t.Errorf("DoubleAcks = %d, want 1 (a plain Ack is not confirmation)", msg.DoubleAcks)
	}
}

// ctxRespectingDLQ fails a publish on a cancelled context, the way a real
// JetStream publish does. The ordinary fakeDLQ ignores ctx entirely, so a
// cancellation test written against it would pass just as happily on code that
// captures on the handler's cancelled context — the fake would never notice.
type ctxRespectingDLQ struct {
	fakeDLQ

	sawDone []bool // one entry per publish: whether its ctx was cancellable at all
}

func (f *ctxRespectingDLQ) PublishMsg(ctx context.Context, m *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	f.mu.Lock()
	f.sawDone = append(f.sawDone, ctx.Done() != nil)
	f.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return f.fakeDLQ.PublishMsg(ctx, m, opts...)
}

// ctxRecordingMsg reports the context its DoubleAck was given. FakeMsg accepts
// and discards it, so the ack half of the terminate path needs this to be
// observable at all.
type ctxRecordingMsg struct {
	*natsmsgtest.FakeMsg

	ackCtxErr  error
	ackCtxDone bool
}

func (m *ctxRecordingMsg) DoubleAck(ctx context.Context) error {
	m.ackCtxErr, m.ackCtxDone = ctx.Err(), ctx.Done() != nil
	return m.FakeMsg.DoubleAck(ctx)
}

// TestTerminateSurvivesAShutdownCancelledContext: a shutdown racing the FINAL
// delivery must not manufacture a strand.
//
// The documented ordering cancels the loops' context, joins them, and only then
// drains the connections (nuts.ShutdownGroup), so for the whole join window the
// handler ctx is done while the connection still works. Capturing on that ctx
// failed on ctx.Err() alone and reported OutcomeStranded — the one outcome that
// means "a human must run the break-glass runbook" — for a message the broker
// would have accepted. The terminal path therefore runs on a context derived
// with WithoutCancel, still bounded by its own timeouts.
func TestTerminateSurvivesAShutdownCancelledContext(t *testing.T) {
	pub := &ctxRespectingDLQ{}
	r, err := NewRetry(testRetryConfig(pub))
	if err != nil {
		t.Fatalf("NewRetry: %v", err)
	}
	msg := &ctxRecordingMsg{FakeMsg: deliveredMsg(2774437, finalDelivery)}

	// Exactly the state Shutdown leaves behind during the join window: context
	// cancelled, connection still up.
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	s, err := r.Settle(ctx, msg, "still not ready")
	if err != nil {
		t.Fatalf("Settle on a cancelled context: %v (want the capture to proceed)", err)
	}
	if s.Outcome != OutcomeDeadLettered {
		t.Errorf("Outcome = %q, want %q — a cancelled handler context must not invent a strand",
			s.Outcome, OutcomeDeadLettered)
	}
	if !s.Captured {
		t.Error("not captured; the DLQ copy is the whole point of the terminal path")
	}
	if got := len(pub.captured()); got != 1 {
		t.Errorf("captured %d messages, want 1", got)
	}
	if msg.DoubleAcks != 1 {
		t.Errorf("DoubleAcks = %d, want 1 — the floor is only released by a confirmed ack", msg.DoubleAcks)
	}
	if msg.ackCtxErr != nil {
		t.Errorf("DoubleAck ctx.Err() = %v, want nil", msg.ackCtxErr)
	}

	// Both halves keep a Done channel, because each still applies its own
	// timeout — detaching cancellation must not mean detaching the bound.
	if !msg.ackCtxDone {
		t.Error("DoubleAck ctx has no deadline; dlqAckTimeout is no longer bounding it")
	}
	pub.mu.Lock()
	sawDone := append([]bool(nil), pub.sawDone...)
	pub.mu.Unlock()
	if len(sawDone) != 1 || !sawDone[0] {
		t.Errorf("capture ctx cancellable = %v, want exactly one bounded publish", sawDone)
	}
}

// TestSettleNeverTerms pins the invariant across every path: this package's
// terminal branch is dead-letter-then-Ack, and a bare Term never happens. Not
// because a Term fails to settle — measured against nats-server it settles
// cleanly, advancing the ack floor past the message — but because it settles
// with no record of what was discarded, which is the surface this package
// exists to close.
func TestSettleNeverTerms(t *testing.T) {
	cases := map[string]func(*Retry, *fakeDLQ) *natsmsgtest.FakeMsg{
		"ladder": func(*Retry, *fakeDLQ) *natsmsgtest.FakeMsg { return deliveredMsg(10, 2) },
		"breaker": func(r *Retry, _ *fakeDLQ) *natsmsgtest.FakeMsg {
			armFloor(r, 9, 30*time.Minute)
			armFailing(r, 10, 30*time.Minute)
			return deliveredMsg(10, breakerDelivery)
		},
		"exhaustion": func(*Retry, *fakeDLQ) *natsmsgtest.FakeMsg { return deliveredMsg(10, exhaustedDelivery) },
		"capture failure": func(_ *Retry, pub *fakeDLQ) *natsmsgtest.FakeMsg {
			pub.err = errors.New("down")
			return deliveredMsg(10, finalDelivery)
		},
		"nak failure": func(*Retry, *fakeDLQ) *natsmsgtest.FakeMsg {
			m := deliveredMsg(10, 2)
			m.NakErr = errors.New("down")
			return m
		},
		"no metadata": func(*Retry, *fakeDLQ) *natsmsgtest.FakeMsg {
			return &natsmsgtest.FakeMsg{DataVal: []byte("x")}
		},
	}
	for name, build := range cases {
		t.Run(name, func(t *testing.T) {
			r, pub := newTestRetry(t, nil)
			msg := build(r, pub)
			// Several cases fail on purpose; the invariant under test is
			// that no failure mode reaches for a Term.
			if _, err := r.Settle(t.Context(), msg, "reason"); err != nil {
				t.Logf("Settle (expected for this case): %v", err)
			}
			if msg.Termed {
				t.Error("Settle Termed the message")
			}
		})
	}
}

// TestDeadLetterCapturesAndAcks: the non-lossy replacement for a bare Term,
// for a failure the handler knows is permanent.
func TestDeadLetterCapturesAndAcks(t *testing.T) {
	r, pub := newTestRetry(t, nil)
	msg := deliveredMsg(2774437, 1)

	s, err := r.DeadLetter(t.Context(), msg, "checkpoint id \"update\" is not a checkpoint id")
	if err != nil {
		t.Fatalf("DeadLetter: %v", err)
	}
	if s.Outcome != OutcomeDeadLettered || s.Cause != CausePermanent {
		t.Fatalf("Settlement = %+v, want dead_lettered/permanent", s)
	}
	if !msg.Acked || msg.Termed {
		t.Errorf("acked=%v termed=%v, want acked only", msg.Acked, msg.Termed)
	}
	captured := pub.captured()
	if len(captured) != 1 {
		t.Fatalf("captured %d messages, want 1", len(captured))
	}
	if reason := captured[0].Header.Get(natsmsg.DLQReasonHeader); !strings.HasPrefix(reason, string(CausePermanent)+": ") {
		t.Errorf("DLQ reason = %q, want it prefixed with the cause", reason)
	}
}

// TestDeadLetterNeverDropsOnCaptureFailure: same never-drop rule as Settle.
func TestDeadLetterNeverDropsOnCaptureFailure(t *testing.T) {
	r, pub := newTestRetry(t, nil)
	pub.err = errors.New("no responders")
	msg := deliveredMsg(2774437, 1)

	if _, err := r.DeadLetter(t.Context(), msg, "unprocessable"); err == nil {
		t.Fatal("DeadLetter succeeded with a failing publisher")
	}
	if msg.Acked || msg.Termed {
		t.Errorf("acked=%v termed=%v after a failed capture, want it unsettled", msg.Acked, msg.Termed)
	}
	if msg.Naks != 0 || len(msg.NakDelays) != 0 {
		t.Errorf("message naked (%d/%d); want it left for the ladder to redeliver", msg.Naks, len(msg.NakDelays))
	}
}

// TestFloorMonitorDrivesAnAdopterOwnedQuarantine is the composition for an
// adopter that wants the stall signal but keeps the judgement itself: a
// monitor on the Config, a Retry with no monitor, and the quarantine rule
// written in the handler against FloorStall + DeadLetter. The library
// supplies the observation and the non-lossy capture; the policy is the
// adopter's.
func TestFloorMonitorDrivesAnAdopterOwnedQuarantine(t *testing.T) {
	mon := mustFloorMonitor(FloorMonitorConfig{FloorAge: 10 * time.Minute})
	r, pub := newTestRetry(t, func(c *RetryConfig) { c.Monitor, c.Breaker = nil, BreakerObserve })

	// The adopter's own rule, whatever shape it likes.
	quarantine := func(msg *natsmsgtest.FakeMsg) {
		if !mon.Stalled() {
			if _, err := r.Settle(t.Context(), msg, "transient"); err != nil {
				t.Fatalf("Settle: %v", err)
			}
			return
		}
		if _, err := r.DeadLetter(t.Context(), msg, "blocking a stalled floor"); err != nil {
			t.Fatalf("DeadLetter: %v", err)
		}
	}

	armStall(mon, 100, 5*time.Minute) // under the threshold
	healthy := deliveredMsg(101, 2)
	quarantine(healthy)
	if healthy.Acked || len(pub.captured()) != 0 {
		t.Fatal("quarantined a message while the monitor read healthy")
	}

	armStall(mon, 100, 30*time.Minute)
	blocker := deliveredMsg(101, 2)
	quarantine(blocker)
	if !blocker.Acked || len(pub.captured()) != 1 {
		t.Error("adopter-owned quarantine did not capture and settle the blocker")
	}
}

// TestConfigRejectsConflictingFloorMonitors: one durable has one ack floor,
// so naming two monitors would mean two poll loops disagreeing about the same
// number.
func TestConfigRejectsConflictingFloorMonitors(t *testing.T) {
	r, _ := newTestRetry(t, nil)
	cfg := Config{
		Stream:       "repo_refs_v1",
		Durable:      "search-indexer-refs",
		Name:         "search-indexer-refs",
		AckWait:      5 * time.Minute,
		MaxDeliver:   6,
		BackOff:      testLadder(),
		Retry:        r,
		FloorMonitor: mustFloorMonitor(FloorMonitorConfig{}), // a different one
	}
	_, err := Start(t.Context(), nil, cfg, func(jetstream.Msg) {})
	if err == nil || !strings.Contains(err.Error(), "different") {
		t.Fatalf("Start(two monitors) err = %v, want a conflict error", err)
	}

	// The same monitor on both is the normal wiring, not a conflict.
	cfg.FloorMonitor = r.Monitor()
	if _, err := Start(t.Context(), nil, cfg, func(jetstream.Msg) {}); err == nil ||
		strings.Contains(err.Error(), "different") {
		t.Fatalf("Start(same monitor on both) err = %v, want it accepted (failing later on the nil conn)", err)
	}
}

// TestConfigPairsTheServerLadderWithRetry: the server owns the redelivery
// schedule and Retry owns where retries end, so BackOff and Retry together are
// the intended shape — not the mutual exclusion an earlier revision enforced.
//
// The defect that rule existed to prevent has not gone away; it has become
// unrepresentable. RetryConfig no longer has ladder fields at all, so a
// consumer cannot run a client-side NakWithDelay schedule against the server's
// — there is nothing to set. Schedule still models and flags the combination
// forever, because fleet CI has to catch it in configs this library would
// never construct (see TestScheduleCatchesTheENT1535Consumer).
func TestConfigPairsTheServerLadderWithRetry(t *testing.T) {
	r, _ := newTestRetry(t, nil)
	cfg := Config{
		Stream:     "repo_refs_v1",
		Durable:    "search-indexer-refs",
		Name:       "search-indexer-refs",
		AckWait:    5 * time.Minute,
		MaxDeliver: 6,
		BackOff:    testLadder(),
		Retry:      r,
	}
	if err := cfg.validate(nil, func(jetstream.Msg) {}); err != nil && !strings.Contains(err.Error(), "nil nats conn") {
		t.Fatalf("validate(server ladder + Retry) = %v, want the pairing accepted", err)
	}
	// And the detector survives for configs the library cannot build.
	mixed := cfg.schedule()
	mixed.NakDelay = 30 * time.Second
	if vs := mixed.Validate(); !hasField(vs, "ServerBackOff") {
		t.Errorf("Schedule stopped flagging two competing ladders; got %v", fields(vs))
	}
}

// TestConfigValidatesAnOmittedLadder: the schedule check must not be
// skippable by leaving BackOff out. Start writes a nil BackOff to the durable,
// which clears any ladder and leaves AckWait as the schedule — so an omitted
// ladder is a real, and often very long, one.
func TestConfigValidatesAnOmittedLadder(t *testing.T) {
	r, _ := newTestRetry(t, func(c *RetryConfig) {
		c.Monitor, c.Breaker = nil, BreakerObserve
		c.MaxTimeToDeadLetter = 25 * time.Minute
	})
	cfg := Config{
		Stream: "repo_refs_v1", Durable: "d", Name: "c",
		AckWait: time.Hour, MaxDeliver: 6, Retry: r, // no BackOff at all
	}
	err := cfg.validate(nil, func(jetstream.Msg) {})
	if err == nil || !strings.Contains(err.Error(), "MaxTimeToDeadLetter") {
		t.Fatalf("validate(no BackOff, 1h AckWait) = %v, want the four-hour effective ladder rejected", err)
	}
	// Sized sensibly, the same shape is legitimate and must pass.
	cfg.AckWait = 5 * time.Minute
	if err := cfg.validate(nil, func(jetstream.Msg) {}); err != nil && !strings.Contains(err.Error(), "nil nats conn") {
		t.Fatalf("validate(no BackOff, 5m AckWait) = %v, want it accepted", err)
	}
}

// TestConfigRejectsRetryMaxDeliverMismatch: with Retry the terminal branch is
// the DLQ capture, so a policy that disagrees with the broker's cap is a lost
// message rather than a late one.
func TestConfigRejectsRetryMaxDeliverMismatch(t *testing.T) {
	r, _ := newTestRetry(t, nil) // MaxDeliver 6
	cfg := Config{
		Stream:     "repo_refs_v1",
		Durable:    "search-indexer-refs",
		Name:       "search-indexer-refs",
		AckWait:    5 * time.Minute,
		MaxDeliver: 7,
		BackOff:    testLadder(),
		Retry:      r,
	}
	_, err := Start(t.Context(), nil, cfg, func(jetstream.Msg) {})
	if err == nil || !strings.Contains(err.Error(), "MaxDeliver") {
		t.Fatalf("Start(mismatched MaxDeliver) err = %v, want a MaxDeliver mismatch error", err)
	}

	// The consumer's own default counts as its cap, not the raw zero field.
	cfg.MaxDeliver = 0
	if _, err := Start(t.Context(), nil, cfg, func(jetstream.Msg) {}); err == nil ||
		!strings.Contains(err.Error(), "consumer's is 8") {
		t.Fatalf("Start(default MaxDeliver) err = %v, want it compared against EffectiveMaxDeliver", err)
	}
}

// TestConfigRejectsUnworkableBackOff keeps the server-ladder escape hatch
// honest: the server rejects these too, but naming both fields beats a bare
// API error at consumer creation.
func TestConfigRejectsUnworkableBackOff(t *testing.T) {
	base := Config{Stream: "s_v1", Durable: "d", Name: "c"}
	tests := []struct {
		name string
		cfg  Config
		want string
	}{
		{"non-positive rung", func() Config {
			c := base
			c.AckWait = time.Minute
			c.BackOff = []time.Duration{time.Minute, 0}
			return c
		}(), "rung 1 must be positive"},
		{"more rungs than redeliveries", func() Config {
			c := base
			c.AckWait = time.Minute
			c.MaxDeliver = 2
			c.BackOff = []time.Duration{time.Minute, time.Minute, time.Minute}
			return c
		}(), "schedules at most 1 of them"}, // MaxDeliver 2 = one redelivery, not two
		{"ackWait disagrees with the first rung", func() Config {
			c := base
			c.AckWait = 30 * time.Second
			c.BackOff = []time.Duration{time.Minute, time.Minute}
			return c
		}(), "AckWait"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Start(t.Context(), nil, tt.cfg, func(jetstream.Msg) {})
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Start() err = %v, want error containing %q", err, tt.want)
			}
		})
	}
}

// TestProcessDeadLettersUndecodable: with a Retry configured, an undecodable
// payload is captured rather than Termed — closing the last
// drop-without-a-record surface in the scaffold. Without one, Process keeps
// its original Term (see TestProcessTermsUndecodable).
func TestProcessDeadLettersUndecodable(t *testing.T) {
	r, pub := newTestRetry(t, nil)
	cfg := testCfg
	cfg.Retry = r
	msg := deliveredMsg(2774437, 1)
	msg.DataVal = []byte("garbage")

	Process(t.Context(), msg, cfg,
		func([]byte) (string, error) { return "", errors.New("bad payload") },
		func(context.Context, trace.Span, jetstream.Msg, string) { t.Error("handle called on a decode error") },
		nil)

	if msg.Termed {
		t.Error("undecodable payload Termed with a Retry configured, want dead-lettered")
	}
	if !msg.Acked {
		t.Error("undecodable payload not Acked after capture")
	}
	captured := pub.captured()
	if len(captured) != 1 {
		t.Fatalf("captured %d messages, want 1", len(captured))
	}
	if reason := captured[0].Header.Get(natsmsg.DLQReasonHeader); !strings.Contains(reason, "undecodable") {
		t.Errorf("DLQ reason = %q, want it to say the payload was undecodable", reason)
	}
	if string(captured[0].Data) != "garbage" {
		t.Errorf("DLQ payload = %q, want the raw undecodable bytes", captured[0].Data)
	}
}

// TestStartWritesTheConfiguredLadder covers the interim adoption path and the
// hazard that comes with it, against a real broker.
//
// search-indexer-refs already carries a stale six-rung ladder on its durable.
// Start writes Config.BackOff over it, so adopting the library replaces the
// old envelope with the declared one — that is how the migration lands
// without a separate consumer-config change.
//
// The same mechanism is why an OMITTED BackOff is dangerous rather than
// neutral: Start sends whatever is configured, so a nil wipes the durable's
// ladder and leaves AckWait as the schedule. A declaratively-managed ladder
// cannot simply be left out here until bind-only mode exists.
func TestStartWritesTheConfiguredLadder(t *testing.T) {
	nc, js := runTestEnv(t)
	if _, err := js.CreateStream(t.Context(), jetstream.StreamConfig{
		Name: "repo_refs_v1", Subjects: []string{"repo.refs.>"},
	}); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	// The durable as an earlier deployment left it.
	stale := []time.Duration{5 * time.Minute, 10 * time.Minute, 30 * time.Minute, time.Hour}
	if _, err := js.CreateOrUpdateConsumer(t.Context(), "repo_refs_v1", jetstream.ConsumerConfig{
		Durable: "search_indexer_refs", AckPolicy: jetstream.AckExplicitPolicy,
		AckWait: 5 * time.Minute, MaxDeliver: 5, BackOff: stale,
	}); err != nil {
		t.Fatalf("create consumer with a stale ladder: %v", err)
	}

	serverBackOff := func(t *testing.T) []time.Duration {
		t.Helper()
		cons, err := js.Consumer(t.Context(), "repo_refs_v1", "search_indexer_refs")
		if err != nil {
			t.Fatalf("consumer: %v", err)
		}
		info, err := cons.Info(t.Context())
		if err != nil {
			t.Fatalf("consumer info: %v", err)
		}
		return info.Config.BackOff
	}
	start := func(t *testing.T, backOff []time.Duration) {
		t.Helper()
		r, _ := newTestRetry(t, nil)
		run, err := Start(t.Context(), nc, Config{
			Stream: "repo_refs_v1", Durable: "search_indexer_refs",
			Name: "search-indexer-refs", AckWait: 5 * time.Minute,
			MaxDeliver: 6, BackOff: backOff, Retry: r,
		}, func(jetstream.Msg) {})
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		t.Cleanup(run.Stop)
	}

	start(t, testLadder())
	if got := serverBackOff(t); len(got) != len(testLadder()) || got[0] != 5*time.Minute {
		t.Fatalf("server BackOff = %v, want the declared ladder written over the stale one", got)
	}

	// And the hazard: omitting it erases what is there.
	start(t, nil)
	if got := serverBackOff(t); len(got) != 0 {
		t.Fatalf("server BackOff = %v, want a nil Config.BackOff to have cleared it", got)
	}
}

// TestStartObservesTheAckFloor drives the poll loop against a live consumer:
// the breaker's input is the durable's real ack floor, and Stop joins the
// poll so the connection it reads through is safe to drain afterwards.
func TestStartObservesTheAckFloor(t *testing.T) {
	nc, js := runTestEnv(t)
	if _, err := js.CreateStream(t.Context(), jetstream.StreamConfig{
		Name: "repo_refs_v1", Subjects: []string{"repo.refs.>"},
	}); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	for range 3 {
		if _, err := js.Publish(t.Context(), "repo.refs.update", []byte("ref")); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}

	r, _ := newTestRetry(t, func(c *RetryConfig) {
		c.Monitor = mustFloorMonitor(FloorMonitorConfig{
			FloorAge: time.Minute, Poll: 10 * time.Millisecond, Name: "search-indexer-refs",
		})
	})
	acked := make(chan struct{}, 3)
	run, err := Start(t.Context(), nc, Config{
		Stream:     "repo_refs_v1",
		Durable:    "search_indexer_refs",
		Name:       "search-indexer-refs",
		MaxDeliver: 6,
		Retry:      r,
	}, func(m jetstream.Msg) {
		if err := m.Ack(); err != nil {
			t.Errorf("ack: %v", err)
		}
		acked <- struct{}{}
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	for range 3 {
		select {
		case <-acked:
		case <-time.After(5 * time.Second):
			t.Fatal("messages did not reach the handler")
		}
	}

	// The poll reads the durable's real ack floor. All three messages acked,
	// so the consumer is caught up: the floor is at 3 and — correctly — NOT
	// reported as stalled, because a floor that stops moving on a drained
	// consumer is idleness, not a stall.
	deadline := time.Now().Add(5 * time.Second)
	for {
		floor, _, stalled := r.cfg.Monitor.FloorStall()
		if floor == 3 {
			if stalled {
				t.Fatal("a fully drained consumer reported as stalled")
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("ack floor never observed at 3 (got %d)", floor)
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Stop joins the poll loop, so nothing is still reading through nc.
	done := make(chan struct{})
	go func() { run.Stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not join the ack-floor poll")
	}
	if got := r.Monitor().pollInterval(); got != 10*time.Millisecond {
		t.Errorf("pollInterval = %s, want the configured 10ms", got)
	}
}

// TestRunJoinsThePollBeforeRecreating: Run recreates a consume loop that
// closed underneath it, and reattaches the SAME monitor to the new consumer.
// If it only cancelled the attempt instead of joining, the old poll could
// still be in an in-flight consumer-info request whose reply lands after the
// reattach and overwrites fresh state with stale — or races the attach
// outright. Driven with a fast poll under -race, and asserted on the state
// the monitor is left in.
func TestRunJoinsThePollBeforeRecreating(t *testing.T) {
	compressRunRetries(t)
	nc, js := runTestEnv(t)
	if _, err := js.CreateStream(t.Context(), jetstream.StreamConfig{
		Name: "repo_refs_v1", Subjects: []string{"repo.refs.>"},
	}); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	mon := mustFloorMonitor(FloorMonitorConfig{
		FloorAge: time.Minute, Poll: time.Millisecond, Name: "recreate",
	})
	r, _ := newTestRetry(t, func(c *RetryConfig) {
		c.Monitor = mon
	})

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, nc, Config{
			Stream: "repo_refs_v1", Durable: "recreate_durable",
			Name: "recreate", MaxDeliver: 6, Retry: r,
		}, func(m jetstream.Msg) {
			if err := m.Ack(); err != nil {
				t.Errorf("ack: %v", err)
			}
		})
	}()

	// Force several close-and-recreate cycles by deleting the durable out
	// from under the live consume loop.
	for range 3 {
		deadline := time.Now().Add(5 * time.Second)
		for {
			if _, err := js.Consumer(t.Context(), "repo_refs_v1", "recreate_durable"); err == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("consumer never appeared")
			}
			time.Sleep(5 * time.Millisecond)
		}
		if err := js.DeleteConsumer(t.Context(), "repo_refs_v1", "recreate_durable"); err != nil {
			t.Fatalf("delete consumer: %v", err)
		}
		time.Sleep(60 * time.Millisecond)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after ctx cancel")
	}
	// The monitor belongs to one consumer, so at no point may two polls have
	// been live: a second one means Run reattached while the previous poll was
	// still running, and that poll's in-flight reply describes a consumer that
	// no longer exists.
	mon.mu.Lock()
	maxPolls, live := mon.maxPolls, mon.polls
	mon.mu.Unlock()
	if maxPolls > 1 {
		t.Errorf("%d ack-floor polls were live at once; Run reattached the monitor without joining the old one", maxPolls)
	}
	if live != 0 {
		t.Errorf("%d ack-floor polls still live after Run returned", live)
	}
	// A drained consumer is not stalled; a stale reading from a dead consumer
	// is exactly what would leave a phantom stall behind.
	if _, age, stalled := mon.FloorStall(); stalled {
		t.Errorf("monitor left reporting a stall (%s) after recreates on an empty stream", age)
	}
}

// TestStartWithoutBreakerRunsNoPoll: a Retry with no monitor is still the one
// retry mechanism, but it costs no consumer-info traffic and Stop has no poll
// to join.
func TestStartWithoutBreakerRunsNoPoll(t *testing.T) {
	nc, js := runTestEnv(t)
	if _, err := js.CreateStream(t.Context(), jetstream.StreamConfig{
		Name: "repo_refs_v1", Subjects: []string{"repo.refs.>"},
	}); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	r, _ := newTestRetry(t, func(c *RetryConfig) { c.Monitor, c.Breaker = nil, BreakerObserve })
	run, err := Start(t.Context(), nc, Config{
		Stream:     "repo_refs_v1",
		Durable:    "no_breaker",
		Name:       "no-breaker",
		MaxDeliver: 6,
		Retry:      r,
	}, func(jetstream.Msg) {})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if run.pollDone != nil {
		t.Error("Start ran an ack-floor poll with no monitor configured")
	}
	run.Stop()
	if r.Monitor() != nil {
		t.Error("Retry reports a monitor it was not given")
	}
}

// TestSettleActuallyServesTheServerLadder is the test whose absence let a P1
// through: everything else here drives a FakeMsg and can only assert which
// disposition method was called, which says nothing about whether the server
// then honours the configured ladder.
//
// It does not. A consumer's BackOff governs acknowledgement TIMEOUTS —
// measured against a live server, a plain Nak redelivers in 0s while letting
// AckWait expire redelivers on the rung. An earlier revision of Settle plain-
// Nak'd on the retry path, so the documented ladder was never served and a
// transient failure hot-looped through MaxDeliver into the DLQ in
// milliseconds. This pins the real timing against a real broker.
func TestSettleActuallyServesTheServerLadder(t *testing.T) {
	nc, js := runTestEnv(t)
	if _, err := js.CreateStream(t.Context(), jetstream.StreamConfig{
		Name: "repo_refs_v1", Subjects: []string{"repo.refs.>"},
	}); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	const rung = 900 * time.Millisecond
	ladder := []time.Duration{rung, rung, rung}

	r, pub := newTestRetry(t, func(c *RetryConfig) {
		c.MaxDeliver, c.Monitor, c.Breaker = 4, nil, BreakerObserve
	})
	var mu sync.Mutex
	var at []time.Time
	run, err := Start(t.Context(), nc, Config{
		Stream: "repo_refs_v1", Durable: "ladder", Name: "ladder",
		AckWait: rung, MaxDeliver: 4, BackOff: ladder, Retry: r,
	}, func(m jetstream.Msg) {
		mu.Lock()
		at = append(at, time.Now())
		n := len(at)
		mu.Unlock()
		if n >= 3 {
			return // let the last ones settle normally
		}
		if _, err := r.Settle(t.Context(), m, "transient"); err != nil {
			t.Errorf("Settle: %v", err)
		}
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer run.Stop()

	if _, err := js.Publish(t.Context(), "repo.refs.update", []byte("ref")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(at)
		mu.Unlock()
		if n >= 3 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(at) < 3 {
		t.Fatalf("saw %d deliveries in 15s, want 3", len(at))
	}
	for i := 1; i < 3; i++ {
		gap := at[i].Sub(at[i-1])
		if gap < rung/2 {
			t.Errorf("redelivery %d came after %s, want ~%s: the ladder is being skipped, not served", i+1, gap, rung)
		}
	}
	if n := len(pub.captured()); n != 0 {
		t.Errorf("captured %d messages; a message riding the ladder must not reach the DLQ early", n)
	}
}

// TestValidateLeavesUntouchedConsumersAlone is the regression for a validator
// that grew teeth and bit consumers that had adopted nothing. A durable with
// no Retry and no ladder has nothing to be coherent ABOUT, and MaxDeliver -1
// is the module's own spelling of "unlimited" — it must not become a hard
// startup failure because this package learned to check schedules.
func TestValidateLeavesUntouchedConsumersAlone(t *testing.T) {
	onMsg := func(jetstream.Msg) {}
	base := Config{Stream: "s_v1", Durable: "d", Name: "legacy"}

	for _, maxDeliver := range []int{-1, 0, 3} {
		cfg := base
		cfg.MaxDeliver = maxDeliver
		if err := cfg.validate(nil, onMsg); err != nil && !strings.Contains(err.Error(), "nil nats conn") {
			t.Errorf("validate(MaxDeliver %d, no Retry, no BackOff) = %v, want no schedule opinion", maxDeliver, err)
		}
	}

	// Unlimited stays legal once a ladder IS declared: there is simply no
	// terminal branch, so the duration checks have nothing to bound.
	unlimited := base
	unlimited.MaxDeliver, unlimited.AckWait = UnlimitedMaxDeliver, time.Minute
	unlimited.BackOff = []time.Duration{time.Minute, 2 * time.Minute}
	if err := unlimited.validate(nil, onMsg); err != nil && !strings.Contains(err.Error(), "nil nats conn") {
		t.Errorf("validate(unlimited + ladder) = %v, want it accepted", err)
	}

	// But a value below the sentinel is still nonsense.
	bad := unlimited
	bad.MaxDeliver = -2
	if err := bad.validate(nil, onMsg); err == nil || !strings.Contains(err.Error(), "MaxDeliver") {
		t.Errorf("validate(MaxDeliver -2) = %v, want a MaxDeliver rejection", err)
	}
}

// TestMaxDeliverMismatchReportsItself: the schedule check runs after the
// cross-check, so a genuine mismatch says so instead of surfacing as generic
// incoherence.
func TestMaxDeliverMismatchReportsItself(t *testing.T) {
	r, _ := newTestRetry(t, nil) // MaxDeliver 6
	cfg := Config{
		Stream: "repo_refs_v1", Durable: "d", Name: "c",
		AckWait: 5 * time.Minute, MaxDeliver: 7, BackOff: testLadder(), Retry: r,
	}
	err := cfg.validate(nil, func(jetstream.Msg) {})
	if err == nil || !strings.Contains(err.Error(), "they must match") {
		t.Fatalf("validate(mismatch) = %v, want the specific cross-check message", err)
	}
	if strings.Contains(err.Error(), "incoherent retry schedule") {
		t.Error("a MaxDeliver mismatch surfaced as generic schedule incoherence")
	}
}

// TestStartChecksTheLadderAgainstStreamRetention: a ladder that outlives the
// stream's max_age never reaches its own dead-letter branch — the stream
// discards the message first, which is silent data loss wearing a retry
// policy. Config.validate cannot see it (retention is server state), so Start
// re-runs the schedule check with the real number.
func TestStartChecksTheLadderAgainstStreamRetention(t *testing.T) {
	nc, js := runTestEnv(t)
	if _, err := js.CreateStream(t.Context(), jetstream.StreamConfig{
		Name: "short_v1", Subjects: []string{"short.>"}, MaxAge: 10 * time.Minute,
	}); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	r, _ := newTestRetry(t, func(c *RetryConfig) { c.Monitor, c.Breaker = nil, BreakerObserve })
	cfg := Config{
		Stream: "short_v1", Durable: "d", Name: "c",
		AckWait: 5 * time.Minute, MaxDeliver: 6, BackOff: testLadder(), Retry: r,
	}
	// The schedule is coherent on its own — 20m to dead-letter, no bound set.
	if err := cfg.validate(nc, func(jetstream.Msg) {}); err != nil {
		t.Fatalf("validate: %v", err)
	}
	// Against a 10-minute stream it is not.
	_, err := Start(t.Context(), nc, cfg, func(jetstream.Msg) {})
	if err == nil || !strings.Contains(err.Error(), "StreamMaxAge") {
		t.Fatalf("Start = %v, want the ladder rejected against 10m retention", err)
	}

	// Roomy retention, same consumer, accepted.
	if _, err := js.CreateStream(t.Context(), jetstream.StreamConfig{
		Name: "long_v1", Subjects: []string{"long.>"}, MaxAge: 6 * time.Hour,
	}); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	cfg.Stream = "long_v1"
	run, err := Start(t.Context(), nc, cfg, func(jetstream.Msg) {})
	if err != nil {
		t.Fatalf("Start(roomy retention): %v", err)
	}
	run.Stop()
}

// TestStartFailsWhenTheStreamRetentionCannotBeRead is the regression for a
// safety check that failed open.
//
// Reading the stream and creating the consumer are separate JetStream API
// subjects, so a credential can be allowed to do the second and not the first.
// The retention check used to answer "fine" for every StreamInfo error, which
// meant exactly that credential started a consumer whose ladder outlives the
// stream's max_age — the message expires before the ladder reaches capture,
// which is the ENT-1492 loss the check exists to prevent. A check that cannot
// read the stream has not passed; it has not run.
//
// The ladder here is the one the test above proves invalid against 10-minute
// retention, so under the old behaviour Start SUCCEEDS and the consumer runs
// unchecked.
func TestStartFailsWhenTheStreamRetentionCannotBeRead(t *testing.T) {
	// Two credentials on one server: an admin that provisions the stream, and the
	// consumer's own, which may create and drive consumers but may not read stream
	// info. That split is expressible in ordinary NATS permissions, which is the
	// whole point — the grant looks complete for consuming.
	const adminUser, restrictedUser, pass = "admin", "capturer", "pw"
	url := runJetStreamServer(t, func(o *natsserver.Options) {
		o.Users = []*natsserver.User{
			{Username: adminUser, Password: pass},
			{Username: restrictedUser, Password: pass, Permissions: &natsserver.Permissions{
				Publish: &natsserver.SubjectPermission{Allow: []string{
					// $JS.API.INFO is needed for the client's own account/version
					// probe; without it nothing JetStream works at all and the test
					// would "pass" for the wrong reason.
					"$JS.API.INFO", "$JS.API.CONSUMER.>", "$JS.ACK.>", "short.>",
				}},
				Subscribe: &natsserver.SubjectPermission{Allow: []string{">"}},
			}},
		}
	})

	admin, err := nats.Connect(url, nats.UserInfo(adminUser, pass))
	if err != nil {
		t.Fatalf("connect admin: %v", err)
	}
	t.Cleanup(admin.Close)
	adminJS, err := jetstream.New(admin)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	if _, err := adminJS.CreateStream(t.Context(), jetstream.StreamConfig{
		Name: "short_v1", Subjects: []string{"short.>"}, MaxAge: 10 * time.Minute,
	}); err != nil {
		t.Fatalf("create stream: %v", err)
	}

	// The consumer's connection. Async permission errors are expected here, so
	// they are collected rather than left to print during the run.
	restricted, err := nats.Connect(url, nats.UserInfo(restrictedUser, pass),
		nats.ErrorHandler(func(*nats.Conn, *nats.Subscription, error) {}))
	if err != nil {
		t.Fatalf("connect restricted: %v", err)
	}
	t.Cleanup(restricted.Close)

	r, _ := newTestRetry(t, func(c *RetryConfig) { c.Monitor, c.Breaker = nil, BreakerObserve })
	cfg := Config{
		Stream: "short_v1", Durable: "d", Name: "c",
		AckWait: 5 * time.Minute, MaxDeliver: 6, BackOff: testLadder(), Retry: r,
	}
	// The schedule is coherent on its own: 20m to dead-letter, no bound declared.
	// Only the stream's 10m retention makes it wrong, and that is what this
	// credential cannot see.
	if err := cfg.validate(restricted, func(jetstream.Msg) {}); err != nil {
		t.Fatalf("validate: %v", err)
	}

	// Deliberately no ctx deadline of the test's own. A denied publish gets no
	// reply, so the probe waits out jetstream's default API timeout (~5s) — the
	// slowest test in this package, and worth it: a tighter deadline would be spent
	// by the probe, leaving CreateOrUpdateConsumer to fail on an expired context
	// instead. Start would then return an error under the OLD behaviour too, and the
	// test would pass while proving nothing.
	_, startErr := Start(t.Context(), restricted, cfg, func(jetstream.Msg) {})
	if startErr == nil {
		t.Fatal("Start succeeded without being able to read the stream's retention; " +
			"an unchecked ladder is how the message expires before capture")
	}
	if !strings.Contains(startErr.Error(), "retention") {
		t.Errorf("Start err = %v, want it to name the retention check it could not run", startErr)
	}
	// The consumer must not exist: failing the check has to happen BEFORE
	// CreateOrUpdateConsumer, or a durable is left behind running an unchecked
	// ladder even though Start reported failure.
	if _, err := adminJS.Consumer(t.Context(), "short_v1", "d"); err == nil {
		t.Error("the durable was created despite the retention check failing")
	} else if !errors.Is(err, jetstream.ErrConsumerNotFound) {
		t.Errorf("checking for the durable: %v, want ErrConsumerNotFound", err)
	}
}

// TestStartRidesOutAStreamThatDoesNotExistYet: making the retention probe report
// its errors must not break the declarative-provisioning race Run is built for.
// A stream that is not there yet surfaces as ErrStreamNotFound, which stays
// retryable, so Run keeps waiting for the provisioner instead of giving up.
func TestStartRidesOutAStreamThatDoesNotExistYet(t *testing.T) {
	nc, _ := runTestEnv(t)
	r, _ := newTestRetry(t, func(c *RetryConfig) { c.Monitor, c.Breaker = nil, BreakerObserve })
	cfg := Config{
		Stream: "not_provisioned_yet", Durable: "d", Name: "c",
		AckWait: 5 * time.Minute, MaxDeliver: 6, BackOff: testLadder(), Retry: r,
	}
	_, err := Start(t.Context(), nc, cfg, func(jetstream.Msg) {})
	if err == nil {
		t.Fatal("Start succeeded against a stream that does not exist")
	}
	if !errors.Is(err, jetstream.ErrStreamNotFound) {
		t.Errorf("Start err = %v, want it to wrap ErrStreamNotFound", err)
	}
	if !isRetryableStartError(err) {
		t.Error("a not-yet-provisioned stream is not retryable; Run would give up on the " +
			"provisioning race it exists to ride out")
	}
}

// TestFinalAckFailureIsUncertainNotStranded: a failed DoubleAck does not prove
// the ack failed. The confirmation may simply have been lost, in which case the
// message settled and is gone. Reporting that as stranded points a responder at
// break-glass removal — and removing a stream message this consumer already
// acked takes it from every OTHER consumer of that stream, so the over-claim
// has a destructive remedy attached to it.
func TestFinalAckFailureIsUncertainNotStranded(t *testing.T) {
	r, pub := newTestRetry(t, nil)
	msg := deliveredMsg(2774437, finalDelivery)
	msg.AckErr = errors.New("context deadline exceeded")

	s, err := r.Settle(t.Context(), msg, "still not ready")
	if err == nil || !strings.Contains(err.Error(), "settle:dlq_ack_unconfirmed") {
		t.Fatalf("Settle err = %v, want settle:dlq_ack_unconfirmed", err)
	}
	if s.Outcome != OutcomeUncertain {
		t.Fatalf("Outcome = %q, want %q — a lost confirmation is not a proven failure", s.Outcome, OutcomeUncertain)
	}
	if !s.Captured {
		t.Error("Captured false, want the DLQ copy reported as safe")
	}
	if len(pub.captured()) != 1 {
		t.Errorf("captured %d messages, want the copy to have landed first", len(pub.captured()))
	}

	// A capture that never happened IS provably stranded: nothing reached the
	// DLQ, so the two states stay distinguishable.
	r2, pub2 := newTestRetry(t, nil)
	pub2.err = errors.New("dlq unavailable")
	msg2 := deliveredMsg(2774437, finalDelivery)
	s2, err := r2.Settle(t.Context(), msg2, "still not ready")
	if err == nil || !strings.Contains(err.Error(), "settle:stranded") {
		t.Fatalf("Settle err = %v, want settle:stranded", err)
	}
	if s2.Outcome != OutcomeStranded || s2.Captured {
		t.Errorf("Settlement = %+v, want stranded with nothing captured", s2)
	}
}
