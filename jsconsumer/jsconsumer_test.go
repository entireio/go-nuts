package jsconsumer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/trace"

	"github.com/entireio/go-nuts/backoff"
	"github.com/entireio/go-nuts/natsmsg/natsmsgtest"
)

var testCfg = Config{Stream: "s_v1", Name: "testconsumer", SpanName: "test.consume"}

// TestProcessDispatchesDecodedEvent: a payload that decodes reaches handle with the
// decoded event and a live span; the message is left for handle to ack/nak (Process
// does not touch it), and onUndecodable is not called.
func TestProcessDispatchesDecodedEvent(t *testing.T) {
	msg := &natsmsgtest.FakeMsg{DataVal: []byte("payload")}
	var (
		gotEv   string
		gotSpan trace.Span
		gotMsg  jetstream.Msg
		dropped bool
	)
	decode := func(b []byte) (string, error) { return "decoded:" + string(b), nil }
	handle := func(_ context.Context, span trace.Span, m jetstream.Msg, ev string) {
		gotEv, gotSpan, gotMsg = ev, span, m
	}
	Process(context.Background(), msg, testCfg, decode, handle, func(context.Context) { dropped = true })

	if gotEv != "decoded:payload" {
		t.Errorf("handle got ev %q, want decoded:payload", gotEv)
	}
	if gotSpan == nil {
		t.Error("handle got a nil span, want the live consumer span")
	}
	if gotMsg != msg {
		t.Error("handle got a different msg than the one passed to Process")
	}
	if msg.Acked || msg.Termed || msg.Naks > 0 || len(msg.NakDelays) > 0 {
		t.Error("Process disposed the message, want none (handle owns ack/nak)")
	}
	if dropped {
		t.Error("onUndecodable called on a successful decode")
	}
}

// TestProcessTermsUndecodable: a decode error terms the message, bumps the caller's
// drop metric via onUndecodable, and never calls handle — a poison payload won't
// decode on redelivery.
func TestProcessTermsUndecodable(t *testing.T) {
	msg := &natsmsgtest.FakeMsg{DataVal: []byte("garbage")}
	handled := false
	dropped := false
	decode := func([]byte) (string, error) { return "", errors.New("bad payload") }
	handle := func(context.Context, trace.Span, jetstream.Msg, string) { handled = true }
	Process(context.Background(), msg, testCfg, decode, handle, func(context.Context) { dropped = true })

	if !msg.Termed {
		t.Error("message not termed on a decode error")
	}
	if !dropped {
		t.Error("onUndecodable not called on a decode error")
	}
	if handled {
		t.Error("handle called despite a decode error")
	}
}

// TestProcessNilOnUndecodable: a nil onUndecodable is tolerated (still terms).
func TestProcessNilOnUndecodable(t *testing.T) {
	msg := &natsmsgtest.FakeMsg{DataVal: []byte("garbage")}
	decode := func([]byte) (string, error) { return "", errors.New("bad") }
	Process(context.Background(), msg, testCfg, decode,
		func(context.Context, trace.Span, jetstream.Msg, string) {}, nil)
	if !msg.Termed {
		t.Error("message not termed")
	}
}

// TestProcessKeepInProgress: with Config.KeepInProgress set, a handler that
// outruns AckWait/3 gets InProgress heartbeats, and the heartbeat goroutine has
// stopped by the time Process returns (no further ticks accrue).
func TestProcessKeepInProgress(t *testing.T) {
	cfg := testCfg
	cfg.KeepInProgress = true
	cfg.AckWait = 30 * time.Millisecond // ticks at 10ms

	msg := &natsmsgtest.FakeMsg{DataVal: []byte("payload")}
	decode := func(b []byte) (string, error) { return string(b), nil }
	handle := func(context.Context, trace.Span, jetstream.Msg, string) {
		time.Sleep(35 * time.Millisecond) // spans at least three ticks
	}
	Process(context.Background(), msg, cfg, decode, handle, nil)

	after := msg.InProgressN
	if after == 0 {
		t.Fatal("no InProgress heartbeat during a long handler")
	}
	time.Sleep(25 * time.Millisecond)
	if msg.InProgressN != after {
		t.Errorf("heartbeat still ticking after Process returned: %d → %d", after, msg.InProgressN)
	}
}

// TestProcessNoKeepInProgressByDefault: without the opt-in, a long handler gets
// no heartbeat — the broker's AckWait failsafe stays untouched.
func TestProcessNoKeepInProgressByDefault(t *testing.T) {
	cfg := testCfg
	cfg.AckWait = 30 * time.Millisecond

	msg := &natsmsgtest.FakeMsg{DataVal: []byte("payload")}
	decode := func(b []byte) (string, error) { return string(b), nil }
	handle := func(context.Context, trace.Span, jetstream.Msg, string) {
		time.Sleep(35 * time.Millisecond)
	}
	Process(context.Background(), msg, cfg, decode, handle, nil)

	if msg.InProgressN != 0 {
		t.Errorf("InProgress called %d times without KeepInProgress opt-in", msg.InProgressN)
	}
}

// TestConfigEffectiveValues pins the zero-value resolution callers must use
// when the real tunables matter outside the scaffold — most importantly
// feeding EffectiveMaxDeliver (never the raw field) into a backoff.Policy,
// where a zero means unlimited redeliveries instead of "default to 8".
func TestConfigEffectiveValues(t *testing.T) {
	var zero Config
	if got := zero.EffectiveAckWait(); got != DefaultAckWait {
		t.Errorf("zero EffectiveAckWait = %v, want DefaultAckWait (%v)", got, DefaultAckWait)
	}
	if got := zero.EffectiveMaxDeliver(); got != DefaultMaxDeliver {
		t.Errorf("zero EffectiveMaxDeliver = %d, want DefaultMaxDeliver (%d)", got, DefaultMaxDeliver)
	}
	set := Config{AckWait: time.Second, MaxDeliver: 3}
	if got := set.EffectiveAckWait(); got != time.Second {
		t.Errorf("set EffectiveAckWait = %v, want 1s", got)
	}
	if got := set.EffectiveMaxDeliver(); got != 3 {
		t.Errorf("set EffectiveMaxDeliver = %d, want 3", got)
	}
}

// TestEffectiveMaxDeliverComposesWithBackoff pins the documented bridge between
// the two packages' MaxDeliver conventions: jsconsumer reads 0 as "default to
// DefaultMaxDeliver", backoff reads <=0 as unlimited (the jetstream.ConsumerConfig
// semantics). Feeding EffectiveMaxDeliver() — never the raw field — into a
// backoff.Policy is what keeps an unset consumer MaxDeliver finite in the policy
// instead of accidentally unlimited (which would make TermOnExhaustion never
// fire and orphan work-queue messages, COR-762).
func TestEffectiveMaxDeliverComposesWithBackoff(t *testing.T) {
	delivered := func(n uint64) *natsmsgtest.FakeMsg {
		return &natsmsgtest.FakeMsg{Meta: &jetstream.MsgMetadata{NumDelivered: n}}
	}
	for _, tc := range []struct {
		name          string
		cfg           Config
		wantEffective int
		finalAt       uint64 // delivery that should read as final; 0 => never final (unlimited)
	}{
		{"unset defaults to DefaultMaxDeliver", Config{}, DefaultMaxDeliver, DefaultMaxDeliver},
		{"positive passes through", Config{MaxDeliver: 3}, 3, 3},
		{"unlimited -1 stays unlimited", Config{MaxDeliver: -1}, -1, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			eff := tc.cfg.EffectiveMaxDeliver()
			if eff != tc.wantEffective {
				t.Fatalf("EffectiveMaxDeliver = %d, want %d", eff, tc.wantEffective)
			}
			policy := backoff.Policy{MaxDeliver: eff}
			if tc.finalAt == 0 {
				if backoff.IsFinalDelivery(delivered(1000), policy.MaxDeliver) {
					t.Error("unlimited policy read a delivery as final")
				}
				return
			}
			if backoff.IsFinalDelivery(delivered(tc.finalAt-1), policy.MaxDeliver) {
				t.Errorf("delivery %d/%d read as final", tc.finalAt-1, eff)
			}
			if !backoff.IsFinalDelivery(delivered(tc.finalAt), policy.MaxDeliver) {
				t.Errorf("delivery %d/%d not read as final", tc.finalAt, eff)
			}
		})
	}
}

// TestRunnerStopIdempotent: Stop is safe on a nil Runner, a zero Runner, and
// twice. A panic on any of these fails the test.
func TestRunnerStopIdempotent(_ *testing.T) {
	var nilRunner *Runner
	nilRunner.Stop() // must not panic

	r := &Runner{} // cc nil (never Started)
	r.Stop()
	r.Stop() // second call is a no-op
}

// TestStartNilConn: a nil connection is rejected up front, not on first use.
func TestStartNilConn(t *testing.T) {
	cfg := testCfg
	cfg.Durable = "test_durable"
	if _, err := Start(context.Background(), nil, cfg, func(context.Context, jetstream.Msg) {}); err == nil {
		t.Fatal("Start(nil conn) succeeded, want error")
	}
}

// TestStartRejectsEmptyDurable: an empty durable would make
// CreateOrUpdateConsumer silently create an ephemeral, server-named consumer,
// losing resume-across-restarts — reject it before touching the broker.
func TestStartRejectsEmptyDurable(t *testing.T) {
	if _, err := Start(context.Background(), nil, testCfg, func(context.Context, jetstream.Msg) {}); err == nil || !strings.Contains(err.Error(), "Durable") {
		t.Fatalf("Start(empty Durable) err = %v, want Durable-required error", err)
	}
}

func TestStartRejectsPermanentLocalConfigErrors(t *testing.T) {
	valid := Config{Stream: "events_v1", Durable: "events_durable", Name: "testconsumer"}
	tests := []struct {
		name  string
		cfg   Config
		onMsg func(context.Context, jetstream.Msg)
		want  string
	}{
		{"empty stream", func() Config { c := valid; c.Stream = ""; return c }(), func(context.Context, jetstream.Msg) {}, "Stream"},
		{"nil handler", valid, nil, "onMsg"},
		{"both filter forms", func() Config {
			c := valid
			c.FilterSubject = "events.one"
			c.FilterSubjects = []string{"events.two"}
			return c
		}(), func(context.Context, jetstream.Msg) {}, "FilterSubject"},
		{"empty multi filter", func() Config {
			c := valid
			c.FilterSubjects = []string{"events.one", ""}
			return c
		}(), func(context.Context, jetstream.Msg) {}, "FilterSubjects"},
		{"negative ack wait", func() Config { c := valid; c.AckWait = -time.Second; return c }(), func(context.Context, jetstream.Msg) {}, "AckWait"},
		{"invalid max deliver", func() Config { c := valid; c.MaxDeliver = -2; return c }(), func(context.Context, jetstream.Msg) {}, "MaxDeliver"},
		{"negative max messages", func() Config { c := valid; c.MaxMessages = -1; return c }(), func(context.Context, jetstream.Msg) {}, "MaxMessages"},
		{"invalid max ack pending", func() Config { c := valid; c.MaxAckPending = -2; return c }(), func(context.Context, jetstream.Msg) {}, "MaxAckPending"},
		{"negative inactive threshold", func() Config { c := valid; c.InactiveThreshold = -time.Second; return c }(), func(context.Context, jetstream.Msg) {}, "InactiveThreshold"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := Start(context.Background(), nil, tt.cfg, tt.onMsg); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Start() err = %v, want error containing %q", err, tt.want)
			}
		})
	}
}

func TestRetryableStartError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"missing declarative stream", fmt.Errorf("create: %w", jetstream.ErrStreamNotFound), true},
		{"no JetStream responder", fmt.Errorf("create: %w", nats.ErrNoResponders), true},
		{"request timeout", fmt.Errorf("create: %w", nats.ErrTimeout), true},
		{"disconnected", fmt.Errorf("create: %w", nats.ErrDisconnected), true},
		{"closed connection", fmt.Errorf("create: %w", nats.ErrConnectionClosed), false},
		{"server failure", fmt.Errorf("create: %w", &jetstream.APIError{Code: 500, ErrorCode: 10999, Description: "temporarily unavailable"}), true},
		{"permission violation", fmt.Errorf("create: %w", nats.ErrPermissionViolation), false},
		{"bad consumer config", fmt.Errorf("create: %w", jetstream.ErrBadRequest), false},
		{"JetStream disabled", fmt.Errorf("create: %w", jetstream.ErrJetStreamNotEnabled), false},
		{"JetStream disabled for account", fmt.Errorf("create: %w", jetstream.ErrJetStreamNotEnabledForAccount), false},
		{"generic error", errors.New("boom"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isRetryableStartError(tt.err); got != tt.want {
				t.Fatalf("isRetryableStartError(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

// TestIsShutdownConsumeErr pins the classifier over BOTH error families: the
// jetstream consume loop emits its own jetstream.ErrConnectionClosed (a
// distinct value wrapping neither core sentinel), and either family is benign
// only once ctx is done — a mid-run connection loss stays a real fault.
func TestIsShutdownConsumeErr(t *testing.T) {
	done, cancel := context.WithCancel(context.Background())
	cancel()
	live := context.Background()

	for _, tc := range []struct {
		name string
		ctx  context.Context
		err  error
		want bool
	}{
		{"jetstream closed, shutting down", done, jetstream.ErrConnectionClosed, true},
		{"core closed, shutting down", done, nats.ErrConnectionClosed, true},
		{"core draining, shutting down", done, nats.ErrConnectionDraining, true},
		{"jetstream closed, mid-run", live, jetstream.ErrConnectionClosed, false},
		{"unrelated error, shutting down", done, errors.New("boom"), false},
	} {
		if got := isShutdownConsumeErr(tc.ctx, tc.err); got != tc.want {
			t.Errorf("%s: isShutdownConsumeErr = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestStartRejectsKeepInProgressWithPrefetch pins the config conflict: the
// heartbeat extends only the in-flight delivery, so a multi-message prefetch
// would let buffered deliveries exhaust AckWait behind a long handler and
// redeliver concurrently — exactly what KeepInProgress exists to prevent.
func TestStartRejectsKeepInProgressWithPrefetch(t *testing.T) {
	cfg := testCfg
	cfg.Durable = "test_durable"
	cfg.KeepInProgress = true
	cfg.MaxMessages = 10
	if _, err := Start(context.Background(), nil, cfg, func(context.Context, jetstream.Msg) {}); err == nil || !strings.Contains(err.Error(), "MaxMessages") {
		t.Fatalf("Start(KeepInProgress, MaxMessages=10) err = %v, want MaxMessages conflict error", err)
	}
}

// startTestConsumer boots an embedded JetStream server, a stream, and a
// running consumer delivering to onMsg, returning the runner, the JetStream
// context for publishing, and the consume context's cancel.
func startTestConsumer(t *testing.T, onMsg func(context.Context, jetstream.Msg)) (*Runner, jetstream.JetStream, context.CancelFunc) {
	t.Helper()
	return startTestConsumerCfg(t, nil, onMsg)
}

// startTestConsumerCfg is startTestConsumer with a hook to shape the Config
// (drain window, logger) before Start.
func startTestConsumerCfg(t *testing.T, mutate func(*Config), onMsg func(context.Context, jetstream.Msg)) (*Runner, jetstream.JetStream, context.CancelFunc) {
	t.Helper()
	url := runJetStreamServer(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name: "events_v1", Subjects: []string{"events.>"},
	}); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	cfg := Config{
		Stream:        "events_v1",
		Durable:       "test_durable",
		FilterSubject: "events.repo",
		Name:          "testconsumer",
		SpanName:      "test.consume",
	}
	if mutate != nil {
		mutate(&cfg)
	}
	run, err := Start(ctx, nc, cfg, onMsg)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	return run, js, cancel
}

// TestStopWaitsForInFlightHandler pins Stop's join semantics: it must not
// return while a handler is mid-flight, so callers can tear down the
// resources handlers use (stores, publishers, the connection) as soon as it
// returns — the cancel → join → drain ordering.
func TestStopWaitsForInFlightHandler(t *testing.T) {
	started := make(chan struct{})
	var handlerDone atomic.Bool
	run, js, _ := startTestConsumer(t, func(_ context.Context, m jetstream.Msg) {
		close(started)
		time.Sleep(300 * time.Millisecond)
		handlerDone.Store(true)
		if err := m.Ack(); err != nil {
			t.Errorf("ack: %v", err)
		}
	})

	if _, err := js.Publish(context.Background(), "events.repo", []byte("slow")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("handler never started")
	}
	run.Stop()
	if !handlerDone.Load() {
		t.Fatal("Stop returned while the handler was still in flight")
	}
}

// TestStopConcurrentWithCancel drives the explicit-Stop-races-context-cancel
// path the race detector previously caught: several goroutines call Stop
// while ctx cancellation triggers the Start-armed stop. All calls must
// return, without panic or race.
func TestStopConcurrentWithCancel(t *testing.T) {
	run, _, cancel := startTestConsumer(t, func(context.Context, jetstream.Msg) {})

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			run.Stop()
		}()
	}
	cancel()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("concurrent Stop calls did not all return")
	}
}

// TestExplicitStopReleasesWatcher guards the one-shot Start lifecycle: a
// caller may stop a Runner while retaining the parent context, and doing so
// must not leave Start's cancellation watcher parked until that parent ends.
func TestExplicitStopReleasesWatcher(t *testing.T) {
	run, _, _ := startTestConsumer(t, func(context.Context, jetstream.Msg) {})
	run.Stop()

	select {
	case <-run.watcherDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Start cancellation watcher remained alive after explicit Stop")
	}
}

// TestStartAppliesConsumerTunables: InactiveThreshold and MaxAckPending must
// land on the on-server consumer config — interest-stream consumers depend on
// InactiveThreshold so a decommissioned durable stops pinning messages, and
// shared-durable work queues bound their in-flight window with MaxAckPending.
func TestStartAppliesConsumerTunables(t *testing.T) {
	url := runJetStreamServer(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name: "events_v1", Subjects: []string{"events.>"},
	}); err != nil {
		t.Fatalf("create stream: %v", err)
	}

	run, err := Start(ctx, nc, Config{
		Stream:            "events_v1",
		Durable:           "tunables_durable",
		FilterSubject:     "events.repo",
		Name:              "testconsumer",
		SpanName:          "test.consume",
		InactiveThreshold: 72 * time.Hour,
		MaxAckPending:     42,
	}, func(context.Context, jetstream.Msg) {})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer run.Stop()

	cons, err := js.Consumer(ctx, "events_v1", "tunables_durable")
	if err != nil {
		t.Fatalf("look up consumer: %v", err)
	}
	info, err := cons.Info(ctx)
	if err != nil {
		t.Fatalf("consumer info: %v", err)
	}
	if got := info.Config.InactiveThreshold; got != 72*time.Hour {
		t.Errorf("on-server InactiveThreshold = %v, want 72h", got)
	}
	if got := info.Config.MaxAckPending; got != 42 {
		t.Errorf("on-server MaxAckPending = %d, want 42", got)
	}
}

// compressRunRetries shrinks Run's supervision envelope so recreate paths are
// exercised in milliseconds, restoring it when the test ends.
func compressRunRetries(t *testing.T) {
	t.Helper()
	origInitial, origMax := runRetryInitial, runRetryMax
	runRetryInitial, runRetryMax = 20*time.Millisecond, 100*time.Millisecond
	t.Cleanup(func() { runRetryInitial, runRetryMax = origInitial, origMax })
}

// runTestEnv boots an embedded JetStream server and a connection, returning
// the JetStream context for stream/consumer manipulation.
func runTestEnv(t *testing.T) (*nats.Conn, jetstream.JetStream) {
	t.Helper()
	url := runJetStreamServer(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	return nc, js
}

// TestRunRejectsInvalidConfig: configuration errors can never succeed on
// retry, so Run must return them immediately instead of supervising forever.
func TestRunRejectsInvalidConfig(t *testing.T) {
	if err := Run(context.Background(), nil, testCfg, func(context.Context, jetstream.Msg) {}); err == nil || !strings.Contains(err.Error(), "Durable") {
		t.Fatalf("Run(empty Durable) err = %v, want immediate Durable-required error", err)
	}
}

// TestRunReturnsPermanentServerConfigError guards the retry classifier beyond
// local validation. Overlapping filters are rejected by the server and cannot
// become valid by retrying, so Run must return rather than leave the process
// alive with no consumer.
func TestRunReturnsPermanentServerConfigError(t *testing.T) {
	nc, js := runTestEnv(t)
	if _, err := js.CreateStream(t.Context(), jetstream.StreamConfig{
		Name: "events_v1", Subjects: []string{"events.>"},
	}); err != nil {
		t.Fatalf("create stream: %v", err)
	}

	cfg := Config{
		Stream:         "events_v1",
		Durable:        "invalid_filters",
		FilterSubjects: []string{"events.>", "events.repo"},
		Name:           "testconsumer",
	}
	done := make(chan error, 1)
	go func() { done <- Run(t.Context(), nc, cfg, func(context.Context, jetstream.Msg) {}) }()

	select {
	case err := <-done:
		if err == nil || !errors.Is(err, jetstream.ErrOverlappingFilterSubjects) {
			t.Fatalf("Run() err = %v, want ErrOverlappingFilterSubjects", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run retried a permanent server configuration error")
	}
}

// TestRunDeliversAndReturnsOnCancel: the supervised loop delivers like Start
// and exits nil once ctx is cancelled — the contract that lets it be the body
// of a ShutdownGroup.Go goroutine.
func TestRunDeliversAndReturnsOnCancel(t *testing.T) {
	nc, js := runTestEnv(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name: "events_v1", Subjects: []string{"events.>"},
	}); err != nil {
		t.Fatalf("create stream: %v", err)
	}

	got := make(chan jetstream.Msg, 1)
	done := make(chan error, 1)
	cfg := Config{Stream: "events_v1", Durable: "run_durable", FilterSubject: "events.repo",
		Name: "testconsumer", SpanName: "test.consume"}
	go func() {
		done <- Run(ctx, nc, cfg, func(_ context.Context, m jetstream.Msg) {
			if err := m.Ack(); err != nil {
				t.Errorf("ack: %v", err)
			}
			got <- m
		})
	}()

	if _, err := js.Publish(ctx, "events.repo", []byte("hello")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	select {
	case m := <-got:
		if string(m.Data()) != "hello" {
			t.Errorf("payload = %q, want hello", m.Data())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("message did not reach onMsg")
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v on cancel, want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after ctx cancel")
	}
}

// TestRunToleratesMissingStreamAtBoot: a stream that is not provisioned yet
// when the service boots (the declarative-provisioning race) is a retry, not
// a crash — once the stream appears, the consumer comes up and delivers.
func TestRunToleratesMissingStreamAtBoot(t *testing.T) {
	compressRunRetries(t)
	nc, js := runTestEnv(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	got := make(chan jetstream.Msg, 1)
	done := make(chan error, 1)
	cfg := Config{Stream: "late_v1", Durable: "late_durable", FilterSubject: "late.repo",
		Name: "testconsumer", SpanName: "test.consume"}
	go func() {
		done <- Run(ctx, nc, cfg, func(_ context.Context, m jetstream.Msg) {
			if err := m.Ack(); err != nil {
				t.Errorf("ack: %v", err)
			}
			got <- m
		})
	}()

	// Let at least one create attempt fail against the absent stream.
	time.Sleep(60 * time.Millisecond)
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name: "late_v1", Subjects: []string{"late.>"},
	}); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	if _, err := js.Publish(ctx, "late.repo", []byte("finally")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	select {
	case m := <-got:
		if string(m.Data()) != "finally" {
			t.Errorf("payload = %q, want finally", m.Data())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("message did not arrive after the stream appeared")
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v on cancel, want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after ctx cancel")
	}
}

// TestRunRecreatesAfterConsumerDeleted: deleting the durable on the server is
// a terminal error for the live consume loop (nats.go stops it and Closed
// fires); Run must notice and recreate rather than leaving the service
// running but deaf.
func TestRunRecreatesAfterConsumerDeleted(t *testing.T) {
	compressRunRetries(t)
	nc, js := runTestEnv(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name: "events_v1", Subjects: []string{"events.>"},
	}); err != nil {
		t.Fatalf("create stream: %v", err)
	}

	got := make(chan string, 2)
	done := make(chan error, 1)
	cfg := Config{Stream: "events_v1", Durable: "recreate_durable", FilterSubject: "events.repo",
		Name: "testconsumer", SpanName: "test.consume"}
	go func() {
		done <- Run(ctx, nc, cfg, func(_ context.Context, m jetstream.Msg) {
			if err := m.Ack(); err != nil {
				t.Errorf("ack: %v", err)
			}
			got <- string(m.Data())
		})
	}()

	if _, err := js.Publish(ctx, "events.repo", []byte("one")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	select {
	case m := <-got:
		if m != "one" {
			t.Fatalf("first delivery = %q, want one", m)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("first message did not arrive")
	}

	if err := js.DeleteConsumer(ctx, "events_v1", "recreate_durable"); err != nil {
		t.Fatalf("delete consumer: %v", err)
	}
	if _, err := js.Publish(ctx, "events.repo", []byte("two")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	// Deleting the durable erased its delivery cursor, so the recreated
	// consumer legitimately redelivers "one" before "two"; wait for "two".
	deadline := time.After(10 * time.Second)
	for m := ""; m != "two"; {
		select {
		case m = <-got:
		case <-deadline:
			t.Fatal("message did not arrive after the consumer was deleted and recreated")
		}
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v on cancel, want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after ctx cancel")
	}
}

// runJetStreamServer starts an in-process JetStream-enabled NATS server on a
// random loopback port and returns its client URL.
func runJetStreamServer(t *testing.T, opts ...func(*natsserver.Options)) string {
	t.Helper()
	o := &natsserver.Options{
		Host:      "127.0.0.1",
		Port:      -1, // pick a free port
		NoLog:     true,
		NoSigs:    true,
		JetStream: true,
		StoreDir:  t.TempDir(),
	}
	// opts may add accounts, users or permissions — the retention-probe test needs
	// a credential that can drive consumers but not read stream info.
	for _, fn := range opts {
		fn(o)
	}
	s, err := natsserver.NewServer(o)
	if err != nil {
		t.Fatalf("new embedded nats server: %v", err)
	}
	go s.Start()
	// Shutdown registered BEFORE the readiness wait, so a server that never
	// becomes ready is still torn down — see startServer in
	// internal/brokersemantics for why the order matters.
	t.Cleanup(s.Shutdown)
	if !s.ReadyForConnections(10 * time.Second) {
		t.Fatal("embedded nats server not ready in time")
	}
	return s.ClientURL()
}

// TestStartDeliversToOnMsg drives the whole scaffold against an embedded
// JetStream server: Start creates the durable, a published message reaches
// onMsg, and cancelling ctx stops the runner.
func TestStartDeliversToOnMsg(t *testing.T) {
	url := runJetStreamServer(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name: "events_v1", Subjects: []string{"events.>"},
	}); err != nil {
		t.Fatalf("create stream: %v", err)
	}

	got := make(chan jetstream.Msg, 1)
	run, err := Start(ctx, nc, Config{
		Stream:        "events_v1",
		Durable:       "test_durable",
		FilterSubject: "events.repo",
		Name:          "testconsumer",
		SpanName:      "test.consume",
	}, func(_ context.Context, m jetstream.Msg) {
		if err := m.Ack(); err != nil {
			t.Errorf("ack: %v", err)
		}
		got <- m
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer run.Stop()

	if _, err := js.Publish(ctx, "events.repo", []byte("hello")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	select {
	case m := <-got:
		if string(m.Data()) != "hello" {
			t.Errorf("payload = %q, want hello", m.Data())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("message did not reach onMsg")
	}
}

// publishAndAwaitStart publishes one message and waits for the handler to
// signal it has begun, so a shutdown can be driven against a live handler.
func publishAndAwaitStart(t *testing.T, js jetstream.JetStream, started <-chan struct{}) {
	t.Helper()
	if _, err := js.Publish(context.Background(), "events.repo", []byte("slow")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("handler never started")
	}
}

// TestHandlerContextSurvivesParentCancel pins the drain contract (COR-2224):
// cancelling the context Start was given stops the pull loop but does NOT
// cancel the handler's context. An in-flight handler finishes on its own
// terms inside the drain window, instead of failing with context.Canceled in
// the same instant SIGTERM lands.
func TestHandlerContextSurvivesParentCancel(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan error, 1)
	run, js, cancel := startTestConsumerCfg(t, func(c *Config) { c.DrainWindow = 5 * time.Second },
		func(ctx context.Context, m jetstream.Msg) {
			close(started)
			<-release
			err := ctx.Err()
			if ackErr := m.Ack(); ackErr != nil {
				t.Errorf("ack: %v", ackErr)
			}
			finished <- err
		})
	publishAndAwaitStart(t, js, started)

	cancel()
	// The parent is gone and the loop has been told to stop; the handler must
	// still hold a live context.
	time.Sleep(200 * time.Millisecond)
	close(release)
	select {
	case err := <-finished:
		if err != nil {
			t.Fatalf("handler ctx.Err() = %v after parent cancel, want nil inside the drain window", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not finish")
	}
	run.Stop()
}

// TestDrainWindowCancelsHandlerContext: a handler that does not finish inside
// Config.DrainWindow has its context cancelled when the window elapses, so
// shutdown stays bounded, and the scaffold says so in the log.
func TestDrainWindowCancelsHandlerContext(t *testing.T) {
	logs := &logCapture{}
	started := make(chan struct{})
	handlerCtxDone := make(chan struct{})
	run, js, cancel := startTestConsumerCfg(t, func(c *Config) {
		c.DrainWindow = 150 * time.Millisecond
		c.Logger = slog.New(logs)
	}, func(ctx context.Context, _ jetstream.Msg) {
		close(started)
		<-ctx.Done()
		close(handlerCtxDone)
	})
	publishAndAwaitStart(t, js, started)

	cancelledAt := time.Now()
	cancel()
	select {
	case <-handlerCtxDone:
	case <-time.After(5 * time.Second):
		t.Fatal("handler context never cancelled after the drain window")
	}
	if elapsed := time.Since(cancelledAt); elapsed < 150*time.Millisecond {
		t.Errorf("handler context cancelled after %v, want the full 150ms drain window", elapsed)
	}
	if !backoff.IsInterrupted(run.handlerCtx) {
		t.Error("window expiry did not read as an interruption to the dispositions")
	}
	run.Stop()
	if logs.count("testconsumer: drain window elapsed; cancelling in-flight handler") == 0 {
		t.Errorf("drain-window expiry not logged; got %v", logs.snapshot())
	}
}

// TestNoDrainWindowLogWhenHandlerFinishes: a clean drain (handler returns
// inside the window) must not log the expiry warning — it is the signal
// operators size the window by, so it has to mean what it says.
func TestNoDrainWindowLogWhenHandlerFinishes(t *testing.T) {
	logs := &logCapture{}
	started := make(chan struct{})
	run, js, cancel := startTestConsumerCfg(t, func(c *Config) {
		c.DrainWindow = 2 * time.Second
		c.Logger = slog.New(logs)
	}, func(_ context.Context, m jetstream.Msg) {
		close(started)
		time.Sleep(50 * time.Millisecond)
		if err := m.Ack(); err != nil {
			t.Errorf("ack: %v", err)
		}
	})
	publishAndAwaitStart(t, js, started)
	cancel()
	run.Stop()
	if logs.count("testconsumer: drain window elapsed; cancelling in-flight handler") != 0 {
		t.Error("drain-window expiry logged although the handler finished inside the window")
	}
}

type ctxKey struct{}

// TestHandlerContextCarriesParentValues: the handler context is detached from
// the parent's CANCELLATION only. Its values — trace baggage, loggers, request
// scope — still flow through.
func TestHandlerContextCarriesParentValues(t *testing.T) {
	url := runJetStreamServer(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), ctxKey{}, "carried"))
	defer cancel()
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "events_v1", Subjects: []string{"events.>"}}); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	got := make(chan any, 1)
	run, err := Start(ctx, nc, Config{Stream: "events_v1", Durable: "test_durable", FilterSubject: "events.repo",
		Name: "testconsumer", SpanName: "test.consume"}, func(hctx context.Context, m jetstream.Msg) {
		if err := m.Ack(); err != nil {
			t.Errorf("ack: %v", err)
		}
		got <- hctx.Value(ctxKey{})
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer run.Stop()
	if _, err := js.Publish(ctx, "events.repo", []byte("x")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	select {
	case v := <-got:
		if v != "carried" {
			t.Errorf("handler ctx value = %v, want carried", v)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("message did not reach onMsg")
	}
}

// TestHandlerContextCancelledOnceLoopCloses: after Stop returns the handler
// context is cancelled even when the window never elapsed, so anything a
// handler forked on it (a heartbeat, a background flush) is released before
// the caller drains the connection — the drain → cancel → nuts.Drain ordering.
func TestHandlerContextCancelledOnceLoopCloses(t *testing.T) {
	started := make(chan struct{})
	var captured atomic.Pointer[context.Context]
	run, js, cancel := startTestConsumerCfg(t, func(c *Config) { c.DrainWindow = 5 * time.Second },
		func(ctx context.Context, m jetstream.Msg) {
			captured.Store(&ctx)
			if err := m.Ack(); err != nil {
				t.Errorf("ack: %v", err)
			}
			close(started)
		})
	publishAndAwaitStart(t, js, started)
	cancel()
	run.Stop()
	if hctx := captured.Load(); hctx == nil || (*hctx).Err() == nil {
		t.Error("handler context still live after Stop returned; want it cancelled so forked goroutines release")
	}
}

// TestStopConcurrentWithCancelDrainsLiveHandler: the concurrent-Stop race
// with a handler actually in flight — every Stop caller, and the watcher's,
// returns only once the handler has, and the handler is cancelled exactly once
// the window elapses rather than by whichever caller got there first.
func TestStopConcurrentWithCancelDrainsLiveHandler(t *testing.T) {
	started := make(chan struct{})
	var handlerReturned atomic.Bool
	run, js, cancel := startTestConsumerCfg(t, func(c *Config) { c.DrainWindow = 150 * time.Millisecond },
		func(ctx context.Context, _ jetstream.Msg) {
			close(started)
			<-ctx.Done()
			handlerReturned.Store(true)
		})
	publishAndAwaitStart(t, js, started)

	cancelledAt := time.Now()
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			run.Stop()
			if !handlerReturned.Load() {
				t.Error("Stop returned while the handler was still in flight")
			}
		}()
	}
	cancel()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("concurrent Stop calls did not all return")
	}
	if elapsed := time.Since(cancelledAt); elapsed < 150*time.Millisecond {
		t.Errorf("handler cancelled after %v, want the full window regardless of how many callers stopped", elapsed)
	}
}

// TestNaturalCloseReleasesHandlerContext: a one-shot Start whose loop closes
// on its own (the durable deleted on the server) must release the detached
// handler context without anyone calling Stop — the parent's cancellation can
// no longer reach it, so this is the only thing that would.
func TestNaturalCloseReleasesHandlerContext(t *testing.T) {
	url := runJetStreamServer(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx := t.Context()
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "events_v1", Subjects: []string{"events.>"}}); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	run, err := Start(ctx, nc, Config{Stream: "events_v1", Durable: "doomed_durable", FilterSubject: "events.repo",
		Name: "testconsumer", SpanName: "test.consume"}, func(context.Context, jetstream.Msg) {})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := js.DeleteConsumer(ctx, "events_v1", "doomed_durable"); err != nil {
		t.Fatalf("delete consumer: %v", err)
	}
	select {
	case <-run.closed:
	case <-time.After(10 * time.Second):
		t.Fatal("consume loop did not close after the durable was deleted")
	}
	select {
	case <-run.handlerCtx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("handler context still live after the loop closed on its own; it leaks until an explicit Stop")
	}
}

// TestExplicitStopArmsDrainWindow: an explicit Stop with a still-live parent
// context is the same drain — stop pulling, wait up to the window, then cancel
// — so a caller that stops a Runner by hand is bounded by the same budget.
func TestExplicitStopArmsDrainWindow(t *testing.T) {
	started := make(chan struct{})
	handlerCtxDone := make(chan struct{})
	run, js, _ := startTestConsumerCfg(t, func(c *Config) { c.DrainWindow = 150 * time.Millisecond },
		func(ctx context.Context, _ jetstream.Msg) {
			close(started)
			<-ctx.Done()
			close(handlerCtxDone)
		})
	publishAndAwaitStart(t, js, started)

	stopped := make(chan struct{})
	go func() { run.Stop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not return after the drain window")
	}
	select {
	case <-handlerCtxDone:
	default:
		t.Error("Stop returned before the handler observed its cancellation")
	}
}

// TestRunJoinsInFlightHandlerWithinDrainWindow: Run's ctx-driven exit is the
// drain too — it returns only after the in-flight handler finished or was
// cancelled by the window, never while one is still running.
func TestRunJoinsInFlightHandlerWithinDrainWindow(t *testing.T) {
	nc, js := runTestEnv(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "events_v1", Subjects: []string{"events.>"}}); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	started := make(chan struct{})
	var handlerReturned atomic.Bool
	done := make(chan error, 1)
	cfg := Config{Stream: "events_v1", Durable: "run_durable", FilterSubject: "events.repo",
		Name: "testconsumer", SpanName: "test.consume", DrainWindow: 150 * time.Millisecond}
	go func() {
		done <- Run(ctx, nc, cfg, func(hctx context.Context, _ jetstream.Msg) {
			close(started)
			<-hctx.Done()
			handlerReturned.Store(true)
		})
	}()
	publishAndAwaitStart(t, js, started)

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v on cancel, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after the drain window")
	}
	if !handlerReturned.Load() {
		t.Error("Run returned while the handler was still in flight")
	}
}

// TestConfigEffectiveDrainWindow: zero means DefaultDrainWindow; a set value
// is used as-is; a negative one is a configuration error.
func TestConfigEffectiveDrainWindow(t *testing.T) {
	if got := (Config{}).EffectiveDrainWindow(); got != DefaultDrainWindow {
		t.Errorf("EffectiveDrainWindow() zero = %v, want %v", got, DefaultDrainWindow)
	}
	if got := (Config{DrainWindow: 3 * time.Second}).EffectiveDrainWindow(); got != 3*time.Second {
		t.Errorf("EffectiveDrainWindow() = %v, want 3s", got)
	}
	cfg := Config{Stream: "events_v1", Durable: "d", Name: "testconsumer", DrainWindow: -time.Second}
	if _, err := Start(context.Background(), nil, cfg, func(context.Context, jetstream.Msg) {}); err == nil || !strings.Contains(err.Error(), "DrainWindow") {
		t.Fatalf("Start(negative DrainWindow) err = %v, want DrainWindow error", err)
	}
}
