package jsconsumer

import (
	"context"
	"errors"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/trace"

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
	if msg.Acked || msg.Termed || len(msg.NakDelays) > 0 {
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
	if _, err := Start(context.Background(), nil, testCfg, func(jetstream.Msg) {}); err == nil {
		t.Fatal("Start(nil conn) succeeded, want error")
	}
}

// runJetStreamServer starts an in-process JetStream-enabled NATS server on a
// random loopback port and returns its client URL.
func runJetStreamServer(t *testing.T) string {
	t.Helper()
	s, err := natsserver.NewServer(&natsserver.Options{
		Host:      "127.0.0.1",
		Port:      -1, // pick a free port
		NoLog:     true,
		NoSigs:    true,
		JetStream: true,
		StoreDir:  t.TempDir(),
	})
	if err != nil {
		t.Fatalf("new embedded nats server: %v", err)
	}
	go s.Start()
	if !s.ReadyForConnections(10 * time.Second) {
		t.Fatal("embedded nats server not ready in time")
	}
	t.Cleanup(s.Shutdown)
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
	}, func(m jetstream.Msg) {
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
