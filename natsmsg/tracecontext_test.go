package natsmsg

import (
	"context"
	"math"
	"os"
	"testing"

	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

func TestMain(m *testing.M) {
	// Inject/Extract round-trip needs a configured propagator; services set
	// this in their otel setup.
	otel.SetTextMapPropagator(propagation.TraceContext{})
	os.Exit(m.Run())
}

// remoteSpanCtx builds a sampled remote span context to stand in for the
// producer-side trace a publisher would inject.
func remoteSpanCtx(t *testing.T) (context.Context, trace.TraceID, trace.SpanID) {
	t.Helper()
	tid, err := trace.TraceIDFromHex("0123456789abcdef0123456789abcdef")
	if err != nil {
		t.Fatalf("trace id: %v", err)
	}
	sid, err := trace.SpanIDFromHex("0123456789abcdef")
	if err != nil {
		t.Fatalf("span id: %v", err)
	}
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    tid,
		SpanID:     sid,
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	})
	return trace.ContextWithSpanContext(context.Background(), sc), tid, sid
}

func TestInjectExtractRoundTrip(t *testing.T) {
	ctx, tid, sid := remoteSpanCtx(t)

	msg := &nats.Msg{Subject: "x"}
	Inject(ctx, msg)
	if len(msg.Header) == 0 {
		t.Fatal("Inject wrote no headers")
	}

	got := trace.SpanContextFromContext(Extract(context.Background(), msg))
	if got.TraceID() != tid {
		t.Errorf("trace id: got %s want %s", got.TraceID(), tid)
	}
	if got.SpanID() != sid {
		t.Errorf("span id: got %s want %s", got.SpanID(), sid)
	}
}

func TestExtractHeaderNilIsNoop(t *testing.T) {
	ctx := context.Background()
	if ExtractHeader(ctx, nil) != ctx {
		t.Error("ExtractHeader(nil) should return ctx unchanged")
	}
}

// fakeHeaderMsg is the minimal Message for StartConsumerSpan: headers carry
// the producer's injected trace context.
type fakeHeaderMsg struct {
	subject string
	header  nats.Header
}

func (m fakeHeaderMsg) Headers() nats.Header { return m.header }
func (m fakeHeaderMsg) Subject() string      { return m.subject }

// TestStartConsumerSpanReparents pins the re-parenting across the NATS hop:
// the context StartConsumerSpan returns must carry the trace id the producer
// injected into the message headers, so the consumer span joins the
// producer's trace instead of starting a fresh one. Uses the global (noop)
// tracer — extraction happens before the span starts, so the propagated span
// context is observable on the returned ctx without an SDK.
func TestStartConsumerSpanReparents(t *testing.T) {
	prodCtx, tid, _ := remoteSpanCtx(t)
	pub := &nats.Msg{Subject: "events.repo"}
	Inject(prodCtx, pub)

	ctx, span := StartConsumerSpan(context.Background(), otel.Tracer("test"),
		fakeHeaderMsg{subject: pub.Subject, header: pub.Header}, "test.consume")
	defer span.End()

	if got := trace.SpanContextFromContext(ctx).TraceID(); got != tid {
		t.Errorf("consumer ctx trace id: got %s want %s", got, tid)
	}
}

func TestClampToInt64(t *testing.T) {
	if got := ClampToInt64(7); got != 7 {
		t.Errorf("ClampToInt64(7) = %d", got)
	}
	if got := ClampToInt64(math.MaxUint64); got != math.MaxInt64 {
		t.Errorf("ClampToInt64(MaxUint64) = %d, want MaxInt64", got)
	}
}
