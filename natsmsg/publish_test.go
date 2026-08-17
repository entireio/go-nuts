package natsmsg

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/entireio/go-nuts/internal/natstest"
)

// testStream is the embedded-server stream the publish tests bind to pub.>.
const testStream = "pub_v1"

// newStreamConn boots an embedded JetStream server with a stream bound to pub.>
// and returns a connection to it.
func newStreamConn(t *testing.T) *nats.Conn {
	t.Helper()
	s := natstest.Run(t, natsserver.Options{
		Host:      "127.0.0.1",
		Port:      -1, // pick a free port
		NoLog:     true,
		NoSigs:    true,
		JetStream: true,
		StoreDir:  t.TempDir(),
	})

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	if _, err := js.CreateStream(context.Background(), jetstream.StreamConfig{
		Name:       testStream,
		Subjects:   []string{"pub.>"},
		Duplicates: time.Minute,
	}); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	return nc
}

// runJetStreamEnv boots an embedded JetStream server with a stream bound to
// pub.> and returns a modern JetStream context for it.
func runJetStreamEnv(t *testing.T) jetstream.JetStream {
	t.Helper()
	js, err := jetstream.New(newStreamConn(t))
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	return js
}

// withTracing installs a recording tracer provider + W3C propagator and returns
// the recorder, restoring the previous globals on cleanup. It lets a test
// assert the producer span's name and attributes (the global noop tracer records
// nothing).
func withTracing(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	prevTP := otel.GetTracerProvider()
	prevProp := otel.GetTextMapPropagator()

	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	t.Cleanup(func() {
		_ = tp.Shutdown(context.Background()) //nolint:errcheck // best-effort tracer-provider shutdown in test cleanup
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
	})
	return rec
}

// fakeModernJS is a jetstream.JetStream that records the published message and
// returns a scripted ack, without a broker — for asserting the modern publish
// prologue. Only PublishMsg is implemented; the embedded nil interface supplies
// the rest of the (unused) method set.
type fakeModernJS struct {
	jetstream.JetStream

	last  *nats.Msg
	ack   *jetstream.PubAck
	err   error
	calls int
}

func (f *fakeModernJS) PublishMsg(_ context.Context, m *nats.Msg, _ ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	f.calls++
	f.last = m
	if f.err != nil {
		return nil, f.err
	}
	return f.ack, nil
}

var (
	_ jetstream.JetStream = (*fakeModernJS)(nil)
	_ JetStream           = (*fakeModernJS)(nil)
)

// TestPublisherStampsHeadersAndAcks drives the whole publish prologue: the
// stored message must carry the Nats-Msg-Id dedup key and the producer's
// injected trace context, and the ack must report the landing stream.
func TestPublisherStampsHeadersAndAcks(t *testing.T) {
	js := runJetStreamEnv(t)
	ctx, _, _ := remoteSpanCtx(t)

	p := Publisher{JS: js}
	ack, err := p.Publish(ctx, &nats.Msg{Subject: "pub.repo", Data: []byte("payload")}, "msg-1")
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if ack.Stream != testStream {
		t.Errorf("ack stream = %q, want pub_v1", ack.Stream)
	}
	if ack.Duplicate {
		t.Error("first publish reported as duplicate")
	}

	stream, err := js.Stream(context.Background(), testStream)
	if err != nil {
		t.Fatalf("bind stream: %v", err)
	}
	stored, err := stream.GetMsg(context.Background(), ack.Sequence)
	if err != nil {
		t.Fatalf("get stored msg: %v", err)
	}
	if got := stored.Header.Get(nats.MsgIdHdr); got != "msg-1" {
		t.Errorf("stored Nats-Msg-Id = %q, want msg-1", got)
	}
	if got := stored.Header.Get("traceparent"); got == "" {
		t.Error("stored message has no traceparent header; trace context was not injected")
	}
}

// TestPublisherDedupesByMsgID: a second publish with the same msg-id inside
// the duplicate window must be suppressed broker-side and reported as such —
// the normal outcome for an at-least-once producer retrying, not an error.
func TestPublisherDedupesByMsgID(t *testing.T) {
	js := runJetStreamEnv(t)
	p := Publisher{JS: js}

	first, err := p.Publish(context.Background(), &nats.Msg{Subject: "pub.repo", Data: []byte("x")}, "dup-1")
	if err != nil {
		t.Fatalf("first publish: %v", err)
	}
	second, err := p.Publish(context.Background(), &nats.Msg{Subject: "pub.repo", Data: []byte("x")}, "dup-1")
	if err != nil {
		t.Fatalf("second publish: %v", err)
	}
	if !second.Duplicate {
		t.Error("second publish with the same msg-id not reported as duplicate")
	}
	if second.Sequence != first.Sequence {
		t.Errorf("duplicate ack sequence = %d, want the original %d", second.Sequence, first.Sequence)
	}
}

// TestPublisherEmptyMsgIDLeavesHeader: an empty msgID must not clobber a
// Nats-Msg-Id the caller already set on the message.
func TestPublisherEmptyMsgIDLeavesHeader(t *testing.T) {
	js := runJetStreamEnv(t)
	p := Publisher{JS: js}

	msg := &nats.Msg{Subject: "pub.repo", Data: []byte("x"), Header: nats.Header{}}
	msg.Header.Set(nats.MsgIdHdr, "preset-1")
	ack, err := p.Publish(context.Background(), msg, "")
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	stream, err := js.Stream(context.Background(), testStream)
	if err != nil {
		t.Fatalf("bind stream: %v", err)
	}
	stored, err := stream.GetMsg(context.Background(), ack.Sequence)
	if err != nil {
		t.Fatalf("get stored msg: %v", err)
	}
	if got := stored.Header.Get(nats.MsgIdHdr); got != "preset-1" {
		t.Errorf("stored Nats-Msg-Id = %q, want the caller's preset-1", got)
	}
}

// TestPublisherErrorsOnUnboundSubject: a subject no stream listens on must
// surface as an error (the broker sends no ack), not hang past the bound.
func TestPublisherErrorsOnUnboundSubject(t *testing.T) {
	js := runJetStreamEnv(t)
	p := Publisher{JS: js, Timeout: 500 * time.Millisecond}

	start := time.Now()
	if _, err := p.Publish(context.Background(), &nats.Msg{Subject: "unbound.subject", Data: []byte("x")}, "m"); err == nil {
		t.Fatal("publish to an unbound subject succeeded, want error")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("publish error took %v, want it bounded well under the default", elapsed)
	}
}

// domainAttrs are the caller-owned domain span attributes the publisher must
// forward onto the producer span it owns (placement / job-identity style tags,
// the kind mirror-pipeline's natspub passes through).
var domainAttrs = []attribute.KeyValue{
	attribute.String("entire.target_ulid", "ulid-1"),
	attribute.Int64("entire.github_repo_id", 42),
}

// TestPublisherSpanContract drives one publish against a fake backend and
// asserts the full behavioral contract of the publish prologue: the stamped
// headers, the producer span (caller-selected span name + standard messaging
// attrs + forwarded domain attrs + PubAck telemetry), and the reported ack.
func TestPublisherSpanContract(t *testing.T) {
	rec := withTracing(t)
	prodCtx, tid, _ := remoteSpanCtx(t)
	msg := &nats.Msg{Subject: "pub.repo", Data: []byte("payload")}

	f := &fakeModernJS{ack: &jetstream.PubAck{Stream: testStream, Sequence: 7, Duplicate: true}}
	a, err := Publisher{JS: f, Operation: "repo.ops.publish"}.Publish(prodCtx, msg, "msg-1", domainAttrs...)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	if a.Stream != testStream || a.Sequence != 7 || !a.Duplicate {
		t.Errorf("ack = %q/%d/%v, want pub_v1/7/true", a.Stream, a.Sequence, a.Duplicate)
	}
	if got := f.last.Header.Get(nats.MsgIdHdr); got != "msg-1" {
		t.Errorf("published Nats-Msg-Id = %q, want msg-1", got)
	}
	if f.last.Header.Get("traceparent") == "" {
		t.Error("no traceparent injected into the published message")
	}
	assertProducerSpan(t, rec, "repo.ops.publish", tid)
}

func assertProducerSpan(t *testing.T, rec *tracetest.SpanRecorder, wantName string, wantTrace trace.TraceID) {
	t.Helper()
	spans := rec.Ended()
	if len(spans) != 1 {
		t.Fatalf("ended spans = %d, want 1", len(spans))
	}
	span := spans[0]
	if span.Name() != wantName {
		t.Errorf("span name = %q, want %q", span.Name(), wantName)
	}
	if span.SpanKind() != trace.SpanKindProducer {
		t.Errorf("span kind = %v, want producer", span.SpanKind())
	}
	if span.SpanContext().TraceID() != wantTrace {
		t.Errorf("span trace id = %s, want the producer's %s (span did not re-parent)", span.SpanContext().TraceID(), wantTrace)
	}
	seen := map[attribute.Key]bool{}
	got := map[attribute.Key]attribute.Value{}
	for _, kv := range span.Attributes() {
		got[kv.Key] = kv.Value
		seen[kv.Key] = true
	}
	for key, want := range map[attribute.Key]string{
		"messaging.system":           "nats",
		"messaging.operation.type":   "publish",
		"messaging.destination.name": "pub.repo",
		"messaging.message.id":       "msg-1",
		"messaging.nats.stream":      testStream,
		"entire.target_ulid":         "ulid-1", // caller-forwarded domain attr
	} {
		if got[key].AsString() != want {
			t.Errorf("span attr %s = %q, want %q", key, got[key].AsString(), want)
		}
	}
	if got["entire.github_repo_id"].AsInt64() != 42 {
		t.Errorf("caller-forwarded entire.github_repo_id = %d, want 42", got["entire.github_repo_id"].AsInt64())
	}
	// messaging.operation.name is reserved for the system-specific operation
	// (send/ack/nack), not the application span name — it must not carry wantName.
	if seen["messaging.operation.name"] {
		t.Errorf("messaging.operation.name = %q set; the caller span name belongs on the span name only", got["messaging.operation.name"].AsString())
	}
	if got["messaging.nats.sequence"].AsInt64() != 7 {
		t.Errorf("sequence attr = %d, want 7", got["messaging.nats.sequence"].AsInt64())
	}
	if !got["messaging.nats.duplicate"].AsBool() {
		t.Error("duplicate attr = false, want true")
	}
}

// TestPublisherDefaultOperationName: with no Operation the span falls back to
// "publish <subject>" and sets no messaging.operation.name.
func TestPublisherDefaultOperationName(t *testing.T) {
	rec := withTracing(t)
	f := &fakeModernJS{ack: &jetstream.PubAck{Stream: testStream, Sequence: 1}}
	if _, err := (Publisher{JS: f}).Publish(context.Background(), &nats.Msg{Subject: "pub.repo", Data: []byte("x")}, "m"); err != nil {
		t.Fatalf("publish: %v", err)
	}
	spans := rec.Ended()
	if len(spans) != 1 || spans[0].Name() != "publish pub.repo" {
		t.Fatalf("default span name = %q, want 'publish pub.repo'", spans[0].Name())
	}
	for _, kv := range spans[0].Attributes() {
		if kv.Key == "messaging.operation.name" {
			t.Error("messaging.operation.name set despite an empty Operation")
		}
	}
}

// TestPublisherRecordsError: a failed publish is wrapped preserving the
// underlying cause (so callers can classify it) and marks the span an error.
func TestPublisherRecordsError(t *testing.T) {
	rec := withTracing(t)
	_, err := (Publisher{JS: &fakeModernJS{err: errors.New("nats down")}}).Publish(context.Background(), &nats.Msg{Subject: "pub.repo"}, "m")
	if err == nil || !strings.Contains(err.Error(), "natsmsg: publish pub.repo") || !strings.Contains(err.Error(), "nats down") {
		t.Errorf("err = %v, want wrapped publish error preserving the cause", err)
	}
	spans := rec.Ended()
	if len(spans) != 1 || spans[0].Status().Code != codes.Error {
		t.Error("publish failure not recorded as a span error")
	}
}
