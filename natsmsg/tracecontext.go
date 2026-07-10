// Package natsmsg holds small JetStream message helpers shared by Entire's
// NATS consumers: W3C trace-context propagation over message headers (so a
// consumer span parents to the remote producer span and publish → consume
// stitches into one trace) and a KeepInProgress heartbeat for long-running
// handlers.
//
// It unifies two service-local copies — entire-core's internal/natsmsg and
// entire-api's internal/otelnats (itself ported from mirror-pipeline) — into
// the one shared implementation. Tracking: COR-929.
package natsmsg

import (
	"context"
	"math"

	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// HeaderCarrier adapts nats.Header to the OTel TextMapCarrier contract so the
// configured propagator can inject/extract traceparent and baggage over a
// NATS hop.
type HeaderCarrier nats.Header

func (c HeaderCarrier) Get(key string) string {
	v := nats.Header(c).Values(key)
	if len(v) == 0 {
		return ""
	}
	return v[0]
}

func (c HeaderCarrier) Set(key, value string) { nats.Header(c).Set(key, value) }

// Values returns every value for key, satisfying [propagation.ValuesGetter]:
// without it the baggage propagator falls back to Get and reads only the
// first of multiple same-named headers, truncating multi-header W3C baggage.
func (c HeaderCarrier) Values(key string) []string { return nats.Header(c).Values(key) }

func (c HeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}

var (
	_ propagation.TextMapCarrier = HeaderCarrier(nil)
	_ propagation.ValuesGetter   = HeaderCarrier(nil)
)

// Inject writes the propagator state from ctx into msg.Header, creating the
// header map if absent. Call before publishing so the consumer can re-parent.
func Inject(ctx context.Context, msg *nats.Msg) {
	if msg.Header == nil {
		msg.Header = nats.Header{}
	}
	otel.GetTextMapPropagator().Inject(ctx, HeaderCarrier(msg.Header))
}

// Extract returns ctx with propagator state from msg.Header applied. Call
// before starting the consumer span so it parents to the remote producer.
func Extract(ctx context.Context, msg *nats.Msg) context.Context {
	return ExtractHeader(ctx, msg.Header)
}

// ExtractHeader is Extract over a bare nats.Header — it takes the header map
// rather than the message so it serves both *nats.Msg (msg.Header) and
// jetstream.Msg (msg.Headers()).
func ExtractHeader(ctx context.Context, h nats.Header) context.Context {
	if h == nil {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, HeaderCarrier(h))
}

// Message is the subset of jetstream.Msg the consumer-span helper needs.
type Message interface {
	Headers() nats.Header
	Subject() string
}

// StartConsumerSpan extracts the propagated trace context from msg's headers
// and opens a consumer span named name on tr, pre-tagged with the standard
// messaging attributes (system=nats, destination=subject). Extra attrs are
// appended. Collapses the extract+start boilerplate shared by every JetStream
// consumer.
func StartConsumerSpan(ctx context.Context, tr trace.Tracer, msg Message, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	ctx = ExtractHeader(ctx, msg.Headers())
	base := append([]attribute.KeyValue{
		attribute.String("messaging.system", "nats"),
		attribute.String("messaging.destination.name", msg.Subject()),
	}, attrs...)
	return tr.Start(ctx, name, trace.WithSpanKind(trace.SpanKindConsumer), trace.WithAttributes(base...)) //nolint:spancheck // the caller owns the span and Ends it
}

// ClampToInt64 narrows a JetStream sequence / delivery count (uint64 on the
// wire) to the int64 OTel attributes take, saturating instead of wrapping.
func ClampToInt64(v uint64) int64 {
	if v > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(v)
}
