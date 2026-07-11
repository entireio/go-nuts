package natsmsg

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// DefaultPublishTimeout bounds the wait for the broker's publish
// acknowledgment when Publisher.Timeout is zero. The bound matters because
// Entire connections reconnect forever and nats.go buffers publishes while
// disconnected: without it, a publisher holding an HTTP request or a message
// disposition would wait indefinitely behind an unreachable broker instead
// of failing over to its retry path (the caller's outbox, nak, or 5xx).
const DefaultPublishTimeout = 5 * time.Second

// StartProducerSpan opens a producer span named name on tr, pre-tagged with
// the standard messaging attributes (system=nats, destination=subject) — the
// publish-side counterpart of [StartConsumerSpan]. Extra attrs are appended.
// The caller owns the span and must End it, and should Inject the returned
// ctx into the outgoing message so the consumer can re-parent.
func StartProducerSpan(ctx context.Context, tr trace.Tracer, subject, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	base := append([]attribute.KeyValue{
		attribute.String("messaging.system", "nats"),
		attribute.String("messaging.destination.name", subject),
	}, attrs...)
	return tr.Start(ctx, name, trace.WithSpanKind(trace.SpanKindProducer), trace.WithAttributes(base...)) //nolint:spancheck // the caller owns the span and Ends it
}

// Publisher is the JetStream publish core shared by Entire's producers: a
// producer span, W3C trace-context injection, Nats-Msg-Id dedup, and a
// bounded wait for the broker's ack. Every service had grown its own copy of
// exactly this prologue around PublishMsg; Publisher owns the prologue and
// nothing else — subject construction, payload encoding, and the response to
// a failed publish (outbox retry, nak, HTTP 5xx) stay with the caller.
type Publisher struct {
	// JS is the JetStream context to publish through.
	JS jetstream.JetStream
	// Tracer opens the producer span; nil uses the global OTel tracer
	// provider.
	Tracer trace.Tracer
	// Timeout bounds the wait for the broker's PubAck; 0 uses
	// DefaultPublishTimeout. It derives from the caller's ctx, so an
	// already-cancelled ctx still fails fast.
	Timeout time.Duration
}

// Publish sends msg and waits — bounded by Timeout — for the broker's ack,
// wrapped in a producer span named "publish <subject>". A non-empty msgID is
// set as the Nats-Msg-Id header, the broker-side dedup key within the
// stream's duplicate window; empty leaves any Nats-Msg-Id already on msg
// untouched. Trace context from ctx is injected into msg's headers (msg is
// mutated) so the consumer span re-parents across the hop.
//
// The returned PubAck reports where the message landed and whether the
// broker deduplicated it (PubAck.Duplicate) — a duplicate is a normal
// outcome for an at-least-once producer retrying, not an error. The
// stream, sequence, and duplicate outcome are also recorded on the span.
func (p Publisher) Publish(ctx context.Context, msg *nats.Msg, msgID string) (*jetstream.PubAck, error) {
	tr := p.Tracer
	if tr == nil {
		tr = otel.Tracer("github.com/entireio/go-nuts/natsmsg")
	}
	ctx, span := StartProducerSpan(ctx, tr, msg.Subject, "publish "+msg.Subject)
	defer span.End()

	if msgID != "" {
		if msg.Header == nil {
			msg.Header = nats.Header{}
		}
		msg.Header.Set(nats.MsgIdHdr, msgID)
		span.SetAttributes(attribute.String("messaging.message.id", msgID))
	}
	Inject(ctx, msg)

	timeout := p.Timeout
	if timeout == 0 {
		timeout = DefaultPublishTimeout
	}
	pubCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ack, err := p.JS.PublishMsg(pubCtx, msg)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "publish")
		return nil, fmt.Errorf("natsmsg: publish %s: %w", msg.Subject, err)
	}
	span.SetAttributes(
		attribute.String("messaging.nats.stream", ack.Stream),
		attribute.Int64("messaging.nats.sequence", ClampToInt64(ack.Sequence)),
		attribute.Bool("messaging.nats.duplicate", ack.Duplicate),
	)
	return ack, nil
}
