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
// acknowledgment when a publisher's Timeout is zero. The bound matters because
// Entire connections reconnect forever and nats.go buffers publishes while
// disconnected: without it, a publisher holding an HTTP request or a message
// disposition would wait indefinitely behind an unreachable broker instead
// of failing over to its retry path (the caller's outbox, nak, or 5xx).
const DefaultPublishTimeout = 5 * time.Second

// StartProducerSpan opens a producer span named name on tr, pre-tagged with
// the standard messaging attributes (system=nats, operation.type=publish,
// destination=subject) — the publish-side counterpart of [StartConsumerSpan].
// Extra attrs are appended. The caller owns the span and must End it, and
// should Inject the returned ctx into the outgoing message so the consumer can
// re-parent.
func StartProducerSpan(ctx context.Context, tr trace.Tracer, subject, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	base := append([]attribute.KeyValue{
		attribute.String("messaging.system", "nats"),
		attribute.String("messaging.operation.type", "publish"),
		attribute.String("messaging.destination.name", subject),
	}, attrs...)
	return tr.Start(ctx, name, trace.WithSpanKind(trace.SpanKindProducer), trace.WithAttributes(base...)) //nolint:spancheck // the caller owns the span and Ends it
}

// Publisher is the JetStream publish core shared by Entire's producers on the
// modern [jetstream.JetStream] API: a producer span, W3C trace-context
// injection, Nats-Msg-Id dedup, and a bounded wait for the broker's ack. Every
// service had grown its own copy of exactly this prologue around PublishMsg;
// Publisher owns the prologue and nothing else — subject construction, payload
// encoding, domain metrics/logging, and the response to a failed publish
// (outbox retry, nak, HTTP 5xx) stay with the caller.
//
// [LegacyPublisher] is the same core for callers still on the legacy
// [nats.JetStreamContext] API; it is a transitional bridge — the modern
// Publisher is the destination, so a caller migrates by swapping the type and
// the LegacyPublisher can be deleted once no caller remains.
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
	// Operation is the caller-selected producer span name (e.g.
	// "repo.ops.publish"); empty falls back to "publish <subject>". It names the
	// span only — the messaging operation is recorded as the standard
	// messaging.operation.type=publish attribute, per OTel semantic conventions,
	// which reserve messaging.operation.name for the system-specific operation
	// (send/ack/nack), not an application span name.
	Operation string
}

// Publish sends msg and waits — bounded by Timeout — for the broker's ack,
// wrapped in a producer span named by Operation (or "publish <subject>" when
// unset). A non-empty msgID is set as the Nats-Msg-Id header, the broker-side
// dedup key within the stream's duplicate window; empty leaves any Nats-Msg-Id
// already on msg untouched. Trace context from ctx is injected into msg's
// headers (msg is mutated) so the consumer span re-parents across the hop.
//
// attrs are the caller's domain span attributes (placement, job identity, …) —
// appended to the standard messaging.* set on the producer span. They stay
// caller-owned: the shared package does not know them, but records them on the
// span it owns so a Phase 3 replacement keeps its existing producer-span tags.
//
// The returned PubAck reports where the message landed and whether the
// broker deduplicated it (PubAck.Duplicate) — a duplicate is a normal
// outcome for an at-least-once producer retrying, not an error. The
// stream, sequence, and duplicate outcome are also recorded on the span.
func (p Publisher) Publish(ctx context.Context, msg *nats.Msg, msgID string, attrs ...attribute.KeyValue) (*jetstream.PubAck, error) {
	var native *jetstream.PubAck
	err := publish(ctx, publishConfig{tracer: p.Tracer, operation: p.Operation, timeout: p.Timeout}, msg, msgID, attrs,
		func(pubCtx context.Context, m *nats.Msg) (ack, error) {
			pa, perr := p.JS.PublishMsg(pubCtx, m)
			if perr != nil {
				return ack{}, perr //nolint:wrapcheck // publish() wraps with the "natsmsg: publish <subject>" prefix
			}
			native = pa
			return ack{stream: pa.Stream, sequence: pa.Sequence, duplicate: pa.Duplicate}, nil
		})
	if err != nil {
		return nil, err
	}
	return native, nil
}

// LegacyJetStream is the subset of the legacy [nats.JetStreamContext] the
// [LegacyPublisher] drives — the classic synchronous PublishMsg, satisfied by
// nats.JetStreamContext. [DLQPublisher] is an alias of it: dead-lettering and
// legacy publishing share the identical publish surface, so a caller can hand
// both the one JetStreamContext it already holds.
type LegacyJetStream interface {
	PublishMsg(m *nats.Msg, opts ...nats.PubOpt) (*nats.PubAck, error)
}

// LegacyPublisher is [Publisher] for callers still on the legacy
// [nats.JetStreamContext] API. It shares the identical publish prologue
// (producer span, trace-context injection, Nats-Msg-Id, bounded ack, PubAck
// telemetry) and differs only in the underlying publish call — legacy
// PublishMsg with a nats.Context deadline, versus the modern ctx-native
// PublishMsg. It is a transitional bridge: prefer [Publisher] for new code;
// this type exists so a legacy caller can adopt go-nuts without first
// migrating its JetStream client, and it can be removed once none remain.
type LegacyPublisher struct {
	// JS is the legacy JetStream context to publish through.
	JS LegacyJetStream
	// Tracer opens the producer span; nil uses the global OTel tracer
	// provider.
	Tracer trace.Tracer
	// Timeout bounds the wait for the broker's PubAck; 0 uses
	// DefaultPublishTimeout.
	Timeout time.Duration
	// Operation is the caller-selected producer span name (e.g.
	// "repo.ops.publish"); empty falls back to "publish <subject>". It names the
	// span only — see [Publisher.Operation].
	Operation string
}

// Publish behaves exactly as [Publisher.Publish] — same span, caller attrs,
// headers, msg-id, bounded ack, and telemetry — over the legacy
// [nats.JetStreamContext] API, returning its native [nats.PubAck].
func (p LegacyPublisher) Publish(ctx context.Context, msg *nats.Msg, msgID string, attrs ...attribute.KeyValue) (*nats.PubAck, error) {
	var native *nats.PubAck
	err := publish(ctx, publishConfig{tracer: p.Tracer, operation: p.Operation, timeout: p.Timeout}, msg, msgID, attrs,
		func(pubCtx context.Context, m *nats.Msg) (ack, error) {
			pa, perr := p.JS.PublishMsg(m, nats.Context(pubCtx))
			if perr != nil {
				return ack{}, perr //nolint:wrapcheck // publish() wraps with the "natsmsg: publish <subject>" prefix
			}
			native = pa
			return ack{stream: pa.Stream, sequence: pa.Sequence, duplicate: pa.Duplicate}, nil
		})
	if err != nil {
		return nil, err
	}
	return native, nil
}

// ack is the broker acknowledgment normalized across the legacy nats.PubAck and
// modern jetstream.PubAck, carrying just the fields the shared core records on
// the producer span.
type ack struct {
	stream    string
	sequence  uint64
	duplicate bool
}

// publishConfig is the publisher configuration the shared core reads,
// independent of which JetStream client performs the publish.
type publishConfig struct {
	tracer    trace.Tracer
	operation string
	timeout   time.Duration
}

// publish is the publish prologue shared by [Publisher] and [LegacyPublisher]:
// open the producer span (named by cfg.operation or "publish <subject>", tagged
// with the caller's attrs alongside the standard messaging.* set), stamp the
// Nats-Msg-Id, inject trace context, and bound the ack wait — then run do, the
// one step that differs between the modern and legacy clients, and record the
// PubAck telemetry (or the error) on the span. do reports the normalized ack and
// returns the raw publish error unwrapped so the core can wrap it and callers
// can still classify the underlying cause.
func publish(ctx context.Context, cfg publishConfig, msg *nats.Msg, msgID string, attrs []attribute.KeyValue, do func(context.Context, *nats.Msg) (ack, error)) error {
	tr := cfg.tracer
	if tr == nil {
		tr = otel.Tracer("github.com/entireio/go-nuts/natsmsg")
	}
	name := cfg.operation
	if name == "" {
		name = "publish " + msg.Subject
	}
	ctx, span := StartProducerSpan(ctx, tr, msg.Subject, name, attrs...)
	defer span.End()

	if msgID != "" {
		if msg.Header == nil {
			msg.Header = nats.Header{}
		}
		msg.Header.Set(nats.MsgIdHdr, msgID)
		span.SetAttributes(attribute.String("messaging.message.id", msgID))
	}
	Inject(ctx, msg)

	timeout := cfg.timeout
	if timeout == 0 {
		timeout = DefaultPublishTimeout
	}
	pubCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	a, err := do(pubCtx, msg)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "publish")
		return fmt.Errorf("natsmsg: publish %s: %w", msg.Subject, err)
	}
	span.SetAttributes(
		attribute.String("messaging.nats.stream", a.stream),
		attribute.Int64("messaging.nats.sequence", ClampToInt64(a.sequence)),
		attribute.Bool("messaging.nats.duplicate", a.duplicate),
	)
	return nil
}
