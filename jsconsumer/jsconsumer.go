// Package jsconsumer is the durable JetStream pull-consumer scaffold shared by
// Entire's decode-and-dispatch consumers. Each of those used to hand-roll the
// same lifecycle: jetstream.New → CreateOrUpdateConsumer (durable, explicit
// ack, bounded ack-wait, bounded MaxDeliver, filter subject) → Consume → stop
// on context cancel; and the same per-message prologue: open a consumer span
// (re-parented across the NATS hop), decode the payload, and Term on a decode
// error.
//
// This package owns exactly that scaffold and nothing else. Each consumer
// keeps its OWN event type, business dispatch, metrics, and log lines — Start
// takes the consumer's message callback, and Process invokes the consumer's
// handle with the decoded event and the live span. The handler owns the
// message's terminal disposition (Ack/Nak/Term); Process never touches a
// message that decoded.
//
// Lifted from entire-api's internal/jsconsumer (COR-929), folding in the
// module's shutdown classification ([nuts.IsShutdownFetchErr] gates the
// consume-error log so a drain during rollout isn't reported as a fault,
// COR-923) and an optional [natsmsg.KeepInProgress] heartbeat for consumers
// whose handlers legitimately outrun AckWait.
package jsconsumer

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/entireio/go-nuts"
	"github.com/entireio/go-nuts/natsmsg"
)

const (
	// DefaultAckWait is how long JetStream waits for an ack before redelivery
	// when Config.AckWait is zero — comfortably longer than an RPC round-trip
	// plus a DB upsert, short enough that a stuck message redelivers promptly.
	DefaultAckWait = 30 * time.Second
	// DefaultMaxDeliver caps redelivery before JetStream stops redelivering a
	// message when Config.MaxDeliver is zero.
	DefaultMaxDeliver = 8
)

// Config describes the durable consumer to create and how to trace/log it. The
// filter is given as either FilterSubject (single) or FilterSubjects (multi),
// mirroring jetstream.ConsumerConfig, so a migrated consumer preserves its
// existing on-server filter form exactly.
type Config struct {
	Stream         string        // JetStream stream to bind (must already exist)
	Durable        string        // durable consumer name, stable across restarts
	FilterSubject  string        // set this OR FilterSubjects
	FilterSubjects []string      // set this OR FilterSubject
	AckWait        time.Duration // 0 → DefaultAckWait (see EffectiveAckWait)
	MaxDeliver     int           // 0 → DefaultMaxDeliver (see EffectiveMaxDeliver)
	SpanName       string        // per-message consumer span name (e.g. "repolifecycle.consume")
	Name           string        // short consumer name, used as the log prefix

	// Tracer opens the per-message consumer span; nil uses the global OTel
	// tracer provider.
	Tracer trace.Tracer
	// Logger receives the scaffold's own log lines (decode-term warnings,
	// consume errors); nil uses slog.Default.
	Logger *slog.Logger

	// KeepInProgress runs a natsmsg.KeepInProgress heartbeat while handle
	// executes, for handlers that legitimately outrun AckWait. The extension
	// is capped, so a wedged handler still redelivers — see
	// natsmsg.KeepInProgress. The heartbeat stops when handle returns; a tick
	// racing the handler's own terminal Ack/Nak/Term is a client-side no-op
	// (nats.go marks the message disposed locally) that halts the heartbeat.
	//
	// Setting this forces the prefetch to one message (see MaxMessages): the
	// heartbeat extends only the delivery the handler holds, so anything
	// buffered behind a long handler would exhaust its AckWait unextended.
	KeepInProgress bool

	// MaxMessages caps how many deliveries the consume loop buffers
	// client-side (jetstream.PullMaxMessages); 0 uses the nats.go default
	// (500). AckWait runs from server delivery into that buffer, not from
	// callback dispatch, and callbacks run serially — so a consumer whose
	// handler can approach AckWait should keep this small or queued
	// deliveries expire and redeliver concurrently to other replicas.
	// KeepInProgress forces it to 1; setting both to conflicting values is a
	// Start error.
	MaxMessages int
}

// EffectiveAckWait is the AckWait actually applied to the JetStream consumer:
// Config.AckWait, or DefaultAckWait when zero. Use it — not the raw field —
// anywhere the real value matters, e.g. wiring a natsmsg.KeepInProgress
// heartbeat by hand (a zero ackWait there is a no-op).
func (c Config) EffectiveAckWait() time.Duration {
	if c.AckWait == 0 {
		return DefaultAckWait
	}
	return c.AckWait
}

// EffectiveMaxDeliver is the MaxDeliver actually applied to the JetStream
// consumer: Config.MaxDeliver, or DefaultMaxDeliver when zero. Use it — not
// the raw field — when building a backoff.Policy for this consumer's
// messages: the two packages read zero differently (here it means "default
// to DefaultMaxDeliver"; on the policy it means unlimited redeliveries, the
// jetstream.ConsumerConfig semantics), so passing a zero Config.MaxDeliver
// through verbatim would make TermOnExhaustion never fire on the broker's
// real final delivery and orphan work-queue messages (COR-762).
func (c Config) EffectiveMaxDeliver() int {
	if c.MaxDeliver == 0 {
		return DefaultMaxDeliver
	}
	return c.MaxDeliver
}

func (c Config) tracer() trace.Tracer {
	if c.Tracer == nil {
		return otel.Tracer("github.com/entireio/go-nuts/jsconsumer")
	}
	return c.Tracer
}

func (c Config) logger() *slog.Logger {
	if c.Logger == nil {
		return slog.Default()
	}
	return c.Logger
}

// Runner is a live consume loop. Stop halts it and is idempotent.
type Runner struct{ cc jetstream.ConsumeContext }

// Stop halts the consume loop. Safe on a nil Runner and safe to call twice.
func (r *Runner) Stop() {
	if r != nil && r.cc != nil {
		r.cc.Stop()
		r.cc = nil
	}
}

// Start creates/updates the durable consumer on nc and begins delivering
// messages to onMsg, returning a Runner that Stops when ctx is cancelled. The
// stream must already exist (provisioned declaratively); callers whose cells
// opt in per-stream gate Start behind that opt-in so a cell without the stream
// doesn't error at CreateOrUpdateConsumer.
//
// Consume errors are logged at Warn, except a drain/close during shutdown
// ([nuts.IsShutdownFetchErr]), which is a clean exit rather than a fault.
func Start(ctx context.Context, nc *nats.Conn, cfg Config, onMsg func(jetstream.Msg)) (*Runner, error) {
	if cfg.KeepInProgress && cfg.MaxMessages > 1 {
		return nil, fmt.Errorf("jsconsumer(%s): KeepInProgress requires MaxMessages <= 1: the heartbeat extends only the in-flight delivery, so %d buffered messages would exhaust AckWait behind a long handler", cfg.Name, cfg.MaxMessages)
	}
	if nc == nil {
		return nil, fmt.Errorf("jsconsumer(%s): nil nats conn", cfg.Name)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("jsconsumer(%s): jetstream.New: %w", cfg.Name, err)
	}
	cons, err := js.CreateOrUpdateConsumer(ctx, cfg.Stream, jetstream.ConsumerConfig{
		Durable:        cfg.Durable,
		AckPolicy:      jetstream.AckExplicitPolicy,
		AckWait:        cfg.EffectiveAckWait(),
		MaxDeliver:     cfg.EffectiveMaxDeliver(),
		FilterSubject:  cfg.FilterSubject,
		FilterSubjects: cfg.FilterSubjects,
	})
	if err != nil {
		return nil, fmt.Errorf("jsconsumer(%s): create consumer %s/%s: %w", cfg.Name, cfg.Stream, cfg.Durable, err)
	}
	consumeOpts := []jetstream.PullConsumeOpt{jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, cerr error) {
		if nuts.IsShutdownFetchErr(ctx, cerr) {
			return
		}
		cfg.logger().WarnContext(ctx, cfg.Name+": consume error", slog.Any("error", cerr))
	})}
	switch {
	case cfg.KeepInProgress:
		consumeOpts = append(consumeOpts, jetstream.PullMaxMessages(1))
	case cfg.MaxMessages > 0:
		consumeOpts = append(consumeOpts, jetstream.PullMaxMessages(cfg.MaxMessages))
	}
	cc, err := cons.Consume(onMsg, consumeOpts...)
	if err != nil {
		return nil, fmt.Errorf("jsconsumer(%s): consume %s: %w", cfg.Name, cfg.Stream, err)
	}
	r := &Runner{cc: cc}
	go func() {
		<-ctx.Done()
		r.Stop()
	}()
	return r, nil
}

// Process runs the per-message prologue shared by every consumer: open the
// consumer span (re-parented to the producer across the NATS hop), decode the
// payload, and on a decode error record it, log "<Name>: decode <Stream>;
// term", invoke onUndecodable (so the caller bumps its own drop metric with
// its own attributes), and Term the message — a poison payload won't decode on
// redelivery. On success it invokes handle with the span-enriched context, the
// live span (for RecordError/SetStatus on the business error paths), the
// message, and the decoded event; with Config.KeepInProgress set, an AckWait
// heartbeat runs for the duration of handle.
//
// Callers wire Process as the func they pass to Start AND the entry point
// their tests drive, so the decode-or-term path is exercised the same way in
// production and tests.
func Process[E any](
	ctx context.Context,
	msg jetstream.Msg,
	cfg Config,
	decode func([]byte) (E, error),
	handle func(ctx context.Context, span trace.Span, msg jetstream.Msg, ev E),
	onUndecodable func(ctx context.Context),
) {
	ctx, span := natsmsg.StartConsumerSpan(ctx, cfg.tracer(), msg, cfg.SpanName)
	defer span.End()

	ev, err := decode(msg.Data())
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "decode")
		cfg.logger().WarnContext(ctx, cfg.Name+": decode "+cfg.Stream+"; term",
			slog.Any("error", err), slog.String("subject", msg.Subject()))
		if onUndecodable != nil {
			onUndecodable(ctx)
		}
		if termErr := msg.Term(); termErr != nil {
			cfg.logger().WarnContext(ctx, cfg.Name+": term failed", slog.Any("error", termErr))
		}
		return
	}
	if cfg.KeepInProgress {
		stop := natsmsg.KeepInProgress(msg.InProgress, cfg.EffectiveAckWait())
		defer stop()
	}
	handle(ctx, span, msg, ev)
}
