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
	"errors"
	"fmt"
	"log/slog"
	"sync"
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

	// InactiveThreshold mirrors jetstream.ConsumerConfig.InactiveThreshold:
	// how long the durable may go without an active subscription before the
	// server deletes it; zero means never (the JetStream default for
	// durables). Consumers on interest-retention streams set this so a
	// decommissioned durable stops pinning every message it would have
	// received, instead of holding them until the stream's MaxAge.
	InactiveThreshold time.Duration

	// MaxAckPending mirrors jetstream.ConsumerConfig.MaxAckPending: the
	// server-side cap on deliveries outstanding un-acked across ALL replicas
	// sharing the durable; zero uses the server default (1000). Distinct
	// from MaxMessages below, which caps only this process's client-side
	// buffer.
	MaxAckPending int

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

// validate rejects the configuration errors no retry can fix; shared by
// [Start] (one-shot) and [Run] (supervised, which must fail fast on these
// rather than retry them forever).
func (c Config) validate(nc *nats.Conn, onMsg func(jetstream.Msg)) error {
	if c.KeepInProgress && c.MaxMessages > 1 {
		return fmt.Errorf("jsconsumer(%s): KeepInProgress requires MaxMessages <= 1: the heartbeat extends only the in-flight delivery, so %d buffered messages would exhaust AckWait behind a long handler", c.Name, c.MaxMessages)
	}
	if c.Stream == "" {
		return fmt.Errorf("jsconsumer(%s): Stream is required", c.Name)
	}
	if c.Durable == "" {
		// CreateOrUpdateConsumer accepts an empty durable and silently makes
		// an ephemeral, server-named consumer — losing the stable
		// resume-across-restarts behavior this scaffold promises.
		return fmt.Errorf("jsconsumer(%s): Durable is required", c.Name)
	}
	if onMsg == nil {
		return fmt.Errorf("jsconsumer(%s): onMsg is required", c.Name)
	}
	if c.FilterSubject != "" && len(c.FilterSubjects) != 0 {
		return fmt.Errorf("jsconsumer(%s): set FilterSubject or FilterSubjects, not both", c.Name)
	}
	for i, subject := range c.FilterSubjects {
		if subject == "" {
			return fmt.Errorf("jsconsumer(%s): FilterSubjects[%d] is empty", c.Name, i)
		}
	}
	if c.AckWait < 0 {
		return fmt.Errorf("jsconsumer(%s): AckWait must not be negative", c.Name)
	}
	if c.MaxDeliver < -1 {
		return fmt.Errorf("jsconsumer(%s): MaxDeliver must be -1 (unlimited), 0 (default), or positive", c.Name)
	}
	if c.MaxMessages < 0 {
		return fmt.Errorf("jsconsumer(%s): MaxMessages must not be negative", c.Name)
	}
	if c.MaxAckPending < -1 {
		return fmt.Errorf("jsconsumer(%s): MaxAckPending must be -1 (unlimited), 0 (server default), or positive", c.Name)
	}
	if c.InactiveThreshold < 0 {
		return fmt.Errorf("jsconsumer(%s): InactiveThreshold must not be negative", c.Name)
	}
	if nc == nil {
		return fmt.Errorf("jsconsumer(%s): nil nats conn", c.Name)
	}
	return nil
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

// isShutdownConsumeErr reports whether a consume error is the benign result
// of the connection going away during shutdown. [nuts.IsShutdownFetchErr]
// covers the core nats.go sentinels, but the jetstream consume loop surfaces
// its OWN [jetstream.ErrConnectionClosed] — a distinct value that wraps
// neither — so it must be matched separately or a clean rollout drain still
// logs a warning (the COR-923 noise this suppression exists to remove). As
// with the root classifier, it is only "clean" when ctx is already done; a
// mid-run connection loss is a genuine fault worth logging.
func isShutdownConsumeErr(ctx context.Context, err error) bool {
	return nuts.IsShutdownFetchErr(ctx, err) ||
		(ctx.Err() != nil && errors.Is(err, jetstream.ErrConnectionClosed))
}

// Runner is a live consume loop. Stop halts it and is idempotent.
type Runner struct {
	cc jetstream.ConsumeContext
	// closed is cc.Closed(), captured while the subscription is live (the
	// same channel is returned on every call, so capturing once avoids any
	// post-Stop race) — it closes when the consume loop has fully wound
	// down, including an in-flight handler.
	closed <-chan struct{}
	// watcherDone closes when Start's context-cancellation watcher exits. It is
	// intentionally internal: callers synchronize on Stop, while tests pin that
	// an explicit Stop does not retain a goroutine until the parent context ends.
	watcherDone <-chan struct{}
	stop        sync.Once
}

// Stop halts the consume loop and blocks until it has fully wound down —
// including an in-flight handler — so a caller may tear down what handlers
// use (stores, publishers, the NATS connection) the moment it returns: the
// cancel → join → drain shutdown ordering (COR-923). Safe on a nil or
// never-Started Runner, idempotent, and safe for concurrent use — an
// explicit Stop can race the context-cancel stop armed by Start. Must not be
// called from inside the handler; it would deadlock waiting on itself.
func (r *Runner) Stop() {
	if r == nil || r.cc == nil {
		return
	}
	r.stop.Do(r.cc.Stop)
	<-r.closed
}

// Start creates/updates the durable consumer on nc and begins delivering
// messages to onMsg, returning a Runner that Stops when ctx is cancelled. The
// stream must already exist (provisioned declaratively); callers whose cells
// opt in per-stream gate Start behind that opt-in so a cell without the stream
// doesn't error at CreateOrUpdateConsumer.
//
// Consume errors are logged at Warn, except a drain/close during shutdown —
// both the core nats.go sentinels ([nuts.IsShutdownFetchErr]) and jetstream's
// own connection-closed error — which is a clean exit rather than a fault.
func Start(ctx context.Context, nc *nats.Conn, cfg Config, onMsg func(jetstream.Msg)) (*Runner, error) {
	if err := cfg.validate(nc, onMsg); err != nil {
		return nil, err
	}
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("jsconsumer(%s): jetstream.New: %w", cfg.Name, err)
	}
	cons, err := js.CreateOrUpdateConsumer(ctx, cfg.Stream, jetstream.ConsumerConfig{
		Durable:           cfg.Durable,
		AckPolicy:         jetstream.AckExplicitPolicy,
		AckWait:           cfg.EffectiveAckWait(),
		MaxDeliver:        cfg.EffectiveMaxDeliver(),
		FilterSubject:     cfg.FilterSubject,
		FilterSubjects:    cfg.FilterSubjects,
		InactiveThreshold: cfg.InactiveThreshold,
		MaxAckPending:     cfg.MaxAckPending,
	})
	if err != nil {
		return nil, fmt.Errorf("jsconsumer(%s): create consumer %s/%s: %w", cfg.Name, cfg.Stream, cfg.Durable, err)
	}
	consumeOpts := []jetstream.PullConsumeOpt{jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, cerr error) {
		if isShutdownConsumeErr(ctx, cerr) {
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
	watcherDone := make(chan struct{})
	r := &Runner{cc: cc, closed: cc.Closed(), watcherDone: watcherDone}
	go func() {
		defer close(watcherDone)
		select {
		case <-ctx.Done():
			r.Stop()
		case <-r.closed:
		}
	}()
	return r, nil
}

// isRetryableStartError classifies the narrow set of failures that can become
// healthy without changing Config or replacing nc. Everything else returns to
// the owner so a bad deployment cannot stay alive indefinitely with no
// consumer. Unknown server-side 5xx responses are retried, except known
// permanent JetStream configuration/account errors whose API happens to use a
// 5xx status.
func isRetryableStartError(err error) bool {
	if errors.Is(err, jetstream.ErrStreamNotFound) ||
		errors.Is(err, nats.ErrNoResponders) ||
		errors.Is(err, nats.ErrTimeout) ||
		errors.Is(err, nats.ErrDisconnected) {
		return true
	}

	var jsErr jetstream.JetStreamError
	if !errors.As(err, &jsErr) || jsErr.APIError() == nil {
		return false
	}
	apiErr := jsErr.APIError()
	switch apiErr.ErrorCode { //nolint:exhaustive // unrecognized 5xx codes are deliberately retryable; all other unknown codes fail closed
	case jetstream.JSErrCodeBadRequest,
		jetstream.JSErrCodeConsumerCreate,
		jetstream.JSErrCodeConsumerNameExists,
		jetstream.JSErrCodeMaximumConsumersLimit,
		jetstream.JSErrCodeJetStreamNotEnabledForAccount,
		jetstream.JSErrCodeJetStreamNotEnabled,
		jetstream.JSErrCodeConsumerAlreadyExists,
		jetstream.JSErrCodeDuplicateFilterSubjects,
		jetstream.JSErrCodeOverlappingFilterSubjects,
		jetstream.JSErrCodeConsumerEmptyFilter,
		jetstream.JSErrCodeConsumerExists:
		return false
	default:
		return apiErr.Code >= 500
	}
}

// Retry envelope for [Run]'s supervision: exponential from runRetryInitial
// doubling to runRetryMax, reset once a consume loop survives longer than the
// max. Package vars (not consts) so tests can compress the schedule.
var (
	runRetryInitial = time.Second
	runRetryMax     = 30 * time.Second
)

// Run supervises the consume loop [Start] builds. Where Start is one-shot —
// an error at consumer creation is returned, and a consume loop that closes
// underneath the caller (consumer deleted on the server, subscription
// invalidated) stays closed — Run retries both with exponential backoff
// (runRetryInitial doubling to runRetryMax, reset after a loop that outlives
// the max). In particular a stream that is not provisioned yet at boot — the
// declarative-provisioning race — is a retry, not a crash.
//
// Run blocks until ctx is cancelled, then joins the live loop (including an
// in-flight handler, see [Runner.Stop]) before returning, so it composes with
// the cancel → join → drain shutdown ordering as the body of a supervised
// goroutine:
//
//	g.Go(func(ctx context.Context) {
//		if err := jsconsumer.Run(ctx, nc, cfg, onMsg); err != nil {
//			log.Error("consumer config rejected", "error", err)
//		}
//	})
//
// Local configuration errors and permanent server responses (invalid consumer
// configuration, authorization/account setup) can never succeed on retry and
// are returned immediately. A missing declarative stream, temporary transport
// failure, or unknown server-side 5xx response remains supervised. A nil return
// means a clean, ctx-driven exit.
func Run(ctx context.Context, nc *nats.Conn, cfg Config, onMsg func(jetstream.Msg)) error {
	if err := cfg.validate(nc, onMsg); err != nil {
		return err
	}
	delay := runRetryInitial
	for {
		// Each attempt gets a child context so the cancel-watcher goroutine
		// Start arms is released when the attempt dies, instead of one
		// accumulating per recreate for the life of the process.
		attemptCtx, cancelAttempt := context.WithCancel(ctx)
		r, err := Start(attemptCtx, nc, cfg, onMsg)
		if err != nil {
			cancelAttempt()
			if ctx.Err() != nil {
				return nil //nolint:nilerr // a Start error during shutdown is a clean, ctx-driven exit, not a fault to report
			}
			if !isRetryableStartError(err) {
				return err
			}
			cfg.logger().WarnContext(ctx, cfg.Name+": start consumer failed; retrying",
				slog.Any("error", err), slog.Duration("retry_in", delay))
		} else {
			started := time.Now()
			select {
			case <-ctx.Done():
				r.Stop()
				cancelAttempt()
				return nil
			case <-r.closed:
				cancelAttempt()
				if ctx.Err() != nil {
					return nil //nolint:nilerr // the loop closing during shutdown is the expected wind-down, not a fault
				}
				// A loop that ran long enough to be called healthy earns a
				// fresh envelope; a flapping one keeps escalating.
				if time.Since(started) >= runRetryMax {
					delay = runRetryInitial
				}
				cfg.logger().WarnContext(ctx, cfg.Name+": consume loop closed; recreating",
					slog.Duration("retry_in", delay))
			}
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(delay):
		}
		delay = min(delay*2, runRetryMax)
	}
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
