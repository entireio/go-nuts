package nuts

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// ErrDrainTimeout is returned when a connection does not reach CLOSED within
// the total drain backstop. The connection is force-closed before return, so
// callers never inherit an ambiguous asynchronous drain.
var ErrDrainTimeout = errors.New("nuts: NATS drain timed out before close")

// transientFetchErrs are the pull-consumer failures a caller recovers from by
// resubscribing: the link to the server went away, or the consumer moved. None
// of them indicate a fault in the caller, and none require operator action —
// the subscribe loop reconnects with backoff and carries on.
//
// The set is drawn from what a rolling restart actually produces. During one
// mirror-pipeline deploy a single pod contributed 57 ErrNoResponders inside 83
// milliseconds as its in-flight fetches failed together; a NATS server upgrade
// produced the same shape via ErrFetchDisconnected and
// ErrConsumerLeadershipChanged. Logged at error level these bursts cross a
// cluster's error-log alert threshold on their own, which trains operators to
// discount the one signal that should mean something (COR-1226 / COR-1228).
//
// nats.ErrConsumerDeleted is deliberately absent. Resubscribing does "recover"
// from it — but by silently recreating the durable with the subscriber's
// default deliver policy and no ack state, which replays the stream's retained
// backlog into downstream systems. A durable that disappears mid-run is
// operator action or a bug, never infrastructure churn, and the log that
// surfaces it is the only evidence of the deletion.
var transientFetchErrs = []error{
	nats.ErrConnectionClosed,
	nats.ErrConnectionDraining,
	nats.ErrDisconnected,
	nats.ErrNoResponders,
	nats.ErrFetchDisconnected,
	nats.ErrConsumerLeadershipChanged,
}

// IsTransientFetchErr reports whether a pull-consumer Fetch or subscribe error
// is a recoverable interruption — a dropped link, a drained connection, a
// consumer that moved or was recreated — rather than a fault. It is
// independent of shutdown: a NATS server rolling underneath a healthy caller
// produces these while ctx is still live.
//
// Callers should log these below error level. The distinction that matters for
// alerting is not "did a fetch fail" but "did it stop recovering", and a single
// interrupted fetch cannot answer that.
//
// nats.ErrTimeout and context.DeadlineExceeded are deliberately absent, and
// the reason is fetch-path-specific: an idle poll reaching MaxWait is the
// steady state of a quiet consumer, and a fetch deadline can be a wedged
// server as easily as a slow one. On a subscribe/create retry path the same
// timeout usually IS deploy churn (a slow JetStream API during an upgrade),
// but this predicate cannot see which path it is on — callers that retry
// subscription setup must classify timeouts themselves.
func IsTransientFetchErr(err error) bool {
	if err == nil {
		return false
	}
	for _, target := range transientFetchErrs {
		if errors.Is(err, target) {
			return true
		}
	}
	return false
}

// IsTransientSubscribeErr reports whether a subscribe/create retry error is a
// recoverable interruption. It is a superset of [IsTransientFetchErr]: a
// retried subscribe also absorbs the stream not existing yet (rollout
// ordering, where the consumer comes up before the service that ensures the
// stream) and a JetStream API slow to answer the consumer-info/create request
// (nats.ErrTimeout / context.DeadlineExceeded — deploy churn on this path,
// unlike a fetch, where a timeout is an idle poll and a deadline can be a
// wedged server).
//
// Keeping the set here rather than in each caller is the point: two services
// already share these consumer loops, and the alert noise this package exists
// to remove comes back the moment their classifications drift.
//
// # The modern client's errors are separate values
//
// The [jetstream] package does not reuse the nats.* error values: its
// ErrStreamNotFound is a distinct type carrying an APIError, so errors.Is finds
// neither across the two. A consumer migrated to the modern client therefore had
// every rollout-ordering failure classified as a FAULT until those spellings were
// added here — which is the exact alert erosion above, arriving through the door
// this predicate holds shut. mirror-pipeline hit it while migrating its lifecycle
// fan (COR-1254) and carried a local extension until this landed.
//
// A MISSING CONSUMER is deliberately absent in both spellings, for the same
// reason nats.ErrConsumerDeleted is (see transientFetchErrs): recreating the
// durable replays the stream's retained backlog, and the log is the only evidence.
// A caller that is still waiting for its FIRST bind knows the same error is
// expected there and can say so itself; this predicate cannot tell those apart
// from the value alone, and guessing wrong is a silent replay.
func IsTransientSubscribeErr(err error) bool {
	return IsTransientFetchErr(err) ||
		errors.Is(err, nats.ErrStreamNotFound) ||
		errors.Is(err, jetstream.ErrStreamNotFound) ||
		errors.Is(err, jetstream.ErrJetStreamNotEnabled) ||
		errors.Is(err, nats.ErrTimeout) ||
		errors.Is(err, context.DeadlineExceeded)
}

// IsShutdownFetchErr reports whether a pull-consumer Fetch error is the benign
// result of our own teardown: the connection was closed or drained underneath
// the caller, with ctx already done. Those two errors are self-inflicted — the
// shutdown path drains the connection on purpose — so exiting without a log is
// honest.
//
// It is intentionally narrower than [IsTransientFetchErr]. The other
// interruptions can coincide with shutdown while meaning something real: a
// fetch that dies with ErrConsumerDeleted during a rolling deploy is the only
// evidence somebody deleted the durable, and swallowing it because ctx
// happened to be done leaves the redelivery storm after restart unexplained.
// Callers should log those (IsTransientFetchErr picks the severity) rather
// than exit silently.
//
// Use it to gate the error log in a fetch loop:
//
//	msgs, err := sub.Fetch(1, nats.MaxWait(5*time.Second))
//	if err != nil {
//		switch {
//		case errors.Is(err, nats.ErrTimeout):   // idle poll; keep fetching
//		case nuts.IsShutdownFetchErr(ctx, err): // our teardown; exit quietly
//		default:                                // log it; IsTransientFetchErr picks the severity
//		}
//	}
func IsShutdownFetchErr(ctx context.Context, err error) bool {
	return ctx.Err() != nil &&
		(errors.Is(err, nats.ErrConnectionClosed) || errors.Is(err, nats.ErrConnectionDraining))
}

// Drain drains nc and waits (bounded by timeout) for it to flush pending
// acks/publishes and reach CLOSED. nats.go's Conn.Drain is asynchronous — it
// starts the drain on a background goroutine and returns immediately — so a
// bare Drain in a shutdown cleanup would let the process exit before the flush
// completes. This subscribes to the connection's CLOSED status event, starts
// the drain, and blocks until the connection closes or timeout elapses. name
// labels the connection in the logs so a wedged drain can be attributed, and
// diagnostics are written to logger (defaulting to [slog.Default] when nil).
// ctx supplies log context only: shutdown callers commonly pass an already
// cancelled process context, so timeout remains the authoritative drain
// budget and preserves the opportunity to flush during teardown.
//
// timeout is the TOTAL time-to-CLOSED backstop, not the subscription-drain
// budget. The connect-time nats.DrainTimeout option caps only the
// subscription-drain phase; nats.go then runs an unconditional 5s publish flush
// before closing, so the true time-to-CLOSED is up to that flush longer than
// nats.DrainTimeout. timeout must exceed the connection's nats.DrainTimeout by
// at least the publish-flush budget or it will fire mid-drain and drop pending
// acks — [ShutdownGroup] sizes it automatically. timeout bounds how long we
// block; it is not a guarantee the drain finished. Once the consumer loops have
// joined (see [ShutdownGroup]) there is nothing left to drain, so on a reachable
// server this returns in milliseconds; the backstop only bites when the server
// is unreachable.
//
// Unlike a raw nc.Drain, this does not touch the connection's ClosedHandler, so
// a handler installed by [Connect] or by the caller (via [WithNATSOptions]) is
// preserved, and concurrent Drain calls on the same connection do not clobber
// one another.
//
// Drain is a no-op on a nil or already-closed connection. It returns nil only
// after the connection reaches CLOSED; immediate drain failures and timeouts
// are returned after force-closing the connection.
func Drain(ctx context.Context, nc *nats.Conn, name string, logger *slog.Logger, timeout time.Duration) error {
	if logger == nil {
		logger = slog.Default()
	}
	if nc == nil || nc.IsClosed() {
		return nil
	}

	// Register for the CLOSED transition before starting the drain so it cannot
	// be missed, and via a status listener rather than SetClosedHandler so we
	// never displace a handler the connection already has.
	closed := nc.StatusChanged(nats.CLOSED)
	defer nc.RemoveStatusListener(closed)

	if err := nc.Drain(); err != nil {
		// The connection can close after the IsClosed check above but before
		// Drain takes its lock. That is the same clean no-op as entering this
		// function with an already-closed connection, not a shutdown failure.
		if errors.Is(err, nats.ErrConnectionClosed) {
			return nil
		}
		logger.WarnContext(ctx, "nuts: NATS drain failed; closing",
			slog.String("conn", name), slog.Any("error", err))
		nc.Close()
		return fmt.Errorf("nuts: drain %s: %w", name, err)
	}

	if !waitTimedOut(closed, timeout) {
		logger.InfoContext(ctx, "nuts: NATS drained", slog.String("conn", name))
		return nil
	}

	logger.WarnContext(ctx, "nuts: NATS drain timed out before close", slog.String("conn", name))
	nc.Close()
	return fmt.Errorf("%w: %s after %s", ErrDrainTimeout, name, timeout)
}
