package nuts

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
)

// ErrDrainTimeout is returned when a connection does not reach CLOSED within
// the total drain backstop. The connection is force-closed before return, so
// callers never inherit an ambiguous asynchronous drain.
var ErrDrainTimeout = errors.New("nuts: NATS drain timed out before close")

// IsShutdownFetchErr reports whether a pull-consumer Fetch error is the benign
// result of the connection being drained/closed during shutdown rather than a
// real fault. It is only "clean" when ctx is already done — a mid-run
// connection loss (ctx still live) is a genuine error worth logging.
//
// Use it to gate the error log in a fetch loop:
//
//	msgs, err := sub.Fetch(1, nats.MaxWait(5*time.Second))
//	if err != nil {
//		if errors.Is(err, nats.ErrTimeout) || nuts.IsShutdownFetchErr(ctx, err) {
//			// benign: idle poll, or a drain/close during shutdown
//		}
//		...
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
