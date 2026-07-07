package entwine

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
)

// IsShutdownFetchErr reports whether a pull-consumer Fetch error is the benign
// result of the connection being drained/closed during shutdown rather than a
// real fault. It is only "clean" when ctx is already done — a mid-run
// connection loss (ctx still live) is a genuine error worth logging.
//
// Use it to gate the error log in a fetch loop:
//
//	msgs, err := sub.Fetch(1, nats.MaxWait(5*time.Second))
//	if err != nil {
//		if errors.Is(err, nats.ErrTimeout) || entwine.IsShutdownFetchErr(ctx, err) {
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
// completes. This installs a ClosedHandler to signal completion, starts the
// drain, and blocks until the connection closes or timeout elapses. name labels
// the connection in the logs so a wedged drain can be attributed.
//
// The connect-time nats.DrainTimeout option caps only the subscription-drain
// phase; nats.go then runs an unconditional publish flush before closing, so
// the true time-to-CLOSED can exceed that option. timeout here is an
// independent backstop that bounds how long we block — not a guarantee the
// drain finished. Once the consumer loops have joined (see [ShutdownGroup])
// there is nothing left to drain, so on a reachable server this returns in
// milliseconds; the backstop only bites when the server is unreachable.
//
// Drain is a no-op on a nil or already-closed connection.
func Drain(ctx context.Context, nc *nats.Conn, name string, timeout time.Duration) {
	if nc == nil || nc.IsClosed() {
		return
	}

	closed := make(chan struct{})
	nc.SetClosedHandler(func(*nats.Conn) { close(closed) })

	if err := nc.Drain(); err != nil {
		slog.WarnContext(ctx, "entwine: NATS drain failed; closing",
			slog.String("conn", name), slog.Any("error", err))
		nc.Close()
		return
	}

	select {
	case <-closed:
		slog.InfoContext(ctx, "entwine: NATS drained", slog.String("conn", name))
	case <-time.After(timeout):
		slog.WarnContext(ctx, "entwine: NATS drain timed out before close", slog.String("conn", name))
	}
}
