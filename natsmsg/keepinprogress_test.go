package natsmsg

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// noopInProgress and disposedInProgress stand in for the message's InProgress
// call that consumers pass to KeepInProgress (func() error { return
// msg.InProgress() }): one keeps the heartbeat ticking, one drives the
// permanent-error exit path (the delivery was already terminally disposed).
func noopInProgress() error     { return nil }
func disposedInProgress() error { return jetstream.ErrMsgAlreadyAckd }

// TestKeepInProgress_StopIsIdempotentAndTerminates smoke-tests the long-handler
// keepalive: with a succeeding callback the ticker goroutine keeps running, and
// stop must terminate that live goroutine, be safe to call twice, and block
// until it has exited — the guarantee consumers lean on so no InProgress can
// race a terminal Ack/Nak/Term.
func TestKeepInProgress_StopIsIdempotentAndTerminates(t *testing.T) {
	stop := KeepInProgress(noopInProgress, 30*time.Millisecond)
	time.Sleep(50 * time.Millisecond) // spans at least one tick (10ms interval)

	done := make(chan struct{})
	go func() {
		stop()
		stop() // idempotent: an explicit call and a deferred one both run
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("stop did not terminate the heartbeat goroutine")
	}
}

// TestKeepInProgress_NonPositiveIntervalIsNoop pins the guard for a zero
// ackWait (possible only on a hand-built consumer config; real consumer
// configs default it): no goroutine, and stop returns immediately.
func TestKeepInProgress_NonPositiveIntervalIsNoop(t *testing.T) {
	stop := KeepInProgress(noopInProgress, 0)
	done := make(chan struct{})
	go func() {
		stop()
		stop() // double-stop must be as safe as on the ticking path
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("no-op stop did not return")
	}
}

// TestKeepInProgress_CapTerminatesTheGoroutine pins the wedged-handler failsafe:
// even with a callback that keeps succeeding, after keepInProgressMaxTicks the
// goroutine exits on its own — without stop being called — so AckWait expiry can
// eventually redeliver a delivery whose handler never returns. Observed via stop
// returning promptly long after the cap has passed.
func TestKeepInProgress_CapTerminatesTheGoroutine(t *testing.T) {
	stop := KeepInProgress(noopInProgress, 3*time.Millisecond) // interval 1ms → cap at ~15ms
	time.Sleep(time.Duration(keepInProgressMaxTicks+5) * time.Millisecond)

	done := make(chan struct{})
	go func() {
		stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("heartbeat goroutine still alive after the cap")
	}
}

// TestKeepInProgress_PermanentErrorStopsTheGoroutine pins the handled-error
// path: a callback that can never extend the delivery again (already
// terminally disposed, no reply subject) exits the goroutine rather than
// spinning. Observed via stop returning promptly even though the goroutine
// has already exited on its own.
func TestKeepInProgress_PermanentErrorStopsTheGoroutine(t *testing.T) {
	stop := KeepInProgress(disposedInProgress, 3*time.Millisecond)
	time.Sleep(20 * time.Millisecond) // several intervals; the first tick already errored out

	done := make(chan struct{})
	go func() {
		stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("goroutine did not exit after the callback returned a permanent error")
	}
}

// TestKeepInProgress_TransientErrorKeepsTicking pins the recovery path: a
// failure that can heal (a reconnect-buffer overflow during a NATS blip) must
// not permanently disable the heartbeat — the goroutine keeps ticking (bounded
// by the cap) so the extension resumes once the connection recovers. The old
// behavior exited on the first error, so the tick count would freeze at 1.
func TestKeepInProgress_TransientErrorKeepsTicking(t *testing.T) {
	var ticks atomic.Int64
	transient := func() error {
		ticks.Add(1)
		return errors.New("nats: outbound buffer limit exceeded") // transient: recovers after reconnect
	}
	stop := KeepInProgress(transient, 9*time.Millisecond) // ticks every 3ms
	defer stop()

	deadline := time.Now().Add(time.Second)
	for ticks.Load() < 3 {
		if time.Now().After(deadline) {
			t.Fatalf("heartbeat stopped after a transient error: %d ticks, want >= 3", ticks.Load())
		}
		time.Sleep(time.Millisecond)
	}
}
