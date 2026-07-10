package natsmsg

import (
	"errors"
	"testing"
	"time"
)

// noopInProgress and failingInProgress stand in for the message's InProgress
// call that consumers pass to KeepInProgress (func() error { return
// msg.InProgress() }): one keeps the heartbeat ticking, one drives the
// handled-error exit path.
func noopInProgress() error    { return nil }
func failingInProgress() error { return errors.New("in progress failed") }

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

// TestKeepInProgress_CallbackErrorStopsTheGoroutine pins the handled-error path:
// a callback that can't extend the delivery (no reply subject, already disposed)
// exits the goroutine rather than spinning. Observed via stop returning promptly
// even though the goroutine has already exited on its own.
func TestKeepInProgress_CallbackErrorStopsTheGoroutine(t *testing.T) {
	stop := KeepInProgress(failingInProgress, 3*time.Millisecond)
	time.Sleep(20 * time.Millisecond) // several intervals; the first tick already errored out

	done := make(chan struct{})
	go func() {
		stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("goroutine did not exit after the callback errored")
	}
}
