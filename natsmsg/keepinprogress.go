package natsmsg

import (
	"errors"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// keepInProgressMaxTicks caps the total AckWait extension at ~5×AckWait
// (ticks fire at AckWait/3). The heartbeat exists so a long handler that IS
// making progress — a whole-installation teardown fan-out, a multi-GB
// data-plane purge — doesn't get redelivered into a concurrent duplicate;
// it must NOT remove the broker's liveness failsafe entirely, or a wedged
// handler (hung I/O with no deadline) would pin its delivery to the wedged
// replica forever. Past the cap the extension stops and AckWait expiry
// redelivers to a healthy replica sharing the durable.
const keepInProgressMaxTicks = 15

// isPermanentInProgressErr reports whether an InProgress failure can never
// succeed on a later tick: the delivery was already terminally disposed (an
// Ack/Nak/Term racing the heartbeat) or carries no JetStream reply subject to
// extend. Both the jetstream and legacy *nats.Msg sentinel values are
// matched, since the callback may wrap either message type. Anything else —
// a reconnect-buffer overflow, a flush timeout — can recover once the
// connection does, so the heartbeat keeps trying until the tick cap.
func isPermanentInProgressErr(err error) bool {
	return errors.Is(err, jetstream.ErrMsgAlreadyAckd) ||
		errors.Is(err, jetstream.ErrMsgNoReply) ||
		errors.Is(err, nats.ErrMsgAlreadyAckd) ||
		errors.Is(err, nats.ErrMsgNoReply)
}

// KeepInProgress calls inProgress at ackWait/3 until stop is called, resetting
// the broker's redelivery timer while a long handler runs. The extension is
// capped (keepInProgressMaxTicks) — see the constant for why.
//
// inProgress wraps the message's InProgress call — func() error { return
// msg.InProgress() } — for either jetstream.Msg or the legacy *nats.Msg.
//
// A failing tick stops the heartbeat only when the failure is permanent
// (already disposed, no reply subject — see isPermanentInProgressErr);
// a transient failure such as a NATS blip keeps ticking, bounded by the
// cap, so one dropped extension during a reconnect doesn't silently
// forfeit the rest of the handler's runway and invite a concurrent
// redelivery mid-run.
//
// A non-positive interval (zero ackWait on a hand-built consumer; real
// consumer configs default it) returns a no-op stop.
//
// stop is idempotent and blocks until the ticker goroutine has exited, so
// no InProgress can race a terminal disposition (Ack/Nak/Term) issued
// after it returns.
func KeepInProgress(inProgress func() error, ackWait time.Duration) (stop func()) {
	interval := ackWait / 3
	if interval <= 0 {
		return func() {}
	}
	done := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for ticks := 0; ; {
			select {
			case <-done:
				return
			case <-ticker.C:
				ticks++
				if ticks > keepInProgressMaxTicks {
					return
				}
				if err := inProgress(); err != nil && isPermanentInProgressErr(err) {
					// A delivery that can't be extended won't start
					// succeeding on a later tick; stop instead of
					// spinning. The real failure surfaces on the
					// terminal Ack/Nak/Term.
					return
				}
			}
		}
	}()
	var once sync.Once
	return func() {
		once.Do(func() { close(done) })
		<-finished
	}
}
