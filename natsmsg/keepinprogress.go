package natsmsg

import (
	"sync"
	"time"
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

// KeepInProgress calls inProgress at ackWait/3 until stop is called, resetting
// the broker's redelivery timer while a long handler runs. The extension is
// capped (keepInProgressMaxTicks) — see the constant for why.
//
// inProgress wraps the message's InProgress call — func() error { return
// msg.InProgress() }. The func() error callback keeps this helper decoupled
// from the jetstream package.
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
				if err := inProgress(); err != nil {
					// A delivery that can't be extended (no JetStream
					// reply subject, or already terminally disposed)
					// won't start succeeding on a later tick; stop
					// instead of spinning. The real failure surfaces
					// on the terminal Ack/Nak/Term.
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
