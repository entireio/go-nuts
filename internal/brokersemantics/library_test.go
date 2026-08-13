package brokersemantics

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/entireio/go-nuts/backoff"
	"github.com/entireio/go-nuts/jsconsumer"
	"github.com/entireio/go-nuts/natsmsg"
)

// TestTermOnExhaustionFiresOnTheBrokersFinalDelivery is the COR-762 claim,
// end-to-end: does backoff.Policy's idea of "final delivery" coincide with the
// broker's, and does Terming there actually stop the message orphaning?
//
// The fake can only confirm that the policy Termed when its own MsgMetadata said
// NumDelivered == MaxDeliver. Two things it cannot check are exactly what the fix
// claims. That the policy's final delivery IS the broker's last one — the counting
// has to agree, or the Term lands one delivery early (settling a message that had
// another attempt coming) or never (the failure mode EffectiveMaxDeliver exists to
// prevent). And that the outcome differs: with TermOnExhaustion the work-queue
// message leaves the stream and the floor advances, and without it the final Nak
// is dropped by the broker and the message orphans un-acked, pinning the floor
// exactly as the issue describes.
func TestTermOnExhaustionFiresOnTheBrokersFinalDelivery(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name             string
		termOnExhaustion bool
		wantOutcome      backoff.Outcome
		wantFloor        uint64
		wantMsgsInStream uint64
	}{
		{
			name:             "TermOnExhaustion settles the exhausted message",
			termOnExhaustion: true,
			wantOutcome:      backoff.OutcomeTerm,
			wantFloor:        1,
			wantMsgsInStream: 0,
		},
		{
			name:             "without it the final Nak is dropped and the message orphans",
			termOnExhaustion: false,
			wantOutcome:      backoff.OutcomeNak,
			wantFloor:        0,
			wantMsgsInStream: 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			nc, _ := connect(t, startServer(t))
			js := jsHandle(t, nc)
			// WorkQueue retention: the stream where an un-acked message orphans,
			// which is the case COR-762 was lifted from.
			st := newStream(t, js, jetstream.StreamConfig{
				Name: "jobs", Subjects: []string{"jobs.>"}, Retention: jetstream.WorkQueuePolicy,
			})
			// Sized by the scaffold's own bridge: the policy must be told the same
			// MaxDeliver the consumer got.
			cfg := jsconsumer.Config{
				Stream:        "jobs",
				Durable:       "worker",
				FilterSubject: "jobs.run",
				Name:          "worker",
				SpanName:      "test.consume",
				AckWait:       shortAckWait,
				MaxDeliver:    3,
			}
			policy := backoff.Policy{
				NakDelay:         20 * time.Millisecond,
				MaxDeliver:       cfg.EffectiveMaxDeliver(),
				TermOnExhaustion: tc.termOnExhaustion,
			}

			var (
				outcomes   = make(chan backoff.Outcome, 8)
				finalSeen  atomic.Int64
				deliveries atomic.Int64
			)
			run, err := jsconsumer.Start(t.Context(), nc, cfg, func(m jetstream.Msg) {
				deliveries.Add(1)
				if backoff.IsFinalDelivery(m, policy.MaxDeliver) {
					finalSeen.Store(int64(backoff.NumDelivered(m)))
				}
				outcome, err := policy.NakOrTerm(m)
				if err != nil {
					t.Errorf("NakOrTerm: %v", err)
				}
				outcomes <- outcome
			})
			if err != nil {
				t.Fatalf("Start: %v", err)
			}
			defer run.Stop()

			publish(t, js, "jobs.run", "poison")

			// Exactly MaxDeliver deliveries, the last one final.
			var got []backoff.Outcome
			deadline := time.After(waitTimeout)
			for len(got) < cfg.EffectiveMaxDeliver() {
				select {
				case o := <-outcomes:
					got = append(got, o)
				case <-deadline:
					t.Fatalf("saw %d dispositions (%v), want %d", len(got), got, cfg.EffectiveMaxDeliver())
				}
			}
			if last := got[len(got)-1]; last != tc.wantOutcome {
				t.Errorf("disposition on the final delivery = %q, want %q", last, tc.wantOutcome)
			}
			for i, o := range got[:len(got)-1] {
				if o != backoff.OutcomeNak {
					t.Errorf("disposition on delivery %d = %q, want %q", i+1, o, backoff.OutcomeNak)
				}
			}
			// The policy's "final" must be the broker's last delivery — not one
			// early, not never.
			if want := int64(cfg.EffectiveMaxDeliver()); finalSeen.Load() != want {
				t.Errorf("IsFinalDelivery fired at NumDelivered=%d, want the broker's last delivery %d",
					finalSeen.Load(), want)
			}
			time.Sleep(shortAckWait + settleWait)
			if extra := deliveries.Load(); extra != int64(cfg.EffectiveMaxDeliver()) {
				t.Errorf("handler ran %d times, want MaxDeliver=%d", extra, cfg.EffectiveMaxDeliver())
			}

			cons, err := js.Consumer(t.Context(), "jobs", "worker")
			if err != nil {
				t.Fatalf("look up consumer: %v", err)
			}
			if got := info(t, cons).AckFloor.Stream; got != tc.wantFloor {
				t.Errorf("ack floor = %d, want %d", got, tc.wantFloor)
			}
			si, err := st.Info(t.Context())
			if err != nil {
				t.Fatalf("stream info: %v", err)
			}
			if si.State.Msgs != tc.wantMsgsInStream {
				t.Errorf("work-queue stream holds %d messages, want %d", si.State.Msgs, tc.wantMsgsInStream)
			}
		})
	}
}

// TestKeepInProgressExtendsAckWaitThenTheCapReclaimsIt pins both halves of the
// heartbeat's contract against the broker that enforces it.
//
// natsmsg.KeepInProgress promises that a long handler is not redelivered
// underneath itself, and — deliberately — that the extension is CAPPED, so a
// wedged handler (hung I/O with no deadline) still loses its delivery to a
// healthy replica instead of pinning it forever. Only a real broker can show
// either: the fake counts InProgress calls, and a counter cannot prove the
// server's redelivery timer moved.
//
// Nothing here calls the returned stop. That is the point: stopping the heartbeat
// by hand tests the caller's own cleanup, and a test that does so stays green even
// if the cap is deleted outright — the redelivery it observes would be explained
// by the stop. The heartbeat is left running against a message the handler never
// disposes of, which is exactly the wedged shape, so the ONLY thing that can
// release the delivery is the cap firing on its own.
//
// The handler starts the heartbeat and returns immediately rather than blocking:
// nats.go dispatches consume callbacks serially, so a handler that slept would
// block the very redelivery this test waits for. The extension does not depend on
// the handler still running — InProgress moves the server's timer, not a
// client-side lease.
//
// Timing, from natsmsg's own constants: ticks fire at AckWait/3 and stop after
// keepInProgressMaxTicks (15), so the extension ends at 5×AckWait and the broker
// reclaims one AckWait later, at ~6×AckWait. The cap is unexported, so the test
// states the arithmetic instead of importing it, and asserts a band wide enough to
// survive tick jitter yet far too tight for an uncapped heartbeat (which would
// never redeliver at all) or a materially different cap.
func TestKeepInProgressExtendsAckWaitThenTheCapReclaimsIt(t *testing.T) {
	t.Parallel()
	_, js := env(t)
	cons := newConsumer(t, js, "events", jetstream.ConsumerConfig{
		Durable:       "heartbeat",
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       shortAckWait,
		MaxDeliver:    5,
		FilterSubject: "events.repo",
	})
	publish(t, js, "events.repo", "slow work")

	const (
		// 15 ticks at AckWait/3.
		capReachedAt = 5 * shortAckWait
		// One more AckWait after the last extension.
		reclaimedAt = capReachedAt + shortAckWait
		// Comfortably inside the cap, comfortably past a bare AckWait.
		duringExtension = 3 * shortAckWait
	)
	var handlerRuns atomic.Int64
	r := consume(t, cons, func(m jetstream.Msg) {
		if handlerRuns.Add(1) > 1 {
			return // the redelivery; leave it alone
		}
		// No stop, ever: the heartbeat must expire on its own cap. It ends itself
		// after 15 ticks even if this test fails early.
		_ = natsmsg.KeepInProgress(m.InProgress, shortAckWait)
	})
	first := r.waitForN(t, 1)

	// Phase 1 — the extension holds. Well past AckWait, still one delivery.
	time.Sleep(duringExtension)
	if got := r.snapshot(); len(got) != 1 {
		t.Fatalf("saw %v after %v of heartbeating, want the single first delivery: the extension is not holding",
			got, duringExtension)
	}
	if ci := info(t, cons); ci.NumRedelivered != 0 {
		t.Errorf("NumRedelivered = %d while the heartbeat was extending AckWait, want 0", ci.NumRedelivered)
	}

	// Phase 2 — the cap releases it, with no help from the test.
	got := r.waitForN(t, 2)
	if got[1].numDelivered != 2 {
		t.Errorf("second delivery reported NumDelivered=%d, want 2", got[1].numDelivered)
	}
	held := got[1].at.Sub(first[0].at)
	if held < capReachedAt-gapEarlySlack {
		t.Errorf("redelivery came %v after the first delivery, want at least the %v the cap should have held it "+
			"(a shorter hold means the heartbeat stopped early)", held.Round(time.Millisecond), capReachedAt)
	}
	if high := reclaimedAt + 2*shortAckWait + gapLateSlack; held > high {
		t.Errorf("redelivery came %v after the first delivery, want ~%v (cap at %v plus one AckWait); "+
			"a longer hold means the extension outlived its cap", held.Round(time.Millisecond), reclaimedAt, capReachedAt)
	}
}

// TestConsumerSurvivesRecreationWithSameDurable pins the create-or-update path
// jsconsumer.Start uses on every process start, against the broker.
//
// Start calls CreateOrUpdateConsumer with the same durable each boot, and the
// scaffold's promise is that this RESUMES: the durable keeps its ack floor and its
// delivery cursor, so a rollout does not replay the stream. The contrast is
// jsconsumer's own documented hazard — a durable that is deleted and recreated
// starts from the stream's DeliverPolicy (all, by default) and replays everything,
// which is the ENT-1492 note about recreating a consumer costing ~192,000
// redeliveries. Both halves are asserted here because the difference is invisible
// without a broker holding the state.
func TestConsumerSurvivesRecreationWithSameDurable(t *testing.T) {
	t.Parallel()
	nc, js := env(t)
	cfg := jsconsumer.Config{
		Stream:        "events",
		Durable:       "resumer",
		FilterSubject: "events.repo",
		Name:          "resumer",
		SpanName:      "test.consume",
		AckWait:       time.Minute,
	}
	// Each call is one process lifetime: Start, and hand back the runner so the
	// caller stops it before the next one begins. Overlapping loops on a shared
	// durable would race for the same deliveries and prove nothing about resume.
	deliver := func() (<-chan string, *jsconsumer.Runner) {
		got := make(chan string, 16)
		run, err := jsconsumer.Start(t.Context(), nc, cfg, func(m jetstream.Msg) {
			if err := m.Ack(); err != nil {
				t.Errorf("ack: %v", err)
			}
			got <- string(m.Data())
		})
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		t.Cleanup(run.Stop) // idempotent, so an explicit Stop below is safe
		return got, run
	}
	nextDelivery := func(t *testing.T, ch <-chan string, what string) string {
		t.Helper()
		select {
		case msg := <-ch:
			return msg
		case <-time.After(waitTimeout):
			t.Fatalf("%s: no delivery", what)
			return ""
		}
	}

	first, firstRun := deliver()
	publish(t, js, "events.repo", "before restart")
	if msg := nextDelivery(t, first, "first boot"); msg != "before restart" {
		t.Fatalf("first delivery = %q, want %q", msg, "before restart")
	}
	firstRun.Stop()

	// Restart: same durable, same config, so CreateOrUpdateConsumer must resume.
	// The already-acked message must not come back.
	second, secondRun := deliver()
	publish(t, js, "events.repo", "after restart")
	if msg := nextDelivery(t, second, "after restart"); msg != "after restart" {
		t.Errorf("after a restart on the same durable the consumer delivered %q; "+
			"CreateOrUpdateConsumer must resume the durable's cursor, not reset it", msg)
	}
	secondRun.Stop()

	// The documented hazard, for contrast: a durable that is DELETED and recreated
	// has no cursor left, so it replays from the stream's DeliverPolicy (all).
	if err := js.DeleteConsumer(t.Context(), "events", cfg.Durable); err != nil {
		t.Fatalf("delete consumer: %v", err)
	}
	third, _ := deliver()
	if msg := nextDelivery(t, third, "after deleting the durable"); msg != "before restart" {
		t.Errorf("recreated durable delivered %q first, want the stream's first message %q "+
			"(a deleted durable replays from DeliverPolicy)", msg, "before restart")
	}
}
