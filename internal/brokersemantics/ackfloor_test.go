package brokersemantics

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// TestAckFloorStaysStationaryWhileLaterMessagesAck is the head-of-line case the
// ack-floor monitor exists for, and the reason the other two search signals read
// healthy through a stall (ENT-1535).
//
// One unacked message pins the floor. Everything behind it is delivered and acked
// out of order, so throughput, backlog depth and last-ack age all look normal —
// AckFloor.Last keeps moving with each out-of-order ack even though
// AckFloor.Stream does not, which is the "last_ack_seconds read 5-6s while the
// floor had not moved for 9 hours" observation, measured.
//
// The floor lands on blocker-1, not on the last sequence acked: it is the boundary
// below which everything is settled, so it can sit on a sequence this consumer
// never acked (or, on a filtered consumer, never even saw).
func TestAckFloorStaysStationaryWhileLaterMessagesAck(t *testing.T) {
	t.Parallel()
	_, js := env(t)
	cons := newConsumer(t, js, "events", jetstream.ConsumerConfig{
		Durable:       "headofline",
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       time.Minute, // no redelivery: only acks may move anything
		FilterSubject: "events.repo",
	})
	publish(t, js, "events.repo", "blocker", "two", "three", "four", "five")

	msgs := fetchAll(t, cons, 5)
	if len(msgs) != 5 {
		t.Fatalf("fetched %d messages, want 5", len(msgs))
	}
	// Ack 2..5 one at a time, checking after each that the floor has not moved
	// while its last-ack timestamp has.
	var lastAckAt time.Time
	for _, m := range msgs[1:] {
		if err := m.Ack(); err != nil {
			t.Fatalf("ack: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
		ci := info(t, cons)
		if ci.AckFloor.Stream != 0 {
			t.Fatalf("ack floor advanced to %d with stream seq 1 unacked, want 0", ci.AckFloor.Stream)
		}
		if ci.AckFloor.Last == nil {
			t.Fatal("AckFloor.Last is unset after an ack; the last-ack age this test reasons about is unavailable")
		}
		if !lastAckAt.IsZero() && !ci.AckFloor.Last.After(lastAckAt) {
			t.Errorf("AckFloor.Last did not advance on an out-of-order ack (%v then %v): a fresh last-ack age cannot rule out a pinned floor",
				lastAckAt, ci.AckFloor.Last)
		}
		lastAckAt = *ci.AckFloor.Last
	}

	ci := info(t, cons)
	if ci.NumAckPending != 1 {
		t.Errorf("NumAckPending = %d, want 1: a large delivered-vs-floor gap with ONE outstanding ack is the poison-message signature", ci.NumAckPending)
	}
	if ci.Delivered.Stream != 5 {
		t.Errorf("Delivered.Stream = %d, want 5: delivery must run past the pinned floor", ci.Delivered.Stream)
	}

	// Settling the blocker releases the whole run at once — the floor jumps past
	// every sequence already acked behind it.
	if err := msgs[0].Ack(); err != nil {
		t.Fatalf("ack blocker: %v", err)
	}
	time.Sleep(settleWait)
	if got := info(t, cons).AckFloor.Stream; got != 5 {
		t.Errorf("ack floor = %d after settling the blocker, want 5", got)
	}
}

// TestBlockerIsNotDerivableFromConsumerInfo pins why neither AckFloor sequence
// identifies the blocking message, on the consumer shape that actually ships: a
// FILTERED one (every jsconsumer consumer sets FilterSubject or FilterSubjects).
//
// AckFloor.Stream+1 is the number the runbook and any floor-age breaker want to
// read, and it is right only by luck. The floor is the boundary below which
// everything is settled, and a stream sequence this consumer does not match is not
// settled until an ack of a MATCHING message below the blocker carries the floor
// over it. So:
//
//   - With non-matching sequences below the blocker and nothing acked beneath them,
//     the floor sits under those, and floor+1 names a message the consumer never
//     receives — an innocent sequence that a break-glass step would delete.
//   - Once a matching message below the blocker acks, the floor jumps over the
//     non-matching ones and floor+1 does name the blocker.
//
// The identity therefore depends on acks unrelated to the blocker, and (see
// TestAckFloorAdvancesOnStreamSideRemoval) stops holding entirely after anything
// is removed from the stream. AckFloor.Consumer+1 is no help either: it is a
// DELIVERY counter pinned to the blocker's FIRST delivery, which goes stale on
// every redelivery and names nothing that can be fetched or deleted.
//
// What a responder can rely on is the message itself: read the candidate sequence
// and check its subject against the consumer's filter before acting on it.
func TestBlockerIsNotDerivableFromConsumerInfo(t *testing.T) {
	t.Parallel()
	_, js := env(t)
	st, err := js.Stream(t.Context(), "events")
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	cons := newConsumer(t, js, "events", jetstream.ConsumerConfig{
		Durable:       "identity",
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       shortAckWait,
		MaxDeliver:    50,
		FilterSubject: "events.repo",
	})
	// seq 1 unmatched, seq 2 matched (acked), seq 3 unmatched, seq 4 the blocker.
	publish(t, js, "events.other", "skip")
	publish(t, js, "events.repo", "early")
	publish(t, js, "events.other", "skip")
	publish(t, js, "events.repo", "blocker")

	const blockerStreamSeq = 4
	// Held back so the two floor readings are separable; the consume callback runs
	// on nats.go's goroutine, so the flag is atomic.
	var ackEarly atomic.Bool
	r := consume(t, cons, func(m jetstream.Msg) {
		if string(m.Data()) == "early" && ackEarly.Load() {
			if err := m.Ack(); err != nil {
				t.Errorf("ack: %v", err)
			}
		}
	})
	r.waitForN(t, 3) // both matching messages, and at least one redelivery
	time.Sleep(settleWait)

	// Phase 1: nothing acked below the blocker. The floor is under the unmatched
	// seq 1, so floor+1 names it — not the blocker.
	ci := info(t, cons)
	candidate := ci.AckFloor.Stream + 1
	if candidate == blockerStreamSeq {
		t.Fatalf("AckFloor.Stream+1 = %d already names the blocker before any ack below it; "+
			"the fixture no longer demonstrates the misidentification", candidate)
	}
	raw, err := st.GetMsg(t.Context(), candidate)
	if err != nil {
		t.Fatalf("read the sequence floor+1 names (%d): %v", candidate, err)
	}
	if raw.Subject == "events.repo" {
		t.Errorf("floor+1 (%d) named subject %q, want a subject outside the consumer's filter", candidate, raw.Subject)
	}
	t.Logf("phase 1: floor=%d so floor+1=%d, which is %q — the blocker is stream seq %d",
		ci.AckFloor.Stream, candidate, raw.Subject, blockerStreamSeq)

	// Phase 2: ack the matching message below the blocker. The floor now skips the
	// unmatched sequences and floor+1 does name the blocker — the identity changed
	// because of an ack that had nothing to do with the blocker.
	ackEarly.Store(true)
	deadline := time.Now().Add(waitTimeout)
	for time.Now().Before(deadline) {
		if info(t, cons).AckFloor.Stream+1 == blockerStreamSeq {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	ci = info(t, cons)
	if got := ci.AckFloor.Stream + 1; got != blockerStreamSeq {
		t.Errorf("after acking below the blocker, AckFloor.Stream+1 = %d, want the blocker's %d", got, blockerStreamSeq)
	}

	// The blocker is many deliveries in; the consumer floor is still at its first.
	var maxConsumerSeq uint64
	for _, d := range r.snapshot() {
		if d.streamSeq == blockerStreamSeq && d.consumerSeq > maxConsumerSeq {
			maxConsumerSeq = d.consumerSeq
		}
	}
	if maxConsumerSeq <= ci.AckFloor.Consumer+1 {
		t.Errorf("blocker's latest delivery (consumer seq %d) did not outrun AckFloor.Consumer+1 (%d); "+
			"the fixture needs more redeliveries to demonstrate the staleness", maxConsumerSeq, ci.AckFloor.Consumer+1)
	}
	t.Logf("phase 2: blocker at stream seq %d is on consumer seq %d, while AckFloor.Consumer+1 reads %d",
		blockerStreamSeq, maxConsumerSeq, ci.AckFloor.Consumer+1)
}

// TestAckFloorAdvancesOnStreamSideRemoval pins the floor movement that is not a
// settlement, and the one that breaks AckFloor.Stream+1 as a blocker identity.
//
// When messages leave the stream underneath a consumer — retention expiry, a
// purge, or an operator's `stream rmm` (the ENT-1535 break-glass) — the server
// drops them from the consumer's pending set and drags the ack floor up to the
// delivered high-water mark. No ack was involved and nothing was processed.
//
// Two things follow. A floor that advanced is not evidence that the messages below
// it were handled, so a floor-advance signal cannot close a poison-message
// incident. And once the floor has reached the delivered HWM, AckFloor.Stream+1
// names a sequence that has not been delivered and may not exist — exactly the
// state ENT-1535's timeline records ("floor jumps to 2,792,129") right after the
// operator deleted the blocker.
func TestAckFloorAdvancesOnStreamSideRemoval(t *testing.T) {
	t.Parallel()
	_, js := env(t)
	st, err := js.Stream(t.Context(), "events")
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	cons := newConsumer(t, js, "events", jetstream.ConsumerConfig{
		Durable:       "removal",
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       time.Minute, // no redelivery: removal is the only mover
		MaxDeliver:    20,
		FilterSubject: "events.repo",
	})
	publish(t, js, "events.repo", "one", "two", "three")

	// Deliver all three, ack none: the floor is at 0 with three pending.
	if got := len(fetchAll(t, cons, 3)); got != 3 {
		t.Fatalf("fetched %d messages, want 3", got)
	}
	if ci := info(t, cons); ci.AckFloor.Stream != 0 || ci.NumAckPending != 3 {
		t.Fatalf("before removal: floor=%d pending=%d, want floor 0 with 3 pending", ci.AckFloor.Stream, ci.NumAckPending)
	}

	// The operator removes the blocker and everything under it.
	for seq := uint64(1); seq <= 2; seq++ {
		if err := st.DeleteMsg(t.Context(), seq); err != nil {
			t.Fatalf("delete stream seq %d: %v", seq, err)
		}
	}
	time.Sleep(settleWait)

	ci := info(t, cons)
	if ci.AckFloor.Stream != 2 {
		t.Errorf("ack floor = %d after removing seqs 1-2, want 2: removal advances the floor with no ack", ci.AckFloor.Stream)
	}
	if ci.NumAckPending != 1 {
		t.Errorf("NumAckPending = %d after the removal, want 1 (only seq 3 is left)", ci.NumAckPending)
	}

	// Remove the last pending message too: now the floor sits at the delivered
	// high-water mark and floor+1 points past the end of the stream.
	if err := st.DeleteMsg(t.Context(), 3); err != nil {
		t.Fatalf("delete stream seq 3: %v", err)
	}
	time.Sleep(settleWait)
	ci = info(t, cons)
	if ci.AckFloor.Stream != ci.Delivered.Stream {
		t.Errorf("ack floor = %d, delivered = %d: want the floor dragged up to the delivered high-water mark",
			ci.AckFloor.Stream, ci.Delivered.Stream)
	}
	if _, err := st.GetMsg(t.Context(), ci.AckFloor.Stream+1); err == nil {
		t.Errorf("AckFloor.Stream+1 (%d) resolves to a real message; want it to name nothing, "+
			"which is why floor+1 cannot identify a blocker after a removal", ci.AckFloor.Stream+1)
	}
	if ci.NumAckPending != 0 {
		t.Errorf("NumAckPending = %d, want 0 once every delivered message was removed", ci.NumAckPending)
	}
}
