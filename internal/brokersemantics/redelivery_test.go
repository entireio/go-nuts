package brokersemantics

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/entireio/go-nuts/backoff"
)

// TestNumDeliveredInflatesWithoutHandlerInvolvement pins what NumDelivered
// actually counts.
//
// The belief it corrects: that NumDelivered is a ladder counter the handler
// drives — one increment per failed attempt, so "delivery 7 of 7" means the
// handler tried seven times. It is not. It counts DELIVERIES, and the server
// makes one every time AckWait expires, whatever the handler did or did not do.
// This test disposes of nothing at all — the shape of a handler that blocked,
// crashed, lost its connection, or (per COR-1257's "client death") had its pod
// killed mid-flight — and the count still climbs to MaxDeliver on the server's
// clock alone.
//
// Two consequences the fake cannot show. backoff.NumDelivered and
// backoff.IsFinalDelivery read this same inflated number, so a handler can be
// handed a message that is ALREADY final on its first invocation, and with
// TermOnExhaustion set it will Term after one real attempt. And the elapsed time
// between first and last delivery is a function of AckWait, not of any ladder the
// consumer configured — so reconstructing "which attempt is this" from wall-clock
// spacing (the ENT-1535 log-timing analysis) does not work.
func TestNumDeliveredInflatesWithoutHandlerInvolvement(t *testing.T) {
	t.Parallel()
	_, js := env(t)
	const maxDeliver = 4
	cons := newConsumer(t, js, "events", jetstream.ConsumerConfig{
		Durable:       "inflater",
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       shortAckWait,
		MaxDeliver:    maxDeliver,
		FilterSubject: "events.repo",
	})
	publish(t, js, "events.repo", "never disposed")

	start := time.Now()
	r := consume(t, cons, nil) // no Ack, no Nak, no Term, no InProgress
	got := r.waitForN(t, maxDeliver)
	elapsed := time.Since(start)

	for i, d := range got[:maxDeliver] {
		if want := uint64(i + 1); d.numDelivered != want {
			t.Errorf("delivery %d reported NumDelivered=%d, want %d", i+1, d.numDelivered, want)
		}
		if d.streamSeq != 1 {
			t.Errorf("delivery %d was of stream seq %d, want 1 (the same message)", i+1, d.streamSeq)
		}
	}
	// Consumer sequence increments per delivery even though the stream sequence
	// does not: it is a delivery counter, not a message identifier.
	if got[maxDeliver-1].consumerSeq != uint64(maxDeliver) {
		t.Errorf("final consumer seq = %d, want %d", got[maxDeliver-1].consumerSeq, maxDeliver)
	}
	// The whole ladder ran on AckWait alone. Three waits for four deliveries.
	if floor := (maxDeliver - 1) * shortAckWait; elapsed < floor-gapEarlySlack {
		t.Errorf("four deliveries in %v, faster than %d AckWait expiries (%v)", elapsed, maxDeliver-1, floor)
	}

	// The library's own readers see the inflated count, so the last delivery is
	// "final" to a policy that never saw a handler failure.
	last := fetchAll(t, cons, 1)
	if len(last) != 0 {
		t.Fatalf("MaxDeliver was spent, but a further delivery arrived: %v", meta(t, last[0]))
	}
	if got := info(t, cons).Delivered.Consumer; got != uint64(maxDeliver) {
		t.Errorf("Delivered.Consumer = %d, want %d", got, maxDeliver)
	}
}

// TestRedeliveryContinuesOnRepeatedAckWaitWithoutBackOff pins that a consumer with
// no BackOff still has a ladder: AckWait, repeated, MaxDeliver times.
//
// The belief it corrects: that BackOff is what makes a consumer retry, so a
// consumer without one retries once, or not at all, or immediately. Absent BackOff
// is not an absent ladder — it is a FLAT ladder at AckWait, which is the envelope
// every jsconsumer consumer runs today (the scaffold sets AckWait and MaxDeliver
// and never sets BackOff). Sizing a stream's max_age against "the redelivery
// envelope" therefore has to count EffectiveAckWait × EffectiveMaxDeliver even
// when no ladder appears anywhere in the config.
func TestRedeliveryContinuesOnRepeatedAckWaitWithoutBackOff(t *testing.T) {
	t.Parallel()
	_, js := env(t)
	cons := newConsumer(t, js, "events", jetstream.ConsumerConfig{
		Durable:       "flat",
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       shortAckWait,
		MaxDeliver:    4,
		FilterSubject: "events.repo",
		// BackOff deliberately unset.
	})
	publish(t, js, "events.repo", "x")

	r := consume(t, cons, nil)
	got := r.waitForN(t, 4)
	if len(info(t, cons).Config.BackOff) != 0 {
		t.Fatal("the consumer under test has a BackOff ladder; it must have none")
	}
	for i, gap := range gapsBetween(got[:4]) {
		assertGap(t, waitLabel(i+2), gap, shortAckWait)
	}
}

// TestBackOffRungsAreServedBeforeDeliveryN pins the ladder arithmetic, including
// the tail repeat and its off-by-one (COR-1255).
//
// The counting rule, measured: the wait served BEFORE delivery N is
// BackOff[min(N-2, len(BackOff)-1)]. Restated in the terms a config review needs:
//
//   - MaxDeliver deliveries are separated by MaxDeliver-1 waits, so a ladder
//     serves one fewer rung than a reader counting deliveries expects. ENT-1535's
//     7 deliveries over a 6-rung ladder sum to all six rungs (17h45m) — correct
//     precisely because 7-1 == 6.
//   - Once the rungs run out the LAST one repeats for every remaining wait. A
//     3-rung ladder under MaxDeliver=7 serves rungs 0,1,2,2,2,2 — six waits, five
//     of them from an array of three, and the tail rung dominates the envelope.
//   - When MaxDeliver == len(BackOff) the final rung is never served at all: it is
//     dead configuration, and a reviewer summing the array overstates the envelope
//     by that rung.
func TestBackOffRungsAreServedBeforeDeliveryN(t *testing.T) {
	t.Parallel()
	const (
		rung0 = 150 * time.Millisecond
		rung1 = 400 * time.Millisecond
		rung2 = 700 * time.Millisecond
	)
	for _, tc := range []struct {
		name       string
		backOff    []time.Duration
		maxDeliver int
		wantGaps   []time.Duration
	}{
		{
			// The COR-1255 shape: more deliveries than rungs, so the last rung
			// tail-repeats. Five waits from a three-rung array.
			name:       "ladder shorter than MaxDeliver tail-repeats the last rung",
			backOff:    []time.Duration{rung0, rung1, rung2},
			maxDeliver: 6,
			wantGaps:   []time.Duration{rung0, rung1, rung2, rung2, rung2},
		},
		{
			// Equal: MaxDeliver-1 waits means the last rung is never served.
			name:       "ladder equal to MaxDeliver never serves its last rung",
			backOff:    []time.Duration{rung0, rung1, rung2},
			maxDeliver: 3,
			wantGaps:   []time.Duration{rung0, rung1},
		},
		{
			// A single rung is a flat ladder, indistinguishable from AckWait alone.
			name:       "single-rung ladder is flat",
			backOff:    []time.Duration{rung1},
			maxDeliver: 4,
			wantGaps:   []time.Duration{rung1, rung1, rung1},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, js := env(t)
			cons := newConsumer(t, js, "events", jetstream.ConsumerConfig{
				Durable:   "ladder",
				AckPolicy: jetstream.AckExplicitPolicy,
				// AckWait must equal BackOff[0] or the server silently
				// rewrites it; see TestAckWaitIsNormalizedToFirstBackOffRung.
				AckWait:       tc.backOff[0],
				BackOff:       tc.backOff,
				MaxDeliver:    tc.maxDeliver,
				FilterSubject: "events.repo",
			})
			publish(t, js, "events.repo", "x")

			r := consume(t, cons, nil) // no disposition: the ladder runs on the server's clock
			got := r.waitForN(t, tc.maxDeliver)
			if len(got) > tc.maxDeliver {
				t.Errorf("saw %d deliveries, want MaxDeliver=%d", len(got), tc.maxDeliver)
			}
			gaps := gapsBetween(got[:tc.maxDeliver])
			if len(gaps) != len(tc.wantGaps) {
				t.Fatalf("%d waits between %d deliveries, want %d", len(gaps), tc.maxDeliver, len(tc.wantGaps))
			}
			for i, want := range tc.wantGaps {
				assertGap(t, waitLabel(i+2), gaps[i], want)
			}
		})
	}
}

// TestBackOffGovernsAckTimeoutsNotNakDelays pins the interaction between a
// server-side BackOff ladder and the client-side NakWithDelay that backoff.Policy
// issues — the belief that cost the most review time, because the two look like
// the same mechanism and are not.
//
// BackOff does not govern Nak. It governs how long the server waits for an ack
// before redelivering. Measured, the three dispositions a handler can choose are:
//
//   - plain Nak: redeliver NOW. The ladder is skipped entirely, so a handler that
//     Naks on a transient failure retries at wire speed and burns MaxDeliver in
//     milliseconds — the opposite of what a configured ladder promises.
//   - NakWithDelay(d): redeliver after d + (BackOff[rung] - BackOff[0]). The
//     server offsets the delivery timestamp by d but still measures it against the
//     current RUNG, so the requested delay is stretched by however much the ladder
//     has grown. On a flat ladder the stretch is zero and d is honoured exactly,
//     which is why a flat-ladder test cannot see this.
//   - no disposition at all: redeliver on the rung, exactly as configured. Doing
//     nothing is what actually follows the ladder.
//
// So a growing ladder and backoff.Policy's NakWithDelay do not compose: the policy
// cannot express its own envelope on a consumer that has one. This is the
// arithmetic behind ENT-1535's unexplained retry spacing ("the first two are ~78s
// apart" against a ladder whose first rung is 5m).
func TestBackOffGovernsAckTimeoutsNotNakDelays(t *testing.T) {
	t.Parallel()
	const (
		rung0    = 200 * time.Millisecond
		rung1    = 900 * time.Millisecond
		nakDelay = 100 * time.Millisecond
	)
	// The stretch: rung1 - rung0 added to every requested delay from the second
	// wait onward.
	stretched := nakDelay + (rung1 - rung0)

	for _, tc := range []struct {
		name     string
		dispose  func(jetstream.Msg) error
		wantGaps []time.Duration
	}{
		{
			name:    "plain Nak ignores the ladder and redelivers immediately",
			dispose: func(m jetstream.Msg) error { return m.Nak() },
			// 0 marks "immediate" — asserted with assertImmediate.
			wantGaps: []time.Duration{0, 0, 0},
		},
		{
			name:    "NakWithDelay is stretched by the ladder's growth",
			dispose: func(m jetstream.Msg) error { return m.NakWithDelay(nakDelay) },
			// Before delivery 2 the consumer is still on rung 0, so there is
			// nothing to stretch; from delivery 3 the rung has grown.
			wantGaps: []time.Duration{nakDelay, stretched, stretched},
		},
		{
			name:     "no disposition follows the ladder exactly",
			dispose:  nil,
			wantGaps: []time.Duration{rung0, rung1, rung1},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, js := env(t)
			cons := newConsumer(t, js, "events", jetstream.ConsumerConfig{
				Durable:       "nakladder",
				AckPolicy:     jetstream.AckExplicitPolicy,
				AckWait:       rung0,
				BackOff:       []time.Duration{rung0, rung1},
				MaxDeliver:    4,
				FilterSubject: "events.repo",
			})
			publish(t, js, "events.repo", "x")

			var dispose func(jetstream.Msg)
			if tc.dispose != nil {
				dispose = func(m jetstream.Msg) {
					if err := tc.dispose(m); err != nil {
						t.Errorf("dispose: %v", err)
					}
				}
			}
			r := consume(t, cons, dispose)
			got := r.waitForN(t, 4)
			gaps := gapsBetween(got[:4])
			for i, want := range tc.wantGaps {
				lbl := waitLabel(i + 2)
				if want == 0 {
					assertImmediate(t, lbl, gaps[i])
					continue
				}
				assertGap(t, lbl, gaps[i], want)
			}
		})
	}
}

// TestBackoffPolicyDelayIsNotWhatTheBrokerServes is the previous test's finding
// stated as a claim about this module's own API, so the composition breaks loudly
// here rather than quietly in a service.
//
// backoff.Policy.DelayFor computes the delay the policy INTENDS. On a consumer
// with a growing server-side BackOff, the broker serves that delay plus the
// ladder's growth. Callers therefore get the policy's envelope only on a consumer
// with no BackOff (or a flat one) — which is how every jsconsumer consumer is
// configured today, and is the assumption this test exists to keep true.
func TestBackoffPolicyDelayIsNotWhatTheBrokerServes(t *testing.T) {
	t.Parallel()
	const (
		rung0 = 200 * time.Millisecond
		rung1 = 900 * time.Millisecond
	)
	policy := backoff.Policy{NakDelay: 100 * time.Millisecond, MaxDeliver: 3}

	for _, tc := range []struct {
		name    string
		backOff []time.Duration
		// wantSecondWait is the gap before delivery 3 — the first wait at which a
		// growing ladder has anything to stretch.
		wantSecondWait time.Duration
	}{
		{
			name:           "no ladder: the policy's delay is served as written",
			backOff:        nil,
			wantSecondWait: policy.NakDelay,
		},
		{
			name:           "growing ladder: the broker serves the policy's delay plus the ladder's growth",
			backOff:        []time.Duration{rung0, rung1},
			wantSecondWait: policy.NakDelay + (rung1 - rung0),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, js := env(t)
			cfg := jetstream.ConsumerConfig{
				Durable:       "policy",
				AckPolicy:     jetstream.AckExplicitPolicy,
				AckWait:       rung0,
				MaxDeliver:    policy.MaxDeliver,
				FilterSubject: "events.repo",
			}
			if tc.backOff != nil {
				cfg.BackOff = tc.backOff
			}
			cons := newConsumer(t, js, "events", cfg)
			publish(t, js, "events.repo", "x")

			r := consume(t, cons, func(m jetstream.Msg) {
				// Exactly what a consumer using the policy does on a transient
				// failure, final delivery included.
				if _, err := policy.NakOrTerm(m); err != nil {
					t.Errorf("NakOrTerm: %v", err)
				}
			})
			got := r.waitForN(t, policy.MaxDeliver)
			gaps := gapsBetween(got[:policy.MaxDeliver])

			// The policy computes the same flat delay for every delivery...
			if got := policy.DelayFor(2); got != policy.NakDelay {
				t.Fatalf("policy.DelayFor(2) = %v, want %v (flat policy)", got, policy.NakDelay)
			}
			// ...but only one of these consumers actually serves it.
			assertGap(t, "wait before delivery 2", gaps[0], policy.NakDelay)
			assertGap(t, "wait before delivery 3", gaps[1], tc.wantSecondWait)
		})
	}
}
