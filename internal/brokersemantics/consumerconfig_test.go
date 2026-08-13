package brokersemantics

import (
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/entireio/go-nuts/backoff"
	"github.com/entireio/go-nuts/jsconsumer"
)

// TestNonPositiveMaxDeliverMeansUnlimited pins the zero-value semantics both this
// module's packages document, from the only authority on them.
//
// A consumer created with MaxDeliver unset does not get "one delivery" or a server
// default cap: the server stores -1, unlimited redeliveries. That is the semantic
// backoff.Policy.MaxDeliver mirrors (non-positive means no delivery is ever final,
// so NakOrTerm always Naks), and the reason jsconsumer.Config.MaxDeliver reads 0 as
// DefaultMaxDeliver instead of passing it through: an unset field would otherwise
// silently become an unbounded retry ladder. Bridging the two conventions with
// EffectiveMaxDeliver() is what keeps TermOnExhaustion able to fire (COR-762).
//
// It also pins that unlimited is a WORKING configuration, not a rejected one — a
// MaxDeliver=-1 consumer starts and delivers — since "non-positive must be
// invalid" was itself one of the review's wrong beliefs.
func TestNonPositiveMaxDeliverMeansUnlimited(t *testing.T) {
	t.Parallel()
	nc, js := env(t)

	for _, tc := range []struct {
		name    string
		durable string
		sent    int
		wantOnS int
	}{
		{"unset becomes unlimited", "md_unset", 0, -1},
		{"explicit -1 stays unlimited", "md_minus_one", -1, -1},
		{"positive passes through", "md_positive", 3, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cons := newConsumer(t, js, "events", jetstream.ConsumerConfig{
				Durable:       tc.durable,
				AckPolicy:     jetstream.AckExplicitPolicy,
				MaxDeliver:    tc.sent,
				FilterSubject: "events.repo",
			})
			if got := info(t, cons).Config.MaxDeliver; got != tc.wantOnS {
				t.Errorf("sent MaxDeliver=%d, server stored %d, want %d", tc.sent, got, tc.wantOnS)
			}
		})
	}

	// An unlimited consumer must actually run: this is the jsconsumer path a
	// caller takes with MaxDeliver: -1 (UnlimitedMaxDeliver on the policy side).
	t.Run("an unlimited consumer starts and delivers", func(t *testing.T) {
		t.Parallel()
		cfg := jsconsumer.Config{
			Stream:        "events",
			Durable:       "unlimited_durable",
			FilterSubject: "events.repo",
			Name:          "unlimited",
			SpanName:      "test.consume",
			MaxDeliver:    backoff.UnlimitedMaxDeliver,
		}
		if got := cfg.EffectiveMaxDeliver(); got != backoff.UnlimitedMaxDeliver {
			t.Fatalf("EffectiveMaxDeliver() = %d, want %d passed through unchanged", got, backoff.UnlimitedMaxDeliver)
		}
		got := make(chan struct{}, 1)
		run, err := jsconsumer.Start(t.Context(), nc, cfg, func(m jetstream.Msg) {
			// No delivery is ever final under an unlimited cap, so a policy built
			// from this Config would Nak forever rather than Term.
			if backoff.IsFinalDelivery(m, cfg.EffectiveMaxDeliver()) {
				t.Errorf("IsFinalDelivery reported final on delivery %d of an unlimited consumer",
					backoff.NumDelivered(m))
			}
			if err := m.Ack(); err != nil {
				t.Errorf("ack: %v", err)
			}
			select {
			case got <- struct{}{}:
			default:
			}
		})
		if err != nil {
			t.Fatalf("Start with unlimited MaxDeliver: %v", err)
		}
		defer run.Stop()
		publish(t, js, "events.repo", "hello")
		select {
		case <-got:
		case <-time.After(waitTimeout):
			t.Fatal("an unlimited-MaxDeliver consumer never delivered")
		}
	})
}

// TestJSConsumerDefaultsLandOnTheServer pins that the scaffold's documented
// defaults are the values the broker actually enforces.
//
// jsconsumer.Config's zero AckWait and MaxDeliver resolve to DefaultAckWait and
// DefaultMaxDeliver, and the doc for EffectiveMaxDeliver rests on the resolution
// happening BEFORE the config reaches the server — because an unresolved zero is
// unlimited there (see TestNonPositiveMaxDeliverMeansUnlimited), not 8. The fake
// cannot distinguish those two outcomes; the on-server config can.
func TestJSConsumerDefaultsLandOnTheServer(t *testing.T) {
	t.Parallel()
	nc, js := env(t)
	cfg := jsconsumer.Config{
		Stream:        "events",
		Durable:       "defaults_durable",
		FilterSubject: "events.repo",
		Name:          "defaults",
		SpanName:      "test.consume",
		// AckWait and MaxDeliver deliberately unset.
	}
	run, err := jsconsumer.Start(t.Context(), nc, cfg, func(jetstream.Msg) {})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer run.Stop()

	cons, err := js.Consumer(t.Context(), "events", "defaults_durable")
	if err != nil {
		t.Fatalf("look up consumer: %v", err)
	}
	got := info(t, cons).Config
	if got.AckWait != jsconsumer.DefaultAckWait {
		t.Errorf("on-server AckWait = %v, want DefaultAckWait (%v)", got.AckWait, jsconsumer.DefaultAckWait)
	}
	if got.MaxDeliver != jsconsumer.DefaultMaxDeliver {
		t.Errorf("on-server MaxDeliver = %d, want DefaultMaxDeliver (%d) — a zero would have become unlimited",
			got.MaxDeliver, jsconsumer.DefaultMaxDeliver)
	}
	if got.AckPolicy != jetstream.AckExplicitPolicy {
		t.Errorf("on-server AckPolicy = %v, want explicit", got.AckPolicy)
	}
	// A policy built from the same Config agrees with the server about which
	// delivery is final — the cross-package bridge, checked against the broker
	// rather than against a fake's metadata.
	if eff := cfg.EffectiveMaxDeliver(); eff != got.MaxDeliver {
		t.Errorf("EffectiveMaxDeliver() = %d but the server enforces %d", eff, got.MaxDeliver)
	}
}

// TestAckWaitIsNormalizedToFirstBackOffRung pins what happens to a consumer whose
// AckWait disagrees with its ladder's first rung — and that the answer depends on
// which client wrote it.
//
// On the ordinary client path (this module, nats.go's jetstream package) the server
// SILENTLY REWRITES AckWait to BackOff[0]. Nothing is rejected and nothing is
// logged, so a config review that reads AckWait from the manifest is reading a
// value the broker discarded — the ladder's first rung is the real ack timeout.
//
// The same mismatch IS rejected when the request sets pedantic mode, which NACK's
// controller does via jsm.go. So a fleet Consumer CR carrying this mismatch fails
// to reconcile in the cluster while the identical config applied from here succeeds
// quietly: a discrepancy worth pinning on both paths, since only one of them is
// exercised by the rest of this suite.
func TestAckWaitIsNormalizedToFirstBackOffRung(t *testing.T) {
	t.Parallel()
	nc, js := env(t)
	ladder := []time.Duration{time.Minute, 2 * time.Minute}

	t.Run("ordinary client path rewrites it", func(t *testing.T) {
		t.Parallel()
		cons := newConsumer(t, js, "events", jetstream.ConsumerConfig{
			Durable:       "normalized",
			AckPolicy:     jetstream.AckExplicitPolicy,
			AckWait:       30 * time.Second, // disagrees with ladder[0]
			BackOff:       ladder,
			MaxDeliver:    5,
			FilterSubject: "events.repo",
		})
		got := info(t, cons).Config.AckWait
		if got != ladder[0] {
			t.Errorf("on-server AckWait = %v, want it silently rewritten to BackOff[0] (%v)", got, ladder[0])
		}
	})

	t.Run("pedantic path rejects it", func(t *testing.T) {
		t.Parallel()
		apiErr := pedanticCreateConsumer(t, nc, "events", jetstream.ConsumerConfig{
			Durable:       "pedantic_mismatch",
			AckPolicy:     jetstream.AckExplicitPolicy,
			AckWait:       30 * time.Second,
			BackOff:       ladder,
			MaxDeliver:    5,
			FilterSubject: "events.repo",
		})
		if apiErr == nil {
			t.Fatal("pedantic create accepted AckWait != BackOff[0], want a rejection")
		}
		if apiErr.ErrorCode != errCodePedantic {
			t.Errorf("pedantic rejection err_code = %d, want %d (pedantic mode)", apiErr.ErrorCode, errCodePedantic)
		}
	})

	t.Run("pedantic path accepts a matching AckWait", func(t *testing.T) {
		t.Parallel()
		apiErr := pedanticCreateConsumer(t, nc, "events", jetstream.ConsumerConfig{
			Durable:       "pedantic_match",
			AckPolicy:     jetstream.AckExplicitPolicy,
			AckWait:       ladder[0],
			BackOff:       ladder,
			MaxDeliver:    5,
			FilterSubject: "events.repo",
		})
		if apiErr != nil {
			t.Errorf("pedantic create with AckWait == BackOff[0] failed: %v", apiErr)
		}
	})
}

// TestBackOffLongerThanMaxDeliverIsRejected pins the one ladder/MaxDeliver
// relationship the server enforces, and the boundary its own error message gets
// wrong.
//
// A ladder with MORE rungs than MaxDeliver is rejected outright (not just in
// pedantic mode). A ladder with EXACTLY MaxDeliver rungs is accepted — even though
// the server's error text says "max deliver is required to be > length of backoff
// values", the check is `len(BackOff) > MaxDeliver`. Both facts matter to an
// admission-control lint (COR-1255): rejecting the equal case would block a legal
// config, and accepting a longer one would let an unreachable rung ship. The equal
// case is legal but suspicious for a different reason — that last rung is never
// served (see TestBackOffRungsAreServedBeforeDeliveryN).
//
// A ladder alongside unlimited MaxDeliver is also accepted, which is the
// combination that repeats the last rung forever.
func TestBackOffLongerThanMaxDeliverIsRejected(t *testing.T) {
	t.Parallel()
	_, js := env(t)
	rungs := []time.Duration{time.Minute, 2 * time.Minute, 3 * time.Minute}

	for _, tc := range []struct {
		name       string
		durable    string
		backOff    []time.Duration
		maxDeliver int
		wantErr    bool
	}{
		{"more rungs than MaxDeliver is rejected", "lb_more", rungs, 2, true},
		{"exactly MaxDeliver rungs is accepted", "lb_equal", rungs, 3, false},
		{"fewer rungs than MaxDeliver is accepted", "lb_fewer", rungs, 4, false},
		{"a ladder with unlimited MaxDeliver is accepted", "lb_unlimited", rungs, -1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := js.CreateOrUpdateConsumer(t.Context(), "events", jetstream.ConsumerConfig{
				Durable:       tc.durable,
				AckPolicy:     jetstream.AckExplicitPolicy,
				AckWait:       rungs[0],
				BackOff:       tc.backOff,
				MaxDeliver:    tc.maxDeliver,
				FilterSubject: "events.repo",
			})
			if tc.wantErr {
				if err == nil {
					t.Fatalf("len(BackOff)=%d with MaxDeliver=%d was accepted, want a rejection", len(tc.backOff), tc.maxDeliver)
				}
				var apiErr *jetstream.APIError
				if !errors.As(err, &apiErr) {
					t.Fatalf("rejection was not a JetStream API error: %v", err)
				}
				if apiErr.ErrorCode != errCodeMaxDeliverBackoff {
					t.Errorf("rejection err_code = %d, want %d (max deliver vs backoff)", apiErr.ErrorCode, errCodeMaxDeliverBackoff)
				}
				return
			}
			if err != nil {
				t.Errorf("len(BackOff)=%d with MaxDeliver=%d was rejected: %v", len(tc.backOff), tc.maxDeliver, err)
			}
		})
	}
}

// TestStreamConsumerLimitsAreInheritedBySilentConsumers pins the stream-level
// defaults that land on a consumer which said nothing.
//
// A stream's ConsumerLimits fill in a consumer's zero MaxAckPending and
// InactiveThreshold. jsconsumer leaves both zero unless the caller sets them —
// documented as "zero uses the server default (1000)" and "zero means never" — and
// on a stream carrying ConsumerLimits neither of those is what happens: the
// STREAM's values are applied instead. A consumer on such a stream can therefore be
// given an InactiveThreshold it never asked for, which deletes the durable after a
// quiet period (and, per jsconsumer's own doc, a deleted durable replays from the
// stream's DeliverPolicy on recreation).
//
// In pedantic mode the same inheritance is a rejection rather than a silent
// default, so a fleet Consumer CR must state both values explicitly on a
// limits-carrying stream.
func TestStreamConsumerLimitsAreInheritedBySilentConsumers(t *testing.T) {
	t.Parallel()
	nc, _ := connect(t, startServer(t))
	js := jsHandle(t, nc)
	const (
		limitMaxAckPending     = 7
		limitInactiveThreshold = time.Hour
	)
	newStream(t, js, jetstream.StreamConfig{
		Name:     "limited",
		Subjects: []string{"limited.>"},
		ConsumerLimits: jetstream.StreamConsumerLimits{
			MaxAckPending:     limitMaxAckPending,
			InactiveThreshold: limitInactiveThreshold,
		},
	})

	t.Run("a silent consumer inherits the stream's limits", func(t *testing.T) {
		t.Parallel()
		cons := newConsumer(t, js, "limited", jetstream.ConsumerConfig{
			Durable:   "silent",
			AckPolicy: jetstream.AckExplicitPolicy,
			// MaxAckPending and InactiveThreshold deliberately unset, exactly as
			// jsconsumer leaves them.
		})
		got := info(t, cons).Config
		if got.MaxAckPending != limitMaxAckPending {
			t.Errorf("on-server MaxAckPending = %d, want the stream's %d (not the 1000 server default)",
				got.MaxAckPending, limitMaxAckPending)
		}
		if got.InactiveThreshold != limitInactiveThreshold {
			t.Errorf("on-server InactiveThreshold = %v, want the stream's %v (not never)",
				got.InactiveThreshold, limitInactiveThreshold)
		}
	})

	t.Run("jsconsumer's zero fields inherit them too", func(t *testing.T) {
		t.Parallel()
		run, err := jsconsumer.Start(t.Context(), nc, jsconsumer.Config{
			Stream:        "limited",
			Durable:       "scaffold_silent",
			FilterSubject: "limited.repo",
			Name:          "silent",
			SpanName:      "test.consume",
		}, func(jetstream.Msg) {})
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		defer run.Stop()
		cons, err := js.Consumer(t.Context(), "limited", "scaffold_silent")
		if err != nil {
			t.Fatalf("look up consumer: %v", err)
		}
		got := info(t, cons).Config
		if got.MaxAckPending != limitMaxAckPending || got.InactiveThreshold != limitInactiveThreshold {
			t.Errorf("scaffold consumer got MaxAckPending=%d InactiveThreshold=%v, want the stream's %d/%v",
				got.MaxAckPending, got.InactiveThreshold, limitMaxAckPending, limitInactiveThreshold)
		}
	})

	t.Run("pedantic mode rejects the silence instead of defaulting", func(t *testing.T) {
		t.Parallel()
		apiErr := pedanticCreateConsumer(t, nc, "limited", jetstream.ConsumerConfig{
			Durable:   "pedantic_silent",
			AckPolicy: jetstream.AckExplicitPolicy,
		})
		if apiErr == nil {
			t.Fatal("pedantic create accepted a consumer that left the stream's limits unstated, want a rejection")
		}
		if apiErr.ErrorCode != errCodePedantic {
			t.Errorf("rejection err_code = %d, want %d (pedantic mode)", apiErr.ErrorCode, errCodePedantic)
		}
	})
}
