package backoff

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/entireio/go-nuts/natsmsg"
	"github.com/entireio/go-nuts/natsmsg/natsmsgtest"
)

const testDelay = 5 * time.Second

// The legacy redelivery entry points accept anything satisfying LegacyMsg; a
// concrete *nats.Msg and the scriptable test double both must.
var (
	_ LegacyMsg = (*nats.Msg)(nil)
	_ LegacyMsg = (*natsmsgtest.FakeLegacyMsg)(nil)
)

func delivered(n uint64) *natsmsgtest.FakeMsg {
	return &natsmsgtest.FakeMsg{Meta: &jetstream.MsgMetadata{NumDelivered: n}}
}

// TestNakOrTermNaksBeforeExhaustion: a transient failure with deliveries left
// redelivers with the flat policy delay.
func TestNakOrTermNaksBeforeExhaustion(t *testing.T) {
	p := Policy{NakDelay: testDelay, MaxDeliver: 5, TermOnExhaustion: true}
	msg := delivered(4)

	got, err := p.NakOrTerm(msg)
	if err != nil {
		t.Fatalf("NakOrTerm: %v", err)
	}
	if got != OutcomeNak {
		t.Errorf("outcome = %q, want nak", got)
	}
	if len(msg.NakDelays) != 1 || msg.NakDelays[0] != testDelay {
		t.Errorf("NakDelays = %v, want one nak with the policy delay", msg.NakDelays)
	}
	if msg.Termed {
		t.Error("termed before exhaustion")
	}
}

// TestNakOrTermTermsOnFinalDelivery pins the COR-762 fix: on the final delivery
// of a work-queue consumer (TermOnExhaustion), Term removes the message cleanly
// instead of a Nak the broker drops — which would orphan it un-acked until
// StreamMaxAge.
func TestNakOrTermTermsOnFinalDelivery(t *testing.T) {
	p := Policy{NakDelay: testDelay, MaxDeliver: 5, TermOnExhaustion: true}
	msg := delivered(5)

	got, err := p.NakOrTerm(msg)
	if err != nil {
		t.Fatalf("NakOrTerm: %v", err)
	}
	if got != OutcomeTerm {
		t.Errorf("outcome = %q, want term", got)
	}
	if !msg.Termed {
		t.Error("final delivery not termed")
	}
	if len(msg.NakDelays) != 0 {
		t.Errorf("NakDelays = %v, want none on the term path", msg.NakDelays)
	}
}

// TestNakOrTermKeepsNakWithoutOptIn: without TermOnExhaustion the final
// delivery still Naks — streams whose max_age doubles as the retry backstop
// keep their existing posture.
func TestNakOrTermKeepsNakWithoutOptIn(t *testing.T) {
	p := Policy{NakDelay: testDelay, MaxDeliver: 5}
	msg := delivered(5)

	got, err := p.NakOrTerm(msg)
	if err != nil {
		t.Fatalf("NakOrTerm: %v", err)
	}
	if got != OutcomeNak {
		t.Errorf("outcome = %q, want nak", got)
	}
	if msg.Termed {
		t.Error("termed without the TermOnExhaustion opt-in")
	}
}

// TestNakOrTermMissingMetadataNaks: a message without JetStream metadata (a
// synthetic test message) reads as non-final — the safe default is another
// retry, never a Term.
func TestNakOrTermMissingMetadataNaks(t *testing.T) {
	p := Policy{NakDelay: testDelay, MaxDeliver: 1, TermOnExhaustion: true}
	msg := &natsmsgtest.FakeMsg{MetaErr: errors.New("no metadata")}

	got, err := p.NakOrTerm(msg)
	if err != nil {
		t.Fatalf("NakOrTerm: %v", err)
	}
	if got != OutcomeNak {
		t.Errorf("outcome = %q, want nak", got)
	}
	if msg.Termed {
		t.Error("termed a message whose delivery count is unknown")
	}
}

// TestNakOrTermZeroMaxDeliverNaks pins the zero-value guard: MaxDeliver left
// at 0 means unlimited redeliveries (the jetstream.ConsumerConfig semantics),
// so no delivery is final and a transient failure Naks — it must NOT read
// every delivery as final and Term on the first failure.
func TestNakOrTermZeroMaxDeliverNaks(t *testing.T) {
	p := Policy{NakDelay: testDelay, TermOnExhaustion: true} // MaxDeliver unset
	msg := delivered(1)

	got, err := p.NakOrTerm(msg)
	if err != nil {
		t.Fatalf("NakOrTerm: %v", err)
	}
	if got != OutcomeNak {
		t.Errorf("outcome = %q, want nak", got)
	}
	if msg.Termed {
		t.Error("termed on the first delivery with an unset MaxDeliver")
	}
}

// TestDelayForFlatByDefault pins zero-value compatibility: without a Factor
// the envelope stays flat at NakDelay for every delivery, exactly the
// pre-growth behavior.
func TestDelayForFlatByDefault(t *testing.T) {
	p := Policy{NakDelay: testDelay}
	for _, n := range []int{0, 1, 2, 5, 50} {
		if got := p.DelayFor(n); got != testDelay {
			t.Errorf("flat DelayFor(%d) = %v, want %v", n, got, testDelay)
		}
	}
}

// TestDelayForGrows covers the two envelopes the services actually run:
// entire-api's 1s doubling capped at 300s and entiredb's 5s doubling capped
// at 30s.
func TestDelayForGrows(t *testing.T) {
	for _, tc := range []struct {
		name string
		p    Policy
		want map[int]time.Duration
	}{
		{
			name: "1s doubling capped at 300s",
			p:    Policy{NakDelay: time.Second, Factor: 2, MaxDelay: 300 * time.Second},
			want: map[int]time.Duration{
				0: time.Second, 1: time.Second, 2: 2 * time.Second, 3: 4 * time.Second,
				9: 256 * time.Second, 10: 300 * time.Second, 20: 300 * time.Second,
			},
		},
		{
			name: "5s doubling capped at 30s",
			p:    Policy{NakDelay: 5 * time.Second, Factor: 2, MaxDelay: 30 * time.Second},
			want: map[int]time.Duration{
				1: 5 * time.Second, 2: 10 * time.Second, 3: 20 * time.Second,
				4: 30 * time.Second, 8: 30 * time.Second,
			},
		},
	} {
		for n, want := range tc.want {
			if got := tc.p.DelayFor(n); got != want {
				t.Errorf("%s: DelayFor(%d) = %v, want %v", tc.name, n, got, want)
			}
		}
	}
}

// TestDelayForSaturatesWithoutCap: uncapped growth must saturate rather than
// wrap negative — a negative NakWithDelay would redeliver immediately, the
// opposite of backing off.
func TestDelayForSaturatesWithoutCap(t *testing.T) {
	p := Policy{NakDelay: time.Hour, Factor: 10}
	if got := p.DelayFor(30); got != time.Duration(math.MaxInt64) {
		t.Errorf("uncapped DelayFor(30) = %v, want saturation at MaxInt64", got)
	}
	if got := p.DelayFor(30); got < 0 {
		t.Errorf("uncapped DelayFor(30) went negative: %v", got)
	}
}

// TestNakOrTermUsesGrownDelay: NakOrTerm must nak with the delay for THIS
// delivery's count from the message metadata, not the base delay.
func TestNakOrTermUsesGrownDelay(t *testing.T) {
	p := Policy{NakDelay: time.Second, Factor: 2, MaxDelay: 300 * time.Second, MaxDeliver: 8}
	msg := delivered(3)

	got, err := p.NakOrTerm(msg)
	if err != nil {
		t.Fatalf("NakOrTerm: %v", err)
	}
	if got != OutcomeNak {
		t.Errorf("outcome = %q, want nak", got)
	}
	if len(msg.NakDelays) != 1 || msg.NakDelays[0] != 4*time.Second {
		t.Errorf("NakDelays = %v, want one nak with the grown 4s delay", msg.NakDelays)
	}
}

// TestNakOrTermGrownDelayMissingMetadata: without metadata the delivery count
// is unknown; the policy naks with the base delay (first-delivery read).
func TestNakOrTermGrownDelayMissingMetadata(t *testing.T) {
	p := Policy{NakDelay: time.Second, Factor: 2, MaxDelay: 300 * time.Second}
	msg := &natsmsgtest.FakeMsg{MetaErr: errors.New("no metadata")}

	if _, err := p.NakOrTerm(msg); err != nil {
		t.Fatalf("NakOrTerm: %v", err)
	}
	if len(msg.NakDelays) != 1 || msg.NakDelays[0] != time.Second {
		t.Errorf("NakDelays = %v, want one nak with the base delay", msg.NakDelays)
	}
}

func TestNumDelivered(t *testing.T) {
	if got := NumDelivered(delivered(3)); got != 3 {
		t.Errorf("NumDelivered = %d, want 3", got)
	}
	if got := NumDelivered(&natsmsgtest.FakeMsg{MetaErr: errors.New("none")}); got != 0 {
		t.Errorf("NumDelivered without metadata = %d, want 0", got)
	}
}

func TestIsFinalDelivery(t *testing.T) {
	if IsFinalDelivery(delivered(4), 5) {
		t.Error("delivery 4/5 read as final")
	}
	if !IsFinalDelivery(delivered(5), 5) {
		t.Error("delivery 5/5 not read as final")
	}
	if !IsFinalDelivery(delivered(6), 5) {
		t.Error("delivery past MaxDeliver not read as final")
	}
	if IsFinalDelivery(delivered(1), 0) {
		t.Error("delivery read as final with MaxDeliver 0 (unlimited)")
	}
	if IsFinalDelivery(delivered(1), -1) {
		t.Error("delivery read as final with MaxDeliver -1 (unlimited)")
	}
}

func legacyDelivered(n uint64) *natsmsgtest.FakeLegacyMsg {
	return &natsmsgtest.FakeLegacyMsg{Meta: &nats.MsgMetadata{NumDelivered: n}}
}

// dispositionMatrix is the shared behavioral contract both the modern and the
// legacy entry points must satisfy: for a given policy and delivery count, the
// same Outcome and — for a nak — the same single delay, or a term.
var dispositionMatrix = []struct {
	name         string
	policy       Policy
	delivered    uint64 // 0 with noMeta => metadata unavailable
	noMeta       bool
	wantOutcome  Outcome
	wantTermed   bool
	wantNakDelay time.Duration // asserted when wantOutcome is nak
}{
	{
		name:         "first delivery naks base",
		policy:       Policy{NakDelay: 5 * time.Second, Factor: 2, MaxDelay: 30 * time.Second, MaxDeliver: 8, TermOnExhaustion: true},
		delivered:    1,
		wantOutcome:  OutcomeNak,
		wantNakDelay: 5 * time.Second,
	},
	{
		name:         "intermediate retry grows",
		policy:       Policy{NakDelay: time.Second, Factor: 2, MaxDelay: 300 * time.Second, MaxDeliver: 8},
		delivered:    3,
		wantOutcome:  OutcomeNak,
		wantNakDelay: 4 * time.Second,
	},
	{
		name:        "final delivery terms with opt-in",
		policy:      Policy{NakDelay: 5 * time.Second, MaxDeliver: 5, TermOnExhaustion: true},
		delivered:   5,
		wantOutcome: OutcomeTerm,
		wantTermed:  true,
	},
	{
		name:         "final delivery naks without opt-in",
		policy:       Policy{NakDelay: 5 * time.Second, MaxDeliver: 5},
		delivered:    5,
		wantOutcome:  OutcomeNak,
		wantNakDelay: 5 * time.Second,
	},
	{
		name:        "past MaxDeliver terms with opt-in",
		policy:      Policy{NakDelay: 5 * time.Second, MaxDeliver: 5, TermOnExhaustion: true},
		delivered:   6,
		wantOutcome: OutcomeTerm,
		wantTermed:  true,
	},
	{
		name:         "metadata failure naks base and never terms",
		policy:       Policy{NakDelay: time.Second, Factor: 2, MaxDelay: 300 * time.Second, MaxDeliver: 1, TermOnExhaustion: true},
		noMeta:       true,
		wantOutcome:  OutcomeNak,
		wantNakDelay: time.Second,
	},
	{
		name:         "unlimited zero never terms",
		policy:       Policy{NakDelay: 5 * time.Second, TermOnExhaustion: true},
		delivered:    3,
		wantOutcome:  OutcomeNak,
		wantNakDelay: 5 * time.Second,
	},
	{
		name:         "unlimited -1 never terms",
		policy:       Policy{NakDelay: 5 * time.Second, MaxDeliver: UnlimitedMaxDeliver, TermOnExhaustion: true},
		delivered:    9,
		wantOutcome:  OutcomeNak,
		wantNakDelay: 5 * time.Second,
	},
}

// TestNakOrTermParity drives the whole disposition matrix through BOTH the
// modern jetstream.Msg entry point and the legacy *nats.Msg one, asserting each
// hits the expected outcome AND that the two agree — the single behavioral
// contract the shared core guarantees across the two message APIs.
func TestNakOrTermParity(t *testing.T) {
	for _, tc := range dispositionMatrix {
		t.Run(tc.name, func(t *testing.T) {
			modern := &natsmsgtest.FakeMsg{}
			legacy := &natsmsgtest.FakeLegacyMsg{}
			if !tc.noMeta {
				modern.Meta = &jetstream.MsgMetadata{NumDelivered: tc.delivered}
				legacy.Meta = &nats.MsgMetadata{NumDelivered: tc.delivered}
			}

			mOut, mErr := tc.policy.NakOrTerm(modern)
			lOut, lErr := tc.policy.NakOrTermLegacy(legacy)
			if mErr != nil || lErr != nil {
				t.Fatalf("NakOrTerm errors: modern=%v legacy=%v", mErr, lErr)
			}
			if mOut != tc.wantOutcome || lOut != tc.wantOutcome {
				t.Fatalf("outcome: modern=%q legacy=%q, want %q", mOut, lOut, tc.wantOutcome)
			}
			if modern.Termed != tc.wantTermed || legacy.Termed != tc.wantTermed {
				t.Errorf("termed: modern=%v legacy=%v, want %v", modern.Termed, legacy.Termed, tc.wantTermed)
			}
			if tc.wantOutcome == OutcomeNak {
				assertSingleNak(t, "modern", modern.NakDelays, tc.wantNakDelay)
				assertSingleNak(t, "legacy", legacy.NakDelays, tc.wantNakDelay)
				if modern.Termed || legacy.Termed {
					t.Error("naked path also termed")
				}
			} else if len(modern.NakDelays) != 0 || len(legacy.NakDelays) != 0 {
				t.Errorf("term path also naked: modern=%v legacy=%v", modern.NakDelays, legacy.NakDelays)
			}
		})
	}
}

func assertSingleNak(t *testing.T, which string, delays []time.Duration, want time.Duration) {
	t.Helper()
	if len(delays) != 1 || delays[0] != want {
		t.Errorf("%s NakDelays = %v, want one nak of %v", which, delays, want)
	}
}

// TestNakOrTermDispositionErrors pins the error-reporting contract across both
// APIs: a failed Term/Nak is returned wrapped, tagged with the Outcome that was
// attempted so the caller can still meter it.
func TestNakOrTermDispositionErrors(t *testing.T) {
	t.Run("term error on final delivery", func(t *testing.T) {
		p := Policy{NakDelay: time.Second, MaxDeliver: 1, TermOnExhaustion: true}
		boom := errors.New("term boom")

		mOut, mErr := p.NakOrTerm(&natsmsgtest.FakeMsg{Meta: &jetstream.MsgMetadata{NumDelivered: 1}, TermErr: boom})
		lOut, lErr := p.NakOrTermLegacy(&natsmsgtest.FakeLegacyMsg{Meta: &nats.MsgMetadata{NumDelivered: 1}, TermErr: boom})
		if mOut != OutcomeTerm || lOut != OutcomeTerm {
			t.Errorf("outcome: modern=%q legacy=%q, want term", mOut, lOut)
		}
		if mErr == nil || !errors.Is(mErr, boom) || lErr == nil || !errors.Is(lErr, boom) {
			t.Errorf("errors: modern=%v legacy=%v, want wrapped term boom", mErr, lErr)
		}
	})

	t.Run("nak error redelivering", func(t *testing.T) {
		p := Policy{NakDelay: time.Second, MaxDeliver: 5}
		boom := errors.New("nak boom")
		modern := &natsmsgtest.FakeMsg{Meta: &jetstream.MsgMetadata{NumDelivered: 2}, NakErr: boom}
		legacy := &natsmsgtest.FakeLegacyMsg{Meta: &nats.MsgMetadata{NumDelivered: 2}, NakErr: boom}

		mOut, mErr := p.NakOrTerm(modern)
		lOut, lErr := p.NakOrTermLegacy(legacy)
		if mOut != OutcomeNak || lOut != OutcomeNak {
			t.Errorf("outcome: modern=%q legacy=%q, want nak", mOut, lOut)
		}
		if mErr == nil || !errors.Is(mErr, boom) || lErr == nil || !errors.Is(lErr, boom) {
			t.Errorf("errors: modern=%v legacy=%v, want wrapped nak boom", mErr, lErr)
		}
	})
}

// TestNumDeliveredAndIsFinalParity pins that the modern and legacy read helpers
// agree over the same delivery counts and MaxDeliver bounds.
func TestNumDeliveredAndIsFinalParity(t *testing.T) {
	for _, n := range []uint64{1, 4, 5, 6} {
		if got := NumDeliveredLegacy(legacyDelivered(n)); got != int(n) {
			t.Errorf("NumDeliveredLegacy(%d) = %d", n, got)
		}
		if NumDelivered(delivered(n)) != NumDeliveredLegacy(legacyDelivered(n)) {
			t.Errorf("NumDelivered disagree at %d", n)
		}
	}
	if got := NumDeliveredLegacy(&natsmsgtest.FakeLegacyMsg{MetaErr: errors.New("none")}); got != 0 {
		t.Errorf("NumDeliveredLegacy without metadata = %d, want 0", got)
	}
	for _, tc := range []struct {
		n   uint64
		max int
	}{{4, 5}, {5, 5}, {6, 5}, {1, 0}, {1, -1}} {
		if IsFinalDelivery(delivered(tc.n), tc.max) != IsFinalDeliveryLegacy(legacyDelivered(tc.n), tc.max) {
			t.Errorf("IsFinalDelivery disagree at delivered=%d max=%d", tc.n, tc.max)
		}
	}
}

// TestEnvelopeRepoops pins entiredb repoops exactly: 5s doubling capped at 30s,
// MaxDeliver -1 (unlimited) so no delivery is ever final.
func TestEnvelopeRepoops(t *testing.T) {
	p := Policy{NakDelay: 5 * time.Second, Factor: 2, MaxDelay: 30 * time.Second, MaxDeliver: UnlimitedMaxDeliver}
	for n, want := range map[int]time.Duration{
		1: 5 * time.Second, 2: 10 * time.Second, 3: 20 * time.Second,
		4: 30 * time.Second, 5: 30 * time.Second, 8: 30 * time.Second,
	} {
		if got := p.DelayFor(n); got != want {
			t.Errorf("repoops DelayFor(%d) = %v, want %v", n, got, want)
		}
	}
	if IsFinalDelivery(delivered(100), p.MaxDeliver) {
		t.Error("repoops unlimited MaxDeliver read a delivery as final")
	}
}

// TestEnvelopePermswebhook pins entiredb permswebhook: 5s doubling capped by the
// consumer's configured ceiling (RetryBackoffMx), tested at two ceilings.
func TestEnvelopePermswebhook(t *testing.T) {
	for _, ceiling := range []time.Duration{30 * time.Second, 2 * time.Minute} {
		p := Policy{NakDelay: 5 * time.Second, Factor: 2, MaxDelay: ceiling, MaxDeliver: UnlimitedMaxDeliver}
		var prev time.Duration
		for n := 1; n <= 12; n++ {
			got := p.DelayFor(n)
			if got > ceiling {
				t.Fatalf("ceiling %v: DelayFor(%d) = %v exceeds ceiling", ceiling, n, got)
			}
			if n == 1 && got != 5*time.Second {
				t.Errorf("ceiling %v: DelayFor(1) = %v, want 5s base", ceiling, got)
			}
			if n > 1 && got < prev {
				t.Errorf("ceiling %v: DelayFor(%d)=%v shrank below %v", ceiling, n, got, prev)
			}
			prev = got
		}
		if prev != ceiling {
			t.Errorf("ceiling %v: envelope never reached the ceiling (max seen %v)", ceiling, prev)
		}
	}
}

// TestEnvelopeIngest pins entire-api ingest exactly: 1s doubling capped at 300s,
// MaxDeliver 8 with a final-delivery Term on the work-queue posture.
func TestEnvelopeIngest(t *testing.T) {
	p := Policy{NakDelay: time.Second, Factor: 2, MaxDelay: 300 * time.Second, MaxDeliver: 8, TermOnExhaustion: true}
	for n, want := range map[int]time.Duration{
		1: time.Second, 2: 2 * time.Second, 3: 4 * time.Second, 4: 8 * time.Second,
		5: 16 * time.Second, 6: 32 * time.Second, 7: 64 * time.Second, 8: 128 * time.Second,
		9: 256 * time.Second, 10: 300 * time.Second, 11: 300 * time.Second,
	} {
		if got := p.DelayFor(n); got != want {
			t.Errorf("ingest DelayFor(%d) = %v, want %v", n, got, want)
		}
	}
	if !IsFinalDelivery(delivered(8), p.MaxDeliver) {
		t.Error("ingest delivery 8/8 not read as final")
	}
	out, err := p.NakOrTerm(delivered(8))
	if err != nil || out != OutcomeTerm {
		t.Errorf("ingest final delivery: outcome=%q err=%v, want term", out, err)
	}
}

type recordingDLQ struct {
	published []*nats.Msg
	err       error
}

func (r *recordingDLQ) PublishMsg(m *nats.Msg, _ ...nats.PubOpt) (*nats.PubAck, error) {
	if r.err != nil {
		return nil, r.err
	}
	r.published = append(r.published, m)
	return &nats.PubAck{Stream: "dlq", Sequence: uint64(len(r.published))}, nil
}

// TestDeadLetterBeforeDisposition pins the documented capture-then-dispose
// composition: on the final delivery a work-queue consumer captures the poison
// to a DLQ and only then Terms; and if the capture fails it must NOT Term, so
// the poison is never removed without a durable copy.
func TestDeadLetterBeforeDisposition(t *testing.T) {
	p := Policy{NakDelay: 5 * time.Second, MaxDeliver: 3, TermOnExhaustion: true}
	final := func() *natsmsgtest.FakeMsg {
		return &natsmsgtest.FakeMsg{
			SubjectVal: "repo.ops.v1.teardown",
			DataVal:    []byte(`{"poison":true}`),
			HeadersVal: nats.Header{},
			Meta:       &jetstream.MsgMetadata{NumDelivered: 3, Sequence: jetstream.SequencePair{Stream: 7}},
		}
	}

	t.Run("capture succeeds then term", func(t *testing.T) {
		msg := final()
		pub := &recordingDLQ{}
		if !IsFinalDelivery(msg, p.MaxDeliver) {
			t.Fatal("setup: message should be final")
		}
		if err := natsmsg.DeadLetter(context.Background(), pub, "repo.ops.dlq.v1.teardown.poison", msg, "poison"); err != nil {
			t.Fatalf("DeadLetter: %v", err)
		}
		if len(pub.published) != 1 {
			t.Fatalf("captured %d msgs, want 1 before disposition", len(pub.published))
		}
		out, err := p.NakOrTerm(msg)
		if err != nil || out != OutcomeTerm || !msg.Termed {
			t.Fatalf("dispose: out=%q err=%v termed=%v, want clean term", out, err, msg.Termed)
		}
		if got := pub.published[0]; string(got.Data) != string(msg.DataVal) ||
			got.Header.Get(natsmsg.DLQReasonHeader) != "poison" {
			t.Errorf("DLQ copy missing payload/provenance: data=%q reason=%q", got.Data, got.Header.Get(natsmsg.DLQReasonHeader))
		}
	})

	t.Run("capture fails so no term", func(t *testing.T) {
		msg := final()
		pub := &recordingDLQ{err: errors.New("dlq down")}
		captured := true
		if IsFinalDelivery(msg, p.MaxDeliver) {
			if err := natsmsg.DeadLetter(context.Background(), pub, "repo.ops.dlq.v1.teardown.poison", msg, "poison"); err != nil {
				captured = false
			}
		}
		if captured {
			t.Fatal("capture unexpectedly succeeded")
		}
		// Contract: on capture failure the caller must not remove the original.
		if msg.Termed {
			t.Error("message termed despite a failed DLQ capture")
		}
	})
}
