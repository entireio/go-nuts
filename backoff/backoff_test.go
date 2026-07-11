package backoff

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/entireio/go-nuts/natsmsg/natsmsgtest"
)

const testDelay = 5 * time.Second

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
