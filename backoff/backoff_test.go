package backoff

import (
	"errors"
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
}
