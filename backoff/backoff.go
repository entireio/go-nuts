// Package backoff is the redelivery policy for transiently-failed JetStream
// deliveries: a flat NakWithDelay envelope bounded by MaxDeliver, with an
// optional Term on the final delivery.
//
// The Term-on-exhaustion posture is the COR-762 fix, lifted from
// mirror-pipeline's fanoutengine: on the final delivery a Nak is dropped
// silently by the broker (MaxDeliver reached), and on a WorkQueue stream the
// un-acked message then orphans — pinning the oldest-pending-age gauge — until
// StreamMaxAge finally expires it. Terming instead removes it cleanly. Streams
// where lingering is acceptable (a webhook stream whose max_age doubles as the
// retry backstop) leave TermOnExhaustion off and keep the plain Nak.
//
// The policy owns disposition only. Dead-letter capture stays with the caller,
// which should capture BEFORE disposing — gate it on [IsFinalDelivery]:
//
//	if backoff.IsFinalDelivery(msg, p.MaxDeliver) {
//		captureToDLQ(ctx, msg)
//	}
//	outcome, err := p.NakOrTerm(msg)
package backoff

import (
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// Outcome is the disposition NakOrTerm chose, for the caller's metrics and
// logs.
type Outcome string

const (
	OutcomeNak  Outcome = "nak"
	OutcomeTerm Outcome = "term"
)

// Policy is the flat redelivery envelope a consumer applies to transient
// failures. MaxDeliver must match the consumer's on-server MaxDeliver — the
// policy detects the final delivery by comparing against it.
type Policy struct {
	// NakDelay is the flat delay before redelivery. Flat, not growing: the
	// envelope only rides out transient failures; MaxDeliver bounds the total.
	NakDelay time.Duration
	// MaxDeliver is the consumer's redelivery cap
	// (jetstream.ConsumerConfig.MaxDeliver). Non-positive means unlimited
	// redeliveries — matching the jetstream field's semantics — so no
	// delivery is ever final and NakOrTerm always Naks.
	MaxDeliver int
	// TermOnExhaustion makes the final delivery Term instead of Nak, so a
	// work-queue message is removed cleanly rather than orphaning un-acked
	// (COR-762). Callers with a DLQ capture before calling NakOrTerm.
	TermOnExhaustion bool
}

// NakOrTerm applies the policy to a transiently-failed delivery: NakWithDelay
// for redelivery, or — on the final delivery with TermOnExhaustion set — Term.
// Without TermOnExhaustion the final delivery still Naks; the broker drops the
// Nak at MaxDeliver and the stream's retention decides the message's fate.
//
// The returned Outcome reports which disposition was chosen, for the caller's
// metrics and logs. A disposition error is worth a log line but nothing more:
// a failed Nak redelivers via AckWait expiry anyway, and a failed Term on a
// work-queue stream means the message lingers exactly as it would have without
// the policy.
func (p Policy) NakOrTerm(msg jetstream.Msg) (Outcome, error) {
	if p.TermOnExhaustion && IsFinalDelivery(msg, p.MaxDeliver) {
		if err := msg.Term(); err != nil {
			return OutcomeTerm, fmt.Errorf("term on final delivery: %w", err)
		}
		return OutcomeTerm, nil
	}
	if err := msg.NakWithDelay(p.NakDelay); err != nil {
		return OutcomeNak, fmt.Errorf("nak with delay: %w", err)
	}
	return OutcomeNak, nil
}

// NumDelivered reports the JetStream delivery count (1 on first delivery), or
// 0 if metadata is unavailable (e.g. a synthetic test message).
func NumDelivered(msg jetstream.Msg) int {
	if meta, err := msg.Metadata(); err == nil {
		return int(meta.NumDelivered)
	}
	return 0
}

// IsFinalDelivery reports whether this is the last delivery before JetStream
// stops redelivering (NumDelivered has reached MaxDeliver), so a transient
// failure here is terminal rather than another Nak-and-retry. A non-positive
// maxDeliver means unlimited redeliveries (the jetstream.ConsumerConfig
// semantics), so no delivery is final — without the guard the zero value
// would read every delivery as final and Term on the first failure.
// Unavailable metadata also reads as non-final: the safe default is another
// retry, not a Term.
func IsFinalDelivery(msg jetstream.Msg, maxDeliver int) bool {
	if maxDeliver <= 0 {
		return false
	}
	if meta, err := msg.Metadata(); err == nil {
		return meta.NumDelivered >= uint64(maxDeliver)
	}
	return false
}
