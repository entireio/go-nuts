// Package backoff is the redelivery policy for transiently-failed JetStream
// deliveries: a NakWithDelay envelope — flat by default, optionally growing
// multiplicatively per delivery — bounded by MaxDeliver, with an optional
// Term on the final delivery.
//
// The policy owns disposition and delay calculation only, and nothing wider:
// dead-letter capture, domain metrics/logging, and the decision of which
// failures are transient stay with the caller. It drives both the modern
// [jetstream.Msg] API (NakOrTerm, NumDelivered, IsFinalDelivery) and the legacy
// *nats.Msg API (the NakOrTermLegacy / NumDeliveredLegacy / IsFinalDeliveryLegacy
// counterparts, over the [LegacyMsg] interface). Both surfaces share one
// implementation, so they behave identically; the legacy set is a transitional
// bridge for consumers not yet on the modern API and can be removed once none
// remain.
//
// Delay calculation ([Policy.DelayFor]) is independent of message disposition:
// it is a pure function of the delivery count, so a caller can pin or meter the
// delay envelope without a message in hand.
//
// # Zero-value and unlimited semantics
//
// MaxDeliver matches [jetstream.ConsumerConfig.MaxDeliver]: a non-positive value
// (0 or [UnlimitedMaxDeliver]) means unlimited redeliveries, so no delivery is
// ever final and NakOrTerm always Naks. Note this reads zero differently from
// jsconsumer.Config.MaxDeliver, where 0 means "default to DefaultMaxDeliver":
// bridge with jsconsumer.Config.EffectiveMaxDeliver() (never the raw field), so
// an unset consumer MaxDeliver becomes the real default here instead of
// unlimited. Missing message metadata (a synthetic test message) reads as the
// first delivery — base NakDelay, never final — the safe default being another
// retry, not a Term.
//
// The Term-on-exhaustion posture is the COR-762 fix, lifted from
// mirror-pipeline's fanoutengine: on the final delivery a Nak is dropped
// silently by the broker (MaxDeliver reached), and on a WorkQueue stream the
// un-acked message then orphans — pinning the oldest-pending-age gauge — until
// StreamMaxAge finally expires it. Terming instead removes it cleanly. Streams
// where lingering is acceptable (a webhook stream whose max_age doubles as the
// retry backstop) leave TermOnExhaustion off and keep the plain Nak.
//
// Dead-letter capture stays with the caller, which should capture BEFORE
// disposing — gate it on [IsFinalDelivery]:
//
//	if backoff.IsFinalDelivery(msg, p.MaxDeliver) {
//		captureToDLQ(ctx, msg)
//	}
//	outcome, err := p.NakOrTerm(msg)
package backoff

import (
	"fmt"
	"math"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// UnlimitedMaxDeliver is the explicit Policy.MaxDeliver value for unlimited
// redeliveries (matching jetstream.ConsumerConfig.MaxDeliver, where a
// non-positive value disables the delivery cap). The zero value means the same
// thing; the named constant lets a caller say so on purpose.
const UnlimitedMaxDeliver = -1

// Outcome is the disposition NakOrTerm chose, for the caller's metrics and
// logs.
type Outcome string

const (
	OutcomeNak  Outcome = "nak"
	OutcomeTerm Outcome = "term"
)

// Policy is the redelivery envelope a consumer applies to transient
// failures. MaxDeliver must match the consumer's on-server MaxDeliver — the
// policy detects the final delivery by comparing against it.
type Policy struct {
	// NakDelay is the base delay before redelivery, and with a zero Factor
	// the whole envelope: flat suits failures expected to clear on their own
	// timetable, with MaxDeliver bounding the total.
	NakDelay time.Duration
	// Factor is the optional multiplicative growth per prior delivery: the
	// Nth delivery naks with NakDelay × Factor^(N-1), capped at MaxDelay.
	// Values ≤ 1 (including the zero value) keep the flat envelope. Growing
	// envelopes suit failures that get cheaper to wait out than to retry —
	// a downstream in a crash loop, a rate limit — where hammering on the
	// flat cadence spends MaxDeliver too fast.
	Factor float64
	// MaxDelay caps the grown delay; it does nothing for a flat policy.
	// Zero with a growing Factor leaves growth bounded only by MaxDeliver
	// ending the redeliveries (the computed delay still saturates rather
	// than overflowing).
	MaxDelay time.Duration
	// MaxDeliver is the consumer's redelivery cap
	// (jetstream.ConsumerConfig.MaxDeliver). Non-positive (0 or
	// UnlimitedMaxDeliver) means unlimited redeliveries — matching the
	// jetstream field's semantics — so no delivery is ever final and NakOrTerm
	// always Naks.
	MaxDeliver int
	// TermOnExhaustion makes the final delivery Term instead of Nak, so a
	// work-queue message is removed cleanly rather than orphaning un-acked
	// (COR-762). Callers with a DLQ capture before calling NakOrTerm.
	TermOnExhaustion bool
}

// LegacyMsg is the subset of the legacy *nats.Msg that the *Legacy redelivery
// entry points drive — Nak-with-delay, Term, and the delivery metadata. *nats.Msg
// satisfies it; a test can stub it.
type LegacyMsg interface {
	NakWithDelay(delay time.Duration, opts ...nats.AckOpt) error
	Term(opts ...nats.AckOpt) error
	Metadata() (*nats.MsgMetadata, error)
}

// NakOrTerm applies the policy to a transiently-failed modern [jetstream.Msg]
// delivery: NakWithDelay (see [Policy.DelayFor]) for redelivery, or — on the
// final delivery with TermOnExhaustion set — Term. Without TermOnExhaustion the
// final delivery still Naks; the broker drops the Nak at MaxDeliver and the
// stream's retention decides the message's fate.
//
// The returned Outcome reports which disposition was chosen, for the caller's
// metrics and logs. A disposition error is worth a log line but nothing more:
// a failed Nak redelivers via AckWait expiry anyway, and a failed Term on a
// work-queue stream means the message lingers exactly as it would have without
// the policy.
func (p Policy) NakOrTerm(msg jetstream.Msg) (Outcome, error) {
	return p.nakOrTerm(modernDisposer{msg})
}

// NakOrTermLegacy is [Policy.NakOrTerm] for a legacy *nats.Msg delivery,
// behaving identically over the [LegacyMsg] API.
func (p Policy) NakOrTermLegacy(msg LegacyMsg) (Outcome, error) {
	return p.nakOrTerm(legacyDisposer{msg})
}

// nakOrTerm is the disposition shared by the modern and legacy entry points.
func (p Policy) nakOrTerm(d disposer) (Outcome, error) {
	if p.TermOnExhaustion && isFinalDelivery(d, p.MaxDeliver) {
		if err := d.term(); err != nil {
			return OutcomeTerm, fmt.Errorf("term on final delivery: %w", err)
		}
		return OutcomeTerm, nil
	}
	if err := d.nakWithDelay(p.DelayFor(deliveryCount(d))); err != nil {
		return OutcomeNak, fmt.Errorf("nak with delay: %w", err)
	}
	return OutcomeNak, nil
}

// DelayFor reports the redelivery delay the policy applies after delivery
// number numDelivered (1 on first delivery): NakDelay for a flat policy,
// NakDelay × Factor^(numDelivered-1) capped at MaxDelay for a growing one.
// numDelivered ≤ 1 — including the 0 that [NumDelivered] reports when
// metadata is unavailable — reads as the first delivery. Exposed so a caller
// can log or meter the delay it is about to apply.
func (p Policy) DelayFor(numDelivered int) time.Duration {
	if p.Factor <= 1 || numDelivered <= 1 {
		return p.NakDelay
	}
	d := float64(p.NakDelay) * math.Pow(p.Factor, float64(numDelivered-1))
	if p.MaxDelay > 0 && d >= float64(p.MaxDelay) {
		return p.MaxDelay
	}
	if d >= float64(math.MaxInt64) {
		// Uncapped growth outran time.Duration; saturate instead of wrapping
		// negative (a negative NakWithDelay would redeliver immediately).
		return math.MaxInt64
	}
	return time.Duration(d)
}

// NumDelivered reports the JetStream delivery count of a modern [jetstream.Msg]
// (1 on first delivery), or 0 if metadata is unavailable (e.g. a synthetic test
// message).
func NumDelivered(msg jetstream.Msg) int {
	return deliveryCount(modernDisposer{msg})
}

// NumDeliveredLegacy is [NumDelivered] for a legacy *nats.Msg.
func NumDeliveredLegacy(msg LegacyMsg) int {
	return deliveryCount(legacyDisposer{msg})
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
	return isFinalDelivery(modernDisposer{msg}, maxDeliver)
}

// IsFinalDeliveryLegacy is [IsFinalDelivery] for a legacy *nats.Msg.
func IsFinalDeliveryLegacy(msg LegacyMsg, maxDeliver int) bool {
	return isFinalDelivery(legacyDisposer{msg}, maxDeliver)
}

// disposer is the message-disposition surface the policy drives, shared by the
// modern jetstream.Msg and legacy *nats.Msg adapters so both entry points run
// the identical logic.
type disposer interface {
	// deliveries reports the JetStream delivery count and whether the message
	// carried metadata to read it from.
	deliveries() (uint64, bool)
	nakWithDelay(delay time.Duration) error
	term() error
}

type modernDisposer struct{ msg jetstream.Msg }

func (m modernDisposer) deliveries() (uint64, bool) {
	if meta, err := m.msg.Metadata(); err == nil && meta != nil {
		return meta.NumDelivered, true
	}
	return 0, false
}

//nolint:wrapcheck // thin adapter; nakOrTerm adds the "nak with delay" context
func (m modernDisposer) nakWithDelay(delay time.Duration) error { return m.msg.NakWithDelay(delay) }

//nolint:wrapcheck // thin adapter; nakOrTerm adds the "term on final delivery" context
func (m modernDisposer) term() error { return m.msg.Term() }

type legacyDisposer struct{ msg LegacyMsg }

func (m legacyDisposer) deliveries() (uint64, bool) {
	if meta, err := m.msg.Metadata(); err == nil && meta != nil {
		return meta.NumDelivered, true
	}
	return 0, false
}

//nolint:wrapcheck // thin adapter; nakOrTerm adds the "nak with delay" context
func (m legacyDisposer) nakWithDelay(delay time.Duration) error { return m.msg.NakWithDelay(delay) }

//nolint:wrapcheck // thin adapter; nakOrTerm adds the "term on final delivery" context
func (m legacyDisposer) term() error { return m.msg.Term() }

// deliveryCount is the shared NumDelivered: the count, or 0 when metadata is
// unavailable (which the delay calc reads as the first delivery).
func deliveryCount(d disposer) int {
	if n, ok := d.deliveries(); ok {
		return int(n)
	}
	return 0
}

// isFinalDelivery is the shared IsFinalDelivery: NumDelivered has reached a
// positive maxDeliver. A non-positive maxDeliver (unlimited) and unavailable
// metadata both read as non-final.
func isFinalDelivery(d disposer, maxDeliver int) bool {
	if maxDeliver <= 0 {
		return false
	}
	if n, ok := d.deliveries(); ok {
		return n >= uint64(maxDeliver)
	}
	return false
}
