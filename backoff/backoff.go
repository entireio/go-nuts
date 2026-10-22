// Package backoff is the redelivery policy for transiently-failed JetStream
// deliveries: a NakWithDelay envelope — flat by default, optionally growing
// multiplicatively per delivery — bounded by MaxDeliver, with an optional
// Term on the final delivery.
//
// The policy owns disposition and delay calculation only, and nothing wider:
// dead-letter capture, domain metrics/logging, and the decision of which
// failures are transient stay with the caller. It drives the modern
// [jetstream.Msg] API (NakOrTerm, NumDelivered, IsFinalDelivery).
//
// Delay calculation ([Policy.DelayFor]) is independent of message disposition:
// it is a pure function of the delivery count, so a caller can pin or meter the
// delay envelope without a message in hand. What the BROKER then serves is another
// matter on a consumer carrying a growing server-side BackOff ladder, which
// stretches every requested delay — see [Policy.DelayFor].
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
// disposing — gate it on [IsFinalDelivery] AND on the delivery not having been
// interrupted, so the gate agrees with the disposition that follows it:
//
//	if !backoff.IsInterrupted(ctx) && backoff.IsFinalDelivery(msg, p.MaxDeliver) {
//		captureToDLQ(ctx, msg)
//	}
//	outcome, err := p.NakOrTerm(ctx, msg)
//
// # An interrupted delivery is a shutdown, not poison
//
// A handler whose context was CANCELLED did not fail on the message; it was
// interrupted — SIGTERM, a rollout, a KEDA scale-down (COR-2224). NakOrTerm
// therefore never Terms on an interrupted delivery, final or not: the message
// is Naked and left for another replica. On the broker's FINAL delivery that
// Nak is dropped (MaxDeliver is spent) and the message stays un-acked — the
// orphan state TermOnExhaustion otherwise avoids, chosen here on purpose
// because an un-acked message is visible (pinned ack floor, pending gauge)
// and reapable by a MAX_DELIVERIES sweeper, where a Term is a silent loss.
// The drain window jsconsumer gives handlers is what keeps this branch rare;
// this check only decides what happens when the window is not enough.
//
// "Interrupted" is [IsInterrupted]: cancellation, never a deadline. A
// per-attempt timeout a handler puts on its own context is a failed attempt —
// slow poison is exactly what TermOnExhaustion exists for — and still Terms.
package backoff

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// UnlimitedMaxDeliver is the explicit Policy.MaxDeliver value for unlimited
// redeliveries (matching jetstream.ConsumerConfig.MaxDeliver, where a
// non-positive value disables the delivery cap). The zero value means the same
// thing; the named constant lets a caller say so on purpose.
const UnlimitedMaxDeliver = -1

// Outcome is the disposition NakOrTerm ATTEMPTED, for the caller's metrics and
// logs. It records the branch the policy took, not a settlement the broker
// confirmed — see [Policy.NakOrTerm] for why those differ.
type Outcome string

const (
	OutcomeNak  Outcome = "nak"
	OutcomeTerm Outcome = "term"
)

// Policy is the redelivery envelope a consumer applies to transient
// failures. MaxDeliver must match the consumer's on-server MaxDeliver — the
// policy detects the final delivery by comparing against it.
//
// The envelope holds only on a consumer whose on-server BackOff ladder is absent
// or flat. A GROWING ladder overrides the policy's timing — the broker serves a
// materially longer delay than the policy computes, so the envelope a caller logs
// or meters is not the one the message experiences. See [Policy.DelayFor].
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

// NakOrTerm applies the policy to a transiently-failed [jetstream.Msg]
// delivery: NakWithDelay (see [Policy.DelayFor]) for redelivery, or — on the
// final delivery with TermOnExhaustion set and ctx still live — Term. Without
// TermOnExhaustion the final delivery still Naks; the broker drops the Nak at
// MaxDeliver and the stream's retention decides the message's fate.
//
// ctx is the handler's context, and a CANCELLED one ([IsInterrupted]) changes
// the final-delivery branch: the failure is read as a shutdown rather than
// poison, and the message is Naked instead of Termed so it is not discarded
// for having been interrupted (COR-2224). A context that merely timed out
// still Terms. See the package doc for what the Nak leaves on the stream. ctx
// is consulted for nothing else — the Nak and Term requests are not bounded
// by it.
//
// The returned Outcome reports which disposition was ATTEMPTED, for the caller's
// metrics and logs. It is not evidence that the broker accepted it, and neither is
// a nil error: Nak and Term are fire-and-forget publishes to $JS.ACK.>, so both
// return once the request is written to the connection. If the connection's
// identity lacks publish permission on $JS.ACK.> — the gap COR-1224 found on five
// live consumers — every disposition here returns nil while the server rejects it,
// the delivery stays outstanding, and the consumer's ack floor stops advancing with
// nothing logged anywhere. The rejection surfaces only on the connection's async
// error handler, which is why a consumer running this policy needs that grant and
// needs [nuts.Connect]'s async-error logging (or its own handler) to notice when it
// is missing. A caller that must KNOW a message settled has to use the message's
// DoubleAck instead and treat its error as "unknown", not "not settled" — a lost
// confirmation is indistinguishable from an ack that landed, so it is not grounds
// to re-dispatch the work.
//
// A returned error is therefore the lesser failure — the client could not even
// write the request — and is worth a log line but nothing more: a failed Nak
// redelivers via AckWait expiry anyway, and a failed Term on a work-queue stream
// means the message lingers exactly as it would have without the policy.
func (p Policy) NakOrTerm(ctx context.Context, msg jetstream.Msg) (Outcome, error) {
	if !IsInterrupted(ctx) && p.TermOnExhaustion && IsFinalDelivery(msg, p.MaxDeliver) {
		if err := msg.Term(); err != nil {
			return OutcomeTerm, fmt.Errorf("term on final delivery: %w", err)
		}
		return OutcomeTerm, nil
	}
	if err := msg.NakWithDelay(p.DelayFor(NumDelivered(msg))); err != nil {
		return OutcomeNak, fmt.Errorf("nak with delay: %w", err)
	}
	return OutcomeNak, nil
}

// IsInterrupted reports whether a delivery's handler was cut short by a
// cancellation rather than finishing or timing out: ctx is done with
// [context.Canceled]. That is the shape of a shutdown — the process signal
// context, a [nuts.ShutdownGroup], or jsconsumer's drain window elapsing — and
// the policy reads it as "this delivery was interrupted", not "this message
// failed". [context.DeadlineExceeded] is deliberately NOT interrupted: a
// deadline is something the handler (or an RPC under it) put on its own
// attempt, and an attempt that ran out of time is a failed attempt.
//
// The distinction only holds if the handler passes the context it was GIVEN,
// or one derived from it, to the disposition: a context the handler cancelled
// itself would read as a shutdown too. Use it to gate anything that should
// not happen to an interrupted delivery — a dead-letter capture, an
// error-level "poison" log — so those decisions agree with [Policy.NakOrTerm]
// and jsconsumer.Retry.Settle, which apply the same rule.
//
// [nuts.ShutdownGroup]: https://pkg.go.dev/github.com/entireio/go-nuts#ShutdownGroup
func IsInterrupted(ctx context.Context) bool {
	return errors.Is(ctx.Err(), context.Canceled)
}

// DelayFor reports the redelivery delay the policy applies after delivery
// number numDelivered (1 on first delivery): NakDelay for a flat policy,
// NakDelay × Factor^(numDelivered-1) capped at MaxDelay for a growing one.
// numDelivered ≤ 1 — including the 0 that [NumDelivered] reports when
// metadata is unavailable — reads as the first delivery. Exposed so a caller
// can log or meter the delay it is about to apply.
//
// It is the delay the policy REQUESTS, which is what the broker serves only when
// the consumer has no on-server BackOff ladder, or a flat one. On a consumer with a
// GROWING ladder the server offsets the delivery's timestamp by the requested delay
// but still measures it against the ladder's current rung, so the delay actually
// served is
//
//	requested + (BackOff[rung] - BackOff[0])
//
// where rung is the delivery's position in the ladder, clamped to its last entry.
// A flat ladder makes that second term zero and the request is honoured exactly; a
// growing one stretches every delay from the second redelivery onward — by hours,
// on a production ladder — while this function keeps reporting the request. A
// caller metering DelayFor there is publishing a retry envelope the broker never
// served, and cannot express its own envelope at all: the disposition that follows
// such a ladder faithfully is doing nothing and letting AckWait expire. Measured in
// internal/brokersemantics (TestBackOffGovernsAckTimeoutsNotNakDelays,
// TestBackoffPolicyDelayIsNotWhatTheBrokerServes).
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

// NumDelivered reports the JetStream delivery count of a [jetstream.Msg]
// (1 on first delivery), or 0 if metadata is unavailable (e.g. a synthetic test
// message).
func NumDelivered(msg jetstream.Msg) int {
	if meta, err := msg.Metadata(); err == nil && meta != nil {
		return int(meta.NumDelivered)
	}
	return 0
}

// IsFinalDelivery reports whether this is the last delivery before JetStream
// stops redelivering (NumDelivered has reached MaxDeliver), so a transient
// failure here is terminal rather than another Nak-and-retry. It reads the
// delivery count only: a caller gating a dead-letter capture on it should also
// check [IsInterrupted], since [Policy.NakOrTerm] will not Term on an
// interrupted delivery and the capture would then precede a Nak, not a Term. A non-positive
// maxDeliver means unlimited redeliveries (the jetstream.ConsumerConfig
// semantics), so no delivery is final — without the guard the zero value
// would read every delivery as final and Term on the first failure.
// Unavailable metadata also reads as non-final: the safe default is another
// retry, not a Term.
func IsFinalDelivery(msg jetstream.Msg, maxDeliver int) bool {
	if maxDeliver <= 0 {
		return false
	}
	if meta, err := msg.Metadata(); err == nil && meta != nil {
		return meta.NumDelivered >= uint64(maxDeliver)
	}
	return false
}
