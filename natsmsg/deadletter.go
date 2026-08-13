package natsmsg

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// dlqPublishTimeout bounds one dead-letter PublishMsg. The consume context has
// no deadline, and PublishMsg waits for the PubAck indefinitely; without a
// bound a NATS blip would hang the handler (and, since a one-in-flight consumer
// keeps a slot occupied, stall the consumer) instead of failing the capture
// loudly.
const dlqPublishTimeout = 15 * time.Second

// DLQ provenance headers added to the captured copy so a replay tool (or a
// human) can see why and from where a message was dead-lettered.
const (
	DLQReasonHeader    = "Nats-Dlq-Reason"
	DLQOriginHeader    = "Nats-Dlq-Origin-Subject"
	DLQDeliveredHeader = "Nats-Dlq-Delivered"
	DLQStreamSeqHeader = "Nats-Dlq-Stream-Seq"
)

// dlqStrippedHeaders are the JetStream publish-control headers removed when
// copying an original into the DLQ. They are instructions to the BROKER about
// the publish, not metadata about the payload, and every one of them is
// evaluated against the stream being published TO — so carried onto the DLQ
// they are asserted about the wrong stream.
//
// This is not hypothetical tidiness. An original published with
// Nats-Expected-Stream naming its own stream fails the capture publish with
// err 10060 on EVERY attempt, so the message rides the whole ladder and ends
// as a strand with the floor still pinned — one producer class defeating the
// "never drop, never strand silently" guarantee, and defeating the breaker
// identically since it too terminates through this capture. Nats-Rollup is
// worse than a failed publish: honoured on the DLQ it would purge the very
// subject the DLQ exists to retain.
//
// Nats-Msg-Id is deliberately NOT in this list. It is the publisher's dedupe
// key rather than an assertion about stream state, and keeping it is what makes
// a re-capture idempotent: when the DLQ publish succeeds but the original's Ack
// does not, the message redelivers and is captured a second time (the
// settle:dlq_ack_failed path in jsconsumer.Retry, which prefers a duplicate in
// the DLQ over a pinned floor). Inside the DLQ stream's duplicate window that
// second publish collapses onto the first, so the deliberate duplicate does not
// become two copies for a replay tool to reconcile.
var dlqStrippedHeaders = []string{
	jetstream.ExpectedStreamHeader,
	jetstream.ExpectedLastSeqHeader,
	jetstream.ExpectedLastSubjSeqHeader,
	jetstream.ExpectedLastSubjSeqSubjHeader,
	jetstream.ExpectedLastMsgIDHeader,
	jetstream.MsgRollup,
}

// DLQPublisher is the publish surface DeadLetter needs — the modern
// [JetStream] publish primitive (satisfied by jetstream.JetStream), aliased
// so DeadLetter's signature reads in DLQ terms. Kept minimal so wiring can
// hand it the same JetStream handle the per-cell publishers already hold,
// and tests can stub it.
type DLQPublisher = JetStream

// SubjectToken sanitizes s into a single valid NATS subject token. NATS tokens
// cannot contain spaces, dots, or wildcards, so every character outside
// [a-z0-9_-] is replaced with '_' (and letters are lowercased). Use it to turn a
// human-readable dead-letter reason ("bad completion payload") into a subject
// token ("bad_completion_payload") — publishing to an unsanitized reason would
// fail, so the caller would never Ack the poison off a WorkQueue.
func SubjectToken(s string) string {
	// Iterate bytes, not runes: we only keep ASCII [a-z0-9_-] and map everything
	// else (including every byte of a multi-byte rune) to '_', so a byte loop is
	// both correct and free of a rune->byte narrowing conversion.
	b := make([]byte, 0, len(s))
	for i := range len(s) {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '_', c == '-':
			b = append(b, c)
		case c >= 'A' && c <= 'Z':
			b = append(b, c-'A'+'a')
		default:
			b = append(b, '_')
		}
	}
	if len(b) == 0 {
		return "unknown"
	}
	return string(b)
}

// DeadLetter republishes a poison message to dlqSubject, preserving its payload
// and headers and adding Nats-Dlq-* provenance (reason, origin subject,
// delivery count, stream sequence). JetStream publish-control headers are the
// one exception to "preserving headers" and are stripped — see
// dlqStrippedHeaders.
//
// It is the capture step a consumer runs before giving up on a message it can
// never process. DeadLetter copies the poison to a durable DLQ subject so it can
// be inspected or replayed, and the caller then removes the original from the
// stream — a Term on the final delivery (see backoff.Policy.TermOnExhaustion,
// whose doc notes callers "DLQ capture before calling NakOrTerm") or a plain
// Ack. Without this step a Term'd or MaxDeliver-exhausted message is discarded
// with no trail; DeadLetter is what makes the give-up auditable and recoverable.
//
// Returns an error if the republish fails; on error the caller MUST NOT remove
// the original — Nak or leave it so the poison is never dropped without a
// captured copy. Publishing is bounded by dlqPublishTimeout regardless of ctx's
// deadline.
func DeadLetter(ctx context.Context, pub DLQPublisher, dlqSubject string, msg jetstream.Msg, reason string) error {
	hdr := nats.Header{}
	for k, v := range msg.Headers() {
		hdr[k] = append([]string(nil), v...)
	}
	// Drop the broker directives before they are re-asserted against the DLQ
	// stream. Deleting after the copy rather than filtering inside it keeps the
	// stripped set readable in one place; see dlqStrippedHeaders for why each
	// one has to go and why Nats-Msg-Id stays.
	for _, h := range dlqStrippedHeaders {
		hdr.Del(h)
	}
	hdr.Set(DLQReasonHeader, reason)
	hdr.Set(DLQOriginHeader, msg.Subject())
	if meta, err := msg.Metadata(); err == nil && meta != nil {
		hdr.Set(DLQDeliveredHeader, strconv.FormatUint(meta.NumDelivered, 10))
		hdr.Set(DLQStreamSeqHeader, strconv.FormatUint(meta.Sequence.Stream, 10))
	}

	out := &nats.Msg{Subject: dlqSubject, Data: msg.Data(), Header: hdr}

	pubCtx, cancel := context.WithTimeout(ctx, dlqPublishTimeout)
	defer cancel()
	if _, err := pub.PublishMsg(pubCtx, out); err != nil {
		return fmt.Errorf("dead-letter publish to %q: %w", dlqSubject, err)
	}
	return nil
}
