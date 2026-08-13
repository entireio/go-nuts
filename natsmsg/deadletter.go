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
	// DLQOriginStreamHeader names the stream the original was captured from.
	// With DLQStreamSeqHeader it identifies the original exactly, which is what
	// the DLQ-scoped Nats-Msg-Id is built from.
	DLQOriginStreamHeader = "Nats-Dlq-Origin-Stream"
	// DLQOriginMsgIDHeader carries the original's own Nats-Msg-Id, which cannot
	// stay in place (see dlqStrippedHeaders) but is still worth keeping: a replay
	// tool restoring a message to its source stream needs the publisher's dedupe
	// key, not the DLQ's.
	DLQOriginMsgIDHeader = "Nats-Dlq-Origin-Msg-Id"
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
// Nats-TTL is in the list for a reason that took a second pass to see: carried
// onto a DLQ that allows per-message TTL it makes the captured RECORD expire on
// the source stream's retention terms, and onto one that does not it fails the
// capture outright. Either way the original is acked against a copy that is not
// durable on the DLQ's own terms. The Nats-Schedule family is worse still: a
// copied schedule expression turns the captured record into a scheduled publish,
// and Nats-Schedule-Target would deliver it somewhere else entirely.
//
// Nats-Msg-Id is stripped too, but it is REPLACED rather than merely dropped —
// see dlqMsgID. Carrying the publisher's own ID through was a silent-loss path:
// JetStream dedupes by that ID alone, stream-wide across every subject in the
// DLQ, so two DIFFERENT originals that happen to share an ID inside the
// duplicate window collapse onto one record. The second publish returns a
// SUCCESSFUL PubAck with Duplicate set, this function reports success, and the
// caller acks an original whose copy was never stored. A DLQ-scoped ID keeps the
// dedupe that is wanted (re-capturing the SAME original after a failed Ack) and
// removes the collision that is not.
//
// The server-set republish/direct-get headers (Nats-Stream, Nats-Sequence and
// friends) are deliberately left alone: they are provenance rather than
// directives, and a message that was itself republished carries real information
// in them.
var dlqStrippedHeaders = []string{
	jetstream.MsgIDHeader,
	jetstream.ExpectedStreamHeader,
	jetstream.ExpectedLastSeqHeader,
	jetstream.ExpectedLastSubjSeqHeader,
	jetstream.ExpectedLastSubjSeqSubjHeader,
	jetstream.ExpectedLastMsgIDHeader,
	jetstream.MsgTTLHeader,
	jetstream.MsgRollup,
	jetstream.ScheduleHeader,
	jetstream.ScheduleTargetHeader,
	jetstream.ScheduleSourceHeader,
	jetstream.ScheduleTTLHeader,
	jetstream.ScheduleTimeZoneHeader,
}

// dlqMsgID is the captured copy's own dedupe key: the original's stream and
// stream sequence, which name it uniquely across every stream in the deployment.
//
// This is what makes a re-capture idempotent without making distinct messages
// collide. When the DLQ publish succeeds but the original's Ack does not, the
// message redelivers and is captured again (the settle:dlq_ack_failed path in
// jsconsumer.Retry, which prefers a duplicate in the DLQ over a pinned floor);
// that second publish carries the SAME origin stream and sequence, so the DLQ's
// duplicate window collapses it onto the first record. Two different originals
// can never produce the same key, so the collapse only ever happens to copies
// that genuinely are the same message.
func dlqMsgID(meta *jetstream.MsgMetadata) string {
	return meta.Stream + "/" + strconv.FormatUint(meta.Sequence.Stream, 10)
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
// and headers and adding Nats-Dlq-* provenance (reason, origin subject, origin
// stream, delivery count, stream sequence, and the original's own Nats-Msg-Id).
//
// JetStream's publish-control headers are the exception to "preserving headers":
// they are directives about the publish, so on the DLQ they would be obeyed
// against the wrong stream. They are stripped, and Nats-Msg-Id is re-derived
// from the original's identity so the copy dedupes as itself — see
// dlqStrippedHeaders and dlqMsgID.
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
	// Keep the original's dedupe key as provenance before dropping it, so a
	// replay tool restoring the message upstream still has the publisher's ID.
	if id := hdr.Get(jetstream.MsgIDHeader); id != "" {
		hdr.Set(DLQOriginMsgIDHeader, id)
	}
	// Drop the broker directives before they are re-asserted against the DLQ
	// stream. Deleting after the copy rather than filtering inside it keeps the
	// stripped set readable in one place; see dlqStrippedHeaders for why each one
	// has to go.
	for _, h := range dlqStrippedHeaders {
		hdr.Del(h)
	}
	hdr.Set(DLQReasonHeader, reason)
	hdr.Set(DLQOriginHeader, msg.Subject())
	if meta, err := msg.Metadata(); err == nil && meta != nil {
		hdr.Set(DLQDeliveredHeader, strconv.FormatUint(meta.NumDelivered, 10))
		hdr.Set(DLQStreamSeqHeader, strconv.FormatUint(meta.Sequence.Stream, 10))
		hdr.Set(DLQOriginStreamHeader, meta.Stream)
		// The copy's OWN dedupe key. Without metadata there is no identity to
		// build one from, so the copy goes out with no Nats-Msg-Id at all: a
		// re-capture would then leave two records, which is the right way to be
		// wrong here — a duplicate is reconcilable, a silent collapse onto an
		// unrelated message is not.
		hdr.Set(jetstream.MsgIDHeader, dlqMsgID(meta))
	}

	out := &nats.Msg{Subject: dlqSubject, Data: msg.Data(), Header: hdr}

	pubCtx, cancel := context.WithTimeout(ctx, dlqPublishTimeout)
	defer cancel()
	if _, err := pub.PublishMsg(pubCtx, out); err != nil {
		return fmt.Errorf("dead-letter publish to %q: %w", dlqSubject, err)
	}
	return nil
}
