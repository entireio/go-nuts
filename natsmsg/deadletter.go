package natsmsg

import (
	"context"
	"fmt"
	"strconv"
	"strings"
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
	// stay in place (see the header policy on DeadLetter) but is still worth
	// keeping: a replay tool restoring a message to its source stream needs the
	// publisher's dedupe key, not the DLQ's.
	DLQOriginMsgIDHeader = "Nats-Dlq-Origin-Msg-Id"

	// DLQHeaderPrefix is the namespace this package writes provenance into, and
	// the only part of the reserved Nats- namespace a captured copy keeps. A copy
	// that is itself dead-lettered later therefore accumulates its chain of
	// provenance rather than losing the earlier hop.
	DLQHeaderPrefix = "Nats-Dlq-"
)

// natsHeaderPrefix is NATS's reserved header namespace. Everything the broker
// interprets lives under it, which is what makes it the right boundary to filter
// on — see the header policy on [DeadLetter].
const natsHeaderPrefix = "Nats-"

// keepsHeaderOnCapture reports whether a header from the original belongs on the
// captured copy.
//
// The rule is a namespace boundary, deliberately, and it is the third design of
// this filter. The first copied everything; the second named the directives to
// drop. Both failed the same way: a deny-list has to be complete against a set
// the broker keeps growing, and each round of "one more header" was a live
// silent-failure path in the meantime — expectations (err 10060), per-message TTL
// (10166), counter increments (10168), atomic-batch publishes (10174), each one
// able to fail a capture on every delivery and strand a message the DLQ would
// have taken.
//
// So this fails closed instead. Nats- is NATS's reserved header namespace: an
// application has no business writing there, and everything the broker
// interprets lives under it. A captured copy therefore keeps
//
//   - every header OUTSIDE that namespace, verbatim — tracing (traceparent),
//     application metadata, anything the payload's own readers rely on; and
//   - nothing INSIDE it except this package's own [DLQHeaderPrefix] provenance.
//
// A directive NATS adds in a future release is dropped by this rule before anyone
// here has heard of it, which is the property the two earlier designs lacked. The
// matching is case-insensitive even though the broker's own lookup is a
// case-sensitive byte compare (server.getHeaderKeyIndex), so an oddly-cased
// directive is inert today: the cost of being stricter than necessary is nothing,
// and it holds if that ever changes.
//
// What this deliberately gives up: the server-set republish and direct-get
// provenance (Nats-Stream, Nats-Sequence, Nats-Time-Stamp, Nats-Subject) on a
// message that was itself republished. Keeping those would mean asserting they
// are safe for a client to publish, which the client library explicitly says they
// are not. The facts that matter about the original are re-stated in this
// package's own namespace instead — origin subject, origin stream, stream
// sequence, delivery count and the publisher's dedupe key — so a replay tool
// reads provenance it can trust the authorship of.
func keepsHeaderOnCapture(key string) bool {
	if !hasPrefixFold(key, natsHeaderPrefix) {
		return true
	}
	return hasPrefixFold(key, DLQHeaderPrefix)
}

// hasPrefixFold is strings.HasPrefix under ASCII case folding. Header keys are
// ASCII by protocol, so a byte-wise fold is the whole of it.
func hasPrefixFold(s, prefix string) bool {
	return len(s) >= len(prefix) && strings.EqualFold(s[:len(prefix)], prefix)
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
// "Headers" there means the ones an application owns. NATS's reserved Nats-
// namespace does not survive the copy at all — those are directives to the
// broker, and on the DLQ they would be obeyed against the wrong stream — with the
// single exception of this package's own Nats-Dlq- provenance. Nats-Msg-Id is
// re-derived from the original's identity so the copy dedupes as itself. See
// [keepsHeaderOnCapture] for the boundary and why it is a namespace rule rather
// than a list, and dlqMsgID for the key.
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
	src := msg.Headers()
	hdr := nats.Header{}
	for k, v := range src {
		// Filter on the way in rather than deleting afterwards: a copy that never
		// holds a directive cannot leak one through a name this package failed to
		// enumerate. See keepsHeaderOnCapture.
		if !keepsHeaderOnCapture(k) {
			continue
		}
		hdr[k] = append([]string(nil), v...)
	}
	// The publisher's dedupe key does not survive as-is (it would collapse
	// unrelated messages in the DLQ, see dlqMsgID) but a replay tool restoring the
	// message upstream needs it, so it moves into this package's namespace.
	if id := src.Get(jetstream.MsgIDHeader); id != "" {
		hdr.Set(DLQOriginMsgIDHeader, id)
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
