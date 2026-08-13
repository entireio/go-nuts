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
	// DLQOriginMsgIDHeader records the Nats-Msg-Id the captured message itself
	// carried, which cannot stay in place under its own name (it would dedupe the
	// copy as the original — see dlqMsgID) but a replay tool restoring the message
	// upstream needs.
	//
	// Read it as "what the captured message's dedupe key was", because that is all
	// it claims. When the captured message came from a producer this is the
	// producer's key. When it was itself a DLQ copy, it is that copy's DLQ key —
	// which points at the previous record rather than at the first publisher.
	// Either way the VALUE came off the message and is not something this package
	// vouches for; only the fact that it was there is.
	DLQOriginMsgIDHeader = "Nats-Dlq-Origin-Msg-Id"

	// DLQHeaderPrefix is the namespace this package writes provenance into. Every
	// header under it on a captured copy was written by the capture that produced
	// that copy — an inbound one is dropped like any other reserved header, so a
	// producer cannot present provenance as though this package had vouched for
	// it. Provenance therefore describes ONE hop; see [DeadLetter] on how to walk
	// a chain of them.
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
// interprets lives under it. A captured copy therefore keeps every header
// OUTSIDE that namespace verbatim — tracing (traceparent), application metadata,
// anything the payload's own readers rely on — and nothing at all from inside it.
//
// A directive NATS adds in a future release is dropped by this rule before anyone
// here has heard of it, which is the property the two earlier designs lacked. The
// matching is case-insensitive even though the broker's own lookup is a
// case-sensitive byte compare (server.getHeaderKeyIndex), so an oddly-cased
// directive is inert today: the cost of being stricter than necessary is nothing,
// and it holds if that ever changes.
//
// Nothing inside the namespace is exempt, including this package's own
// [DLQHeaderPrefix]. That is an authorship rule, not tidiness: a header this
// package did not write is a header whose truth it cannot vouch for, and both
// alternatives are worse. Keeping an inbound Nats-Dlq- header lets any producer
// hand a replay tool provenance that looks like the library's, and the fields are
// overwritten for the current hop anyway — so retaining them preserved nothing
// while lending a stranger's claims this package's authority.
//
// Also given up deliberately: the server-set republish and direct-get provenance
// (Nats-Stream, Nats-Sequence, Nats-Time-Stamp, Nats-Subject) on a message that
// was itself republished. Keeping those would mean asserting they are safe for a
// client to publish, which the client library explicitly says they are not. What
// matters about the captured message is re-stated in this package's own namespace,
// where the authorship is known.
func keepsHeaderOnCapture(key string) bool {
	return !hasPrefixFold(key, natsHeaderPrefix)
}

// hasPrefixFold is strings.HasPrefix under ASCII case folding. Header keys are
// ASCII by protocol, so a byte-wise fold is the whole of it.
func hasPrefixFold(s, prefix string) bool {
	return len(s) >= len(prefix) && strings.EqualFold(s[:len(prefix)], prefix)
}

// dlqMsgID is the captured copy's own dedupe key, naming the stored message the
// copy was made from: its domain, stream, stream sequence and store timestamp.
//
// The key exists to make a re-capture idempotent. When the DLQ publish succeeds
// but the original's Ack does not, the message redelivers and is captured again
// (the settle:dlq_ack_failed path in jsconsumer.Retry, which prefers a duplicate
// in the DLQ over a pinned floor); presenting the same key lets the DLQ's
// duplicate window collapse the second copy onto the first. That requires two
// properties, and both are why the key is built from these four fields and no
// others.
//
// STABLE for one stored message. Every component is a fixed property of the
// message as stored, so it is identical on delivery 1 and delivery 6, before and
// after a consumer restart. NumDelivered is deliberately NOT part of it: it
// changes per delivery, and including it would give each redelivery a different
// key — turning the intended collapse into a pile of near-duplicate records.
//
// DISTINCT for any two stored messages. Stream sequence alone is not enough,
// which is the reason for the other three. A sequence identifies a message only
// within one incarnation of one stream: delete a stream and recreate it and
// numbering restarts at 1, so a DLQ that outlives the source — the normal case,
// since the DLQ is what the source's messages are rescued INTO — would see the
// same stream/sequence pair naming two unrelated messages. Inside a duplicate
// window that collapsed the second onto the first, and because a duplicate
// PubAck reports success the caller would then Ack an original whose copy was
// never stored. The store timestamp separates incarnations (a recreated stream
// cannot re-store a message at the same nanosecond), and the domain separates
// same-named streams in different JetStream domains, which Sequence and Stream
// together cannot.
//
// The dot separator is unambiguous rather than merely tidy: domain and stream
// arrive as single tokens of the dot-delimited $JS.ACK reply subject, so neither
// can contain a dot, and the other two components are decimal digits. An absent
// domain is written as "_", mirroring the wire format's own sentinel, so the
// field is never empty.
func dlqMsgID(meta *jetstream.MsgMetadata) string {
	domain := meta.Domain
	if domain == "" {
		domain = "_"
	}
	// A real delivery always carries a store time; a zero one means the metadata
	// did not come from the broker. Write it as 0 rather than letting UnixNano
	// report its undefined value for the zero Time, which is a large negative
	// number that reads like a real timestamp.
	stored := "0"
	if !meta.Timestamp.IsZero() {
		stored = strconv.FormatInt(meta.Timestamp.UnixNano(), 10)
	}
	return strings.Join([]string{
		domain,
		meta.Stream,
		strconv.FormatUint(meta.Sequence.Stream, 10),
		stored,
	}, ".")
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
// broker, and on the DLQ they would be obeyed against the wrong stream — and the
// provenance this function adds is written fresh on top. Nats-Msg-Id is re-derived
// from the captured message's identity so the copy dedupes as itself. See
// [keepsHeaderOnCapture] for the boundary and why it is a namespace rule rather
// than a list, and dlqMsgID for the key.
//
// # Provenance describes one hop
//
// Every Nats-Dlq- header on a copy was written by the capture that produced that
// copy, and describes that capture only. Dead-lettering a message that is ITSELF
// a DLQ copy — a replay tool giving up on a record — does not accumulate the
// earlier hop's fields into the new copy: they are overwritten with this hop's,
// and an inbound Nats-Dlq- header is dropped before that anyway, because a header
// this package did not write is one whose truth it cannot vouch for.
//
// A chain is still walkable, as a chain of records rather than a chain of headers.
// DLQOriginStreamHeader and DLQStreamSeqHeader name the exact stored message the
// copy was made from, so the previous hop is one GetMsg away, and its own
// provenance names the hop before it. What that costs is honest to state: the
// walk needs each intermediate record to still exist. On a DLQ that retains its
// messages they do; where a replay tool Acks records off a work-queue DLQ as it
// reprocesses them, a link can dangle, and only the fields on the copy in hand
// remain. Callers that need first-publisher identity to survive an arbitrary
// number of hops should carry it in their own header, outside the reserved
// namespace, where it is theirs to keep.
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
