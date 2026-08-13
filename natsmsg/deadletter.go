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
//
// They divide into two groups, and the division is the whole contract. ORIGIN
// headers describe where the message ultimately came from and are WRITE-ONCE: a
// capture fills one in only if it is not already there, so re-capturing a message
// — whether the same original after a failed Ack, or a DLQ record a replay tool
// has given up on — never overwrites the first hop's answer. PER-HOP headers
// describe the capture that just happened and are written fresh every time.
const (
	// DLQReasonHeader is per-hop: why THIS capture gave up.
	DLQReasonHeader = "Nats-Dlq-Reason"
	// DLQDeliveredHeader is per-hop: the delivery count this capture saw.
	DLQDeliveredHeader = "Nats-Dlq-Delivered"
	// DLQHopsHeader counts captures. The library derives it by incrementing
	// whatever it found, so it is 1 on a first capture and tells a reader that
	// earlier hops exist — their reasons are not in these headers, because reason
	// is per-hop.
	DLQHopsHeader = "Nats-Dlq-Hops"

	// DLQOriginHeader is write-once: the subject the message was first published
	// to.
	DLQOriginHeader = "Nats-Dlq-Origin-Subject"
	// DLQStreamSeqHeader is write-once: the stream sequence the message was first
	// stored at.
	DLQStreamSeqHeader = "Nats-Dlq-Stream-Seq"
	// DLQOriginStreamHeader is write-once: the stream the message was first stored
	// on.
	DLQOriginStreamHeader = "Nats-Dlq-Origin-Stream"
	// DLQOriginDomainHeader is write-once: the JetStream domain that stream was
	// in, empty outside a domain. Two deployments can hold same-named streams, so
	// replay tooling needs it to tell their records apart.
	DLQOriginDomainHeader = "Nats-Dlq-Origin-Domain"
	// DLQOriginTimestampHeader is write-once: when the message was first stored, as
	// RFC3339 with nanoseconds.
	//
	// It is the field that distinguishes two incarnations of one stream, since a
	// recreated stream restarts its sequence numbering. Origin stream, sequence,
	// domain and timestamp together identify a stored message, which is what
	// replay-side dedupe should key on — see [DeadLetter] on why capture no longer
	// dedupes at all.
	DLQOriginTimestampHeader = "Nats-Dlq-Origin-Timestamp"
	// DLQOriginMsgIDHeader is write-once: the Nats-Msg-Id the message carried when
	// it was first captured, which is the publisher's own dedupe key and the thing
	// a replay tool needs to restore it upstream. It cannot stay under its own name
	// — see [DeadLetter] on the header policy.
	DLQOriginMsgIDHeader = "Nats-Dlq-Origin-Msg-Id"
	// DLQOriginRecordedHeader marks the origin block as filled in, and is what makes
	// write-once mean write-once.
	//
	// Field-by-field absence cannot carry that fact, because absence is ambiguous:
	// a message captured outside a JetStream domain has no origin domain to record,
	// and one captured with no metadata at all has no origin stream, sequence or
	// store time either. Reading those absences as "not yet recorded" let a LATER
	// hop fill them in from itself — writing its own domain, or the DLQ stream it
	// read the record from, as the message's origin. Silently wrong provenance,
	// which is worse than none.
	//
	// So the block is written exactly once, as a block. When this marker is present
	// every origin field is carried through as-is and none is derived, whatever is
	// or is not there: an absent field means the hop that recorded the origin had
	// nothing to record for it, and no later hop is entitled to a better answer.
	DLQOriginRecordedHeader = "Nats-Dlq-Origin-Recorded"

	// DLQHeaderPrefix is the namespace this package writes provenance into. It is
	// NOT a retention rule: an inbound header merely because it carries this prefix
	// is dropped, and only the specific origin headers above are carried forward.
	// See [DeadLetter].
	DLQHeaderPrefix = "Nats-Dlq-"
)

// dlqCarriedProvenance are the origin headers a capture preserves from the
// message it is capturing, rather than deriving. They are the write-once set: a
// value already present describes an earlier hop and is the answer to keep.
//
// It is a closed list of keys this package defines, deliberately, rather than the
// whole [DLQHeaderPrefix]. Retaining the prefix would let a producer invent any
// Nats-Dlq- header and have it ride into the DLQ looking like the library's own.
// The narrower rule does not make the VALUES unforgeable — a producer can still
// set Nats-Dlq-Origin-Subject on a message it publishes, and no header-level rule
// can tell that from a previous hop's writing — so read origin headers as "what
// the capture chain reported", not as something this package vouches for. What the
// list does buy is that the SHAPE is fixed: only these keys exist, only the per-hop
// ones are freshly authored, and an unknown Nats-Dlq- header never appears.
var dlqCarriedProvenance = []string{
	DLQOriginRecordedHeader,
	DLQOriginHeader,
	DLQStreamSeqHeader,
	DLQOriginStreamHeader,
	DLQOriginDomainHeader,
	DLQOriginTimestampHeader,
	DLQOriginMsgIDHeader,
}

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
// Nothing inside the namespace passes this filter, including this package's own
// [DLQHeaderPrefix]: a Nats-Dlq- header is not kept because it carries the prefix.
// The specific origin headers in [dlqCarriedProvenance] are then carried forward
// deliberately, by name, on top of the filtered copy — so an invented Nats-Dlq-
// field never rides through, and only the closed set this package defines exists on
// a captured copy.
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

// dlqHops is how many captures the message being captured has already been
// through, from [DLQHopsHeader]. An absent or unparseable value reads as zero, so
// the next capture writes 1: a garbled count is worth less than a wrong one is
// harmful, and this field is reported rather than vouched for like the rest.
func dlqHops(src nats.Header) int {
	n, err := strconv.Atoi(src.Get(DLQHopsHeader))
	if err != nil || n < 0 {
		return 0
	}
	return n
}

// Capture does not dedupe, and that is a decision rather than an omission.
//
// A captured copy goes to the DLQ with NO Nats-Msg-Id, so the broker stores every
// publish. The dedupe that used to be here existed for one narrow case: when the
// DLQ publish succeeds but the original's Ack does not, the message redelivers and
// is captured again, and a stable key let the duplicate window collapse the second
// copy onto the first.
//
// Every identity that can be synthesized for that has a collision class, and each
// one found so far was found in review rather than by reasoning ahead. The
// publisher's own Nats-Msg-Id collapses distinct originals, because JetStream
// dedupes on it stream-wide across every subject in the DLQ. Origin stream and
// sequence collapse across a stream that was deleted and recreated, since
// sequences restart. Adding the domain and the store timestamp closes those two —
// and the next scheme would meet the next broker behaviour the same way.
//
// The requirement is what gives, because the two failure directions are not
// comparable. A capture that stores a second copy is VISIBLE: a duplicate in a DLQ
// is a record someone can read, reconcile and discard, and at-least-once delivery
// already puts duplicates everywhere else in JetStream. A capture suppressed by a
// key collision is INVISIBLE: the broker answers with a successful PubAck marked
// Duplicate, the caller reads success and Acks the original, and the message is
// gone with no record — defeating the one guarantee this whole path exists to
// provide. So capture chooses its error direction the way the breaker's clock does
// (late, never early): duplicate, never suppressed.
//
// The cost is an occasional extra record in the failed-Ack window, bounded by the
// deliveries left on the ladder. Whoever replays can dedupe with far more context
// than a publish-time key affords, keying on the origin provenance above —
// [DLQOriginStreamHeader], [DLQStreamSeqHeader], [DLQOriginDomainHeader] and
// [DLQOriginTimestampHeader] identify the stored message the copy was made from.

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
// namespace does not survive the copy — those are directives to the broker, and on
// the DLQ they would be obeyed against the wrong stream — and provenance is put
// back on top afterwards. See [keepsHeaderOnCapture] for the boundary and why it is
// a namespace rule rather than a list.
//
// The copy carries NO Nats-Msg-Id, so capture never dedupes; see the note above
// [DLQPublisher] for why that is deliberate and what it costs.
//
// # Write-once origin, fresh per-hop
//
// Capturing the same message twice must not lose what the first capture recorded.
// That happens for an ordinary reason: when the DLQ publish succeeds but the
// original's Ack does not, the message redelivers and is captured again. It also
// happens when a replay tool gives up on a DLQ record and dead-letters that.
//
// So the origin headers ([dlqCarriedProvenance]) are WRITE-ONCE: a value already on
// the message is carried through untouched, and only an absent one is filled in
// from the message being captured. First-hop identity — publisher's Nats-Msg-Id,
// origin subject, and the stream, sequence, domain and store time it was first
// stored at — therefore survives any number of hops without needing intermediate
// records to still exist. Per-hop fields are the opposite: reason and delivery
// count describe the capture that just happened and are written fresh, with
// [DLQHopsHeader] counting how many captures a record has been through.
//
// One limit, stated rather than implied: no header-level rule can distinguish an
// origin header written by an earlier hop from one a producer set on a message it
// published. Origin headers are what the capture chain REPORTED. What this package
// guarantees is the shape — only its own keys appear, per-hop fields are always its
// own, and an invented Nats-Dlq- field never survives.
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
	meta, metaErr := msg.Metadata()
	if metaErr != nil {
		meta = nil
	}

	// The origin block is written once, as a block, and afterwards only carried.
	// Deciding that from [DLQOriginRecordedHeader] rather than from whether each
	// field looks empty is what keeps a later hop from answering a question an
	// earlier one already answered with "nothing" — see that header.
	if src.Get(DLQOriginRecordedHeader) != "" {
		// The namespace filter dropped these with the rest of Nats-; put back
		// exactly the closed set and nothing else, absences included.
		for _, h := range dlqCarriedProvenance {
			if v := src.Get(h); v != "" {
				hdr.Set(h, v)
			}
		}
	} else {
		hdr.Set(DLQOriginRecordedHeader, "1")
		record := func(h, v string) {
			if v != "" {
				hdr.Set(h, v)
			}
		}
		record(DLQOriginHeader, msg.Subject())
		record(DLQOriginMsgIDHeader, src.Get(jetstream.MsgIDHeader))
		if meta != nil {
			record(DLQStreamSeqHeader, strconv.FormatUint(meta.Sequence.Stream, 10))
			record(DLQOriginStreamHeader, meta.Stream)
			// Empty outside a JetStream domain, and left absent when it is: the
			// marker above already says the block was recorded, so absent reads as
			// "no domain" rather than "ask the next hop".
			record(DLQOriginDomainHeader, meta.Domain)
			if !meta.Timestamp.IsZero() {
				record(DLQOriginTimestampHeader, meta.Timestamp.UTC().Format(time.RFC3339Nano))
			}
		}
	}

	// Per-hop: always this capture's own.
	hdr.Set(DLQReasonHeader, reason)
	hdr.Set(DLQHopsHeader, strconv.Itoa(dlqHops(src)+1))
	if meta != nil {
		hdr.Set(DLQDeliveredHeader, strconv.FormatUint(meta.NumDelivered, 10))
	}

	out := &nats.Msg{Subject: dlqSubject, Data: msg.Data(), Header: hdr}

	pubCtx, cancel := context.WithTimeout(ctx, dlqPublishTimeout)
	defer cancel()
	if _, err := pub.PublishMsg(pubCtx, out); err != nil {
		return fmt.Errorf("dead-letter publish to %q: %w", dlqSubject, err)
	}
	return nil
}
