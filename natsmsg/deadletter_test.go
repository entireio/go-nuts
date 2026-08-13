package natsmsg_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/entireio/go-nuts/natsmsg"
	"github.com/entireio/go-nuts/natsmsg/natsmsgtest"
)

// dlqStreamName is the stream the fake publisher stands in for. A publish
// carrying an expectation about any OTHER stream is rejected, exactly as the
// broker does.
const dlqStreamName = "repo_ops_dlq_v1"

// storedAt stands in for the broker's store time on the fixtures' originals. Real
// deliveries always carry one; it is part of the copy's dedupe key because it is
// what tells two incarnations of the same stream sequence apart.
var storedAt = time.Date(2026, 8, 13, 9, 41, 12, 345678901, time.UTC)

// wantKey is the dedupe key a copy of the fixtures' stream at seq is expected to
// carry: no domain, the origin stream, the origin sequence, the store time.
func wantKey(seq uint64) string {
	return "_.repo_ops_v1." + strconv.FormatUint(seq, 10) + "." +
		strconv.FormatInt(storedAt.UnixNano(), 10)
}

type fakeDLQPublisher struct {
	msgs    []*nats.Msg
	seenIDs map[string]bool
	err     error
}

// PublishMsg enforces the one broker behaviour this file's regression test
// turns on: Nats-Expected-Stream is checked against the stream being published
// TO, and a mismatch is refused with err 10060. Without this the "control
// headers are stripped" test would pass just as happily on the broken code,
// because a fake that ignores expectations cannot fail the way the broker does.
func (f *fakeDLQPublisher) PublishMsg(_ context.Context, m *nats.Msg, _ ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	if f.err != nil {
		return nil, f.err
	}
	if want := m.Header.Get(jetstream.ExpectedStreamHeader); want != "" && want != dlqStreamName {
		return nil, &jetstream.APIError{ErrorCode: 10060, Code: 400,
			Description: fmt.Sprintf("expected stream does not match (%s)", want)}
	}
	// Dedupe the way JetStream does: on Nats-Msg-Id alone, STREAM-wide across
	// every subject, returning a SUCCESSFUL PubAck marked Duplicate and storing
	// nothing. A fake that skipped this could not show the difference between
	// deduping the same original twice and collapsing two different ones.
	if id := m.Header.Get(jetstream.MsgIDHeader); id != "" {
		if f.seenIDs == nil {
			f.seenIDs = map[string]bool{}
		}
		if f.seenIDs[id] {
			return &jetstream.PubAck{Stream: dlqStreamName, Duplicate: true}, nil
		}
		f.seenIDs[id] = true
	}
	f.msgs = append(f.msgs, m)
	return &jetstream.PubAck{Stream: dlqStreamName, Sequence: uint64(len(f.msgs))}, nil
}

var _ natsmsg.DLQPublisher = (*fakeDLQPublisher)(nil)

func TestSubjectToken(t *testing.T) {
	cases := map[string]string{
		"bad completion payload": "bad_completion_payload",
		"bad subject":            "bad_subject",
		"unparseable repo_ulid":  "unparseable_repo_ulid",
		"panic":                  "panic",
		"already-ok_token":       "already-ok_token",
		"weird.*>chars":          "weird___chars",
		"":                       "unknown",
	}
	for in, want := range cases {
		if got := natsmsg.SubjectToken(in); got != want {
			t.Errorf("SubjectToken(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestDeadLetter(t *testing.T) {
	orig := nats.Header{}
	orig.Set("Nats-Msg-Id", "teardown/123")
	orig.Set("traceparent", "00-abc-def-01")
	msg := &natsmsgtest.FakeMsg{
		SubjectVal: "repo.ops.v1.us.acme.teardown",
		DataVal:    []byte(`{"target":{"ulid":"X"}}`),
		HeadersVal: orig,
		Meta:       &jetstream.MsgMetadata{NumDelivered: 4, Stream: "repo_ops_v1", Sequence: jetstream.SequencePair{Stream: 999}, Timestamp: storedAt},
	}

	t.Run("captures payload + provenance", func(t *testing.T) {
		pub := &fakeDLQPublisher{}
		if err := natsmsg.DeadLetter(context.Background(), pub, "repo.ops.dlq.v1.teardown.bad_body", msg, "bad_body"); err != nil {
			t.Fatalf("DeadLetter: %v", err)
		}
		if len(pub.msgs) != 1 {
			t.Fatalf("published %d msgs, want 1", len(pub.msgs))
		}
		got := pub.msgs[0]
		if got.Subject != "repo.ops.dlq.v1.teardown.bad_body" {
			t.Errorf("subject = %q", got.Subject)
		}
		if string(got.Data) != string(msg.DataVal) {
			t.Errorf("data not preserved: %q", got.Data)
		}
		if got.Header.Get(natsmsg.DLQReasonHeader) != "bad_body" {
			t.Errorf("reason = %q", got.Header.Get(natsmsg.DLQReasonHeader))
		}
		if got.Header.Get(natsmsg.DLQOriginHeader) != "repo.ops.v1.us.acme.teardown" {
			t.Errorf("origin = %q", got.Header.Get(natsmsg.DLQOriginHeader))
		}
		if got.Header.Get(natsmsg.DLQDeliveredHeader) != "4" {
			t.Errorf("delivered = %q", got.Header.Get(natsmsg.DLQDeliveredHeader))
		}
		if got.Header.Get(natsmsg.DLQStreamSeqHeader) != "999" {
			t.Errorf("stream seq = %q", got.Header.Get(natsmsg.DLQStreamSeqHeader))
		}
		// Original headers are preserved alongside the provenance ones — except
		// the dedupe key, which is re-scoped to the copy and kept as provenance.
		if got.Header.Get("traceparent") != "00-abc-def-01" {
			t.Errorf("original headers not preserved: %v", got.Header)
		}
		if got.Header.Get(natsmsg.DLQOriginMsgIDHeader) != "teardown/123" {
			t.Errorf("origin msg id = %q", got.Header.Get(natsmsg.DLQOriginMsgIDHeader))
		}
		if got.Header.Get(jetstream.MsgIDHeader) != wantKey(999) {
			t.Errorf("copy dedupe key = %q, want %q", got.Header.Get(jetstream.MsgIDHeader), wantKey(999))
		}
	})

	t.Run("publish failure returns error so caller does not ack", func(t *testing.T) {
		pub := &fakeDLQPublisher{err: errors.New("nats down")}
		if err := natsmsg.DeadLetter(context.Background(), pub, "repo.ops.dlq.v1.teardown.bad_body", msg, "bad_body"); err == nil {
			t.Fatal("expected error when the DLQ publish fails")
		}
	})
}

// TestDeadLetterStripsJetStreamControlHeaders is the regression test for a
// producer class that could never be captured. Publish-control headers name the
// stream they are asserted about, so copying them onto the DLQ publish asserts
// them against the DLQ — false by construction. An original carrying
// Nats-Expected-Stream therefore failed capture with err 10060 on every
// delivery, rode the whole ladder, and ended stranded with the ack floor still
// pinned; the breaker terminated through the same capture and was defeated
// identically. One producer setting one header defeated "never drop, never
// strand silently".
func TestDeadLetterStripsJetStreamControlHeaders(t *testing.T) {
	orig := nats.Header{}
	orig.Set(jetstream.ExpectedStreamHeader, "repo_ops_v1") // the ORIGINAL's stream, not the DLQ's
	orig.Set(jetstream.ExpectedLastSeqHeader, "41")
	orig.Set(jetstream.ExpectedLastSubjSeqHeader, "7")
	orig.Set(jetstream.ExpectedLastSubjSeqSubjHeader, "repo.ops.v1.us.acme.teardown")
	orig.Set(jetstream.ExpectedLastMsgIDHeader, "teardown/122")
	orig.Set(jetstream.MsgRollup, "sub")
	orig.Set(jetstream.MsgTTLHeader, "30s")
	orig.Set(jetstream.ScheduleHeader, "@every 1h")
	orig.Set(jetstream.ScheduleTargetHeader, "somewhere.else")
	orig.Set(jetstream.ScheduleSourceHeader, "repo.ops.v1.>")
	orig.Set(jetstream.ScheduleTTLHeader, "5m")
	orig.Set(jetstream.ScheduleTimeZoneHeader, "America/New_York")
	orig.Set("Nats-Schedule-Rollup", "sub")
	orig.Set("Nats-Incr", "1")
	orig.Set("Nats-Counter-Sources", `{"s":{"foo":"1"}}`)
	orig.Set("Nats-Batch-Id", "uuid")
	orig.Set("Nats-Batch-Sequence", "1")
	orig.Set("Nats-Batch-Commit", "1")
	orig.Set(jetstream.MsgIDHeader, "teardown/123")
	orig.Set("traceparent", "00-abc-def-01")

	msg := &natsmsgtest.FakeMsg{
		SubjectVal: "repo.ops.v1.us.acme.teardown",
		DataVal:    []byte(`{"target":{"ulid":"X"}}`),
		HeadersVal: orig,
		Meta:       &jetstream.MsgMetadata{NumDelivered: 4, Stream: "repo_ops_v1", Sequence: jetstream.SequencePair{Stream: 999}, Timestamp: storedAt},
	}

	pub := &fakeDLQPublisher{}
	// The capture must SUCCEED: on the broken code the fake refuses this
	// publish with 10060, exactly as the broker did.
	if err := natsmsg.DeadLetter(context.Background(), pub, "repo.ops.dlq.v1.teardown.bad_body", msg, "bad_body"); err != nil {
		t.Fatalf("DeadLetter on a message carrying publish expectations: %v", err)
	}
	if len(pub.msgs) != 1 {
		t.Fatalf("published %d msgs, want 1", len(pub.msgs))
	}
	got := pub.msgs[0]

	for _, h := range []string{
		jetstream.ExpectedStreamHeader,
		jetstream.ExpectedLastSeqHeader,
		jetstream.ExpectedLastSubjSeqHeader,
		jetstream.ExpectedLastSubjSeqSubjHeader,
		jetstream.ExpectedLastMsgIDHeader,
		// Nats-TTL would make the captured RECORD expire on the source stream's
		// retention terms, or fail the capture on a DLQ that disallows it.
		jetstream.MsgTTLHeader,
		jetstream.MsgRollup,
		// A copied schedule turns the captured record into a scheduled publish,
		// and a copied target delivers it somewhere else entirely.
		jetstream.ScheduleHeader,
		jetstream.ScheduleTargetHeader,
		jetstream.ScheduleSourceHeader,
		jetstream.ScheduleTTLHeader,
		jetstream.ScheduleTimeZoneHeader,
		// Not exported by the pinned client, so named literally here — and the
		// reason the filter is a namespace rule rather than this list. Measured
		// against the real broker in internal/brokersemantics.
		"Nats-Incr",
		"Nats-Counter-Sources",
		"Nats-Batch-Id",
		"Nats-Batch-Sequence",
		"Nats-Batch-Commit",
		"Nats-Schedule-Rollup",
	} {
		if v := got.Header.Get(h); v != "" {
			t.Errorf("%s = %q on the captured copy, want it stripped", h, v)
		}
	}

	// Nats-Msg-Id does not survive either — it is replaced by a key scoped to
	// this copy, with the publisher's own kept as provenance. See
	// TestDeadLetterScopesTheDedupeKeyToTheCopy.
	if v := got.Header.Get(jetstream.MsgIDHeader); v != wantKey(999) {
		t.Errorf("%s = %q, want the DLQ-scoped key %q", jetstream.MsgIDHeader, v, wantKey(999))
	}
	if v := got.Header.Get(natsmsg.DLQOriginMsgIDHeader); v != "teardown/123" {
		t.Errorf("%s = %q, want the publisher's key kept as provenance", natsmsg.DLQOriginMsgIDHeader, v)
	}
	// Everything that is not a broker directive still rides along.
	if v := got.Header.Get("traceparent"); v != "00-abc-def-01" {
		t.Errorf("traceparent = %q, want it preserved", v)
	}
	if v := got.Header.Get(natsmsg.DLQStreamSeqHeader); v != "999" {
		t.Errorf("%s = %q, want provenance intact", natsmsg.DLQStreamSeqHeader, v)
	}

	// The original is not mutated — the caller may still need its headers.
	if v := msg.HeadersVal.Get(jetstream.ExpectedStreamHeader); v != "repo_ops_v1" {
		t.Errorf("original's %s = %q, want it untouched", jetstream.ExpectedStreamHeader, v)
	}
}

// TestDeadLetterScopesTheDedupeKeyToTheCopy is the regression test for a silent
// data-loss path.
//
// JetStream dedupes on Nats-Msg-Id alone, stream-wide across every subject in
// the DLQ. Carrying the publisher's own ID onto the copy meant two DIFFERENT
// originals sharing an ID inside the duplicate window collapsed onto one record:
// the second publish returned a SUCCESSFUL PubAck with Duplicate set, DeadLetter
// reported success, and the caller acked an original whose copy was never
// stored. The message was gone with no DLQ record — the precise failure the
// capture path exists to prevent.
//
// The copy's key is now derived from the original's own identity (origin stream
// and stream sequence), so re-capturing the SAME message still collapses — which
// is the wanted dedupe, since a failed Ack has the message redelivered and
// captured again — while distinct originals can never collide.
func TestDeadLetterScopesTheDedupeKeyToTheCopy(t *testing.T) {
	// Two different messages, on different subjects and sequences, whose
	// producer happened to derive the same dedupe key.
	const sharedID = "repo/01KWHDFJ0C"
	newMsg := func(subject string, seq uint64) *natsmsgtest.FakeMsg {
		h := nats.Header{}
		h.Set(jetstream.MsgIDHeader, sharedID)
		return &natsmsgtest.FakeMsg{
			SubjectVal: subject,
			DataVal:    []byte(`{"seq":` + strconv.FormatUint(seq, 10) + `}`),
			HeadersVal: h,
			Meta: &jetstream.MsgMetadata{
				NumDelivered: 6,
				Stream:       "repo_ops_v1",
				Sequence:     jetstream.SequencePair{Stream: seq},
				Timestamp:    storedAt,
			},
		}
	}
	first, second := newMsg("repo.ops.v1.us.acme.teardown", 41), newMsg("repo.ops.v1.us.acme.rebuild", 77)

	pub := &fakeDLQPublisher{}
	for _, m := range []*natsmsgtest.FakeMsg{first, second} {
		if err := natsmsg.DeadLetter(context.Background(), pub, "repo.ops.dlq.v1.bad_body", m, "bad_body"); err != nil {
			t.Fatalf("DeadLetter(%s): %v", m.SubjectVal, err)
		}
	}
	// Two distinct originals must leave two records. On the old behaviour both
	// carried sharedID and the second was silently swallowed as a duplicate.
	if len(pub.msgs) != 2 {
		t.Fatalf("stored %d DLQ records for 2 distinct messages, want 2: a collapse here is a lost message", len(pub.msgs))
	}
	if got := pub.msgs[0].Header.Get(jetstream.MsgIDHeader); got != wantKey(41) {
		t.Errorf("copy dedupe key = %q, want %q", got, wantKey(41))
	}
	if got := pub.msgs[1].Header.Get(jetstream.MsgIDHeader); got != wantKey(77) {
		t.Errorf("copy dedupe key = %q, want %q", got, wantKey(77))
	}
	// The publisher's key is not lost, just relocated.
	for i, m := range pub.msgs {
		if got := m.Header.Get(natsmsg.DLQOriginMsgIDHeader); got != sharedID {
			t.Errorf("record %d: %s = %q, want the original's key preserved as provenance",
				i, natsmsg.DLQOriginMsgIDHeader, got)
		}
		if got := m.Header.Get(natsmsg.DLQOriginStreamHeader); got != "repo_ops_v1" {
			t.Errorf("record %d: %s = %q", i, natsmsg.DLQOriginStreamHeader, got)
		}
	}

	// The dedupe that IS wanted still works: re-capturing the same original
	// after a failed Ack collapses onto its existing record rather than adding
	// a second copy to reconcile.
	if err := natsmsg.DeadLetter(context.Background(), pub, "repo.ops.dlq.v1.bad_body", first, "bad_body"); err != nil {
		t.Fatalf("re-capture of the same original: %v", err)
	}
	if len(pub.msgs) != 2 {
		t.Errorf("stored %d records after re-capturing a message already captured, want 2", len(pub.msgs))
	}
}

// TestDeadLetterKeepsOnlyApplicationHeaders pins the boundary itself rather than
// a list of names: nothing in NATS's reserved namespace survives the copy except
// this package's own provenance, and everything outside it survives untouched.
// That is what makes a directive added by a future NATS release safe by default.
func TestDeadLetterKeepsOnlyApplicationHeaders(t *testing.T) {
	orig := nats.Header{}
	// Application-owned: must all survive.
	orig.Set("traceparent", "00-abc-def-01")
	orig.Set("tracestate", "vendor=1")
	orig.Set("X-Entire-Repo", "01KWHDFJ0C")
	orig.Set("content-type", "application/json")
	orig.Add("X-Multi", "one")
	orig.Add("X-Multi", "two")
	// Reserved namespace: must not, whatever the name or casing. The invented
	// names stand in for directives NATS has not shipped yet.
	orig.Set("Nats-Some-Future-Directive", "on")
	orig.Set("nats-lowercased-directive", "on")
	orig.Set("NATS-SHOUTED-DIRECTIVE", "on")
	// A prior hop's provenance: dropped too, so nothing on the copy is provenance
	// this package did not write (see
	// TestDeadLetterDoesNotInheritProvenanceItDidNotWrite).
	orig.Set("Nats-Dlq-Reason", "earlier hop")

	msg := &natsmsgtest.FakeMsg{
		SubjectVal: "repo.ops.v1.us.acme.teardown",
		DataVal:    []byte(`{}`),
		HeadersVal: orig,
		Meta:       &jetstream.MsgMetadata{NumDelivered: 2, Stream: "repo_ops_v1", Sequence: jetstream.SequencePair{Stream: 5}, Timestamp: storedAt},
	}

	pub := &fakeDLQPublisher{}
	if err := natsmsg.DeadLetter(context.Background(), pub, "repo.ops.dlq.v1.bad_body", msg, "bad_body"); err != nil {
		t.Fatalf("DeadLetter: %v", err)
	}
	got := pub.msgs[0].Header

	for k, want := range map[string]string{
		"traceparent":   "00-abc-def-01",
		"tracestate":    "vendor=1",
		"X-Entire-Repo": "01KWHDFJ0C",
		"content-type":  "application/json",
	} {
		if got.Get(k) != want {
			t.Errorf("application header %s = %q, want %q", k, got.Get(k), want)
		}
	}
	if len(got["X-Multi"]) != 2 {
		t.Errorf("X-Multi = %v, want both values preserved", got["X-Multi"])
	}
	for _, k := range []string{"Nats-Some-Future-Directive", "nats-lowercased-directive", "NATS-SHOUTED-DIRECTIVE"} {
		if v := got.Get(k); v != "" {
			t.Errorf("reserved-namespace header %s = %q, want it dropped", k, v)
		}
	}
	// The provenance on the copy is this hop's, written after the filter ran.
	if got.Get(natsmsg.DLQReasonHeader) != "bad_body" {
		t.Errorf("%s = %q, want this hop's reason", natsmsg.DLQReasonHeader, got.Get(natsmsg.DLQReasonHeader))
	}

	// Belt and braces: enumerate what the copy actually carries from the reserved
	// namespace. Exactly two things may — this package's own Nats-Dlq- provenance,
	// and the Nats-Msg-Id it authors itself for the copy's dedupe. Nothing that
	// came from the original survives. Nats-Msg-Id is safe to author where the
	// rejected directives were not: dedupe is always available on a stream, so it
	// can never fail the capture.
	for k := range got {
		low := strings.ToLower(k)
		if !strings.HasPrefix(low, "nats-") {
			continue
		}
		if strings.HasPrefix(low, strings.ToLower(natsmsg.DLQHeaderPrefix)) || low == "nats-msg-id" {
			continue
		}
		t.Errorf("captured copy carries reserved header %q from the original", k)
	}
}

// TestDeadLetterDedupeKeyDistinguishesStreamIncarnations: a stream sequence names
// a message only within one incarnation of one stream. Delete a stream and
// recreate it and numbering restarts, so a DLQ that outlives the source — the
// normal case, since the DLQ is what the source's messages are rescued into —
// would see one stream/sequence pair naming two unrelated messages. Keyed on that
// pair alone the second capture collapsed onto the first and the caller acked an
// original with no stored copy.
//
// The store time separates them, and the domain separates same-named streams in
// different JetStream domains. Measured against a real recreated stream in
// internal/brokersemantics; this pins the key's shape.
func TestDeadLetterDedupeKeyDistinguishesStreamIncarnations(t *testing.T) {
	newMsg := func(stream, domain string, seq uint64, stored time.Time) *natsmsgtest.FakeMsg {
		return &natsmsgtest.FakeMsg{
			SubjectVal: "repo.ops.v1.us.acme.teardown",
			DataVal:    []byte(`{}`),
			HeadersVal: nats.Header{},
			Meta: &jetstream.MsgMetadata{
				NumDelivered: 1, Stream: stream, Domain: domain,
				Sequence: jetstream.SequencePair{Stream: seq}, Timestamp: stored,
			},
		}
	}
	later := storedAt.Add(90 * time.Second)

	cases := map[string]struct{ a, b *natsmsgtest.FakeMsg }{
		"same stream and sequence, different incarnation": {
			newMsg("repo_ops_v1", "", 1, storedAt),
			newMsg("repo_ops_v1", "", 1, later),
		},
		"same stream and sequence, different domain": {
			newMsg("repo_ops_v1", "hub", 1, storedAt),
			newMsg("repo_ops_v1", "leaf", 1, storedAt),
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			pub := &fakeDLQPublisher{}
			for _, m := range []*natsmsgtest.FakeMsg{tc.a, tc.b} {
				if err := natsmsg.DeadLetter(context.Background(), pub, "repo.ops.dlq.v1.bad_body", m, "bad_body"); err != nil {
					t.Fatalf("DeadLetter: %v", err)
				}
			}
			if len(pub.msgs) != 2 {
				t.Errorf("stored %d records for 2 distinct messages, want 2: a collapse here is a lost message", len(pub.msgs))
			}
		})
	}

	// The other half of the contract: the key must NOT vary with anything that
	// changes between deliveries of one message, or the re-capture after a failed
	// Ack would pile up near-duplicates instead of collapsing.
	t.Run("stable across redeliveries of one message", func(t *testing.T) {
		pub := &fakeDLQPublisher{}
		for _, delivered := range []uint64{1, 4, 6} {
			m := newMsg("repo_ops_v1", "", 41, storedAt)
			m.Meta.NumDelivered = delivered
			if err := natsmsg.DeadLetter(context.Background(), pub, "repo.ops.dlq.v1.bad_body", m, "bad_body"); err != nil {
				t.Fatalf("DeadLetter (delivery %d): %v", delivered, err)
			}
		}
		if len(pub.msgs) != 1 {
			t.Errorf("stored %d records for 3 captures of ONE message, want 1", len(pub.msgs))
		}
	})
}

// TestDeadLetterDoesNotInheritProvenanceItDidNotWrite: provenance describes one
// hop, and every Nats-Dlq- header on a copy is written by the capture that made
// it. An inbound one is dropped like any other reserved header — otherwise a
// producer could hand a replay tool provenance that looks like this package's, and
// the fields get overwritten for the current hop anyway, so retaining them
// preserved nothing while lending a stranger's claims the library's authority.
func TestDeadLetterDoesNotInheritProvenanceItDidNotWrite(t *testing.T) {
	// A producer forging every provenance field, plus a plausible-looking hop
	// from some earlier DLQ.
	orig := nats.Header{}
	orig.Set(natsmsg.DLQReasonHeader, "not the real reason")
	orig.Set(natsmsg.DLQOriginHeader, "some.other.subject")
	orig.Set(natsmsg.DLQOriginStreamHeader, "not_the_real_stream")
	orig.Set(natsmsg.DLQStreamSeqHeader, "1")
	orig.Set(natsmsg.DLQDeliveredHeader, "99")
	orig.Set(natsmsg.DLQOriginMsgIDHeader, "forged/id")
	orig.Set("Nats-Dlq-Invented-Field", "whatever")
	orig.Set(jetstream.MsgIDHeader, "producer/real")

	msg := &natsmsgtest.FakeMsg{
		SubjectVal: "repo.ops.v1.us.acme.teardown",
		DataVal:    []byte(`{}`),
		HeadersVal: orig,
		Meta: &jetstream.MsgMetadata{
			NumDelivered: 4, Stream: "repo_ops_v1",
			Sequence: jetstream.SequencePair{Stream: 999}, Timestamp: storedAt,
		},
	}

	pub := &fakeDLQPublisher{}
	if err := natsmsg.DeadLetter(context.Background(), pub, "repo.ops.dlq.v1.bad_body", msg, "bad_body"); err != nil {
		t.Fatalf("DeadLetter: %v", err)
	}
	got := pub.msgs[0].Header

	// Every field describes THIS capture, not the forged one.
	for h, want := range map[string]string{
		natsmsg.DLQReasonHeader:       "bad_body",
		natsmsg.DLQOriginHeader:       "repo.ops.v1.us.acme.teardown",
		natsmsg.DLQOriginStreamHeader: "repo_ops_v1",
		natsmsg.DLQStreamSeqHeader:    "999",
		natsmsg.DLQDeliveredHeader:    "4",
		// Authored from the Nats-Msg-Id the message really carried, not from the
		// forged Origin-Msg-Id.
		natsmsg.DLQOriginMsgIDHeader: "producer/real",
	} {
		if got.Get(h) != want {
			t.Errorf("%s = %q, want %q — provenance must describe this capture", h, got.Get(h), want)
		}
	}
	// A field this package does not write does not survive just because it is in
	// the namespace.
	if v := got.Get("Nats-Dlq-Invented-Field"); v != "" {
		t.Errorf("Nats-Dlq-Invented-Field = %q, want it dropped; nothing inbound is trusted", v)
	}
}

// TestDeadLetterWithoutMetadataOmitsTheDedupeKey: a synthetic message has no
// stream identity to scope a key to, so the copy carries none. A re-capture
// would then leave two records — the right way to be wrong, since a duplicate is
// reconcilable and a collapse onto an unrelated message is not.
func TestDeadLetterWithoutMetadataOmitsTheDedupeKey(t *testing.T) {
	h := nats.Header{}
	h.Set(jetstream.MsgIDHeader, "producer-key")
	msg := &natsmsgtest.FakeMsg{
		SubjectVal: "repo.ops.v1.synthetic",
		DataVal:    []byte(`{}`),
		HeadersVal: h,
		MetaErr:    errors.New("not a jetstream message"),
	}

	pub := &fakeDLQPublisher{}
	if err := natsmsg.DeadLetter(context.Background(), pub, "repo.ops.dlq.v1.bad_body", msg, "bad_body"); err != nil {
		t.Fatalf("DeadLetter: %v", err)
	}
	if len(pub.msgs) != 1 {
		t.Fatalf("published %d msgs, want 1", len(pub.msgs))
	}
	if got := pub.msgs[0].Header.Get(jetstream.MsgIDHeader); got != "" {
		t.Errorf("%s = %q, want none — the producer's key must not dedupe the copy",
			jetstream.MsgIDHeader, got)
	}
	if got := pub.msgs[0].Header.Get(natsmsg.DLQOriginMsgIDHeader); got != "producer-key" {
		t.Errorf("%s = %q, want the original's key kept as provenance", natsmsg.DLQOriginMsgIDHeader, got)
	}
}
