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

// traceparent is an application-owned header: it must survive every capture
// untouched, which several tests here check.
const traceparent = "00-abc-def-01"

// storedAt stands in for the broker's store time on the fixtures' originals. Real
// deliveries always carry one; it is recorded as origin provenance because it is
// what tells two incarnations of the same stream sequence apart at replay time.
var storedAt = time.Date(2026, 8, 13, 9, 41, 12, 345678901, time.UTC)

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
	// Dedupe the way JetStream does: on Nats-Msg-Id alone, STREAM-wide across every
	// subject, returning a SUCCESSFUL PubAck marked Duplicate and storing nothing.
	// Captured copies no longer carry an ID, so this never fires for them — it is
	// kept as the GUARD that makes that fact testable. Reintroduce a dedupe identity
	// and the "one record per capture" tests here start failing, which is exactly the
	// silent suppression the no-dedupe decision exists to prevent.
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
	orig.Set("traceparent", traceparent)
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
		// Application headers are preserved alongside the provenance ones. The
		// publisher's dedupe key moves into the provenance namespace and the copy
		// goes out with none of its own — capture does not dedupe.
		if got.Header.Get("traceparent") != traceparent {
			t.Errorf("original headers not preserved: %v", got.Header)
		}
		if got.Header.Get(natsmsg.DLQOriginMsgIDHeader) != "teardown/123" {
			t.Errorf("origin msg id = %q", got.Header.Get(natsmsg.DLQOriginMsgIDHeader))
		}
		if v := got.Header.Get(jetstream.MsgIDHeader); v != "" {
			t.Errorf("%s = %q, want no dedupe identity on the copy", jetstream.MsgIDHeader, v)
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
	orig.Set("traceparent", traceparent)

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

	// Nats-Msg-Id does not survive either, and nothing replaces it: the copy
	// carries no dedupe identity, so no capture can be suppressed. See
	// TestDeadLetterNeverSuppressesACapture.
	if v := got.Header.Get(jetstream.MsgIDHeader); v != "" {
		t.Errorf("%s = %q, want none on the copy", jetstream.MsgIDHeader, v)
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

// TestDeadLetterKeepsOnlyApplicationHeaders pins the boundary itself rather than a
// list of names: nothing in NATS's reserved namespace passes the filter, and
// everything outside it survives untouched. That is what makes a directive added by
// a future NATS release safe by default. Provenance is then written back on top, by
// name — see TestDeadLetterWritesOriginProvenanceOnce.
func TestDeadLetterKeepsOnlyApplicationHeaders(t *testing.T) {
	orig := nats.Header{}
	// Application-owned: must all survive.
	orig.Set("traceparent", traceparent)
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
	// A per-hop provenance field from an earlier hop: not carried, because reason
	// describes the capture that just happened.
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
		"traceparent":   traceparent,
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

	// Belt and braces: the ONLY reserved headers a copy may carry are this
	// package's own Nats-Dlq- provenance. Not even Nats-Msg-Id, since capture no
	// longer dedupes.
	for k := range got {
		low := strings.ToLower(k)
		if !strings.HasPrefix(low, "nats-") {
			continue
		}
		if strings.HasPrefix(low, strings.ToLower(natsmsg.DLQHeaderPrefix)) {
			continue
		}
		t.Errorf("captured copy carries reserved header %q", k)
	}
}

// TestDeadLetterNeverSuppressesACapture is the invariant capture-time dedupe was
// traded away for: every capture leaves a visible record, whatever it is a capture
// of.
//
// Two schemes for a copy's Nats-Msg-Id were tried and each had a collision class
// found in review — the publisher's own ID collapses distinct originals because
// JetStream dedupes on it stream-wide, and origin stream plus sequence collapses
// across a recreated stream. Both failed in the invisible direction: the broker
// answers a suppressed publish with a SUCCESSFUL PubAck marked Duplicate, the
// caller reads success and Acks the original, and the message is gone with no
// record. So the copy now carries no dedupe identity at all. A duplicate record is
// readable and reconcilable; a suppressed one is neither.
func TestDeadLetterNeverSuppressesACapture(t *testing.T) {
	newMsg := func(subject string, seq uint64, producerID string) *natsmsgtest.FakeMsg {
		h := nats.Header{}
		if producerID != "" {
			h.Set(jetstream.MsgIDHeader, producerID)
		}
		return &natsmsgtest.FakeMsg{
			SubjectVal: subject,
			DataVal:    []byte(`{"seq":` + strconv.FormatUint(seq, 10) + `}`),
			HeadersVal: h,
			Meta: &jetstream.MsgMetadata{
				NumDelivered: 6, Stream: "repo_ops_v1",
				Sequence: jetstream.SequencePair{Stream: seq}, Timestamp: storedAt,
			},
		}
	}

	t.Run("distinct originals sharing a producer dedupe key", func(t *testing.T) {
		// The collision that defeated carrying Nats-Msg-Id through: one producer
		// deriving the same key for two messages.
		const shared = "repo/01KWHDFJ0C"
		pub := &fakeDLQPublisher{}
		for _, m := range []*natsmsgtest.FakeMsg{
			newMsg("repo.ops.v1.us.acme.teardown", 41, shared),
			newMsg("repo.ops.v1.us.acme.rebuild", 77, shared),
		} {
			if err := natsmsg.DeadLetter(context.Background(), pub, "repo.ops.dlq.v1.bad_body", m, "bad_body"); err != nil {
				t.Fatalf("DeadLetter(%s): %v", m.SubjectVal, err)
			}
		}
		if len(pub.msgs) != 2 {
			t.Fatalf("stored %d records for 2 distinct messages, want 2", len(pub.msgs))
		}
	})

	t.Run("same stream sequence from a recreated stream", func(t *testing.T) {
		// The collision that defeated <stream>/<seq>: numbering restarts, so two
		// unrelated messages present sequence 1 on a stream of the same name.
		a, b := newMsg("repo.ops.v1.us.acme.teardown", 1, ""), newMsg("repo.ops.v1.us.acme.rebuild", 1, "")
		b.Meta.Timestamp = storedAt.Add(90 * time.Second)
		pub := &fakeDLQPublisher{}
		for _, m := range []*natsmsgtest.FakeMsg{a, b} {
			if err := natsmsg.DeadLetter(context.Background(), pub, "repo.ops.dlq.v1.bad_body", m, "bad_body"); err != nil {
				t.Fatalf("DeadLetter: %v", err)
			}
		}
		if len(pub.msgs) != 2 {
			t.Fatalf("stored %d records for 2 distinct messages, want 2", len(pub.msgs))
		}
	})

	t.Run("re-capturing one message leaves one record per capture", func(t *testing.T) {
		// The case the dedupe existed for: the DLQ publish lands, the Ack does not,
		// the message redelivers and is captured again. Three captures now leave
		// three records — exactly-N, not one. Bounded by the deliveries left on the
		// ladder, visible, and the accepted cost of never suppressing.
		pub := &fakeDLQPublisher{}
		for _, delivered := range []uint64{4, 5, 6} {
			m := newMsg("repo.ops.v1.us.acme.teardown", 41, "producer/key")
			m.Meta.NumDelivered = delivered
			if err := natsmsg.DeadLetter(context.Background(), pub, "repo.ops.dlq.v1.bad_body", m, "bad_body"); err != nil {
				t.Fatalf("DeadLetter (delivery %d): %v", delivered, err)
			}
		}
		if len(pub.msgs) != 3 {
			t.Errorf("stored %d records for 3 captures, want 3 — suppression is the failure mode", len(pub.msgs))
		}
		for i, m := range pub.msgs {
			if v := m.Header.Get(jetstream.MsgIDHeader); v != "" {
				t.Errorf("record %d carries %s = %q, want no dedupe identity at all",
					i, jetstream.MsgIDHeader, v)
			}
		}
	})
}

// TestDeadLetterRecordsTheStoredMessageIdentity: with no publish-time dedupe, the
// identity of the captured message has to be legible in the record instead, because
// that is where dedupe now happens — at replay, by tooling or a human with full
// context. Origin stream, sequence, domain and store time together name one stored
// message, and the store time is what tells two incarnations of a recreated stream
// apart.
func TestDeadLetterRecordsTheStoredMessageIdentity(t *testing.T) {
	h := nats.Header{}
	h.Set(jetstream.MsgIDHeader, "producer/01KWHDFJ0C")
	msg := &natsmsgtest.FakeMsg{
		SubjectVal: "repo.ops.v1.us.acme.teardown",
		DataVal:    []byte(`{}`),
		HeadersVal: h,
		Meta: &jetstream.MsgMetadata{
			NumDelivered: 6, Stream: "repo_ops_v1", Domain: "hub",
			Sequence: jetstream.SequencePair{Stream: 41}, Timestamp: storedAt,
		},
	}

	pub := &fakeDLQPublisher{}
	if err := natsmsg.DeadLetter(context.Background(), pub, "repo.ops.dlq.v1.bad_body", msg, "bad_body"); err != nil {
		t.Fatalf("DeadLetter: %v", err)
	}
	got := pub.msgs[0].Header

	for h, want := range map[string]string{
		natsmsg.DLQOriginStreamHeader:    "repo_ops_v1",
		natsmsg.DLQStreamSeqHeader:       "41",
		natsmsg.DLQOriginDomainHeader:    "hub",
		natsmsg.DLQOriginTimestampHeader: storedAt.Format(time.RFC3339Nano),
		natsmsg.DLQOriginMsgIDHeader:     "producer/01KWHDFJ0C",
		natsmsg.DLQOriginHeader:          "repo.ops.v1.us.acme.teardown",
		natsmsg.DLQReasonHeader:          "bad_body",
		natsmsg.DLQDeliveredHeader:       "6",
		natsmsg.DLQHopsHeader:            "1",
	} {
		if got.Get(h) != want {
			t.Errorf("%s = %q, want %q", h, got.Get(h), want)
		}
	}
}

// TestDeadLetterWritesOriginProvenanceOnce: capturing a message twice must not lose
// what the first capture recorded.
//
// This is not an exotic path. When the DLQ publish succeeds but the original's Ack
// does not the message redelivers and is captured again, and a replay tool that
// gives up on a DLQ record dead-letters that record in turn. Origin headers are
// therefore write-once — carried through untouched when already present — while
// per-hop headers describe the capture that just happened and are rewritten, with
// the hop count saying how many there have been.
func TestDeadLetterWritesOriginProvenanceOnce(t *testing.T) {
	// A DLQ record as the previous hop wrote it, now being captured again.
	first := nats.Header{}
	first.Set(natsmsg.DLQOriginRecordedHeader, "1")
	first.Set(natsmsg.DLQOriginHeader, "repo.ops.v1.us.acme.teardown")
	first.Set(natsmsg.DLQOriginStreamHeader, "repo_ops_v1")
	first.Set(natsmsg.DLQStreamSeqHeader, "41")
	first.Set(natsmsg.DLQOriginDomainHeader, "hub")
	first.Set(natsmsg.DLQOriginTimestampHeader, storedAt.Format(time.RFC3339Nano))
	first.Set(natsmsg.DLQOriginMsgIDHeader, "producer/first-hop")
	first.Set(natsmsg.DLQReasonHeader, "undecodable")
	first.Set(natsmsg.DLQDeliveredHeader, "6")
	first.Set(natsmsg.DLQHopsHeader, "1")
	first.Set("traceparent", traceparent)
	// An invented field, and a producer-authored dedupe key on the record itself.
	first.Set("Nats-Dlq-Invented-Field", "whatever")
	first.Set(jetstream.MsgIDHeader, "dlq-record/999")

	// The re-capture reads from the DLQ stream, at its own sequence and time.
	recapture := &natsmsgtest.FakeMsg{
		SubjectVal: "repo.ops.dlq.v1.bad_body",
		DataVal:    []byte(`{}`),
		HeadersVal: first,
		Meta: &jetstream.MsgMetadata{
			NumDelivered: 3, Stream: "repo_ops_dlq_v1", Domain: "leaf",
			Sequence:  jetstream.SequencePair{Stream: 7},
			Timestamp: storedAt.Add(time.Hour),
		},
	}

	pub := &fakeDLQPublisher{}
	if err := natsmsg.DeadLetter(context.Background(), pub, "repo.ops.dlq.v2.gave_up", recapture, "gave up replaying"); err != nil {
		t.Fatalf("DeadLetter of a DLQ record: %v", err)
	}
	got := pub.msgs[0].Header

	// Every origin field still describes the FIRST hop, not this one.
	for h, want := range map[string]string{
		natsmsg.DLQOriginHeader:          "repo.ops.v1.us.acme.teardown",
		natsmsg.DLQOriginStreamHeader:    "repo_ops_v1",
		natsmsg.DLQStreamSeqHeader:       "41",
		natsmsg.DLQOriginDomainHeader:    "hub",
		natsmsg.DLQOriginTimestampHeader: storedAt.Format(time.RFC3339Nano),
		natsmsg.DLQOriginMsgIDHeader:     "producer/first-hop",
	} {
		if got.Get(h) != want {
			t.Errorf("%s = %q, want %q — origin provenance is write-once", h, got.Get(h), want)
		}
	}
	// Per-hop fields describe THIS capture.
	for h, want := range map[string]string{
		natsmsg.DLQReasonHeader:    "gave up replaying",
		natsmsg.DLQDeliveredHeader: "3",
		natsmsg.DLQHopsHeader:      "2",
	} {
		if got.Get(h) != want {
			t.Errorf("%s = %q, want %q — per-hop provenance is rewritten each capture", h, got.Get(h), want)
		}
	}
	// A field this package does not define never rides through on the prefix alone.
	if v := got.Get("Nats-Dlq-Invented-Field"); v != "" {
		t.Errorf("Nats-Dlq-Invented-Field = %q, want it dropped", v)
	}
	// Still no dedupe identity, and the record's own inbound one did not survive.
	if v := got.Get(jetstream.MsgIDHeader); v != "" {
		t.Errorf("%s = %q, want none", jetstream.MsgIDHeader, v)
	}
	if got.Get("traceparent") != traceparent {
		t.Errorf("traceparent = %q, want it preserved", got.Get("traceparent"))
	}
}

// TestDeadLetterDoesNotBackfillAnOriginThatWasRecordedAsAbsent is the regression for
// write-once's blind spot: an origin field can be legitimately EMPTY, and reading
// that as "not recorded yet" let a later hop answer for the first one.
//
// Two fields are legitimately absent. A message captured outside a JetStream domain
// has no origin domain; one captured with no metadata at all has no origin stream,
// sequence or store time. Field-by-field absence therefore cannot mean "uninitialized"
// — so an explicit marker carries that fact and the block is written once, whole.
// Without it the record ends up asserting the LATER hop's domain, or the DLQ stream
// it was read from, as where the message came from: provenance that is silently
// wrong, which is worse than provenance that is missing.
func TestDeadLetterDoesNotBackfillAnOriginThatWasRecordedAsAbsent(t *testing.T) {
	t.Run("origin recorded outside a domain, re-captured inside one", func(t *testing.T) {
		// Exactly what a first capture writes in a domainless deployment: the block
		// marked recorded, with no origin domain in it.
		rec := nats.Header{}
		rec.Set(natsmsg.DLQOriginRecordedHeader, "1")
		rec.Set(natsmsg.DLQOriginHeader, "repo.ops.v1.us.acme.teardown")
		rec.Set(natsmsg.DLQOriginStreamHeader, "repo_ops_v1")
		rec.Set(natsmsg.DLQStreamSeqHeader, "41")
		rec.Set(natsmsg.DLQOriginTimestampHeader, storedAt.Format(time.RFC3339Nano))

		// The re-capture happens inside a domain, so this hop's metadata has one.
		msg := &natsmsgtest.FakeMsg{
			SubjectVal: "repo.ops.dlq.v1.bad_body",
			DataVal:    []byte(`{}`),
			HeadersVal: rec,
			Meta: &jetstream.MsgMetadata{
				NumDelivered: 1, Stream: "repo_ops_dlq_v1", Domain: "leaf",
				Sequence: jetstream.SequencePair{Stream: 7}, Timestamp: storedAt.Add(time.Hour),
			},
		}

		pub := &fakeDLQPublisher{}
		if err := natsmsg.DeadLetter(context.Background(), pub, "repo.ops.dlq.v2.gave_up", msg, "gave up"); err != nil {
			t.Fatalf("DeadLetter: %v", err)
		}
		got := pub.msgs[0].Header

		if v := got.Get(natsmsg.DLQOriginDomainHeader); v != "" {
			t.Errorf("%s = %q, want it still absent — the origin was recorded without a "+
				"domain, and this hop's domain is not the message's origin", natsmsg.DLQOriginDomainHeader, v)
		}
		// And the fields that WERE recorded are untouched.
		if v := got.Get(natsmsg.DLQOriginStreamHeader); v != "repo_ops_v1" {
			t.Errorf("%s = %q, want repo_ops_v1", natsmsg.DLQOriginStreamHeader, v)
		}
	})

	t.Run("origin recorded without metadata, re-captured with it", func(t *testing.T) {
		// What a first capture of a synthetic message writes: marked recorded, with
		// no stream, sequence or store time — there was none to record.
		rec := nats.Header{}
		rec.Set(natsmsg.DLQOriginRecordedHeader, "1")
		rec.Set(natsmsg.DLQOriginHeader, "repo.ops.v1.synthetic")
		rec.Set(natsmsg.DLQOriginMsgIDHeader, "producer-key")

		msg := &natsmsgtest.FakeMsg{
			SubjectVal: "repo.ops.dlq.v1.bad_body",
			DataVal:    []byte(`{}`),
			HeadersVal: rec,
			Meta: &jetstream.MsgMetadata{
				NumDelivered: 2, Stream: "repo_ops_dlq_v1", Domain: "hub",
				Sequence: jetstream.SequencePair{Stream: 12}, Timestamp: storedAt,
			},
		}

		pub := &fakeDLQPublisher{}
		if err := natsmsg.DeadLetter(context.Background(), pub, "repo.ops.dlq.v2.gave_up", msg, "gave up"); err != nil {
			t.Fatalf("DeadLetter: %v", err)
		}
		got := pub.msgs[0].Header

		for _, h := range []string{
			natsmsg.DLQOriginStreamHeader, natsmsg.DLQStreamSeqHeader,
			natsmsg.DLQOriginDomainHeader, natsmsg.DLQOriginTimestampHeader,
		} {
			if v := got.Get(h); v != "" {
				t.Errorf("%s = %q, want it still absent — filling it in from this hop would "+
					"name the DLQ as the message's origin", h, v)
			}
		}
		if v := got.Get(natsmsg.DLQOriginHeader); v != "repo.ops.v1.synthetic" {
			t.Errorf("%s = %q, want the first hop's subject", natsmsg.DLQOriginHeader, v)
		}
		// Per-hop fields still describe this capture.
		if v := got.Get(natsmsg.DLQDeliveredHeader); v != "2" {
			t.Errorf("%s = %q, want 2", natsmsg.DLQDeliveredHeader, v)
		}
	})
}

// TestDeadLetterWithoutMetadataStillRecordsWhatItKnows: a synthetic message carries
// no stream identity, so the stored-message fields are absent rather than guessed.
// Origin subject and the publisher's key come off the message itself and are still
// written.
func TestDeadLetterWithoutMetadataStillRecordsWhatItKnows(t *testing.T) {
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
	got := pub.msgs[0].Header

	for h, want := range map[string]string{
		natsmsg.DLQOriginHeader:      "repo.ops.v1.synthetic",
		natsmsg.DLQOriginMsgIDHeader: "producer-key",
		natsmsg.DLQReasonHeader:      "bad_body",
		natsmsg.DLQHopsHeader:        "1",
		// Marked recorded even though most of the block is empty: that is the
		// point — a later hop must not read these absences as its cue.
		natsmsg.DLQOriginRecordedHeader: "1",
	} {
		if got.Get(h) != want {
			t.Errorf("%s = %q, want %q", h, got.Get(h), want)
		}
	}
	for _, h := range []string{
		natsmsg.DLQOriginStreamHeader, natsmsg.DLQStreamSeqHeader,
		natsmsg.DLQOriginDomainHeader, natsmsg.DLQOriginTimestampHeader,
		natsmsg.DLQDeliveredHeader,
	} {
		if v := got.Get(h); v != "" {
			t.Errorf("%s = %q, want it absent when there is no metadata to derive it from", h, v)
		}
	}
	if v := got.Get(jetstream.MsgIDHeader); v != "" {
		t.Errorf("%s = %q, want none", jetstream.MsgIDHeader, v)
	}
}
