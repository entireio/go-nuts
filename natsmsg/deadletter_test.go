package natsmsg_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/entireio/go-nuts/natsmsg"
	"github.com/entireio/go-nuts/natsmsg/natsmsgtest"
)

// dlqStreamName is the stream the fake publisher stands in for. A publish
// carrying an expectation about any OTHER stream is rejected, exactly as the
// broker does.
const dlqStreamName = "repo_ops_dlq_v1"

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
		Meta:       &jetstream.MsgMetadata{NumDelivered: 4, Stream: "repo_ops_v1", Sequence: jetstream.SequencePair{Stream: 999}},
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
		if got.Header.Get(jetstream.MsgIDHeader) != "repo_ops_v1/999" {
			t.Errorf("copy dedupe key = %q, want it scoped to the origin", got.Header.Get(jetstream.MsgIDHeader))
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
	orig.Set(jetstream.MsgIDHeader, "teardown/123")
	orig.Set("traceparent", "00-abc-def-01")

	msg := &natsmsgtest.FakeMsg{
		SubjectVal: "repo.ops.v1.us.acme.teardown",
		DataVal:    []byte(`{"target":{"ulid":"X"}}`),
		HeadersVal: orig,
		Meta:       &jetstream.MsgMetadata{NumDelivered: 4, Stream: "repo_ops_v1", Sequence: jetstream.SequencePair{Stream: 999}},
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
	} {
		if v := got.Header.Get(h); v != "" {
			t.Errorf("%s = %q on the captured copy, want it stripped", h, v)
		}
	}

	// Nats-Msg-Id does not survive either — it is replaced by a key scoped to
	// this copy, with the publisher's own kept as provenance. See
	// TestDeadLetterScopesTheDedupeKeyToTheCopy.
	if v := got.Header.Get(jetstream.MsgIDHeader); v != "repo_ops_v1/999" {
		t.Errorf("%s = %q, want the DLQ-scoped key", jetstream.MsgIDHeader, v)
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
	if got := pub.msgs[0].Header.Get(jetstream.MsgIDHeader); got != "repo_ops_v1/41" {
		t.Errorf("copy dedupe key = %q, want it scoped to the origin (repo_ops_v1/41)", got)
	}
	if got := pub.msgs[1].Header.Get(jetstream.MsgIDHeader); got != "repo_ops_v1/77" {
		t.Errorf("copy dedupe key = %q, want it scoped to the origin (repo_ops_v1/77)", got)
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
