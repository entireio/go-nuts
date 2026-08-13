package natsmsg_test

import (
	"context"
	"errors"
	"fmt"
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
	msgs []*nats.Msg
	err  error
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
	f.msgs = append(f.msgs, m)
	return &jetstream.PubAck{Stream: dlqStreamName, Sequence: 1}, nil
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
		Meta:       &jetstream.MsgMetadata{NumDelivered: 4, Sequence: jetstream.SequencePair{Stream: 999}},
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
		// Original headers are preserved alongside the provenance ones.
		if got.Header.Get("Nats-Msg-Id") != "teardown/123" || got.Header.Get("traceparent") != "00-abc-def-01" {
			t.Errorf("original headers not preserved: %v", got.Header)
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
	orig.Set(jetstream.MsgIDHeader, "teardown/123")
	orig.Set("traceparent", "00-abc-def-01")

	msg := &natsmsgtest.FakeMsg{
		SubjectVal: "repo.ops.v1.us.acme.teardown",
		DataVal:    []byte(`{"target":{"ulid":"X"}}`),
		HeadersVal: orig,
		Meta:       &jetstream.MsgMetadata{NumDelivered: 4, Sequence: jetstream.SequencePair{Stream: 999}},
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
		jetstream.MsgRollup,
	} {
		if v := got.Header.Get(h); v != "" {
			t.Errorf("%s = %q on the captured copy, want it stripped", h, v)
		}
	}

	// Nats-Msg-Id stays: it dedupes the deliberate re-capture after a failed
	// Ack, within the DLQ stream's duplicate window.
	if v := got.Header.Get(jetstream.MsgIDHeader); v != "teardown/123" {
		t.Errorf("%s = %q, want it kept for re-capture dedupe", jetstream.MsgIDHeader, v)
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
