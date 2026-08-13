package brokersemantics

import (
	"errors"
	"strings"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/entireio/go-nuts/natsmsg"
)

// JetStream API error codes for the publish-directive rejections, from
// nats-server's jetstream_errors_generated.go. Asserting the wire code rather
// than the description keeps these independent of the server's error text.
const (
	// errCodeWrongLastStream is JSStreamNotMatchErr, "expected stream does not
	// match" — a Nats-Expected-Stream naming a stream other than the one being
	// published to.
	errCodeWrongLastStream jetstream.ErrorCode = 10060
	// errCodeTTLDisabled is JSMessageTTLDisabledErr, "per-message TTL is
	// disabled".
	errCodeTTLDisabled jetstream.ErrorCode = 10166
	// errCodeCounterDisabled is JSMessageIncrDisabledErr, "message counters is
	// disabled".
	errCodeCounterDisabled jetstream.ErrorCode = 10168
	// errCodeAtomicPublishDisabled is JSAtomicPublishDisabledErr, "atomic publish
	// is disabled".
	errCodeAtomicPublishDisabled jetstream.ErrorCode = 10174
)

// publishDirectives are the headers that instruct the broker about a publish, as
// the pinned server names them. Referencing natsserver's own constants rather
// than string literals is deliberate: a rename or removal on the next bump breaks
// this file's compilation, which is the signal a header-filtering rule wants.
//
// natsmsg.DeadLetter must not carry ANY of them onto a captured copy — see
// TestDeadLetterCarriesNoPublishDirectiveOntoTheDLQ. The client library exports
// only some, which is why natsmsg filters by namespace rather than by name.
var publishDirectives = map[string]string{
	natsserver.JSExpectedStream:          "events",
	natsserver.JSExpectedLastSeq:         "1",
	natsserver.JSExpectedLastSubjSeq:     "1",
	natsserver.JSExpectedLastSubjSeqSubj: "events.repo",
	natsserver.JSExpectedLastMsgId:       "prior-id",
	natsserver.JSMessageTTL:              "30s",
	natsserver.JSMsgRollup:               "sub",
	natsserver.JSMessageIncr:             "1",
	natsserver.JSMessageCounterSources:   `{"s":{"events":"1"}}`,
	natsserver.JSBatchId:                 "batch-uuid",
	natsserver.JSBatchSeq:                "1",
	natsserver.JSBatchCommit:             "1",
	natsserver.JSSchedulePattern:         "@every 1h",
	natsserver.JSScheduleTarget:          "somewhere.else",
	natsserver.JSScheduleSource:          "events.>",
	natsserver.JSScheduleTTL:             "5m",
	natsserver.JSScheduleTimeZone:        "America/New_York",
	natsserver.JSScheduleRollup:          "sub",
}

// publishAPIErr publishes msg and returns the JetStream API error it was refused
// with, or nil if it was accepted.
func publishAPIErr(t *testing.T, js jetstream.JetStream, msg *nats.Msg) *jetstream.APIError {
	t.Helper()
	_, err := js.PublishMsg(t.Context(), msg)
	if err == nil {
		return nil
	}
	var apiErr *jetstream.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("publish to %s failed with a non-API error: %v", msg.Subject, err)
	}
	return apiErr
}

// TestOrdinaryStreamRejectsPublishDirectivesItDoesNotEnable is the measurement
// behind natsmsg's header filter: a DLQ is an ordinary stream, and an ordinary
// stream REFUSES a publish carrying a directive for a feature it has not enabled.
//
// This is what makes a copied directive a stall rather than an untidiness. The
// refusals are deterministic and permanent — nothing about retrying changes the
// stream's configuration — so a capture that carries one fails on every delivery
// of the ladder, and the message ends stranded with the ack floor pinned. The
// breaker does not help, because it terminates through the same capture.
func TestOrdinaryStreamRejectsPublishDirectivesItDoesNotEnable(t *testing.T) {
	t.Parallel()
	_, js := env(t)
	// A DLQ as this module's adopters actually declare one: no per-message TTL,
	// no counters, no atomic publish, no rollup.
	newStream(t, js, jetstream.StreamConfig{Name: "events_dlq_plain", Subjects: []string{"dlq.plain.>"}})

	cases := map[string]struct {
		header, value string
		want          jetstream.ErrorCode
	}{
		"per-message TTL": {natsserver.JSMessageTTL, "30s", errCodeTTLDisabled},
		"counter increment": {
			natsserver.JSMessageIncr, "1", errCodeCounterDisabled,
		},
		"atomic batch": {natsserver.JSBatchId, "batch-uuid", errCodeAtomicPublishDisabled},
		"expectation about another stream": {
			natsserver.JSExpectedStream, "events", errCodeWrongLastStream,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			m := nats.NewMsg("dlq.plain.capture")
			m.Data = []byte(`{"captured":true}`)
			m.Header.Set(tc.header, tc.value)

			apiErr := publishAPIErr(t, js, m)
			if apiErr == nil {
				t.Fatalf("publish carrying %s was accepted; want a rejection", tc.header)
			}
			if apiErr.ErrorCode != tc.want {
				t.Errorf("publish carrying %s: err_code = %d (%s), want %d",
					tc.header, apiErr.ErrorCode, apiErr.Description, tc.want)
			}
		})
	}
}

// TestDeadLetterCarriesNoPublishDirectiveOntoTheDLQ is the end-to-end regression
// for the whole class, measured against the real broker rather than a fake that
// cannot refuse anything.
//
// An original carrying every directive the pinned server knows is consumed and
// dead-lettered into a plain DLQ. Two things must hold: the capture SUCCEEDS —
// on any of the earlier filter designs the rejections measured above made it
// fail permanently — and the stored copy carries none of the directives, so the
// record's durability is on the DLQ's own terms rather than the source's.
func TestDeadLetterCarriesNoPublishDirectiveOntoTheDLQ(t *testing.T) {
	t.Parallel()
	_, js := env(t)
	newStream(t, js, jetstream.StreamConfig{Name: "events_dlq_directives", Subjects: []string{"dlq.directives.>"}})
	cons := newConsumer(t, js, "events", jetstream.ConsumerConfig{
		Durable:       "directive_capturer",
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       time.Minute,
		FilterSubject: "events.repo",
	})

	// Publish the original with the directives set as plain headers rather than
	// through the publish options: the source stream would refuse most of them
	// too, and what is under test is the CAPTURE of a message that carries them,
	// however it came to. This is exactly the shape a producer on a
	// TTL-or-counter-enabled stream hands the consumer.
	orig := nats.NewMsg("events.repo")
	orig.Data = []byte(`{"repo":"01KWHDFJ0C"}`)
	orig.Header.Set("traceparent", "00-abcdef-01") // application headers must survive
	orig.Header.Set("X-Entire-Repo", "01KWHDFJ0C")
	orig.Header.Set(jetstream.MsgIDHeader, "producer/01KWHDFJ0C")
	for h, v := range publishDirectives {
		orig.Header.Set(h, v)
	}
	// The source stream is ordinary too, so it would refuse most of these on
	// publish for the same reasons measured above. The message therefore goes in
	// plain and the directives are presented on the DELIVERED copy, which is the
	// state a consumer on a TTL-or-counter-enabled stream really sees and keeps
	// the test about DeadLetter rather than about publishing.
	publish(t, js, "events.repo", "placeholder")

	msgs := fetchAll(t, cons, 1)
	if len(msgs) != 1 {
		t.Fatalf("fetched %d messages, want 1", len(msgs))
	}
	poison := &headerOverrideMsg{Msg: msgs[0], hdr: orig.Header}

	if err := natsmsg.DeadLetter(t.Context(), js, "dlq.directives.bad_body", poison, "bad_body"); err != nil {
		t.Fatalf("DeadLetter of a message carrying every publish directive: %v", err)
	}
	if err := msgs[0].Ack(); err != nil {
		t.Fatalf("ack after capture: %v", err)
	}

	dlq, err := js.Stream(t.Context(), "events_dlq_directives")
	if err != nil {
		t.Fatalf("dlq stream: %v", err)
	}
	stored, err := dlq.GetMsg(t.Context(), 1)
	if err != nil {
		t.Fatalf("read the captured copy: %v", err)
	}

	for h := range publishDirectives {
		if v := stored.Header.Get(h); v != "" {
			t.Errorf("captured copy carries directive %s = %q", h, v)
		}
	}
	// Everything the application owns is untouched.
	if got := stored.Header.Get("traceparent"); got != "00-abcdef-01" {
		t.Errorf("traceparent = %q, want it preserved through the capture", got)
	}
	if got := stored.Header.Get("X-Entire-Repo"); got != "01KWHDFJ0C" {
		t.Errorf("X-Entire-Repo = %q, want it preserved through the capture", got)
	}
	// Provenance is present and the dedupe key is the copy's own: the producer's
	// key is relocated, not carried, so it cannot collapse this record onto an
	// unrelated one that shared it.
	if got := stored.Header.Get(natsmsg.DLQOriginMsgIDHeader); got != "producer/01KWHDFJ0C" {
		t.Errorf("%s = %q, want the producer's key relocated here", natsmsg.DLQOriginMsgIDHeader, got)
	}
	if got := stored.Header.Get(jetstream.MsgIDHeader); got != "events/1" {
		t.Errorf("copy dedupe key = %q, want events/1 (origin stream and sequence)", got)
	}
	// And nothing from the reserved namespace leaked in under a name this suite
	// did not think to enumerate.
	for h := range stored.Header {
		low := strings.ToLower(h)
		if !strings.HasPrefix(low, "nats-") {
			continue
		}
		if strings.HasPrefix(low, strings.ToLower(natsmsg.DLQHeaderPrefix)) || low == "nats-msg-id" {
			continue
		}
		t.Errorf("captured copy carries reserved header %q", h)
	}
}

// headerOverrideMsg presents a delivered message with a different header set,
// so a test can capture a message carrying headers the source stream would have
// refused on publish. Everything else, including Metadata and the ack methods,
// is the real delivered message.
type headerOverrideMsg struct {
	jetstream.Msg

	hdr nats.Header
}

func (m *headerOverrideMsg) Headers() nats.Header { return m.hdr }
