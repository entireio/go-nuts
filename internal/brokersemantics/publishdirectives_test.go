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
	// The copy carries no dedupe identity at all — see
	// TestEveryCaptureLeavesItsOwnDLQRecord.
	if got := stored.Header.Get(jetstream.MsgIDHeader); got != "" {
		t.Errorf("%s = %q on the captured copy, want none", jetstream.MsgIDHeader, got)
	}
	// And nothing from the reserved namespace leaked in under a name this suite
	// did not think to enumerate. Only this package's own provenance may appear.
	for h := range stored.Header {
		low := strings.ToLower(h)
		if !strings.HasPrefix(low, "nats-") {
			continue
		}
		if strings.HasPrefix(low, strings.ToLower(natsmsg.DLQHeaderPrefix)) {
			continue
		}
		t.Errorf("captured copy carries reserved header %q", h)
	}
}

// TestEveryCaptureLeavesItsOwnDLQRecord is the invariant capture-time dedupe was
// traded away for, measured against the real broker: N captures leave N records,
// whatever they are captures of.
//
// Two schemes for a copy's Nats-Msg-Id were tried and each had a collision class.
// Both failed in the invisible direction — JetStream answers a suppressed publish
// with a SUCCESSFUL PubAck marked Duplicate, so the caller reads success, Acks the
// original, and the message is gone with no record. Copies therefore carry no
// dedupe identity, and the DLQ here declares a duplicate window wide enough that
// any identity WOULD have collapsed these captures, so the test would fail if one
// came back.
//
// Case (a) is the source-recreation case: the stream is deleted and recreated, so
// the second message is stored at sequence 1 exactly as the first was, on a stream
// of the same name. Case (b) is the general one, including re-capturing the very
// same stored message — the case the dedupe existed for.
func TestEveryCaptureLeavesItsOwnDLQRecord(t *testing.T) {
	t.Parallel()
	_, js := env(t)
	newStream(t, js, jetstream.StreamConfig{
		Name: "events_dlq_norecords", Subjects: []string{"dlq.norecords.>"},
		Duplicates: time.Minute,
	})
	consumer := func(t *testing.T) jetstream.Consumer {
		t.Helper()
		return newConsumer(t, js, "events", jetstream.ConsumerConfig{
			Durable:       "norecords_capturer",
			AckPolicy:     jetstream.AckExplicitPolicy,
			AckWait:       time.Minute,
			FilterSubject: "events.repo",
		})
	}

	// (a) First incarnation: publish, capture, settle.
	cons := consumer(t)
	publish(t, js, "events.repo", "first incarnation")
	first := fetchAll(t, cons, 1)
	if len(first) != 1 {
		t.Fatalf("fetched %d messages, want 1", len(first))
	}
	if err := natsmsg.DeadLetter(t.Context(), js, "dlq.norecords.bad_body", first[0], "bad_body"); err != nil {
		t.Fatalf("DeadLetter (first incarnation): %v", err)
	}
	if err := first[0].Ack(); err != nil {
		t.Fatalf("ack: %v", err)
	}

	// Delete and recreate the source. Sequence numbering restarts at 1.
	if err := js.DeleteStream(t.Context(), "events"); err != nil {
		t.Fatalf("delete source stream: %v", err)
	}
	newStream(t, js, jetstream.StreamConfig{Name: "events", Subjects: []string{"events.>"}})

	cons = consumer(t)
	publish(t, js, "events.repo", "second incarnation")
	second := fetchAll(t, cons, 1)
	if len(second) != 1 {
		t.Fatalf("fetched %d messages, want 1", len(second))
	}
	meta, err := second[0].Metadata()
	if err != nil {
		t.Fatalf("metadata: %v", err)
	}
	if meta.Sequence.Stream != 1 {
		t.Fatalf("message stored at sequence %d, want 1 — the reused sequence is what "+
			"this case is about", meta.Sequence.Stream)
	}
	if err := natsmsg.DeadLetter(t.Context(), js, "dlq.norecords.bad_body", second[0], "bad_body"); err != nil {
		t.Fatalf("DeadLetter (second incarnation): %v", err)
	}

	// (b) And capture that same stored message twice more, as the failed-Ack path
	// does: the DLQ publish lands, the Ack does not, the message redelivers.
	for i := range 2 {
		if err := natsmsg.DeadLetter(t.Context(), js, "dlq.norecords.bad_body", second[0], "bad_body"); err != nil {
			t.Fatalf("re-capture %d: %v", i+1, err)
		}
	}
	if err := second[0].Ack(); err != nil {
		t.Fatalf("ack: %v", err)
	}

	dlq, err := js.Stream(t.Context(), "events_dlq_norecords")
	if err != nil {
		t.Fatalf("dlq stream: %v", err)
	}
	si, err := dlq.Info(t.Context())
	if err != nil {
		t.Fatalf("dlq info: %v", err)
	}
	// Four captures: two distinct originals plus two re-captures of the second.
	if si.State.Msgs != 4 {
		t.Fatalf("DLQ holds %d records after 4 captures, want 4: a missing record means "+
			"an original was acked with nothing stored", si.State.Msgs)
	}
	for seq := uint64(1); seq <= 4; seq++ {
		rec, err := dlq.GetMsg(t.Context(), seq)
		if err != nil {
			t.Fatalf("read captured copy %d: %v", seq, err)
		}
		if v := rec.Header.Get(jetstream.MsgIDHeader); v != "" {
			t.Errorf("record %d carries %s = %q, want no dedupe identity", seq, jetstream.MsgIDHeader, v)
		}
	}
	// Both incarnations really are in there, told apart by their origin store time.
	r1, err := dlq.GetMsg(t.Context(), 1)
	if err != nil {
		t.Fatalf("read record 1: %v", err)
	}
	r2, err := dlq.GetMsg(t.Context(), 2)
	if err != nil {
		t.Fatalf("read record 2: %v", err)
	}
	if string(r1.Data) != "first incarnation" || string(r2.Data) != "second incarnation" {
		t.Errorf("captured payloads = %q, %q; want the two distinct originals", r1.Data, r2.Data)
	}
	if r1.Header.Get(natsmsg.DLQStreamSeqHeader) != "1" || r2.Header.Get(natsmsg.DLQStreamSeqHeader) != "1" {
		t.Errorf("origin sequences = %q, %q; want both 1 — the reuse is real",
			r1.Header.Get(natsmsg.DLQStreamSeqHeader), r2.Header.Get(natsmsg.DLQStreamSeqHeader))
	}
	if t1, t2 := r1.Header.Get(natsmsg.DLQOriginTimestampHeader), r2.Header.Get(natsmsg.DLQOriginTimestampHeader); t1 == t2 || t1 == "" {
		t.Errorf("origin store times = %q, %q; want distinct, non-empty values — this is "+
			"what replay-side dedupe tells incarnations apart by", t1, t2)
	}
}

// TestReCaptureKeepsTheFirstHopProvenance: the failed-Ack path captures one message
// more than once, and the later captures must not overwrite what the first recorded.
// Origin provenance is write-once; only the per-hop fields move.
func TestReCaptureKeepsTheFirstHopProvenance(t *testing.T) {
	t.Parallel()
	_, js := env(t)
	newStream(t, js, jetstream.StreamConfig{Name: "events_dlq_recapture", Subjects: []string{"dlq.recapture.>"}})
	cons := newConsumer(t, js, "events", jetstream.ConsumerConfig{
		Durable:       "recapture_capturer",
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       time.Minute,
		FilterSubject: "events.repo",
	})

	orig := nats.NewMsg("events.repo")
	orig.Data = []byte(`{"repo":"01KWHDFJ0C"}`)
	orig.Header.Set(jetstream.MsgIDHeader, "producer/01KWHDFJ0C")
	if _, err := js.PublishMsg(t.Context(), orig); err != nil {
		t.Fatalf("publish: %v", err)
	}

	msgs := fetchAll(t, cons, 1)
	if len(msgs) != 1 {
		t.Fatalf("fetched %d messages, want 1", len(msgs))
	}
	// Two captures of the same delivery, then a third standing in for a later
	// redelivery with a higher delivery count.
	for _, reason := range []string{"bad_body", "bad_body_again"} {
		if err := natsmsg.DeadLetter(t.Context(), js, "dlq.recapture.bad_body", msgs[0], reason); err != nil {
			t.Fatalf("DeadLetter (%s): %v", reason, err)
		}
	}

	dlq, err := js.Stream(t.Context(), "events_dlq_recapture")
	if err != nil {
		t.Fatalf("dlq stream: %v", err)
	}
	first, err := dlq.GetMsg(t.Context(), 1)
	if err != nil {
		t.Fatalf("read first copy: %v", err)
	}
	second, err := dlq.GetMsg(t.Context(), 2)
	if err != nil {
		t.Fatalf("read second copy: %v", err)
	}

	// Every origin field is identical across the two records.
	for _, h := range []string{
		natsmsg.DLQOriginHeader, natsmsg.DLQOriginStreamHeader, natsmsg.DLQStreamSeqHeader,
		natsmsg.DLQOriginDomainHeader, natsmsg.DLQOriginTimestampHeader, natsmsg.DLQOriginMsgIDHeader,
	} {
		if a, b := first.Header.Get(h), second.Header.Get(h); a != b {
			t.Errorf("%s = %q on the first copy and %q on the re-capture; origin provenance is write-once", h, a, b)
		}
	}
	if got := first.Header.Get(natsmsg.DLQOriginMsgIDHeader); got != "producer/01KWHDFJ0C" {
		t.Errorf("first-hop publisher key = %q, want producer/01KWHDFJ0C", got)
	}
	if got := first.Header.Get(natsmsg.DLQOriginStreamHeader); got != "events" {
		t.Errorf("origin stream = %q, want events", got)
	}
	// Per-hop fields move: this hop's reason, and a hop count that advanced.
	if got := second.Header.Get(natsmsg.DLQReasonHeader); got != "bad_body_again" {
		t.Errorf("re-capture reason = %q, want this hop's reason", got)
	}
	if a, b := first.Header.Get(natsmsg.DLQHopsHeader), second.Header.Get(natsmsg.DLQHopsHeader); a != "1" || b != "1" {
		// Both are hop 1: each capture reads the ORIGINAL, which carries no hop
		// count. The counter advances only when a DLQ RECORD is itself captured.
		t.Errorf("hop counts = %q, %q; want both 1 — re-capturing an original is still its first hop", a, b)
	}

	// Re-capturing the ORIGINAL cannot actually distinguish write-once from
	// overwrite: both captures derive the same values from the same stored message,
	// so the assertions above hold either way. The case that discriminates is
	// capturing a DLQ RECORD, where this hop's own stream, sequence and subject all
	// differ from the first hop's — so that is measured here too, against the real
	// broker rather than only in the package's unit tests.
	newStream(t, js, jetstream.StreamConfig{Name: "events_dlq2_recapture", Subjects: []string{"dlq2.recapture.>"}})
	dlqCons := newConsumer(t, js, "events_dlq_recapture", jetstream.ConsumerConfig{
		Durable:       "replay_gave_up",
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       time.Minute,
		FilterSubject: "dlq.recapture.>",
	})
	records := fetchAll(t, dlqCons, 1)
	if len(records) != 1 {
		t.Fatalf("fetched %d DLQ records, want 1", len(records))
	}
	if err := natsmsg.DeadLetter(t.Context(), js, "dlq2.recapture.gave_up", records[0], "gave up replaying"); err != nil {
		t.Fatalf("DeadLetter of a DLQ record: %v", err)
	}

	dlq2, err := js.Stream(t.Context(), "events_dlq2_recapture")
	if err != nil {
		t.Fatalf("second dlq stream: %v", err)
	}
	hop2, err := dlq2.GetMsg(t.Context(), 1)
	if err != nil {
		t.Fatalf("read the re-captured copy: %v", err)
	}
	// Origin still names the FIRST hop — the source stream and subject, not the DLQ
	// the record was just read from.
	for h, want := range map[string]string{
		natsmsg.DLQOriginStreamHeader: "events",
		natsmsg.DLQOriginHeader:       "events.repo",
		natsmsg.DLQStreamSeqHeader:    first.Header.Get(natsmsg.DLQStreamSeqHeader),
		natsmsg.DLQOriginMsgIDHeader:  "producer/01KWHDFJ0C",
	} {
		if got := hop2.Header.Get(h); got != want {
			t.Errorf("after re-capturing a DLQ record, %s = %q, want %q — origin provenance is write-once", h, got, want)
		}
	}
	if got := hop2.Header.Get(natsmsg.DLQReasonHeader); got != "gave up replaying" {
		t.Errorf("re-capture reason = %q, want this hop's", got)
	}
	if got := hop2.Header.Get(natsmsg.DLQHopsHeader); got != "2" {
		t.Errorf("hop count = %q, want 2 after capturing a DLQ record", got)
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
