package brokersemantics

import (
	"context"
	"errors"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/entireio/go-nuts/natsmsg"
)

// TestTermSettlesAndAdvancesAckFloor is the ENT-1492 belief, measured.
//
// The belief under test — recorded in ENT-1492, in COR-944, and still carried in
// the fleet stream manifests as "a message leaves the stream only on a positive
// Ack: a Term or a MaxDeliver-exhausted Nak does NOT delete it (verified live on
// NATS 2.14.2)" — is that Term is not a settlement: the consumer's ack floor stays
// pinned behind a Term'd message.
//
// It does not reproduce. On every retention policy the Term settles the delivery
// immediately: the floor advances past the Term'd sequence and NumAckPending drops
// to zero. Running this same test against an embedded 2.14.2 (the version ENT-1492
// cites) gives identical results, so the belief is not a 2.14.2-vs-2.14.3
// difference either.
//
// What DOES differ by retention is whether the message leaves the stream, and that
// is the distinction the two issues conflate: on WorkQueue and Interest a Term
// removes it, on Limits it stays until the stream's own retention expires it. So
// backoff.Policy.TermOnExhaustion's promise — "a work-queue message is removed
// cleanly rather than orphaning un-acked" (COR-762) — holds; ENT-1492's premise
// for replacing Term with dead-letter-then-Ack does not. Dead-lettering remains
// right for a different and better reason: it keeps the payload (see
// TestDeadLetterCaptureThenAckSettlesAndPreservesProvenance).
func TestTermSettlesAndAdvancesAckFloor(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name            string
		retention       jetstream.RetentionPolicy
		wantMsgsInStrm  uint64
		wantTermedInStr bool
	}{
		{"limits keeps the message in the stream", jetstream.LimitsPolicy, 3, true},
		{"workqueue removes it", jetstream.WorkQueuePolicy, 0, false},
		{"interest removes it", jetstream.InterestPolicy, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			nc, _ := connect(t, startServer(t))
			js := jsHandle(t, nc)
			st := newStream(t, js, jetstream.StreamConfig{
				Name: "poison", Subjects: []string{"poison.>"}, Retention: tc.retention,
			})
			cons := newConsumer(t, js, "poison", jetstream.ConsumerConfig{
				Durable:   "settler",
				AckPolicy: jetstream.AckExplicitPolicy,
				AckWait:   time.Minute, // long: no redelivery may confound the reading
			})
			publish(t, js, "poison.a", "one", "two", "three")

			// Term the head of line, Ack the rest: if Term did not settle, the
			// floor would stay at 0 with one message ack-pending.
			for _, m := range fetchAll(t, cons, 3) {
				if meta(t, m).Sequence.Stream == 1 {
					if err := m.Term(); err != nil {
						t.Fatalf("term: %v", err)
					}
					continue
				}
				if err := m.Ack(); err != nil {
					t.Fatalf("ack: %v", err)
				}
			}
			time.Sleep(settleWait)

			ci := info(t, cons)
			if ci.AckFloor.Stream != 3 {
				t.Errorf("ack floor at stream seq %d after Term+Ack, want 3: a Term left the floor pinned", ci.AckFloor.Stream)
			}
			if ci.NumAckPending != 0 {
				t.Errorf("NumAckPending = %d after Term+Ack, want 0: the Term'd delivery is still outstanding", ci.NumAckPending)
			}

			si, err := st.Info(t.Context())
			if err != nil {
				t.Fatalf("stream info: %v", err)
			}
			if si.State.Msgs != tc.wantMsgsInStrm {
				t.Errorf("stream holds %d messages after the Term, want %d", si.State.Msgs, tc.wantMsgsInStrm)
			}
			_, err = st.GetMsg(t.Context(), 1)
			switch {
			case tc.wantTermedInStr && err != nil:
				t.Errorf("Term'd message is gone from a %v stream: %v", tc.retention, err)
			case !tc.wantTermedInStr && err == nil:
				t.Errorf("Term'd message survives in a %v stream, want it removed", tc.retention)
			}
		})
	}
}

// TestDeadLetterCaptureThenAckSettlesAndPreservesProvenance drives
// natsmsg.DeadLetter against a real broker: the capture-then-Ack sequence the
// ENT-1492 fix adopted, and the one backoff's doc points callers to ("capture
// BEFORE disposing").
//
// The fake can show that DeadLetter published something. Only the broker can show
// the two things that matter operationally: the captured copy is durable in the
// DLQ stream with its Nats-Dlq-* provenance (so the poison payload survives the
// give-up, which is what the manual break-glass in ENT-1535 destroyed), and the
// Ack that follows advances the floor past the poison — the stall remedy itself.
func TestDeadLetterCaptureThenAckSettlesAndPreservesProvenance(t *testing.T) {
	t.Parallel()
	_, js := env(t)
	newStream(t, js, jetstream.StreamConfig{Name: "events_dlq", Subjects: []string{"dlq.events.>"}})
	cons := newConsumer(t, js, "events", jetstream.ConsumerConfig{
		Durable:       "capturer",
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       time.Minute,
		FilterSubject: "events.repo",
	})
	publish(t, js, "events.repo", "poison", "healthy")

	msgs := fetchAll(t, cons, 1)
	if len(msgs) != 1 {
		t.Fatalf("fetched %d messages, want 1", len(msgs))
	}
	poison := msgs[0]
	reason := natsmsg.SubjectToken("bad completion payload")
	if err := natsmsg.DeadLetter(t.Context(), js, "dlq.events."+reason, poison, "bad completion payload"); err != nil {
		t.Fatalf("DeadLetter: %v", err)
	}
	if err := poison.Ack(); err != nil {
		t.Fatalf("ack after capture: %v", err)
	}
	time.Sleep(settleWait)

	if got := info(t, cons).AckFloor.Stream; got != 1 {
		t.Errorf("ack floor at %d after capture-then-Ack, want 1 (the poison settled)", got)
	}

	// The captured copy must be readable from the DLQ stream, payload and
	// provenance intact — this is the record the break-glass path destroys.
	dlq, err := js.Stream(t.Context(), "events_dlq")
	if err != nil {
		t.Fatalf("dlq stream: %v", err)
	}
	captured, err := dlq.GetMsg(t.Context(), 1)
	if err != nil {
		t.Fatalf("read captured message: %v", err)
	}
	if string(captured.Data) != "poison" {
		t.Errorf("captured payload = %q, want %q", captured.Data, "poison")
	}
	for header, want := range map[string]string{
		natsmsg.DLQReasonHeader:    "bad completion payload",
		natsmsg.DLQOriginHeader:    "events.repo",
		natsmsg.DLQDeliveredHeader: "1",
		natsmsg.DLQStreamSeqHeader: "1",
	} {
		if got := captured.Header.Get(header); got != want {
			t.Errorf("captured %s = %q, want %q", header, got, want)
		}
	}
	if captured.Subject != "dlq.events."+reason {
		t.Errorf("captured on subject %q, want %q", captured.Subject, "dlq.events."+reason)
	}
}

// TestAckWithoutPublishPermissionSilentlySucceeds pins the sharpest edge in the
// whole disposition surface: Ack, Nak, Term and InProgress are fire-and-forget
// publishes to $JS.ACK.>, so a nil error is NOT evidence that the message
// settled. When the connection's identity lacks publish permission on $JS.ACK —
// the exact gap COR-1224 found on five webhooks_github_v1 consumers — the server
// rejects the ack, the floor stays pinned, and the client sees success. The only
// trace is an async error on the connection handler.
//
// This matters twice over. It is a mechanism that produces ENT-1492's symptom
// (a floor pinned for tens of hours with no redelivery and no log line) without
// requiring Term-does-not-settle to be true, which the test above shows it is
// not. And it qualifies backoff's own documentation: "a disposition error is
// worth a log line but nothing more" understates the failure, because on this
// path there is no error to log. A caller who must know the message settled has
// to use DoubleAck.
//
// Note what a DoubleAck failure does and does not establish. It is a lost
// CONFIRMATION, which is indistinguishable from an ack that landed and whose
// reply went missing — so it means "unknown", not "not settled", and a caller must
// not respond by re-dispatching work as though the message were still theirs. And
// the error to match is the context's: this test asserts errors.Is(err,
// context.DeadlineExceeded) rather than nats.ErrTimeout, because a classifier
// built from nats sentinels alone silently misses it.
func TestAckWithoutPublishPermissionSilentlySucceeds(t *testing.T) {
	t.Parallel()
	s := startServer(t, func(o *natsserver.Options) {
		o.Users = []*natsserver.User{{
			Username: "indexer", Password: "pw",
			Permissions: &natsserver.Permissions{
				// Everything a consumer needs EXCEPT $JS.ACK.> — the COR-1224 shape.
				Publish:   &natsserver.SubjectPermission{Allow: []string{"$JS.API.>", "events.>"}},
				Subscribe: &natsserver.SubjectPermission{Allow: []string{">"}},
			},
		}}
	})
	nc, asyncErrs := connect(t, s, nats.UserInfo("indexer", "pw"))
	js := jsHandle(t, nc)
	newStream(t, js, jetstream.StreamConfig{Name: "events", Subjects: []string{"events.>"}})
	cons := newConsumer(t, js, "events", jetstream.ConsumerConfig{
		Durable:   "denied",
		AckPolicy: jetstream.AckExplicitPolicy,
		AckWait:   time.Minute,
	})
	publish(t, js, "events.repo", "first", "second")

	msgs := fetchAll(t, cons, 1)
	if len(msgs) != 1 {
		t.Fatalf("fetched %d messages, want 1", len(msgs))
	}
	if err := msgs[0].Ack(); err != nil {
		t.Fatalf("Ack returned %v; this test's premise is that it returns nil even when denied", err)
	}
	time.Sleep(settleWait)

	ci := info(t, cons)
	if ci.AckFloor.Stream != 0 {
		t.Errorf("ack floor advanced to %d despite the denied ack, want 0", ci.AckFloor.Stream)
	}
	if ci.NumAckPending != 1 {
		t.Errorf("NumAckPending = %d, want 1: the delivery is still outstanding after the silent Ack", ci.NumAckPending)
	}
	// The rejection is observable only here — not on the Ack call.
	var sawViolation bool
	for _, err := range asyncErrs() {
		if errors.Is(err, nats.ErrPermissionViolation) {
			sawViolation = true
		}
	}
	if !sawViolation {
		t.Errorf("no permissions violation on the async error handler; saw %v", asyncErrs())
	}

	// DoubleAck is the surface that does report it: it waits for the server's
	// confirmation, which never comes. Note the error is the context's, not
	// nats.ErrTimeout — a caller classifying only nats sentinels will miss it.
	second := fetchAll(t, cons, 1)
	if len(second) != 1 {
		t.Fatalf("fetched %d messages on the second pull, want 1", len(second))
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	err := second[0].DoubleAck(ctx)
	if err == nil {
		t.Fatal("DoubleAck succeeded without $JS.ACK publish permission, want an error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("DoubleAck error = %v (%T), want a context deadline; callers matching only nats sentinels miss it", err, err)
	}
}

// TestExhaustedDeliveryPinsFloorWithNoAckPending pins the accounting signature of
// a message that has run out MaxDeliver without settling — the state ENT-1535's
// page was reading.
//
// Once the last delivery's AckWait expires the server stops redelivering, drops
// the message from NumAckPending, and leaves the ack floor pinned behind it
// forever (a limits stream keeps the message until max_age). So the operational
// reading is counter-intuitive twice: "Outstanding Acks: 0" does not mean nothing
// is outstanding, and a floor that is not moving is not evidence of an active
// retry ladder — the ladder may have finished hours ago.
//
// This is also the state backoff.Policy.TermOnExhaustion exists to avoid: without
// it, the final Nak is dropped by the broker and the message lands exactly here.
func TestExhaustedDeliveryPinsFloorWithNoAckPending(t *testing.T) {
	t.Parallel()
	_, js := env(t)
	cons := newConsumer(t, js, "events", jetstream.ConsumerConfig{
		Durable:       "exhauster",
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       shortAckWait,
		MaxDeliver:    2,
		FilterSubject: "events.repo",
	})
	publish(t, js, "events.repo", "poison", "healthy")

	// Never dispose of the poison; Ack everything behind it.
	r := consume(t, cons, func(m jetstream.Msg) {
		if string(m.Data()) != "poison" {
			if err := m.Ack(); err != nil {
				t.Errorf("ack: %v", err)
			}
		}
	})
	// Two deliveries of the poison (MaxDeliver=2) plus the healthy one.
	r.waitForN(t, 3)
	// Let the final AckWait expire so the server retires the delivery.
	time.Sleep(shortAckWait + settleWait)

	ci := info(t, cons)
	if ci.AckFloor.Stream != 0 {
		t.Errorf("ack floor = %d, want 0: an exhausted message must still pin the floor", ci.AckFloor.Stream)
	}
	if ci.NumAckPending != 0 {
		t.Errorf("NumAckPending = %d, want 0: an exhausted delivery stops counting as outstanding", ci.NumAckPending)
	}
	if ci.NumPending != 0 {
		t.Errorf("NumPending = %d, want 0: nothing is left undelivered", ci.NumPending)
	}
	if ci.Delivered.Stream != 2 {
		t.Errorf("Delivered.Stream = %d, want 2: delivery ran past the pinned floor", ci.Delivered.Stream)
	}
	// No further redelivery, however long we wait: MaxDeliver is spent.
	before := len(r.snapshot())
	time.Sleep(3 * shortAckWait)
	if after := len(r.snapshot()); after != before {
		t.Errorf("saw %d deliveries after exhaustion, want the %d already recorded", after, before)
	}
}
