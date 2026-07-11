package natsmsg

import (
	"context"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// runJetStreamEnv boots an embedded JetStream server with a stream bound to
// pub.> and returns a JetStream context for it.
func runJetStreamEnv(t *testing.T) jetstream.JetStream {
	t.Helper()
	s, err := natsserver.NewServer(&natsserver.Options{
		Host:      "127.0.0.1",
		Port:      -1, // pick a free port
		NoLog:     true,
		NoSigs:    true,
		JetStream: true,
		StoreDir:  t.TempDir(),
	})
	if err != nil {
		t.Fatalf("new embedded nats server: %v", err)
	}
	go s.Start()
	if !s.ReadyForConnections(10 * time.Second) {
		t.Fatal("embedded nats server not ready in time")
	}
	t.Cleanup(s.Shutdown)

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	if _, err := js.CreateStream(context.Background(), jetstream.StreamConfig{
		Name:       "pub_v1",
		Subjects:   []string{"pub.>"},
		Duplicates: time.Minute,
	}); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	return js
}

// TestPublisherStampsHeadersAndAcks drives the whole publish prologue: the
// stored message must carry the Nats-Msg-Id dedup key and the producer's
// injected trace context, and the ack must report the landing stream.
func TestPublisherStampsHeadersAndAcks(t *testing.T) {
	js := runJetStreamEnv(t)
	ctx, _, _ := remoteSpanCtx(t)

	p := Publisher{JS: js}
	ack, err := p.Publish(ctx, &nats.Msg{Subject: "pub.repo", Data: []byte("payload")}, "msg-1")
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if ack.Stream != "pub_v1" {
		t.Errorf("ack stream = %q, want pub_v1", ack.Stream)
	}
	if ack.Duplicate {
		t.Error("first publish reported as duplicate")
	}

	stream, err := js.Stream(context.Background(), "pub_v1")
	if err != nil {
		t.Fatalf("bind stream: %v", err)
	}
	stored, err := stream.GetMsg(context.Background(), ack.Sequence)
	if err != nil {
		t.Fatalf("get stored msg: %v", err)
	}
	if got := stored.Header.Get(nats.MsgIdHdr); got != "msg-1" {
		t.Errorf("stored Nats-Msg-Id = %q, want msg-1", got)
	}
	if got := stored.Header.Get("traceparent"); got == "" {
		t.Error("stored message has no traceparent header; trace context was not injected")
	}
}

// TestPublisherDedupesByMsgID: a second publish with the same msg-id inside
// the duplicate window must be suppressed broker-side and reported as such —
// the normal outcome for an at-least-once producer retrying, not an error.
func TestPublisherDedupesByMsgID(t *testing.T) {
	js := runJetStreamEnv(t)
	p := Publisher{JS: js}

	first, err := p.Publish(context.Background(), &nats.Msg{Subject: "pub.repo", Data: []byte("x")}, "dup-1")
	if err != nil {
		t.Fatalf("first publish: %v", err)
	}
	second, err := p.Publish(context.Background(), &nats.Msg{Subject: "pub.repo", Data: []byte("x")}, "dup-1")
	if err != nil {
		t.Fatalf("second publish: %v", err)
	}
	if !second.Duplicate {
		t.Error("second publish with the same msg-id not reported as duplicate")
	}
	if second.Sequence != first.Sequence {
		t.Errorf("duplicate ack sequence = %d, want the original %d", second.Sequence, first.Sequence)
	}
}

// TestPublisherEmptyMsgIDLeavesHeader: an empty msgID must not clobber a
// Nats-Msg-Id the caller already set on the message.
func TestPublisherEmptyMsgIDLeavesHeader(t *testing.T) {
	js := runJetStreamEnv(t)
	p := Publisher{JS: js}

	msg := &nats.Msg{Subject: "pub.repo", Data: []byte("x"), Header: nats.Header{}}
	msg.Header.Set(nats.MsgIdHdr, "preset-1")
	ack, err := p.Publish(context.Background(), msg, "")
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	stream, err := js.Stream(context.Background(), "pub_v1")
	if err != nil {
		t.Fatalf("bind stream: %v", err)
	}
	stored, err := stream.GetMsg(context.Background(), ack.Sequence)
	if err != nil {
		t.Fatalf("get stored msg: %v", err)
	}
	if got := stored.Header.Get(nats.MsgIdHdr); got != "preset-1" {
		t.Errorf("stored Nats-Msg-Id = %q, want the caller's preset-1", got)
	}
}

// TestPublisherErrorsOnUnboundSubject: a subject no stream listens on must
// surface as an error (the broker sends no ack), not hang past the bound.
func TestPublisherErrorsOnUnboundSubject(t *testing.T) {
	js := runJetStreamEnv(t)
	p := Publisher{JS: js, Timeout: 500 * time.Millisecond}

	start := time.Now()
	if _, err := p.Publish(context.Background(), &nats.Msg{Subject: "unbound.subject", Data: []byte("x")}, "m"); err == nil {
		t.Fatal("publish to an unbound subject succeeded, want error")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("publish error took %v, want it bounded well under the default", elapsed)
	}
}
