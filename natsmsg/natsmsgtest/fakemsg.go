// Package natsmsgtest provides test doubles for the jetstream message types
// NATS consumers handle. It lives apart from the packages under test so
// white-box unit tests (package foo, not foo_test) can import it without
// cycles or extra fixture dependencies.
//
// # Scope: library logic, never broker semantics
//
// A fake answers one question: given this input, what did the code DO — which
// disposition did it choose, with which delay, after which branch. That is all it
// may be used for.
//
// It cannot answer what the BROKER does in response, because it encodes the same
// model of JetStream that the code under test does: a belief held wrongly in both
// places produces a green test. Every P1 in the A1 review cycle had that shape —
// Term's settlement, NumDelivered's meaning, the BackOff ladder's arithmetic — so
// assertions of that kind belong in the real-broker suite
// (internal/brokersemantics), which measures them against an embedded nats-server
// and runs in the same default `go test ./...` (COR-1257).
//
// In particular, a FakeMsg's Meta is whatever the test sets: scripting
// NumDelivered proves how the code reads a delivery count, never that the server
// would have produced it.
package natsmsgtest

import (
	"context"
	"errors"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// FakeMsg is a jetstream.Msg for unit-testing a consumer's message disposition
// without a live broker. Script the fields the handler reads (SubjectVal,
// DataVal, HeadersVal, Meta) and assert the terminal call it made (Acked,
// Termed, Naks, NakDelays). Because jetstream.Msg is an interface — unlike the
// concrete *nats.Msg whose Ack/Nak/Term hit the wire — the disposition is
// directly observable. Script NakErr / TermErr to exercise a caller's
// disposition-error path.
type FakeMsg struct {
	SubjectVal string
	DataVal    []byte
	HeadersVal nats.Header
	Meta       *jetstream.MsgMetadata
	MetaErr    error
	NakErr     error // returned by Nak/NakWithDelay after the call is recorded
	TermErr    error // returned by Term/TermWithReason after the call is recorded
	AckErr     error // returned by Ack/DoubleAck after the call is recorded

	Acked       bool
	DoubleAcks  int // DoubleAck calls; a plain Ack does not count
	Termed      bool
	Naks        int // plain Nak() calls; delayed naks land in NakDelays
	NakDelays   []time.Duration
	InProgressN int
}

func (m *FakeMsg) Subject() string      { return m.SubjectVal }
func (m *FakeMsg) Data() []byte         { return m.DataVal }
func (m *FakeMsg) Headers() nats.Header { return m.HeadersVal }
func (m *FakeMsg) Reply() string        { return "" }

// Metadata returns the scripted metadata, the scripted error, or — when neither
// is set — a non-JetStream error, matching how *nats.Msg.Metadata behaves for a
// message that carries none. Consumers guard this with `if err == nil`.
func (m *FakeMsg) Metadata() (*jetstream.MsgMetadata, error) {
	if m.MetaErr != nil {
		return nil, m.MetaErr
	}
	if m.Meta != nil {
		return m.Meta, nil
	}
	return nil, errors.New("fakemsg: no metadata set")
}

func (m *FakeMsg) Ack() error { m.Acked = true; return m.AckErr }
func (m *FakeMsg) DoubleAck(context.Context) error {
	m.Acked, m.DoubleAcks = true, m.DoubleAcks+1
	return m.AckErr
}
func (m *FakeMsg) Nak() error { m.Naks++; return m.NakErr }
func (m *FakeMsg) NakWithDelay(d time.Duration) error {
	m.NakDelays = append(m.NakDelays, d)
	return m.NakErr
}
func (m *FakeMsg) InProgress() error           { m.InProgressN++; return nil }
func (m *FakeMsg) Term() error                 { m.Termed = true; return m.TermErr }
func (m *FakeMsg) TermWithReason(string) error { m.Termed = true; return m.TermErr }

var _ jetstream.Msg = (*FakeMsg)(nil)
