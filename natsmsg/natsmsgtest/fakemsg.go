// Package natsmsgtest provides test doubles for the jetstream message types
// NATS consumers handle. It lives apart from the packages under test so
// white-box unit tests (package foo, not foo_test) can import it without
// cycles or extra fixture dependencies.
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

	Acked       bool
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

func (m *FakeMsg) Ack() error                      { m.Acked = true; return nil }
func (m *FakeMsg) DoubleAck(context.Context) error { m.Acked = true; return nil }
func (m *FakeMsg) Nak() error                      { m.Naks++; return m.NakErr }
func (m *FakeMsg) NakWithDelay(d time.Duration) error {
	m.NakDelays = append(m.NakDelays, d)
	return m.NakErr
}
func (m *FakeMsg) InProgress() error           { m.InProgressN++; return nil }
func (m *FakeMsg) Term() error                 { m.Termed = true; return m.TermErr }
func (m *FakeMsg) TermWithReason(string) error { m.Termed = true; return m.TermErr }

var _ jetstream.Msg = (*FakeMsg)(nil)

// FakeLegacyMsg is the legacy *nats.Msg counterpart of [FakeMsg] for
// unit-testing redelivery over the backoff.LegacyMsg surface without a live
// broker: script Meta / MetaErr and assert the disposition (Termed, Naks,
// NakDelays). A concrete *nats.Msg sends its Nak/Term over the wire and parses
// metadata from its reply subject, so it can't be driven this way — this double
// makes the same disposition directly observable. Script NakErr / TermErr to
// exercise the disposition-error path.
type FakeLegacyMsg struct {
	Meta    *nats.MsgMetadata
	MetaErr error
	NakErr  error // returned by NakWithDelay after the call is recorded
	TermErr error // returned by Term after the call is recorded

	Termed    bool
	Naks      int
	NakDelays []time.Duration
}

// Metadata mirrors *nats.Msg.Metadata: the scripted metadata, the scripted
// error, or — when neither is set — a non-JetStream error, matching how a
// message carrying no metadata behaves.
func (m *FakeLegacyMsg) Metadata() (*nats.MsgMetadata, error) {
	if m.MetaErr != nil {
		return nil, m.MetaErr
	}
	if m.Meta != nil {
		return m.Meta, nil
	}
	return nil, errors.New("fakelegacymsg: no metadata set")
}

func (m *FakeLegacyMsg) NakWithDelay(d time.Duration, _ ...nats.AckOpt) error {
	m.Naks++
	m.NakDelays = append(m.NakDelays, d)
	return m.NakErr
}
func (m *FakeLegacyMsg) Term(_ ...nats.AckOpt) error { m.Termed = true; return m.TermErr }
