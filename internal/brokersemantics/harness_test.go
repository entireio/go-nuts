package brokersemantics

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// The suite's own tunables. Ladder rungs are milliseconds rather than the
// minutes/hours of a production ladder: the arithmetic under test is scale-free,
// and the whole suite has to fit in a merge gate. They are still an order of
// magnitude above the broker's own timer granularity (ackWaitDelay is 1ms, and
// checkPending re-arms on the next rung), which is what keeps the measured gaps
// separable.
const (
	// shortAckWait is long enough that a delivery, a fetch round-trip and an
	// assertion fit inside it, short enough that a MaxDeliver ladder of them
	// finishes in a test.
	shortAckWait = 200 * time.Millisecond
	// settleWait is how long to let the server's pending timer and ack
	// bookkeeping catch up before reading ConsumerInfo.
	settleWait = 300 * time.Millisecond
	// waitTimeout bounds every wait-for-N-deliveries helper.
	waitTimeout = 15 * time.Second
)

// JetStream API error codes that nats.go declares no constant for, taken from
// nats-server's jetstream_errors_generated.go. Asserting on the wire code keeps
// the rejection tests independent of the server's description strings — one of
// which (10116) misstates the rule it enforces.
const (
	// errCodePedantic is JSPedanticErrF, "pedantic mode: {err}" — every
	// pedantic-mode rejection, whatever the underlying field.
	errCodePedantic jetstream.ErrorCode = 10157
	// errCodeMaxDeliverBackoff is JSConsumerMaxDeliverBackoffErr, "max deliver is
	// required to be > length of backoff values".
	errCodeMaxDeliverBackoff jetstream.ErrorCode = 10116
)

// startServer boots an in-process JetStream nats-server — the version in go.mod,
// which is the whole point of the suite — on a random loopback port, and shuts it
// down when the test ends. opts may adjust the options (accounts, permissions,
// limits) before start.
func startServer(t *testing.T, opts ...func(*natsserver.Options)) *natsserver.Server {
	t.Helper()
	o := &natsserver.Options{
		Host:      "127.0.0.1",
		Port:      -1, // pick a free port
		NoLog:     true,
		NoSigs:    true,
		JetStream: true,
		StoreDir:  t.TempDir(),
	}
	for _, fn := range opts {
		fn(o)
	}
	s, err := natsserver.NewServer(o)
	if err != nil {
		t.Fatalf("new embedded nats server: %v", err)
	}
	go s.Start()
	if !s.ReadyForConnections(10 * time.Second) {
		t.Fatal("embedded nats server not ready in time")
	}
	t.Cleanup(s.Shutdown)
	return s
}

// connect dials s and closes the connection when the test ends. Async errors are
// recorded rather than logged to stderr, and returned by the drain func — the
// permissions tests read them, and a test that ignores them still gets them in
// its failure output.
func connect(t *testing.T, s *natsserver.Server, opts ...nats.Option) (*nats.Conn, func() []error) {
	t.Helper()
	var mu sync.Mutex
	var asyncErrs []error
	opts = append(opts, nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
		mu.Lock()
		asyncErrs = append(asyncErrs, err)
		mu.Unlock()
	}))
	nc, err := nats.Connect(s.ClientURL(), opts...)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	return nc, func() []error {
		mu.Lock()
		defer mu.Unlock()
		return append([]error(nil), asyncErrs...)
	}
}

// env is the common setup: a fresh server, a connection, a JetStream handle, and
// a limits-retention stream named "events" over "events.>".
func env(t *testing.T) (*nats.Conn, jetstream.JetStream) {
	t.Helper()
	nc, _ := connect(t, startServer(t))
	js := jsHandle(t, nc)
	newStream(t, js, jetstream.StreamConfig{Name: "events", Subjects: []string{"events.>"}})
	return nc, js
}

func jsHandle(t *testing.T, nc *nats.Conn) jetstream.JetStream {
	t.Helper()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	return js
}

func newStream(t *testing.T, js jetstream.JetStream, cfg jetstream.StreamConfig) jetstream.Stream {
	t.Helper()
	st, err := js.CreateStream(t.Context(), cfg)
	if err != nil {
		t.Fatalf("create stream %s: %v", cfg.Name, err)
	}
	return st
}

func newConsumer(t *testing.T, js jetstream.JetStream, stream string, cfg jetstream.ConsumerConfig) jetstream.Consumer {
	t.Helper()
	cons, err := js.CreateOrUpdateConsumer(t.Context(), stream, cfg)
	if err != nil {
		t.Fatalf("create consumer %s/%s: %v", stream, cfg.Durable, err)
	}
	return cons
}

func publish(t *testing.T, js jetstream.JetStream, subject string, payloads ...string) {
	t.Helper()
	for _, p := range payloads {
		if _, err := js.Publish(t.Context(), subject, []byte(p)); err != nil {
			t.Fatalf("publish %s: %v", subject, err)
		}
	}
}

func info(t *testing.T, cons jetstream.Consumer) *jetstream.ConsumerInfo {
	t.Helper()
	ci, err := cons.Info(t.Context())
	if err != nil {
		t.Fatalf("consumer info: %v", err)
	}
	return ci
}

// fetchAll pulls up to n messages and returns them, leaving their disposition to
// the caller. A short MaxWait keeps a test that expects fewer than n from
// stalling.
func fetchAll(t *testing.T, cons jetstream.Consumer, n int) []jetstream.Msg {
	t.Helper()
	batch, err := cons.Fetch(n, jetstream.FetchMaxWait(2*time.Second))
	if err != nil {
		t.Fatalf("fetch %d: %v", n, err)
	}
	var got []jetstream.Msg
	for m := range batch.Messages() {
		got = append(got, m)
	}
	if err := batch.Error(); err != nil {
		t.Fatalf("fetch %d: batch error: %v", n, err)
	}
	return got
}

// meta is the message's JetStream metadata, which every belief about delivery
// counting and sequencing is read from.
func meta(t *testing.T, m jetstream.Msg) *jetstream.MsgMetadata {
	t.Helper()
	md, err := m.Metadata()
	if err != nil {
		t.Fatalf("metadata: %v", err)
	}
	return md
}

// delivery is one observed delivery: which stream message, which delivery
// attempt, and when it arrived. Redelivery timing is asserted from the gaps
// between consecutive arrival times.
type delivery struct {
	streamSeq    uint64
	consumerSeq  uint64
	numDelivered uint64
	at           time.Time
}

func (d delivery) String() string {
	return fmt.Sprintf("{stream:%d consumer:%d delivered:%d}", d.streamSeq, d.consumerSeq, d.numDelivered)
}

// recorder collects deliveries from a consume callback. The callback runs on
// nats.go's goroutine, so every field is mutex-guarded.
type recorder struct {
	mu  sync.Mutex
	got []delivery
}

// record timestamps a delivery. Call it first in the callback, before any
// disposition, so the recorded time is arrival time and not handler time.
func (r *recorder) record(t *testing.T, m jetstream.Msg) delivery {
	t.Helper()
	md, err := m.Metadata()
	if err != nil {
		r.mu.Lock()
		defer r.mu.Unlock()
		t.Errorf("metadata in consume callback: %v", err)
		return delivery{}
	}
	d := delivery{
		streamSeq:    md.Sequence.Stream,
		consumerSeq:  md.Sequence.Consumer,
		numDelivered: md.NumDelivered,
		at:           time.Now(),
	}
	r.mu.Lock()
	r.got = append(r.got, d)
	r.mu.Unlock()
	return d
}

func (r *recorder) snapshot() []delivery {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]delivery(nil), r.got...)
}

// waitForN blocks until at least n deliveries have been recorded, then returns
// them. It fails the test rather than returning short, so callers can index the
// result.
func (r *recorder) waitForN(t *testing.T, n int) []delivery {
	t.Helper()
	deadline := time.Now().Add(waitTimeout)
	for time.Now().Before(deadline) {
		if got := r.snapshot(); len(got) >= n {
			return got
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("saw %v deliveries, want at least %d", r.snapshot(), n)
	return nil
}

// consume runs a consume loop for the test's duration, recording every delivery
// before handing the message to onMsg. onMsg may be nil, which is itself a
// disposition under test: "do nothing" is what a handler that dies, blocks, or
// deliberately declines to answer looks like to the broker.
func consume(t *testing.T, cons jetstream.Consumer, onMsg func(jetstream.Msg)) *recorder {
	t.Helper()
	r := &recorder{}
	cc, err := cons.Consume(func(m jetstream.Msg) {
		r.record(t, m)
		if onMsg != nil {
			onMsg(m)
		}
	}, jetstream.PullMaxMessages(1))
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	t.Cleanup(cc.Stop)
	return r
}

// gapsBetween returns the intervals between consecutive deliveries: gap i is the
// wait the server served BEFORE delivery i+2 (deliveries are 1-indexed), which is
// the form every ladder assertion is stated in.
func gapsBetween(ds []delivery) []time.Duration {
	var gaps []time.Duration
	for i := 1; i < len(ds); i++ {
		gaps = append(gaps, ds[i].at.Sub(ds[i-1].at))
	}
	return gaps
}

// Timing tolerances. The lower bound is tight — a redelivery that arrives
// materially early means the server used a different (usually smaller) rung, which
// is exactly the class of error the suite exists to catch. The upper bound is
// loose, because a busy CI box can delay a wakeup but cannot make one early.
//
// Neither is the real constraint. A tolerance WIDER than the distance to the next
// plausible explanation makes a timing assertion unfalsifiable however carefully
// its expected value was derived — a ±500ms band around a 150ms rung silently
// accepts the 400ms rung beside it, so the fixture would pass under exactly the
// off-by-one it exists to reject. assertGap therefore takes the rival delays and
// tightens the band against them; these two are only the outer caps.
const (
	gapEarlySlack = 40 * time.Millisecond
	gapLateSlack  = 500 * time.Millisecond
	// minRivalMargin is the smallest half-separation a fixture may leave between
	// its expected delay and the nearest rival. Below this the two are not
	// separable on a loaded machine — jitter would either fail a correct run or
	// admit a wrong one — so the fixture, not the assertion, is what has to
	// change: spread the ladder's rungs further apart.
	minRivalMargin = 150 * time.Millisecond
)

// assertGap pins an observed redelivery gap to the delay the server should have
// served, and — because a band is only as strong as what it EXCLUDES — narrows the
// tolerance to half the distance to the nearest rival: a delay the broker could
// plausibly have served instead. Pass every such candidate: the adjacent ladder
// rungs, the bare AckWait, the un-stretched request. A fixture whose rivals sit
// closer than minRivalMargin fails outright, so indistinguishable delays cannot be
// reintroduced by choosing tighter fixture values later.
//
// label names the wait ("wait before delivery 3") so a failure reads as a ladder
// position rather than an index, and the message names the rival the observation
// landed on when it does.
func assertGap(t *testing.T, label string, got, want time.Duration, rivals ...time.Duration) {
	t.Helper()
	early, late := gapEarlySlack, gapLateSlack
	for _, rival := range rivals {
		if rival == want {
			continue // the same delay appearing twice in a ladder is not a rival
		}
		margin := (want - rival).Abs() / 2
		if margin < minRivalMargin {
			t.Fatalf("%s: fixture cannot separate the expected %v from the rival %v "+
				"(half-separation %v < %v): spread the fixture's delays further apart rather than "+
				"asserting a band that would accept either",
				label, want, rival, margin, minRivalMargin)
		}
		early, late = min(early, margin), min(late, margin)
	}
	low, high := want-early, want+late
	if got >= low && got <= high {
		return
	}
	// Name the single closest rival when the observation is nearer one than the
	// expectation: that is the diagnosis, not just the miss.
	nearest, found := time.Duration(0), false
	for _, rival := range rivals {
		if rival == want || (got-rival).Abs() >= (got-want).Abs() {
			continue
		}
		if !found || (got-rival).Abs() < (got-nearest).Abs() {
			nearest, found = rival, true
		}
	}
	if found {
		t.Errorf("%s: gap %v, want ~%v (band %v..%v); the observation is nearest the rival %v, which is what the server appears to have served",
			label, got.Round(time.Millisecond), want, low, high, nearest)
		return
	}
	t.Errorf("%s: gap %v, want ~%v (band %v..%v)", label, got.Round(time.Millisecond), want, low, high)
}

// assertImmediate pins a redelivery that must not wait on any ladder rung — the
// plain-Nak path, whose whole point is that it does not.
func assertImmediate(t *testing.T, label string, got time.Duration) {
	t.Helper()
	if got > 150*time.Millisecond {
		t.Errorf("%s: gap %v, want immediate redelivery (<150ms)", label, got.Round(time.Millisecond))
	}
}

// pedanticCreateConsumer creates a consumer through a hand-rolled JS API request
// with pedantic mode set, returning the API error the server replied with (nil on
// success). nats.go's jetstream package has no pedantic option — but NACK's
// controller drives consumer CRs through jsm.go, which sets it, so a fleet
// Consumer CR is validated under rules the ordinary client path never applies.
// Both paths are asserted, because a config that this module accepts and NACK
// rejects is a deployment that fails only in the cluster.
func pedanticCreateConsumer(t *testing.T, nc *nats.Conn, stream string, cfg jetstream.ConsumerConfig) *jetstream.APIError {
	t.Helper()
	req, err := json.Marshal(struct {
		Stream   string                   `json:"stream_name"`
		Config   jetstream.ConsumerConfig `json:"config"`
		Action   string                   `json:"action,omitempty"`
		Pedantic bool                     `json:"pedantic"`
	}{Stream: stream, Config: cfg, Pedantic: true})
	if err != nil {
		t.Fatalf("marshal pedantic consumer request: %v", err)
	}
	name := cfg.Durable
	if name == "" {
		name = cfg.Name
	}
	reply, err := nc.Request(fmt.Sprintf("$JS.API.CONSUMER.CREATE.%s.%s", stream, name), req, 5*time.Second)
	if err != nil {
		t.Fatalf("pedantic consumer create request: %v", err)
	}
	var resp struct {
		Error *jetstream.APIError `json:"error"`
	}
	if err := json.Unmarshal(reply.Data, &resp); err != nil {
		t.Fatalf("unmarshal pedantic response %q: %v", reply.Data, err)
	}
	return resp.Error
}

// TestMain names the server version on the way out of a FAILING run.
//
// Attribution is only useful when something is red, and it has to survive the
// non-verbose `go test` that the merge gate runs (mise run test:ci): a t.Logf in a
// passing test is discarded without -v, which would leave a semantic failure in
// this package with no record of which server produced it. Writing to stderr after
// a failed run puts the version in the same output as the failures, and keeps a
// green run silent.
func TestMain(m *testing.M) {
	code := m.Run()
	if code != 0 {
		fmt.Fprintf(os.Stderr, "\nFAIL brokersemantics: measured against embedded nats-server %s "+
			"(the version pinned in go.mod). A semantic failure here is a finding about THIS version: "+
			"re-decide the belief in the doc comment the test cites, rather than adjusting the test.\n",
			natsserver.VERSION)
	}
	os.Exit(code)
}

// TestPinnedServerVersion pins that the server's self-reported version is
// consistent, so the attribution TestMain prints means something. It asserts only
// that consistency — pinning a literal version here would turn every deliberate
// bump into a spurious failure, when the point of a bump is to re-run the suite and
// see which beliefs still hold.
func TestPinnedServerVersion(t *testing.T) {
	t.Parallel()
	_, js := env(t)
	cons := newConsumer(t, js, "events", jetstream.ConsumerConfig{
		Durable:   "version_probe",
		AckPolicy: jetstream.AckExplicitPolicy,
	})
	// The server stamps the version that created the consumer into its metadata,
	// which is the same field prod configs were read from in ENT-1492.
	got := info(t, cons).Config.Metadata["_nats.ver"]
	t.Logf("embedded nats-server VERSION=%s, consumer metadata _nats.ver=%s", natsserver.VERSION, got)
	if got == "" {
		t.Fatal("consumer metadata carries no _nats.ver; cannot attribute this suite's measurements to a server version")
	}
	if got != natsserver.VERSION {
		t.Errorf("linked server VERSION %q and consumer metadata _nats.ver %q disagree; "+
			"this suite's measurements cannot be attributed to a single version", natsserver.VERSION, got)
	}
}

// waitLabel names a ladder position for an assertion message, so a failure reads
// "wait before delivery 3" rather than "gaps[1]".
func waitLabel(delivery int) string {
	return fmt.Sprintf("wait before delivery %d", delivery)
}
