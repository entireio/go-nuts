package nuts

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestIsShutdownFetchErr(t *testing.T) {
	done, cancel := context.WithCancel(t.Context())
	cancel()
	live := t.Context()

	tests := []struct {
		name string
		ctx  context.Context
		err  error
		want bool
	}{
		{"closed during shutdown", done, nats.ErrConnectionClosed, true},
		{"draining during shutdown", done, nats.ErrConnectionDraining, true},
		{"wrapped closed during shutdown", done, fmt.Errorf("fetch: %w", nats.ErrConnectionClosed), true},
		{"unrelated error during shutdown", done, nats.ErrTimeout, false},
		{"nil error during shutdown", done, nil, false},
		{"closed while still running", live, nats.ErrConnectionClosed, false},

		// Transient but not self-inflicted: these can coincide with shutdown
		// while meaning something real (a durable deleted mid-deploy, JetStream
		// losing quorum as we exit), so they must not be swallowed — callers
		// log them and IsTransientFetchErr picks the severity.
		{"no responders during shutdown", done, nats.ErrNoResponders, false},
		{"fetch disconnected during shutdown", done, nats.ErrFetchDisconnected, false},
		{"leadership changed during shutdown", done, nats.ErrConsumerLeadershipChanged, false},
		{"consumer deleted during shutdown", done, nats.ErrConsumerDeleted, false},
		{"server disconnected during shutdown", done, nats.ErrDisconnected, false},

		// Still running: recoverable, but not OUR teardown. Callers must keep
		// looping rather than exit, so this stays false.
		{"no responders while still running", live, nats.ErrNoResponders, false},
		{"leadership changed while still running", live, nats.ErrConsumerLeadershipChanged, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsShutdownFetchErr(tt.ctx, tt.err); got != tt.want {
				t.Errorf("IsShutdownFetchErr = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsTransientFetchErr(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"connection closed", nats.ErrConnectionClosed, true},
		{"connection draining", nats.ErrConnectionDraining, true},
		{"server disconnected", nats.ErrDisconnected, true},
		{"no responders", nats.ErrNoResponders, true},
		{"fetch disconnected", nats.ErrFetchDisconnected, true},
		{"consumer leadership changed", nats.ErrConsumerLeadershipChanged, true},
		{"wrapped", fmt.Errorf("fetch: %w", nats.ErrFetchDisconnected), true},

		// Excluded on purpose — see the doc comment. A deleted durable is
		// operator action whose only evidence is this log line; an idle poll is
		// the steady state of a quiet consumer; a fetch deadline is too
		// ambiguous to swallow (a slow server and a wedged one look alike).
		{"consumer deleted", nats.ErrConsumerDeleted, false},
		{"idle poll timeout", nats.ErrTimeout, false},
		{"context deadline", context.DeadlineExceeded, false},
		{"unrelated", errors.New("boom"), false},
		{"nil", nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsTransientFetchErr(tt.err); got != tt.want {
				t.Errorf("IsTransientFetchErr(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestIsTransientSubscribeErr(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		// Everything fetch-transient is subscribe-transient too.
		{"connection closed", nats.ErrConnectionClosed, true},
		{"no responders", nats.ErrNoResponders, true},

		// The subscribe-path extras: rollout ordering and a slow JS API.
		{"stream not found", nats.ErrStreamNotFound, true},
		{"timeout", nats.ErrTimeout, true},
		{"context deadline", context.DeadlineExceeded, true},
		{"wrapped timeout", fmt.Errorf("subscribe: %w", nats.ErrTimeout), true},

		// Still faults everywhere.
		{"consumer deleted", nats.ErrConsumerDeleted, false},
		{"unrelated", errors.New("boom"), false},
		{"nil", nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsTransientSubscribeErr(tt.err); got != tt.want {
				t.Errorf("IsTransientSubscribeErr(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

// The shutdown set is deliberately NARROWER than the transient set: only the
// two self-inflicted teardown errors (closed, draining) may exit silently.
// Pin that relationship — an earlier revision defined shutdown as "transient
// AND ctx done", which silently discarded the one log line proving a durable
// was deleted mid-deploy.
func TestShutdownFetchErrNarrowerThanTransient(t *testing.T) {
	done, cancel := context.WithCancel(t.Context())
	cancel()

	for _, err := range transientFetchErrs {
		selfInflicted := errors.Is(err, nats.ErrConnectionClosed) || errors.Is(err, nats.ErrConnectionDraining)
		if got := IsShutdownFetchErr(done, err); got != selfInflicted {
			t.Errorf("IsShutdownFetchErr(done, %v) = %v, want %v — only our own teardown exits silently", err, got, selfInflicted)
		}
		if IsShutdownFetchErr(t.Context(), err) {
			t.Errorf("IsShutdownFetchErr(live, %v) = true, want false while still running", err)
		}
	}
}

func TestDrainNilIsNoOp(t *testing.T) {
	// A nil connection must not panic and must return immediately.
	if err := Drain(t.Context(), nil, "nil", nil, time.Second); err != nil {
		t.Fatalf("Drain(nil) err = %v, want nil", err)
	}
}

// TestDrainWaitsForClose restores the origin embedded-server coverage: Drain
// must block until the connection actually reaches CLOSED and return well
// inside its backstop on a reachable server.
func TestDrainWaitsForClose(t *testing.T) {
	nc, err := Connect(t.Context(), runEmbeddedServer(t), WithoutTLS())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	start := time.Now()
	if err := Drain(t.Context(), nc, "worker", nil, 5*time.Second); err != nil {
		t.Fatalf("Drain() err = %v, want nil", err)
	}
	elapsed := time.Since(start)

	if !nc.IsClosed() {
		t.Fatal("Drain returned but the connection is not CLOSED")
	}
	if elapsed > 4*time.Second {
		t.Fatalf("Drain took %v; expected a prompt close on a reachable server", elapsed)
	}
}

// TestDrainTimeoutIsReturnedAndCloses guards the observable backstop: a drain
// that cannot join an in-flight subscription before the caller's timeout must
// return ErrDrainTimeout and force the connection closed rather than leave an
// ambiguous asynchronous drain running.
func TestDrainTimeoutIsReturnedAndCloses(t *testing.T) {
	nc, err := Connect(t.Context(), runEmbeddedServer(t), WithoutTLS(),
		WithNATSOptions(nats.DrainTimeout(5*time.Second)))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	started := make(chan struct{})
	release := make(chan struct{})
	if _, err := nc.Subscribe("slow", func(*nats.Msg) {
		close(started)
		<-release
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush subscription: %v", err)
	}
	if err := nc.Publish("slow", []byte("work")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush publish: %v", err)
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("slow handler did not start")
	}

	err = Drain(t.Context(), nc, "worker", nil, 25*time.Millisecond)
	close(release)
	if !errors.Is(err, ErrDrainTimeout) {
		t.Fatalf("Drain() err = %v, want ErrDrainTimeout", err)
	}
	if !nc.IsClosed() {
		t.Fatal("connection remained open after drain timeout")
	}
}

// TestDrainTimeoutPrefersCompletedDrain makes the CLOSED signal and timeout
// ready together. Drain's shared wait helper must prefer CLOSED so a completed
// drain is never turned into ErrDrainTimeout by select choosing the timer.
func TestDrainTimeoutPrefersCompletedDrain(t *testing.T) {
	closed := make(chan nats.Status)
	close(closed)

	for range 100 {
		if waitTimedOut(closed, 0) {
			t.Fatal("waitTimedOut reported a timeout for an already-closed connection")
		}
	}
}

// TestDrainAlreadyClosedConn is the second origin test: Drain on a
// pre-closed connection is a prompt no-op, not a block on the CLOSED wait.
func TestDrainAlreadyClosedConn(t *testing.T) {
	nc, err := Connect(t.Context(), runEmbeddedServer(t), WithoutTLS())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	nc.Close()

	done := make(chan struct{})
	go func() {
		if err := Drain(t.Context(), nc, "worker", nil, 5*time.Second); err != nil {
			t.Errorf("Drain(already closed) err = %v, want nil", err)
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Drain on an already-closed connection did not return promptly")
	}
}

// TestDrainConcurrentCloseIsClean stresses the race between the initial
// IsClosed check and nc.Drain. If Close wins after the check, nats.go returns
// ErrConnectionClosed; Drain must still classify that as the documented clean
// already-closed no-op.
func TestDrainConcurrentCloseIsClean(t *testing.T) {
	url := runEmbeddedServer(t)
	for range 100 {
		nc, err := Connect(t.Context(), url, WithoutTLS())
		if err != nil {
			t.Fatalf("connect: %v", err)
		}

		start := make(chan struct{})
		closed := make(chan struct{})
		go func() {
			<-start
			nc.Close()
			close(closed)
		}()
		close(start)

		if err := Drain(t.Context(), nc, "worker", nil, time.Second); err != nil {
			t.Fatalf("Drain racing Close() err = %v, want nil", err)
		}
		<-closed
	}
}

// TestDrainPreservesCallerClosedHandler guards the fix for Drain clobbering the
// connection's ClosedHandler: a handler the caller installed must still fire.
func TestDrainPreservesCallerClosedHandler(t *testing.T) {
	fired := make(chan struct{})
	nc, err := Connect(t.Context(), runEmbeddedServer(t), WithoutTLS(),
		WithNATSOptions(nats.ClosedHandler(func(*nats.Conn) { close(fired) })))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	if err := Drain(t.Context(), nc, "worker", nil, 5*time.Second); err != nil {
		t.Fatalf("Drain() err = %v, want nil", err)
	}

	select {
	case <-fired:
	case <-time.After(2 * time.Second):
		t.Fatal("caller-installed ClosedHandler did not fire; Drain clobbered it")
	}
}

// TestDrainConcurrentCallsDoNotClobber guards the other half of the
// ClosedHandler fix: two concurrent Drains on the same connection must both
// observe the close, not have one block on its full backstop because the other
// stole its completion signal.
func TestDrainConcurrentCallsDoNotClobber(t *testing.T) {
	nc, err := Connect(t.Context(), runEmbeddedServer(t), WithoutTLS())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	var wg sync.WaitGroup
	start := time.Now()
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := Drain(t.Context(), nc, "worker", nil, 5*time.Second); err != nil {
				t.Errorf("concurrent Drain() err = %v, want nil", err)
			}
		}()
	}
	wg.Wait()

	if elapsed := time.Since(start); elapsed > 4*time.Second {
		t.Fatalf("concurrent Drain took %v; a call blocked on its backstop (completion signal clobbered)", elapsed)
	}
	if !nc.IsClosed() {
		t.Fatal("connection not closed after concurrent Drain")
	}
}

// TestDrainLogsToProvidedLogger guards the fix for drain diagnostics bypassing
// the caller's logger: the success line must land on the passed logger, not on
// slog.Default().
func TestDrainLogsToProvidedLogger(t *testing.T) {
	nc, err := Connect(t.Context(), runEmbeddedServer(t), WithoutTLS())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	logger, h := newCapturingLogger()
	if err := Drain(t.Context(), nc, "worker", logger, 5*time.Second); err != nil {
		t.Fatalf("Drain() err = %v, want nil", err)
	}

	if !h.has("nuts: NATS drained") {
		t.Fatalf("drain success not logged to the provided logger; saw %v", h.snapshot())
	}
}
