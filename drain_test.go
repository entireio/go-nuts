package nuts

import (
	"context"
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsShutdownFetchErr(tt.ctx, tt.err); got != tt.want {
				t.Errorf("IsShutdownFetchErr = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDrainNilIsNoOp(t *testing.T) {
	// A nil connection must not panic and must return immediately.
	Drain(t.Context(), nil, "nil", nil, time.Second)
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
	Drain(t.Context(), nc, "worker", nil, 5*time.Second)
	elapsed := time.Since(start)

	if !nc.IsClosed() {
		t.Fatal("Drain returned but the connection is not CLOSED")
	}
	if elapsed > 4*time.Second {
		t.Fatalf("Drain took %v; expected a prompt close on a reachable server", elapsed)
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
		Drain(t.Context(), nc, "worker", nil, 5*time.Second)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Drain on an already-closed connection did not return promptly")
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

	Drain(t.Context(), nc, "worker", nil, 5*time.Second)

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
			Drain(t.Context(), nc, "worker", nil, 5*time.Second)
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
	Drain(t.Context(), nc, "worker", logger, 5*time.Second)

	if !h.has("nuts: NATS drained") {
		t.Fatalf("drain success not logged to the provided logger; saw %v", h.snapshot())
	}
}
