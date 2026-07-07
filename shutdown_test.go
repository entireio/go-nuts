package entwine

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestShutdownGroupJoinsLoops(t *testing.T) {
	g := NewShutdownGroup(t.Context())
	var stopped atomic.Int32
	for range 3 {
		g.Go(func(ctx context.Context) {
			<-ctx.Done()
			stopped.Add(1)
		})
	}
	// A nil connection registered for drain must be tolerated.
	g.AddConn("nil-conn", nil)

	start := time.Now()
	g.Shutdown()
	if elapsed := time.Since(start); elapsed > DefaultJoinTimeout {
		t.Fatalf("Shutdown took %v, expected prompt return", elapsed)
	}
	if n := stopped.Load(); n != 3 {
		t.Fatalf("stopped = %d, want 3 (loops not joined before drain)", n)
	}
}

func TestShutdownGroupCancelsContext(t *testing.T) {
	g := NewShutdownGroup(t.Context())
	if g.Context().Err() != nil {
		t.Fatal("context cancelled before Shutdown")
	}
	g.Shutdown()
	if g.Context().Err() == nil {
		t.Fatal("context not cancelled after Shutdown")
	}
}

func TestShutdownGroupJoinTimeout(t *testing.T) {
	g := NewShutdownGroup(t.Context(), WithJoinTimeout(50*time.Millisecond))
	release := make(chan struct{})
	g.Go(func(context.Context) {
		<-release // deliberately ignores cancellation
	})

	start := time.Now()
	g.Shutdown() // must return after ~joinTimeout, not hang
	elapsed := time.Since(start)
	close(release) // let the stuck goroutine exit

	if elapsed < 50*time.Millisecond {
		t.Fatalf("Shutdown returned in %v, before the join timeout", elapsed)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("Shutdown took %v; join timeout not enforced", elapsed)
	}
}

func TestShutdownIsIdempotent(t *testing.T) {
	g := NewShutdownGroup(t.Context())
	var runs atomic.Int32
	g.Go(func(ctx context.Context) {
		<-ctx.Done()
		runs.Add(1)
	})
	g.Shutdown()
	g.Shutdown() // second call is a no-op
	if runs.Load() != 1 {
		t.Fatalf("loop ran %d times, want 1", runs.Load())
	}
}

// TestGoRecoversPanic guards that a panic in one loop is contained and logged
// rather than crashing the whole process. If the panic escaped, the test
// process itself would die.
func TestGoRecoversPanic(t *testing.T) {
	logger, h := newCapturingLogger()
	g := NewShutdownGroup(t.Context(), WithGroupLogger(logger))

	var sibling atomic.Bool
	g.Go(func(ctx context.Context) {
		<-ctx.Done()
		sibling.Store(true) // a sibling loop must survive the panic
	})
	g.Go(func(context.Context) { panic("boom") })

	h.waitFor(t, "entwine: background loop panicked")

	g.Shutdown() // must complete despite the earlier panic
	if !sibling.Load() {
		t.Fatal("sibling loop did not run to completion after the panic")
	}
}

// TestGoLogsPrematureReturn guards the dead-man detection: a loop that returns
// while the context is still live is surfaced at ERROR.
func TestGoLogsPrematureReturn(t *testing.T) {
	logger, h := newCapturingLogger()
	g := NewShutdownGroup(t.Context(), WithGroupLogger(logger))

	g.Go(func(context.Context) {
		// Returns immediately without waiting for cancellation.
	})

	h.waitFor(t, "entwine: background loop returned before shutdown")
}

// TestGoCleanReturnOnShutdownIsSilent guards that a normal return in response to
// cancellation is NOT flagged as a dead subsystem.
func TestGoCleanReturnOnShutdownIsSilent(t *testing.T) {
	logger, h := newCapturingLogger()
	g := NewShutdownGroup(t.Context(), WithGroupLogger(logger))

	g.Go(func(ctx context.Context) { <-ctx.Done() })
	g.Shutdown()

	if h.has("entwine: background loop returned before shutdown") {
		t.Fatalf("a cancellation-driven return was wrongly flagged; saw %v", h.snapshot())
	}
}

// TestGoAfterShutdownIsRefused guards that registering a loop once shutdown has
// begun does not start it (it would never be joined before draining).
func TestGoAfterShutdownIsRefused(t *testing.T) {
	g := NewShutdownGroup(t.Context())
	g.Shutdown()

	ran := make(chan struct{})
	g.Go(func(context.Context) { close(ran) })

	select {
	case <-ran:
		t.Fatal("loop registered after Shutdown was started anyway")
	case <-time.After(200 * time.Millisecond):
	}
}

// TestAddConnAfterShutdownIsRefused guards that a connection registered once
// shutdown has begun is refused (and logged) rather than silently omitted from
// the drain snapshot.
func TestAddConnAfterShutdownIsRefused(t *testing.T) {
	nc, err := Connect(t.Context(), runEmbeddedServer(t), WithoutTLS())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() {
		if !nc.IsClosed() {
			nc.Close()
		}
	})

	logger, h := newCapturingLogger()
	g := NewShutdownGroup(t.Context(), WithGroupLogger(logger))
	g.Shutdown()

	g.AddConn("late", nc) // after shutdown: must be refused, not drained

	if nc.IsClosed() {
		t.Fatal("late-registered connection was drained despite shutdown having completed")
	}
	if !h.has("entwine: ShutdownGroup.AddConn called after shutdown; connection not registered for drain") {
		t.Fatalf("expected a refusal warning for late AddConn; saw %v", h.snapshot())
	}
}

// TestGoConcurrentWithShutdown stresses the Go/Shutdown coordination. Under the
// race detector this catches both the "Add called concurrently with Wait"
// WaitGroup panic and any data race on the group's shared state.
func TestGoConcurrentWithShutdown(t *testing.T) {
	for range 200 {
		g := NewShutdownGroup(t.Context())
		var starters sync.WaitGroup
		for range 8 {
			starters.Add(1)
			go func() {
				defer starters.Done()
				g.Go(func(ctx context.Context) { <-ctx.Done() })
			}()
		}
		g.Shutdown()
		starters.Wait()
	}
}

// TestShutdownGroupDrainBackstop guards that the per-connection drain budget
// wins over the group default and that publish-flush headroom is added.
func TestShutdownGroupDrainBackstop(t *testing.T) {
	nc, err := Connect(t.Context(), runEmbeddedServer(t), WithoutTLS(), WithDrainTimeout(30*time.Second))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() {
		if !nc.IsClosed() {
			nc.Close()
		}
	})

	g := NewShutdownGroup(t.Context()) // group default is DefaultDrainTimeout (5s)

	if got, want := g.drainBackstop(nc), 30*time.Second+natsPublishDrainTimeout+time.Second; got != want {
		t.Fatalf("drainBackstop(conn) = %v, want %v (per-connection budget must win)", got, want)
	}
	if got, want := g.drainBackstop(nil), DefaultDrainTimeout+natsPublishDrainTimeout+time.Second; got != want {
		t.Fatalf("drainBackstop(nil) = %v, want %v (fallback to group default)", got, want)
	}
}

// TestShutdownGroupDrainsAllConns exercises the concurrent drain path and
// asserts every registered connection is drained to CLOSED.
func TestShutdownGroupDrainsAllConns(t *testing.T) {
	url := runEmbeddedServer(t)
	g := NewShutdownGroup(t.Context())

	conns := make([]*nats.Conn, 0, 3)
	for i := range 3 {
		name := fmt.Sprintf("conn-%d", i)
		nc, err := Connect(g.Context(), url, WithoutTLS(), WithName(name))
		if err != nil {
			t.Fatalf("connect %s: %v", name, err)
		}
		g.AddConn(name, nc)
		conns = append(conns, nc)
	}

	g.Shutdown()

	for i, nc := range conns {
		if !nc.IsClosed() {
			t.Fatalf("conn-%d not closed after Shutdown", i)
		}
	}
}
