package nuts

import (
	"context"
	"errors"
	"fmt"
	"strings"
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
	if err := g.Shutdown(); err != nil {
		t.Fatalf("Shutdown() err = %v, want nil", err)
	}
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
	if err := g.Shutdown(); err != nil {
		t.Fatalf("Shutdown() err = %v, want nil", err)
	}
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
	err := g.Shutdown() // must return after ~joinTimeout, not hang
	elapsed := time.Since(start)
	close(release) // let the stuck goroutine exit
	if err == nil || !strings.Contains(err.Error(), "join timeout") {
		t.Fatalf("Shutdown() err = %v, want join-timeout error", err)
	}

	if elapsed < 50*time.Millisecond {
		t.Fatalf("Shutdown returned in %v, before the join timeout", elapsed)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("Shutdown took %v; join timeout not enforced", elapsed)
	}
}

// TestJoinTimedOutPrefersCompletedJoin makes both select cases ready and
// verifies completion always wins. Without the timeout-branch recheck this is
// pseudo-random and can turn a clean shutdown into a join-timeout failure.
func TestJoinTimedOutPrefersCompletedJoin(t *testing.T) {
	done := make(chan struct{})
	close(done)

	for range 100 {
		if joinTimedOut(done, 0) {
			t.Fatal("joinTimedOut reported a timeout for an already-completed join")
		}
	}
}

func TestShutdownIsIdempotent(t *testing.T) {
	g := NewShutdownGroup(t.Context())
	var runs atomic.Int32
	g.Go(func(ctx context.Context) {
		<-ctx.Done()
		runs.Add(1)
	})
	if err := g.Shutdown(); err != nil {
		t.Fatalf("first Shutdown() err = %v, want nil", err)
	}
	if err := g.Shutdown(); err != nil { // second call is a no-op
		t.Fatalf("second Shutdown() err = %v, want nil", err)
	}
	if runs.Load() != 1 {
		t.Fatalf("loop ran %d times, want 1", runs.Load())
	}
}

// TestGoPanicCancelsGroup guards that a panic is recovered for graceful
// teardown but still becomes a process-visible group failure. If it were only
// logged, a critical consumer could die while the pod remained ready.
func TestGoPanicCancelsGroup(t *testing.T) {
	logger, h := newCapturingLogger()
	g := NewShutdownGroup(t.Context(), WithGroupLogger(logger))

	siblingStopped := make(chan struct{})
	g.Go(func(ctx context.Context) {
		<-ctx.Done()
		close(siblingStopped)
	})
	g.Go(func(context.Context) { panic("boom") })

	h.waitFor(t, "nuts: background loop panicked")
	select {
	case <-g.Context().Done():
	case <-time.After(2 * time.Second):
		t.Fatal("group context was not cancelled after loop panic")
	}
	if cause := context.Cause(g.Context()); cause == nil || !strings.Contains(cause.Error(), "panicked") {
		t.Fatalf("context cause = %v, want panic failure", cause)
	}
	select {
	case <-siblingStopped:
	case <-time.After(2 * time.Second):
		t.Fatal("sibling loop was not cancelled after loop panic")
	}
	if err := g.Err(); err == nil || !strings.Contains(err.Error(), "panicked") {
		t.Fatalf("Err() = %v, want panic failure", err)
	}

	if err := g.Shutdown(); err == nil || !strings.Contains(err.Error(), "panicked") {
		t.Fatalf("Shutdown() err = %v, want panic failure", err)
	}
}

// TestRecordErrorCancelsOnlyForStoredFailure guards the first-failure
// invariant shared by Err and the group's cancellation cause. A later live
// failure must not cancel with an error that lost the race to be recorded.
func TestRecordErrorCancelsOnlyForStoredFailure(t *testing.T) {
	g := NewShutdownGroup(t.Context())
	first := errors.New("first failure")
	second := errors.New("second failure")

	g.recordError(first, false)
	g.recordError(second, true)

	if got := g.Err(); !errors.Is(got, first) {
		t.Fatalf("Err() = %v, want first failure", got)
	}
	if cause := context.Cause(g.Context()); cause != nil {
		t.Fatalf("context cause = %v, want nil; losing failure cancelled the group", cause)
	}
}

// TestGoPrematureReturnCancelsGroup guards the dead-man detection: a loop that
// returns while the context is still live is a process-visible failure.
func TestGoPrematureReturnCancelsGroup(t *testing.T) {
	logger, h := newCapturingLogger()
	g := NewShutdownGroup(t.Context(), WithGroupLogger(logger))

	g.Go(func(context.Context) {
		// Returns immediately without waiting for cancellation.
	})

	h.waitFor(t, "nuts: background loop returned before shutdown")
	select {
	case <-g.Context().Done():
	case <-time.After(2 * time.Second):
		t.Fatal("group context was not cancelled after premature return")
	}
	if cause := context.Cause(g.Context()); cause == nil || !strings.Contains(cause.Error(), "returned before shutdown") {
		t.Fatalf("context cause = %v, want premature-return failure", cause)
	}
	if err := g.Shutdown(); err == nil || !strings.Contains(err.Error(), "returned before shutdown") {
		t.Fatalf("Shutdown() err = %v, want premature-return failure", err)
	}
}

// TestGoCleanReturnOnShutdownIsSilent guards that a normal return in response to
// cancellation is NOT flagged as a dead subsystem.
func TestGoCleanReturnOnShutdownIsSilent(t *testing.T) {
	logger, h := newCapturingLogger()
	g := NewShutdownGroup(t.Context(), WithGroupLogger(logger))

	g.Go(func(ctx context.Context) { <-ctx.Done() })
	if err := g.Shutdown(); err != nil {
		t.Fatalf("Shutdown() err = %v, want nil", err)
	}

	if h.has("nuts: background loop returned before shutdown") {
		t.Fatalf("a cancellation-driven return was wrongly flagged; saw %v", h.snapshot())
	}
}

// TestGoAfterShutdownIsRefused guards that registering a loop once shutdown has
// begun does not start it (it would never be joined before draining).
func TestGoAfterShutdownIsRefused(t *testing.T) {
	g := NewShutdownGroup(t.Context())
	if err := g.Shutdown(); err != nil {
		t.Fatalf("Shutdown() err = %v, want nil", err)
	}

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
	if err := g.Shutdown(); err != nil {
		t.Fatalf("Shutdown() err = %v, want nil", err)
	}

	g.AddConn("late", nc) // after shutdown: must be refused, not drained

	if nc.IsClosed() {
		t.Fatal("late-registered connection was drained despite shutdown having completed")
	}
	if !h.has("nuts: ShutdownGroup.AddConn called after shutdown; connection not registered for drain") {
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
		if err := g.Shutdown(); err != nil {
			t.Fatalf("Shutdown() err = %v, want nil", err)
		}
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

	if err := g.Shutdown(); err != nil {
		t.Fatalf("Shutdown() err = %v, want nil", err)
	}

	for i, nc := range conns {
		if !nc.IsClosed() {
			t.Fatalf("conn-%d not closed after Shutdown", i)
		}
	}
}
