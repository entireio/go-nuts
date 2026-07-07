package entwine

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
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
