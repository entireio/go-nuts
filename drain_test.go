package entwine

import (
	"context"
	"fmt"
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
	Drain(t.Context(), nil, "nil", time.Second)
}
