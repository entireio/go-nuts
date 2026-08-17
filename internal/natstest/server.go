// Package natstest owns the lifecycle contract for embedded NATS servers used
// by this module's tests.
package natstest

import (
	"fmt"
	"sync"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
)

const (
	defaultReadyTimeout = 10 * time.Second
	startJoinTimeout    = 5 * time.Second
)

// Run starts an embedded server and fails t if it is not ready within the
// default timeout. opts is copied by value before the server takes ownership.
func Run(t testing.TB, opts natsserver.Options) *natsserver.Server {
	t.Helper()
	s, err := TryRun(t, opts, defaultReadyTimeout)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// TryRun starts an embedded server with a bounded readiness wait. Every exit
// path shuts it down, waits for server shutdown, and joins the Start goroutine
// before returning or releasing the caller's test resources.
func TryRun(t testing.TB, opts natsserver.Options, readyTimeout time.Duration) (*natsserver.Server, error) {
	t.Helper()
	s, err := natsserver.NewServer(&opts)
	if err != nil {
		return nil, fmt.Errorf("new embedded nats server: %w", err)
	}

	startDone := make(chan struct{})
	go func() {
		defer close(startDone)
		s.Start()
	}()

	var shutdownOnce sync.Once
	shutdown := func() {
		shutdownOnce.Do(func() {
			s.Shutdown()
			s.WaitForShutdown()
			select {
			case <-startDone:
			case <-time.After(startJoinTimeout):
				t.Errorf("embedded nats Start did not return after shutdown")
			}
		})
	}
	t.Cleanup(shutdown)
	if !s.ReadyForConnections(readyTimeout) {
		shutdown()
		return nil, fmt.Errorf("embedded nats server not ready in %s", readyTimeout)
	}
	return s, nil
}
