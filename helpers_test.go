package entwine

import (
	"context"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
)

// runEmbeddedServer starts an in-process NATS server on a random loopback port
// and returns its client URL. The server is shut down when the test ends.
func runEmbeddedServer(t *testing.T) string {
	t.Helper()
	s, err := natsserver.NewServer(&natsserver.Options{
		Host:   "127.0.0.1",
		Port:   -1, // pick a free port
		NoLog:  true,
		NoSigs: true,
	})
	if err != nil {
		t.Fatalf("new embedded nats server: %v", err)
	}
	go s.Start()
	if !s.ReadyForConnections(10 * time.Second) {
		t.Fatal("embedded nats server not ready in time")
	}
	t.Cleanup(s.Shutdown)
	return s.ClientURL()
}

// capturingHandler is a thread-safe slog.Handler that records log messages so
// tests can assert what was logged, including from background goroutines.
type capturingHandler struct {
	mu   sync.Mutex
	msgs []string
}

func (h *capturingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *capturingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	h.msgs = append(h.msgs, r.Message)
	h.mu.Unlock()
	return nil
}

func (h *capturingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *capturingHandler) WithGroup(string) slog.Handler      { return h }

func (h *capturingHandler) has(msg string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.Contains(h.msgs, msg)
}

func (h *capturingHandler) snapshot() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.Clone(h.msgs)
}

// waitFor blocks until a message equal to msg has been logged, or fails the
// test after a short deadline. Used to synchronize on logs emitted from a
// background loop goroutine.
func (h *capturingHandler) waitFor(t *testing.T, msg string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if h.has(msg) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("did not observe log %q; saw %v", msg, h.snapshot())
}

// newCapturingLogger returns a logger writing to a fresh capturingHandler.
func newCapturingLogger() (*slog.Logger, *capturingHandler) {
	h := &capturingHandler{}
	return slog.New(h), h
}
