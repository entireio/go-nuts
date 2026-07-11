package nuts

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
	return runEmbeddedServerWith(t, &natsserver.Options{}).ClientURL()
}

// runEmbeddedServerWith starts an in-process NATS server from opts (host, port,
// and log/signal handling are forced to test-safe values) and returns the
// server handle for tests that drive server-side behavior such as lame duck
// mode. The server is shut down when the test ends.
func runEmbeddedServerWith(t *testing.T, opts *natsserver.Options) *natsserver.Server {
	t.Helper()
	opts.Host = "127.0.0.1"
	opts.Port = -1 // pick a free port
	opts.NoLog = true
	opts.NoSigs = true
	s, err := natsserver.NewServer(opts)
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
