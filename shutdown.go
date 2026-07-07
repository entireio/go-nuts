package entwine

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

// DefaultJoinTimeout bounds how long [ShutdownGroup.Shutdown] waits for
// background loops to return before draining the connections anyway.
const DefaultJoinTimeout = 10 * time.Second

// ShutdownGroup coordinates graceful shutdown of a set of background
// pull-consumer loops and the NATS connections they use. It encodes the
// ordering that prevents rollout noise (COR-923): on Shutdown it cancels the
// loops' context, waits for them to return, and only THEN drains the
// connections — so a Drain never races an in-flight Fetch.
//
// Typical use in a service main:
//
//	g := entwine.NewShutdownGroup(ctx)
//	nc, _ := entwine.Connect(g.Context(), url, entwine.WithName("worker"))
//	g.AddConn("worker", nc)
//	g.Go(func(ctx context.Context) { consumer.Run(ctx) }) // returns on ctx.Done
//	<-ctx.Done() // SIGTERM/SIGINT
//	g.Shutdown()
type ShutdownGroup struct {
	ctx          context.Context
	cancel       context.CancelFunc
	logger       *slog.Logger
	joinTimeout  time.Duration
	drainTimeout time.Duration

	wg    sync.WaitGroup
	mu    sync.Mutex
	conns []groupConn
	once  sync.Once
}

type groupConn struct {
	name string
	nc   *nats.Conn
}

// GroupOption configures a [ShutdownGroup].
type GroupOption func(*ShutdownGroup)

// WithGroupLogger sets the logger for shutdown diagnostics. Defaults to
// [slog.Default].
func WithGroupLogger(l *slog.Logger) GroupOption { return func(g *ShutdownGroup) { g.logger = l } }

// WithJoinTimeout overrides [DefaultJoinTimeout] — the bound on waiting for
// background loops to return.
func WithJoinTimeout(d time.Duration) GroupOption {
	return func(g *ShutdownGroup) { g.joinTimeout = d }
}

// WithGroupDrainTimeout overrides the per-connection drain timeout
// ([DefaultDrainTimeout]) used when draining registered connections.
func WithGroupDrainTimeout(d time.Duration) GroupOption {
	return func(g *ShutdownGroup) { g.drainTimeout = d }
}

// NewShutdownGroup returns a group whose context derives from parent and is
// cancelled by [ShutdownGroup.Shutdown].
func NewShutdownGroup(parent context.Context, opts ...GroupOption) *ShutdownGroup {
	ctx, cancel := context.WithCancel(parent)
	g := &ShutdownGroup{
		ctx:          ctx,
		cancel:       cancel,
		logger:       slog.Default(),
		joinTimeout:  DefaultJoinTimeout,
		drainTimeout: DefaultDrainTimeout,
	}
	for _, o := range opts {
		o(g)
	}
	if g.logger == nil {
		g.logger = slog.Default()
	}
	return g
}

// Context returns the group's context, cancelled when Shutdown is called. Pass
// it to every background loop and to [Connect] so the loops exit before their
// connections are drained.
func (g *ShutdownGroup) Context() context.Context { return g.ctx }

// Go runs fn as a tracked background loop. fn MUST return when its context is
// cancelled; Shutdown blocks (bounded by the join timeout) until it does.
func (g *ShutdownGroup) Go(fn func(ctx context.Context)) {
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		fn(g.ctx)
	}()
}

// AddConn registers a connection to be drained after the loops have joined.
// Draining last — never before the fetch loops stop — is the ordering that
// keeps a Drain/Close from racing an in-flight Fetch (COR-923).
func (g *ShutdownGroup) AddConn(name string, nc *nats.Conn) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.conns = append(g.conns, groupConn{name: name, nc: nc})
}

// Shutdown cancels the group context, waits (bounded by the join timeout) for
// every Go loop to return, then drains every registered connection. It is safe
// to call more than once; only the first call has an effect.
func (g *ShutdownGroup) Shutdown() {
	g.once.Do(g.shutdown)
}

func (g *ShutdownGroup) shutdown() {
	g.cancel()

	done := make(chan struct{})
	go func() {
		g.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(g.joinTimeout):
		g.logger.WarnContext(g.ctx, "entwine: background loops did not stop within join timeout; draining anyway",
			slog.Duration("join_timeout", g.joinTimeout))
	}

	g.mu.Lock()
	conns := g.conns
	g.mu.Unlock()
	for _, c := range conns {
		Drain(g.ctx, c.nc, c.name, g.drainTimeout)
	}
}
