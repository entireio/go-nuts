package nuts

import (
	"context"
	"log/slog"
	"runtime/debug"
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
//	g := nuts.NewShutdownGroup(ctx)
//	nc, _ := nuts.Connect(g.Context(), url, nuts.WithName("worker"))
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

	wg     sync.WaitGroup
	mu     sync.Mutex
	conns  []groupConn
	closed bool // set under mu once shutdown starts; gates Go registration
	once   sync.Once
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

// WithGroupDrainTimeout overrides the fallback subscription-drain budget
// ([DefaultDrainTimeout]) used when draining a registered connection that does
// not carry its own nats.DrainTimeout. A connection created with
// [WithDrainTimeout] is drained on its own budget regardless; the total
// time-to-CLOSED backstop adds publish-flush headroom on top (see [Drain]).
func WithGroupDrainTimeout(d time.Duration) GroupOption {
	return func(g *ShutdownGroup) { g.drainTimeout = d }
}

// NewShutdownGroup returns a group whose context derives from parent and is
// cancelled by [ShutdownGroup.Shutdown].
func NewShutdownGroup(parent context.Context, opts ...GroupOption) *ShutdownGroup {
	//nolint:gosec // cancel is retained in g.cancel and invoked by Shutdown (via shutdown), not leaked
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
//
// A panic in fn is recovered and logged with a stack (at ERROR) rather than
// crashing the whole process, so one bad message in one loop does not take down
// every other loop on the pod. A return while the group context is still live
// is treated as a dead subsystem and logged at ERROR too — a tracked loop is
// expected to run until Shutdown cancels it.
//
// Registering a loop after Shutdown has begun is a no-op: the loop would not be
// joined before the connections drain, so it is refused and logged rather than
// started. Go is safe to call concurrently with Shutdown.
func (g *ShutdownGroup) Go(fn func(ctx context.Context)) {
	// Add under the same lock that Shutdown takes before it calls wg.Wait, so a
	// concurrent Go/Shutdown can never race Add against Wait (which panics), and
	// a loop registered after shutdown starts is refused rather than left
	// unjoined.
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		g.logger.WarnContext(g.ctx, "nuts: ShutdownGroup.Go called after shutdown; loop not started")
		return
	}
	g.wg.Add(1)
	g.mu.Unlock()

	go func() {
		defer g.wg.Done()
		defer func() {
			if r := recover(); r != nil {
				g.logger.ErrorContext(g.ctx, "nuts: background loop panicked",
					slog.Any("panic", r), slog.String("stack", string(debug.Stack())))
				return
			}
			// A clean return before the context is cancelled means the loop
			// stopped supervising its subsystem while it was still supposed to
			// be running — surface it instead of letting the pod look healthy.
			if g.ctx.Err() == nil {
				g.logger.ErrorContext(g.ctx, "nuts: background loop returned before shutdown")
			}
		}()
		fn(g.ctx)
	}()
}

// AddConn registers a connection to be drained after the loops have joined.
// Draining last — never before the fetch loops stop — is the ordering that
// keeps a Drain/Close from racing an in-flight Fetch (COR-923).
//
// Like [ShutdownGroup.Go], registering after Shutdown has begun is a no-op:
// the drain set is snapshotted when shutdown starts, so a connection added
// afterward (including during the join window) would never be drained. It is
// refused and logged rather than silently dropped — the caller then owns
// tearing it down. Uses the same lock as shutdown's snapshot, so a connection
// is either in the snapshot or explicitly refused, never lost to a race.
func (g *ShutdownGroup) AddConn(name string, nc *nats.Conn) {
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		g.logger.WarnContext(g.ctx, "nuts: ShutdownGroup.AddConn called after shutdown; connection not registered for drain",
			slog.String("conn", name))
		return
	}
	g.conns = append(g.conns, groupConn{name: name, nc: nc})
	g.mu.Unlock()
}

// Shutdown cancels the group context, waits (bounded by the join timeout) for
// every Go loop to return, then drains every registered connection. It is safe
// to call more than once; only the first call has an effect.
func (g *ShutdownGroup) Shutdown() {
	g.once.Do(g.shutdown)
}

func (g *ShutdownGroup) shutdown() {
	// Mark closed and snapshot the connections under the lock that Go also
	// takes. After this point no new loop can register (see Go), so the wg.Wait
	// below cannot race a concurrent Add.
	g.mu.Lock()
	g.closed = true
	conns := g.conns
	g.mu.Unlock()

	g.cancel()

	done := make(chan struct{})
	go func() {
		g.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(g.joinTimeout):
		g.logger.WarnContext(g.ctx, "nuts: background loops did not stop within join timeout; draining anyway",
			slog.Duration("join_timeout", g.joinTimeout))
	}

	// Drain the connections concurrently: they are independent, so the group's
	// worst-case shutdown is one drain backstop, not the sum across connections.
	var dwg sync.WaitGroup
	for _, c := range conns {
		dwg.Add(1)
		go func() {
			defer dwg.Done()
			Drain(g.ctx, c.nc, c.name, g.logger, g.drainBackstop(c.nc))
		}()
	}
	dwg.Wait()
}

// drainBackstop returns the total time-to-CLOSED budget for draining nc. It
// honors the connection's own configured nats.DrainTimeout (set via
// [WithDrainTimeout]) when present, falling back to the group's drain timeout,
// then adds headroom for the publish flush nats.go runs after the
// subscription-drain phase so the backstop does not fire mid-drain (see
// [Drain]).
func (g *ShutdownGroup) drainBackstop(nc *nats.Conn) time.Duration {
	subDrain := g.drainTimeout
	if nc != nil && nc.Opts.DrainTimeout > 0 {
		subDrain = nc.Opts.DrainTimeout
	}
	return subDrain + natsPublishDrainTimeout + time.Second
}
