package nuts

import (
	"context"
	"errors"
	"fmt"
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
//	<-g.Context().Done() // SIGTERM/SIGINT, panic, or premature loop return
//	if err := g.Shutdown(); err != nil { return err }
type ShutdownGroup struct {
	ctx          context.Context
	cancel       context.CancelCauseFunc
	logger       *slog.Logger
	joinTimeout  time.Duration
	drainTimeout time.Duration

	wg     sync.WaitGroup
	mu     sync.Mutex
	conns  []groupConn
	closed bool  // set under mu once shutdown starts; gates Go registration
	err    error // first unexpected loop or shutdown failure
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
	ctx, cancel := context.WithCancelCause(parent)
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

// Context returns the group's context, cancelled when Shutdown is called or a
// tracked loop fails. Pass it to every background loop and to [Connect] so the
// loops exit before their connections are drained.
func (g *ShutdownGroup) Context() context.Context { return g.ctx }

// Err returns the first unexpected loop or shutdown failure, or nil while the
// group is healthy and after a clean shutdown. It is safe to call from a
// readiness check while the group is running.
func (g *ShutdownGroup) Err() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.err
}

// Go runs fn as a tracked background loop. fn MUST return when its context is
// cancelled; Shutdown blocks (bounded by the join timeout) until it does.
//
// A panic in fn is recovered so Shutdown can still join the other loops and
// drain connections, but it remains fatal to the group: the panic is recorded,
// logged with a stack, and cancels the group context. A return while the group
// context is still live is handled the same way. The owning process can observe
// the failure through [ShutdownGroup.Err] or [ShutdownGroup.Shutdown].
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
				err := fmt.Errorf("nuts: background loop panicked: %v", r)
				g.logger.ErrorContext(g.ctx, "nuts: background loop panicked",
					slog.Any("panic", r), slog.String("stack", string(debug.Stack())))
				g.recordError(err, true)
				return
			}
			if g.recordPrematureReturn() {
				g.logger.ErrorContext(g.ctx, "nuts: background loop returned before shutdown")
			}
		}()
		fn(g.ctx)
	}()
}

// recordPrematureReturn atomically distinguishes a real dead subsystem from a
// loop returning concurrently with Shutdown. Shutdown marks closed under the
// same lock before cancelling, so a normal teardown cannot be misclassified.
func (g *ShutdownGroup) recordPrematureReturn() bool {
	err := errors.New("nuts: background loop returned before shutdown")
	g.mu.Lock()
	if g.closed || g.ctx.Err() != nil {
		g.mu.Unlock()
		return false
	}
	if g.err != nil {
		g.mu.Unlock()
		return false
	}
	g.err = err
	g.mu.Unlock()
	g.cancel(err)
	return true
}

// recordError stores the first group failure. cancelGroup is true for a live
// loop failure, which must stop siblings and the owner; shutdown-time failures
// are returned by Shutdown without re-cancelling an already cancelled group.
func (g *ShutdownGroup) recordError(err error, cancelGroup bool) {
	if err == nil {
		return
	}
	g.mu.Lock()
	recorded := false
	if g.err == nil {
		g.err = err
		recorded = true
	}
	g.mu.Unlock()
	if cancelGroup && recorded {
		g.cancel(err)
	}
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
// every Go loop to return, then drains every registered connection. It returns
// the first unexpected loop, join, or drain failure. It is safe to call more
// than once; later calls return the same result.
func (g *ShutdownGroup) Shutdown() error {
	g.once.Do(g.shutdown)
	return g.Err()
}

func (g *ShutdownGroup) shutdown() {
	// Mark closed and snapshot the connections under the lock that Go also
	// takes. After this point no new loop can register (see Go), so the wg.Wait
	// below cannot race a concurrent Add.
	g.mu.Lock()
	g.closed = true
	conns := g.conns
	g.mu.Unlock()

	g.cancel(nil)

	done := make(chan struct{})
	go func() {
		g.wg.Wait()
		close(done)
	}()
	if joinTimedOut(done, g.joinTimeout) {
		g.logger.WarnContext(g.ctx, "nuts: background loops did not stop within join timeout; draining anyway",
			slog.Duration("join_timeout", g.joinTimeout))
		g.recordError(fmt.Errorf("nuts: background loop join timeout after %s", g.joinTimeout), false)
	}

	// Drain the connections concurrently: they are independent, so the group's
	// worst-case shutdown is one drain backstop, not the sum across connections.
	var dwg sync.WaitGroup
	for _, c := range conns {
		dwg.Add(1)
		go func() {
			defer dwg.Done()
			if err := Drain(g.ctx, c.nc, c.name, g.logger, g.drainBackstop(c.nc)); err != nil {
				g.recordError(err, false)
			}
		}()
	}
	dwg.Wait()
}

// joinTimedOut waits for the tracked loops and reports whether the timeout won.
// If completion and the timer become ready together, prefer completion: the
// group did join within the observable boundary and must not fail shutdown due
// to select choosing the timer pseudo-randomly.
func joinTimedOut(done <-chan struct{}, timeout time.Duration) bool {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-done:
		return false
	case <-timer.C:
		select {
		case <-done:
			return false
		default:
			return true
		}
	}
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
