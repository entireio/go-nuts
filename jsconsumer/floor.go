package jsconsumer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const (
	// DefaultFloorAge is how long a durable's ack floor may stay stalled
	// before [FloorMonitor] reports it, when FloorMonitorConfig.FloorAge is
	// zero. Sized in ENT-1535's Session-1 forensics: above ~10m so an ordinary
	// transient that settles by attempt 3 never reaches it, below ~25m so the
	// whole stall — including the monitor's own evaluation lag — fits the
	// 30-minute SLA.
	DefaultFloorAge = 15 * time.Minute

	// Bounds on the derived poll interval when FloorMonitorConfig.Poll is
	// zero: FloorAge/10, clamped. The poll is one consumer-info request, so
	// the ceiling matters more than the floor.
	minFloorPoll = time.Second
	maxFloorPoll = 30 * time.Second
	// floorPollTimeout bounds one consumer-info request so a NATS blip cannot
	// wedge the poll loop — and, through it, [Runner.Stop], which joins it.
	floorPollTimeout = 5 * time.Second
)

// FloorMonitorConfig describes a [FloorMonitor].
type FloorMonitorConfig struct {
	// FloorAge is how long the floor must be stalled before [FloorMonitor.Stalled]
	// reports it and the stall is logged. Zero uses DefaultFloorAge.
	FloorAge time.Duration
	// Poll is how often the durable's consumer info is read. Zero derives
	// FloorAge/10, clamped to [1s, 30s]. Must be shorter than FloorAge — a
	// stall that is never sampled cannot be resolved.
	Poll time.Duration
	// Name prefixes the monitor's log lines and Logger receives them. Both are
	// optional: [Start] fills them from Config.Name and Config.Logger when
	// unset.
	Name   string
	Logger *slog.Logger
}

// FloorMonitor is an advisory health signal for one durable consumer: it polls
// the durable's ack floor and reports how long that floor has been STALLED.
//
// It is deliberately separate from disposition. Ack-floor state is
// consumer-global and read here from a client-side snapshot, while the
// decision to give up on a message is message-local — so this type observes
// and reports, and something else decides what that is worth. [Retry] can
// consume it to drive its circuit breaker; an adopter that would rather keep
// that judgement can use the monitor alone, publish [FloorMonitor.FloorStall]
// as a gauge, and call [Retry.DeadLetter] on its own terms.
//
// [Start] attaches the live consumer and runs the poll; [Runner.Stop] joins
// it. A monitor belongs to exactly one durable consumer.
//
// # Stalled is not stationary
//
// The distinction is the whole correctness of anything built on this. A
// caught-up consumer's floor sits still because nothing arrived, and a floor
// that has been still for four idle hours says nothing about the next message
// to arrive: crediting that message with the idle time — and quarantining it
// on its second delivery — would be a false positive of exactly the kind this
// signal exists to avoid. So the clock runs only while the consumer has
// delivered past its own floor, the same floor-versus-last-delivered
// comparison the incident runbook makes by hand, and it restarts the moment
// the consumer catches up.
//
// The age is measured from the first observation of a stall, so a freshly
// started process under-reports an already-old one rather than over-reporting
// it: this signal can be late, never early.
type FloorMonitor struct {
	cfg FloorMonitorConfig

	mu     sync.Mutex
	cons   jetstream.Consumer
	name   string
	logger *slog.Logger
	// clock reads the current time; a package-internal seam so tests can
	// drive the stall clock without sleeping.
	clock func() time.Time

	stalled     bool
	floor       uint64
	floorSince  time.Time
	stallLogged bool

	// polls counts live pollFloor invocations, and maxPolls the high-water
	// mark. A monitor belongs to one consumer, so two concurrent polls means a
	// caller reattached it while an earlier poll was still running — an
	// in-flight consumer-info reply from the OLD consumer can then land after
	// the reattach and overwrite fresh state with stale. It is a lifecycle
	// bug rather than a data race, so nothing else would catch it; [Run]'s
	// recreate path is the one that has to get this right.
	polls, maxPolls int
}

// NewFloorMonitor validates cfg, applies its defaults, and returns the
// monitor.
func NewFloorMonitor(cfg FloorMonitorConfig) (*FloorMonitor, error) {
	if cfg.FloorAge < 0 {
		return nil, fmt.Errorf("jsconsumer: FloorMonitorConfig.FloorAge must not be negative, got %s (omit the monitor entirely to disable it)", cfg.FloorAge)
	}
	if cfg.FloorAge == 0 {
		cfg.FloorAge = DefaultFloorAge
	}
	if cfg.Poll < 0 {
		return nil, fmt.Errorf("jsconsumer: FloorMonitorConfig.Poll must not be negative, got %s", cfg.Poll)
	}
	m := &FloorMonitor{cfg: cfg, clock: time.Now}
	if p := m.pollInterval(); p >= cfg.FloorAge {
		return nil, fmt.Errorf("jsconsumer: FloorMonitorConfig.Poll (%s) must be shorter than FloorAge (%s): a stall that is never sampled cannot be resolved", p, cfg.FloorAge)
	}
	return m, nil
}

// FloorAge reports the configured stall threshold.
func (m *FloorMonitor) FloorAge() time.Duration { return m.cfg.FloorAge }

// pollInterval is the effective poll cadence: Poll, or FloorAge/10 clamped to
// [minFloorPoll, maxFloorPoll].
func (m *FloorMonitor) pollInterval() time.Duration {
	if m.cfg.Poll > 0 {
		return m.cfg.Poll
	}
	return max(minFloorPoll, min(m.cfg.FloorAge/10, maxFloorPoll))
}

// FloorStall reports the durable's ack floor (a stream sequence), how long it
// has been stalled there, and whether it is stalled at all. Publish it as a
// gauge — it is the signal the ack-floor monitor pages on, available from
// inside the process rather than by polling consumer info a second time.
//
// See the type doc for what "stalled" means, and why it is not the same as
// the floor merely being motionless.
func (m *FloorMonitor) FloorStall() (floor uint64, age time.Duration, stalled bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.stalled {
		return m.floor, 0, false
	}
	return m.floor, m.clock().Sub(m.floorSince), true
}

// Stalled reports whether the floor has been stalled past FloorAge — the
// condition worth acting on, as opposed to any stall at all.
func (m *FloorMonitor) Stalled() bool {
	_, age, stalled := m.FloorStall()
	return stalled && age >= m.cfg.FloorAge
}

// observeFloor records one reading of the durable's ack floor against its
// last-delivered sequence, starting the stall clock when work is outstanding
// below the delivered mark and clearing it when the consumer catches up.
//
// delivered <= floor means every delivery this consumer has made is settled:
// there is no blocked message to age, whatever the floor has been doing. Note
// this stays true while a blocker waits out a ladder rung — it has been
// delivered, so Delivered stays above the floor even though nothing is
// ack-pending at that instant, which is why NumAckPending is the wrong gate
// here.
func (m *FloorMonitor) observeFloor(floor, delivered uint64, at time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	caughtUp := delivered <= floor
	sameStall := m.stalled && m.floor == floor
	m.floor = floor
	switch {
	case caughtUp:
		m.stalled = false
	case sameStall:
		return // still the same stall; the clock runs on
	default:
		m.stalled = true
		m.floorSince = at
	}
	m.stallLogged = false
}

// attach binds the live consumer (and the host's log identity, when the config
// left it unset). Called by [Start] on every attempt, including [Run]'s
// recreates.
func (m *FloorMonitor) attach(cons jetstream.Consumer, name string, logger *slog.Logger) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cons = cons
	m.name = firstNonEmpty(m.cfg.Name, name)
	if m.cfg.Logger != nil {
		m.logger = m.cfg.Logger
	} else {
		m.logger = logger
	}
}

func (m *FloorMonitor) log() (*slog.Logger, string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	logger, name := m.logger, m.name
	if logger == nil {
		if m.cfg.Logger != nil {
			logger = m.cfg.Logger
		} else {
			logger = slog.Default()
		}
	}
	if name == "" {
		name = firstNonEmpty(m.cfg.Name, "jsconsumer")
	}
	return logger, name
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// pollFloor reads the ack floor until ctx is cancelled or stop is closed (the
// consume loop winding down). It samples once immediately so a long-lived
// stall is observed at Start rather than one interval later.
func (m *FloorMonitor) pollFloor(ctx context.Context, stop <-chan struct{}) {
	m.mu.Lock()
	m.polls++
	m.maxPolls = max(m.maxPolls, m.polls)
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		m.polls--
		m.mu.Unlock()
	}()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	// Cancel on the consume loop closing too, so an in-flight consumer-info
	// request cannot hold Runner.Stop open for floorPollTimeout. The watcher
	// exits with ctx via the deferred cancel.
	go func() {
		select {
		case <-stop:
			cancel()
		case <-ctx.Done():
		}
	}()

	m.pollOnce(ctx)
	t := time.NewTicker(m.pollInterval())
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			m.pollOnce(ctx)
		}
	}
}

func (m *FloorMonitor) pollOnce(ctx context.Context) {
	m.mu.Lock()
	cons := m.cons
	m.mu.Unlock()
	if cons == nil {
		return
	}
	infoCtx, cancel := context.WithTimeout(ctx, floorPollTimeout)
	defer cancel()
	info, err := cons.Info(infoCtx)
	if err != nil {
		if ctx.Err() != nil {
			return // shutting down; a failed read here is the wind-down, not a fault
		}
		logger, name := m.log()
		logger.WarnContext(ctx, name+": ack floor poll failed", slog.Any("error", err))
		return
	}
	m.observeFloor(info.AckFloor.Stream, info.Delivered.Stream, m.clock())

	// Report a stall past the threshold once per stall. This is the signal
	// ENT-1535 had none of in-process, and it stays visible whether or not
	// anything acts on it.
	floor, age, stalled := m.FloorStall()
	if !stalled || age < m.cfg.FloorAge {
		return
	}
	m.mu.Lock()
	first := !m.stallLogged
	m.stallLogged = true
	m.mu.Unlock()
	if !first {
		return
	}
	logger, name := m.log()
	logger.WarnContext(ctx, name+": ack floor stalled past FloorAge",
		slog.Uint64("ack_floor", floor),
		slog.Duration("stalled_for", age),
		slog.Uint64("last_delivered", info.Delivered.Stream),
		slog.Int("num_ack_pending", info.NumAckPending))
}

// errMonitorConflict is returned when a Config names one floor monitor and its
// Retry another: there is one durable and one poll loop, so there can only be
// one monitor.
var errMonitorConflict = errors.New("Config.FloorMonitor and Retry's monitor are different: one durable has one ack floor, so both must be the same *FloorMonitor (or leave Config.FloorMonitor nil and let Start use the Retry's)")
