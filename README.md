<p align="center">
  <img src="assets/go-nuts.png" alt="go-nuts" width="600">
</p>

# go-nuts

A small, dependency-light toolkit over
[`nats.go`](https://github.com/nats-io/nats.go) for building
[NATS](https://nats.io) and JetStream services in Go. The root package handles
connection lifecycle — connecting, draining, and shutting down cleanly; opt-in
subpackages add a durable JetStream pull-consumer scaffold, a publish helper,
trace-context propagation, and a redelivery policy.

## Install

```
go get github.com/entireio/go-nuts
```

## Connection lifecycle (root package)

- **`Connect`** — dial with a resilient default posture: reconnect-forever (so a
  long-lived service rides out a NATS outage instead of permanently closing
  after the client's default 60 attempts), a bounded drain timeout, optional
  rotation-aware mTLS, and lifecycle log handlers that don't mistake a clean
  shutdown for a fault. Async errors (slow-consumer drops, permissions
  violations) and lame-duck notices are logged through the configured logger
  instead of nats.go's stderr default.
- **`Drain` / `IsShutdownFetchErr`** — graceful shutdown. `Drain` starts
  nats.go's asynchronous drain and blocks until the connection flushes and
  closes (bounded by a timeout), returning an error and force-closing when it
  cannot complete; `IsShutdownFetchErr` lets a fetch loop treat a drain/close
  during shutdown as a clean exit rather than an error.
- **`ShutdownGroup`** — cancels tracked background loops, waits for them to
  return, then drains the registered connections — the ordering that keeps a
  `Drain` from racing an in-flight `Fetch`. A panic or unexpected return in a
  tracked loop cancels the group and is returned by `Shutdown`, so a dead
  consumer cannot leave its process looking healthy.

## Usage

The example below drives a hand-rolled fetch loop on nats.go's legacy pull
API; consumers on the modern `jetstream` API get the whole loop from
[`jsconsumer`](#subpackages) instead.

```go
ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
defer stop()

g := nuts.NewShutdownGroup(ctx)
nc, err := nuts.Connect(g.Context(), natsURL, nuts.WithName("worker"))
if err != nil {
	return err
}
g.AddConn("worker", nc)

g.Go(func(ctx context.Context) {
	for {
		// Stop fetching once the group is shutting down, so the loop returns
		// before its connection is drained (Shutdown cancels, joins, then
		// drains — the loop must observe the cancel to keep that ordering).
		if ctx.Err() != nil {
			return
		}
		msgs, err := sub.Fetch(1, nats.MaxWait(5*time.Second))
		switch {
		case errors.Is(err, nats.ErrTimeout):
			continue // idle poll
		case nuts.IsShutdownFetchErr(ctx, err):
			return // clean shutdown: connection drained/closed
		case err != nil:
			// Real error — log/metric, then back off so a fast-failing Fetch
			// (e.g. connection closed mid-run) does not busy-spin the CPU.
			slog.ErrorContext(ctx, "fetch failed", slog.Any("error", err))
			select {
			case <-ctx.Done():
			case <-time.After(time.Second):
			}
			continue
		}
		handle(msgs)
	}
})

<-g.Context().Done() // process signal, or a tracked loop failed
if err := g.Shutdown(); err != nil { // cancel loops → join → drain, in that order
	return err
}
```

By default `Connect` builds mTLS from the `ENTIRE_INTERNAL_TLS_{CERT,KEY,CA}_FILE`
environment variables; use `WithTLSConfig`, `WithoutTLS`, or the other `Option`s
to override.

## Subpackages

The root package is deliberately nats.go-only. The consumer-side layers live in
subpackages so importing the root links none of their dependencies (`natsmsg`
and `jsconsumer` add the OpenTelemetry API; all three use `nats.go/jetstream`):

- **`natsmsg`** — small JetStream message helpers: W3C trace-context
  propagation over message headers (`Inject` / `ExtractHeader` /
  `StartConsumerSpan`, so publish → consume stitches into one trace),
  `Publisher` — the publish core (producer span with a caller-selected
  `Operation` name + standard `messaging.*` attributes + trace inject +
  `Nats-Msg-Id` dedup + bounded pub-ack wait + PubAck telemetry, with
  `StartProducerSpan` for callers composing by hand); it owns just that
  prologue — subject construction, payload encoding, domain metrics/logging, and
  the response to a failed publish stay with the caller (its `JS` field is the
  narrow publish slice of `jetstream.JetStream`, so tests stub one method). Also
  `KeepInProgress`, an AckWait heartbeat for long handlers, capped so a wedged
  handler still redelivers, and `DeadLetter` / `SubjectToken`, the dead-letter
  capture that copies a poison message to a DLQ subject with `Nats-Dlq-*`
  provenance before a consumer gives up on it (the capture step `backoff`'s
  Term-on-exhaustion below expects). `natsmsg/natsmsgtest` ships `FakeMsg`, a
  scriptable message for asserting a consumer's ack/nak/term disposition
  without a broker.
- **`jsconsumer`** — the durable JetStream pull-consumer scaffold:
  `Start` (create-or-update durable → consume → stop on context cancel),
  `Run` (`Start` under supervision: recreate on a closed loop with exponential
  backoff, tolerate a not-yet-provisioned stream at boot), and
  `Process` (consumer span re-parented across the NATS hop → decode →
  Term-on-undecodable → dispatch to the handler, which owns the message's
  disposition). AckExplicit, bounded AckWait and MaxDeliver, optional
  InactiveThreshold / MaxAckPending, shutdown-aware consume-error logging,
  optional `KeepInProgress` heartbeat. Also `Retry` — see below.
- **`jsconsumer.Schedule`** — a consumer's retry timing as plain values, and
  `Validate` / `Err`: the single implementation of the arithmetic that says
  whether it hangs together. Pure — no NATS connection, no I/O, no clock — so
  the same function runs at `Start`, in a fleet CI lint over rendered NACK
  Consumer CRs at merge time, and (when bind-only mode lands) at startup
  against the durable's real server-side config. Compile it in rather
  than restating the arithmetic; duplicated timing maths is exactly how
  ENT-1535's consumer came to advertise 17h45m while really taking 34h22m.
  Checks the cumulative ladder against `MaxTimeToDeadLetter` and the stream's
  `maxAge`, `ackWait` against `backOff[0]`, rung count against `maxDeliver`,
  the recovery envelope against the breaker threshold, and both ladders being
  set at once. Zero fields are "unknown" and skip their checks, so a caller
  that knows only part of a config still gets everything that part supports.
  `Validate` returns every violation for a merge-time report; `Err` folds them
  into one error.
- **`jsconsumer.Retry`** — where a consumer's retries *end*: dead-letter
  capture, then settle. It does **not** own the redelivery schedule. The server
  does — through the durable's `BackOff` ladder, set via `Config.BackOff` — and
  on the retry path `Retry` disposes of **nothing**, letting AckWait expire so
  the ladder redelivers. That detail is load-bearing: `BackOff` governs
  acknowledgement *timeouts*, so a plain `Nak` asks for immediate redelivery
  and skips the ladder entirely (measured against a live server: 0s versus the
  configured rung). A consumer that Naks burns `MaxDeliver` in milliseconds.

  That split is the ENT-1535 finding, not a detail. A JetStream consumer has
  two possible redelivery schedulers, and setting both does not pick one: the
  server stretches each client `NakWithDelay` by the BackOff increments, so
  redeliveries follow neither and the configured ladder becomes dead config
  that still reads as authoritative — a consumer advertising 17h45m while
  really taking 34h22m, with nothing saying so. Exactly one scheduler is what
  prevents that, and the one that composes with declarative fleet management is
  the server's. `RetryConfig` has no ladder fields at all, so the combination
  is unrepresentable rather than merely rejected.

  Bound that ladder with `MaxTimeToDeadLetter` and it is the whole remedy: a
  poison message reaches capture-then-Ack inside the SLA with no inference
  about which message is at fault. This is what a consumer should adopt today.
  Two things about that ladder that are easy to get wrong, and that `Schedule`
  models so the bound is checked against what actually runs: the server repeats
  the last `backOff` entry once the array runs out, so a short list is not a
  short ladder; and an *absent* `backOff` is not an absent ladder — the broker
  redelivers on `AckWait`, so the effective schedule becomes AckWait repeated
  up to `MaxDeliver`.

  Never a drop, and never a bare `Term`. A failed capture leaves the message
  for the server to redeliver, so it survives and the stall stays visible — and `CaptureReserve` holds
  back deliveries specifically to retry it. When even those are spent the
  message is reported as `OutcomeStranded`, not dressed up as a retry: at the
  delivery cap nothing redelivers, so nothing will touch it again and it needs
  the break-glass runbook. Alert on that outcome.

  **Explicit non-goal:** a handler that *dies* on the poison message — panics,
  OOMs — rather than returning an error. `Settle` is the only entry point to
  every disposition the package owns, so such a message is not settled by any
  of them; that follows from the callback contract, not from the breaker.
  Recovering the panic to Ack the message would hide the bug and leave the
  handler's state unreconciled, against the module's existing posture that a
  panicking loop is fatal (`ShutdownGroup`). The stall stays loud — the
  monitor polls independently of message flow and the ack-floor monitor still
  pages — it just isn't auto-remediated.
- **`jsconsumer.FloorMonitor`** — the durable's ack-floor health signal, and
  the telemetry half of the poison-message story: it polls the floor and
  reports how long it has been *stalled*, which is the signal the ack-floor
  monitor pages on, available in-process rather than by polling consumer info
  a second time. `Start` polls it and `Stop` joins the poll. Use it for a
  gauge and an alert; that is its supported role.

  It measures **floor-stall age**, not message stream-age: the two diverge
  badly under backlog, where 35 minutes of receipt→first-delivery lag on a
  perfectly healthy message would read as a stall. Stalled is also not the
  same as *stationary* — the clock runs only while the consumer has delivered
  past its own floor, so an idle consumer's motionless floor never accrues
  time to charge the next arriving message with.

  **The circuit breaker built on it is experimental.** Attaching the monitor to
  `RetryConfig.Monitor` lets `Retry` evaluate whether a stalled floor plus a
  message that has itself been failing that long warrants quarantining it —
  but `Breaker`'s zero value is `BreakerObserve`, which measures without
  acting, and that is the only supported mode. `BreakerEnforce` exists, keeps
  its tests, and is documented as not-for-production pending a multi-week
  observe soak of real trip counts; **deleting it is an acceptable outcome** if
  bounded ladders prove sufficient. Every serious defect found in this package
  has been in that path, all from the same root — acting on a client-side
  inference about which message holds a consumer-global floor — while the
  bounded ladder covers the incident with no inference at all. Leave
  `Monitor` nil and none of the breaker's machinery exists: no failure clocks,
  no quarantine budget, no consumer-global judgement.

## Development

Requires Go 1.26. Tasks via [mise](https://mise.jdx.dev): `mise run test`,
`mise run lint`, `mise run fmt`.

### Testing: the fake vs the real broker

Tests here split by what they can prove, because the two halves fail in opposite
directions:

- **`natsmsgtest.FakeMsg`** — library logic only. Given this input, which
  disposition did the code choose, with which delay, after which branch. A fake
  cannot check what the broker does in response: it encodes the same model of
  JetStream as the code, so a belief held wrongly in both places passes.
- **`internal/brokersemantics`** — every JetStream semantic this module's code and
  docs rely on, measured against an embedded in-process `nats-server` at the
  version pinned in `go.mod`: settlement and ack-floor movement, delivery
  counting, `BackOff` ladder arithmetic, `NakWithDelay` under a ladder, config
  normalization and rejection (including pedantic mode), and the library's own
  end-to-end claims (`TermOnExhaustion`, `KeepInProgress`, `DeadLetter`, durable
  resume).

If a doc comment in this module states a JetStream behaviour, a test in
`internal/brokersemantics` measures it. That suite carries **no build tag** — it
runs in the default `go test ./...`, so it gates every merge; a tag CI forgets to
pass is a gate that silently does not run.

**On a `nats-server` bump, re-run it and read the failures as findings**, not as
tests to fix: a red assertion there means the belief in the doc comment it cites
needs re-deciding at the new version. `TestPinnedServerVersion` records which
version the measurements came from.

## License

MIT — see [LICENSE](./LICENSE).
