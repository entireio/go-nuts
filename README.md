# entwine

Shared NATS connection-lifecycle helpers for Entire services.

`entwine` is the one place the org's NATS plumbing lives, so services stop
re-deriving (and drifting on) connection setup, graceful shutdown, and
pull-consumer teardown. It is deliberately small and grows only as real
consumers converge onto it. Tracking issue: **COR-925**.

## Status — thin first slice

This first slice ships the connection-lifecycle core, extracted from the
copies that had grown in entire-core (`entiredb`), `entire-api`, and
`mirror-pipeline`:

- **`Connect`** — rotation-aware mTLS dial with the org-standard resiliency
  posture: reconnect-forever (long-lived services must outlive NATS outages
  rather than permanently CLOSE after 60 attempts), a bounded drain timeout,
  and lifecycle log handlers that route the nil-error disconnect on an explicit
  `Close` to INFO so a clean shutdown doesn't look like a fault.
- **`Drain` / `IsShutdownFetchErr`** — graceful shutdown: drain-and-wait
  (nats.go's `Drain` is async), and a classifier so a fetch loop treats a
  drain/close during shutdown as a clean exit instead of an ERROR (COR-923).
- **`ShutdownGroup`** — cancels background loops, joins them, and only *then*
  drains the connections — the ordering that keeps a `Drain` from racing an
  in-flight `Fetch`.

Planned as consumers migrate (not here yet): `jsconsumer` (durable JetStream
pull-consumer scaffold), `natsmsg` (KeepInProgress heartbeat + trace-context
propagation), `backoff` (Term-on-final-delivery + NakWithDelay policy).

## Usage

```go
ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
defer stop()

g := entwine.NewShutdownGroup(ctx)
nc, err := entwine.Connect(g.Context(), natsURL, entwine.WithName("worker"))
if err != nil {
	return err
}
g.AddConn("worker", nc)

g.Go(func(ctx context.Context) {
	for {
		msgs, err := sub.Fetch(1, nats.MaxWait(5*time.Second))
		if err != nil {
			if errors.Is(err, nats.ErrTimeout) {
				continue // idle poll
			}
			if entwine.IsShutdownFetchErr(ctx, err) {
				return // clean shutdown: connection drained/closed
			}
			continue // real error — log/metric
		}
		handle(msgs)
	}
})

<-ctx.Done()
g.Shutdown() // cancel the loop → join it → drain nc, in that order
```

## Development

`entwine` is developed against its consumers with a **local `go.work`**
(git-ignored) so the module and, e.g., `mirror-pipeline` co-evolve without a
tag-and-bump cycle. A real version is cut only once the API settles.

```
go 1.26

use (
	./entwine
	./mirror-pipeline
)
```

Tasks (via [mise](https://mise.jdx.dev)): `mise run test`, `mise run lint`,
`mise run fmt`.

## License

MIT — see [LICENSE](./LICENSE).
