// Package nuts provides shared NATS connection-lifecycle helpers for Entire
// services: a rotation-aware mTLS [Connect] with the org-standard resiliency
// options, a [Drain] that waits for a connection to flush and close on graceful
// shutdown, an [IsShutdownFetchErr] classifier so pull-consumer loops can treat
// a drain/close during shutdown as a clean exit rather than an error, and a
// [ShutdownGroup] that cancels background loops, joins them, and only then
// drains the connections — the ordering that keeps a Drain from racing an
// in-flight Fetch (COR-923).
//
// # Shutdown ordering
//
// A process built on this module shuts down in one fixed order, and each
// piece owns one step of it:
//
//  1. the signal context is cancelled (SIGTERM, or a tracked loop failing);
//  2. every jsconsumer loop stops pulling new deliveries;
//  3. in-flight handlers keep running on a context that survives the cancel,
//     for up to jsconsumer.Config.DrainWindow (default 10s);
//  4. handlers still running when the window elapses have their context
//     cancelled, and the expiry is logged;
//  5. the loops return once their handlers have, and [ShutdownGroup] joins
//     them (bounded by [DefaultJoinTimeout], which must exceed the window);
//  6. only then are the connections drained with [Drain], so no Ack, Nak or
//     dead-letter capture is written to a draining connection.
//
// Steps 2–4 are COR-2224: before it, step 1 cancelled every handler at once,
// so "waiting for the in-flight handler" waited for a context.Canceled failure
// and a final delivery landing on a rollout was Termed or dead-lettered as
// poison. The dispositions agree with the drain: backoff.Policy.NakOrTerm and
// jsconsumer.Retry.Settle treat a CANCELLED context (backoff.IsInterrupted —
// a deadline is a failed attempt, not an interruption) as a shutdown rather
// than a verdict on the message, and leave it for another replica wherever
// the broker still has deliveries to serve.
//
// This root package is the connection-lifecycle core extracted from
// entire-core (entiredb), entire-api, and mirror-pipeline, which had each
// grown their own copy of this plumbing (COR-925). It depends only on nats.go.
//
// The subpackages carry the message-layer pieces those services shared
// (COR-929, deepened in COR-984) — [natsmsg] (trace-context propagation over
// message headers, the bounded deduped Publisher with a caller-selected
// Operation span name, the KeepInProgress AckWait heartbeat, the DeadLetter
// dead-letter capture, and the natsmsgtest.FakeMsg test double),
// [jsconsumer] (the durable JetStream pull-consumer scaffold, one-shot via Start
// or supervised via Run, plus Retry — the consumer's single retry mechanism —
// Schedule, the pure validator for a consumer's retry timing that fleet CI and
// the library share, and FloorMonitor, the ack-floor stall telemetry that can
// also arm Retry's experimental circuit breaker, ENT-1535), and [backoff] (the
// NakWithDelay redelivery policy —
// flat or growing — with Term-on-final-delivery, COR-762). The legacy
// nats.JetStreamContext publishing bridges were transitional and were removed
// once the last caller migrated (COR-1004). natsmsg and
// jsconsumer additionally depend on the OpenTelemetry API, and all three on
// nats.go's jetstream package; importing only the root package links none of
// that.
//
// [natsmsg]: https://pkg.go.dev/github.com/entireio/go-nuts/natsmsg
// [jsconsumer]: https://pkg.go.dev/github.com/entireio/go-nuts/jsconsumer
// [backoff]: https://pkg.go.dev/github.com/entireio/go-nuts/backoff
package nuts
