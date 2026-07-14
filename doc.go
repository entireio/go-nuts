// Package nuts provides shared NATS connection-lifecycle helpers for Entire
// services: a rotation-aware mTLS [Connect] with the org-standard resiliency
// options, a [Drain] that waits for a connection to flush and close on graceful
// shutdown, an [IsShutdownFetchErr] classifier so pull-consumer loops can treat
// a drain/close during shutdown as a clean exit rather than an error, and a
// [ShutdownGroup] that cancels background loops, joins them, and only then
// drains the connections — the ordering that keeps a Drain from racing an
// in-flight Fetch (COR-923).
//
// This root package is the connection-lifecycle core extracted from
// entire-core (entiredb), entire-api, and mirror-pipeline, which had each
// grown their own copy of this plumbing (COR-925). It depends only on nats.go.
//
// The subpackages carry the message-layer pieces those services shared
// (COR-929) — [natsmsg] (trace-context propagation over message headers, the
// bounded deduped Publisher, the KeepInProgress AckWait heartbeat, the
// DeadLetter dead-letter capture, and the natsmsgtest.FakeMsg test double),
// [jsconsumer] (the durable JetStream
// pull-consumer scaffold, one-shot via Start or supervised via Run), and
// [backoff] (the NakWithDelay redelivery policy — flat or growing — with
// Term-on-final-delivery, COR-762). natsmsg and jsconsumer additionally
// depend on the OpenTelemetry API, and all three on nats.go's jetstream
// package; importing only the root package links none of that.
//
// [natsmsg]: https://pkg.go.dev/github.com/entireio/go-nuts/natsmsg
// [jsconsumer]: https://pkg.go.dev/github.com/entireio/go-nuts/jsconsumer
// [backoff]: https://pkg.go.dev/github.com/entireio/go-nuts/backoff
package nuts
