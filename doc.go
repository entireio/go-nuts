// Package entwine provides shared NATS connection-lifecycle helpers for Entire
// services: a rotation-aware mTLS [Connect] with the org-standard resiliency
// options, a [Drain] that waits for a connection to flush and close on graceful
// shutdown, an [IsShutdownFetchErr] classifier so pull-consumer loops can treat
// a drain/close during shutdown as a clean exit rather than an error, and a
// [ShutdownGroup] that cancels background loops, joins them, and only then
// drains the connections — the ordering that keeps a Drain from racing an
// in-flight Fetch (COR-923).
//
// This is the thin first slice extracted from entire-core (entiredb),
// entire-api, and mirror-pipeline, which had each grown their own copy of this
// plumbing. Consumer scaffolding (jsconsumer), message helpers (natsmsg), and
// the redelivery/DLQ policy (backoff) will land here as those services converge
// onto the module. Tracking: COR-925.
package entwine
