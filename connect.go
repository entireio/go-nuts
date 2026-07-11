package nuts

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/nats-io/nats.go"
)

// Default connection tunables. DefaultDrainTimeout matches entire-core's tuned
// value (COR-923); nats.go's own default is 30s. It caps only the
// subscription-drain phase — see [Drain] for why it is not the whole budget.
const (
	DefaultDrainTimeout  = 5 * time.Second
	DefaultReconnectWait = 2 * time.Second
)

// natsPublishDrainTimeout is the fixed publish-flush budget nats.go spends
// after the subscription-drain phase before a drained connection reaches CLOSED
// (the hardcoded FlushTimeout in (*nats.Conn).drainConnection). A total
// time-to-CLOSED backstop must exceed a connection's nats.DrainTimeout by at
// least this much, plus a little slack, to avoid firing mid-flush.
const natsPublishDrainTimeout = 5 * time.Second

// Environment variables that carry the org-wide mTLS client identity. The same
// triple is consumed by every Entire service that dials NATS.
const (
	envTLSCert = "ENTIRE_INTERNAL_TLS_CERT_FILE"
	envTLSKey  = "ENTIRE_INTERNAL_TLS_KEY_FILE"
	envTLSCA   = "ENTIRE_INTERNAL_TLS_CA_FILE"
)

// config holds the resolved connection settings. Callers mutate it through
// [Option] values.
type config struct {
	name                 string
	logger               *slog.Logger
	tlsConfig            *tls.Config
	tlsFromEnv           bool
	drainTimeout         time.Duration
	reconnectWait        time.Duration
	maxReconnects        int
	retryOnFailedConnect bool
	extra                []nats.Option
}

// Option configures a [Connect] call.
type Option func(*config)

// WithName sets the NATS connection name (nats.Name), which labels the client
// in server monitoring and in the logs.
func WithName(name string) Option { return func(c *config) { c.name = name } }

// WithLogger sets the logger used for connection lifecycle events. Defaults to
// [slog.Default]. Context-aware log methods are used, so a handler that reads
// trace context from the context still works.
func WithLogger(l *slog.Logger) Option { return func(c *config) { c.logger = l } }

// WithTLSConfig supplies an explicit *tls.Config and disables the default
// env-var mTLS lookup. Pass this (or [WithoutTLS]) when the standard
// ENTIRE_INTERNAL_TLS_* identity does not apply.
func WithTLSConfig(t *tls.Config) Option {
	return func(c *config) {
		c.tlsConfig = t
		c.tlsFromEnv = false
	}
}

// WithoutTLS disables TLS entirely (plaintext). Intended for local development
// and tests against an embedded server; production dials always use mTLS.
func WithoutTLS() Option {
	return func(c *config) {
		c.tlsConfig = nil
		c.tlsFromEnv = false
	}
}

// WithDrainTimeout overrides [DefaultDrainTimeout].
func WithDrainTimeout(d time.Duration) Option { return func(c *config) { c.drainTimeout = d } }

// WithReconnectWait overrides [DefaultReconnectWait].
func WithReconnectWait(d time.Duration) Option { return func(c *config) { c.reconnectWait = d } }

// WithRetryOnFailedConnect keeps [Connect] from returning an error when the
// initial dial fails: nats.go instead returns a connection in RECONNECTING
// state and retries in the background (buffering publishes). It is off by
// default so a misconfigured connection (bad URL, wrong TLS material) fails
// fast at startup — crash-looping the pod so a bad rollout is visible — rather
// than silently accepting work it cannot deliver. Enable it only for
// optional/secondary connections a service is designed to run without.
func WithRetryOnFailedConnect() Option {
	return func(c *config) { c.retryOnFailedConnect = true }
}

// WithNATSOptions appends raw nats.Option values, applied after nuts's
// defaults so a caller can override any of them. An escape hatch for options
// nuts does not model.
func WithNATSOptions(opts ...nats.Option) Option {
	return func(c *config) { c.extra = append(c.extra, opts...) }
}

// Connect opens a NATS connection with the org-standard resiliency posture:
// rotation-aware mTLS (from the ENTIRE_INTERNAL_TLS_* env vars unless
// overridden), reconnect-forever (long-lived services must outlive arbitrary
// NATS outages rather than permanently CLOSE after the client default of 60
// attempts), a bounded drain timeout, and lifecycle handlers that log at the
// right level — notably routing the nil-error DisconnectErr that nats.go fires
// on an explicit Close to INFO, so a clean shutdown does not look like a fault.
// Async errors (slow-consumer drops, permissions violations) and lame-duck
// notices are logged through the configured logger instead of nats.go's
// stderr default; override either handler via [WithNATSOptions].
//
// The initial dial is fail-fast: if the first connect does not succeed Connect
// returns an error, so a misconfigured service crash-loops visibly rather than
// booting into a broken state. Reconnect-forever applies only once a connection
// has been established. Pass [WithRetryOnFailedConnect] to opt into background
// retry of the initial dial instead.
//
// The returned connection should be torn down with [Drain] (or via a
// [ShutdownGroup]) on graceful shutdown, not nc.Close, so in-flight work
// finishes and pull-consumer fetch loops exit cleanly.
func Connect(ctx context.Context, url string, opts ...Option) (*nats.Conn, error) {
	if url == "" {
		return nil, errors.New("nuts: nats url is empty")
	}
	cfg := config{
		logger:        slog.Default(),
		tlsFromEnv:    true,
		drainTimeout:  DefaultDrainTimeout,
		reconnectWait: DefaultReconnectWait,
		maxReconnects: -1,
	}
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.logger == nil {
		cfg.logger = slog.Default()
	}

	var tlsConf *tls.Config
	switch {
	case cfg.tlsConfig != nil:
		tlsConf = cfg.tlsConfig
	case cfg.tlsFromEnv:
		var err error
		if tlsConf, err = tlsConfigFromEnv(); err != nil {
			return nil, err
		}
	}

	// The lifecycle handlers below outlive the Connect call — they fire on
	// disconnects and reconnects for the whole life of the connection, long
	// after a startup- or request-scoped ctx is cancelled. Log through a
	// cancellation-detached copy so a context-aware slog handler still sees the
	// trace values but never drops a mid-life event because ctx is already done.
	logCtx := context.WithoutCancel(ctx)
	connAttr := slog.String("conn", cfg.name)

	natsOpts := []nats.Option{
		nats.DrainTimeout(cfg.drainTimeout),
		nats.RetryOnFailedConnect(cfg.retryOnFailedConnect),
		nats.MaxReconnects(cfg.maxReconnects),
		nats.ReconnectWait(cfg.reconnectWait),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			// nats.go fires this with err==nil when the caller calls Close()
			// explicitly (see nats.Options.DisconnectedErrCB). Logging that at
			// ERROR makes clean shutdowns look like failures, so route the
			// explicit-close case to INFO.
			if err == nil {
				cfg.logger.InfoContext(logCtx, "nuts: NATS disconnected", connAttr)
				return
			}
			cfg.logger.ErrorContext(logCtx, "nuts: NATS disconnected", connAttr, slog.Any("error", err))
		}),
		nats.ClosedHandler(func(_ *nats.Conn) {
			cfg.logger.InfoContext(logCtx, "nuts: NATS connection closed", connAttr)
		}),
		nats.ReconnectHandler(func(_ *nats.Conn) {
			cfg.logger.InfoContext(logCtx, "nuts: NATS reconnected", connAttr)
		}),
		nats.ReconnectErrHandler(func(_ *nats.Conn, err error) {
			cfg.logger.ErrorContext(logCtx, "nuts: NATS reconnect failed", connAttr, slog.Any("error", err))
		}),
		// Without a handler, nats.go routes async errors — slow-consumer
		// drops, permissions violations on missing server grants — to a
		// default that prints to stderr, bypassing the service's structured
		// logs entirely. A permissions violation is how a misconfigured
		// subscription surfaces (the server silently rejects it and the
		// client keeps running), so it must land in the real log stream.
		nats.ErrorHandler(func(_ *nats.Conn, sub *nats.Subscription, err error) {
			attrs := []any{connAttr, slog.Any("error", err)}
			if sub != nil {
				attrs = append(attrs, slog.String("subject", sub.Subject))
			}
			cfg.logger.ErrorContext(logCtx, "nuts: NATS async error", attrs...)
		}),
		// The server announces lame duck mode before it starts evicting
		// clients; reconnection is automatic, but the log line attributes the
		// coming disconnect to server maintenance rather than a fault.
		nats.LameDuckModeHandler(func(_ *nats.Conn) {
			cfg.logger.WarnContext(logCtx, "nuts: NATS server entering lame duck mode", connAttr)
		}),
	}
	if tlsConf != nil {
		natsOpts = append(natsOpts, nats.Secure(tlsConf))
	}
	if cfg.name != "" {
		natsOpts = append(natsOpts, nats.Name(cfg.name))
	}
	natsOpts = append(natsOpts, cfg.extra...)

	nc, err := nats.Connect(url, natsOpts...)
	if err != nil {
		return nil, fmt.Errorf("nuts: connect to NATS: %w", err)
	}
	// With RetryOnFailedConnect enabled, nats.Connect returns a nil error even
	// when the initial dial failed — the connection is in RECONNECTING and the
	// client is retrying in the background. Report that honestly instead of
	// claiming "connected".
	if status := nc.Status(); status != nats.CONNECTED {
		cfg.logger.WarnContext(ctx, "nuts: NATS not yet connected; retrying in background",
			connAttr, slog.String("url", url), slog.String("status", status.String()))
		return nc, nil
	}
	cfg.logger.InfoContext(ctx, "nuts: NATS connected", connAttr, slog.String("url", url))
	return nc, nil
}

// tlsConfigFromEnv builds the mTLS config from the ENTIRE_INTERNAL_TLS_* env
// vars, requiring all three to be set.
func tlsConfigFromEnv() (*tls.Config, error) {
	cert, key, ca := os.Getenv(envTLSCert), os.Getenv(envTLSKey), os.Getenv(envTLSCA)
	if cert == "" || key == "" || ca == "" {
		return nil, fmt.Errorf("nuts: %s, %s, %s all required for NATS mTLS", envTLSCert, envTLSKey, envTLSCA)
	}
	return TLSConfigFromFiles(cert, key, ca)
}

// TLSConfigFromFiles builds a fully rotation-aware mTLS *tls.Config from cert,
// key, and CA files on disk. Both sides of the trust are re-read from disk on
// every handshake: GetClientCertificate reloads the client keypair, and
// VerifyConnection rebuilds the root pool from the CA file and verifies the
// server against it. A cert-manager rotation of either the client identity or
// the trusted CA is therefore picked up on the next reconnect without a process
// restart. The files are read and parsed once here so a misconfiguration fails
// fast at construction time rather than on the first handshake.
//
// Because the roots must be resolved per handshake, verification is done in
// VerifyConnection (which also re-checks the server hostname) rather than via
// the static RootCAs field.
func TLSConfigFromFiles(certFile, keyFile, caFile string) (*tls.Config, error) {
	if _, err := tls.LoadX509KeyPair(certFile, keyFile); err != nil {
		return nil, fmt.Errorf("nuts: load tls keypair: %w", err)
	}
	if _, err := caPoolFromFile(caFile); err != nil {
		return nil, err
	}
	return &tls.Config{
		GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			cert, err := tls.LoadX509KeyPair(certFile, keyFile)
			if err != nil {
				return nil, fmt.Errorf("nuts: reload tls keypair: %w", err)
			}
			return &cert, nil
		},
		// Roots are resolved per handshake in VerifyConnection, so default
		// verification (which would pin a static RootCAs) is turned off and
		// replaced with an equivalent check against a freshly-read pool.
		InsecureSkipVerify: true, //nolint:gosec // server cert + hostname are verified against a freshly-read CA pool in VerifyConnection to stay rotation-aware
		VerifyConnection: func(cs tls.ConnectionState) error {
			roots, err := caPoolFromFile(caFile)
			if err != nil {
				return err
			}
			if len(cs.PeerCertificates) == 0 {
				return errors.New("nuts: server presented no certificate")
			}
			opts := x509.VerifyOptions{
				Roots:         roots,
				DNSName:       cs.ServerName,
				Intermediates: x509.NewCertPool(),
			}
			for _, cert := range cs.PeerCertificates[1:] {
				opts.Intermediates.AddCert(cert)
			}
			if _, err := cs.PeerCertificates[0].Verify(opts); err != nil {
				return fmt.Errorf("nuts: verify server certificate: %w", err)
			}
			return nil
		},
		MinVersion: tls.VersionTLS13,
	}, nil
}

// caPoolFromFile reads a PEM CA bundle from disk and returns it as a cert pool,
// erroring if the file cannot be read or contains no parseable certificates.
func caPoolFromFile(caFile string) (*x509.CertPool, error) {
	caBytes, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("nuts: read ca: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caBytes) {
		return nil, errors.New("nuts: ca PEM: no certs parsed")
	}
	return pool, nil
}
