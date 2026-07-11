package nuts

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"log/slog"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

func TestConnectEmptyURL(t *testing.T) {
	if _, err := Connect(t.Context(), ""); err == nil {
		t.Fatal("expected an error for an empty url")
	}
}

func TestConnectMissingTLSEnv(t *testing.T) {
	// Force the default env-var mTLS lookup to fail deterministically.
	for _, k := range []string{envTLSCert, envTLSKey, envTLSCA} {
		t.Setenv(k, "")
	}
	if _, err := Connect(t.Context(), "nats://127.0.0.1:4222"); err == nil {
		t.Fatal("expected an mTLS-required error when the env triple is unset")
	}
}

func TestTLSConfigFromFilesMissing(t *testing.T) {
	if _, err := TLSConfigFromFiles("/nope/cert", "/nope/key", "/nope/ca"); err == nil {
		t.Fatal("expected an error for missing cert files")
	}
}

func TestTLSConfigFromFilesAndRotation(t *testing.T) {
	dir := t.TempDir()
	certFile := filepath.Join(dir, "tls.crt")
	keyFile := filepath.Join(dir, "tls.key")
	caFile := filepath.Join(dir, "ca.crt")

	firstCertPEM := writeSelfSignedCert(t, certFile, keyFile)
	// Use the leaf as its own CA so the pool parses.
	if err := os.WriteFile(caFile, firstCertPEM, 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := TLSConfigFromFiles(certFile, keyFile, caFile)
	if err != nil {
		t.Fatalf("TLSConfigFromFiles: %v", err)
	}
	if cfg.MinVersion != tls.VersionTLS13 {
		t.Errorf("MinVersion = %x, want TLS 1.3", cfg.MinVersion)
	}

	got1, err := cfg.GetClientCertificate(&tls.CertificateRequestInfo{})
	if err != nil {
		t.Fatalf("GetClientCertificate: %v", err)
	}

	// Rotate the cert+key on disk. The callback must serve the NEW cert on the
	// next handshake without the config being rebuilt.
	writeSelfSignedCert(t, certFile, keyFile)
	got2, err := cfg.GetClientCertificate(&tls.CertificateRequestInfo{})
	if err != nil {
		t.Fatalf("GetClientCertificate after rotation: %v", err)
	}
	if bytes.Equal(got1.Certificate[0], got2.Certificate[0]) {
		t.Fatal("GetClientCertificate did not pick up the rotated cert")
	}
}

// writeSelfSignedCert generates a fresh self-signed cert (random serial + key)
// and writes it as PEM to certFile/keyFile, returning the certificate PEM. Each
// call produces a distinct certificate, so calling it twice simulates rotation.
func writeSelfSignedCert(t *testing.T, certFile, keyFile string) []byte {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          randSerial(t),
		Subject:               pkix.Name{CommonName: "nuts-test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	if err := os.WriteFile(certFile, certPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, keyPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	return certPEM
}

func TestConnectFailsFastOnUnreachableServer(t *testing.T) {
	// The default posture is fail-fast: an unreachable server must yield an
	// error, not a silently-reconnecting connection reported as healthy.
	if _, err := Connect(t.Context(), "nats://127.0.0.1:1", WithoutTLS()); err == nil {
		t.Fatal("expected an error connecting to an unreachable server without retry-on-failed-connect")
	}
}

func TestConnectRetryOnFailedConnectReturnsPendingConn(t *testing.T) {
	// Opting in keeps the initial failure non-fatal: a connection is returned in
	// a not-yet-connected state and retries in the background.
	nc, err := Connect(t.Context(), "nats://127.0.0.1:1", WithoutTLS(), WithRetryOnFailedConnect())
	if err != nil {
		t.Fatalf("retry-on-failed-connect should not error on initial failure: %v", err)
	}
	if nc == nil {
		t.Fatal("expected a non-nil connection in retry-on-failed-connect mode")
	}
	t.Cleanup(nc.Close)
	if nc.IsConnected() {
		t.Fatal("did not expect an established connection to an unreachable server")
	}
}

func TestConnectLogsConnName(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	nc, err := Connect(t.Context(), runEmbeddedServer(t), WithoutTLS(), WithName("worker"), WithLogger(logger))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)

	if !strings.Contains(buf.String(), "conn=worker") {
		t.Fatalf("connected log is missing the conn-name attribute: %q", buf.String())
	}
}

// TestConnectLogsAsyncErrors: a permissions violation is an async error — the
// Subscribe call itself succeeds and the server rejects it out of band — so
// without a handler it reaches only nats.go's stderr default, invisible to the
// service's structured logs. Connect must route it through the configured
// logger.
func TestConnectLogsAsyncErrors(t *testing.T) {
	s := runEmbeddedServerWith(t, &natsserver.Options{
		Users: []*natsserver.User{{
			Username: "worker",
			Password: "pw",
			Permissions: &natsserver.Permissions{
				Subscribe: &natsserver.SubjectPermission{Deny: []string{"denied.>"}},
			},
		}},
	})
	logger, captured := newCapturingLogger()
	nc, err := Connect(t.Context(), s.ClientURL(), WithoutTLS(), WithLogger(logger),
		WithNATSOptions(nats.UserInfo("worker", "pw")))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)

	// The violation surfaces asynchronously; the subscribe itself must not fail.
	if _, err := nc.Subscribe("denied.topic", func(*nats.Msg) {}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	captured.waitFor(t, "nuts: NATS async error")
}

// TestConnectLogsLameDuckMode: the server's lame-duck announcement must be
// logged so the coming eviction reads as maintenance, not a fault.
func TestConnectLogsLameDuckMode(t *testing.T) {
	s := runEmbeddedServerWith(t, &natsserver.Options{
		LameDuckDuration: 250 * time.Millisecond,
		// Negative means "flip positive but skip the grace<duration
		// validation" — the documented nats-server test bypass.
		LameDuckGracePeriod: -10 * time.Millisecond,
	})
	logger, captured := newCapturingLogger()
	nc, err := Connect(t.Context(), s.ClientURL(), WithoutTLS(), WithLogger(logger))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)

	// LameDuckShutdown blocks for the full lame-duck duration.
	go s.LameDuckShutdown()
	captured.waitFor(t, "nuts: NATS server entering lame duck mode")
}

// TestTLSConfigCARotation guards that the trusted CA is re-read from disk on
// every handshake (via VerifyConnection) rather than pinned once at startup, so
// a CA rotation is honored without a process restart.
func TestTLSConfigCARotation(t *testing.T) {
	dir := t.TempDir()
	certFile := filepath.Join(dir, "tls.crt")
	keyFile := filepath.Join(dir, "tls.key")
	caFile := filepath.Join(dir, "ca.crt")

	// A client identity keypair — GetClientCertificate just needs to load it.
	writeSelfSignedCert(t, certFile, keyFile)

	caA := newTestCA(t)
	caB := newTestCA(t)
	serverLeaf := caA.signLeaf(t, "nats.internal") // signed by CA A only

	// Start out trusting CA A.
	if err := os.WriteFile(caFile, caA.certPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := TLSConfigFromFiles(certFile, keyFile, caFile)
	if err != nil {
		t.Fatalf("TLSConfigFromFiles: %v", err)
	}

	stateA := tls.ConnectionState{ServerName: "nats.internal", PeerCertificates: []*x509.Certificate{serverLeaf}}
	if err := cfg.VerifyConnection(stateA); err != nil {
		t.Fatalf("verify against the trusting CA should pass: %v", err)
	}

	// Rotate the CA file to a different CA that did NOT sign the server leaf.
	if err := os.WriteFile(caFile, caB.certPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := cfg.VerifyConnection(stateA); err == nil {
		t.Fatal("verify must fail after the CA on disk is rotated to one that did not sign the server cert")
	}

	// And a leaf signed by the newly-trusted CA B must now verify.
	stateB := tls.ConnectionState{ServerName: "nats.internal", PeerCertificates: []*x509.Certificate{caB.signLeaf(t, "nats.internal")}}
	if err := cfg.VerifyConnection(stateB); err != nil {
		t.Fatalf("verify against the rotated-in CA should pass: %v", err)
	}
}

// testCA is a self-signed certificate authority for tests.
type testCA struct {
	cert    *x509.Certificate
	key     *ecdsa.PrivateKey
	certPEM []byte
}

func newTestCA(t *testing.T) testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          randSerial(t),
		Subject:               pkix.Name{CommonName: "nuts-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return testCA{cert: cert, key: key, certPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

// signLeaf returns a server leaf certificate with the given DNS SAN, signed by
// the CA.
func (ca testCA) signLeaf(t *testing.T, dnsName string) *x509.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: randSerial(t),
		Subject:      pkix.Name{CommonName: dnsName},
		DNSNames:     []string{dnsName},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return cert
}

func randSerial(t *testing.T) *big.Int {
	t.Helper()
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		t.Fatal(err)
	}
	return serial
}
