package entwine

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
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
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "entwine-test"},
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
