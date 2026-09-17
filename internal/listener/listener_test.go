package listener

import (
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

	"github.com/blakegolliher/shunt/internal/config"
)

// writeCert generates a self-signed cert for names into dir and returns the pair paths.
func writeCert(t *testing.T, dir, base string, names ...string) config.CertPair {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: names[0]}, DNSNames: names,
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	kb, _ := x509.MarshalECPrivateKey(key)
	p := config.CertPair{Cert: filepath.Join(dir, base+".crt"), Key: filepath.Join(dir, base+".key")}
	if err := os.WriteFile(p.Cert, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p.Key, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kb}), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestSNISelection(t *testing.T) {
	dir := t.TempDir()
	def := writeCert(t, dir, "def", "*.shunt.example.com", "shunt.example.com")
	alt := writeCert(t, dir, "alt", "*.s3.example.net")
	exact := writeCert(t, dir, "exact", "special.example.org")
	tc, err := tlsConfig(config.Listener{TLS: config.ListenerTLS{
		Cert: def.Cert, Key: def.Key, MinVersion: "1.2",
		SNI: map[string]config.CertPair{"*.s3.example.net": alt, "special.example.org": exact},
	}})
	if err != nil {
		t.Fatal(err)
	}
	cn := func(name string) string {
		c, err := tc.GetCertificate(&tls.ClientHelloInfo{ServerName: name})
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		leaf, _ := x509.ParseCertificate(c.Certificate[0])
		return leaf.Subject.CommonName
	}
	cases := map[string]string{
		"b.shunt.example.com": "*.shunt.example.com",
		"b.s3.example.net":    "*.s3.example.net",
		"B.S3.EXAMPLE.NET":    "*.s3.example.net",
		"special.example.org": "special.example.org",
		"other.example.org":   "*.shunt.example.com", // default
		"":                    "*.shunt.example.com", // no SNI → default
	}
	for name, want := range cases {
		if got := cn(name); got != want {
			t.Errorf("SNI %q → %q, want %q", name, got, want)
		}
	}
	if tc.MinVersion != tls.VersionTLS12 || len(tc.NextProtos) != 1 || tc.NextProtos[0] != "http/1.1" || tc.CipherSuites != nil {
		t.Errorf("tls config: min=%x protos=%v suites=%v", tc.MinVersion, tc.NextProtos, tc.CipherSuites)
	}
}

func TestMinVersionAndNoDefault(t *testing.T) {
	dir := t.TempDir()
	alt := writeCert(t, dir, "alt", "*.s3.example.net")
	tc, err := tlsConfig(config.Listener{TLS: config.ListenerTLS{MinVersion: "1.3", SNI: map[string]config.CertPair{"*.s3.example.net": alt}}})
	if err != nil {
		t.Fatal(err)
	}
	if tc.MinVersion != tls.VersionTLS13 {
		t.Errorf("min version %x", tc.MinVersion)
	}
	if _, err := tc.GetCertificate(&tls.ClientHelloInfo{ServerName: "nomatch.example.org"}); err == nil {
		t.Error("expected no certificate for an unmatched name without a default")
	}
	if _, err := tlsConfig(config.Listener{}); err != errNoCertificate {
		t.Errorf("want ErrNoCertificate, got %v", err)
	}
	if _, err := tlsConfig(config.Listener{TLS: config.ListenerTLS{Cert: "/nope.crt", Key: "/nope.key"}}); err == nil {
		t.Error("expected error for missing files")
	}
}

func TestListenHandshake(t *testing.T) {
	dir := t.TempDir()
	def := writeCert(t, dir, "def", "*.shunt.example.com")
	ln, err := Listen(config.Listener{Address: "127.0.0.1:0", TLS: config.ListenerTLS{Cert: def.Cert, Key: def.Key, MinVersion: "1.2"}})
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		c, aerr := ln.Accept()
		if aerr == nil {
			_ = c.(*tls.Conn).Handshake()
			c.Close()
		}
	}()
	conn, err := tls.Dial("tcp", ln.Addr().String(), &tls.Config{ServerName: "b.shunt.example.com", InsecureSkipVerify: true, NextProtos: []string{"h2", "http/1.1"}}) //nolint:gosec // test
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if st := conn.ConnectionState(); st.NegotiatedProtocol != "http/1.1" || st.Version < tls.VersionTLS12 {
		t.Fatalf("negotiated %q version %x", st.NegotiatedProtocol, st.Version)
	}
}

func BenchmarkGetCertificate(b *testing.B) {
	dir := b.TempDir()
	def := writeCert(&testing.T{}, dir, "def", "*.shunt.example.com")
	alt := writeCert(&testing.T{}, dir, "alt", "*.s3.example.net")
	tc, err := tlsConfig(config.Listener{TLS: config.ListenerTLS{Cert: def.Cert, Key: def.Key, SNI: map[string]config.CertPair{"*.s3.example.net": alt}}})
	if err != nil {
		b.Fatal(err)
	}
	hello := &tls.ClientHelloInfo{ServerName: "bucket.s3.example.net"}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := tc.GetCertificate(hello); err != nil {
			b.Fatal(err)
		}
	}
}

// A plaintext listener serves raw TCP and needs no certificate at all.
func TestListenPlaintext(t *testing.T) {
	ln, err := Listen(config.Listener{Address: "127.0.0.1:0", Plaintext: true})
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	if _, isTLS := ln.(interface{ ConnectionState() tls.ConnectionState }); isTLS {
		t.Fatal("plaintext listener wraps TLS")
	}
	if _, err := Listen(config.Listener{Address: "127.0.0.1:0"}); err == nil {
		t.Fatal("a TLS listener without a certificate opened")
	}
}
