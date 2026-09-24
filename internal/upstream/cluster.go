package upstream

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/blakegolliher/shunt/internal/config"
	"github.com/blakegolliher/shunt/internal/sigv4"
)

// Cluster is one backend with its endpoints, transport, and, in resign mode, the credentials and
// capability profile shunt signs and plans request bodies with.
type Cluster struct {
	Name      string
	Type      string
	Scheme    string
	Region    string
	ID        string   // opaque, stable id: the uploadId prefix (docs/DESIGN.md §2.4)
	Endpoints []string // host:port
	Transport *http.Transport

	Creds sigv4.Credentials // set by NewSet when secrets are resolved (resign mode)
	// SecretGeneration is the generation of the secret Creds signs with: the directory version
	// that last changed it (directory.SecretResource), 0 where the directory does not track it
	// (a lab's env: or file: ref). Proxies report it, so a rotation shows who signs with the new
	// secret yet (ADR-0021 D1).
	SecretGeneration int64
	EnforcesSHA256   bool // backend rejects a wrong hex x-amz-content-sha256 itself
	UnsignedTrailer  bool // backend accepts STREAMING-UNSIGNED-PAYLOAD-TRAILER
	ConditionalWrite bool // backend honors If-None-Match: * on PUT (ADR-0004)

	next atomic.Uint64
}

// Options tune the transport. Zero values get the defaults below.
type Options struct {
	ExpectContinueTimeout time.Duration // default 1s
	MaxIdlePerHost        int           // default 256
	IdleConnTimeout       time.Duration // default 90s
	DialTimeout           time.Duration // default 5s
}

// errNoEndpoints is returned by New for a cluster with an empty endpoint list.
var errNoEndpoints = errors.New("upstream: cluster has no endpoints")

// ClusterID derives a cluster's opaque id: the first 6 hex characters of SHA-256 over its name.
// It is stable across restarts and reveals neither the name nor the backend type.
func ClusterID(name string) string {
	sum := sha256.Sum256([]byte("shunt-cluster:" + name))
	return hex.EncodeToString(sum[:3])
}

// New builds a Cluster from config. dns endpoint mode is deferred (POC.md); a dns-mode cluster
// is treated as a one-entry static list, which is what POC-3 specifies for AWS.
func New(name string, c config.Cluster, o Options) (*Cluster, error) {
	eps := c.Endpoints
	if c.EndpointMode == "dns" && c.Endpoint != "" {
		eps = []string{c.Endpoint}
	}
	if len(eps) == 0 {
		return nil, errNoEndpoints
	}
	if o.ExpectContinueTimeout == 0 {
		o.ExpectContinueTimeout = time.Second
	}
	if o.MaxIdlePerHost == 0 {
		o.MaxIdlePerHost = 256
	}
	if o.IdleConnTimeout == 0 {
		o.IdleConnTimeout = 90 * time.Second
	}
	if o.DialTimeout == 0 {
		o.DialTimeout = 5 * time.Second
	}
	tr := &http.Transport{
		Proxy:                 nil, // never use the environment's HTTP proxy for backends
		DialContext:           (&net.Dialer{Timeout: o.DialTimeout, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:     false, // HTTP/1.1 only (docs/DESIGN.md decision 5)
		TLSNextProto:          map[string]func(string, *tls.Conn) http.RoundTripper{},
		MaxIdleConns:          o.MaxIdlePerHost * len(eps),
		MaxIdleConnsPerHost:   o.MaxIdlePerHost,
		IdleConnTimeout:       o.IdleConnTimeout,
		ExpectContinueTimeout: o.ExpectContinueTimeout,
		// No ResponseHeaderTimeout: metadata ops carry a context deadline, data ops an idle
		// deadline; a fixed header timeout would kill slow CompleteMultipartUpload calls.
		DisableCompression: true, // bodies are opaque; never let the transport add Accept-Encoding
	}
	if c.Scheme == "https" {
		tc := &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: c.TLS.InsecureSkipVerify} //nolint:gosec // operator opt-in, validated and logged
		if c.TLS.CA != "" {
			pem, err := os.ReadFile(c.TLS.CA)
			if err != nil {
				return nil, fmt.Errorf("upstream %s: read ca: %w", name, err)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(pem) {
				return nil, fmt.Errorf("upstream %s: no certificates in %s", name, c.TLS.CA)
			}
			tc.RootCAs = pool
		}
		tr.TLSClientConfig = tc
	}
	return &Cluster{
		Name: name, Type: c.Type, Scheme: c.Scheme, Region: c.Region, ID: ClusterID(name), Endpoints: eps, Transport: tr,
		Creds:          sigv4.Credentials{AccessKey: c.Credentials.AccessKey},
		EnforcesSHA256: c.Capabilities.EnforcesSHA256Or(true), UnsignedTrailer: c.Capabilities.UnsignedTrailerOr(true),
		ConditionalWrite: c.Capabilities.ConditionalWriteOr(true),
	}, nil
}

// withSecret returns a copy of c that signs with secret and shares c's transport, so a
// secret-only rotation keeps the pooled connections. c itself keeps its secret: a request that
// loaded it finishes signing with the secret it started with.
func (c *Cluster) withSecret(secret string, generation int64) *Cluster {
	n := &Cluster{Name: c.Name, Type: c.Type, Scheme: c.Scheme, Region: c.Region, ID: c.ID, Endpoints: c.Endpoints, Transport: c.Transport,
		Creds: c.Creds, EnforcesSHA256: c.EnforcesSHA256, UnsignedTrailer: c.UnsignedTrailer, ConditionalWrite: c.ConditionalWrite,
		SecretGeneration: generation}
	n.Creds.Secret = secret
	return n
}

// Next returns the next endpoint, round-robin. Health and ejection are P3a.
func (c *Cluster) Next() string {
	n := c.next.Add(1) - 1
	return c.Endpoints[n%uint64(len(c.Endpoints))]
}

// Close releases idle connections.
func (c *Cluster) Close() { c.Transport.CloseIdleConnections() }

// IsConnectError reports whether err is a failure to connect to an endpoint: no request byte was
// sent, so the request may be retried on another endpoint.
func IsConnectError(err error) bool {
	var op *net.OpError
	return errors.As(err, &op) && op.Op == "dial"
}

// Set is every configured cluster, by name and by opaque id.
type Set struct {
	byName map[string]*Cluster
	byID   map[string]*Cluster
	names  []string
}

// Get returns a cluster by name.
func (s *Set) Get(name string) (*Cluster, bool) {
	c, ok := s.byName[name]
	return c, ok
}

// ByID returns a cluster by opaque id.
func (s *Set) ByID(id string) (*Cluster, bool) {
	c, ok := s.byID[id]
	return c, ok
}

// Names lists cluster names, sorted.
func (s *Set) Names() []string { return s.names }

// Close releases every cluster's idle connections.
func (s *Set) Close() {
	for _, c := range s.byName {
		c.Close()
	}
}
