package upstream

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/blakegolliher/shunt/internal/config"
)

// Cluster is one backend with its endpoints and transport.
type Cluster struct {
	Name      string
	Type      string
	Scheme    string
	Region    string
	Endpoints []string // host:port
	Transport *http.Transport

	next atomic.Uint64
}

// Options tune the transport. Zero values get the defaults below.
type Options struct {
	ExpectContinueTimeout time.Duration // default 1s
	MaxIdlePerHost        int           // default 256
	IdleConnTimeout       time.Duration // default 90s
	DialTimeout           time.Duration // default 5s
}

// ErrNoEndpoints is returned by New for a cluster with an empty endpoint list.
var ErrNoEndpoints = errors.New("upstream: cluster has no endpoints")

// New builds a Cluster from config. dns endpoint mode is deferred (POC.md); a dns-mode cluster
// is treated as a one-entry static list, which is what POC-3 specifies for AWS.
func New(name string, c config.Cluster, o Options) (*Cluster, error) {
	eps := c.Endpoints
	if c.EndpointMode == "dns" && c.Endpoint != "" {
		eps = []string{c.Endpoint}
	}
	if len(eps) == 0 {
		return nil, ErrNoEndpoints
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
	return &Cluster{Name: name, Type: c.Type, Scheme: c.Scheme, Region: c.Region, Endpoints: eps, Transport: tr}, nil
}

// Next returns the next endpoint, round-robin. Health and ejection are P3a.
func (c *Cluster) Next() string {
	n := c.next.Add(1) - 1
	return c.Endpoints[n%uint64(len(c.Endpoints))]
}

// Close releases idle connections.
func (c *Cluster) Close() { c.Transport.CloseIdleConnections() }
