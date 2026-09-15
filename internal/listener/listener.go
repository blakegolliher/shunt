package listener

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strings"

	"github.com/blakegolliher/shunt/internal/config"
)

// certSet is the default pair plus SNI-selected pairs. Immutable after construction.
type certSet struct {
	def *tls.Certificate
	sni map[string]*tls.Certificate // exact host or "*.suffix"
}

// ErrNoCertificate is returned when neither a default pair nor an SNI entry is configured.
var ErrNoCertificate = errors.New("listener: no certificate configured")

// TLSConfig builds the server TLS configuration from config. Certificates are loaded once.
func TLSConfig(c config.Listener) (*tls.Config, error) {
	cs := &certSet{sni: map[string]*tls.Certificate{}}
	if c.TLS.Cert != "" {
		cert, err := tls.LoadX509KeyPair(c.TLS.Cert, c.TLS.Key)
		if err != nil {
			return nil, fmt.Errorf("listener: default certificate: %w", err)
		}
		cs.def = &cert
	}
	for name, pair := range c.TLS.SNI {
		cert, err := tls.LoadX509KeyPair(pair.Cert, pair.Key)
		if err != nil {
			return nil, fmt.Errorf("listener: sni %s: %w", name, err)
		}
		cs.sni[strings.ToLower(name)] = &cert
	}
	if cs.def == nil && len(cs.sni) == 0 {
		return nil, ErrNoCertificate
	}
	minVersion := uint16(tls.VersionTLS12)
	if c.TLS.MinVersion == "1.3" {
		minVersion = tls.VersionTLS13
	}
	return &tls.Config{
		MinVersion:     minVersion,
		GetCertificate: cs.get,
		NextProtos:     []string{"http/1.1"}, // no h2 (docs/DESIGN.md decision 5)
		// CipherSuites left nil: Go orders by CPU support (docs/DESIGN.md §1.4).
	}, nil
}

// get selects by exact SNI name, then wildcard suffix, then the default pair.
func (cs *certSet) get(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
	name := strings.ToLower(hello.ServerName)
	if name != "" {
		if c, ok := cs.sni[name]; ok {
			return c, nil
		}
		if i := strings.IndexByte(name, '.'); i > 0 {
			if c, ok := cs.sni["*"+name[i:]]; ok {
				return c, nil
			}
		}
	}
	if cs.def != nil {
		return cs.def, nil
	}
	return nil, fmt.Errorf("listener: no certificate for %q", hello.ServerName)
}

// Listen opens the TLS listener on c.Address.
func Listen(c config.Listener) (net.Listener, error) {
	tc, err := TLSConfig(c)
	if err != nil {
		return nil, err
	}
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", c.Address)
	if err != nil {
		return nil, fmt.Errorf("listener: %w", err)
	}
	return tls.NewListener(ln, tc), nil
}
