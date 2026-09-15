package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"go.yaml.in/yaml/v4"
)

// Config is the root of the shunt configuration file.
type Config struct {
	Listener   Listener             `yaml:"listener"`
	Admin      Admin                `yaml:"admin"`
	Auth       Auth                 `yaml:"auth"`
	Proxy      Proxy                `yaml:"proxy"`
	Clusters   map[string]Cluster   `yaml:"clusters"`
	Tenants    map[string]Tenant    `yaml:"tenants"`
	Placements map[string]Placement `yaml:"placements"`
	Telemetry  Telemetry            `yaml:"telemetry"`
	Features   Features             `yaml:"features"`
}

// Listener is the client-facing TLS listener (docs/DESIGN.md §2.9).
type Listener struct {
	Address string      `yaml:"address"`
	Domains []string    `yaml:"domains"` // wildcard domains for virtual-host addressing, e.g. "*.s3.example.net"
	TLS     ListenerTLS `yaml:"tls"`
}

// ListenerTLS holds the default certificate pair and an optional SNI map.
type ListenerTLS struct {
	Cert       string              `yaml:"cert"`
	Key        string              `yaml:"key"`
	MinVersion string              `yaml:"min_version"` // "1.2" (default) or "1.3"
	SNI        map[string]CertPair `yaml:"sni"`
}

// CertPair is one certificate and key on disk.
type CertPair struct {
	Cert string `yaml:"cert"`
	Key  string `yaml:"key"`
}

// Admin is the separate admin listener (/-/healthz, /-/metrics, /debug/pprof).
type Admin struct {
	Address string `yaml:"address"`
}

// Auth selects the auth mode (ADR-0001).
type Auth struct {
	Mode            string        `yaml:"mode"` // passthrough | resign
	CredentialsFile string        `yaml:"credentials_file"`
	ClockSkew       time.Duration `yaml:"clock_skew"`
}

// Proxy holds data-path tunables and, until the directory arrives (POC-3), the one cluster
// every request is forwarded to.
type Proxy struct {
	Cluster         string        `yaml:"cluster"` // name of the clusters: entry to forward to (passthrough mode)
	CopyBufferBytes int           `yaml:"copy_buffer_bytes"`
	IdleTimeout     time.Duration `yaml:"idle_timeout"`     // data ops: no progress for this long → abort
	MetadataTimeout time.Duration `yaml:"metadata_timeout"` // metadata ops: total deadline
	DrainTimeout    time.Duration `yaml:"drain_timeout"`
}

// Cluster is one backend (docs/DESIGN.md §2.3).
type Cluster struct {
	Type           string      `yaml:"type"`   // vast | minio | aws | s3
	Scheme         string      `yaml:"scheme"` // https | http — required, never defaulted
	Region         string      `yaml:"region"`
	EndpointMode   string      `yaml:"endpoint_mode"` // static (default) | dns
	Endpoints      []string    `yaml:"endpoints"`     // static mode
	Endpoint       string      `yaml:"endpoint"`      // dns mode
	TLS            ClusterTLS  `yaml:"tls"`
	Credentials    Credentials `yaml:"credentials"`
	StorageClasses string      `yaml:"storage_classes"` // native | emulated
	StorageClass   string      `yaml:"storage_class"`   // emulated cold clusters only
	Access         string      `yaml:"access"`          // instant | restore-required
}

// ClusterTLS is the upstream TLS configuration for an https cluster.
type ClusterTLS struct {
	CA                 string `yaml:"ca"`
	InsecureSkipVerify bool   `yaml:"insecure_skip_verify"`
}

// Credentials are the cluster's own S3 credentials. Secrets are referenced, never inlined.
type Credentials struct {
	AccessKey string `yaml:"access_key"`
	SecretRef string `yaml:"secret_ref"` // env:NAME or file:/path
}

// Tenant is a customer namespace.
type Tenant struct {
	DefaultCluster string `yaml:"default_cluster"`
}

// Placement maps a (tenant, bucket) to clusters and a state (docs/DESIGN.md §2.3, §2.5).
type Placement struct {
	State     string            `yaml:"state"`
	Primary   string            `yaml:"primary"`
	Source    string            `yaml:"source"`
	Ramp      *Ramp             `yaml:"ramp"`
	Names     map[string]string `yaml:"names"` // cluster → backend bucket name
	Cold      string            `yaml:"cold"`
	Tier      string            `yaml:"tier"` // native | emulated
	Lifecycle string            `yaml:"lifecycle"`
}

// Ramp is the deterministic write shift during RAMPING (decision 15).
type Ramp struct {
	Ratio    float64  `yaml:"ratio"`
	Prefixes []string `yaml:"prefixes"`
}

// Telemetry configures the access log and slow ring.
type Telemetry struct {
	AccessLog AccessLog `yaml:"access_log"`
	Slow      Slow      `yaml:"slow"`
}

// AccessLog is the JSON access log sink.
type AccessLog struct {
	Enabled bool   `yaml:"enabled"`
	Path    string `yaml:"path"` // empty → stdout
}

// Slow configures the slow-request ring.
type Slow struct {
	RingSize  int           `yaml:"ring_size"`
	Threshold time.Duration `yaml:"threshold"`
}

// Features holds feature flags. All default off; each has a removal criterion in its comment.
// None exist yet; the struct is here so `features:` is a known key and any flag name is an error.
type Features struct{}

// Placement states (docs/DESIGN.md §2.5).
const (
	StateActive    = "ACTIVE"
	StateRamping   = "RAMPING"
	StateMigrating = "MIGRATING"
	StateCutover   = "CUTOVER"
)

// Defaults applied before validation. Everything that is safe to default is here; scheme is not.
const (
	DefaultListenerAddress = ":443"
	DefaultAdminAddress    = "127.0.0.1:9900"
	DefaultCopyBufferBytes = 256 << 10
	DefaultIdleTimeout     = 60 * time.Second
	DefaultMetadataTimeout = 30 * time.Second
	DefaultDrainTimeout    = 30 * time.Second
	DefaultClockSkew       = 15 * time.Minute
	DefaultSlowRingSize    = 100
	DefaultSlowThreshold   = 500 * time.Millisecond
	DefaultMinTLSVersion   = "1.2"
)

// ErrEmpty is returned when the input has no YAML document.
var ErrEmpty = errors.New("config: empty document")

// Load reads, parses, applies defaults, and validates a config file.
func Load(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close() //nolint:errcheck // read-only file
	return Read(f)
}

// Read parses, applies defaults, and validates a config from r.
func Read(r io.Reader) (*Config, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	return Parse(data)
}

// Parse parses, applies defaults, and validates a config from bytes.
func Parse(data []byte) (*Config, error) {
	if len(bytes.TrimSpace(data)) == 0 {
		return nil, ErrEmpty
	}
	var c Config
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&c); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, ErrEmpty
		}
		return nil, decodeError(data, err)
	}
	c.applyDefaults()
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

// decodeError turns the YAML library's positional errors into errors that name the key path
// (e.g. `proxy.idle_timeout`), so check-config output points at the offending key, not a line number.
func decodeError(data []byte, err error) error {
	var le *yaml.LoadErrors
	if !errors.As(err, &le) || len(le.Errors) == 0 {
		return fmt.Errorf("config: %w", err)
	}
	var root yaml.Node
	if yaml.Unmarshal(data, &root) != nil {
		return fmt.Errorf("config: %w", err)
	}
	out := make([]error, 0, len(le.Errors))
	for _, e := range le.Errors {
		key := keyPath(&root, e.Mark.Line, e.Mark.Column)
		if key == "" {
			key = fmt.Sprintf("line %d", e.Mark.Line)
		}
		msg := e.Message
		if strings.HasPrefix(msg, "field ") && strings.Contains(msg, " not found in type ") {
			msg = "unknown key"
		}
		out = append(out, &Error{Key: key, Msg: msg})
	}
	return errors.Join(out...)
}

// keyPath returns the dotted path of the deepest mapping key at or before (line, col).
// Positions are 1-indexed on both the error marks and the nodes.
func keyPath(root *yaml.Node, line, col int) string {
	n := root
	if n.Kind == yaml.DocumentNode && len(n.Content) > 0 {
		n = n.Content[0]
	}
	var parts []string
	for {
		switch n.Kind {
		case yaml.MappingNode:
			var key, val *yaml.Node
			for i := 0; i+1 < len(n.Content); i += 2 {
				k := n.Content[i]
				if before(k.Line, k.Column, line, col) {
					key, val = k, n.Content[i+1]
				}
			}
			if key == nil {
				return strings.Join(parts, ".")
			}
			parts = append(parts, key.Value)
			if val.Kind == yaml.ScalarNode || val.Kind == yaml.AliasNode || !before(val.Line, val.Column, line, col) {
				return strings.Join(parts, ".")
			}
			n = val
		case yaml.SequenceNode:
			idx := -1
			for i, item := range n.Content {
				if before(item.Line, item.Column, line, col) {
					idx = i
				}
			}
			if idx < 0 {
				return strings.Join(parts, ".")
			}
			parts[len(parts)-1] += fmt.Sprintf("[%d]", idx)
			n = n.Content[idx]
			if n.Kind == yaml.ScalarNode {
				return strings.Join(parts, ".")
			}
		default:
			return strings.Join(parts, ".")
		}
	}
}

// before reports whether (l1, c1) is at or before (l2, c2) in document order.
func before(l1, c1, l2, c2 int) bool {
	return l1 < l2 || (l1 == l2 && c1 <= c2)
}

func (c *Config) applyDefaults() {
	if c.Listener.Address == "" {
		c.Listener.Address = DefaultListenerAddress
	}
	if c.Listener.TLS.MinVersion == "" {
		c.Listener.TLS.MinVersion = DefaultMinTLSVersion
	}
	if c.Admin.Address == "" {
		c.Admin.Address = DefaultAdminAddress
	}
	if c.Auth.ClockSkew == 0 {
		c.Auth.ClockSkew = DefaultClockSkew
	}
	if c.Proxy.CopyBufferBytes == 0 {
		c.Proxy.CopyBufferBytes = DefaultCopyBufferBytes
	}
	if c.Proxy.IdleTimeout == 0 {
		c.Proxy.IdleTimeout = DefaultIdleTimeout
	}
	if c.Proxy.MetadataTimeout == 0 {
		c.Proxy.MetadataTimeout = DefaultMetadataTimeout
	}
	if c.Proxy.DrainTimeout == 0 {
		c.Proxy.DrainTimeout = DefaultDrainTimeout
	}
	if c.Telemetry.Slow.RingSize == 0 {
		c.Telemetry.Slow.RingSize = DefaultSlowRingSize
	}
	if c.Telemetry.Slow.Threshold == 0 {
		c.Telemetry.Slow.Threshold = DefaultSlowThreshold
	}
	for name := range c.Clusters {
		if c.Clusters[name].EndpointMode == "" {
			cl := c.Clusters[name]
			cl.EndpointMode = "static"
			c.Clusters[name] = cl
		}
	}
}
