package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"go.yaml.in/yaml/v4"
)

// Config is the root of the shunt configuration file.
type Config struct {
	Listener  Listener           `yaml:"listener"`
	Admin     Admin              `yaml:"admin"`
	Auth      Auth               `yaml:"auth"`
	Proxy     Proxy              `yaml:"proxy"`
	Clusters  map[string]Cluster `yaml:"clusters"` // passthrough only; resign-mode clusters live in the directory file
	Directory Directory          `yaml:"directory"`
	Telemetry Telemetry          `yaml:"telemetry"`
	Features  Features           `yaml:"features"`
	// KillSwitches turn off behavior that is normally on; they are not feature flags (CLAUDE.md).
	KillSwitches KillSwitches `yaml:"kill_switches"`
}

// Directory points at the directory file: tenants and placements (docs/DESIGN.md §2.3, ADR-0005).
// It lives in its own file because shunt writes it (CreateBucket, DeleteBucket, set-state) and
// must never rewrite the operator's config. Required in resign mode, forbidden in passthrough.
type Directory struct {
	File string `yaml:"file"`
	// SecretsDir holds the cluster secrets shunt was given at `shunt cluster add` (one 0600 file each,
	// referenced from the directory as file: refs). Default: a secrets/ directory next to File.
	SecretsDir   string        `yaml:"secrets_dir"`
	PollInterval time.Duration `yaml:"poll_interval"` // how often serve checks the file for other writers' changes
}

// Listener is the client-facing TLS listener (docs/DESIGN.md §2.9).
type Listener struct {
	Address string `yaml:"address"`
	// Plaintext serves clients over http with no TLS. It exists for labs (POC-5 walkthrough) and
	// is never a default: it must be stated, it refuses any tls setting beside it, and serve
	// marks every startup line with it.
	Plaintext bool        `yaml:"plaintext"`
	Domains   []string    `yaml:"domains"` // wildcard domains for virtual-host addressing, e.g. "*.s3.example.net"
	TLS       ListenerTLS `yaml:"tls"`
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

// Admin is the separate admin listener (/-/healthz, /-/metrics, /debug/pprof, and the control API
// under /v1/ in resign mode).
type Admin struct {
	Address string `yaml:"address"`
	// ControlTokenRef (env:NAME or file:/path) is the bearer token the control API requires. Unset,
	// the control API answers loopback peers only.
	ControlTokenRef string `yaml:"control_token_ref"`
}

// Auth selects the auth mode (ADR-0001).
type Auth struct {
	Mode            string        `yaml:"mode"` // passthrough | resign
	CredentialsFile string        `yaml:"credentials_file"`
	ClockSkew       time.Duration `yaml:"clock_skew"`
}

// Proxy holds data-path tunables and, in passthrough mode, the one cluster every request is
// forwarded to. Resign mode routes by placement (directory.file) instead.
type Proxy struct {
	Cluster         string        `yaml:"cluster"` // passthrough only: the clusters: entry to forward to
	CopyBufferBytes int           `yaml:"copy_buffer_bytes"`
	IdleTimeout     time.Duration `yaml:"idle_timeout"`     // data ops: no progress for this long → abort
	MetadataTimeout time.Duration `yaml:"metadata_timeout"` // metadata ops: total deadline
	DrainTimeout    time.Duration `yaml:"drain_timeout"`
}

// Cluster is one backend (docs/DESIGN.md §2.3).
type Cluster struct {
	Type           string       `yaml:"type" json:"type,omitempty"`     // vast | minio | aws | s3
	Scheme         string       `yaml:"scheme" json:"scheme,omitempty"` // https | http — required, never defaulted
	Region         string       `yaml:"region" json:"region,omitempty"`
	EndpointMode   string       `yaml:"endpoint_mode,omitempty" json:"endpoint_mode,omitempty"` // static (default) | dns
	Endpoints      []string     `yaml:"endpoints,omitempty" json:"endpoints,omitempty"`         // static mode
	Endpoint       string       `yaml:"endpoint,omitempty" json:"endpoint,omitempty"`           // dns mode
	TLS            ClusterTLS   `yaml:"tls,omitempty" json:"tls,omitempty"`
	Credentials    Credentials  `yaml:"credentials" json:"credentials,omitempty"`
	StorageClasses string       `yaml:"storage_classes,omitempty" json:"storage_classes,omitempty"` // native | emulated
	StorageClass   string       `yaml:"storage_class,omitempty" json:"storage_class,omitempty"`     // emulated cold clusters only
	Access         string       `yaml:"access,omitempty" json:"access,omitempty"`                   // instant | restore-required
	Capabilities   Capabilities `yaml:"capabilities,omitempty" json:"capabilities,omitempty"`
}

// Capabilities is the hand-written capability profile of a cluster, filled from `shunt probe`
// output (docs/DESIGN.md decision 3; auto-emit is P3b). Unset booleans default to true, the
// safe assumption that the backend enforces its own checks.
type Capabilities struct {
	EnforcesSHA256  *bool `yaml:"enforces_sha256,omitempty" json:"enforces_sha256,omitempty"`   // rejects a hex x-amz-content-sha256 that does not match the body
	UnsignedTrailer *bool `yaml:"unsigned_trailer,omitempty" json:"unsigned_trailer,omitempty"` // accepts STREAMING-UNSIGNED-PAYLOAD-TRAILER
	// ConditionalWrite: honors If-None-Match: * on PUT. The mover's overwrite guard depends on it
	// (ADR-0004); a backend that ignores it forces the weaker HEAD-then-commit guard.
	ConditionalWrite *bool `yaml:"conditional_write,omitempty" json:"conditional_write,omitempty"`
	// ConditionalDelete: honors If-Match on DeleteObject, refusing a mismatch with 412 and leaving
	// the object. The mover withdraws its own copy with it (ADR-0004 race 1). Unlike the others it
	// defaults to false: a backend that ignores the header deletes unconditionally, so assuming
	// support where there is none deletes a client's newer write.
	ConditionalDelete *bool `yaml:"conditional_delete,omitempty" json:"conditional_delete,omitempty"`
}

// EnforcesSHA256Or reports the capability with the default applied.
func (c Capabilities) EnforcesSHA256Or(def bool) bool {
	if c.EnforcesSHA256 == nil {
		return def
	}
	return *c.EnforcesSHA256
}

// UnsignedTrailerOr reports the capability with the default applied.
func (c Capabilities) UnsignedTrailerOr(def bool) bool {
	if c.UnsignedTrailer == nil {
		return def
	}
	return *c.UnsignedTrailer
}

// ConditionalWriteOr reports the capability with the default applied.
func (c Capabilities) ConditionalWriteOr(def bool) bool {
	if c.ConditionalWrite == nil {
		return def
	}
	return *c.ConditionalWrite
}

// ConditionalDeleteOr reports the capability with the default applied.
func (c Capabilities) ConditionalDeleteOr(def bool) bool {
	if c.ConditionalDelete == nil {
		return def
	}
	return *c.ConditionalDelete
}

// ClusterTLS is the upstream TLS configuration for an https cluster.
type ClusterTLS struct {
	CA                 string `yaml:"ca" json:"ca,omitempty"`
	InsecureSkipVerify bool   `yaml:"insecure_skip_verify" json:"insecure_skip_verify,omitempty"`
}

// Credentials are the cluster's own S3 credentials. Secrets are referenced, never inlined.
type Credentials struct {
	AccessKey string `yaml:"access_key" json:"access_key,omitempty"`
	SecretRef string `yaml:"secret_ref" json:"secret_ref,omitempty"` // env:NAME or file:/path
}

// Telemetry configures the access log and slow ring.
type Telemetry struct {
	// LogFormat is how serve writes its own log: "console" (one short line per event, for a
	// person at a terminal), "json" (for machines), or "auto" (console when stderr is a terminal,
	// json otherwise). The access log is always JSON.
	LogFormat string    `yaml:"log_format"`
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

// Defaults applied before validation. Everything that is safe to default is here; scheme is not.
const (
	defaultListenerAddress = ":443"
	defaultAdminAddress    = "127.0.0.1:9900"
	defaultCopyBufferBytes = 256 << 10
	defaultIdleTimeout     = 60 * time.Second
	defaultMetadataTimeout = 30 * time.Second
	defaultDrainTimeout    = 30 * time.Second
	defaultClockSkew       = 15 * time.Minute
	defaultSlowRingSize    = 100
	defaultSlowThreshold   = 500 * time.Millisecond
	defaultMinTLSVersion   = "1.2"
	defaultPollInterval    = time.Second
)

// errEmpty is returned when the input has no YAML document.
var errEmpty = errors.New("config: empty document")

// Load reads, parses, applies defaults, and validates a config file.
func Load(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close() //nolint:errcheck // read-only file
	return read(f)
}

// read parses, applies defaults, and validates a config from r.
func read(r io.Reader) (*Config, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	return Parse(data)
}

// Parse parses, applies defaults, and validates a config from bytes.
func Parse(data []byte) (*Config, error) {
	if len(bytes.TrimSpace(data)) == 0 {
		return nil, errEmpty
	}
	var c Config
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&c); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, errEmpty
		}
		return nil, decodeError(data, err)
	}
	c.applyDefaults()
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

func decodeError(data []byte, err error) error { return KeyedDecodeError("config", data, err) }

// KeyedDecodeError turns the YAML library's positional errors into errors that name the key path
// (e.g. `proxy.idle_timeout`), so check-config output points at the offending key, not a line
// number. what prefixes errors that carry no position. The directory file uses it too.
func KeyedDecodeError(what string, data []byte, err error) error {
	var le *yaml.LoadErrors
	if !errors.As(err, &le) || len(le.Errors) == 0 {
		return fmt.Errorf("%s: %w", what, err)
	}
	var root yaml.Node
	if yaml.Unmarshal(data, &root) != nil {
		return fmt.Errorf("%s: %w", what, err)
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
		c.Listener.Address = defaultListenerAddress
	}
	if c.Listener.TLS.MinVersion == "" {
		c.Listener.TLS.MinVersion = defaultMinTLSVersion
	}
	if c.Admin.Address == "" {
		c.Admin.Address = defaultAdminAddress
	}
	if c.Directory.File != "" && c.Directory.SecretsDir == "" {
		c.Directory.SecretsDir = filepath.Join(filepath.Dir(c.Directory.File), "secrets")
	}
	if c.Auth.ClockSkew == 0 {
		c.Auth.ClockSkew = defaultClockSkew
	}
	if c.Proxy.CopyBufferBytes == 0 {
		c.Proxy.CopyBufferBytes = defaultCopyBufferBytes
	}
	if c.Proxy.IdleTimeout == 0 {
		c.Proxy.IdleTimeout = defaultIdleTimeout
	}
	if c.Proxy.MetadataTimeout == 0 {
		c.Proxy.MetadataTimeout = defaultMetadataTimeout
	}
	if c.Proxy.DrainTimeout == 0 {
		c.Proxy.DrainTimeout = defaultDrainTimeout
	}
	if c.Directory.File != "" && c.Directory.PollInterval == 0 {
		c.Directory.PollInterval = defaultPollInterval
	}
	if c.Telemetry.LogFormat == "" {
		c.Telemetry.LogFormat = "auto"
	}
	if c.Telemetry.Slow.RingSize == 0 {
		c.Telemetry.Slow.RingSize = defaultSlowRingSize
	}
	if c.Telemetry.Slow.Threshold == 0 {
		c.Telemetry.Slow.Threshold = defaultSlowThreshold
	}
	ApplyClusterDefaults(c.Clusters)
}

// ApplyClusterDefaults fills the defaults of every cluster in m in place: endpoint_mode static.
// The config (passthrough) and the directory file (resign, since POC-5) both hold clusters.
func ApplyClusterDefaults(m map[string]Cluster) {
	for name := range m {
		if m[name].EndpointMode == "" {
			cl := m[name]
			cl.EndpointMode = "static"
			m[name] = cl
		}
	}
}
