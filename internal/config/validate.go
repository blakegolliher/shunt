package config

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"slices"
	"sort"
	"strings"
)

var (
	authModes      = []string{"passthrough", "resign"}
	clusterTypes   = []string{"vast", "minio", "aws", "s3"}
	schemes        = []string{"https", "http"}
	endpointModes  = []string{"static", "dns"}
	storageClasses = []string{"native", "emulated"}
	accessModes    = []string{"instant", "restore-required"}
	tlsVersions    = []string{"1.2", "1.3"}
	// archiveClasses are the only storage classes that may pair with access: restore-required (§2.6).
	archiveClasses = []string{"GLACIER", "DEEP_ARCHIVE"}
)

// Error is one validation failure. Key is the dotted path of the offending key.
type Error struct {
	Key string
	Msg string
}

func (e *Error) Error() string { return e.Key + ": " + e.Msg }

type errs []error

func (e *errs) add(key, format string, args ...any) {
	*e = append(*e, &Error{Key: key, Msg: fmt.Sprintf(format, args...)})
}

func (e *errs) oneOf(key, val string, allowed []string) bool {
	if slices.Contains(allowed, val) {
		return true
	}
	if val == "" {
		e.add(key, "required; one of %s", strings.Join(allowed, "|"))
	} else {
		e.add(key, "%q is not one of %s", val, strings.Join(allowed, "|"))
	}
	return false
}

// Validate checks structure and references. It returns every failure joined, each naming its key.
// Tenants and placements live in the directory file and are validated by internal/directory
// against these clusters (check-config does both).
func (c *Config) Validate() error {
	var e errs
	c.validateListener(&e)
	c.validateAdmin(&e)
	c.validateAuth(&e)
	c.validateProxy(&e)
	c.validateDirectory(&e)
	c.validateControl(&e)
	c.validateTelemetry(&e)
	c.validateClusters(&e)
	if len(e) == 0 {
		return nil
	}
	return errors.Join(e...)
}

func (c *Config) validateListener(e *errs) {
	l := c.Listener
	if l.Address == "" {
		e.add("listener.address", "required")
	}
	hasDefault := l.TLS.Cert != "" || l.TLS.Key != ""
	if l.Plaintext {
		if hasDefault || len(l.TLS.SNI) > 0 {
			e.add("listener.tls", "not allowed with listener.plaintext: true; a listener is TLS or plaintext, never both")
		}
		return
	}
	if hasDefault && (l.TLS.Cert == "" || l.TLS.Key == "") {
		e.add("listener.tls", "cert and key must both be set")
	}
	if !hasDefault && len(l.TLS.SNI) == 0 {
		e.add("listener.tls.cert", "required (or at least one listener.tls.sni entry; a lab without TLS states listener.plaintext: true)")
	}
	e.oneOf("listener.tls.min_version", l.TLS.MinVersion, tlsVersions)
	for _, name := range sortedKeys(l.TLS.SNI) {
		p := l.TLS.SNI[name]
		if p.Cert == "" || p.Key == "" {
			e.add("listener.tls.sni."+name, "cert and key are both required")
		}
	}
	for i, d := range l.Domains {
		if d == "" {
			e.add(fmt.Sprintf("listener.domains[%d]", i), "empty")
		}
	}
}

func (c *Config) validateAdmin(e *errs) {
	if ref := c.Admin.ControlTokenRef; ref != "" && !strings.HasPrefix(ref, "env:") && !strings.HasPrefix(ref, "file:") {
		e.add("admin.control_token_ref", "%q must start with env: or file:; the token itself never goes in the config", ref)
	}
}

func (c *Config) validateAuth(e *errs) {
	a := c.Auth
	if !e.oneOf("auth.mode", a.Mode, authModes) {
		return
	}
	if a.Mode == "resign" && a.CredentialsFile == "" && !c.Control.Member() {
		e.add("auth.credentials_file", "required when auth.mode is resign")
	}
	if a.Mode == "passthrough" && a.CredentialsFile != "" {
		e.add("auth.credentials_file", "not used in passthrough mode; remove it")
	}
	if a.ClockSkew < 0 {
		e.add("auth.clock_skew", "must not be negative")
	}
}

func (c *Config) validateProxy(e *errs) {
	p := c.Proxy
	switch {
	case c.Auth.Mode == "resign" && p.Cluster != "":
		e.add("proxy.cluster", "not used in resign mode; placements in directory.file choose the cluster per bucket")
	case c.Auth.Mode == "resign":
	case p.Cluster == "":
		e.add("proxy.cluster", "required in passthrough mode; the name of the clusters: entry to forward to")
	case len(c.Clusters) > 0:
		if _, ok := c.Clusters[p.Cluster]; !ok {
			e.add("proxy.cluster", "unknown cluster %q", p.Cluster)
		}
	}
	if p.CopyBufferBytes < 4096 {
		e.add("proxy.copy_buffer_bytes", "must be at least 4096, got %d", p.CopyBufferBytes)
	}
	if p.IdleTimeout <= 0 {
		e.add("proxy.idle_timeout", "must be positive")
	}
	if p.MetadataTimeout <= 0 {
		e.add("proxy.metadata_timeout", "must be positive")
	}
	if p.DrainTimeout <= 0 {
		e.add("proxy.drain_timeout", "must be positive")
	}
}

func (c *Config) validateDirectory(e *errs) {
	d := c.Directory
	switch c.Auth.Mode {
	case "resign":
		if d.File == "" && !c.Control.Member() {
			e.add("directory.file", "required in resign mode: the tenants and placements that route each bucket (or control.endpoints, for a fleet member)")
		}
	case "passthrough":
		if d.File != "" {
			e.add("directory.file", "not used in passthrough mode (the client's signature fixes the bucket name); remove it")
		}
	}
	if d.PollInterval < 0 {
		e.add("directory.poll_interval", "must not be negative")
	}
}

var proxyIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

// ValidProxyID reports whether id can name a fleet member (control.proxy_id, ADR-0016): 1-64
// letters, digits, '.', '_' or '-', starting with a letter or digit.
func ValidProxyID(id string) bool { return proxyIDPattern.MatchString(id) }

func (c *Config) validateControl(e *errs) {
	ct := c.Control
	for i, ep := range ct.Endpoints {
		u, err := url.Parse(ep)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || (u.Path != "" && u.Path != "/") {
			e.add(fmt.Sprintf("control.endpoints[%d]", i), "want a shunt-control API address as http(s)://host:port, got %q", ep)
		} else if u.Scheme == "http" && !ct.Plaintext {
			e.add(fmt.Sprintf("control.endpoints[%d]", i), "is http: the control channel would carry cluster secrets and client keys in the clear; state control.plaintext: true to accept that (TLS for it is deferred, ADR-0015)")
		}
	}
	if ct.Member() {
		if c.Auth.Mode != "resign" {
			e.add("control.endpoints", "a fleet member routes by the control plane's directory, which only resign mode has")
		}
		if c.Directory.File != "" {
			e.add("directory.file", "a fleet member takes its directory from the control plane (control.endpoints); remove directory")
		}
		if c.Auth.CredentialsFile != "" {
			e.add("auth.credentials_file", "a fleet member takes its client keys from the control plane (control.endpoints); remove it")
		}
		if ct.CacheDir == "" {
			e.add("control.cache_dir", "required for a fleet member: where the last installed directory is kept for a restart with the control plane down")
		}
	} else {
		for _, k := range []struct {
			set bool
			key string
		}{{ct.TokenRef != "", "control.token_ref"}, {ct.Plaintext, "control.plaintext"}, {ct.ProxyID != "", "control.proxy_id"}, {ct.CacheDir != "", "control.cache_dir"}} {
			if k.set {
				e.add(k.key, "only used with control.endpoints")
			}
		}
	}
	if ref := ct.TokenRef; ref != "" && !strings.HasPrefix(ref, "env:") && !strings.HasPrefix(ref, "file:") {
		e.add("control.token_ref", "must be env:NAME or file:/path; a token is never inline")
	}
	if ct.ProxyID != "" && !ValidProxyID(ct.ProxyID) {
		e.add("control.proxy_id", "want 1-64 letters, digits, '.', '_' or '-', starting with a letter or digit; got %q", ct.ProxyID)
	}
	if ct.HeartbeatInterval <= 0 {
		e.add("control.heartbeat_interval", "must be positive")
	}
	if ct.LeaseTTL < 3*ct.HeartbeatInterval {
		e.add("control.lease_ttl", "must be at least three heartbeat intervals (%v), got %v: one lost heartbeat must not make a proxy stale", 3*ct.HeartbeatInterval, ct.LeaseTTL)
	}
}

func (c *Config) validateTelemetry(e *errs) {
	switch c.Telemetry.LogFormat {
	case "auto", "json", "console":
	default:
		e.add("telemetry.log_format", "must be one of auto|json|console, got %q", c.Telemetry.LogFormat)
	}
	if c.Telemetry.Slow.RingSize <= 0 {
		e.add("telemetry.slow.ring_size", "must be positive")
	}
	if c.Telemetry.Slow.Threshold <= 0 {
		e.add("telemetry.slow.threshold", "must be positive")
	}
}

func (c *Config) validateClusters(e *errs) {
	if c.Auth.Mode == "resign" {
		if len(c.Clusters) > 0 {
			e.add("clusters", "not allowed in resign mode: clusters live in the directory file (directory.file) since POC-5, where shunt cluster add changes them without a restart")
		}
		return
	}
	if len(c.Clusters) == 0 {
		e.add("clusters", "at least one cluster is required")
		return
	}
	validateClusters(e, "clusters", c.Clusters)
}

// ValidateClusters checks cluster definitions wherever they are held: the config in passthrough
// mode, the directory file in resign mode. Every failure names its key under prefix. Call
// ApplyClusterDefaults first.
func ValidateClusters(prefix string, clusters map[string]Cluster) error {
	var e errs
	validateClusters(&e, prefix, clusters)
	if len(e) == 0 {
		return nil
	}
	return errors.Join(e...)
}

func validateClusters(e *errs, prefix string, clusters map[string]Cluster) {
	for _, name := range sortedKeys(clusters) {
		k := prefix + "." + name
		cl := clusters[name]
		e.oneOf(k+".type", cl.Type, clusterTypes)
		e.oneOf(k+".scheme", cl.Scheme, schemes)
		if cl.Region == "" {
			e.add(k+".region", "required")
		}
		if cl.Barrier != nil {
			if err := cl.Barrier.Validate(); err != nil {
				e.add(k+".barrier", "%v", err)
			} else if cl.Barrier.Kind != BarrierMutations {
				e.add(k+".barrier.kind", "a cluster barrier closes mutations; got %q", cl.Barrier.Kind)
			}
		}
		if e.oneOf(k+".endpoint_mode", cl.EndpointMode, endpointModes) {
			switch cl.EndpointMode {
			case "static":
				if len(cl.Endpoints) == 0 {
					e.add(k+".endpoints", "required in static endpoint_mode")
				}
				if cl.Endpoint != "" {
					e.add(k+".endpoint", "not allowed in static endpoint_mode; use endpoints")
				}
			case "dns":
				if cl.Endpoint == "" {
					e.add(k+".endpoint", "required in dns endpoint_mode")
				}
				if len(cl.Endpoints) != 0 {
					e.add(k+".endpoints", "not allowed in dns endpoint_mode; use endpoint")
				}
			}
		}
		for i, ep := range cl.Endpoints {
			if strings.Contains(ep, "://") {
				e.add(fmt.Sprintf("%s.endpoints[%d]", k, i), "host:port only; the scheme comes from %s.scheme", k)
			}
		}
		if cl.Scheme == "http" && (cl.TLS.CA != "" || cl.TLS.InsecureSkipVerify) {
			e.add(k+".tls", "set but scheme is http")
		}
		if cl.Credentials.AccessKey == "" {
			e.add(k+".credentials.access_key", "required")
		}
		if ref := cl.Credentials.SecretRef; ref == "" {
			e.add(k+".credentials.secret_ref", "required (env:NAME or file:/path)")
		} else if !strings.HasPrefix(ref, "env:") && !strings.HasPrefix(ref, "file:") && !strings.HasPrefix(ref, "control:") {
			e.add(k+".credentials.secret_ref", "%q must start with env:, file:, or control: (a secret the control plane stores, ADR-0015)", ref)
		}
		if cl.StorageClasses != "" {
			e.oneOf(k+".storage_classes", cl.StorageClasses, storageClasses)
		}
		validateStorageClassPairing(e, k, cl)
	}
}

// validateStorageClassPairing enforces §2.3: an emulated cold cluster states storage_class and
// access together, and restore-required is legal only with GLACIER or DEEP_ARCHIVE.
func validateStorageClassPairing(e *errs, k string, cl Cluster) {
	switch {
	case cl.StorageClass == "" && cl.Access == "":
		return
	case cl.StorageClass == "":
		e.add(k+".storage_class", "required when access is set")
		return
	case cl.Access == "":
		e.add(k+".access", "required when storage_class is set; one of %s", strings.Join(accessModes, "|"))
		return
	}
	if !e.oneOf(k+".access", cl.Access, accessModes) {
		return
	}
	if cl.Access == "restore-required" && !slices.Contains(archiveClasses, cl.StorageClass) {
		e.add(k+".access", "restore-required is only legal with storage_class %s, got %q",
			strings.Join(archiveClasses, "|"), cl.StorageClass)
	}
	if cl.StorageClasses == "native" {
		e.add(k+".storage_class", "not allowed on a storage_classes: native cluster; the backend owns its classes")
	}
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
