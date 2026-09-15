package config

import (
	"errors"
	"fmt"
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
	tierModes      = []string{"native", "emulated"}
	tlsVersions    = []string{"1.2", "1.3"}
	states         = []string{StateActive, StateRamping, StateMigrating, StateCutover}
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
// State-transition legality (e.g. refusing MIGRATING on a versioned source) is checked at
// transition time by the directory, not here: the validator only sees structure.
func (c *Config) Validate() error {
	var e errs
	c.validateListener(&e)
	c.validateAuth(&e)
	c.validateProxy(&e)
	c.validateTelemetry(&e)
	c.validateClusters(&e)
	c.validateTenants(&e)
	c.validatePlacements(&e)
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
	if hasDefault && (l.TLS.Cert == "" || l.TLS.Key == "") {
		e.add("listener.tls", "cert and key must both be set")
	}
	if !hasDefault && len(l.TLS.SNI) == 0 {
		e.add("listener.tls.cert", "required (or at least one listener.tls.sni entry)")
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

func (c *Config) validateAuth(e *errs) {
	a := c.Auth
	if !e.oneOf("auth.mode", a.Mode, authModes) {
		return
	}
	if a.Mode == "resign" && a.CredentialsFile == "" {
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
	case p.Cluster == "":
		e.add("proxy.cluster", "required; the name of the clusters: entry to forward to")
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

func (c *Config) validateTelemetry(e *errs) {
	if c.Telemetry.Slow.RingSize <= 0 {
		e.add("telemetry.slow.ring_size", "must be positive")
	}
	if c.Telemetry.Slow.Threshold <= 0 {
		e.add("telemetry.slow.threshold", "must be positive")
	}
}

func (c *Config) validateClusters(e *errs) {
	if len(c.Clusters) == 0 {
		e.add("clusters", "at least one cluster is required")
		return
	}
	for _, name := range sortedKeys(c.Clusters) {
		k := "clusters." + name
		cl := c.Clusters[name]
		e.oneOf(k+".type", cl.Type, clusterTypes)
		e.oneOf(k+".scheme", cl.Scheme, schemes)
		if cl.Region == "" {
			e.add(k+".region", "required")
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
		} else if !strings.HasPrefix(ref, "env:") && !strings.HasPrefix(ref, "file:") {
			e.add(k+".credentials.secret_ref", "%q must start with env: or file:", ref)
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

func (c *Config) validateTenants(e *errs) {
	for _, name := range sortedKeys(c.Tenants) {
		k := "tenants." + name
		t := c.Tenants[name]
		if t.DefaultCluster == "" {
			e.add(k+".default_cluster", "required")
		} else if _, ok := c.Clusters[t.DefaultCluster]; !ok {
			e.add(k+".default_cluster", "unknown cluster %q", t.DefaultCluster)
		}
	}
}

func (c *Config) validatePlacements(e *errs) {
	for _, key := range sortedKeys(c.Placements) {
		k := "placements." + key
		p := c.Placements[key]
		tenant, bucket, ok := strings.Cut(key, "/")
		if !ok || tenant == "" || bucket == "" {
			e.add(k, "key must be <tenant>/<bucket>")
		} else if _, found := c.Tenants[tenant]; !found {
			e.add(k, "unknown tenant %q", tenant)
		}
		if !e.oneOf(k+".state", p.State, states) {
			continue
		}
		c.ref(e, k+".primary", p.Primary, true)
		migrating := p.State != StateActive
		switch {
		case migrating && p.Source == "":
			e.add(k+".source", "required in state %s", p.State)
		case !migrating && p.Source != "":
			e.add(k+".source", "not allowed in state ACTIVE")
		case p.Source != "":
			c.ref(e, k+".source", p.Source, true)
			if p.Source == p.Primary {
				e.add(k+".source", "must differ from primary")
			}
		}
		if p.Ramp != nil {
			if p.State != StateRamping {
				e.add(k+".ramp", "only allowed in state RAMPING")
			}
			if p.Ramp.Ratio < 0 || p.Ramp.Ratio > 1 {
				e.add(k+".ramp.ratio", "must be within [0, 1], got %v", p.Ramp.Ratio)
			}
			if p.Ramp.Ratio == 0 && len(p.Ramp.Prefixes) == 0 {
				e.add(k+".ramp", "needs a ratio or at least one prefix")
			}
		} else if p.State == StateRamping {
			e.add(k+".ramp", "required in state RAMPING")
		}
		for _, cl := range sortedKeys(p.Names) {
			c.ref(e, k+".names."+cl, cl, true)
			if p.Names[cl] == "" {
				e.add(k+".names."+cl, "backend bucket name is empty")
			}
		}
		for _, cl := range []string{p.Primary, p.Source, p.Cold} {
			if cl == "" {
				continue
			}
			if _, ok := c.Clusters[cl]; !ok {
				continue // already reported
			}
			if _, ok := p.Names[cl]; !ok {
				e.add(k+".names", "missing backend bucket name for cluster %q", cl)
			}
		}
		c.validateTier(e, k, p)
	}
}

func (c *Config) validateTier(e *errs, k string, p Placement) {
	if p.Tier != "" && !e.oneOf(k+".tier", p.Tier, tierModes) {
		return
	}
	switch p.Tier {
	case "":
		if p.Cold != "" {
			e.add(k+".tier", "required when cold is set; one of %s", strings.Join(tierModes, "|"))
		}
		if p.Lifecycle != "" {
			e.add(k+".tier", "required when lifecycle is set")
		}
	case "native":
		if p.Cold != "" {
			e.add(k+".cold", "not allowed with tier: native; the backend owns its classes")
		}
		if cl, ok := c.Clusters[p.Primary]; ok && cl.StorageClasses != "native" {
			e.add(k+".tier", "native requires clusters.%s.storage_classes: native", p.Primary)
		}
	case "emulated":
		if p.Cold == "" {
			e.add(k+".cold", "required with tier: emulated")
		} else if c.ref(e, k+".cold", p.Cold, true) {
			if cl := c.Clusters[p.Cold]; cl.StorageClass == "" {
				e.add(k+".cold", "cluster %q has no storage_class; an emulated cold cluster must state one", p.Cold)
			}
			if p.Cold == p.Primary {
				e.add(k+".cold", "must differ from primary")
			}
		}
	}
}

// ref reports an error unless name references a known cluster. Returns true when it resolves.
func (c *Config) ref(e *errs, key, name string, required bool) bool {
	if name == "" {
		if required {
			e.add(key, "required")
		}
		return false
	}
	if _, ok := c.Clusters[name]; !ok {
		e.add(key, "unknown cluster %q", name)
		return false
	}
	return true
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
