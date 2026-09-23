package auth

import (
	"bytes"
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"go.yaml.in/yaml/v4"

	"github.com/blakegolliher/shunt/internal/config"
	"github.com/blakegolliher/shunt/internal/directory"
	"github.com/blakegolliher/shunt/internal/sigv4"
)

// File is the credentials file schema (auth.credentials_file).
//
//	credentials:
//	  - access_key: SHUNTAKIAEXAMPLE
//	    secret: inline-secret            # or secret_ref: env:NAME / file:/path
//	    tenant: acme
//	    buckets: [data, logs]            # optional allowlist
type File struct {
	Credentials []Entry `yaml:"credentials"`
}

// Entry is one credential in the file.
type Entry struct {
	AccessKey string   `yaml:"access_key"`
	Secret    string   `yaml:"secret"`
	SecretRef string   `yaml:"secret_ref,omitempty"`
	Tenant    string   `yaml:"tenant,omitempty"` // empty: the default tenant
	Buckets   []string `yaml:"buckets,omitempty"`
}

// Static is a CredentialStore over a credentials file. Reads are lock-free: Lookup runs on every
// request, and a key imported by the control API swaps the whole map behind an atomic pointer
// (ADR-0012). Writes are serialized by mu and rewrite the file.
type Static struct {
	byKey atomic.Pointer[map[string]sigv4.Credential]

	mu      sync.Mutex
	path    string  // the credentials file, when Load read one; "" for a parsed-only store
	entries []Entry // the file's entries as written, secret_refs kept as refs
}

// errPermissions is returned when the credentials file is readable by group or others.
var errPermissions = errors.New("auth: credentials file must not be readable by group or others (chmod 0600)")

// Load reads and validates a credentials file. Inline secrets are allowed because this file is
// the secret store; it must be mode 0600 (encrypted at rest is P3c).
func Load(path string) (*Static, error) {
	st, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("auth: %w", err)
	}
	if st.Mode().Perm()&(fs.ModePerm&0o077) != 0 {
		return nil, fmt.Errorf("%w: %s is %04o", errPermissions, path, st.Mode().Perm())
	}
	data, err := os.ReadFile(path) //nolint:gosec // operator-configured path, validated above
	if err != nil {
		return nil, fmt.Errorf("auth: %w", err)
	}
	st2, err := parse(data)
	if err != nil {
		return nil, err
	}
	st2.path = path
	return st2, nil
}

// parse builds a Static store from YAML bytes. Unknown keys are errors.
func parse(data []byte) (*Static, error) {
	var f File
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&f); err != nil {
		return nil, fmt.Errorf("auth: credentials file: %w", err)
	}
	s := &Static{}
	byKey := make(map[string]sigv4.Credential, len(f.Credentials))
	for i, e := range f.Credentials {
		k := fmt.Sprintf("credentials[%d]", i)
		if e.AccessKey == "" {
			return nil, fmt.Errorf("auth: %s.access_key: required", k)
		}
		if e.Tenant == "" {
			e.Tenant = directory.DefaultTenant
		}
		if _, dup := byKey[e.AccessKey]; dup {
			return nil, fmt.Errorf("auth: %s.access_key: duplicate access key", k)
		}
		secret := e.Secret
		switch {
		case secret != "" && e.SecretRef != "":
			return nil, fmt.Errorf("auth: %s: set secret or secret_ref, not both", k)
		case secret == "" && e.SecretRef == "":
			return nil, fmt.Errorf("auth: %s: secret or secret_ref required", k)
		case secret == "":
			v, err := config.ResolveSecret(e.SecretRef)
			if err != nil {
				return nil, fmt.Errorf("auth: %s.secret_ref: %w", k, err)
			}
			secret = v
		}
		byKey[e.AccessKey] = sigv4.Credential{AccessKey: e.AccessKey, Secret: secret, Tenant: e.Tenant, Buckets: e.Buckets}
	}
	s.entries = f.Credentials
	s.byKey.Store(&byKey)
	return s, nil
}

// Lookup implements sigv4.CredentialStore.
func (s *Static) Lookup(_ context.Context, accessKey string) (sigv4.Credential, error) {
	c, ok := (*s.byKey.Load())[accessKey]
	if !ok {
		return sigv4.Credential{}, sigv4.ErrUnknownAccessKey
	}
	return c, nil
}

// Len returns the number of credentials.
func (s *Static) Len() int { return len(*s.byKey.Load()) }

// errNoFile is returned by Add on a store that was not loaded from a file.
var errNoFile = errors.New("auth: this shunt has no credentials file to write to")

// ErrDuplicateKey is returned by Add for an access key the store already holds under another
// secret or tenant.
var ErrDuplicateKey = errors.New("auth: the credentials file already holds this access key")

// Merge is what adding c does to held, a key the store already has under the same access key
// (ADR-0012). The same secret and tenant is the same client key imported again, for another bucket
// or another cluster, and succeeds: its bucket allowlist becomes the union of both, and no
// allowlist on either side means every bucket of the tenant. A different secret or tenant is a
// different key under a name already in use, and is refused with ErrDuplicateKey.
func Merge(held, c sigv4.Credential) (sigv4.Credential, error) {
	tenant := c.Tenant
	if tenant == "" {
		tenant = directory.DefaultTenant
	}
	switch {
	case held.Tenant != tenant:
		return held, fmt.Errorf("%w: for tenant %s", ErrDuplicateKey, held.Tenant)
	case subtle.ConstantTimeCompare([]byte(held.Secret), []byte(c.Secret)) != 1:
		return held, fmt.Errorf("%w: with a different secret", ErrDuplicateKey)
	}
	if len(held.Buckets) == 0 || len(c.Buckets) == 0 {
		held.Buckets = nil
		return held, nil
	}
	buckets := slices.Clone(held.Buckets)
	for _, b := range c.Buckets {
		if !slices.Contains(buckets, b) {
			buckets = append(buckets, b)
		}
	}
	held.Buckets = buckets
	return held, nil
}

// Add stores one client key and rewrites the credentials file with it, atomically and 0600. The
// proxy uses the key on the next request: shunt holds the keys clients already have, so inserting
// shunt and removing it again cost no client change (docs/DESIGN.md §11, ADR-0012).
//
// The file is rewritten from the entries shunt parsed, so a secret_ref stays a ref and an inline
// secret stays inline, but comments and formatting do not survive.
func (s *Static) Add(c sigv4.Credential) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.path == "" {
		return errNoFile
	}
	if c.AccessKey == "" || c.Secret == "" {
		return errors.New("auth: a client key needs an access key and a secret")
	}
	tenant := c.Tenant
	if tenant == "" {
		tenant = directory.DefaultTenant
	}
	var entries []Entry
	if held, dup := (*s.byKey.Load())[c.AccessKey]; dup {
		merged, err := Merge(held, c)
		if err != nil {
			return err
		}
		if slices.Equal(merged.Buckets, held.Buckets) {
			return nil // imported again, for a bucket it already reaches
		}
		c.Buckets = merged.Buckets
		entries = slices.Clone(s.entries)
		for i := range entries {
			if entries[i].AccessKey == c.AccessKey {
				entries[i].Buckets = c.Buckets // the secret or secret_ref stays as it was written
			}
		}
	} else {
		entries = append(slices.Clone(s.entries), Entry{AccessKey: c.AccessKey, Secret: c.Secret, Tenant: tenant, Buckets: c.Buckets})
	}
	body, err := yaml.Marshal(File{Credentials: entries})
	if err != nil {
		return fmt.Errorf("auth: %w", err)
	}
	if err := writeFileAtomically(s.path, append([]byte("# shunt writes this file when a client key is imported or removed; comments are not kept.\n"), body...)); err != nil {
		return err
	}
	next := maps.Clone(*s.byKey.Load())
	next[c.AccessKey] = sigv4.Credential{AccessKey: c.AccessKey, Secret: c.Secret, Tenant: tenant, Buckets: c.Buckets}
	s.entries = entries
	s.byKey.Store(&next)
	return nil
}

// ErrUnknownKey is returned by Remove for a key the store does not hold.
var ErrUnknownKey = errors.New("auth: no such access key in the credentials file")

// Remove drops one client key and rewrites the credentials file without it: the other half of Add,
// for a key that has been replaced or was never used (a lab key after the real ones are imported).
// The proxy refuses that key from the next request on.
func (s *Static) Remove(accessKey string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.path == "" {
		return errNoFile
	}
	if _, ok := (*s.byKey.Load())[accessKey]; !ok {
		return fmt.Errorf("%w: %s", ErrUnknownKey, accessKey)
	}
	entries := slices.DeleteFunc(slices.Clone(s.entries), func(e Entry) bool { return e.AccessKey == accessKey })
	body, err := yaml.Marshal(File{Credentials: entries})
	if err != nil {
		return fmt.Errorf("auth: %w", err)
	}
	if err := writeFileAtomically(s.path, append([]byte("# shunt writes this file when a client key is imported or removed; comments are not kept.\n"), body...)); err != nil {
		return err
	}
	next := maps.Clone(*s.byKey.Load())
	delete(next, accessKey)
	s.entries = entries
	s.byKey.Store(&next)
	return nil
}

// writeFileAtomically writes data to a new 0600 file next to path and renames it over path, so a
// reader never sees a half-written credentials file and the secrets never touch a world-readable one.
func writeFileAtomically(path string, data []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".credentials-*")
	if err != nil {
		return fmt.Errorf("auth: %w", err)
	}
	defer func() { _ = os.Remove(tmp.Name()) }()
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("auth: %w", err)
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("auth: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("auth: %w", err)
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return fmt.Errorf("auth: %w", err)
	}
	return nil
}

// All returns every credential, secrets included, ordered by access key: what the control plane
// delivers to member proxies (internal/cp keeps its own store; a lab proxy answers from this one).
func (s *Static) All() []sigv4.Credential {
	m := *s.byKey.Load()
	out := make([]sigv4.Credential, 0, len(m))
	for _, c := range m {
		out = append(out, c)
	}
	slices.SortFunc(out, func(a, b sigv4.Credential) int { return strings.Compare(a.AccessKey, b.AccessKey) })
	return out
}

// Replace swaps in a whole set of credentials without a file: a member proxy installs the client
// keys the control plane delivered with each directory version (ADR-0015). Add and Remove are
// refused on such a store; keys change through the control plane.
func (s *Static) Replace(creds []sigv4.Credential) {
	next := make(map[string]sigv4.Credential, len(creds))
	for _, c := range creds {
		if c.Tenant == "" {
			c.Tenant = directory.DefaultTenant
		}
		next[c.AccessKey] = c
	}
	s.byKey.Store(&next)
}

// NewEmpty returns a store with no keys and no file, filled by Replace.
func NewEmpty() *Static {
	s := &Static{}
	s.Replace(nil)
	return s
}

// Tenant returns a tenant's credentials, secrets included, ordered by access key. The control API's
// step-out check signs with them to ask a cluster what those clients could do without shunt.
func (s *Static) Tenant(tenant string) []sigv4.Credential {
	var out []sigv4.Credential
	for _, c := range *s.byKey.Load() {
		if c.Tenant == tenant {
			out = append(out, c)
		}
	}
	slices.SortFunc(out, func(a, b sigv4.Credential) int { return strings.Compare(a.AccessKey, b.AccessKey) })
	return out
}
