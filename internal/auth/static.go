package auth

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"slices"
	"strings"

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

// Static is an in-memory CredentialStore loaded once from a file.
type Static struct {
	byKey map[string]sigv4.Credential
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
	return parse(data)
}

// parse builds a Static store from YAML bytes. Unknown keys are errors.
func parse(data []byte) (*Static, error) {
	var f File
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&f); err != nil {
		return nil, fmt.Errorf("auth: credentials file: %w", err)
	}
	s := &Static{byKey: make(map[string]sigv4.Credential, len(f.Credentials))}
	for i, e := range f.Credentials {
		k := fmt.Sprintf("credentials[%d]", i)
		if e.AccessKey == "" {
			return nil, fmt.Errorf("auth: %s.access_key: required", k)
		}
		if e.Tenant == "" {
			e.Tenant = directory.DefaultTenant
		}
		if _, dup := s.byKey[e.AccessKey]; dup {
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
		s.byKey[e.AccessKey] = sigv4.Credential{AccessKey: e.AccessKey, Secret: secret, Tenant: e.Tenant, Buckets: e.Buckets}
	}
	return s, nil
}

// Lookup implements sigv4.CredentialStore.
func (s *Static) Lookup(_ context.Context, accessKey string) (sigv4.Credential, error) {
	c, ok := s.byKey[accessKey]
	if !ok {
		return sigv4.Credential{}, sigv4.ErrUnknownAccessKey
	}
	return c, nil
}

// Len returns the number of credentials.
func (s *Static) Len() int { return len(s.byKey) }

// Tenant returns a tenant's credentials, secrets included, ordered by access key. The control API's
// step-out check signs with them to ask a cluster what those clients could do without shunt.
func (s *Static) Tenant(tenant string) []sigv4.Credential {
	var out []sigv4.Credential
	for _, c := range s.byKey {
		if c.Tenant == tenant {
			out = append(out, c)
		}
	}
	slices.SortFunc(out, func(a, b sigv4.Credential) int { return strings.Compare(a.AccessKey, b.AccessKey) })
	return out
}
