package config

import (
	"errors"
	"fmt"
	"os"
	"strings"
)

// errSecretRef is returned for a malformed or unresolvable secret reference.
var errSecretRef = errors.New("config: secret reference")

// ResolveSecret turns a `env:NAME` or `file:/path` reference into the secret value. The value is
// never logged by callers; this function only reads it.
func ResolveSecret(ref string) (string, error) {
	switch {
	case strings.HasPrefix(ref, "env:"):
		name := strings.TrimPrefix(ref, "env:")
		v, ok := os.LookupEnv(name)
		if !ok || v == "" {
			return "", fmt.Errorf("%w %q: environment variable %s is unset or empty", errSecretRef, ref, name)
		}
		return v, nil
	case strings.HasPrefix(ref, "file:"):
		path := strings.TrimPrefix(ref, "file:")
		b, err := os.ReadFile(path) //nolint:gosec // operator-configured secret file path
		if err != nil {
			return "", fmt.Errorf("%w %q: %w", errSecretRef, ref, err)
		}
		v := strings.TrimRight(string(b), "\r\n")
		if v == "" {
			return "", fmt.Errorf("%w %q: file is empty", errSecretRef, ref)
		}
		return v, nil
	}
	return "", fmt.Errorf("%w %q: must start with env or file prefix", errSecretRef, ref)
}
