package main

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	"go.yaml.in/yaml/v4"

	"github.com/blakegolliher/shunt/internal/auth"
	"github.com/blakegolliher/shunt/internal/config"
)

// labConfig prepares a state directory for `shunt serve --plaintext` and returns the config it
// implies (ADR-0010). On first start it creates the directory file, a secrets directory, and one
// client key for the default tenant; later starts reuse them. The client's secret is never logged:
// `shunt client show` reads it from the 0600 file.
func labConfig(stateDir, listen, admin string, notes io.Writer) (*config.Config, error) {
	dir, err := filepath.Abs(stateDir)
	if err != nil {
		return nil, err
	}
	for _, d := range []string{dir, filepath.Join(dir, "secrets")} {
		if mkErr := os.MkdirAll(d, 0o700); mkErr != nil {
			return nil, fmt.Errorf("state directory: %w", mkErr)
		}
	}
	directoryFile := filepath.Join(dir, "directory.yaml")
	if _, statErr := os.Stat(directoryFile); errors.Is(statErr, fs.ErrNotExist) {
		if writeErr := os.WriteFile(directoryFile, []byte("version: 1\n"), 0o600); writeErr != nil {
			return nil, fmt.Errorf("state directory: %w", writeErr)
		}
	}
	credentialsFile := filepath.Join(dir, "credentials.yaml")
	if _, statErr := os.Stat(credentialsFile); errors.Is(statErr, fs.ErrNotExist) {
		ak, keyErr := newClientKey(credentialsFile)
		if keyErr != nil {
			return nil, fmt.Errorf("state directory: %w", keyErr)
		}
		_, _ = fmt.Fprintf(notes, "shunt: created a client key %s in %s; `shunt client show --state-dir %s` prints its secret\n", ak, credentialsFile, stateDir)
	}
	yamlText := fmt.Sprintf("listener: { address: %q, plaintext: true }\nadmin: { address: %q }\n"+
		"auth: { mode: resign, credentials_file: %q }\ndirectory: { file: %q, poll_interval: 1s }\n",
		listen, admin, credentialsFile, directoryFile)
	cfg, err := config.Parse([]byte(yamlText))
	if err != nil {
		return nil, fmt.Errorf("lab config: %w", err)
	}
	return cfg, nil
}

// newClientKey writes a credentials file holding one generated key for the default tenant.
func newClientKey(path string) (string, error) {
	var id [10]byte
	var secret [30]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", err
	}
	if _, err := rand.Read(secret[:]); err != nil {
		return "", err
	}
	ak := "SHUNT" + strings.ToUpper(hex.EncodeToString(id[:]))
	f := auth.File{Credentials: []auth.Entry{{AccessKey: ak, Secret: base64.RawURLEncoding.EncodeToString(secret[:])}}}
	b, err := yaml.Dump(f, yaml.WithIndent(2))
	if err != nil {
		return "", err
	}
	header := "# shunt client keys. Keys with no tenant belong to the default tenant. Keep this file 0600.\n"
	return ak, os.WriteFile(path, append([]byte(header), b...), 0o600)
}

func newClient() *cobra.Command {
	cmd := &cobra.Command{Use: "client", Short: "The client keys of a lab shunt's state directory"}
	var stateDir string
	show := &cobra.Command{
		Use:   "show",
		Short: "Print the client keys from the state directory, secrets included",
		Long: "Reads <state-dir>/credentials.yaml on this host (it is 0600, so only its owner can) and prints each key\n" +
			"as access_key=... secret=... tenant=..., for aws-cli or any S3 client pointed at shunt.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			path := filepath.Join(stateDir, "credentials.yaml")
			b, err := os.ReadFile(path) //nolint:gosec // an operator-chosen state directory
			if err != nil {
				return fmt.Errorf("%w (start `shunt serve --plaintext --state-dir %s` first)", err, stateDir)
			}
			var f auth.File
			if err := yaml.Unmarshal(b, &f); err != nil {
				return fmt.Errorf("%s: %w", path, err)
			}
			out := cmd.OutOrStdout()
			for _, e := range f.Credentials {
				tenant := e.Tenant
				if tenant == "" {
					tenant = "default"
				}
				_, _ = fmt.Fprintf(out, "access_key=%s\nsecret=%s\ntenant=%s\n", e.AccessKey, e.Secret, tenant)
			}
			return nil
		},
	}
	show.Flags().StringVar(&stateDir, "state-dir", "shunt-data", "the state directory `shunt serve --plaintext` uses")
	cmd.AddCommand(show)
	return cmd
}
