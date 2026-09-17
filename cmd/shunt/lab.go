package main

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	"go.yaml.in/yaml/v4"

	"github.com/blakegolliher/shunt/internal/auth"
	"github.com/blakegolliher/shunt/internal/config"
	"github.com/blakegolliher/shunt/internal/control"
	"github.com/blakegolliher/shunt/internal/directory"
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
	cmd.AddCommand(show, newClientAdd(), newClientRemove())
	return cmd
}

// newClientAdd imports a key clients already use on a cluster, so they keep their own credentials
// while shunt is in front of it, and when shunt leaves again (ADR-0012).
func newClientAdd() *cobra.Command {
	var (
		o       apiOptions
		req     control.ClientKeyRequest
		tenant  string
		buckets []string
	)
	cmd := &cobra.Command{
		Use:   "add <access-key>",
		Short: "Import a client key a cluster already issued, so clients keep their own credentials",
		Long: "Prompts for the secret key (or reads one line from stdin), checks the pair against the cluster,\n" +
			"and stores it in shunt's credentials file. shunt then verifies client signatures with it, so the\n" +
			"clients that use this key on the cluster can use it through shunt unchanged, and can go back to\n" +
			"the cluster unchanged when shunt steps out (shunt step-out).",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			req.AccessKey, req.Buckets = args[0], buckets
			secret, err := readSecret(cmd, "secret key for "+req.AccessKey+": ")
			if err != nil {
				return err
			}
			req.Secret = secret
			api, err := o.client()
			if err != nil {
				return err
			}
			var out control.ClientKeyResult
			if callErr := api.call(cmd.Context(), "POST", "/v1/tenants/"+url.PathEscape(tenant)+"/client-keys", req, &out); callErr != nil {
				return callErr
			}
			if o.json {
				return printJSON(cmd, out)
			}
			where := "stored; it was not checked against a cluster"
			if out.Checked != "" {
				where = out.Checked + " accepts it"
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "client key %s imported: %s, and shunt now verifies clients with it\n", out.AccessKey, where)
			return err
		},
	}
	addAPIFlags(cmd, &o)
	f := cmd.Flags()
	f.StringVar(&tenant, "tenant", directory.DefaultTenant, "the tenant whose clients use this key")
	f.StringVar(&req.Cluster, "check", "", "the `cluster` to check the key against (default: the tenant's default cluster)")
	f.StringSliceVar(&buckets, "buckets", nil, "limit this key to these `buckets` through shunt")
	return cmd
}

// newClientRemove drops a client key shunt holds: one that was replaced, or the key `serve
// --plaintext` generated once the clients' own keys are imported (ADR-0012).
func newClientRemove() *cobra.Command {
	var (
		o      apiOptions
		tenant string
	)
	cmd := &cobra.Command{
		Use:   "remove <access-key>",
		Short: "Drop a client key shunt holds; clients using it are refused from then on",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			api, err := o.client()
			if err != nil {
				return err
			}
			var out control.ClientKeyResult
			if callErr := api.call(cmd.Context(), "DELETE", "/v1/tenants/"+url.PathEscape(tenant)+"/client-keys/"+url.PathEscape(args[0]), nil, &out); callErr != nil {
				return callErr
			}
			if o.json {
				return printJSON(cmd, out)
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "client key %s removed; %d key(s) left for these clients\n", out.AccessKey, out.Left)
			return err
		},
	}
	addAPIFlags(cmd, &o)
	cmd.Flags().StringVar(&tenant, "tenant", directory.DefaultTenant, "the tenant whose key this is")
	return cmd
}
