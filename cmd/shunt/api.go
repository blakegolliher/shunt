package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/blakegolliher/shunt/internal/config"
	"github.com/blakegolliher/shunt/internal/control"
)

// apiOptions are the flags every command that talks to a running shunt takes.
type apiOptions struct {
	url      string
	tokenRef string
	json     bool
}

func addAPIFlags(cmd *cobra.Command, o *apiOptions) {
	def := os.Getenv("SHUNT_API")
	if def == "" {
		def = "http://127.0.0.1:9900"
	}
	f := cmd.Flags()
	f.StringVar(&o.url, "api", def, "the control API: shunt's admin listener (env SHUNT_API)")
	f.StringVar(&o.tokenRef, "token-ref", os.Getenv("SHUNT_API_TOKEN_REF"), "env:NAME or file:/path holding admin.control_token_ref's token (env SHUNT_API_TOKEN_REF)")
	f.BoolVar(&o.json, "json", false, "print the API's JSON answer instead of a summary")
}

// apiClient calls the control API (docs/reference/control-api.md).
type apiClient struct {
	base  string
	token string
	http  *http.Client
}

func (o apiOptions) client() (*apiClient, error) {
	c := &apiClient{base: strings.TrimRight(o.url, "/"), http: &http.Client{}}
	if o.tokenRef != "" {
		tok, err := config.ResolveSecret(o.tokenRef)
		if err != nil {
			return nil, fmt.Errorf("--token-ref: %w", err)
		}
		c.token = tok
	}
	return c, nil
}

// call sends one request. A non-2xx answer becomes an error carrying the API's message.
func (c *apiClient) call(ctx context.Context, method, path string, body, out any) error {
	var rd io.Reader = http.NoBody
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, rd)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("control API %s: %w (is shunt serve running with its admin listener there?)", c.base, err)
	}
	defer resp.Body.Close() //nolint:errcheck // read below
	data, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode >= 300 {
		var e control.Error
		if json.Unmarshal(data, &e) == nil && e.Message != "" {
			if e.Code == "refused" && !strings.HasPrefix(e.Message, "refused") {
				return fmt.Errorf("refused: %s", e.Message)
			}
			return fmt.Errorf("%s", e.Message)
		}
		return fmt.Errorf("control API %s %s: HTTP %d %s", method, path, resp.StatusCode, strings.TrimSpace(string(data)))
	}
	if out != nil {
		return json.Unmarshal(data, out)
	}
	return nil
}

// printJSON writes v as indented JSON.
func printJSON(cmd *cobra.Command, v any) error {
	enc := json.NewEncoder(cmd.OutOrStdout())
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

// placementPath is the API path of a tenant/bucket argument.
func placementPath(key string) (string, error) {
	tenant, bucket, ok := strings.Cut(key, "/")
	if !ok || tenant == "" || bucket == "" || strings.Contains(bucket, "/") {
		return "", fmt.Errorf("%q: want <tenant>/<bucket>", key)
	}
	return "/v1/placements/" + tenant + "/" + bucket, nil
}

// waitContext bounds a call that may legitimately take a while, such as cutover's quiet window.
func waitContext(cmd *cobra.Command, d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(cmd.Context(), d)
}
