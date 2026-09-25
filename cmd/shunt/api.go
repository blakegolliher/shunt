package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"

	"github.com/blakegolliher/shunt/internal/config"
	"github.com/blakegolliher/shunt/internal/control"
	"github.com/blakegolliher/shunt/internal/directory"
)

// apiOptions are the flags every command that talks to a running shunt takes.
type apiOptions struct {
	url       string
	tokenRef  string
	json      bool
	requestID string
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
	f.StringVar(&o.requestID, "request-id", "", "this command's request id: its changes are sent with Idempotency-Key <id>-1, <id>-2, …; "+
		"repeat a command that lost its connection with the id it printed, and the control plane answers with what it already did (default: random)")
}

// apiClient calls the control API (docs/reference/control-api.md).
type apiClient struct {
	base  string
	token string
	http  *http.Client
	// requestID keys this command's changes; mutations counts them, so a repeat with the same
	// id sends the same keys in the same order.
	requestID string
	mutations int
	mu        sync.Mutex
}

func (o apiOptions) client() (*apiClient, error) {
	c := &apiClient{base: strings.TrimRight(o.url, "/"), http: &http.Client{}, requestID: o.requestID}
	if c.requestID == "" {
		var b [8]byte
		_, _ = rand.Read(b[:]) //nolint:errcheck // crypto/rand does not fail short
		c.requestID = hex.EncodeToString(b[:])
	}
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
	if method != http.MethodGet {
		c.mu.Lock()
		c.mutations++
		req.Header.Set(control.HeaderIdempotencyKey, fmt.Sprintf("%s-%d", c.requestID, c.mutations))
		c.mu.Unlock()
	}
	resp, err := c.http.Do(req)
	if err != nil {
		if method != http.MethodGet {
			return fmt.Errorf("control API %s: %w; the change may or may not have been made: repeat the command with --request-id %s to find out without making it twice", c.base, err, c.requestID)
		}
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
				return fmt.Errorf("refused: %s", shownText(e.Message))
			}
			return fmt.Errorf("%s", shownText(e.Message))
		}
		return fmt.Errorf("control API %s %s: HTTP %d %s", method, path, resp.StatusCode, strings.TrimSpace(string(data)))
	}
	if out != nil {
		return json.Unmarshal(data, out)
	}
	return nil
}

// operate starts an operation record for one action and polls it until it ends (ADR-0017). The
// server runs the action on its own context, so a --wait longer than a listener's timeout is
// fine, and an operator who loses the connection can still read the outcome from the record.
func (c *apiClient) operate(ctx context.Context, req control.OperationRequest, out any) error {
	var op control.Operation
	if err := c.call(ctx, http.MethodPost, "/v1/operations", req, &op); err != nil {
		return err
	}
	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()
	for !op.Terminal() {
		select {
		case <-ctx.Done():
			// The wait ran out with the operation unfinished: exit 3 and say where it stands and what
			// to do (contracts §2, "CLI completion semantics"). It keeps running on the control node.
			what := fmt.Sprintf("operation %s is still %s (phase %s)", op.ID, op.Status, op.Phase)
			if len(op.Blockers) > 0 {
				what += "; waiting on " + control.BlockerText(op.Blockers)
			}
			next := fmt.Sprintf("it keeps running: `shunt operation wait %s` follows it", op.ID)
			if slices.Contains(op.AllowedActions, control.ActionCancel) {
				next += fmt.Sprintf(", `shunt operation cancel %s` restores its durable precommit hold", op.ID)
			}
			return &exitError{code: exitWaitDeadline, err: fmt.Errorf("%s; %s", what, next)}
		case <-tick.C:
		}
		if err := c.call(ctx, http.MethodGet, "/v1/operations/"+op.ID, nil, &op); err != nil {
			if ctx.Err() != nil {
				continue // the next select reports the wait deadline with the last complete record
			}
			return err
		}
	}
	msg := "operation " + op.ID + " " + op.Status
	if op.Error != nil {
		msg = op.Error.Message
	}
	switch op.Status {
	case control.StatusSucceeded:
		if out != nil && len(op.Result) > 0 {
			return json.Unmarshal(op.Result, out)
		}
		return nil
	case control.StatusFailed:
		if op.Error != nil && op.Error.Code == "refused" && !strings.HasPrefix(msg, "refused") {
			return fmt.Errorf("refused: %s", shownText(msg))
		}
	}
	return fmt.Errorf("%s", shownText(msg))
}

// argsOf is an operation's args: the request body the action's own route takes.
func argsOf(v any) json.RawMessage {
	if v == nil {
		return nil
	}
	raw, _ := json.Marshal(v) //nolint:errcheck // request structs marshal
	return raw
}

// printJSON writes v as indented JSON.
func printJSON(cmd *cobra.Command, v any) error {
	enc := json.NewEncoder(cmd.OutOrStdout())
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

// placementPath is the API path of a tenant/bucket argument.
func placementPath(arg string) (string, error) {
	key, err := placementKey(arg)
	if err != nil {
		return "", err
	}
	tenant, bucket, _ := strings.Cut(key, "/")
	return "/v1/placements/" + tenant + "/" + bucket, nil
}

// placementKey reads a bucket argument: a bare name is the default tenant's bucket, tenant/bucket
// names a tenant explicitly.
func placementKey(arg string) (string, error) {
	tenant, bucket, ok := strings.Cut(arg, "/")
	if !ok {
		tenant, bucket = directory.DefaultTenant, arg
	}
	if tenant == "" || bucket == "" || strings.Contains(bucket, "/") {
		return "", fmt.Errorf("%q: want <bucket>, or <tenant>/<bucket>", arg)
	}
	return directory.Key(tenant, bucket), nil
}

// shown is a placement key as an operator reads it: the default tenant's buckets by bare name.
func shown(key string) string {
	return strings.TrimPrefix(key, directory.DefaultTenant+"/")
}

// defaultTenantText rewrites the directory's reference paths for the default tenant into words, so a
// one-team deployment never reads "tenants.default…" or "placements.default/…".
var defaultTenantText = strings.NewReplacer(
	"tenants."+directory.DefaultTenant+".default_cluster", "the default cluster for new buckets",
	"placements."+directory.DefaultTenant+"/", "bucket ",
	"`shunt migrate run "+directory.DefaultTenant+"/", "`shunt migrate run ",
	" "+directory.DefaultTenant+"/", " ",
)

// shownText is an API message or reference list as an operator reads it.
func shownText(s string) string {
	if strings.HasPrefix(s, directory.DefaultTenant+"/") {
		s = strings.TrimPrefix(s, directory.DefaultTenant+"/")
	}
	return defaultTenantText.Replace(s)
}

// waitContext bounds a call that may legitimately take a while, such as cutover's quiet window.
func waitContext(cmd *cobra.Command, d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(cmd.Context(), d)
}
