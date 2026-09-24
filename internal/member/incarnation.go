package member

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/blakegolliher/shunt/internal/control"
)

// The incarnation protocol (ADR-0021 D2). Each process of a proxy draws an incarnation and, before
// it serves anything, writes a marker in its cache directory saying the incarnation is running and
// unclean: if the process ends any other way than through Retire, the marker says so to the next
// process, which reports it to the control plane. Retire is the clean end: admission has stopped,
// every request has ended, the marker says retired with the count of backend outcomes never
// learned, and the control plane is told; a lost reply is retried until the caller gives up.

// markerFile is the incarnation marker in the cache directory.
const markerFile = "incarnation.json"

// marker is what the file holds: the incarnation and how it stands.
type marker struct {
	Schema      int       `json:"schema"`
	ProxyID     string    `json:"proxy_id"`
	Incarnation string    `json:"incarnation"`
	Started     time.Time `json:"started"`
	// State is control.IncarnationActive while the process runs (unclean if it ends so), or
	// control.IncarnationRetired or control.IncarnationUnclean once Retire has run.
	State     string    `json:"state"`
	Ended     time.Time `json:"ended,omitzero"`
	Uncertain int64     `json:"uncertain,omitempty"`
}

// newIncarnation draws an incarnation id: 128 random bits, hex.
func newIncarnation() string {
	var b [16]byte
	_, _ = rand.Read(b[:]) //nolint:errcheck // crypto/rand does not fail short
	return hex.EncodeToString(b[:])
}

// Incarnation is this process's incarnation id.
func (c *Client) Incarnation() string { return c.incarnation }

func (c *Client) markerPath() string { return filepath.Join(c.cfg.CacheDir, markerFile) }

// mark writes the incarnation marker durably, before the process serves and when it retires.
func (c *Client) mark(state string, uncertain int64) error {
	m := marker{Schema: 1, ProxyID: c.cfg.ProxyID, Incarnation: c.incarnation, Started: c.started, State: state, Uncertain: uncertain}
	if state != control.IncarnationActive {
		m.Ended = c.Now().UTC()
	}
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(c.cfg.CacheDir, ".incarnation-*") // 0600
	if err != nil {
		return err
	}
	renamed := false
	defer func() {
		if !renamed {
			_ = os.Remove(tmp.Name())
		}
	}()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp.Name(), c.markerPath()); err != nil {
		return err
	}
	renamed = true
	return c.ops.dirSync(c.cfg.CacheDir)
}

// readPrevious reads the marker the previous process left, as the control plane should hear of
// it: nil when there was none, or it was another proxy's. A process that ended while its marker
// still said active never retired: it is reported unclean.
func (c *Client) readPrevious() *control.Incarnation {
	data, err := os.ReadFile(c.markerPath())
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			c.log.Warn("incarnation marker unreadable; the previous process, if any, cannot be reported", "path", c.markerPath(), "err", err.Error())
		}
		return nil
	}
	var m marker
	if json.Unmarshal(data, &m) != nil || m.Schema != 1 || m.ProxyID != c.cfg.ProxyID || !control.ValidIncarnation(m.Incarnation) {
		c.log.Warn("incarnation marker ignored: torn, of another schema, or another proxy's", "path", c.markerPath())
		return nil
	}
	inc := &control.Incarnation{ID: m.Incarnation, Started: m.Started, Ended: m.Ended, State: m.State, Uncertain: m.Uncertain}
	if inc.State == control.IncarnationActive || inc.State == "" {
		inc.State = control.IncarnationUnclean
	}
	return inc
}

// previousToReport is the previous incarnation until the control plane has recorded it.
func (c *Client) previousToReport() *control.Incarnation {
	if c.previousDone.Load() {
		return nil
	}
	return c.previous
}

// Retire ends this incarnation cleanly: the caller has stopped admitting requests and every one
// has ended; uncertain is how many backend outcomes were never learned (the gates' count, plus any
// request cut short by the drain deadline). The marker is written first, so a crash after it
// leaves a retirement on disk, then the control plane is told, retrying until ctx ends. It returns
// the last error when the control plane never recorded it: the next process reports the marker.
func (c *Client) Retire(ctx context.Context, uncertain int64) error {
	state := control.IncarnationRetired
	if uncertain > 0 {
		state = control.IncarnationUnclean
	}
	if err := c.mark(state, uncertain); err != nil {
		c.log.Error("retirement marker not written; the next process reports this one as unclean", "err", err.Error())
	}
	req := control.RetireRequest{Incarnation: c.incarnation, Uncertain: uncertain}
	var err error
	for {
		cctx, cancel := context.WithTimeout(ctx, c.cfg.Interval*3)
		_, err = c.call(cctx, http.MethodPost, "/v1/fleet/"+c.cfg.ProxyID+"/retire", req, nil)
		cancel()
		if err == nil {
			c.log.Info("retired from the fleet", "proxy", c.cfg.ProxyID, "incarnation", c.incarnation, "uncertain", uncertain)
			return nil
		}
		var e *Error
		if errors.As(err, &e) && e.Status < 500 && e.Status != http.StatusTooManyRequests {
			return fmt.Errorf("retirement refused: %w", err)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("retirement not recorded: %w (last: %w)", ctx.Err(), err)
		case <-time.After(c.cfg.Interval):
		}
	}
}
