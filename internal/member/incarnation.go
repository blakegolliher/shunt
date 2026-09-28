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
	"slices"
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

// marker is what the file holds: the incarnation and how it stands, and the earlier processes of
// this proxy the control plane has not recorded yet.
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
	// Registered: a heartbeat of this incarnation was answered, so the control plane knows it.
	Registered bool `json:"registered,omitempty"`
	// Predecessors are earlier processes of this proxy the control plane has not recorded,
	// oldest first (schema 2; R3-04). A process that started and crashed while the control plane
	// was unreachable is evidence only this file holds: each is carried into the next marker
	// until a heartbeat answer says it is recorded.
	Predecessors []control.Incarnation `json:"predecessors,omitempty"`
}

// markerSchema is the marker's schema; schema 1 (no predecessors) is read too.
const markerSchema = 2

// newIncarnation draws an incarnation id: 128 random bits, hex.
func newIncarnation() string {
	var b [16]byte
	_, _ = rand.Read(b[:]) //nolint:errcheck // crypto/rand does not fail short
	return hex.EncodeToString(b[:])
}

// Incarnation is this process's incarnation id.
func (c *Client) Incarnation() string { return c.incarnation }

func (c *Client) markerPath() string { return filepath.Join(c.cfg.CacheDir, markerFile) }

// mark records this incarnation's state and writes the marker durably: when it retires.
func (c *Client) mark(state string, uncertain int64) error {
	c.marking.Lock()
	defer c.marking.Unlock()
	c.markState, c.markUncertain = state, uncertain
	return c.writeMarker()
}

// writeMarker writes the marker from what c.marking guards, durably; c.marking is held.
func (c *Client) writeMarker() error {
	m := marker{Schema: markerSchema, ProxyID: c.cfg.ProxyID, Incarnation: c.incarnation, Started: c.started, State: c.markState,
		Uncertain: c.markUncertain, Registered: c.registered, Predecessors: c.predecessors}
	if m.State != control.IncarnationActive {
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

// readPredecessors reads what the marker the previous process left says of the processes before
// this one, oldest first, as the control plane should hear of them: the previous process's own
// predecessors, still unrecorded, then the previous process itself. A process that ended while its
// marker still said active never retired: it is reported unclean. One that retired cleanly and was
// never registered is dropped: the control plane never knew it, and nothing of it is uncertain. More
// than maxPredecessors refuses to start: a proxy restarted that often without the control plane
// hearing of it stops rather than let evidence go (R3-04).
func (c *Client) readPredecessors() ([]control.Incarnation, error) {
	data, err := os.ReadFile(c.markerPath())
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			c.log.Warn("incarnation marker unreadable; the previous process, if any, cannot be reported", "path", c.markerPath(), "err", err.Error())
		}
		return nil, nil
	}
	var m marker
	if json.Unmarshal(data, &m) != nil || (m.Schema != 1 && m.Schema != markerSchema) || m.ProxyID != c.cfg.ProxyID || !control.ValidIncarnation(m.Incarnation) {
		c.log.Warn("incarnation marker ignored: torn, of another schema, or another proxy's", "path", c.markerPath())
		return nil, nil
	}
	out := slices.Clone(m.Predecessors)
	prev := control.Incarnation{ID: m.Incarnation, Started: m.Started, Ended: m.Ended, State: m.State, Uncertain: m.Uncertain}
	if prev.State == control.IncarnationActive || prev.State == "" {
		prev.State = control.IncarnationUnclean
	}
	if prev.State != control.IncarnationRetired || prev.Uncertain > 0 || m.Registered || m.Schema == 1 {
		out = append(out, prev)
	}
	if len(out) > maxPredecessors {
		return nil, fmt.Errorf("%d earlier processes of proxy %s ended without the control plane recording them, more than the %d its "+
			"marker keeps: each may have left a backend outcome unknown, so this process does not start and the marker (%s) keeps them; "+
			"resolve them with the control plane (`shunt proxy show %s`) once it is reachable, or retire this cache directory",
			len(out), c.cfg.ProxyID, maxPredecessors, c.markerPath(), c.cfg.ProxyID)
	}
	return out, nil
}

// maxPredecessors bounds the unrecorded earlier processes a marker keeps: several heartbeats' worth,
// since each heartbeat carries at most control.MaxUnresolvedIncarnations of them.
const maxPredecessors = 4 * control.MaxUnresolvedIncarnations

// predecessorsToReport is what the next heartbeat carries: the oldest earlier processes the control
// plane has not recorded yet, as many as a heartbeat takes. Once they are recorded the next ones go.
func (c *Client) predecessorsToReport() []control.Incarnation {
	c.marking.Lock()
	defer c.marking.Unlock()
	return slices.Clone(c.predecessors[:min(len(c.predecessors), control.MaxUnresolvedIncarnations)])
}

// answered takes a heartbeat's answer: this incarnation is registered, and, when the answer says the
// predecessors it carried are recorded, they leave the marker. The marker is rewritten only when
// either changes; a failed write keeps them in memory and on disk as they were, and they are sent
// again, which the control plane takes as a repeat.
func (c *Client) answered(sent []control.Incarnation, recorded bool) {
	c.marking.Lock()
	defer c.marking.Unlock()
	changed := !c.registered
	c.registered = true
	if recorded && len(sent) > 0 {
		before := len(c.predecessors)
		c.predecessors = slices.DeleteFunc(c.predecessors, func(p control.Incarnation) bool {
			return slices.ContainsFunc(sent, func(s control.Incarnation) bool { return s.ID == p.ID })
		})
		changed = changed || len(c.predecessors) != before
	}
	if !changed {
		return
	}
	if err := c.writeMarker(); err != nil {
		c.log.Warn("incarnation marker not rewritten after the control plane's answer; the earlier processes are sent again", "err", err.Error())
	}
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
	c.retired.Store(true) // no heartbeat after this: it would renew the lease of a retired process
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
