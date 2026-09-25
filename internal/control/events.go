package control

import (
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/blakegolliher/shunt/internal/directory"
)

// GET /v1/events (ADR-0017): server-sent events from this control node. Types: directory (a
// record changed, with the record), fence (an operation's phase and the proxies it waits on),
// fleet (a proxy joined, left, fell silent, came back, or applied a version), telemetry
// (reserved for UI-1), and reset (the client's Last-Event-ID is not in this node's ring: reload
// the read models). Ids are <epoch>:<seq>, the epoch being this node's start, so an id from
// another node or an earlier run is recognized as foreign.

const (
	defaultEventRing   = 4096
	defaultKeepalive   = 15 * time.Second
	subscriberBuffer   = 256
	maxEventStreams    = 64
	eventTypeDir       = "directory"
	eventTypeFence     = "fence"
	eventTypeFleet     = "fleet"
	eventTypeReset     = "reset"
	eventTypeTelemetry = "telemetry" // a completed 10-second telemetry window was merged
)

// Event is one server-sent event.
type Event struct {
	ID   string          `json:"id"`
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

// DirectoryEvent is a directory event's data: one record that changed at a version, or the
// version itself once every record of that version has been sent.
type DirectoryEvent struct {
	Version int64  `json:"version"`
	Kind    string `json:"kind"`          // placement | cluster | tenant | version
	Key     string `json:"key,omitempty"` // tenant/bucket, the cluster name, or the tenant
	Op      string `json:"op,omitempty"`  // put | delete
	Record  any    `json:"record,omitempty"`
}

// FenceEvent is a fence event's data: an operation's progress through the fence.
type FenceEvent struct {
	Operation string   `json:"operation"`
	Kind      string   `json:"kind"`
	Placement string   `json:"placement,omitempty"`
	Cluster   string   `json:"cluster,omitempty"`
	Status    string   `json:"status"`
	Phase     string   `json:"phase,omitempty"`
	WaitingOn []string `json:"waiting_on,omitempty"`
	Silent    []string `json:"silent,omitempty"`
	Version   int64    `json:"version,omitempty"`
}

// FleetEvent is a fleet event's data.
type FleetEvent struct {
	ID      string `json:"id"`
	Event   string `json:"event"` // joined | left | silent | live | applied
	Applied int64  `json:"applied,omitempty"`
	Version int64  `json:"version"` // the directory version now
}

// Events is the ring and its subscribers.
type Events struct {
	// Keepalive is how often an idle stream gets a comment line; default 15s.
	Keepalive time.Duration

	epoch string
	mu    sync.Mutex
	seq   uint64
	ring  []Event // ring[seq % len(ring)]
	subs  map[chan Event]struct{}
	prev  *directory.Snapshot
}

// NewEvents returns a broker keeping the last size events.
func NewEvents(size int) *Events {
	if size <= 0 {
		size = defaultEventRing
	}
	return &Events{epoch: strconv.FormatInt(time.Now().UnixNano(), 36), ring: make([]Event, size), subs: map[chan Event]struct{}{}}
}

// Publish appends one event and hands it to every subscriber; a subscriber that has fallen
// subscriberBuffer events behind is dropped, and reconnects with its last id.
func (e *Events) Publish(typ string, data any) {
	raw, err := json.Marshal(data)
	if err != nil {
		raw = []byte("null")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.seq++
	ev := Event{ID: e.epoch + ":" + strconv.FormatUint(e.seq, 10), Type: typ, Data: raw}
	e.ring[e.seq%uint64(len(e.ring))] = ev
	for ch := range e.subs {
		select {
		case ch <- ev:
		default:
			delete(e.subs, ch)
			close(ch)
		}
	}
}

// Subscribe registers a stream. lastID is the client's Last-Event-ID: events after it are
// returned for replay when it is in the ring; reset says it was not (foreign or too old), so the
// client must reload. ok is false when there are too many streams already.
func (e *Events) Subscribe(lastID string) (replay []Event, ch <-chan Event, reset, ok bool, cancel func()) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.subs) >= maxEventStreams {
		return nil, nil, false, false, func() {}
	}
	c := make(chan Event, subscriberBuffer)
	e.subs[c] = struct{}{}
	if lastID != "" {
		epoch, seqText, _ := strings.Cut(lastID, ":")
		last, err := strconv.ParseUint(seqText, 10, 64)
		size := uint64(len(e.ring))
		oldest := uint64(1)
		if e.seq > size {
			oldest = e.seq - size + 1
		}
		switch {
		case epoch != e.epoch || err != nil || last > e.seq || (last+1 < oldest):
			reset = true
		default:
			for i := last + 1; i <= e.seq; i++ {
				replay = append(replay, e.ring[i%size])
			}
		}
	}
	cancel = func() {
		e.mu.Lock()
		defer e.mu.Unlock()
		if _, live := e.subs[c]; live {
			delete(e.subs, c)
			close(c)
		}
	}
	return replay, c, reset, true, cancel
}

// Streams reports how many streams are open.
func (e *Events) Streams() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.subs)
}

// Directory is the store's OnInstall hook: it publishes one event per record that differs from
// the previous snapshot, then the version. The first call is the baseline and publishes nothing;
// a version already seen (the store re-installing after a reload) is ignored.
func (e *Events) Directory(snap *directory.Snapshot) {
	e.mu.Lock()
	prev := e.prev
	if prev != nil && snap.Version() <= prev.Version() {
		e.mu.Unlock()
		return
	}
	e.prev = snap
	e.mu.Unlock()
	if prev == nil {
		return
	}
	v := snap.Version()
	before, after := prev.File(), snap.File()
	for _, k := range sortedKeys(after.Clusters) {
		if b, ok := before.Clusters[k]; !ok || !reflect.DeepEqual(b, after.Clusters[k]) {
			e.Publish(eventTypeDir, DirectoryEvent{Version: v, Kind: "cluster", Key: k, Op: "put", Record: after.Clusters[k]})
		}
	}
	for _, k := range sortedKeys(before.Clusters) {
		if _, ok := after.Clusters[k]; !ok {
			e.Publish(eventTypeDir, DirectoryEvent{Version: v, Kind: "cluster", Key: k, Op: "delete"})
		}
	}
	for _, k := range sortedKeys(after.Tenants) {
		if b, ok := before.Tenants[k]; !ok || !reflect.DeepEqual(b, after.Tenants[k]) {
			e.Publish(eventTypeDir, DirectoryEvent{Version: v, Kind: "tenant", Key: k, Op: "put", Record: after.Tenants[k]})
		}
	}
	for _, k := range sortedKeys(before.Tenants) {
		if _, ok := after.Tenants[k]; !ok {
			e.Publish(eventTypeDir, DirectoryEvent{Version: v, Kind: "tenant", Key: k, Op: "delete"})
		}
	}
	for _, k := range sortedKeys(after.Placements) {
		if b, ok := before.Placements[k]; !ok || !reflect.DeepEqual(b, after.Placements[k]) {
			e.Publish(eventTypeDir, DirectoryEvent{Version: v, Kind: "placement", Key: k, Op: "put", Record: after.Placements[k]})
		}
	}
	for _, k := range sortedKeys(before.Placements) {
		if _, ok := after.Placements[k]; !ok {
			e.Publish(eventTypeDir, DirectoryEvent{Version: v, Kind: "placement", Key: k, Op: "delete"})
		}
	}
	e.Publish(eventTypeDir, DirectoryEvent{Version: v, Kind: "version"})
}

// Fence is the operations store's OnChange hook.
func (e *Events) Fence(op Operation) {
	e.Publish(eventTypeFence, FenceEvent{Operation: op.ID, Kind: op.Kind, Placement: op.Placement, Cluster: op.Cluster,
		Status: op.Status, Phase: op.Phase, WaitingOn: op.WaitingOn, Silent: op.Silent, Version: op.Version})
}

// fleetEvents publishes what changed in the fleet since the last call.
func (s *Server) fleetEvents(ms []Member) {
	if s.Events == nil {
		return
	}
	s.mu.Lock()
	prev, seeded := s.prevFleet, s.fleetSeeded
	s.prevFleet, s.fleetSeeded = ms, true
	s.mu.Unlock()
	if !seeded {
		return
	}
	v := s.Dir.Snapshot().Version()
	before := map[string]Member{}
	for i := range prev {
		m := &prev[i]
		before[m.ID] = *m
	}
	seen := map[string]bool{}
	for i := range ms {
		m := &ms[i]
		seen[m.ID] = true
		b, ok := before[m.ID]
		switch {
		case !ok:
			s.Events.Publish(eventTypeFleet, FleetEvent{ID: m.ID, Event: "joined", Applied: m.Applied, Version: v})
		case b.Live && !m.Live:
			s.Events.Publish(eventTypeFleet, FleetEvent{ID: m.ID, Event: "silent", Applied: m.Applied, Version: v})
		case !b.Live && m.Live:
			s.Events.Publish(eventTypeFleet, FleetEvent{ID: m.ID, Event: "live", Applied: m.Applied, Version: v})
		case m.Live && b.Applied != m.Applied:
			s.Events.Publish(eventTypeFleet, FleetEvent{ID: m.ID, Event: "applied", Applied: m.Applied, Version: v})
		}
	}
	for id := range before {
		if !seen[id] {
			s.Events.Publish(eventTypeFleet, FleetEvent{ID: id, Event: "left", Version: v})
		}
	}
}

// events is GET /v1/events.
func (s *Server) events(w http.ResponseWriter, r *http.Request) {
	if s.Events == nil {
		writeError(w, http.StatusServiceUnavailable, "unavailable", "this shunt publishes no events")
		return
	}
	last := r.Header.Get("Last-Event-ID")
	if last == "" {
		last = r.URL.Query().Get("last_event_id")
	}
	replay, ch, reset, ok, cancel := s.Events.Subscribe(last)
	if !ok {
		writeError(w, http.StatusServiceUnavailable, "unavailable", fmt.Sprintf("this node already serves %d event streams", maxEventStreams))
		return
	}
	defer cancel()
	rc := http.NewResponseController(w)
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-store")
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	write := func(ev Event) bool {
		_, err := fmt.Fprintf(w, "id: %s\nevent: %s\ndata: %s\n\n", ev.ID, ev.Type, ev.Data)
		return err == nil && rc.Flush() == nil
	}
	if reset {
		raw, _ := json.Marshal(map[string]string{"reason": "the last event id is not in this node's ring; reload"}) //nolint:errcheck // a map of strings marshals
		if !write(Event{ID: last, Type: eventTypeReset, Data: raw}) {
			return
		}
	}
	for _, ev := range replay {
		if !write(ev) {
			return
		}
	}
	if err := rc.Flush(); err != nil {
		return
	}
	keepalive := s.Events.Keepalive
	if keepalive <= 0 {
		keepalive = defaultKeepalive
	}
	tick := time.NewTicker(keepalive)
	defer tick.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case ev, live := <-ch:
			if !live || !write(ev) {
				return
			}
		case <-tick.C:
			if _, err := fmt.Fprint(w, ": keepalive\n\n"); err != nil || rc.Flush() != nil {
				return
			}
		}
	}
}
