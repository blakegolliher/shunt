package control

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/blakegolliher/shunt/internal/directory"
)

// The browser-facing surface of UI-0 (ADR-0017): operation records, dry runs and their tokens,
// the event stream, the read models, and the audit tail, over the file-backed rig.

// prepare adopts acme/data01 on vast01 and expands it to vast02, with two objects on the source.
func (rg *rig) prepare() {
	rg.t.Helper()
	if err := rg.vast01.be.CreateBucket("data01"); err != nil {
		rg.t.Fatal(err)
	}
	rg.vast01.put(rg.t, "data01", "a", "one")
	rg.vast01.put(rg.t, "data01", "dir/b", "two")
	rg.must("POST", "/v1/clusters", ClusterRequest{Name: "vast01", Cluster: rg.vast01.definition(true)}, nil)
	rg.must("POST", "/v1/placements/acme/data01/adopt", AdoptRequest{Cluster: "vast01"}, nil)
	rg.must("POST", "/v1/clusters", ClusterRequest{Name: "vast02", Cluster: rg.vast02.definition(true)}, nil)
	rg.must("POST", "/v1/placements/acme/data01/expand", ExpandRequest{To: "vast02", Create: true}, nil)
}

// cutOver takes acme/data01 to CUTOVER with both objects on the primary.
func (rg *rig) cutOver() {
	rg.t.Helper()
	rg.must("POST", "/v1/placements/acme/data01/migrate", MigrateRequest{}, nil)
	rg.vast02.put(rg.t, "data01-001", "a", "one")
	rg.vast02.put(rg.t, "data01-001", "dir/b", "two")
	rg.must("POST", "/v1/placements/acme/data01/mover-progress", Progress{Source: "vast01", Primary: "vast02", Pass: 2, Skipped: 2, Done: true, Converged: true}, nil)
	rg.must("POST", "/v1/placements/acme/data01/cutover", CutoverRequest{Window: "5s"}, nil)
}

// await polls an operation record until it is no longer running.
func (rg *rig) await(id string) Operation {
	rg.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		var op Operation
		rg.must("GET", "/v1/operations/"+id, nil, &op)
		if op.Status != StatusRunning {
			return op
		}
		if time.Now().After(deadline) {
			rg.t.Fatalf("operation %s still running: %+v", id, op)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestOperationRecords(t *testing.T) {
	rg := newRig(t)
	rg.prepare()

	// An operation started by POST /v1/operations answers 202 and runs to its outcome.
	var op Operation
	if code, raw := rg.call("POST", "/v1/operations", OperationRequest{Kind: OpRamp, Placement: "acme/data01", Args: json.RawMessage(`{"ratio": 0.5}`)}, &op); code != http.StatusAccepted {
		t.Fatalf("start: HTTP %d %s", code, raw)
	}
	if op.ID == "" || op.Status != StatusRunning || op.Kind != OpRamp || op.Placement != "acme/data01" || op.Node != "lab" || op.Actor != "api:127.0.0.1" {
		t.Fatalf("started record: %+v", op)
	}
	done := rg.await(op.ID)
	var res TransitionResult
	if done.Status != StatusSucceeded || done.Phase != PhaseDone || done.Version == 0 || json.Unmarshal(done.Result, &res) != nil {
		t.Fatalf("finished record: %+v", done)
	}
	if res.To != directory.StateRamping || res.Ratio != 0.5 || res.Operation != op.ID {
		t.Fatalf("result: %+v", res)
	}

	// The action's own route runs under a record too, and answers with its id.
	var tr TransitionResult
	rg.must("POST", "/v1/placements/acme/data01/ramp", RampRequest{Ratio: 0.7}, &tr)
	if tr.Operation == "" || rg.await(tr.Operation).Status != StatusSucceeded {
		t.Fatalf("sync route record: %+v", tr)
	}
	var list OperationList
	rg.must("GET", "/v1/operations?placement=acme/data01", nil, &list)
	if len(list.Operations) != 2 || list.Operations[0].ID != tr.Operation || list.Operations[1].ID != op.ID {
		t.Fatalf("list, newest first: %+v", list)
	}
	rg.must("GET", "/v1/operations?placement=acme/other", nil, &list)
	if len(list.Operations) != 0 {
		t.Fatalf("list of another placement: %+v", list)
	}

	// A refused step ends its record refused, with the reason.
	rg.call("POST", "/v1/operations", OperationRequest{Kind: OpRamp, Placement: "acme/data01", Args: json.RawMessage(`{"ratio": 0.2}`)}, &op)
	done = rg.await(op.ID)
	if done.Status != StatusRefused || done.Error == nil || done.Error.Code != "refused" || !strings.Contains(done.Error.Message, "only grows") {
		t.Fatalf("refused record: %+v", done)
	}
	if !strings.Contains(rg.log.String(), "WARN  ramp refused  actor=api:127.0.0.1 operation="+op.ID) {
		t.Errorf("an async refusal is not in the log:\n%s", rg.log.String())
	}

	// What is checked before a record exists.
	rg.answers("POST", "/v1/operations", map[string]any{"kind": "ramp", "placement": "acme/data01", "args": map[string]any{"ratioo": 1}}, http.StatusBadRequest, "bad_request")
	rg.answers("POST", "/v1/operations", OperationRequest{Kind: "dance", Placement: "acme/data01"}, http.StatusBadRequest, "bad_request")
	rg.answers("POST", "/v1/operations", OperationRequest{Kind: OpRamp, Placement: "acme/nope", Args: json.RawMessage(`{"ratio": 1}`)}, http.StatusNotFound, "not_found")
	rg.answers("POST", "/v1/operations", OperationRequest{Kind: OpRamp, Placement: "data01", Args: json.RawMessage(`{"ratio": 1}`)}, http.StatusBadRequest, "bad_request")
	rg.answers("POST", "/v1/operations", OperationRequest{Kind: OpPurge, Placement: "acme/data01", Args: json.RawMessage(`{"dry_run": true}`)}, http.StatusBadRequest, "bad_request")
	rg.answers("GET", "/v1/operations/nope", nil, http.StatusNotFound, "not_found")
}

func TestMemOperations(t *testing.T) {
	var seen []string
	m := &MemOperations{Limit: 3, OnChange: func(op Operation) { seen = append(seen, op.ID+":"+op.Status) }}
	ctx := context.Background()
	for _, id := range []string{"1", "2", "3", "4"} {
		if err := m.Put(ctx, &Operation{ID: id, Placement: "t/b", Status: StatusRunning}); err != nil {
			t.Fatal(err)
		}
	}
	if err := m.Put(ctx, &Operation{ID: "4", Placement: "t/b", Status: StatusSucceeded}); err != nil {
		t.Fatal(err)
	}
	if op, _ := m.Get(ctx, "1"); op != nil {
		t.Error("the oldest record was not forgotten")
	}
	if op, _ := m.Get(ctx, "4"); op == nil || op.Status != StatusSucceeded {
		t.Errorf("get: %+v", op)
	}
	ops, _ := m.List(ctx, "t/b", "", 10)
	if len(ops) != 3 || ops[0].ID != "4" || ops[2].ID != "2" {
		t.Errorf("list: %+v", ops)
	}
	if ops, _ := m.List(ctx, "", "c", 10); len(ops) != 0 {
		t.Errorf("list by cluster: %+v", ops)
	}
	if len(seen) != 5 || seen[4] != "4:succeeded" {
		t.Errorf("on change: %v", seen)
	}
}

func TestPurgeDryRunAndToken(t *testing.T) {
	rg := newRig(t)
	rg.prepare()
	rg.refused("POST", "/v1/placements/acme/data01/purge-source", PurgeRequest{Token: "x"}, "purge-source runs on a placement in CUTOVER")
	rg.cutOver()

	var dry PurgeDryRun
	rg.must("POST", "/v1/placements/acme/data01/purge-source", PurgeRequest{DryRun: true}, &dry)
	if !dry.Allowed || dry.Token == "" || dry.Objects != 2 || dry.Bytes != 6 || dry.UploadsInFlight != 0 || dry.ExpiresAt.Sub(rg.ctl.Now()) != confirmTTL {
		t.Fatalf("dry run: %+v", dry)
	}
	rg.refused("POST", "/v1/placements/acme/data01/purge-source", nil, "needs the confirmation token")
	rg.refused("POST", "/v1/placements/acme/data01/purge-source", PurgeRequest{Token: "garbage"}, "malformed")

	// The source cluster's definition changes under the token: it no longer matches.
	rg.must("POST", "/v1/clusters", ClusterRequest{Name: "vast01", Cluster: rg.vast01.definition(false)}, nil)
	rg.refused("POST", "/v1/placements/acme/data01/purge-source", PurgeRequest{Token: dry.Token}, "does not match the current state")

	// A token outlives its ten minutes only on the clock of the operator who forgot it.
	rg.must("POST", "/v1/placements/acme/data01/purge-source", PurgeRequest{DryRun: true}, &dry)
	issued := rg.ctl.Now()
	rg.ctl.Now = func() time.Time { return issued.Add(confirmTTL + time.Minute) }
	rg.refused("POST", "/v1/placements/acme/data01/purge-source", PurgeRequest{Token: dry.Token}, "has expired")
	rg.ctl.Now = func() time.Time { return issued }

	var pg PurgeResult
	rg.must("POST", "/v1/placements/acme/data01/purge-source", PurgeRequest{Token: dry.Token}, &pg)
	if pg.ObjectsDeleted != 2 || pg.Operation == "" {
		t.Fatalf("purge: %+v", pg)
	}
	if ok, _ := rg.vast01.be.BucketExists("data01"); ok {
		t.Error("the source bucket still exists")
	}
	op := rg.await(pg.Operation)
	if op.Status != StatusSucceeded || op.Progress == nil || op.Progress.Done != 2 || op.Progress.Total != 2 || op.Progress.Unit != "objects" {
		t.Fatalf("purge record: %+v %+v", op, op.Progress)
	}
	rg.must("POST", "/v1/placements/acme/data01/purge-source", PurgeRequest{DryRun: true}, &dry)
	if dry.Allowed || !strings.Contains(dry.Reason, "runs on a placement in CUTOVER") {
		t.Fatalf("dry run after the purge: %+v", dry)
	}
}

func TestRemoveDryRunAndToken(t *testing.T) {
	rg := newRig(t)
	rg.prepare()

	var dry RemoveDryRun
	rg.must("DELETE", "/v1/clusters/vast01?dry_run=1", nil, &dry)
	if dry.Allowed || dry.Token != "" || len(dry.References) != 2 || !strings.Contains(dry.Reason, "still referenced by tenants.acme.default_cluster, placements.acme/data01") {
		t.Fatalf("dry run of a referenced cluster: %+v", dry)
	}
	rg.refused("DELETE", "/v1/clusters/vast01", RemoveRequest{Token: "x"}, "still referenced")
	rg.answers("DELETE", "/v1/clusters/nope?dry_run=1", nil, http.StatusNotFound, "not_found")

	// Once nothing references it, the token is bound to its definition.
	rg.must("POST", "/v1/tenants/acme/default-cluster", TenantDefaultRequest{Cluster: "vast02"}, nil)
	rg.must("DELETE", "/v1/clusters/vast02?dry_run=1", nil, &dry)
	if dry.Allowed || len(dry.References) != 2 {
		t.Fatalf("vast02 is the target and the default: %+v", dry)
	}
	rg.cutOver()
	var tr TransitionResult
	rg.must("POST", "/v1/placements/acme/data01/finish", nil, &tr)
	if tr.To != directory.StateActive || tr.Operation == "" {
		t.Fatalf("finish: %+v", tr)
	}
	rg.must("DELETE", "/v1/clusters/vast01?dry_run=1", nil, &dry)
	if !dry.Allowed || dry.Token == "" {
		t.Fatalf("dry run once unreferenced: %+v", dry)
	}
	rg.must("POST", "/v1/clusters", ClusterRequest{Name: "vast01", Cluster: rg.vast01.definition(false)}, nil)
	rg.refused("DELETE", "/v1/clusters/vast01", RemoveRequest{Token: dry.Token}, "does not match the current state")
	rg.must("DELETE", "/v1/clusters/vast01?dry_run=1", nil, &dry)
	var rm RemoveResult
	rg.must("DELETE", "/v1/clusters/vast01", RemoveRequest{Token: dry.Token}, &rm)
	if rm.Removed != "vast01" || rg.await(rm.Operation).Status != StatusSucceeded {
		t.Fatalf("remove: %+v", rm)
	}
}

// stream reads one server-sent event stream.
type stream struct {
	t    *testing.T
	resp *http.Response
	sc   *bufio.Scanner
}

func (rg *rig) stream(lastID string) *stream {
	rg.t.Helper()
	req, _ := http.NewRequest(http.MethodGet, rg.api.URL+"/v1/events", nil)
	if lastID != "" {
		req.Header.Set("Last-Event-ID", lastID)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		rg.t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Type") != "text/event-stream" {
		rg.t.Fatalf("events: HTTP %d %s", resp.StatusCode, resp.Header.Get("Content-Type"))
	}
	s := &stream{t: rg.t, resp: resp, sc: bufio.NewScanner(resp.Body)}
	rg.t.Cleanup(s.close)
	return s
}

func (s *stream) close() { _ = s.resp.Body.Close() }

// next returns the next event, or a comment as an Event of type ":", within timeout.
func (s *stream) next(timeout time.Duration) (Event, bool) {
	s.t.Helper()
	done := make(chan Event, 1)
	go func() {
		var ev Event
		for s.sc.Scan() {
			line := s.sc.Text()
			switch {
			case line == "":
				if ev.Type != "" {
					done <- ev
					return
				}
			case strings.HasPrefix(line, ":"):
				done <- Event{Type: ":", Data: json.RawMessage(line)}
				return
			case strings.HasPrefix(line, "id: "):
				ev.ID = strings.TrimPrefix(line, "id: ")
			case strings.HasPrefix(line, "event: "):
				ev.Type = strings.TrimPrefix(line, "event: ")
			case strings.HasPrefix(line, "data: "):
				ev.Data = json.RawMessage(strings.TrimPrefix(line, "data: "))
			}
		}
		close(done)
	}()
	select {
	case ev, ok := <-done:
		return ev, ok
	case <-time.After(timeout):
		return Event{}, false
	}
}

// until reads events until one of type typ whose data satisfies match, returning every event read.
func (s *stream) until(typ string, match func(json.RawMessage) bool) []Event {
	s.t.Helper()
	var got []Event
	for {
		ev, ok := s.next(5 * time.Second)
		if !ok {
			s.t.Fatalf("stream ended or timed out after %d events: %+v", len(got), got)
		}
		if ev.Type == ":" {
			continue
		}
		got = append(got, ev)
		if ev.Type == typ && (match == nil || match(ev.Data)) {
			return got
		}
	}
}

func versionEvent(v int64) func(json.RawMessage) bool {
	return func(raw json.RawMessage) bool {
		var d DirectoryEvent
		return json.Unmarshal(raw, &d) == nil && d.Kind == "version" && d.Version == v
	}
}

func seqOf(t *testing.T, id string) uint64 {
	t.Helper()
	var epoch string
	var seq uint64
	if _, err := parseID(id, &epoch, &seq); err != nil {
		t.Fatalf("event id %q: %v", id, err)
	}
	return seq
}

func parseID(id string, epoch *string, seq *uint64) (int, error) {
	e, s, _ := strings.Cut(id, ":")
	*epoch = e
	n, err := parseUint(s)
	*seq = n
	return 0, err
}

func parseUint(s string) (uint64, error) {
	var n uint64
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, io.ErrUnexpectedEOF
		}
		n = n*10 + uint64(c-'0')
	}
	return n, nil
}

func TestEventsReplay(t *testing.T) {
	rg := newRig(t)
	if err := rg.vast01.be.CreateBucket("data01"); err != nil {
		t.Fatal(err)
	}

	// A live stream sees a change as one event per record, then the version.
	live := rg.stream("")
	rg.must("POST", "/v1/clusters", ClusterRequest{Name: "vast01", Cluster: rg.vast01.definition(true)}, nil)
	v := rg.dir.Snapshot().Version()
	got := live.until(eventTypeDir, versionEvent(v))
	var first DirectoryEvent
	if len(got) != 2 || json.Unmarshal(got[0].Data, &first) != nil || first.Kind != "cluster" || first.Key != "vast01" || first.Op != "put" || first.Version != v {
		t.Fatalf("cluster add events: %+v", got)
	}
	if raw := string(got[0].Data); strings.Contains(raw, `"secret":`) {
		t.Fatalf("a cluster event carries a secret: %s", raw)
	}
	last := got[1].ID
	live.close()

	// Two changes while disconnected are replayed from Last-Event-ID, contiguous, with no reset.
	rg.must("POST", "/v1/placements/acme/data01/adopt", AdoptRequest{Cluster: "vast01"}, nil)
	rg.must("POST", "/v1/clusters", ClusterRequest{Name: "vast02", Cluster: rg.vast02.definition(true)}, nil)
	v = rg.dir.Snapshot().Version()
	back := rg.stream(last)
	got = back.until(eventTypeDir, versionEvent(v))
	if got[0].Type == eventTypeReset || seqOf(t, got[0].ID) != seqOf(t, last)+1 {
		t.Fatalf("replay after %s: %+v", last, got)
	}
	for i := 1; i < len(got); i++ {
		if seqOf(t, got[i].ID) != seqOf(t, got[i-1].ID)+1 {
			t.Fatalf("replay is not contiguous at %d: %+v", i, got)
		}
	}
	kinds := make([]string, 0, len(got))
	for _, ev := range got {
		var d DirectoryEvent
		_ = json.Unmarshal(ev.Data, &d)
		kinds = append(kinds, d.Kind+"/"+d.Key)
	}
	if want := "tenant/acme placement/acme/data01 version/ cluster/vast02 version/"; strings.Join(kinds, " ") != want {
		t.Fatalf("replayed records: %v, want %s", kinds, want)
	}
	last = got[len(got)-1].ID
	back.close()

	// A fence event follows an operation, and a foreign id or one older than the ring resets.
	foreign := rg.stream("other:1")
	if ev, ok := foreign.next(5 * time.Second); !ok || ev.Type != eventTypeReset || ev.ID != "other:1" {
		t.Fatalf("foreign id: %+v %v", ev, ok)
	}
	foreign.close()
	rg.must("POST", "/v1/placements/acme/data01/expand", ExpandRequest{To: "vast02", Create: true}, nil)
	var tr TransitionResult
	rg.must("POST", "/v1/placements/acme/data01/ramp", RampRequest{Ratio: 0.5}, &tr)
	fenced := rg.stream("")
	rg.must("POST", "/v1/placements/acme/data01/ramp", RampRequest{Ratio: 1}, &tr)
	got = fenced.until(eventTypeFence, func(raw json.RawMessage) bool {
		var f FenceEvent
		return json.Unmarshal(raw, &f) == nil && f.Operation == tr.Operation && f.Status == StatusSucceeded
	})
	var f FenceEvent
	_ = json.Unmarshal(got[len(got)-1].Data, &f)
	if f.Kind != OpRamp || f.Placement != "acme/data01" || f.Phase != PhaseDone || f.Version != tr.Version {
		t.Fatalf("fence event: %+v", f)
	}
	fenced.close()
	old := rg.stream(last) // the ring holds 8 events; more than that have passed since
	if ev, ok := old.next(5 * time.Second); !ok || ev.Type != eventTypeReset {
		t.Fatalf("an id older than the ring: %+v %v", ev, ok)
	}
}

// A listener's read timeout bounds reading the request, not the stream: keepalives keep flowing
// past it.
func TestEventsOutliveReadTimeout(t *testing.T) {
	rg := newRig(t)
	rg.events.Keepalive = 100 * time.Millisecond
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: rg.ctl.Handler(), ReadTimeout: 300 * time.Millisecond, ReadHeaderTimeout: 300 * time.Millisecond}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	resp, err := http.Get("http://" + ln.Addr().String() + "/v1/events")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	s := &stream{t: t, resp: resp, sc: bufio.NewScanner(resp.Body)}
	start := time.Now()
	keepalives := 0
	for time.Since(start) < time.Second {
		ev, ok := s.next(2 * time.Second)
		if !ok {
			t.Fatalf("the stream ended after %s and %d keepalives", time.Since(start), keepalives)
		}
		if ev.Type == ":" {
			keepalives++
		}
	}
	if keepalives < 5 {
		t.Fatalf("%d keepalives in a second", keepalives)
	}
}

func TestViews(t *testing.T) {
	rg := newRig(t)
	rg.prepare()
	rg.must("POST", "/v1/placements/acme/data01/ramp", RampRequest{Ratio: 0.5}, nil)

	var cv ClusterView
	rg.must("GET", "/v1/clusters/vast01/view", nil, &cv)
	if !cv.Probe.Reachable || cv.Probe.Error != "" || cv.Probe.CheckedAt.IsZero() || cv.Name != "vast01" || cv.Type != "vast" {
		t.Fatalf("cluster view: %+v", cv)
	}
	if !cv.Capabilities.ConditionalWrite.Known || !cv.Capabilities.ConditionalWrite.Value || cv.Capabilities.ConditionalDelete.Known {
		t.Fatalf("capabilities: %+v", cv.Capabilities)
	}
	if len(cv.References) != 2 {
		t.Fatalf("references: %v", cv.References)
	}
	rg.answers("GET", "/v1/clusters/nope/view", nil, http.StatusNotFound, "not_found")
	rg.vast02.srv.Close()
	rg.must("GET", "/v1/clusters/vast02/view", nil, &cv)
	if cv.Probe.Reachable || cv.Probe.Error == "" {
		t.Fatalf("an unreachable cluster probes reachable: %+v", cv.Probe)
	}

	var pv PlacementView
	code, raw := rg.call("GET", "/v1/placements/acme/data01/view", nil, &pv)
	if code != http.StatusOK {
		t.Fatalf("placement view: HTTP %d %s", code, raw)
	}
	if strings.Contains(raw, `"secret":`) || strings.Contains(raw, `"secrets"`) {
		t.Fatalf("the view carries a secret: %s", raw)
	}
	if pv.State != directory.StateRamping || pv.Fence.Version != rg.dir.Snapshot().Version() || pv.Fence.Held || pv.Fence.Proxies != 0 || len(pv.Fence.WaitingOn) != 0 {
		t.Fatalf("placement view: %+v", pv)
	}
	if pv.SourceUploadsInFlight == nil || *pv.SourceUploadsInFlight != 0 || len(pv.Operations) != 0 || len(pv.Clusters) != 2 {
		t.Fatalf("placement view details: uploads %v ops %v clusters %v", pv.SourceUploadsInFlight, pv.Operations, pv.Clusters)
	}
	rg.answers("GET", "/v1/placements/acme/nope/view", nil, http.StatusNotFound, "not_found")

	var page AuditPage
	rg.must("GET", "/v1/audit?limit=2", nil, &page)
	if len(page.Changes) != 2 || page.Changes[0].Version <= page.Changes[1].Version || page.Changes[0].Actor != "api:127.0.0.1" || page.Changes[0].Op != "set-state" {
		t.Fatalf("audit: %+v", page.Changes)
	}
	before := page.Changes[1].Version - 1
	rg.must("GET", "/v1/audit?limit=100&before="+itoa(before), nil, &page)
	if len(page.Changes) == 0 || page.Changes[0].Version != before {
		t.Fatalf("audit before %d: %+v", before, page.Changes)
	}
	rg.answers("GET", "/v1/audit?limit=x", nil, http.StatusBadRequest, "bad_request")
}

func itoa(v int64) string { return json.Number(strings.TrimSpace(string(mustJSON(v)))).String() }

func mustJSON(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}

func TestRouteTableIsUnique(t *testing.T) {
	seen := map[string]bool{}
	for _, r := range Routes() {
		k := r.Method + " " + r.Pattern
		if seen[k] {
			t.Errorf("route %s is listed twice", k)
		}
		seen[k] = true
		if r.Mutation != (r.Method != http.MethodGet) {
			t.Errorf("route %s: mutation %v", k, r.Mutation)
		}
	}
	if len(seen) != 29 {
		t.Errorf("%d routes; update this count with the route table", len(seen))
	}
}

func BenchmarkEventsPublish(b *testing.B) {
	e := NewEvents(4096)
	for range 8 {
		_, ch, _, _, cancel := e.Subscribe("")
		defer cancel()
		go func() {
			for range ch {
			}
		}()
	}
	data := DirectoryEvent{Version: 1, Kind: "placement", Key: "t/b", Op: "put", Record: directory.Placement{State: directory.StateActive, Primary: "c"}}
	b.ReportAllocs()
	for b.Loop() {
		e.Publish(eventTypeDir, data)
	}
}
