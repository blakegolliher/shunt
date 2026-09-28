package control

import (
	"net/http"
	"testing"

	"github.com/blakegolliher/shunt/internal/directory"
)

// A mover's report is bound to the move it describes (third review, R3-02). Two scoped moves of one
// bucket share their clusters: archive/ converges, is cut over and purged; then data/ starts. The
// archive/ move's converged report, replayed, is refused as superseded, so cutover of data/ has no
// evidence and is refused, with data/a only on the source. A report that names no move is refused.
// The data/ move's own converged report, once data/a is copied, cuts it over. Negative control:
// without the ingestion check the replay is accepted and authorizes data/'s cutover.
func TestOldMoveReportCannotAuthorizeCutover(t *testing.T) {
	rg := newRig(t)
	if err := rg.vast01.be.CreateBucket("data01"); err != nil {
		t.Fatal(err)
	}
	rg.vast01.put(t, "data01", "archive/a", "archived")
	rg.vast01.put(t, "data01", "data/a", "not yet copied")
	rg.must("POST", "/v1/clusters", ClusterRequest{Name: "vast01", Cluster: rg.vast01.definition(true)}, nil)
	rg.must("POST", "/v1/clusters", ClusterRequest{Name: "vast02", Cluster: rg.vast02.definition(true)}, nil)
	const key = "acme/data01"
	base := "/v1/placements/" + key
	rg.must("POST", base+"/adopt", AdoptRequest{Cluster: "vast01"}, nil)
	rg.must("POST", base+"/prefixes", PrefixRequest{Prefix: "archive/"}, nil)
	rg.must("POST", base+"/prefixes", PrefixRequest{Prefix: "data/"}, nil)
	rg.must("POST", base+"/migrate", MigrateRequest{Scope: "archive/", To: "vast02", Name: "data01-target", Create: true}, nil)
	rg.vast02.put(t, "data01-target", "archive/a", "archived")
	old := rg.current(key, Progress{Source: "vast01", Primary: "vast02", Pass: 2, Done: true, Converged: true})
	rg.must("POST", base+"/mover-progress", old, nil)
	rg.must("POST", base+"/cutover", CutoverRequest{Window: "1s"}, nil)
	var dry PurgeDryRun
	rg.must("POST", base+"/purge-source", PurgeRequest{DryRun: true}, &dry)
	rg.must("POST", base+"/purge-source", PurgeRequest{Token: dry.Token}, nil)

	rg.must("POST", base+"/migrate", MigrateRequest{Scope: "data/", To: "vast02"}, nil)
	rg.refused("POST", base+"/mover-progress", old, "has been superseded")
	rg.refused("POST", base+"/cutover", CutoverRequest{Window: "1s"}, "no mover has reported") // finishing archive/ forgot its report
	if p, _ := rg.dir.Snapshot().Lookup("acme", "data01"); p.State != directory.StateMigrating {
		t.Fatalf("data/ after the refused cutover: %s", p.State)
	}
	if !rg.vast01.has("data01", "data/a") || rg.vast02.has("data01-target", "data/a") {
		t.Fatal("data/a moved without a mover")
	}
	unbound := Progress{Source: "vast01", Primary: "vast02", Pass: 1, Done: true, Converged: true}
	if code, raw := rg.call("POST", base+"/mover-progress", unbound, nil); code != http.StatusBadRequest {
		t.Fatalf("a report that names no move: %d %s", code, raw)
	}

	rg.vast02.put(t, "data01-target", "data/a", "not yet copied") // the data/ mover's work
	rg.must("POST", base+"/mover-progress", rg.current(key, Progress{Source: "vast01", Primary: "vast02", Pass: 2, Done: true, Converged: true}), nil)
	rg.must("POST", base+"/cutover", CutoverRequest{Window: "1s"}, nil)
	if p, _ := rg.dir.Snapshot().Lookup("acme", "data01"); p.State != directory.StateCutover {
		t.Fatalf("data/ after its own mover converged: %s", p.State)
	}
}

// A converged report goes stale with the placement it was made on (R3-02): the placement changes after
// the mover's last pass (a read-only switched on and off: another writer's pass through the
// placement), and cutover refuses the report, which describes an earlier state, until the mover
// reports on the current one. Negative control: without cutover's generation check the stale report
// authorizes the cutover.
func TestMoverReportGoesStaleWithThePlacement(t *testing.T) {
	rg := newRig(t)
	rg.prepare()
	const key = "acme/data01"
	base := "/v1/placements/" + key
	rg.must("POST", base+"/migrate", MigrateRequest{}, nil)
	rg.must("POST", base+"/mover-progress", rg.current(key, Progress{Source: "vast01", Primary: "vast02", Pass: 2, Done: true, Converged: true}), nil)
	rg.must("POST", base+"/read-only", ReadOnlyRequest{ReadOnly: true}, nil)
	rg.must("POST", base+"/read-only", ReadOnlyRequest{ReadOnly: false}, nil)
	rg.refused("POST", base+"/cutover", CutoverRequest{Window: "1s"}, "describes an earlier move or state")
	var st PlacementView
	rg.must("GET", base+"/view", nil, &st)
	if st.Mover != nil {
		t.Fatalf("status shows a report from an earlier state as this move's: %+v", st.Mover)
	}
	rg.must("POST", base+"/mover-progress", rg.current(key, Progress{Source: "vast01", Primary: "vast02", Pass: 3, Done: true, Converged: true}), nil)
	rg.must("GET", base+"/view", nil, &st)
	if st.Mover == nil || !st.Mover.Converged || st.Mover.Pass != 3 {
		t.Fatalf("status of the current move's report: %+v", st.Mover)
	}
	rg.must("POST", base+"/cutover", CutoverRequest{Window: "1s"}, nil)
}
