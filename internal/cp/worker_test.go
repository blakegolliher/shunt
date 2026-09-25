package cp

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blakegolliher/shunt/internal/config"
	"github.com/blakegolliher/shunt/internal/control"
	"github.com/blakegolliher/shunt/internal/directory"
)

// resolve-worker across two control nodes on embedded etcd. Node a owns an external mover and
// runs its tracker; the worker's heartbeat goes through node b, which writes the durable record
// alone and moves its sequence past a's tracker. One resolution through a must land on the
// record, and the run must end failed with the evidence and free the placement.
func TestResolveWorkerAfterHeartbeatThroughAnotherNodeOnEtcd(t *testing.T) {
	tc := startCluster(t, 2)
	key := make([]byte, 32)
	key[5] = 21
	a, b := startNode(t, tc, 0, key, time.Second), startNode(t, tc, 1, key, time.Second)
	src, dst := fakeS3(t), fakeS3(t)
	cw := true
	def := func(srv *httptest.Server, name string) config.Cluster {
		return config.Cluster{Type: "minio", Scheme: "http", Region: "us-east-1", Endpoints: []string{strings.TrimPrefix(srv.URL, "http://")},
			Credentials: config.Credentials{AccessKey: "AK", SecretRef: "control:" + name}, Capabilities: config.Capabilities{ConditionalWrite: &cw}}
	}
	a.must("POST", "/v1/clusters", control.ClusterRequest{Name: "vast01", Cluster: def(src, "vast01"), Secret: "s1"}, nil)
	a.must("POST", "/v1/clusters", control.ClusterRequest{Name: "vast02", Cluster: def(dst, "vast02"), Secret: "s2"}, nil)
	a.must("POST", "/v1/placements/acme/data01/adopt", control.AdoptRequest{Cluster: "vast01"}, nil)
	a.must("POST", "/v1/placements/acme/data01/expand", control.ExpandRequest{To: "vast02", Create: true}, nil)
	a.must("POST", "/v1/placements/acme/data01/ramp", control.RampRequest{Ratio: 1}, nil)
	a.must("POST", "/v1/placements/acme/data01/migrate", control.MigrateRequest{}, nil)
	if err := b.store.WaitVersion(context.Background(), a.store.Version()); err != nil {
		t.Fatal(err)
	}

	var clock atomic.Int64
	clock.Store(time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC).UnixNano())
	now := func() time.Time { return time.Unix(0, clock.Load()).UTC() }
	a.ctl.Now, a.ctl.WorkerTTL = now, time.Second
	b.ctl.Now, b.ctl.WorkerTTL = now, time.Second
	// a's run parks in its poll and writes nothing until the test wakes it.
	parked, wake := make(chan struct{}, 64), make(chan struct{}, 64)
	a.ctl.Sleep = func(ctx context.Context, _ time.Duration) error {
		parked <- struct{}{}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-wake:
			return nil
		}
	}
	durable := func(id string) *control.Operation {
		t.Helper()
		op, err := a.ops.Get(context.Background(), id)
		if err != nil || op == nil {
			t.Fatalf("operation %s: %v %v", id, op, err)
		}
		return op
	}

	const session = "5555666677778888999900001111aaaa"
	args, _ := json.Marshal(control.MoverRequest{External: true, Session: session, Wait: "0s"})
	var started control.Operation
	if code, raw := a.call("POST", "/v1/operations", control.OperationRequest{Kind: control.OpMover, Placement: "acme/data01", Args: args}, &started); code != http.StatusAccepted {
		t.Fatalf("start external mover on a: HTTP %d %s", code, raw)
	}
	select {
	case <-parked:
	case <-time.After(10 * time.Second):
		t.Fatal("the mover's run on a did not park")
	}
	before := durable(started.ID)

	file := b.store.Snapshot().File()
	beat := control.WorkerHeartbeat{Session: session, Identity: file.Identity,
		Generation: file.Generation(directory.PlacementResource("acme/data01")), Sequence: 1, Inflight: 1, Uncertain: 1}
	b.must("POST", "/v1/operations/"+started.ID+"/worker-heartbeat", beat, nil)
	if after := durable(started.ID); after.Sequence <= before.Sequence || after.Worker == nil || after.Worker.Sequence != 1 {
		t.Fatalf("precondition: the heartbeat through b did not advance the record past %d: %+v", before.Sequence, after)
	}

	clock.Add(int64(2 * time.Second))
	const why = "the mover host was powered off at 12:00; the backend's request log shows nothing from it since"
	a.must("POST", "/v1/operations/"+started.ID+"/resolve-worker", control.ResolveWorkerRequest{Session: session, Attestation: why}, nil)
	rec := durable(started.ID)
	if w := rec.Worker; w == nil || w.ID != session || w.State != control.WorkerCompleted || w.ResolvedBy != "api:127.0.0.1" || w.Attestation != why {
		t.Fatalf("resolve-worker answered 200 but the durable record (sequence %d) has worker %+v", rec.Sequence, rec.Worker)
	}
	// The old mover's late heartbeat through b cannot reopen the resolved session.
	beat.Sequence = 3
	b.refused("POST", "/v1/operations/"+started.ID+"/worker-heartbeat", beat, "has completed")

	wake <- struct{}{}
	var done *control.Operation
	waitFor(t, 10*time.Second, "the resolved mover did not end", func() bool {
		done = durable(started.ID)
		return done.Terminal()
	})
	if done.Status != control.StatusFailed || done.Error == nil || !strings.Contains(done.Error.Message, "resolved by api:127.0.0.1") ||
		done.Worker == nil || done.Worker.State != control.WorkerCompleted || done.Worker.Attestation != why || done.Worker.ResolvedBy != "api:127.0.0.1" {
		t.Fatalf("the resolved mover's record: %+v worker %+v", done, done.Worker)
	}
	// The placement is free: a new mover is accepted, through either node.
	args2, _ := json.Marshal(control.MoverRequest{External: true, Session: "bbbbaaaa99998888777766665555ffff", Wait: "0s"})
	var next control.Operation
	if code, raw := b.call("POST", "/v1/operations", control.OperationRequest{Kind: control.OpMover, Placement: "acme/data01", Args: args2}, &next); code != http.StatusAccepted {
		t.Fatalf("a new mover after the resolution: HTTP %d %s", code, raw)
	}
	b.must("POST", "/v1/operations/"+next.ID+"/worker-heartbeat", control.WorkerHeartbeat{Session: "bbbbaaaa99998888777766665555ffff",
		Identity: file.Identity, Generation: beat.Generation, Sequence: 1, Complete: true}, nil)
	waitFor(t, 10*time.Second, "the next mover did not end", func() bool { return durable(next.ID).Terminal() })
}
