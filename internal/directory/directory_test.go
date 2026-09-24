package directory

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/blakegolliher/shunt/internal/config"
	"github.com/blakegolliher/shunt/internal/s3"
)

func sampleClusters(t testing.TB) map[string]config.Cluster {
	t.Helper()
	c, err := config.Load("testdata/clusters.yaml")
	if err != nil {
		t.Fatal(err)
	}
	return c.Clusters
}

func expectLine(t *testing.T, path string) string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Scan()
	line := sc.Text()
	if !strings.HasPrefix(line, "# expect: ") {
		t.Fatalf("%s: first line must be '# expect: <key>'", path)
	}
	return strings.TrimPrefix(line, "# expect: ")
}

func TestValidSample(t *testing.T) {
	f, err := Load("testdata/valid/mixed.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if f.Version != 7 || len(f.Placements) != 7 || len(f.Tenants) != 2 || len(f.Clusters) != 6 ||
		f.Clusters["vast-a"].EndpointMode != "static" || f.Clusters["aws-use1"].EndpointMode != "dns" {
		t.Fatalf("shape: version %d, %d placements, %d tenants", f.Version, len(f.Placements), len(f.Tenants))
	}
	s := newSnapshot(f)
	p, ok := s.Lookup("acme", "runs")
	if !ok || p.State != StateRamping || p.Ramp == nil || len(p.Ramp.Prefixes) != 1 || p.Source != "vast-a" {
		t.Fatalf("acme/runs: %+v", p)
	}
	if got := s.Buckets("acme"); !slices.Equal(got, []string{"archive", "checkpoints", "runs", "training-sets"}) {
		t.Fatalf("buckets: %v", got)
	}
	if p, _ := s.Lookup("acme", "training-sets"); p.Created.IsZero() {
		t.Fatal("created not parsed")
	}
}

func TestInvalidSamplesNameTheKey(t *testing.T) {
	clusters := sampleClusters(t)
	files, _ := filepath.Glob("testdata/invalid/*.yaml")
	if len(files) < 20 {
		t.Fatalf("expected at least 20 invalid samples, found %d", len(files))
	}
	for _, f := range files {
		t.Run(filepath.Base(f), func(t *testing.T) {
			want := expectLine(t, f)
			_, err := loadSample(t, f, clusters)
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("want an error naming %q, got %v", want, err)
			}
		})
	}
}

func TestEmptyFileIsEmptyDirectory(t *testing.T) {
	f, err := parse([]byte("  \n# nothing\n"))
	if err != nil || f.Version != 0 || len(f.Placements) != 0 {
		t.Fatalf("%+v %v", f, err)
	}
	s := newSnapshot(f)
	if _, ok := s.Lookup("a", "b"); ok || s.Buckets("a") != nil {
		t.Fatal("empty snapshot returned something")
	}
}

func active(primary string) Placement {
	return Placement{State: StateActive, Primary: primary, Names: map[string]string{primary: "data"}}
}

func TestReadOnlyMutations(t *testing.T) {
	f := &File{Clusters: map[string]config.Cluster{"one": {}}, Placements: map[string]Placement{"acme/data": active("one")}}
	if err := f.SetPlacementReadOnly("acme", "data", true, false, ""); err != nil {
		t.Fatal(err)
	}
	if p := f.Placements["acme/data"]; !p.ReadOnly || p.RejectWrites {
		t.Fatalf("placement flags: %+v", p)
	}
	if err := f.SetClusterReadOnly("one", true, true, ""); err != nil {
		t.Fatal(err)
	}
	if c := f.Clusters["one"]; !c.ReadOnly || !c.RejectWrites {
		t.Fatalf("cluster flags: %+v", c)
	}
	if err := f.SetPlacementReadOnly("acme", "data", false, true, ""); err != nil {
		t.Fatal(err)
	}
	if p := f.Placements["acme/data"]; p.ReadOnly || p.RejectWrites {
		t.Fatalf("writable placement retained flags: %+v", p)
	}
}

// every state pair: the six legal transitions succeed with the arguments they need, and every
// other pair is refused naming both states.
func TestTransitionMatrix(t *testing.T) {
	placements := map[string]Placement{
		StateActive:    active("garage"),
		StateRamping:   {State: StateRamping, Primary: "minio", Source: "garage", Ramp: &Ramp{Hash: RampHash, Ratio: 0.1}, Names: map[string]string{"garage": "data", "minio": "data2"}},
		StateMigrating: {State: StateMigrating, Primary: "minio", Source: "garage", Names: map[string]string{"garage": "data", "minio": "data2"}},
		StateCutover:   {State: StateCutover, Primary: "minio", Source: "garage", Names: map[string]string{"garage": "data", "minio": "data2"}},
	}
	args := func(from, to string) Transition {
		t := Transition{To: to}
		if from == StateActive {
			t.Target, t.Name = "minio", "data2"
		}
		if to == StateRamping {
			t.Ratio = 0.5
		}
		return t
	}
	clusters := sampleClusters(t)
	legalCount := 0
	for _, from := range States {
		for _, to := range States {
			np, err := Apply(placements[from], args(from, to))
			wantLegal := legal[[2]string{from, to}]
			if !wantLegal {
				var te *TransitionError
				if !errors.As(err, &te) || te.From != from || te.To != to || !strings.Contains(err.Error(), from+" -> "+to) {
					t.Errorf("%s -> %s: want TransitionError naming both states, got %v", from, to, err)
				}
				continue
			}
			legalCount++
			if err != nil {
				t.Errorf("%s -> %s: %v", from, to, err)
				continue
			}
			f := &File{Version: 1, Clusters: clusters, Tenants: map[string]Tenant{"acme": {DefaultCluster: "garage"}}, Placements: map[string]Placement{"acme/data": np}}
			if err := validate(f); err != nil {
				t.Errorf("%s -> %s produced an invalid placement: %v\n%+v", from, to, err, np)
			}
		}
	}
	if legalCount != 6 {
		t.Fatalf("legal transitions: %d", legalCount)
	}
}

func TestTransitionDetails(t *testing.T) {
	p := active("garage")
	if _, err := Apply(p, Transition{To: StateMigrating}); err == nil {
		t.Error("leaving ACTIVE without a target accepted")
	}
	if _, err := Apply(p, Transition{To: StateMigrating, Target: "garage", Name: "data"}); err == nil {
		t.Error("the primary's own bucket accepted as the target")
	}
	if _, err := Apply(p, Transition{To: StateMigrating, Target: "garage"}); err == nil {
		t.Error("the primary's cluster accepted as the target with no other bucket named")
	}
	if _, err := Apply(p, Transition{To: StateRamping, Target: "minio", Name: "x"}); err == nil {
		t.Error("RAMPING without ratio or prefix accepted")
	}
	r, err := Apply(p, Transition{To: StateRamping, Target: "minio", Name: "x", Prefixes: []string{"2026-09/"}})
	if err != nil || r.Source != "garage" || r.Primary != "minio" || r.Names["minio"] != "x" || p.Names["minio"] != "" {
		t.Fatalf("ACTIVE -> RAMPING: %+v %v (input mutated: %v)", r, err, p.Names)
	}
	if _, err := Apply(r, Transition{To: StateRamping, Ratio: 0}); err == nil {
		t.Error("empty ramp step accepted")
	}
	r2, err := Apply(r, Transition{To: StateRamping, Ratio: 0.3, Prefixes: []string{"2026-10/"}})
	if err != nil || r2.Ramp.Ratio != 0.3 || len(r2.Ramp.Prefixes) != 2 || len(r.Ramp.Prefixes) != 1 {
		t.Fatalf("ramp step: %+v %v", r2.Ramp, err)
	}
	if r.Ramp.Hash != RampHash || r2.Ramp.Hash != RampHash {
		t.Errorf("the ramp hash is written at RAMPING start and kept: %q %q", r.Ramp.Hash, r2.Ramp.Hash)
	}
	foreign := r2
	foreign.Ramp = &Ramp{Hash: "fnv1a-v0", Ratio: 0.3}
	if _, err := Apply(foreign, Transition{To: StateRamping, Ratio: 0.5}); err == nil || !strings.Contains(err.Error(), `"fnv1a-v0"`) {
		t.Errorf("extending a ramp split by an unknown hash: %v", err)
	}
	if _, err := Apply(foreign, Transition{To: StateMigrating}); err != nil {
		t.Errorf("an unknown-hash ramp can still finish to MIGRATING, where the hash no longer matters: %v", err)
	}
	if _, err := Apply(r2, Transition{To: StateRamping, Ratio: 0.2}); err == nil || !strings.Contains(err.Error(), "only grows") {
		t.Errorf("shrinking ramp: %v", err)
	}
	if _, err := Apply(r2, Transition{To: StateMigrating, Target: "vast"}); err == nil {
		t.Error("target on a non-ACTIVE transition accepted")
	}
	m, _ := Apply(r2, Transition{To: StateMigrating})
	c, _ := Apply(m, Transition{To: StateCutover})
	a, err := Apply(c, Transition{To: StateActive})
	if err != nil || m.Ramp != nil || a.Source != "" || a.Primary != "minio" || len(a.Names) != 1 || a.Names["minio"] != "x" {
		t.Fatalf("to ACTIVE: %+v %v", a, err)
	}
	if _, err := Apply(p, Transition{To: "MOVING"}); err == nil || !strings.Contains(err.Error(), "unknown state") {
		t.Errorf("unknown state: %v", err)
	}
}

func TestBackendName(t *testing.T) {
	letters := "abcdefghijklmnopqrstuvwxyz0123456789"
	rng := rand.New(rand.NewPCG(1, 2))
	gen := func(minLen, maxLen int, extra string) string {
		n := minLen + rng.IntN(maxLen-minLen+1)
		b := make([]byte, n)
		for i := range b {
			pool := letters
			if i > 0 && i < n-1 {
				pool += extra
			}
			b[i] = pool[rng.IntN(len(pool))]
		}
		return string(b)
	}
	for i := 0; i < 10000; i++ {
		tenant := gen(1, 32, "-")
		bucket := gen(3, 63, "-.")
		if !validTenant(tenant) || !s3.ValidBucketName(bucket) {
			continue
		}
		name := BackendName(tenant, bucket, i%3)
		if !s3.ValidBucketName(name) || len(name) > 63 || name == bucket {
			t.Fatalf("BackendName(%q, %q) = %q", tenant, bucket, name)
		}
	}
	first := BackendName("e2e-a", "data", 0)
	if again := BackendName("e2e-a", "data", 0); again != first {
		t.Fatalf("not stable: %q then %q", first, again)
	}
	a, b := BackendName("e2e-a", "data", 0), BackendName("e2e-b", "data", 0)
	if a == b || !strings.HasPrefix(a, "e2e-a-") || !strings.HasSuffix(a, "-data") || len(a) != len("e2e-a-0000-data") {
		t.Fatalf("names: %q %q", a, b)
	}
	if BackendName("e2e-a", "data", 1) == a {
		t.Fatal("attempt does not change the name")
	}
}

// writeDir writes a directory file with the sample clusters and tenants acme and zed, and returns
// its path.
func writeDir(t testing.TB, version int64) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "directory.yaml")
	f := &File{Version: version, Clusters: sampleClusters(t),
		Tenants: map[string]Tenant{"acme": {DefaultCluster: "garage"}, "zed": {DefaultCluster: "minio"}}}
	body, err := marshal(f)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, body, 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// loadSample loads a directory sample that may leave out clusters: the sample clusters stand in.
func loadSample(t testing.TB, path string, clusters map[string]config.Cluster) (*File, error) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	f, err := parse(data)
	if err != nil {
		return nil, err
	}
	if f.Clusters == nil {
		f.Clusters = clusters
	}
	return f, validate(f)
}

func changes(t *testing.T, d *FileDir) []Change {
	t.Helper()
	data, err := os.ReadFile(d.ChangeLogPath())
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	out := make([]Change, 0, len(lines))
	for _, line := range lines {
		var c Change
		if err := json.Unmarshal([]byte(line), &c); err != nil {
			t.Fatalf("change log line %q: %v", line, err)
		}
		out = append(out, c)
	}
	return out
}

func TestFileDirWrites(t *testing.T) {
	ctx := context.Background()
	path := writeDir(t, 3)
	d, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Create(ctx, "acme", "data", "garage", "acme-1234-data", "test"); err != nil {
		t.Fatal(err)
	}
	s := d.Snapshot()
	p, ok := s.Lookup("acme", "data")
	if !ok || s.Version() != 4 || p.Primary != "garage" || p.Names["garage"] != "acme-1234-data" || p.Created.IsZero() {
		t.Fatalf("own write not visible: version %d %+v", s.Version(), p)
	}
	if err := d.Create(ctx, "acme", "data", "minio", "other", "test"); !errors.Is(err, ErrExists) {
		t.Fatalf("duplicate create: %v", err)
	}
	if err := d.Create(ctx, "nobody", "data", "garage", "nobody-0000-data", "test"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown tenant: %v", err)
	}
	if err := d.Create(ctx, "zed", "data", "garage", "acme-1234-data", "test"); err == nil || !strings.Contains(err.Error(), "already used by placement") {
		t.Fatalf("shared backend bucket accepted: %v", err)
	}
	// The failed writes left the file alone.
	if d.Snapshot().Version() != 4 {
		t.Fatalf("failed writes bumped the version to %d", d.Snapshot().Version())
	}
	if err := d.SetState(ctx, "acme", "data", StateRamping, Transition{To: StateMigrating}, "test"); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale from-state: %v", err)
	}
	if err := d.SetState(ctx, "acme", "data", StateActive, Transition{To: StateMigrating, Target: "minio", Name: "acme-9999-data"}, "op"); err != nil {
		t.Fatal(err)
	}
	if err := d.Delete(ctx, "acme", "data", "test"); !errors.Is(err, ErrConflict) {
		t.Fatalf("delete of a MIGRATING placement: %v", err)
	}
	if err := d.Delete(ctx, "acme", "missing", "test"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("delete missing: %v", err)
	}
	for _, to := range []string{StateCutover, StateActive} {
		from := d.Snapshot().File().Placements["acme/data"].State
		if err := d.SetState(ctx, "acme", "data", from, Transition{To: to}, "op"); err != nil {
			t.Fatal(err)
		}
	}
	if err := d.Delete(ctx, "acme", "data", "test"); err != nil {
		t.Fatal(err)
	}
	cs := changes(t, d)
	ops := make([]string, len(cs))
	for i, c := range cs {
		ops[i] = c.Op
	}
	if !slices.Equal(ops, []string{"create", "set-state", "set-state", "set-state", "delete"}) {
		t.Fatalf("change log ops: %v", ops)
	}
	if cs[0].Before != nil || cs[0].After == nil || cs[4].After != nil || cs[4].Before == nil || cs[1].Actor != "op" || cs[4].Version != 8 {
		t.Fatalf("change records: %+v", cs)
	}
	// A second instance opening the file sees the same content.
	d2, err := Open(path)
	if err != nil || d2.Snapshot().Version() != 8 {
		t.Fatalf("reopen: %v", err)
	}
}

func TestReload(t *testing.T) {
	ctx := context.Background()
	path := writeDir(t, 1)
	a, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	b, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := a.Reload(); changed || err != nil {
		t.Fatalf("unchanged file reloaded: %v %v", changed, err)
	}
	if err := b.Create(ctx, "zed", "logs", "minio", "zed-0000-logs", "b"); err != nil {
		t.Fatal(err)
	}
	if changed, err := a.Reload(); !changed || err != nil {
		t.Fatalf("other writer's change not loaded: %v %v", changed, err)
	}
	if _, ok := a.Snapshot().Lookup("zed", "logs"); !ok {
		t.Fatal("reloaded snapshot lacks the row")
	}
	// Invalid content keeps the last good snapshot, reports once.
	if err := os.WriteFile(path, []byte("version: 9\nplacements: [broken"), 0o644); err != nil {
		t.Fatal(err)
	}
	if changed, err := a.Reload(); changed || err == nil {
		t.Fatalf("broken file: %v %v", changed, err)
	}
	if changed, err := a.Reload(); changed || err != nil {
		t.Fatalf("broken file reported twice: %v %v", changed, err)
	}
	if a.Snapshot().Version() != 2 {
		t.Fatalf("last good snapshot lost: %d", a.Snapshot().Version())
	}
	// A version that does not increase is rejected.
	good, _ := marshal(a.Snapshot().File())
	stale := strings.Replace(string(good), "zed-0000-logs", "zed-0001-logs", 1)
	if err := os.WriteFile(path, []byte(stale), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Reload(); !errors.Is(err, errStaleVersion) {
		t.Fatalf("stale version: %v", err)
	}
	// Kubernetes ConfigMap style: path is a symlink swapped to a new target with a higher version.
	dir := t.TempDir()
	link := filepath.Join(dir, "directory.yaml")
	v1, v2 := filepath.Join(dir, "..v1"), filepath.Join(dir, "..v2")
	f := a.Snapshot().File()
	f.Version = 10
	out, _ := marshal(f)
	_ = os.WriteFile(v1, out, 0o644)
	if err := os.Symlink(v1, link); err != nil {
		t.Fatal(err)
	}
	c, err := Open(link)
	if err != nil {
		t.Fatal(err)
	}
	f.Version = 11
	delete(f.Placements, "zed/logs")
	out, _ = marshal(f)
	_ = os.WriteFile(v2, out, 0o644)
	tmpLink := link + ".new"
	_ = os.Symlink(v2, tmpLink)
	if err := os.Rename(tmpLink, link); err != nil {
		t.Fatal(err)
	}
	if changed, err := c.Reload(); !changed || err != nil || c.Snapshot().Version() != 11 {
		t.Fatalf("symlink swap: %v %v version %d", changed, err, c.Snapshot().Version())
	}
	if _, ok := c.Snapshot().Lookup("zed", "logs"); ok {
		t.Fatal("symlink swap content not installed")
	}
}

func TestReadOnlyDirectory(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	path := writeDir(t, 1)
	d, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Dir(path)
	if err := os.Chmod(dir, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })
	if err := d.Create(context.Background(), "acme", "data", "garage", "acme-0000-data", "t"); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("read-only directory: %v", err)
	}
	if _, ok := d.Snapshot().Lookup("acme", "data"); ok {
		t.Fatal("failed write visible in snapshot")
	}
}

func TestLockTimeout(t *testing.T) {
	path := writeDir(t, 1)
	d, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	lf, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer lf.Close()
	if err := syscall.Flock(int(lf.Fd()), syscall.LOCK_EX); err != nil {
		t.Fatal(err)
	}
	d.LockTimeout = 50 * time.Millisecond
	start := time.Now()
	if err := d.Create(context.Background(), "acme", "data", "garage", "acme-0000-data", "t"); !errors.Is(err, ErrLockTimeout) {
		t.Fatalf("held lock: %v", err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("lock timeout not honored")
	}
}

// TestConcurrentCreate races writers for the same key: goroutines on two FileDir instances and
// a separate process. Exactly one create wins; the file stays valid and the version counts every
// successful write.
func TestConcurrentCreate(t *testing.T) {
	if os.Getenv("SHUNT_DIRECTORY_HELPER") != "" {
		return
	}
	ctx := context.Background()
	path := writeDir(t, 1)
	a, _ := Open(path)
	b, _ := Open(path)
	var wins, exists int
	var mu sync.Mutex
	var wg sync.WaitGroup
	record := func(err error) {
		mu.Lock()
		defer mu.Unlock()
		switch {
		case err == nil:
			wins++
		case errors.Is(err, ErrExists):
			exists++
		default:
			t.Errorf("unexpected: %v", err)
		}
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestConcurrentCreateHelper$")
	cmd.Env = append(os.Environ(), "SHUNT_DIRECTORY_HELPER="+path)
	wg.Add(1)
	go func() {
		defer wg.Done()
		out, err := cmd.CombinedOutput()
		switch {
		case err != nil:
			t.Errorf("helper: %v\n%s", err, out)
		case strings.Contains(string(out), "HELPER-WON"):
			record(nil)
		case strings.Contains(string(out), "HELPER-EXISTS"):
			record(ErrExists)
		default:
			t.Errorf("helper output: %s", out)
		}
	}()
	for i := 0; i < 8; i++ {
		d := a
		if i%2 == 1 {
			d = b
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			record(d.Create(ctx, "acme", "race", "garage", "acme-0000-race", "g"))
		}()
	}
	// Unrelated writes interleave with the race and must all land.
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := b.Create(ctx, "zed", fmt.Sprintf("other-%d", i), "minio", fmt.Sprintf("zed-0000-other-%d", i), "g"); err != nil {
				t.Errorf("unrelated create: %v", err)
			}
		}()
	}
	wg.Wait()
	if wins != 1 || exists != 8 {
		t.Fatalf("wins %d exists %d", wins, exists)
	}
	f, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if f.Version != 1+1+4 || len(f.Placements) != 5 {
		t.Fatalf("version %d placements %d", f.Version, len(f.Placements))
	}
}

func TestConcurrentCreateHelper(t *testing.T) {
	path := os.Getenv("SHUNT_DIRECTORY_HELPER")
	if path == "" {
		t.Skip("helper process only")
	}
	d, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	switch err := d.Create(context.Background(), "acme", "race", "garage", "acme-0000-race", "subprocess"); {
	case err == nil:
		fmt.Println("HELPER-WON")
	case errors.Is(err, ErrExists):
		fmt.Println("HELPER-EXISTS")
	default:
		t.Fatal(err)
	}
}

func FuzzParse(f *testing.F) {
	files, _ := filepath.Glob("testdata/*/*.yaml")
	for _, p := range files {
		b, err := os.ReadFile(p)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(b)
	}
	f.Add([]byte("version: 1\nplacements: { a/b: { state: ACTIVE } }"))
	clusters := sampleClusters(f)
	f.Fuzz(func(t *testing.T, data []byte) {
		d, err := parse(data)
		if err != nil {
			return
		}
		if d.Clusters == nil {
			d.Clusters = clusters
		}
		if validate(d) != nil {
			return
		}
		out, err := marshal(d)
		if err != nil {
			t.Fatalf("marshal of a valid directory: %v", err)
		}
		back, err := parse(out)
		if err != nil {
			t.Fatalf("round trip does not parse: %v\n%s", err, out)
		}
		if err := validate(back); err != nil || back.Version != d.Version || len(back.Placements) != len(d.Placements) || len(back.Clusters) != len(d.Clusters) {
			t.Fatalf("round trip changed the directory: %v\n%s", err, out)
		}
		newSnapshot(back)
	})
}

func BenchmarkLookup(b *testing.B) {
	f := &File{Version: 1, Tenants: map[string]Tenant{}, Placements: map[string]Placement{}}
	for i := 0; i < 10000; i++ {
		f.Placements[fmt.Sprintf("tenant-%d/bucket-%d", i%50, i)] = active("garage")
	}
	s := newSnapshot(f)
	b.ReportAllocs()
	for b.Loop() {
		if _, ok := s.Lookup("tenant-7", "bucket-5007"); !ok {
			b.Fatal("miss")
		}
	}
}

func BenchmarkCreate(b *testing.B) {
	d, err := Open(writeDir(b, 1))
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		i++
		if err := d.Create(ctx, "acme", fmt.Sprintf("bucket-%d", i), "garage", fmt.Sprintf("acme-0000-bucket-%d", i), "bench"); err != nil {
			b.Fatal(err)
		}
	}
}

// POC-5: clusters are directory state. A cluster can be added, replaced, and removed once nothing
// names it; each change is logged with the definition (secret_ref only) before and after.
func TestClusterLifecycle(t *testing.T) {
	ctx := context.Background()
	d, err := Open(writeDir(t, 1))
	if err != nil {
		t.Fatal(err)
	}
	vast02 := config.Cluster{Type: "vast", Scheme: "http", Region: "us-east-1", Endpoints: []string{"10.0.0.2:80"},
		Credentials: config.Credentials{AccessKey: "AK2", SecretRef: "file:/etc/shunt/vast02.secret"}}
	if err := d.PutCluster(ctx, "vast02", vast02, "", "api:test"); err != nil {
		t.Fatal(err)
	}
	got, ok := d.Snapshot().Cluster("vast02")
	if !ok || got.EndpointMode != "static" || got.Credentials.SecretRef != "file:/etc/shunt/vast02.secret" {
		t.Fatalf("vast02 not added with defaults: %+v", got)
	}
	bad := vast02
	bad.Scheme = "ftp"
	if err := d.PutCluster(ctx, "vast03", bad, "", "api:test"); err == nil || !strings.Contains(err.Error(), "clusters.vast03.scheme") {
		t.Fatalf("an invalid cluster was accepted: %v", err)
	}
	// garage is the default cluster of tenant acme: in use.
	if err := d.RemoveCluster(ctx, "garage", "api:test"); !errors.Is(err, ErrInUse) || !strings.Contains(err.Error(), "tenants.acme.default_cluster") {
		t.Fatalf("removing a referenced cluster: %v", err)
	}
	if err := d.Adopt(ctx, "acme", "data01", "vast02", "data01", "api:test"); err != nil {
		t.Fatal(err)
	}
	if err := d.RemoveCluster(ctx, "vast02", "api:test"); !errors.Is(err, ErrInUse) || !strings.Contains(err.Error(), "placements.acme/data01") {
		t.Fatalf("removing a cluster a placement uses: %v", err)
	}
	if err := d.RemoveCluster(ctx, "nope", "api:test"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("removing an unknown cluster: %v", err)
	}
	if err := d.RemoveCluster(ctx, "cold", "api:test"); err != nil {
		t.Fatalf("removing an unused cluster: %v", err)
	}
	cs := changes(t, d)
	if len(cs) != 3 || cs[0].Op != "cluster-put" || cs[0].ClusterBefore != nil || cs[0].ClusterAfter == nil ||
		cs[2].Op != "cluster-remove" || cs[2].ClusterBefore == nil || cs[2].ClusterAfter != nil || cs[2].Key != "clusters/cold" {
		t.Fatalf("change records: %+v", cs)
	}
}

// Adopt creates the tenant when it is new; expand records a target the first transition inherits.
func TestAdoptExpandAndCutoverEvidence(t *testing.T) {
	ctx := context.Background()
	d, err := Open(writeDir(t, 1))
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Adopt(ctx, "newco", "data01", "garage", "data01", "api:test"); err != nil {
		t.Fatal(err)
	}
	if tn, ok := d.Snapshot().Tenant("newco"); !ok || tn.DefaultCluster != "garage" {
		t.Fatalf("tenant not created: %+v", tn)
	}
	if err := d.Adopt(ctx, "newco", "data01", "garage", "data01", "api:test"); !errors.Is(err, ErrExists) {
		t.Fatalf("second adopt: %v", err)
	}
	if err := d.SetTarget(ctx, "newco", "data01", "garage", "data01-001", "api:test"); err == nil {
		t.Fatal("a target equal to the primary was accepted")
	}
	if err := d.SetTarget(ctx, "newco", "data01", "minio", "data01-001", "api:test"); err != nil {
		t.Fatal(err)
	}
	if err := d.SetTarget(ctx, "newco", "data01", "cold", "data01-001", "api:test"); !errors.Is(err, ErrConflict) {
		t.Fatalf("a second target: %v", err)
	}
	// The ramp inherits the recorded target: no --to, and a different one is refused.
	if err := d.SetState(ctx, "newco", "data01", StateActive, Transition{To: StateRamping, Target: "cold", Ratio: 0.5}, "op"); err == nil {
		t.Fatal("a target other than the expanded one was accepted")
	}
	if err := d.SetState(ctx, "newco", "data01", StateActive, Transition{To: StateRamping, Ratio: 0.5}, "op"); err != nil {
		t.Fatal(err)
	}
	p, _ := d.Snapshot().Lookup("newco", "data01")
	if p.Primary != "minio" || p.Source != "garage" || p.Target != "" || p.Names["minio"] != "data01-001" {
		t.Fatalf("ramp from the recorded target: %+v", p)
	}
	for _, to := range []string{StateMigrating} {
		if err := d.SetState(ctx, "newco", "data01", p.State, Transition{To: to}, "op"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := Apply(*p, Transition{To: StateRamping, Ratio: 0.9, Cutover: &CutoverEvidence{}}); err == nil {
		t.Error("cutover evidence on a ramp step was accepted")
	}
	ev := &CutoverEvidence{At: time.Unix(1_800_000_000, 0).UTC(), Window: time.Minute, FallbackReads: 12}
	if err := d.SetState(ctx, "newco", "data01", StateMigrating, Transition{To: StateCutover, Cutover: ev}, "op"); err != nil {
		t.Fatal(err)
	}
	if p, _ := d.Snapshot().Lookup("newco", "data01"); p.Cutover == nil || p.Cutover.Window != time.Minute || p.Cutover.FallbackReads != 12 {
		t.Fatalf("cutover evidence not recorded: %+v", p.Cutover)
	}
	if err := d.SetState(ctx, "newco", "data01", StateCutover, Transition{To: StateActive}, "op"); err != nil {
		t.Fatal(err)
	}
	if p, _ := d.Snapshot().Lookup("newco", "data01"); p.Cutover != nil || p.Source != "" || len(p.Names) != 1 {
		t.Fatalf("ACTIVE keeps cutover state: %+v", p)
	}
	// Evidence outside CUTOVER is invalid on disk.
	f := d.Snapshot().File()
	pl := f.Placements["newco/data01"]
	pl.Cutover = ev
	f.Placements["newco/data01"] = pl
	if err := validate(f); err == nil || !strings.Contains(err.Error(), "placements.newco/data01.cutover") {
		t.Fatalf("cutover evidence on an ACTIVE placement: %v", err)
	}
}

// T02 on the file backend: a candidate whose write never reaches disk is never committed, so
// what it prepared (the proxy's clusters) never goes live.
func TestPrepareIsNotCommittedWhenTheWriteFails(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root writes into a read-only directory")
	}
	ctx := context.Background()
	path := writeDir(t, 1)
	d, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	var prepared, committed []int64
	d.Prepare = func(f *File, _ func(string) (string, error)) (func(), error) {
		prepared = append(prepared, f.Version)
		return func() { committed = append(committed, f.Version) }, nil
	}
	if err := d.Create(ctx, "acme", "data", "garage", "acme-0000-data", "t"); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Dir(path)
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	if err := d.Create(ctx, "acme", "more", "garage", "acme-0000-more", "t"); err == nil {
		t.Fatal("a write into a read-only directory succeeded")
	}
	if len(prepared) != 2 || len(committed) != 1 || committed[0] != 2 {
		t.Fatalf("prepared %v, committed %v: want 2 and 3 prepared, only 2 committed", prepared, committed)
	}
	if d.Snapshot().Version() != 2 {
		t.Fatalf("installed version %d after a failed write", d.Snapshot().Version())
	}
}

// Prepare runs before a version is installed and can refuse it; OnInstall runs after, on writes
// and reloads alike.
func TestPrepareAndOnInstallHooks(t *testing.T) {
	ctx := context.Background()
	path := writeDir(t, 1)
	d, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	var installed []int64
	d.OnInstall = func(s *Snapshot) { installed = append(installed, s.Version()) }
	refuse := errors.New("cannot build cluster")
	d.Prepare = func(f *File, _ func(string) (string, error)) (func(), error) {
		if _, ok := f.Clusters["broken"]; ok {
			return nil, refuse
		}
		return func() {}, nil
	}
	broken := config.Cluster{Type: "s3", Scheme: "http", Region: "r", Endpoints: []string{"10.0.0.9:80"}, Credentials: config.Credentials{AccessKey: "A", SecretRef: "env:NOPE"}}
	if err := d.PutCluster(ctx, "broken", broken, "", "t"); !errors.Is(err, refuse) {
		t.Fatalf("Prepare did not refuse the write: %v", err)
	}
	if on, _ := Load(path); on.Version != 1 {
		t.Fatalf("a refused write reached disk: version %d", on.Version)
	}
	if err := d.Create(ctx, "acme", "data", "garage", "acme-0000-data", "t"); err != nil {
		t.Fatal(err)
	}
	// Another writer adds the broken cluster; the reload is refused and the last good version stays.
	other, _ := Open(path)
	if err := other.PutCluster(ctx, "broken", broken, "", "t"); err != nil {
		t.Fatal(err)
	}
	if changed, err := d.Reload(); changed || !errors.Is(err, refuse) {
		t.Fatalf("reload of a version Prepare refuses: %v %v", changed, err)
	}
	if d.Snapshot().Version() != 2 {
		t.Fatalf("refused reload installed version %d", d.Snapshot().Version())
	}
	if err := other.RemoveCluster(ctx, "broken", "t"); err != nil {
		t.Fatal(err)
	}
	if changed, err := d.Reload(); !changed || err != nil {
		t.Fatalf("reload after the fix: %v %v", changed, err)
	}
	if !slices.Equal(installed, []int64{2, 4}) {
		t.Fatalf("OnInstall versions: %v", installed)
	}
}

// TestHeldSteps covers ADR-0016's hold: a step written as a hold keeps the ramp in force, completes
// only to what it held, and a release puts the placement back exactly as it was.
func TestHeldSteps(t *testing.T) {
	clusters := sampleClusters(t)
	valid := func(name string, p Placement) {
		t.Helper()
		f := &File{Version: 1, Clusters: clusters, Tenants: map[string]Tenant{"acme": {DefaultCluster: "garage"}}, Placements: map[string]Placement{"acme/data": p}}
		if err := validate(f); err != nil {
			t.Errorf("%s produced an invalid placement: %v\n%+v", name, err, p)
		}
	}
	a := active("garage")
	a.Target, a.Names["minio"] = "minio", "data2" // expanded

	// ACTIVE, held: RAMPING with nothing in force and the step in hold.
	h, err := Apply(a, Transition{To: StateRamping, Ratio: 0.5, Hold: true})
	if err != nil {
		t.Fatal(err)
	}
	if h.State != StateRamping || h.Primary != "minio" || h.Source != "garage" || h.Ramp.Ratio != 0 || h.Ramp.Hold == nil || h.Ramp.Hold.Ratio != 0.5 || h.Ramp.Hash != RampHash || !h.Held() {
		t.Fatalf("held from ACTIVE: %+v ramp %+v", h, h.Ramp)
	}
	valid("held from ACTIVE", h)
	if _, err := Apply(h, Transition{To: StateRamping, Ratio: 0.8, Hold: true}); err == nil {
		t.Error("a second hold on top of a hold accepted")
	}
	if _, err := Apply(h, Transition{To: StateRamping, Ratio: 0.8}); err == nil {
		t.Error("completing a held step beyond what it held accepted")
	}
	if _, err := Apply(h, Transition{To: StateMigrating}); err == nil {
		t.Error("MIGRATING over a held ramp step accepted")
	}
	done, err := Apply(h, Transition{To: StateRamping, Ratio: 0.5, Complete: true})
	if err != nil || done.Ramp.Ratio != 0.5 || done.Ramp.Hold != nil || done.Held() {
		t.Fatalf("completing the hold: %+v %v", done.Ramp, err)
	}
	// Completing is a compare-and-swap on the hold: once it is released (or completed) by another
	// call, completing again must fail rather than move writes no proxy held.
	if _, err := Apply(done, Transition{To: StateRamping, Ratio: 0.8, Complete: true}); err == nil {
		t.Error("completing a step with nothing held accepted")
	}
	valid("completed hold", done)

	// Releasing a hold from ACTIVE restores ACTIVE with the target recorded.
	back, err := Apply(h, Transition{Release: true})
	if err != nil || back.State != StateActive || back.Primary != "garage" || back.Source != "" || back.Target != "minio" || back.Ramp != nil || back.Names["minio"] != "data2" {
		t.Fatalf("release from ACTIVE: %+v %v", back, err)
	}
	valid("released to ACTIVE", back)
	if _, err := Apply(back, Transition{Release: true}); err == nil {
		t.Error("release with nothing held accepted")
	}

	// A held step on a running ramp keeps the ramp in force; a prefix step holds its prefix.
	hp, err := Apply(done, Transition{To: StateRamping, Prefixes: []string{"runs/"}, Hold: true})
	if err != nil || hp.Ramp.Ratio != 0.5 || hp.Ramp.Hold == nil || hp.Ramp.Hold.Ratio != 0.5 || !slices.Equal(hp.Ramp.Hold.Prefixes, []string{"runs/"}) {
		t.Fatalf("held prefix step: %+v %v", hp.Ramp, err)
	}
	if _, err := Apply(hp, Transition{To: StateRamping, Prefixes: []string{"other/"}}); err == nil {
		t.Error("completing a held prefix step with a different prefix accepted")
	}
	rel, err := Apply(hp, Transition{Release: true})
	if err != nil || rel.State != StateRamping || rel.Ramp.Ratio != 0.5 || rel.Ramp.Hold != nil || len(rel.Ramp.Prefixes) != 0 {
		t.Fatalf("release on a running ramp: %+v %v", rel.Ramp, err)
	}
	if hp.Ramp.Hold == nil {
		t.Error("release mutated its input")
	}

	// migrate start, held: RAMPING with hold.ratio 1, then MIGRATING completes it.
	hm, err := Apply(done, Transition{To: StateMigrating, Hold: true})
	if err != nil || hm.State != StateRamping || hm.Ramp.Ratio != 0.5 || hm.Ramp.Hold == nil || hm.Ramp.Hold.Ratio != 1 {
		t.Fatalf("held migrate start: %+v %v", hm.Ramp, err)
	}
	valid("held migrate start", hm)
	m, err := Apply(hm, Transition{To: StateMigrating})
	if err != nil || m.State != StateMigrating || m.Ramp != nil {
		t.Fatalf("completing a held migrate start: %+v %v", m, err)
	}
	if _, err := Apply(a, Transition{To: StateCutover, Hold: true}); err == nil {
		t.Error("a held CUTOVER accepted")
	}

	// The hold round-trips through the file format.
	f := &File{Version: 1, Clusters: clusters, Tenants: map[string]Tenant{"acme": {DefaultCluster: "garage"}}, Placements: map[string]Placement{"acme/data": hp}}
	data, err := marshal(f)
	if err != nil {
		t.Fatal(err)
	}
	g, err := parse(data)
	if err != nil {
		t.Fatal(err)
	}
	if got := g.Placements["acme/data"].Ramp.Hold; got == nil || got.Ratio != 0.5 || !slices.Equal(got.Prefixes, []string{"runs/"}) {
		t.Errorf("hold after a round trip: %+v\n%s", got, data)
	}
}

// ClearTarget forgets what expand recorded, only while ACTIVE and only when there is a target, and
// keeps a backend name the cold tier still uses.
func TestClearTarget(t *testing.T) {
	f := &File{Placements: map[string]Placement{
		"acme/data": {State: StateActive, Primary: "garage", Target: "minio", Names: map[string]string{"garage": "data", "minio": "data-001"}},
		"acme/cold": {State: StateActive, Primary: "garage", Target: "cold", Cold: "cold", Names: map[string]string{"garage": "c", "cold": "c"}},
		"acme/move": {State: StateRamping, Primary: "minio", Source: "garage", Names: map[string]string{"garage": "m", "minio": "m"}},
		"acme/none": active("garage"),
	}}
	if err := f.ClearTarget("acme", "data"); err != nil {
		t.Fatal(err)
	}
	if p := f.Placements["acme/data"]; p.Target != "" || len(p.Names) != 1 || p.Primary != "garage" {
		t.Fatalf("after clear: %+v", p)
	}
	if err := f.ClearTarget("acme", "cold"); err != nil || f.Placements["acme/cold"].Names["cold"] != "c" {
		t.Fatalf("cold name dropped: %v %+v", err, f.Placements["acme/cold"])
	}
	for _, b := range []string{"move", "none", "data"} {
		if err := f.ClearTarget("acme", b); !errors.Is(err, ErrConflict) {
			t.Errorf("clear %s: %v, want ErrConflict", b, err)
		}
	}
	if err := f.ClearTarget("acme", "gone"); !errors.Is(err, ErrNotFound) {
		t.Errorf("clear of a missing placement: %v", err)
	}
}
