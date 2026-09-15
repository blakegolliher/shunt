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
	cfg, err := config.Load("../config/testdata/valid/mixed.yaml")
	if err != nil {
		t.Fatal(err)
	}
	f, err := Load("testdata/valid/mixed.yaml", cfg.Clusters)
	if err != nil {
		t.Fatal(err)
	}
	if f.Version != 7 || len(f.Placements) != 7 || len(f.Tenants) != 2 {
		t.Fatalf("shape: version %d, %d placements, %d tenants", f.Version, len(f.Placements), len(f.Tenants))
	}
	s := newSnapshot(f)
	p, ok := s.Lookup("acme", "runs")
	if !ok || p.State != StateRamping || p.Ramp == nil || len(p.Ramp.Prefixes) != 1 || p.Route() != "vast-a" {
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
			_, err := Load(f, clusters)
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("want an error naming %q, got %v", want, err)
			}
		})
	}
}

func TestEmptyFileIsEmptyDirectory(t *testing.T) {
	f, err := Parse([]byte("  \n# nothing\n"))
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

// every state pair: the six legal transitions succeed with the arguments they need, and every
// other pair is refused naming both states.
func TestTransitionMatrix(t *testing.T) {
	placements := map[string]Placement{
		StateActive:    active("garage"),
		StateRamping:   {State: StateRamping, Primary: "minio", Source: "garage", Ramp: &Ramp{Ratio: 0.1}, Names: map[string]string{"garage": "data", "minio": "data2"}},
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
			f := &File{Version: 1, Tenants: map[string]Tenant{"acme": {DefaultCluster: "garage"}}, Placements: map[string]Placement{"acme/data": np}}
			if err := Validate(f, clusters); err != nil {
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
	if _, err := Apply(p, Transition{To: StateMigrating, Target: "garage", Name: "x"}); err == nil {
		t.Error("target equal to primary accepted")
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
		if !ValidTenant(tenant) || !s3.ValidBucketName(bucket) {
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

// writeDir writes a directory file with tenants acme and zed and returns its path.
func writeDir(t *testing.T, version int64) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "directory.yaml")
	body := fmt.Sprintf("version: %d\ntenants:\n  acme: { default_cluster: garage }\n  zed: { default_cluster: minio }\n", version)
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
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
	d, err := Open(path, sampleClusters(t))
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
	d2, err := Open(path, sampleClusters(t))
	if err != nil || d2.Snapshot().Version() != 8 {
		t.Fatalf("reopen: %v", err)
	}
}

func TestReload(t *testing.T) {
	ctx := context.Background()
	clusters := sampleClusters(t)
	path := writeDir(t, 1)
	a, err := Open(path, clusters)
	if err != nil {
		t.Fatal(err)
	}
	b, err := Open(path, clusters)
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
	good, _ := Marshal(a.Snapshot().File())
	stale := strings.Replace(string(good), "zed-0000-logs", "zed-0001-logs", 1)
	if err := os.WriteFile(path, []byte(stale), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Reload(); !errors.Is(err, ErrStaleVersion) {
		t.Fatalf("stale version: %v", err)
	}
	// Kubernetes ConfigMap style: path is a symlink swapped to a new target with a higher version.
	dir := t.TempDir()
	link := filepath.Join(dir, "directory.yaml")
	v1, v2 := filepath.Join(dir, "..v1"), filepath.Join(dir, "..v2")
	f := a.Snapshot().File()
	f.Version = 10
	out, _ := Marshal(f)
	_ = os.WriteFile(v1, out, 0o644)
	if err := os.Symlink(v1, link); err != nil {
		t.Fatal(err)
	}
	c, err := Open(link, clusters)
	if err != nil {
		t.Fatal(err)
	}
	f.Version = 11
	delete(f.Placements, "zed/logs")
	out, _ = Marshal(f)
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
	d, err := Open(path, sampleClusters(t))
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
	d, err := Open(path, sampleClusters(t))
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
	clusters := sampleClusters(t)
	path := writeDir(t, 1)
	a, _ := Open(path, clusters)
	b, _ := Open(path, clusters)
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
	f, err := Load(path, clusters)
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
	d, err := Open(path, sampleClusters(t))
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
		d, err := Parse(data)
		if err != nil {
			return
		}
		if Validate(d, clusters) != nil {
			return
		}
		out, err := Marshal(d)
		if err != nil {
			t.Fatalf("marshal of a valid directory: %v", err)
		}
		back, err := Parse(out)
		if err != nil {
			t.Fatalf("round trip does not parse: %v\n%s", err, out)
		}
		if err := Validate(back, clusters); err != nil || back.Version != d.Version || len(back.Placements) != len(d.Placements) {
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
	dir := b.TempDir()
	path := filepath.Join(dir, "directory.yaml")
	_ = os.WriteFile(path, []byte("version: 1\ntenants: { acme: { default_cluster: garage } }\n"), 0o644)
	d, err := Open(path, sampleClusters(b))
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
