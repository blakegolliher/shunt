package proxy

// DeleteObjects on a bucket spread over legs (ADR-0018, amended 2026-09-25), driven through the
// handler: every leg is sent the request, each key's answer comes from its owner, and while a
// move is under way its source leg goes first.

import (
	"crypto/md5" //nolint:gosec // G501: S3's Content-MD5
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"net/http"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"

	"go.yaml.in/yaml/v4"

	"github.com/blakegolliher/shunt/internal/directory"
	"github.com/blakegolliher/shunt/internal/migrate"
)

// deleteBody is a DeleteObjects body naming keys, and its Content-MD5.
func deleteBody(quiet bool, keys ...string) ([]byte, string) {
	var b strings.Builder
	b.WriteString("<Delete>")
	if quiet {
		b.WriteString("<Quiet>true</Quiet>")
	}
	for _, k := range keys {
		b.WriteString("<Object><Key>" + esc(k) + "</Key></Object>")
	}
	b.WriteString("</Delete>")
	sum := md5.Sum([]byte(b.String())) //nolint:gosec // G401: S3's Content-MD5
	return []byte(b.String()), base64.StdEncoding.EncodeToString(sum[:])
}

func deleteObjects(t *testing.T, m *mixedRig, bucket string, quiet bool, keys ...string) (reply, deleteResult) {
	t.Helper()
	body, sum := deleteBody(quiet, keys...)
	r := m.send(t, "POST", "/"+bucket+"?delete", "", body, acmeAK, acmeSK, map[string]string{"Content-MD5": sum})
	var res deleteResult
	if r.StatusCode == http.StatusOK {
		if err := xml.Unmarshal(r.body, &res); err != nil {
			t.Fatalf("DeleteObjects answer: %v\n%s", err, r.body)
		}
	}
	return r, res
}

func deletedKeys(res deleteResult) []string {
	out := make([]string, 0, len(res.Deleted))
	for _, d := range res.Deleted {
		out = append(out, d.Key)
	}
	sort.Strings(out)
	return out
}

// At rest: each named key is deleted from the leg that owns it and reported once; a key that
// exists nowhere is reported deleted, as S3 does; a stray copy on a leg that does not own the key
// goes too; Quiet reports nothing; keys not named stay. Before, the request answered 501.
func TestSpreadDeleteObjects(t *testing.T) {
	m := newMixedRig(t, nil)
	p := spread(t, m)
	keys := make([]string, 0, 40)
	for i := range 40 {
		key := fmt.Sprintf("obj/%03d", i)
		keys = append(keys, key)
		if r := m.acme(t, "PUT", "/spread/"+key, []byte("v")); r.StatusCode != 200 {
			t.Fatalf("PUT %s: %d %s", key, r.StatusCode, r.body)
		}
	}
	_, _, other, otherBucket := legBucket(t, m, p, "obj/005")
	other.put(otherBucket, "obj/005", []byte("stray"))

	named := append(slices.Clone(keys[:20]), "missing/key")
	r, res := deleteObjects(t, m, "spread", false, named...)
	if r.StatusCode != http.StatusOK {
		t.Fatalf("DeleteObjects: %d %s", r.StatusCode, r.body)
	}
	want := slices.Clone(named)
	sort.Strings(want)
	if got := deletedKeys(res); !slices.Equal(got, want) || len(res.Errors) != 0 {
		t.Fatalf("DeleteObjects reported %v, errors %v; want each named key once: %v", got, res.Errors, want)
	}
	for _, k := range keys[:20] {
		owner, ob, _, _ := legBucket(t, m, p, k)
		if owner.holds(ob, k) {
			t.Fatalf("%s is still on its owner", k)
		}
	}
	if other.holds(otherBucket, "obj/005") {
		t.Fatal("the stray copy of obj/005 on the leg that does not own it survived")
	}
	for _, k := range keys[20:] {
		if owner, ob, _, _ := legBucket(t, m, p, k); !owner.holds(ob, k) {
			t.Fatalf("%s was not named and is gone", k)
		}
	}
	r, res = deleteObjects(t, m, "spread", true, keys[20:25]...)
	if r.StatusCode != http.StatusOK || len(res.Deleted) != 0 || len(res.Errors) != 0 {
		t.Fatalf("a quiet DeleteObjects: %d %+v %s", r.StatusCode, res, r.body)
	}
	for _, k := range keys[20:25] {
		if owner, ob, _, _ := legBucket(t, m, p, k); owner.holds(ob, k) {
			t.Fatalf("quiet: %s is still on its owner", k)
		}
	}
	m.noLeak(t, "spread DeleteObjects", r)

	// A leg that fails the whole request fails it: its keys' outcomes are unknown.
	m.minio.mu.Lock()
	m.minio.deleteObjectsStatus = http.StatusInternalServerError
	m.minio.mu.Unlock()
	if r, _ := deleteObjects(t, m, "spread", false, keys[25:30]...); r.StatusCode < 500 {
		t.Fatalf("a leg failing its DeleteObjects: %d %s", r.StatusCode, r.body)
	}
}

// During a move: a key in the moving range is deleted from the source leg first and then the
// destination, as a single DELETE of it is (ADR-0004 race 1); the answer is the destination's. A
// source leg that fails is logged and metered, and the owners' answers stand.
func TestSpreadDeleteObjectsDuringAMove(t *testing.T) {
	m := newMixedRig(t, nil)
	spread(t, m)
	f := m.dir.Snapshot().File()
	pl := f.Placements["acme/spread"]
	var garageRange directory.HashRange
	for _, o := range pl.Owners {
		if o.Leg == "garage" {
			garageRange = directory.HashRange{From: o.From, To: o.To}
		}
	}
	pl.State, pl.Move = directory.StateMigrating, &directory.Move{Range: garageRange, From: "garage", To: "minio"}
	f.Placements["acme/spread"] = pl
	f.Version++
	raw, err := yaml.Marshal(f)
	if err != nil {
		t.Fatal(err)
	}
	if err := directory.WriteAtomic(m.dirPath, raw); err != nil {
		t.Fatal(err)
	}
	if _, err := m.dir.Reload(); err != nil {
		t.Fatal(err)
	}
	p, _ := m.dir.Snapshot().Lookup("acme", "spread")
	var moving, settled string
	for i := 0; moving == "" || settled == ""; i++ {
		k := fmt.Sprintf("k/%03d", i)
		switch {
		case migrate.InMove(p, k) && moving == "":
			moving = k
		case !migrate.InMove(p, k) && settled == "":
			settled = k
		}
	}
	m.garage.put("spread-g", moving, []byte("on the source"))
	m.minio.put("spread-m", moving, []byte("on the destination"))
	m.minio.put("spread-m", settled, []byte("owned by minio"))

	// The source's DeleteObjects must arrive before the destination's.
	var mu sync.Mutex
	var order []string
	for _, fk := range []*fakeS3{m.garage, m.minio} {
		fk.mu.Lock()
		fk.before = func(r *http.Request) {
			if r.Method == http.MethodPost && r.URL.Query().Has("delete") {
				mu.Lock()
				order = append(order, fk.name)
				mu.Unlock()
			}
		}
		fk.mu.Unlock()
	}
	r, res := deleteObjects(t, m, "spread", false, moving, settled)
	if r.StatusCode != http.StatusOK {
		t.Fatalf("DeleteObjects during a move: %d %s", r.StatusCode, r.body)
	}
	want := []string{moving, settled}
	sort.Strings(want)
	if got := deletedKeys(res); !slices.Equal(got, want) {
		t.Fatalf("reported %v", got)
	}
	if m.garage.holds("spread-g", moving) || m.minio.holds("spread-m", moving) || m.minio.holds("spread-m", settled) {
		t.Fatal("a named key survived on a leg")
	}
	mu.Lock()
	if len(order) != 2 || order[0] != m.garage.name {
		t.Fatalf("DeleteObjects reached the legs in the order %v; the move's source (%s) goes first", order, m.garage.name)
	}
	mu.Unlock()
	if got := counterValue(t, m.h.Metrics.DualDelete.WithLabelValues("acme/spread", "both")); got != 1 {
		t.Fatalf("dual_delete{outcome=both} = %v", got)
	}

	m.garage.mu.Lock()
	m.garage.deleteObjectsStatus = http.StatusInternalServerError
	m.garage.mu.Unlock()
	m.minio.put("spread-m", moving, []byte("again"))
	r, res = deleteObjects(t, m, "spread", false, moving)
	if r.StatusCode != http.StatusOK || !slices.Equal(deletedKeys(res), []string{moving}) || m.minio.holds("spread-m", moving) {
		t.Fatalf("with the source failing: %d %+v", r.StatusCode, res)
	}
	if got := counterValue(t, m.h.Metrics.DualDelete.WithLabelValues("acme/spread", "source_failed")); got != 1 {
		t.Fatalf("dual_delete{outcome=source_failed} = %v", got)
	}
	if !strings.Contains(m.alerts.String(), "delete did not reach the migration source") {
		t.Fatalf("the failed source leg was not logged: %s", m.alerts.String())
	}
}

// FuzzParseDeleteResult: a leg's DeleteObjects answer, whatever the backend sends, parses or is
// refused without a panic, and what parses survives shunt's own encoding of it.
func FuzzParseDeleteResult(f *testing.F) {
	f.Add([]byte(`<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Deleted><Key>a</Key></Deleted><Error><Key>b</Key><Code>AccessDenied</Code><Message>no</Message></Error></DeleteResult>`))
	f.Add([]byte(`<DeleteResult><Deleted><Key>k</Key><VersionId>v1</VersionId><DeleteMarker>true</DeleteMarker><DeleteMarkerVersionId>m</DeleteMarkerVersionId></Deleted></DeleteResult>`))
	f.Add([]byte(`<Error><Code>InternalError</Code></Error>`))
	f.Fuzz(func(t *testing.T, body []byte) {
		res, err := parseDeleteResult(body)
		if err != nil {
			return
		}
		out, err := xml.Marshal(res)
		if err != nil {
			return // a key with a character XML 1.0 cannot carry; the handler answers 500 for it
		}
		again, err := parseDeleteResult(out)
		if err != nil || len(again.Deleted) != len(res.Deleted) || len(again.Errors) != len(res.Errors) {
			t.Fatalf("re-encoded DeleteResult does not parse the same: %v\n%s", err, out)
		}
	})
}
