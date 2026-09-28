package proxy

// DeleteObjects on a spread bucket answers every key from the leg that owns it (third review,
// R3-07): while a scoped move is under way, the move's source is a redundant copy for the keys in
// the move and the owner of every other key it holds, so its failure is an error for those keys,
// quiet or not.

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/blakegolliher/shunt/internal/admission"
	"github.com/blakegolliher/shunt/internal/directory"
	"github.com/blakegolliher/shunt/internal/migrate"
)

// movingSpread spreads acme/spread over garage and minio and moves archive/'s share of garage to
// minio, as the review did. It returns three keys, each on its backends: one garage still owns
// (outside the move's scope), one in the move (on garage, the source, and already on minio), and
// one minio owns.
func movingSpread(t *testing.T, m *mixedRig) (sourceOwned, inMove, minioOwned string) {
	t.Helper()
	p := spread(t, m)
	if err := m.dir.Carve(context.Background(), "acme", "spread", "archive/", "test"); err != nil {
		t.Fatal(err)
	}
	rewriteDirectory(t, m, func(f *directory.File) {
		pl := f.Placements["acme/spread"]
		var rg directory.HashRange
		for _, o := range pl.Owners {
			if o.Leg == "garage" {
				rg = directory.HashRange{From: o.From, To: o.To}
			}
		}
		pl.State, pl.Move = directory.StateMigrating, &directory.Move{Scope: "archive/", Range: rg, From: "garage", To: "minio"}
		f.Placements["acme/spread"] = pl
	})
	moving, _ := m.dir.Snapshot().Lookup("acme", "spread")
	for i := 0; i < 200 && (sourceOwned == "" || minioOwned == "" || inMove == ""); i++ {
		stay, arch := fmt.Sprintf("stay/%d", i), fmt.Sprintf("archive/%d", i)
		switch owner, _ := migrate.OwnerOf(p, stay); {
		case owner == "garage" && sourceOwned == "":
			sourceOwned = stay
		case owner == "minio" && minioOwned == "":
			minioOwned = stay
		}
		if inMove == "" && migrate.InMove(moving, arch) {
			inMove = arch
		}
	}
	if sourceOwned == "" || minioOwned == "" || inMove == "" {
		t.Fatalf("keys: %q %q %q", sourceOwned, inMove, minioOwned)
	}
	m.garage.put("spread-g", sourceOwned, []byte("v"))
	m.garage.put("spread-g", inMove, []byte("v"))
	m.minio.put("spread-m", inMove, []byte("v"))
	m.minio.put("spread-m", minioOwned, []byte("v"))
	return sourceOwned, inMove, minioOwned
}

func errorKeys(res deleteResult) []string {
	out := make([]string, 0, len(res.Errors))
	for _, e := range res.Errors {
		out = append(out, e.Key)
	}
	slices.Sort(out)
	return out
}

// The source leg failing whole, quiet and verbose: the key it owns is an error (the review's case),
// the keys the other legs own are answered by them, and the key in the move by its destination. A
// source leg answering per-key errors: the key it owns carries its error; the key in the move, whose
// source copy only failed to go, does not. A source leg whose delete may have landed (the
// connection ends after the request): the key it owns is an error, and the mutation's outcome is
// uncertain. Negative control: without answering for the source's own keys, the first two answer
// 200 with no error for a key that is still there, and the last settles as definitive.
func TestSpreadDeleteAnswersForEveryKey(t *testing.T) {
	cases := []struct {
		name      string
		quiet     bool
		prepare   func(t *testing.T, m *mixedRig, sourceOwned, inMove string)
		uncertain int64
	}{
		{name: "the source leg fails whole, quiet", quiet: true, prepare: func(_ *testing.T, m *mixedRig, _, _ string) {
			m.garage.mu.Lock()
			m.garage.deleteObjectsStatus = http.StatusServiceUnavailable
			m.garage.mu.Unlock()
		}},
		{name: "the source leg fails whole, verbose", prepare: func(_ *testing.T, m *mixedRig, _, _ string) {
			m.garage.mu.Lock()
			m.garage.deleteObjectsStatus = http.StatusServiceUnavailable
			m.garage.mu.Unlock()
		}},
		{name: "the source leg answers per-key errors", prepare: func(_ *testing.T, m *mixedRig, sourceOwned, inMove string) {
			m.garage.mu.Lock()
			m.garage.deleteKeyErrors = map[string]string{sourceOwned: "AccessDenied", inMove: "AccessDenied"}
			m.garage.mu.Unlock()
		}},
		{name: "the source leg's outcome unknown", uncertain: 1, prepare: func(t *testing.T, m *mixedRig, _, _ string) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPost && r.URL.Query().Has("delete") {
					_, _ = io.Copy(io.Discard, r.Body) // the whole request arrived; its answer never does
					conn, _, err := w.(http.Hijacker).Hijack()
					if err == nil {
						_ = conn.Close()
					}
					return
				}
				m.garage.ServeHTTP(w, r)
			}))
			t.Cleanup(srv.Close)
			cl := m.clusters["garage"]
			cl.Endpoints = []string{strings.TrimPrefix(srv.URL, "http://")}
			if err := m.dir.PutCluster(context.Background(), "garage", cl, "", "test"); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			m := newMixedRig(t, nil)
			sourceOwned, inMove, minioOwned := movingSpread(t, m)
			c.prepare(t, m, sourceOwned, inMove)
			r, res := deleteObjects(t, m, "spread", c.quiet, sourceOwned, inMove, minioOwned)
			if r.StatusCode != http.StatusOK {
				t.Fatalf("DeleteObjects: %d %s", r.StatusCode, r.body)
			}
			if got := errorKeys(res); !slices.Equal(got, []string{sourceOwned}) {
				t.Fatalf("errors for %v, want only the source's own key %s: %s", got, sourceOwned, r.body)
			}
			if want := []string{inMove, minioOwned}; !c.quiet && !slices.Equal(deletedKeys(res), sortedCopy(want)) {
				t.Fatalf("deleted %v, want %v", deletedKeys(res), want)
			}
			if m.minio.holds("spread-m", inMove) || m.minio.holds("spread-m", minioOwned) {
				t.Fatal("a key answered deleted is still on its owner")
			}
			if got := m.gates.State("acme/spread").Uncertain[admission.Mutations]; got != c.uncertain {
				t.Fatalf("uncertain mutations: %d, want %d", got, c.uncertain)
			}
		})
	}
}

func sortedCopy(s []string) []string {
	out := slices.Clone(s)
	slices.Sort(out)
	return out
}
