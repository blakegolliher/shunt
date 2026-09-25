package upstream

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blakegolliher/shunt/internal/config"
)

// connServer counts the connections it has seen closed.
func connServer(t *testing.T) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var closed atomic.Int64
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "ok") }))
	srv.Config.ConnState = func(_ net.Conn, st http.ConnState) {
		if st == http.StateClosed {
			closed.Add(1)
		}
	}
	srv.Start()
	t.Cleanup(srv.Close)
	return srv, &closed
}

// park makes one request through cl and leaves its connection idle in cl's transport.
func park(t *testing.T, cl *Cluster) {
	t.Helper()
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://"+cl.Next()+"/", nil)
	resp, err := cl.Transport.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
}

func closedWithin(n *atomic.Int64, d time.Duration) bool {
	for end := time.Now().Add(d); time.Now().Before(end); time.Sleep(5 * time.Millisecond) {
		if n.Load() > 0 {
			return true
		}
	}
	return n.Load() > 0
}

// A replaced cluster's transport stays open while a runtime bundle holds its set, and closes its
// idle connections when the last set using it lets go. A secret-only rotation shares the transport,
// so releasing the set before it closes nothing. Negative control: closing at commit, as before H1c,
// fails the "held" check.
func TestPoolLifetime(t *testing.T) {
	srv, closed := connServer(t)
	hostport := strings.TrimPrefix(srv.URL, "http://")
	_, port, _ := net.SplitHostPort(hostport)
	secrets := map[string]string{"env:A": "a1"}
	r := NewRegistry(Options{}, func(ref string) (string, error) { return secrets[ref], nil })
	if _, _, err := r.Apply(map[string]config.Cluster{"a": regCluster(hostport, "env:A")}); err != nil {
		t.Fatal(err)
	}
	set1 := r.Load()
	if !set1.Retain() { // a bundle on set1
		t.Fatal("the live set refused a reference")
	}
	a1, _ := set1.Get("a")
	park(t, a1)

	secrets["env:A"] = "a2" // a secret-only rotation: set2 shares a1's transport
	if _, _, err := r.Apply(map[string]config.Cluster{"a": regCluster(hostport, "env:A")}); err != nil {
		t.Fatal(err)
	}
	set2 := r.Load()
	if a2, _ := set2.Get("a"); a2.Transport != a1.Transport || a2.Creds.Secret != "a2" {
		t.Fatalf("rotation: shared transport %v, secret %q", a2.Transport == a1.Transport, a2.Creds.Secret)
	}
	set2.Retain() // a bundle on set2
	set1.Release()
	if closedWithin(closed, 100*time.Millisecond) {
		t.Fatal("releasing set1 closed a transport set2 still uses")
	}

	// An endpoint change builds a new transport; the registry lets go of set2, but its bundle holds it.
	if _, _, err := r.Apply(map[string]config.Cluster{"a": regCluster("localhost:"+port, "env:A")}); err != nil {
		t.Fatal(err)
	}
	if closedWithin(closed, 100*time.Millisecond) {
		t.Fatal("a held set's transport was closed when the registry replaced it")
	}
	set2.Release()
	if !closedWithin(closed, 5*time.Second) {
		t.Fatal("the last release did not close the retired transport's idle connection")
	}
	if set2.Retain() {
		t.Fatal("a released set took a reference again")
	}
}

// A candidate that is never committed takes no reference: the live set's transports stay counted
// once, and replacing the live set still closes them.
func TestDroppedCandidateHoldsNothing(t *testing.T) {
	srv, closed := connServer(t)
	hostport := strings.TrimPrefix(srv.URL, "http://")
	r := NewRegistry(Options{}, func(string) (string, error) { return "s", nil })
	if _, _, err := r.Apply(map[string]config.Cluster{"a": regCluster(hostport, "env:A")}); err != nil {
		t.Fatal(err)
	}
	a, _ := r.Load().Get("a")
	park(t, a)
	if _, err := r.Prepare(map[string]config.Cluster{"a": regCluster(hostport, "env:A")}, nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.Apply(map[string]config.Cluster{}); err != nil {
		t.Fatal(err)
	}
	if !closedWithin(closed, 5*time.Second) {
		t.Fatal("a dropped candidate kept the live transport open")
	}
}
