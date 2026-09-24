package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/blakegolliher/shunt/internal/control"
)

// oneMember is a fleet of one proxy, as the test describes it.
type oneMember struct{ m control.Member }

func (f oneMember) Heartbeat(context.Context, string, control.Heartbeat) (control.Grant, error) {
	return control.Grant{LeaseTTL: time.Second}, nil
}
func (f oneMember) Members(context.Context) ([]control.Member, error) {
	return []control.Member{f.m}, nil
}
func (f oneMember) Retire(context.Context, string, string, int64) error           { return nil }
func (f oneMember) RequestRetire(context.Context, string) error                   { return nil }
func (f oneMember) Resolve(context.Context, string, string, string, string) error { return nil }
func (f oneMember) Forget(context.Context, string) error                          { return nil }

// proxy show prints one proxy's install state and what is off with it.
func TestProxyShow(t *testing.T) {
	rg := newAPIRig(t)
	id := rg.dir.Snapshot().File().Identity
	rg.ctl.Fleet = oneMember{control.Member{ID: "p1", Live: true, Host: "h1", Identity: id, Applied: 1, Installed: 1, Durable: 0,
		CacheError: "version 1 not durable: fsync: disk full"}}
	out := rg.must(t, "proxy", "show", "p1")
	for _, want := range []string{"proxy p1 (live, h1)", "requests use 1; installed 1; restart cache durable at 0", "! restart cache not durable: version 1 not durable: fsync: disk full"} {
		if !strings.Contains(out, want) {
			t.Errorf("proxy show lacks %q:\n%s", want, out)
		}
	}
	if _, _, err := runIn(t, "", "proxy", "show", "nope", "--api", rg.url); err == nil || !strings.Contains(err.Error(), "no proxy nope") {
		t.Fatalf("an unknown proxy: %v", err)
	}
}
