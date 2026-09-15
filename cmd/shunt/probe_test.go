package main

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
)

// The probe must run to completion against an in-process fake and report every field; the
// values themselves are the fake's business, not asserted.
func TestProbeAgainstFake(t *testing.T) {
	be := s3mem.New()
	srv := httptest.NewServer(gofakes3.New(be).Server())
	defer srv.Close()
	p, err := newProber(srv.URL, "us-east-1", "probe-bucket", "", "AK", "SK", false)
	if err != nil {
		t.Fatal(err)
	}
	res, err := p.run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for name, v := range map[string]string{"sha256": res.EnforcesSHA256, "trailer": res.UnsignedTrailer, "ifnonematch": res.IfNoneMatchPut, "ifmatch": res.IfMatchPut} {
		if v == "" {
			t.Errorf("%s not reported", name)
		}
	}
	if len(res.Checksums) != 5 || res.UnsignedGetRoot == 0 {
		t.Errorf("incomplete result: %+v", res)
	}
	var sb strings.Builder
	printProbe(&sb, res)
	if !strings.Contains(sb.String(), "If-None-Match: * on PUT") {
		t.Errorf("table: %s", sb.String())
	}
	if _, err := newProber("ftp://x", "r", "b", "", "a", "s", false); err == nil {
		t.Error("bad scheme accepted")
	}
}
