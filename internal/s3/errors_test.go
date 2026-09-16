package s3

import (
	"bytes"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestTableIsConsistent(t *testing.T) {
	for code, e := range table {
		if e.Code != code {
			t.Errorf("%s: row code is %s", code, e.Code)
		}
		if e.Status < 300 || e.Status > 599 {
			t.Errorf("%s: status %d out of range", code, e.Status)
		}
		if e.Message == "" {
			t.Errorf("%s: empty message", code)
		}
	}
	if len(codes()) != len(table) {
		t.Fatal("Codes() length mismatch")
	}
}

func TestLookupUnknown(t *testing.T) {
	e := Lookup("NoSuchThing")
	if e.Status != http.StatusInternalServerError || e.Code != "NoSuchThing" {
		t.Fatalf("unknown code: %+v", e)
	}
	if !bytes.Contains([]byte(e.Error()), []byte("NoSuchThing")) {
		t.Fatal("Error() should carry the code")
	}
}

func TestRenderRoundTrip(t *testing.T) {
	body := Render(NoSuchKey, "", "/b/k", "req-1", "host-1")
	if !bytes.HasPrefix(body, []byte(xml.Header)) {
		t.Fatalf("missing XML header:\n%s", body)
	}
	var got ErrorResponse
	if err := xml.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if got.Code != NoSuchKey || got.Message != table[NoSuchKey].Message || got.Resource != "/b/k" || got.RequestID != "req-1" || got.HostID != "host-1" {
		t.Fatalf("round trip: %+v", got)
	}
	if !bytes.Contains(body, []byte("<Error><Code>NoSuchKey</Code><Message>")) {
		t.Fatalf("wire shape:\n%s", body)
	}
}

func TestRenderOverrideAndOmit(t *testing.T) {
	body := Render(InvalidArgument, "custom text", "", "", "")
	if !bytes.Contains(body, []byte("<Message>custom text</Message>")) {
		t.Fatalf("override lost:\n%s", body)
	}
	for _, absent := range []string{"<Resource>", "<RequestId>", "<HostId>"} {
		if bytes.Contains(body, []byte(absent)) {
			t.Errorf("%s should be omitted when empty:\n%s", absent, body)
		}
	}
}

func TestWrite(t *testing.T) {
	rec := httptest.NewRecorder()
	Write(rec, SignatureDoesNotMatch, "", "/b", "r-9")
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/xml" {
		t.Fatalf("content-type %q", ct)
	}
	if rec.Header().Get("x-amz-request-id") != "r-9" {
		t.Fatal("request id header missing")
	}
	if cl := rec.Header().Get("Content-Length"); cl != itoa(rec.Body.Len()) {
		t.Fatalf("content-length %s vs body %d", cl, rec.Body.Len())
	}
}

func TestItoa(t *testing.T) {
	for _, n := range []int{0, 1, 9, 10, 255, 4096, 1 << 30} {
		if got, want := itoa(n), fmtInt(n); got != want {
			t.Errorf("itoa(%d) = %q, want %q", n, got, want)
		}
	}
}

func fmtInt(n int) string {
	if n == 0 {
		return "0"
	}
	s := ""
	for n > 0 {
		s = string(rune('0'+n%10)) + s
		n /= 10
	}
	return s
}

func BenchmarkRender(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		_ = Render(NoSuchKey, "", "/bucket/key", "0123456789ABCDEF", "")
	}
}

func BenchmarkLookup(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		_ = Lookup(NoSuchBucket)
	}
}
