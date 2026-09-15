package proxy

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

// A signed-chunk body whose first chunk header declares a negative size must be answered as a
// client error. Found by FuzzSignedChunkReader in the lifted versitygw reader during the POC-2
// make all gate: before the fix the reader sliced with the negative size and panicked. The seed
// signature is valid (Content-Length is not a signed header here), so the request reaches the
// decoder rather than failing verification.
func TestResignNegativeChunkSizeIsRejected(t *testing.T) {
	rec := &seen{}
	r := newResignRig(t, backend(t, rec, 200, ""), Capabilities{EnforcesSHA256: true, UnsignedTrailer: true})
	req, _ := signedChunkRequest(t, r.front.URL+"/bbb/k", bytes.Repeat([]byte("n"), 99), "")
	wire := "-1;chunk-signature=" + strings.Repeat("0", 64) + "\r\n"
	req.Body = io.NopCloser(strings.NewReader(wire))
	req.ContentLength = int64(len(wire))
	resp, body := do(t, req)
	if resp.StatusCode/100 != 4 {
		t.Fatalf("negative chunk size: status %d, body %s", resp.StatusCode, body)
	}
	if got := rec.snap().bodyLen; got > 0 {
		t.Fatalf("upstream received %d body bytes from a malformed chunk stream", got)
	}
}

// panicReader simulates a future decoder bug.
type panicReader struct{}

func (panicReader) Read([]byte) (int, error) { panic("decoder bug") }

func TestGuardReaderConvertsPanicToError(t *testing.T) {
	n, err := (&guardReader{r: panicReader{}}).Read(make([]byte, 8))
	if n != 0 || err == nil || !strings.Contains(err.Error(), "IncompleteBody") {
		t.Fatalf("guard: n=%d err=%v", n, err)
	}
}
