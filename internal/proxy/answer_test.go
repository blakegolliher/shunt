package proxy

// A mutation's outcome is its backend's whole answer, not its status line (third review, R3-03):
// CompleteMultipartUpload, CopyObject and UploadPartCopy can answer 200 before they have finished and
// write their result, or an error, afterwards.

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/blakegolliher/shunt/internal/admission"
	"github.com/blakegolliher/shunt/internal/sigv4"
)

const completeResult = `<?xml version="1.0" encoding="UTF-8"?><CompleteMultipartUploadResult><Bucket>acme-1111-data</Bucket><Key>k</Key><ETag>"x-1"</ETag></CompleteMultipartUploadResult>`

// completeRig routes acme/data's CompleteMultipartUpload to complete and everything else to the
// garage fake, and returns an upload id begun through shunt.
func completeRig(t *testing.T, complete http.HandlerFunc) (*mixedRig, string) {
	t.Helper()
	m := newMixedRig(t, nil)
	m.h.IdleTimeout, m.h.MetadataTimeout = 150*time.Millisecond, 150*time.Millisecond
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Query().Get("uploadId") != "" {
			_, _ = io.Copy(io.Discard, r.Body)
			complete(w, r)
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
	init := m.acme(t, "POST", "/data/k?uploads", nil)
	id := between(init.body, "<UploadId>", "</UploadId>")
	if id == "" {
		t.Fatalf("init: %d %s", init.StatusCode, init.body)
	}
	return m, id
}

// completeThrough sends the completion through shunt and reads the whole answer; a client that
// leaves after leave (0: stays) cancels its request then.
func completeThrough(t *testing.T, m *mixedRig, id string, leave time.Duration) (status int, body string) {
	t.Helper()
	ctx := context.Background()
	if leave > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, leave)
		defer cancel()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, m.front.URL+"/data/k?uploadId="+id, strings.NewReader("<CompleteMultipartUpload/>"))
	if err != nil {
		t.Fatal(err)
	}
	sigv4.Sign(req, sigv4.Credentials{AccessKey: acmeAK, Secret: acmeSK}, "us-east-1", sigv4.UnsignedPayload, time.Now())
	resp, err := fresh().Do(req)
	if err != nil {
		return 0, ""
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// settled waits for acme/data's tokens to come back and returns its uncertain mutations.
func settled(t *testing.T, m *mixedRig) int64 {
	t.Helper()
	until := time.Now().Add(2 * time.Second)
	for m.gates.State("acme/data").Inflight[admission.Mutations] > 0 {
		if time.Now().After(until) {
			t.Fatalf("the completion's token never came back: %+v", m.gates.State("acme/data"))
		}
		time.Sleep(time.Millisecond)
	}
	return m.gates.State("acme/data").Uncertain[admission.Mutations]
}

// Every way a completion's answer can arrive: an early 200 and then the result, or then an error
// (both definitive: the backend decided); a result cut short at a clean end of stream (the decision
// never arrived: uncertain); no result before the proxy's deadline (the review's case: uncertain);
// the client gone before the result, which then arrives (definitive: the proxy reads it) or never
// does (uncertain). The proxy's deadline is 150 ms. Negative control: without learnAnswer's verdict
// the uncertain cases settle as definitive.
func TestCompletionOutcomeIsTheWholeAnswer(t *testing.T) {
	// answer sends an early 200 and whitespace, waits until after (or for never), then writes rest.
	answer := func(rest string, after time.Duration, never <-chan struct{}) http.HandlerFunc {
		return func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(" "))
			w.(http.Flusher).Flush()
			select {
			case <-time.After(after):
			case <-never:
				return
			}
			_, _ = w.Write([]byte(rest))
		}
	}
	const forever = time.Hour
	errorBody := `<Error><Code>InternalError</Code><Message>completion failed</Message></Error>`
	cases := []struct {
		name      string
		complete  func(never <-chan struct{}) http.HandlerFunc
		leave     time.Duration
		body      string // what the client reads, when it reads a whole answer
		uncertain int64
	}{
		{name: "early 200, then the result", body: " " + strings.Replace(completeResult, "acme-1111-data", "data", 1),
			complete: func(never <-chan struct{}) http.HandlerFunc {
				return answer(completeResult, 20*time.Millisecond, never)
			}},
		{name: "early 200, then an error", body: " " + errorBody,
			complete: func(never <-chan struct{}) http.HandlerFunc { return answer(errorBody, 20*time.Millisecond, never) }},
		{name: "a result cut short", uncertain: 1, complete: func(<-chan struct{}) http.HandlerFunc {
			return func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Length", "60")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(completeResult[:60]))
			}
		}},
		{name: "no result before the deadline", uncertain: 1,
			complete: func(never <-chan struct{}) http.HandlerFunc { return answer("", forever, never) }},
		{name: "the client gone, then the result", leave: 30 * time.Millisecond,
			complete: func(never <-chan struct{}) http.HandlerFunc {
				return answer(completeResult, 80*time.Millisecond, never)
			}},
		{name: "the client gone, the backend never finishing", leave: 30 * time.Millisecond, uncertain: 1,
			complete: func(never <-chan struct{}) http.HandlerFunc { return answer("", forever, never) }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			never := make(chan struct{})
			m, id := completeRig(t, c.complete(never))
			t.Cleanup(func() { close(never) }) // before completeRig's server closes: it waits for its handlers
			status, body := completeThrough(t, m, id, c.leave)
			if c.body != "" && (status != http.StatusOK || body != c.body) {
				t.Fatalf("the client read %d %q, want 200 %q", status, body, c.body)
			}
			if got := settled(t, m); got != c.uncertain {
				t.Fatalf("uncertain mutations: %d, want %d (%+v)", got, c.uncertain, m.gates.State("acme/data"))
			}
		})
	}
}

// A read is not a mutation: a GET whose client leaves mid-body counts nothing uncertain.
func TestAbandonedReadIsNotUncertain(t *testing.T) {
	m := newMixedRig(t, nil)
	m.garage.put("acme-1111-data", "big", []byte(strings.Repeat("x", 8<<20)))
	req, err := http.NewRequest(http.MethodGet, m.front.URL+"/data/big", nil)
	if err != nil {
		t.Fatal(err)
	}
	sigv4.Sign(req, sigv4.Credentials{AccessKey: acmeAK, Secret: acmeSK}, "us-east-1", sigv4.UnsignedPayload, time.Now())
	resp, err := fresh().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.CopyN(io.Discard, resp.Body, 1<<10)
	_ = resp.Body.Close()
	time.Sleep(100 * time.Millisecond)
	if st := m.gates.State("acme/data"); st.Uncertain != [2]int64{} || m.gates.Uncertain() != 0 {
		t.Fatalf("an abandoned read: %+v", st)
	}
}

// staged is a response body that yields its parts one read at a time.
type staged struct{ parts []string }

func (s *staged) Read(b []byte) (int, error) {
	if len(s.parts) == 0 {
		return 0, io.EOF
	}
	n := copy(b, s.parts[0])
	s.parts = s.parts[1:]
	return n, nil
}

// A relay that stopped after the early 200 (its client gone) reads the rest of the answer before it
// decides: the result arrives, so the outcome is definitive. Negative control: without the drain the
// outcome is uncertain though the backend answered.
func TestLearnAnswerReadsTheRestAfterTheRelayStops(t *testing.T) {
	h := &Handler{}
	src := &progressReader{r: &staged{parts: []string{" ", completeResult}}, ans: &answerWatch{result: true}}
	if _, err := src.Read(make([]byte, 1)); err != nil { // the relay forwarded the early whitespace, then its client left
		t.Fatal(err)
	}
	o := &outcome{}
	h.learnAnswer(o, src)
	if o.uncertain || !src.ans.eof {
		t.Fatalf("an answer read to its end after the relay stopped: uncertain=%v eof=%v", o.uncertain, src.ans.eof)
	}
	// The same, with the result never closing: uncertain.
	src = &progressReader{r: &staged{parts: []string{" ", completeResult[:40]}}, ans: &answerWatch{result: true}}
	o = &outcome{}
	h.learnAnswer(o, src)
	if !o.uncertain {
		t.Fatal("an answer whose result never closed settled as definitive")
	}
}

// The tail keeps the last bytes across reads of any size.
func TestAnswerWatchTail(t *testing.T) {
	a := &answerWatch{result: true}
	for _, part := range []string{strings.Repeat("a", 100), "<CopyObjectResult><ETag>", "x</ETag></CopyObj", "ectResult>\n"} {
		a.record([]byte(part))
	}
	a.eof = true
	if !a.whole() {
		t.Fatalf("tail %q does not close its result", a.tail[:a.tailLen])
	}
	a = &answerWatch{result: true, eof: true}
	a.record([]byte("<CopyObjectResult><ETag>x</ETag>"))
	if a.whole() {
		t.Fatal("an unclosed result read as whole")
	}
}
