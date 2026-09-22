package web

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHandlerServesIndexAndSPAFallback(t *testing.T) {
	h := Handler()
	for _, target := range []string{"/", "/clusters/source"} {
		r := httptest.NewRecorder()
		h.ServeHTTP(r, httptest.NewRequest(http.MethodGet, target, nil))
		if r.Code != http.StatusOK || !strings.Contains(strings.ToLower(r.Body.String()), "shunt") {
			t.Fatalf("%s: %d %q", target, r.Code, r.Body.String())
		}
	}
}

func TestHandlerDoesNotMaskMissingAssetsOrAPI(t *testing.T) {
	h := Handler()
	for _, target := range []string{"/missing.js", "/v1/control"} {
		r := httptest.NewRecorder()
		h.ServeHTTP(r, httptest.NewRequest(http.MethodGet, target, nil))
		if r.Code != http.StatusNotFound {
			t.Fatalf("%s: status %d", target, r.Code)
		}
	}
}
