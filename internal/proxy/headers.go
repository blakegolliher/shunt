package proxy

import (
	"net/http"
	"strings"
)

// hopByHop headers are never forwarded in either direction (RFC 9110 §7.6.1). Expect is
// deliberately not here: it must reach the upstream for 100-continue chaining.
var hopByHop = map[string]bool{
	"Connection":          true,
	"Keep-Alive":          true,
	"Proxy-Authenticate":  true,
	"Proxy-Authorization": true,
	"Proxy-Connection":    true,
	"Te":                  true,
	"Trailer":             true,
	"Transfer-Encoding":   true,
	"Upgrade":             true,
}

// copyHeaders copies src into dst, dropping hop-by-hop headers and any header named in
// src's Connection field. dst is expected to be empty.
func copyHeaders(dst, src http.Header) {
	var named []string
	for _, v := range src["Connection"] {
		for _, tok := range strings.Split(v, ",") {
			if tok = strings.TrimSpace(tok); tok != "" {
				named = append(named, http.CanonicalHeaderKey(tok))
			}
		}
	}
	for k, vv := range src {
		if hopByHop[k] {
			continue
		}
		skip := false
		for _, n := range named {
			if n == k {
				skip = true
				break
			}
		}
		if skip {
			continue
		}
		dst[k] = vv // share the slice; neither side mutates header values after this point
	}
}

// appendVia adds this proxy to the Via chain.
func appendVia(h http.Header, via string) {
	if prev := h.Get("Via"); prev != "" {
		h.Set("Via", prev+", "+via)
		return
	}
	h.Set("Via", via)
}
