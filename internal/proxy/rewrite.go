package proxy

import (
	"encoding/xml"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"

	"github.com/blakegolliher/shunt/internal/s3"
	"github.com/blakegolliher/shunt/internal/s3/xmlrw"
	"github.com/blakegolliher/shunt/internal/upstream"
)

// rewriteOps are the operations whose 2xx bodies can echo a backend bucket name, an endpoint, or
// a backend uploadId (ADR-0006). Every response with a status of 300 or more is rewritten too:
// error bodies echo the bucket and the upstream path. CopyObject, UploadPartCopy, and
// CompleteMultipartUpload are here because they can answer 200 with an <Error> body.
var rewriteOps = map[s3.Op]bool{
	s3.OpListObjects: true, s3.OpListObjectsV2: true, s3.OpListObjectVersions: true,
	s3.OpListMultipartUploads: true, s3.OpCreateMultipartUpload: true, s3.OpListParts: true,
	s3.OpCompleteMultipartUpload: true, s3.OpCopyObject: true, s3.OpUploadPartCopy: true,
}

func (h *Handler) shouldRewrite(r *http.Request, o *outcome, resp *http.Response) bool {
	if r.Method == http.MethodHead || resp.StatusCode < 200 || resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusNotModified {
		return false
	}
	if ce := resp.Header.Get("Content-Encoding"); ce != "" && !strings.EqualFold(ce, "identity") {
		return false
	}
	return resp.StatusCode >= 300 || rewriteOps[o.info.Op]
}

// editor rewrites one response's echoes for xmlrw: the backend bucket becomes the client's
// bucket, cluster endpoints become the client-facing host, uploadIds get the cluster prefix.
type editor struct {
	backend, client string
	clusterID       string
	vhost           bool
	scheme, host    string // client-facing
	endpoints       []string
}

func newEditor(r *http.Request, info s3.RequestInfo, cl *upstream.Cluster, backend string) *editor {
	scheme := "https"
	if r.TLS == nil {
		scheme = "http"
	}
	return &editor{backend: backend, client: info.Bucket, clusterID: cl.ID, vhost: info.Style == s3.StyleVirtualHost,
		scheme: scheme, host: r.Host, endpoints: cl.Endpoints}
}

// Edit implements xmlrw.Editor. text and the result are XML-escaped character data.
func (e *editor) Edit(dst []byte, f xmlrw.Field, text []byte) []byte {
	switch f {
	case xmlrw.FieldBucket:
		if string(text) == e.backend {
			return appendEscaped(dst, e.client)
		}
	case xmlrw.FieldUploadID:
		if len(text) > 0 {
			dst = append(dst, e.clusterID...)
			dst = append(dst, '~')
		}
	case xmlrw.FieldLocation:
		if loc, ok := e.location(string(text), true); ok {
			return append(dst, loc...)
		}
		return append(dst, e.scrub(string(text), true)...)
	case xmlrw.FieldResource:
		return append(dst, e.resource(string(text), true)...)
	case xmlrw.FieldMessage:
		return append(dst, e.scrub(string(text), true)...)
	case xmlrw.FieldEndpoint:
		return appendEscaped(dst, e.host)
	}
	return append(dst, text...)
}

// locationHeader rewrites a Location response header (not XML-escaped).
func (e *editor) locationHeader(v string) string {
	if loc, ok := e.location(v, false); ok {
		return loc
	}
	return e.scrub(v, false)
}

// location rebuilds a URL naming the backend bucket, path-style or virtual-host, as the URL the
// client addressed: scheme://host/<client>/<key> for path-style, scheme://host/<key> for
// virtual-host. The key part is kept exactly as the backend wrote it.
func (e *editor) location(s string, escaped bool) (string, bool) {
	i := strings.Index(s, "://")
	if i < 0 {
		return "", false
	}
	authority, path, _ := strings.Cut(s[i+3:], "/")
	var key string
	switch {
	case strings.HasPrefix(authority, e.backend+"."):
		key = path
	case path == e.backend:
	case strings.HasPrefix(path, e.backend+"/"):
		key = path[len(e.backend)+1:]
	default:
		return "", false
	}
	base := e.scheme + "://" + e.hostText(escaped) + "/"
	switch {
	case e.vhost:
		return base + key, true
	case key == "":
		return base + e.client, true
	}
	return base + e.client + "/" + key, true
}

// resource rewrites an error's <Resource>: the upstream path always starts with the backend
// bucket, which becomes the client bucket (path-style) or disappears (virtual-host).
func (e *editor) resource(s string, escaped bool) string {
	pre := "/" + e.backend
	if s == pre || strings.HasPrefix(s, pre+"/") {
		rest := s[len(pre):]
		if e.vhost {
			if rest == "" {
				rest = "/"
			}
			return rest
		}
		return "/" + e.client + rest
	}
	return e.scrub(s, escaped)
}

// scrub replaces the backend bucket name and cluster endpoints anywhere in free text.
func (e *editor) scrub(s string, escaped bool) string {
	if e.backend != e.client && e.backend != "" {
		s = strings.ReplaceAll(s, e.backend, e.client)
	}
	host := e.hostText(escaped)
	bare := host
	if hh, _, err := net.SplitHostPort(e.host); err == nil {
		bare = hh
		if escaped {
			bare = escapeString(hh)
		}
	}
	for _, ep := range e.endpoints {
		s = strings.ReplaceAll(s, ep, host)
		if hn, _, err := net.SplitHostPort(ep); err == nil && hn != "" && net.ParseIP(hn) == nil {
			s = strings.ReplaceAll(s, hn, bare)
		}
	}
	return s
}

func (e *editor) hostText(escaped bool) string {
	if escaped {
		return escapeString(e.host)
	}
	return e.host
}

func escapeString(s string) string {
	if !strings.ContainsAny(s, `&<>"'`+"\r\n\t") {
		return s
	}
	var b strings.Builder
	_ = xml.EscapeText(&b, []byte(s))
	return b.String()
}

func appendEscaped(dst []byte, s string) []byte { return append(dst, escapeString(s)...) }

// spill holds a rewritten body in a fixed scratch buffer. If the body completes within it the
// response goes out with an exact Content-Length; otherwise the headers go out without one
// (chunked) the moment the scratch would overflow, followed by the held bytes and the rest of the
// stream (ADR-0006).
type spill struct {
	w      http.ResponseWriter
	dst    io.Writer
	status int
	buf    []byte
	n      int
	sent   bool
}

func (s *spill) Write(p []byte) (int, error) {
	if s.sent {
		return s.dst.Write(p)
	}
	if s.n+len(p) <= len(s.buf) {
		copy(s.buf[s.n:], p)
		s.n += len(p)
		return len(p), nil
	}
	s.w.WriteHeader(s.status)
	s.sent = true
	if s.n > 0 {
		if _, err := s.dst.Write(s.buf[:s.n]); err != nil {
			return 0, err
		}
	}
	return s.dst.Write(p)
}

// finish sends a body that fit in the scratch, with its exact length.
func (s *spill) finish() error {
	if s.sent {
		return nil
	}
	s.w.Header().Set("Content-Length", strconv.Itoa(s.n))
	s.w.WriteHeader(s.status)
	s.sent = true
	if s.n == 0 {
		return nil
	}
	_, err := s.dst.Write(s.buf[:s.n])
	return err
}
