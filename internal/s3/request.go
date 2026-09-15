package s3

import (
	"net"
	"net/http"
	"net/url"
	"strings"
)

// Style is how the client addressed the bucket.
type Style uint8

// Addressing styles.
const (
	StylePath Style = iota
	StyleVirtualHost
)

func (s Style) String() string {
	if s == StyleVirtualHost {
		return "virtual-host"
	}
	return "path"
}

// Level is the resource level of the request path.
type Level uint8

// Resource levels.
const (
	LevelService Level = iota
	LevelBucket
	LevelObject
)

func (l Level) String() string {
	switch l {
	case LevelBucket:
		return "bucket"
	case LevelObject:
		return "object"
	}
	return "service"
}

// RequestInfo is the parsed request model. Key is decoded for routing decisions (ramp hashing in
// POC-4); the raw path is what gets proxied. Key never appears in logs or metrics.
type RequestInfo struct {
	Bucket string
	Key    string
	Op     Op
	Style  Style
	Level  Level
	Query  url.Values
}

// Domains is the set of configured wildcard domains, pre-processed for suffix matching.
type Domains struct {
	suffixes []string // ".example.net" for "*.example.net"
	bare     []string // "example.net"
}

// NewDomains accepts entries like "*.s3.example.net" or "s3.example.net". A wildcard entry
// matches any single or multi-label prefix as the bucket; the bare name is path-style.
func NewDomains(entries []string) Domains {
	var d Domains
	for _, e := range entries {
		e = strings.ToLower(strings.TrimSuffix(e, "."))
		if e == "" {
			continue
		}
		base := strings.TrimPrefix(e, "*.")
		d.suffixes = append(d.suffixes, "."+base)
		d.bare = append(d.bare, base)
	}
	return d
}

// bucketFromHost returns the bucket named by a virtual-host Host header, or "" for path style.
func (d Domains) bucketFromHost(host string) string {
	host = strings.ToLower(host)
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	for i, suf := range d.suffixes {
		if host == d.bare[i] {
			return ""
		}
		if strings.HasSuffix(host, suf) && len(host) > len(suf) {
			return host[:len(host)-len(suf)]
		}
	}
	return ""
}

// Parse extracts bucket, key, level, style, and operation from r. It never fails: an
// unrecognizable request is LevelService/OpUnknown and still proxied.
func Parse(r *http.Request, d Domains) RequestInfo {
	info := RequestInfo{Query: r.URL.Query()}
	path := r.URL.EscapedPath()
	rest := strings.TrimPrefix(path, "/")

	if b := d.bucketFromHost(r.Host); b != "" {
		info.Style = StyleVirtualHost
		info.Bucket = b
		info.Level = LevelBucket
		if rest != "" {
			info.Level = LevelObject
			info.Key = unescape(rest)
		}
	} else if bucket, key, hasKey := strings.Cut(rest, "/"); bucket != "" {
		// An empty bucket segment ("//key") is not a bucket request; it stays at service level
		// with OpUnknown and is proxied as-is (found by FuzzParse).
		// The bucket segment is taken raw: valid bucket names never need escaping, and decoding
		// would let "%2F" smuggle a "/" into a bucket name (found by FuzzParse).
		info.Bucket = bucket
		info.Level = LevelBucket
		if hasKey && key != "" {
			info.Level = LevelObject
			info.Key = unescape(key)
		} else if hasKey {
			// "/bucket/" — a trailing slash on the bucket is a bucket-level request.
			info.Level = LevelBucket
		}
	}
	info.Op = Classify(r.Method, info.Level, info.Query, r.Header)
	return info
}

// unescape decodes percent-encoding; '+' stays literal (S3 keys are path segments, not form data).
func unescape(s string) string {
	if !strings.Contains(s, "%") {
		return s
	}
	if u, err := url.PathUnescape(s); err == nil {
		return u
	}
	return s
}
