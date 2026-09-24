package migrate

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

// idLen is the length of an opaque cluster id (upstream.ClusterID), and of a bucket tag.
const idLen = 6

// BucketTag tells two backend buckets on one cluster apart in an upload id: the first six hex of
// the bucket name's SHA-256. An upload id carries one, <clusterID>.<tag>~<backendID>, only when a
// request's two buckets share a cluster, a move between two legs there (ADR-0018 N3b).
func BucketTag(bucket string) string {
	sum := sha256.Sum256([]byte(bucket))
	return hex.EncodeToString(sum[:3])
}

// SplitUploadPrefix splits what DecodeUploadID returned as the prefix into the cluster id and the
// bucket tag, "" when the id carries none.
func SplitUploadPrefix(prefix string) (clusterID, tag string) {
	clusterID, tag, _ = strings.Cut(prefix, ".")
	return clusterID, tag
}

// encodeUploadID returns <clusterID>~<backendID>. An empty backend id stays empty, so the empty
// markers of an unpaginated listing are not turned into something that looks like an id.
func encodeUploadID(clusterID, backendID string) string {
	if backendID == "" {
		return ""
	}
	return clusterID + "~" + backendID
}

// DecodeUploadID splits an incoming uploadId on its first '~'. It is prefixed only when what
// precedes the '~' has the shape of a cluster id (six lowercase hex characters), or of a cluster id
// and a bucket tag joined by '.' (SplitUploadPrefix takes them apart); the backend id
// after it may contain anything, including more '~'. An id without that prefix is returned whole
// with prefixed false: the tolerance rule routes it by the bucket's placement, so uploads started
// before the codec existed still complete.
func DecodeUploadID(v string) (prefix, backendID string, prefixed bool) {
	i := strings.IndexByte(v, '~')
	switch {
	case i == idLen && isLowerHex(v[:i]):
	case i == 2*idLen+1 && v[idLen] == '.' && isLowerHex(v[:idLen]) && isLowerHex(v[idLen+1:i]):
		// <clusterID>.<bucket tag>: one of two buckets on a cluster (BucketTag).
	default:
		return "", v, false
	}
	return v[:i], v[i+1:], true
}

func isLowerHex(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}
