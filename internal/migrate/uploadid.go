package migrate

import "strings"

// IDLen is the length of an opaque cluster id (upstream.ClusterID).
const IDLen = 6

// EncodeUploadID returns <clusterID>~<backendID>. An empty backend id stays empty, so the empty
// markers of an unpaginated listing are not turned into something that looks like an id.
func EncodeUploadID(clusterID, backendID string) string {
	if backendID == "" {
		return ""
	}
	return clusterID + "~" + backendID
}

// DecodeUploadID splits an incoming uploadId on its first '~'. It is prefixed only when what
// precedes the '~' has the shape of a cluster id (six lowercase hex characters); the backend id
// after it may contain anything, including more '~'. An id without that prefix is returned whole
// with prefixed false: the tolerance rule routes it by the bucket's placement, so uploads started
// before the codec existed still complete.
func DecodeUploadID(v string) (clusterID, backendID string, prefixed bool) {
	i := strings.IndexByte(v, '~')
	if i != IDLen || !isLowerHex(v[:i]) {
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
