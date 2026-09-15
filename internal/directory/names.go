package directory

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"
)

// BackendName generates the backend bucket name for a client bucket created through shunt:
// <tenant>-<4 hex>-<bucket> (docs/CONTEXT.md naming rule). The 4 hex are the first two bytes of
// SHA-256 over "tenant/bucket", so the name is stable for a pair and differs between tenants that
// use the same bucket name. attempt > 0 mixes a counter into the hash, for retries after a backend
// reports the name taken. The result is lowercased and cut to 63 characters by shortening the
// bucket part; for a valid tenant and a valid bucket name it is a valid S3 bucket name.
func BackendName(tenant, bucket string, attempt int) string {
	h := sha256.New()
	h.Write([]byte(tenant + "/" + bucket))
	if attempt > 0 {
		h.Write([]byte("#" + strconv.Itoa(attempt)))
	}
	var sum [sha256.Size]byte
	h.Sum(sum[:0])
	name := strings.ToLower(tenant) + "-" + hex.EncodeToString(sum[:2]) + "-" + strings.ToLower(bucket)
	if len(name) > 63 {
		name = strings.TrimRight(name[:63], ".-")
	}
	return name
}
