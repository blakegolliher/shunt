package s3

import (
	"net"
	"strings"
)

var (
	reservedBucketPrefixes = []string{"xn--", "sthree-", "amzn-s3-demo-"}
	reservedBucketSuffixes = []string{"-s3alias", "--ol-s3", ".mrap", "--x-s3", "--table-s3"}
)

// ValidBucketName reports whether name follows AWS's rules for general purpose bucket names:
// 3–63 characters of lowercase letters, digits, '.' and '-'; starts and ends with a letter or
// digit; no two adjacent periods; not formatted as an IPv4 address; none of AWS's reserved
// prefixes or suffixes. shunt applies it to every client bucket name it creates and to every
// generated backend name, so a name valid here is valid on every backend type shunt fronts.
func ValidBucketName(name string) bool {
	if len(name) < 3 || len(name) > 63 {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9':
		case c == '.' || c == '-':
			if i == 0 || i == len(name)-1 {
				return false
			}
			if c == '.' && name[i-1] == '.' {
				return false
			}
		default:
			return false
		}
	}
	if ip := net.ParseIP(name); ip != nil {
		return false
	}
	for _, p := range reservedBucketPrefixes {
		if strings.HasPrefix(name, p) {
			return false
		}
	}
	for _, s := range reservedBucketSuffixes {
		if strings.HasSuffix(name, s) {
			return false
		}
	}
	return true
}
