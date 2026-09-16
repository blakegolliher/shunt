package s3

import (
	"net/url"
	"strings"
)

// DecodeListingKey undoes the percent-encoding of an encoding-type=url listing key. A backend that
// ignores encoding-type returns the key verbatim, which decodes to itself unless the key really
// contains a percent escape; that is the one key shape this normalization cannot tell apart, and
// it is recorded in docs/reference/backend-compat.md rather than guessed at. The merged listing
// and purge-source's listing diff both compare keys this way (docs/DESIGN.md §2.5).
func DecodeListingKey(s string) string {
	if !strings.Contains(s, "%") {
		return s
	}
	if d, err := url.PathUnescape(s); err == nil {
		return d
	}
	return s
}
