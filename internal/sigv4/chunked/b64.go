package chunked

import "encoding/base64"

func b64(b []byte) string { return base64.StdEncoding.EncodeToString(b) }
