package chunked

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/blakegolliher/shunt/internal/s3"
)

// Negative chunk sizes must be rejected as a malformed body, never used as a slice bound. The
// fuzzer found "-1;chunk-signature=…" panicking the lifted signed reader (upstream versitygw at
// 4dc0debf has the same parse); the unsigned reader already rejected negatives.
func TestSignedReaderRejectsNegativeChunkSize(t *testing.T) {
	for _, size := range []string{"-1", "-10", "-7fffffffffffffff"} {
		body := size + ";chunk-signature=" + strings.Repeat("0", 64) + "\r\n"
		rd, err := NewSignedChunkReader(bytes.NewReader([]byte(body)), AuthData{Access: "AK", Region: "us-east-1", Signature: strings.Repeat("0", 64)},
			"", []byte("key"), fixedDate, "", false, 99)
		if err != nil {
			t.Fatal(err)
		}
		_, err = io.ReadAll(rd)
		var se s3.Error
		if err == nil {
			t.Fatalf("size %s: accepted", size)
		}
		if ok := asS3Error(err, &se); !ok || se.Code != s3.IncompleteBody {
			t.Fatalf("size %s: want IncompleteBody, got %v", size, err)
		}
	}
}

func TestUnsignedReaderRejectsNegativeChunkSize(t *testing.T) {
	rd, err := newUnsignedChunkReader(bytes.NewReader([]byte("-1\r\nx\r\n0\r\n\r\n")), checksumTypeCrc32, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(rd); err == nil {
		t.Fatal("unsigned reader accepted a negative chunk size")
	}
}

func asS3Error(err error, target *s3.Error) bool {
	e, ok := err.(s3.Error) //nolint:errorlint // readers return s3.Error values directly
	if ok {
		*target = e
	}
	return ok
}
