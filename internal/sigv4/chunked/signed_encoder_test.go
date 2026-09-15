package chunked

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"hash/crc32"
	"io"
	"strings"
	"testing"
	"time"
)

// The AWS documentation example for streaming SigV4 (sigv4-streaming.html): 66560 bytes of 'a',
// 64 KiB chunks, AKIAIOSFODNN7EXAMPLE / wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY, us-east-1,
// 20130524T000000Z, seed signature 4f232c43…. These are AWS's published vectors.
const (
	awsSecret = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	awsSeed   = "4f232c4386841ef735655705268965c44a0e4690baa4adea153f7db9fa80a0a9"
	awsChunk1 = "ad80c730a21e5b8d04586a2213dd63b9a0e99e0e2307b0ade35a65485a288648"
	awsChunk2 = "0055627c9e194cb4542bae2aa5492e3c1575bbb81b612b7d234b86a503ef5497"
	awsChunk3 = "b6c6ea8a5354eaf15b3cb7646744f4275b71ea724fed81ceb9323e279d449df9"
)

var awsDate = time.Date(2013, 5, 24, 0, 0, 0, 0, time.UTC)

func awsSigningKey() []byte {
	h := func(k []byte, d string) []byte { m := hmac.New(sha256.New, k); m.Write([]byte(d)); return m.Sum(nil) }
	k := h([]byte("AWS4"+awsSecret), "20130524")
	k = h(k, "us-east-1")
	k = h(k, "s3")
	return h(k, "aws4_request")
}

func TestSignedEncoderMatchesAWSVectors(t *testing.T) {
	payload := bytes.Repeat([]byte("a"), 66560)
	enc := NewSignedEncoder(bytes.NewReader(payload), awsSigningKey(), awsSeed, awsDate, "us-east-1", "", nil, 65536)
	out, err := io.ReadAll(enc)
	if err != nil {
		t.Fatal(err)
	}
	s := string(out)
	for _, sig := range []string{awsChunk1, awsChunk2, awsChunk3} {
		if !strings.Contains(s, "chunk-signature="+sig) {
			t.Errorf("missing AWS vector signature %s", sig[:12])
		}
	}
	if !strings.HasPrefix(s, "10000;chunk-signature="+awsChunk1+"\r\n") {
		t.Errorf("first header wrong: %q", s[:90])
	}
	if int64(len(out)) != SignedEncodedLength(66560, 65536, "", 0) {
		t.Errorf("length %d vs precomputed %d", len(out), SignedEncodedLength(66560, 65536, "", 0))
	}
	// The lifted decoder must accept exactly this wire form.
	rd, err := NewSignedChunkReader(bytes.NewReader(out), AuthData{Access: "AKIAIOSFODNN7EXAMPLE", Region: "us-east-1", Signature: awsSeed}, "", awsSigningKey(), awsDate, "", false, 66560)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(rd)
	if err != nil {
		t.Fatalf("decoder rejected AWS vector stream: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("decoded payload differs")
	}
}

func TestSignedEncoderTrailerRoundTrip(t *testing.T) {
	for _, size := range []int{0, 1, 65535, 65536, 200000} {
		payload := bytes.Repeat([]byte("xyz"), size/3+1)[:size]
		enc := NewSignedEncoder(bytes.NewReader(payload), awsSigningKey(), awsSeed, awsDate, "us-east-1", "x-amz-checksum-crc32", crc32.NewIEEE(), 65536)
		out, err := io.ReadAll(enc)
		if err != nil {
			t.Fatal(err)
		}
		if int64(len(out)) != SignedEncodedLength(int64(size), 65536, "x-amz-checksum-crc32", 8) {
			t.Fatalf("size %d: length %d vs precomputed %d", size, len(out), SignedEncodedLength(int64(size), 65536, "x-amz-checksum-crc32", 8))
		}
		rd, err := NewSignedChunkReader(bytes.NewReader(out), AuthData{Access: "AK", Region: "us-east-1", Signature: awsSeed}, "", awsSigningKey(), awsDate, checksumTypeCrc32, true, int64(size))
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(rd)
		if err != nil {
			t.Fatalf("size %d: %v", size, err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatalf("size %d: payload differs", size)
		}
		// Tamper one payload byte: the chunk signature must fail.
		bad := append([]byte(nil), out...)
		if size > 0 {
			i := bytes.IndexByte(bad, '\n') + 1
			bad[i] ^= 0xff
			rd2, _ := NewSignedChunkReader(bytes.NewReader(bad), AuthData{Access: "AK", Region: "us-east-1", Signature: awsSeed}, "", awsSigningKey(), awsDate, checksumTypeCrc32, true, int64(size))
			if _, err := io.ReadAll(rd2); err == nil {
				t.Fatalf("size %d: tampered body accepted", size)
			}
		}
	}
}
