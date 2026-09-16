package chunked

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"hash"
	"hash/crc32"
	"io"
	"strings"
	"testing"
	"time"
)

var fixedDate = time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC)

// hashTrailer computes the checksum as bytes pass through.
type hashTrailer struct {
	h    hash.Hash
	algo string
}

func (t *hashTrailer) Algorithm() string { return t.algo }
func (t *hashTrailer) Checksum() string  { return base64.StdEncoding.EncodeToString(t.h.Sum(nil)) }
func (t *hashTrailer) wrap(r io.Reader) io.Reader {
	return io.TeeReader(r, t.h)
}

func TestEncoderRoundTripThroughUnsignedReader(t *testing.T) {
	for _, size := range []int{0, 1, 100, 64 << 10, 64<<10 + 1, 200<<10 + 17} {
		for _, chunk := range []int{8192, 64 << 10} {
			payload := bytes.Repeat([]byte("abcdefghij"), size/10+1)[:size]
			tr := &hashTrailer{h: crc32.NewIEEE(), algo: "CRC32"}
			enc := NewEncoder(tr.wrap(bytes.NewReader(payload)), int64(size), "x-amz-checksum-crc32", tr, chunk)
			encoded, err := io.ReadAll(enc)
			if err != nil {
				t.Fatalf("size %d chunk %d: encode: %v", size, chunk, err)
			}
			if want := EncodedLength(int64(size), chunk, "x-amz-checksum-crc32", Base64Len("CRC32")); int64(len(encoded)) != want {
				t.Fatalf("size %d chunk %d: encoded %d bytes, precomputed %d\n%q", size, chunk, len(encoded), want, encoded[:min(200, len(encoded))])
			}
			// Decode with the lifted unsigned reader, which also validates the trailer checksum.
			rd, err := newUnsignedChunkReader(bytes.NewReader(encoded), checksumTypeCrc32, int64(size))
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := io.ReadAll(rd)
			if err != nil {
				t.Fatalf("size %d chunk %d: decode: %v", size, chunk, err)
			}
			if !bytes.Equal(decoded, payload) {
				t.Fatalf("size %d chunk %d: round trip mismatch", size, chunk)
			}
			if rd.Checksum() != tr.Checksum() {
				t.Fatalf("checksum mismatch %s vs %s", rd.Checksum(), tr.Checksum())
			}
		}
	}
}

func TestEncoderShortSource(t *testing.T) {
	tr := &hashTrailer{h: sha256.New(), algo: "SHA256"}
	enc := NewEncoder(tr.wrap(strings.NewReader("short")), 100, "x-amz-checksum-sha256", tr, 8192)
	if _, err := io.ReadAll(enc); err == nil {
		t.Fatal("expected an error when the source is shorter than the declared length")
	}
}

func TestBase64Len(t *testing.T) {
	for algo, want := range map[string]int{"CRC32": 8, "CRC32C": 8, "SHA1": 28, "SHA256": 44, "CRC64NVME": 12} {
		if got := Base64Len(algo); got != want {
			t.Errorf("%s: %d want %d", algo, got, want)
		}
	}
}

func FuzzUnsignedChunkReader(f *testing.F) {
	f.Add([]byte("5\r\nhello\r\n0\r\nx-amz-checksum-crc32:AAAAAA==\r\n\r\n"), int64(5))
	f.Add([]byte("0\r\n\r\n"), int64(0))
	f.Add([]byte("zz\r\n"), int64(1))
	f.Add([]byte(""), int64(10))
	f.Fuzz(func(t *testing.T, body []byte, declared int64) {
		if declared < 0 || declared > 1<<20 {
			return
		}
		rd, err := newUnsignedChunkReader(bytes.NewReader(body), checksumTypeCrc32, declared)
		if err != nil {
			return
		}
		n, _ := io.Copy(io.Discard, rd)
		if n > declared {
			t.Fatalf("decoded %d bytes, more than the declared %d", n, declared)
		}
	})
}

func FuzzSignedChunkReader(f *testing.F) {
	f.Add([]byte("5;chunk-signature="+strings.Repeat("a", 64)+"\r\nhello\r\n0;chunk-signature="+strings.Repeat("b", 64)+"\r\n\r\n"), int64(5))
	f.Add([]byte("garbage"), int64(3))
	f.Add([]byte(""), int64(0))
	f.Fuzz(func(t *testing.T, body []byte, declared int64) {
		if declared < 0 || declared > 1<<20 {
			return
		}
		rd, err := NewSignedChunkReader(bytes.NewReader(body), AuthData{Access: "AK", Region: "r", Signature: strings.Repeat("0", 64)},
			"canon", []byte("key"), fixedDate, "", false, declared)
		if err != nil {
			return
		}
		n, _ := io.Copy(io.Discard, rd)
		if n > declared {
			t.Fatalf("decoded %d bytes, more than the declared %d", n, declared)
		}
	})
}

func BenchmarkEncode1MiB(b *testing.B) {
	payload := make([]byte, 1<<20)
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	for b.Loop() {
		tr := &hashTrailer{h: crc32.NewIEEE(), algo: "CRC32"}
		enc := NewEncoder(tr.wrap(bytes.NewReader(payload)), int64(len(payload)), "x-amz-checksum-crc32", tr, DefaultChunkSize)
		if _, err := io.Copy(io.Discard, enc); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkUnsignedDecode1MiB(b *testing.B) {
	payload := make([]byte, 1<<20)
	tr := &hashTrailer{h: crc32.NewIEEE(), algo: "CRC32"}
	enc := NewEncoder(tr.wrap(bytes.NewReader(payload)), int64(len(payload)), "x-amz-checksum-crc32", tr, DefaultChunkSize)
	encoded, _ := io.ReadAll(enc)
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	for b.Loop() {
		rd, _ := newUnsignedChunkReader(bytes.NewReader(encoded), checksumTypeCrc32, int64(len(payload)))
		if _, err := io.Copy(io.Discard, rd); err != nil {
			b.Fatal(err)
		}
	}
}
