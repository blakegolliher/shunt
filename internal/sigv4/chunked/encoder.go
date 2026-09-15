package chunked

import (
	"encoding/base64"
	"fmt"
	"io"
	"strconv"
)

// DefaultChunkSize is the encoder's fixed chunk size. 64 KiB matches what SDKs emit.
const DefaultChunkSize = 64 << 10

// Trailer produces the trailing checksum value once the source is exhausted.
type Trailer interface {
	Algorithm() string // e.g. "CRC32"
	Checksum() string  // base64, valid only after the source returned io.EOF
}

// EncodedLength returns the exact aws-chunked (unsigned, with trailer) byte length for a body
// of decodedLen bytes at chunkSize with a trailer named trailerName whose base64 value has
// checksumLen characters. Upstream requests are sent with this Content-Length, never chunked
// transfer encoding, so a backend can pre-validate the frame.
func EncodedLength(decodedLen int64, chunkSize int, trailerName string, checksumLen int) int64 {
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}
	var n int64
	full := decodedLen / int64(chunkSize)
	rem := decodedLen % int64(chunkSize)
	hexLen := func(v int64) int64 { return int64(len(strconv.FormatInt(v, 16))) }
	// each chunk: <hex size>\r\n<data>\r\n
	n += full * (hexLen(int64(chunkSize)) + 2 + int64(chunkSize) + 2)
	if rem > 0 {
		n += hexLen(rem) + 2 + rem + 2
	}
	// final: 0\r\n<name>:<value>\r\n\r\n
	n += 3 + int64(len(trailerName)) + 1 + int64(checksumLen) + 2 + 2
	return n
}

// Base64Len is the base64 length of an algorithm's checksum.
func Base64Len(algorithm string) int {
	raw := checksumLengths[algorithm]
	return base64.StdEncoding.EncodedLen(raw)
}

// Encoder re-frames a plain body as STREAMING-UNSIGNED-PAYLOAD-TRAILER. It reads the source
// through the caller's buffer, so memory is bounded by chunkSize regardless of body size.
type Encoder struct {
	src       io.Reader
	trailer   Trailer
	name      string // x-amz-checksum-<algo>
	chunkSize int
	remaining int64 // decoded bytes still expected
	buf       []byte
	pending   []byte // encoded bytes not yet handed to the caller
	srcEOF    bool
	finalDone bool
	produced  int64
}

// NewEncoder wraps src (decodedLen bytes) with trailer name (e.g. "x-amz-checksum-crc32").
func NewEncoder(src io.Reader, decodedLen int64, name string, trailer Trailer, chunkSize int) *Encoder {
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}
	return &Encoder{src: src, trailer: trailer, name: name, chunkSize: chunkSize, remaining: decodedLen, buf: make([]byte, chunkSize)}
}

// Read implements io.Reader.
func (e *Encoder) Read(p []byte) (int, error) {
	for len(e.pending) == 0 {
		if e.finalDone {
			return 0, io.EOF
		}
		if err := e.fill(); err != nil {
			return 0, err
		}
	}
	n := copy(p, e.pending)
	e.pending = e.pending[n:]
	e.produced += int64(n)
	return n, nil
}

// fill reads one chunk of source and frames it, or emits the final chunk and trailer.
func (e *Encoder) fill() error {
	if !e.srcEOF {
		want := e.buf
		if e.remaining >= 0 && int64(len(want)) > e.remaining {
			want = want[:e.remaining]
		}
		n, err := io.ReadFull(e.src, want)
		if n > 0 {
			hdr := strconv.FormatInt(int64(n), 16) + "\r\n"
			// frame in place: header + data + \r\n, reusing a scratch slice
			frame := make([]byte, 0, len(hdr)+n+2)
			frame = append(frame, hdr...)
			frame = append(frame, want[:n]...)
			frame = append(frame, "\r\n"...)
			e.pending = frame
			e.remaining -= int64(n)
		}
		switch {
		case err == nil && e.remaining == 0:
			// All declared bytes delivered. Drain the source so a decoding reader consumes its
			// final chunk and trailer (and reports any trailer error) before we ask for the checksum.
			if derr := drain(e.src); derr != nil {
				return derr
			}
			e.srcEOF = true
		case err == io.EOF || err == io.ErrUnexpectedEOF:
			e.srcEOF = true
			if e.remaining > 0 {
				return fmt.Errorf("chunked encoder: source ended %d bytes short of x-amz-decoded-content-length", e.remaining)
			}
		case err != nil:
			return err
		}
		if n > 0 {
			return nil
		}
	}
	// Final chunk and trailer.
	e.finalDone = true
	e.pending = []byte("0\r\n" + e.name + ":" + e.trailer.Checksum() + "\r\n\r\n")
	return nil
}

// Produced returns the encoded bytes handed out so far.
func (e *Encoder) Produced() int64 { return e.produced }

// drain reads the source until EOF, failing if any data appears after the declared length.
func drain(r io.Reader) error {
	var one [1]byte
	for {
		n, err := r.Read(one[:])
		if n > 0 {
			return fmt.Errorf("chunked encoder: source produced data beyond x-amz-decoded-content-length")
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}
