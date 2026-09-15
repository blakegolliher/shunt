package chunked

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"hash"
	"io"
	"strconv"
	"time"
)

// SignedEncoder produces the client side of STREAMING-AWS4-HMAC-SHA256-PAYLOAD(-TRAILER): the
// framing a client sends. shunt itself never emits it; it exists for tests, s3diff's
// signing-mode client, and shunt probe, and it doubles as an independent check on the lifted
// decoder (the two were written separately and must agree on the AWS documentation vectors).
type SignedEncoder struct {
	src        io.Reader
	signingKey []byte
	prevSig    string
	prefixData string // "AWS4-HMAC-SHA256-PAYLOAD\n<date>\n<scope>\n"
	prefixTrl  string // "AWS4-HMAC-SHA256-TRAILER\n<date>\n<scope>\n"
	chunkSize  int
	buf        []byte
	pending    []byte
	done       bool
	trailer    string // x-amz-checksum-<algo>, "" for no trailer
	th         hash.Hash
	final      bool
}

// NewSignedEncoder wraps src. seedSignature is the request's header signature; signingKey is
// kSigning for the same scope; date is x-amz-date. trailer is "" or "x-amz-checksum-<algo>".
func NewSignedEncoder(src io.Reader, signingKey []byte, seedSignature string, date time.Time, region, trailer string, th hash.Hash, chunkSize int) *SignedEncoder {
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}
	scope := date.UTC().Format("20060102") + "/" + region + "/s3/aws4_request"
	ts := date.UTC().Format("20060102T150405Z")
	return &SignedEncoder{
		src: src, signingKey: signingKey, prevSig: seedSignature, chunkSize: chunkSize, buf: make([]byte, chunkSize),
		prefixData: "AWS4-HMAC-SHA256-PAYLOAD\n" + ts + "\n" + scope + "\n",
		prefixTrl:  "AWS4-HMAC-SHA256-TRAILER\n" + ts + "\n" + scope + "\n",
		trailer:    trailer, th: th,
	}
}

// SignedEncodedLength is the wire length for decodedLen bytes at chunkSize with an optional
// trailer (checksumLen is the base64 length; 0 for no trailer).
func SignedEncodedLength(decodedLen int64, chunkSize int, trailer string, checksumLen int) int64 {
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}
	const sigOverhead = len(";chunk-signature=") + 64 + 2 // + \r\n after the header
	var n int64
	full := decodedLen / int64(chunkSize)
	rem := decodedLen % int64(chunkSize)
	hexLen := func(v int64) int64 { return int64(len(strconv.FormatInt(v, 16))) }
	n += full * (hexLen(int64(chunkSize)) + int64(sigOverhead) + int64(chunkSize) + 2)
	if rem > 0 {
		n += hexLen(rem) + int64(sigOverhead) + rem + 2
	}
	n += 1 + int64(sigOverhead) // "0;chunk-signature=<64>\r\n"
	if trailer != "" {
		n += int64(len(trailer)) + 1 + int64(checksumLen) + 2 // name:value\r\n
		n += int64(len("x-amz-trailer-signature:")) + 64 + 2
	}
	n += 2 // final \r\n
	return n
}

func (e *SignedEncoder) sign(stringToSign string) string {
	m := hmac.New(sha256.New, e.signingKey)
	m.Write([]byte(stringToSign))
	return hex.EncodeToString(m.Sum(nil))
}

// Read implements io.Reader.
func (e *SignedEncoder) Read(p []byte) (int, error) {
	for len(e.pending) == 0 {
		if e.final {
			return 0, io.EOF
		}
		if err := e.fill(); err != nil {
			return 0, err
		}
	}
	n := copy(p, e.pending)
	e.pending = e.pending[n:]
	return n, nil
}

func (e *SignedEncoder) chunkSig(data []byte) string {
	sum := sha256.Sum256(data)
	empty := sha256.Sum256(nil)
	sts := e.prefixData + e.prevSig + "\n" + hex.EncodeToString(empty[:]) + "\n" + hex.EncodeToString(sum[:])
	sig := e.sign(sts)
	e.prevSig = sig
	return sig
}

func (e *SignedEncoder) fill() error {
	if !e.done {
		n, err := io.ReadFull(e.src, e.buf)
		if n > 0 {
			if e.th != nil {
				e.th.Write(e.buf[:n])
			}
			sig := e.chunkSig(e.buf[:n])
			frame := make([]byte, 0, 32+64+n+4)
			frame = append(frame, strconv.FormatInt(int64(n), 16)...)
			frame = append(frame, ";chunk-signature="...)
			frame = append(frame, sig...)
			frame = append(frame, "\r\n"...)
			frame = append(frame, e.buf[:n]...)
			frame = append(frame, "\r\n"...)
			e.pending = frame
		}
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			e.done = true
		} else if err != nil {
			return err
		}
		if n > 0 {
			return nil
		}
	}
	// Final zero chunk, optional trailer with its own signature, then the closing CRLF.
	e.final = true
	sig := e.chunkSig(nil)
	out := "0;chunk-signature=" + sig + "\r\n"
	if e.trailer != "" {
		value := base64Sum(e.th)
		line := e.trailer + ":" + value + "\n"
		lineSum := sha256.Sum256([]byte(line))
		tsig := e.sign(e.prefixTrl + e.prevSig + "\n" + hex.EncodeToString(lineSum[:]))
		out += e.trailer + ":" + value + "\r\n" + "x-amz-trailer-signature:" + tsig + "\r\n"
	}
	out += "\r\n"
	e.pending = []byte(out)
	return nil
}

func base64Sum(h hash.Hash) string {
	if h == nil {
		return ""
	}
	return b64(h.Sum(nil))
}
