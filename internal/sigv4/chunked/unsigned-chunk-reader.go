// Copyright 2024 Versity Software
// This file is licensed under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
// Modified by Blake Golliher for github.com/blakegolliher/shunt, 2026-09-15: package renamed to chunked;
// debuglogger calls removed; s3err replaced by the shunt s3 error table; aws-sdk types replaced by string;
// sha512/md5/xxhash hashers dropped (Versity extensions, not AWS algorithms).
// Modified by Blake Golliher for github.com/blakegolliher/shunt, 2026-09-16: NewUnsignedChunkReader
// unexported and an unused method removed (G1: the reader is the test oracle for shunt's encoder,
// and only this package's tests use it).

package chunked

import (
	"bufio"
	"bytes"
	"crypto/sha1" //nolint:gosec // G505: SHA-1 is an S3 checksum algorithm here, not a security primitive
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"hash"
	"hash/crc32"
	"hash/crc64"
	"io"
	"math/bits"
	"strconv"
	"strings"

	"github.com/blakegolliher/shunt/internal/s3"
)

var (
	trailerDelim       = []byte{'\n', '\r', '\n'}
	minChunkSize int64 = 8192
)

// UnsignedChunkReader decodes AWS aws-chunked unsigned streaming request
// bodies. It strips chunk headers/trailers, validates chunk framing and trailing
// checksums, and exposes only object payload bytes through Read.
//
// The reader is intentionally streaming: chunk payload bytes are read directly
// into the caller's buffer. It only keeps bounded header/trailer parsing state
// and a few counters, so a large client-declared chunk does not become a large
// allocation in the gateway.
type UnsignedChunkReader struct {
	reader         *bufio.Reader
	checksumType   checksumType
	parsedChecksum string
	hasher         hash.Hash
	// chunkDataLeft is the number of object data bytes still unread from the
	// current chunk. These bytes are streamed directly into the caller's buffer
	chunkDataLeft int64
	// needChunkEnd means the current chunk payload has been fully returned and
	// the next Read must consume the chunk's trailing "\r\n" before parsing the
	// next chunk header
	needChunkEnd bool
	// isEOF is set after the zero-sized chunk and trailer are parsed, so later
	// reads return io.EOF without touching the underlying request body again.
	isEOF bool
	// The chunk-size rule needs information about the previous chunk: if the
	// next parsed chunk is non-zero, the previous chunk was not the final chunk
	// and must have been at least minChunkSize
	chunkNumber     int64
	lastChunkNumber int64
	lastChunkSize   int64
	seenChunk       bool
	// TODO: Keep these fields ready for the future InvalidChunkSizeError shape:
	// <Chunk> should be invalidChunkNumber and <BadChunkSize> should be
	// invalidChunkSize
	invalidChunkNumber int64
	invalidChunkSize   int64

	cLength int64
	// This data is necessary for the decoded content length mismatch error
	// TODO: add 'NumberBytesExpected' and 'NumberBytesProvided' in the error
	dataRead int64
}

func newUnsignedChunkReader(r io.Reader, ct checksumType, decContentLength int64) (*UnsignedChunkReader, error) {
	var hasher hash.Hash
	var err error
	if ct != "" {
		hasher, err = getHasher(ct)
	}
	if err != nil {
		return nil, err
	}

	return &UnsignedChunkReader{
		reader:       bufio.NewReaderSize(r, maxHeaderSize),
		checksumType: ct,
		hasher:       hasher,
		cLength:      decContentLength,
	}, nil
}

// Algorithm returns the checksum algorithm
func (ucr *UnsignedChunkReader) Algorithm() string {
	return strings.TrimPrefix(string(ucr.checksumType), "x-amz-checksum-")
}

// Checksum returns the parsed trailing checksum
func (ucr *UnsignedChunkReader) Checksum() string {
	return ucr.parsedChecksum
}

func (ucr *UnsignedChunkReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	if ucr.isEOF {
		return 0, io.EOF
	}

	var n int

	for n < len(p) {
		// Once a chunk body is drained, validate its CRLF boundary before any
		// more data can be returned. This preserves chunk framing validation
		// while still allowing the body bytes themselves to pass through.
		if ucr.needChunkEnd {
			if err := ucr.readAndSkip('\r', '\n'); err != nil {
				return n, err
			}
			ucr.needChunkEnd = false
		}

		if ucr.chunkDataLeft == 0 {
			// No payload is pending, so the next bytes must be the chunk-size
			chunkSize, err := ucr.extractChunkSize()
			if err != nil {
				return n, err
			}

			if chunkSize == 0 {
				// The zero-sized chunk ends the object data stream. At this
				// point all declared chunk payload bytes have been consumed, so
				// validate the decoded content length and then parse trailers.
				ucr.isEOF = true
				if ucr.cLength != ucr.dataRead {
					err := errContentLengthMismatch
					return n, err
				}

				if err := ucr.readTrailer(); err != nil {
					return n, err
				}

				return n, io.EOF
			}

			ucr.dataRead += chunkSize
			ucr.chunkDataLeft = chunkSize
		}

		contentLeft := ucr.remainingContentLength()
		if contentLeft == 0 && ucr.chunkDataLeft > 0 {
			// The client declared more chunk payload bytes than the decoded
			// content length allows. Do not pass those bytes to the backend
			// writer; return the S3 error from this reader instead.
			return n, ucr.handleExcessChunkData()
		}

		// Read only as much object data as fits in p, the current chunk, and the
		// decoded content length. This is the key streaming path: data is copied
		// from the request body into p without allocating a chunk-sized buffer.
		limit := min(int64(len(p)-n), ucr.chunkDataLeft, contentLeft)
		readEnd := int64(n) + limit
		read, err := ucr.reader.Read(p[n:readEnd])
		if read > 0 {
			if ucr.hasher != nil {
				if _, hashErr := ucr.hasher.Write(p[n : n+read]); hashErr != nil {
					return n, hashErr
				}
			}
			ucr.chunkDataLeft -= int64(read)
			n += read
			if ucr.chunkDataLeft == 0 {
				ucr.needChunkEnd = true
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = s3.Lookup(s3.IncompleteBody)
			}
			return n, err
		}
		if read == 0 {
			return n, nil
		}
	}

	return n, nil
}

func (ucr *UnsignedChunkReader) remainingContentLength() int64 {
	// dataRead is the sum of parsed chunk sizes, while chunkDataLeft is the
	// unread part of the current chunk. Their difference is the decoded object
	// byte count already returned or ready to return to the caller.
	read := ucr.dataRead - ucr.chunkDataLeft
	if read >= ucr.cLength {
		return 0
	}

	return ucr.cLength - read
}

func (ucr *UnsignedChunkReader) handleExcessChunkData() error {
	// When the decoded content length is exhausted in the middle of a chunk,
	// distinguish "extra payload data" from "chunk ended before its declared
	// size". The latter is an incomplete body; the former is a content-length
	// mismatch. Peek keeps the bytes buffered and avoids forwarding either case
	// to the backend writer.
	buf, err := ucr.reader.Peek(2)
	if len(buf) > 0 && buf[0] != '\r' {
		return errContentLengthMismatch
	}
	if len(buf) > 1 && buf[1] != '\n' {
		return errContentLengthMismatch
	}
	if err != nil {
		return s3.Lookup(s3.IncompleteBody)
	}
	return s3.Lookup(s3.IncompleteBody)
}

// Reads and validates the bytes provided from the underlying io.Reader
func (ucr *UnsignedChunkReader) readAndSkip(data ...byte) error {
	for _, d := range data {
		b, err := ucr.reader.ReadByte()
		if err != nil {
			return s3.Lookup(s3.IncompleteBody)
		}

		if b != d {
			return s3.Lookup(s3.IncompleteBody)
		}
	}

	return nil
}

// Extracts the chunk size from the payload
func (ucr *UnsignedChunkReader) extractChunkSize() (int64, error) {
	line, err := ucr.readChunkSizeLine()
	if err != nil {
		return 0, s3.Lookup(s3.IncompleteBody)
	}

	chunkSize, err := strconv.ParseInt(line, 16, 64)
	if err != nil || chunkSize < 0 {
		return 0, s3.Lookup(s3.IncompleteBody)
	}
	ucr.chunkNumber++

	if !ucr.isValidChunkSize(chunkSize) {
		return chunkSize, s3.Lookup(s3.InvalidChunkSizeError)
	}

	ucr.lastChunkNumber = ucr.chunkNumber
	ucr.lastChunkSize = chunkSize
	ucr.seenChunk = true

	return chunkSize, nil
}

func (ucr *UnsignedChunkReader) readChunkSizeLine() (string, error) {
	var line []byte
	for {
		// ReadSlice lets normal headers use bufio's internal buffer. The append
		// path only handles split or oversized headers and is bounded by
		// maxHeaderSize, so malformed headers cannot grow memory unboundedly.
		part, err := ucr.reader.ReadSlice('\r')
		line = append(line, part...)
		if len(line) > maxHeaderSize {
			return "", s3.Lookup(s3.IncompleteBody)
		}
		if err == nil {
			break
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		return "", err
	}

	err := ucr.readAndSkip('\n')
	if err != nil {
		return "", err
	}

	return strings.TrimSpace(string(line)), nil
}

// isValidChunkSize checks if the parsed chunk size is valid
// they follow one rule: all chunk sizes except for the last one
// should be greater than 8192
func (ucr *UnsignedChunkReader) isValidChunkSize(size int64) bool {
	if !ucr.seenChunk {
		// any valid number is valid as a first chunk size
		return true
	}

	// any chunk size, except the last one should be greater than 8192
	if size != 0 && ucr.lastChunkSize < minChunkSize {
		ucr.invalidChunkNumber = ucr.lastChunkNumber
		ucr.invalidChunkSize = ucr.lastChunkSize
		return false
	}

	return true
}

// Reads and validates the trailer at the end
func (ucr *UnsignedChunkReader) readTrailer() error {
	var trailerBuffer bytes.Buffer
	var hasChecksum bool

	for {
		v, err := ucr.reader.ReadByte()
		if err != nil {
			return s3.Lookup(s3.IncompleteBody)
		}
		if v != '\r' {
			hasChecksum = true
			trailerBuffer.WriteByte(v)
			continue
		}

		if !hasChecksum {
			// in case the payload doesn't contain trailer
			// the first 2 bytes(\r\n) have been read
			// only read the last byte: \n
			err := ucr.readAndSkip('\n')
			if err != nil {
				return s3.Lookup(s3.IncompleteBody)
			}

			break
		}

		var tmp [3]byte
		_, err = io.ReadFull(ucr.reader, tmp[:])
		if err != nil {
			return s3.Lookup(s3.IncompleteBody)
		}
		if !bytes.Equal(tmp[:], trailerDelim) {
			return s3.Lookup(s3.IncompleteBody)
		}
		break
	}

	// Parse the trailer
	trailerHeader := trailerBuffer.String()
	trailerHeader = strings.TrimSpace(trailerHeader)
	if trailerHeader == "" {
		if ucr.checksumType != "" {
			return s3.Lookup(s3.MalformedTrailerError)
		}

		return nil
	}
	trailerHeaderParts := strings.Split(trailerHeader, ":")
	if len(trailerHeaderParts) != 2 {
		return s3.Lookup(s3.MalformedTrailerError)
	}

	checksumKey := checksumType(trailerHeaderParts[0])
	checksum := trailerHeaderParts[1]

	if !checksumKey.isValid() {
		return s3.Lookup(s3.MalformedTrailerError)
	}

	if checksumKey != ucr.checksumType {
		return s3.Lookup(s3.MalformedTrailerError)
	}

	ucr.parsedChecksum = checksum

	// Validate checksum
	return ucr.validateChecksum()
}

// Validates the trailing checksum sent at the end
func (ucr *UnsignedChunkReader) validateChecksum() error {
	algo := (strings.ToUpper(strings.TrimPrefix(string(ucr.checksumType), "x-amz-checksum-")))
	// validate the checksum
	if !isValidChecksum(ucr.parsedChecksum, algo) {
		return errInvalidTrailingChecksum(string(ucr.checksumType))
	}

	checksum := ucr.calculateChecksum()

	// compare the calculated and parsed checksums
	if checksum != ucr.parsedChecksum {
		return errBadDigest(algo)
	}

	return nil
}

// calculateChecksum calculates the checksum with the unsigned reader hasher
func (ucr *UnsignedChunkReader) calculateChecksum() string {
	csum := ucr.hasher.Sum(nil)
	return base64.StdEncoding.EncodeToString(csum)
}

// Returns the hash calculator based on the hash type provided
func getHasher(ct checksumType) (hash.Hash, error) {
	switch ct {
	case checksumTypeCrc32:
		return crc32.NewIEEE(), nil
	case checksumTypeCrc32c:
		return crc32.New(crc32.MakeTable(crc32.Castagnoli)), nil
	case checksumTypeCrc64nvme:
		table := crc64.MakeTable(bits.Reverse64(0xad93d23594c93659))
		return crc64.New(table), nil
	case checksumTypeSha1:
		return sha1.New(), nil //nolint:gosec // G401: S3 checksum algorithm
	case checksumTypeSha256:
		return sha256.New(), nil
	default:
		return nil, errors.New("unsupported checksum type")
	}
}
