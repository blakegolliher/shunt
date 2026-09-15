# ADR-0003: A body cut short is never delivered as complete

Status: accepted (POC-1). Source: docs/DESIGN.md §2.1, §2.8; P1 acceptance "killing the backend mid-GET results in the client seeing an error, never a silent short read".

## Context

shunt streams bodies with `io.CopyBuffer`; nothing is buffered, so an upstream connection that dies mid-body is discovered while the client is already receiving that body. net/http's server decides how the response ends when the handler returns, and its default for a chunked response is to write a clean terminating chunk, which would make a truncated body look complete.

## Decision

`internal/proxy` handles a failed body copy in one code path, `panic(http.ErrAbortHandler)`, after recording metrics, the access-log line, and the slow-ring entry. Verified against the Go 1.27.1 source of `net/http/server.go`:

- **Upstream response with `Content-Length: N`, connection dies after k < N bytes.** The abort makes `conn.serve`'s deferred recover close the client TCP connection without `finishRequest`. The client has a header promising N bytes and receives k, so every HTTP client reports an error (`io.ErrUnexpectedEOF` in Go, "transfer closed with N-k bytes remaining" in curl). Returning normally would also close the connection here (`shouldReuseConnection` refuses to reuse a connection whose body was short), but the abort keeps one path.
- **Upstream response without `Content-Length` (chunked to the client).** Returning normally would run `chunkWriter.close`, which writes `0\r\n\r\n`, and the client would see a complete body. The abort skips that: the connection is closed without a terminating chunk, and the client sees a truncated chunked stream, which is an error in every client.
- **Client stops reading (write error) after the body was complete.** No abort; the handler returns normally. There is nothing to hide.
- **Streaming PUT, upstream dies before responding.** `RoundTrip` returns an error and no client bytes have been written, so shunt answers with an S3 error body: `504 RequestTimeout` when the cause is a deadline or shunt's own idle watchdog, `502 InternalError` otherwise. net/http closes the client connection afterwards if the request body was partially consumed.

The idle-progress watchdog (data ops) and the metadata deadline (docs/DESIGN.md §2.8) produce the same abort when they fire mid-body.

## Consequences

- A client can never cache or checksum a truncated object as if it were whole. That is the whole point.
- `http.ErrAbortHandler` is the one sanctioned way to abort in net/http (it suppresses the panic log); hijacking the connection would bypass net/http's connection accounting and was rejected.
- Bodies smaller than net/http's 4 KiB write buffer that fail mid-copy may reach the client as a connection close before any header; that is still an error, not a silent short read.
- Tests: `internal/proxy` `TestBackendDiesMidGetContentLength`, `TestBackendDiesMidGetChunked` (asserts no `0\r\n\r\n` on the wire), `TestIdleTimeoutStalledBackend`, `TestUpstreamDown`.
