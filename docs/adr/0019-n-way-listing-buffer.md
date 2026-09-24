# ADR-0019: the N-way listing of a spread bucket holds one bounded page per leg

Status: accepted (2026-09-23, ADR-0018 N2). Extends ADR-0007 (bounded buffers in migration paths)
from two sides to up to `directory.MaxLegs` legs.

## Context

A bucket spread over legs (ADR-0018) has its keys on up to 32 backend buckets, each owning a range of
the key hash. A listing has to merge them in order. The two-sided migration merge
(`internal/proxy/merge.go`, ADR-0007) holds one backend page per side, 1 000 entries each. Held the
same way over 32 legs that is 32 000 entries and 32 backend reads of 1 000 keys for every client
page of 1 000, of which 31 000 are read and thrown away. CLAUDE.md requires an ADR and a benchmark
for any body buffering.

## Decision

- **One page per leg, sized to the client's page.** Keys interleave across legs by hash, so each
  leg holds about `max-keys / n` of any page. A leg reads `max-keys/n + max-keys/(4n) + 16` entries
  at a time, never more than `max-keys`; a leg that runs short before the page is full reads its
  next page. The merge holds at most one page per leg: about `1.25 × max-keys + 16n` entries in all,
  1 762 for a 1 000-key page over 32 legs.
- **The first page of every leg is read at once.** The listing waits for the slowest leg, not for
  the sum of the legs. Later pages, needed only for a leg that ran short, are read one at a time.
- **Each response body is read through the same 8 MiB limit as the two-sided merge**, parsed, and
  released before the page is merged. With the first reads in parallel, up to `n` bodies are in
  flight at once.
- **Every leg keeps only the keys it owns.** A leftover of a finished move on the wrong leg is never
  listed. A common prefix several legs report is one entry.
- **The token does not grow with the legs:** `{last, prefix, done}` (base64 JSON). Every leg resumes
  with `start-after=last`, and after a common prefix with U+10FFFF appended, because Garage lists a
  common prefix again after `start-after=<prefix>` and MinIO does not. VAST does the reverse: it
  skips the prefix after `start-after=<prefix>` and lists it again after the suffix
  (docs/reference/backend-compat.md). No one value is exact on all three, so the merge drops anything
  at or before `last` whatever a backend answers; the backends only ever repeat, never skip. Without
  that rule a delimited listing on VAST pages forever. ListObjects v1 resumes from `marker` the same
  way.
- **A leg whose bucket is missing fails the listing** (a 5xx), because an answer without it would
  be silently incomplete.

## Consequences

- **Benchmark** (`BenchmarkSpreadListingPage`, a 1 000-key page, keys written through shunt so each
  leg holds only its own, fake backends on one host, 2026-09-23):

  | legs | ns/op | B/op | allocs/op |
  |---|---|---|---|
  | 1 (a plain bucket) | 11 125 689 | 8 307 767 | 11 710 |
  | 2 | 25 607 703 | 12 173 340 | 76 306 |
  | 8 | 18 412 156 | 12 480 196 | 78 751 |
  | 32 | 22 054 964 | 13 370 493 | 87 993 |

  A spread listing costs about twice a plain one and stays flat from 2 to 32 legs, in time and in
  memory. The first version, which read every leg's full page in turn, measured 61 ms at 2 legs and
  0.94 s and 337 MB per page at 32, on data that padded every leg with the same keys.
- **Backend reads per client page** are about `1.25 × max-keys` in all, against `max-keys` for a plain
  bucket, plus one request per leg.
- **Uneven data costs more reads, never more memory.** A prefix owned mostly by one leg makes that
  leg read several short pages in turn; each page is still bounded by the formula above.
- **ListMultipartUploads is not merged yet**: it answers `NotImplemented` on a spread bucket, as does
  any other bucket-level request that would have to reach every leg (ADR-0018 N2).
