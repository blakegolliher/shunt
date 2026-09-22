# Why there is no objects table

The control plane keeps a record per cluster, per tenant, per bucket, per client key, and per
proxy. It keeps nothing per object, and never will (CLAUDE.md; docs/DESIGN.md §1.5).

**Everything shunt needs about an object is in the request, in the bucket's placement, or in the
backend.** A bucket in `RAMPING` splits its keys by a hash the placement names, so any proxy
decides a key's side from the key alone. A bucket in `MIGRATING` reads the new cluster and falls
back to the old on a miss, so the backend answers where the object is. Multipart uploads carry the
cluster that issued them in the upload id (§2.4). Conditional writes are judged by asking both
clusters (ADR-0013). Copies that no backend can make are streamed (ADR-0014). Cutover waits until
no read has needed the old cluster (ADR-0004), and purge-source compares the two listings before
it deletes anything.

**What an objects table would cost.** Every write would become a write to the control plane
too, which turns a store that changes when an operator types a command into one that changes a
hundred thousand times a second, and puts consensus on the data path (§1.3 says it never is). A
billion objects would be a billion rows to keep consistent with two backends that can change
without shunt (a client going direct, a lifecycle rule, a restore). The table would be wrong
whenever it mattered.

**What the control plane holds instead** is bounded by what operators create: tenants, buckets,
clusters, keys, and the proxies in the fleet. A million placements is a few hundred megabytes of
etcd; a thousand proxies is a thousand leased keys. Nothing in it grows with the data.

The one exception the design allows is a job's own bookkeeping: a mover's cursor and ledger are
per object *while a pass runs*, and live in a file on the mover's host or in object storage, never
in the control plane (ADR-0009; P3d makes movers workers with range claims in etcd, which are per
range, not per object).
