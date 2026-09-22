# Runbook: a proxy is silent, or a step is waiting on it

**Signal.** `shunt proxy list` shows a member `SILENT`, or `APPLIED` behind the directory version;
a `ramp`, `migrate start` or `cutover` answers `PENDING: not yet installed on <id>` or is refused
`waiting on <id>`; `shunt_fleet_members{state="silent"} > 0`.

**What it means.** The proxy has not sent a heartbeat within its lease (silent), or has not
installed the latest directory version (behind). A silent proxy has already stopped writing to
moving buckets on its own; a behind proxy is about to install the version, or cannot.

**Do.**
1. On the proxy's host: `curl http://<admin>/-/fleet` says whether it is stale, which control node
   it talks to, and the version it has. Its log says why a directory poll or heartbeat failed
   (`directory poll failed`, `lease with the control plane lapsed`), or why a version was refused
   (`refused on this proxy`: a cluster it cannot build, usually a secret it could not resolve).
2. If the proxy cannot reach the control nodes, that is a network or token problem between them;
   fix it and the proxy re-joins by itself, catching up before its lease returns.
3. If the proxy refused a version, fix what it names (a `file:` secret missing on that host, a
   cluster endpoint it cannot resolve) and it installs the version on its next poll.
4. If the proxy is gone for good (decommissioned, host lost): `shunt proxy forget <id>`. It is
   refused while the proxy is live. A bucket's first step waits for every member until then; later
   steps already go ahead without a silent member.

**Do not** forget a proxy that is merely partitioned before a bucket's first step and then take
that step: when it comes back it will write that bucket's keys to the old cluster until it has the
new directory, which is exactly what the wait prevents. Forget means gone.
