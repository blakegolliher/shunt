# Runbook: a proxy is silent, or a step is waiting on it

**Signal.** `shunt proxy list` shows a member `SILENT`, or `APPLIED` behind the directory version;
a `ramp`, `migrate start` or `cutover` exits 3 with `waiting on proxy_missing <id>` or
`install_pending <id>`, or `shunt operation show <op>` lists such a blocker for any step;
`purge-source`'s dry run is refused with `directory version N is not on every proxy yet`;
`shunt_fleet_members{state="silent"} > 0`; `shunt_barrier_blockers{code="proxy_missing"} > 0`.

**What it means.** The proxy has not sent a heartbeat within its lease (silent), or has not
installed the latest directory version (behind). A silent proxy has already stopped writing to
moving buckets on its own, but it may still write to a bucket it thinks is ACTIVE, and a request it
admitted may still reach a backend. So every step waits on it: nothing goes ahead without it, and
no timeout releases the step's hold. The operation stays `blocked` until the proxy is back, is
retired, or its incarnation is resolved.

**Do.**
1. `shunt operation show <op>` names the blocker and the proxy. `shunt proxy show <id>` gives its
   lease, its incarnation and what is off.
2. On the proxy's host: `curl http://<admin>/-/fleet` says whether it is stale, which control node
   it talks to, and the version it has. Its log says why a directory poll or heartbeat failed
   (`directory poll failed`, `lease with the control plane lapsed`), or why a version was refused
   (`refused on this proxy`: a cluster it cannot build, usually a secret it could not resolve).
3. If the proxy cannot reach the control nodes, that is a network or token problem between them;
   fix it and the proxy re-joins by itself, catching up before its lease returns. It then
   acknowledges the hold, drains, and the step carries on with no command.
4. If the proxy refused a version, fix what it names (a `file:` secret missing on that host, a
   cluster endpoint it cannot resolve) and it installs the version on its next poll.
5. If the proxy is running but should leave the fleet: `shunt proxy retire <id> --wait 2m`. It
   drains, records its retirement and exits; a cleanly retired proxy counts out of every step. If
   `--wait` ends with `retired with backend outcomes unknown`, go to
   [crashed-proxy.md](crashed-proxy.md).
6. If the process is gone (crashed, killed, host lost), go to [crashed-proxy.md](crashed-proxy.md).
   Forget comes last, once its incarnation is resolved.

**Do not** resolve or forget a proxy that is merely partitioned. Its process may still hold
requests against a backend, and when it comes back it writes a bucket's keys by the routing it
last had. Resolving says the process is stopped; only say so when it is.

**Do not** cancel a step to get past a silent proxy unless you want the step undone. Cancel releases
that operation's hold and puts the routing back as it was; the silent proxy then blocks the next
step in the same way.
