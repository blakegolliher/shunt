# Runbook: a proxy crashed, or never learned a backend outcome

**Signal.** `shunt proxy list` shows a member `UNRESOLVED(n)`, or `SILENT` with its host down; an
operation blocks on `incarnation_unresolved <id>` or `backend_outcome_unknown <id>` (`shunt
operation show <op>`); `shunt proxy forget <id>` answers `retirement_unproven`;
`shunt_fleet_unresolved_incarnations > 0`. At eight unresolved incarnations a new process of the
proxy cannot register (its heartbeats answer `retirement_unproven`) and stays stale.

**What it means.** Each process of a proxy is an incarnation. One that ended without a clean
retirement (a crash, `kill -9`, a lost host, a SIGTERM drain cut short by `proxy.drain_timeout`, or
a retirement that reported outcomes unknown) may have sent a mutation that can still land on a
backend. shunt cannot tell when that work has ended; lease expiry and silence prove nothing. So
every step waits on the incarnation (`incarnation_unresolved`), and forget refuses, until an
operator records how it was established that the work has ended. A running proxy that never
learned how a mutation ended blocks the same way on `backend_outcome_unknown`.

**What it costs.** A proxy counts backend outcomes it never learned for the life of its process.
One idle-progress timeout on a PUT (the backend had the whole request and did not answer within
`proxy.idle_timeout`) makes every later step on that bucket wait until the proxy is retired and its
incarnation resolved with an attestation. The same holds for a metadata deadline
(`proxy.metadata_timeout`) or a broken connection after the backend had the whole request. A client
that disconnects or times out does not cause this: once the backend has the whole request, the
proxy waits for its answer, bounded by its own deadlines. A client that leaves mid-body stops the
request, and a short body is never committed.

**Do.**
1. `shunt proxy show <id>` names the incarnations: `incarnation <inc>: active since …; N backend
   outcomes unknown` for the current process, `unresolved incarnation <inc>: unclean, ended …, N
   outcomes unknown` for earlier ones.
2. If the process is running and reports outcomes unknown, retire it: `shunt proxy retire <id>
   --wait 2m`. It drains, records an unclean retirement and exits (`retired with backend outcomes
   unknown`); its incarnation is now unresolved.
3. Establish that the process is stopped and the backend holds no request of it: the process is not
   running, or its host is off or cut off from every backend, and the backend's own request timeout
   has passed since, or the backend shows no connection from that host. A proxy that is only
   partitioned from the control plane is not stopped: restore it instead
   ([lagging-proxy.md](lagging-proxy.md)). If clients need to know what became of the request, the
   proxy's access log names it; check that key on the backend.
4. Resolve the incarnation with what you established:

   ```sh
   shunt proxy resolve proxy-c --incarnation 5f0c… --attest "host powered off 14:02; backend request timeout 60s passed"
   ```

   It answers `proxy proxy-c: incarnation 5f0c… resolved; 0 unresolved left`. The attestation, your
   actor and the time stay on the incarnation. A live incarnation is refused: a running process is
   retired, not resolved. Resolve each unresolved incarnation the same way.
5. Start the proxy again (its new process joins and acknowledges), or, if it is gone for good,
   `shunt proxy forget proxy-c`. Until one of the two, steps block on `proxy_missing` (`has no
   running process`).
6. The blocked operation carries on by itself (`shunt operation wait <op>`).

**Do not** resolve to get a step moving. There is no force flag: the attestation is the only way
past an unknown outcome, and a wrong one lets a late write land after the step it should have
preceded, which is the lost write the step exists to prevent.

**To make it rarer.** Set `proxy.drain_timeout` longer than the longest upload, so a SIGTERM retires
cleanly, and `proxy.idle_timeout` longer than the slowest backend answer to a whole PUT.
