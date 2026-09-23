# Browser demo: move `ui-demo` from Garage to MinIO

This is the manual acceptance script for the web UI. It starts the same shape as the fleet test:
three control nodes, two proxies, Garage, MinIO, and a checked client workload. The browser performs
the eight demo actions; the terminal is only used to start, observe, and stop the fixture.

## Start

From the repository root, with Go 1.27.1 on `PATH`:

```sh
export PATH=/usr/local/go/bin:$PATH
make demo-ui
```

The command brings up the backend containers, builds the web assets before the binaries, seeds 40
objects in Garage's `ui-demo` bucket, starts three control nodes and two proxies, and prints:

- the UI URL and bearer token;
- the Garage and MinIO endpoints, regions, access keys, and secrets to paste into the UI;
- the proxy endpoint and log directory.

It returns with the fixture running. If the host is remote, forward the control port from your
workstation and browse the local end:

```sh
ssh -L 9951:127.0.0.1:9951 USER@HOST
```

Open `http://127.0.0.1:9951/` and paste the printed token. The token and credentials are also in
`test/e2e/data/demo-ui/demo.env`, mode 0600.

## Walk the UI

1. **Control plane.** Notice three healthy members, a two-member majority, two live proxies, and
   matching applied directory revisions. Open **Add node** to see that the UI gives an exact join
   command rather than pretending it can log in to another host.
2. **Clusters → Add cluster.** Add `garage` with scheme `http`, endpoint `127.0.0.1:3900`, region
   `garage`, and the printed Garage key and secret. Run the probe, inspect the assumed capability
   warning, then save. Add `minio` the same way with endpoint `127.0.0.1:9000`, region
   `us-east-1`, and its printed credentials.
3. **Buckets → Adopt or create → Adopt existing.** Tenant is `default`, client and backend bucket
   are both `ui-demo`, cluster is `garage`. Under **Optional client key import**, paste the same
   Garage access key and secret. This is the credential the verifier and a real brownfield client
   already use. Saving the placement starts the continuous verifier automatically.
4. Select `ui-demo`, choose **Expand**, select `minio`, and click **Create target and continue to
   Migrations**. Notice the generated `ui-demo-001` name and the now-measured target capability
   profile.
5. **Migrations → Ramp traffic.** Choose **50%**, apply, and watch the fence move from its held phase
   to applied on both proxies. Keep the workload running long enough for a completed 10-second
   window; the source/target write bar should contain both colors. Apply **100%**, then **Enter
   MIGRATING**.
6. **Mover → Start mover.** Watch copied/already-there/vanished/failed counts, the all-keys range,
   ledger tail, and repeated passes. Do not cut over until it says `Converged: pass N copied 0` and
   fallback reads trend to zero.
7. **Cutover.** Leave the five-second demo window selected. Note the source multipart count before
   starting. The fence stays visible while the quiet window and fleet settle complete; the state
   becomes `CUTOVER` only after both checks pass.
8. **Purge or forget.** Click **Dry-run purge** and read the bucket name, object/byte count, uploads,
   and confirmation-token expiry before confirming. After the placement is `ACTIVE` on MinIO,
   click **Make minio tenant default**, then **Remove garage** and confirm its dry run.

At each state change, **Audit** should show the token-fingerprint actor. **Telemetry** should show
fleet, both cluster, and both proxy scopes. Select the two clusters in the comparison panel and
notice the 50/50 hold in their p99/request history; the four latency panels are emitted p50/p90/p99/
p99.9 values, not percentiles calculated by the browser.

## Verify and evidence

The verifier runs in consecutive one-minute checked workloads from adoption until shutdown:

```sh
tail -f test/e2e/data/demo-ui/verify.log
jq '{ops,errors,latency_p50_us,latency_p99_us}' test/e2e/data/demo-ui/verify.json
```

Each completed report must have `errors: 0`. During the manual pass, capture the Telemetry screen
after the split has produced completed windows and save it as `docs/telemetry-dashboard.png`; that
is the intentionally manual UI-5 visual artifact.

The refusal paths are safe to demonstrate too: try shrinking a ramp, opening purge before cutover,
starting the mover before ratio 100%, or removing Garage before changing the tenant default. The UI
must show the API's refusal text verbatim.

## Stop

```sh
make demo-ui-down
```

This stops only the PIDs recorded by `make demo-ui`; logs and the last verifier JSON remain in
`test/e2e/data/demo-ui`. `make e2e-down` additionally removes the backend containers and test data.
