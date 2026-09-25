export interface ControlMember {
  name: string
  id: string
  peer_urls: string[]
  leader: boolean
  learner?: boolean
  started: boolean
}

// Incarnation is one process of a proxy (ADR-0021 D2): active, retired (stopped cleanly), unclean
// (ended without retiring, or with backend outcomes unknown) or resolved by an operator.
export interface Incarnation {
  id: string
  state: 'active' | 'retired' | 'unclean' | 'resolved'
  started?: string
  ended?: string
  uncertain?: number
  attestation?: string
  resolved_by?: string
  resolved_at?: string
}

export interface ProxyMember {
  id: string
  live: boolean
  applied: number
  // incarnation is the proxy's current process; unresolved are earlier ones that did not retire
  // cleanly, on which every barrier waits until an operator resolves them. retire_requested: an
  // operator asked the proxy to retire. uncertain: backend outcomes this process never learned.
  incarnation?: Incarnation
  unresolved?: Incarnation[]
  retire_requested?: boolean
  uncertain?: number
  // durable is the version the proxy's restart cache holds durably; it lags applied while a cache
  // write is in progress or failing.
  durable?: number
  // installed is set when the proxy has installed a newer version than its requests use (install
  // backpressure); cache_error says why its restart cache is not durable.
  installed?: number
  cache_error?: string
  seq: number
  started?: string
  seen?: string
  since_seen?: number
  host?: string
  version?: string
}

export interface ControlStatus {
  node: string
  version: string
  cluster: {
    members: ControlMember[]
    quorum: number
    started: number
    has_quorum: boolean
    revision: number
    db_bytes: number
    db_in_use_bytes: number
    quota_bytes: number
    leader: string
  }
  fleet: ProxyMember[]
  fleet_error?: string
  directory: number
  directory_loaded: boolean
  last_compaction: string | null
  join: string
}

export interface FleetStatus {
  version: number
  members: ProxyMember[]
}

export interface ClusterStatus {
  name: string
  type: string
  scheme: string
  region: string
  endpoints: string[]
  access_key: string
  secret_ref: string
  conditional_write: boolean
  conditional_delete: boolean
  conditional_write_known?: boolean
  conditional_delete_known?: boolean
  capability_profile?: 'measured' | 'assumed'
  references?: string[]
  read_only: boolean
  reject_writes: boolean
  // barrier is the drain barrier of a change in progress (ADR-0021 D2): read_only is desired, and
  // effective only once the barrier is gone.
  barrier?: Barrier
}

// Barrier is the drain barrier of a change in progress on a bucket or a cluster (ADR-0021 D2).
export interface Barrier { id: string; kind: 'mutations' | 'source' }

export interface PlacementStatus {
  key: string
  state: string
  primary: string
  source?: string
  target?: string
  // names maps each cluster to its backend bucket; null for a bucket spread over legs, whose
  // buckets are in legs instead (ADR-0018).
  names: Record<string, string> | null
  read_only: boolean
  reject_writes: boolean
  barrier?: Barrier
  // watch: an operator asked for this bucket's traffic by backend; per_bucket_telemetry: proxies
  // count it by backend now (spread, moving or watched).
  watch?: boolean
  per_bucket_telemetry?: boolean
  client_keys?: number // keys that can reach the bucket; absent when this shunt holds no keys
  legs?: { id: string; cluster: string; bucket: string; share: number; ranges: { from: string; to: string }[]; idle?: boolean }[] // a bucket spread over legs; primary and names are empty then. idle: owns no key in any scope
  move?: { from: string; to: string; range: { from: string; to: string }; share: number } // part of a spread bucket moving; primary and source are its clusters then
  ratio?: number
  prefixes?: string[]
  ramp_writes?: Record<string, number>
  fallback_reads?: number
  dual_deletes?: Record<string, number>
  cutover?: { at: string; window: number; fallback_reads: number }
  mover?: MoverProgress
  migration_window?: {
    start: string
    end: string
    writes: Record<string, number>
    reads: Record<string, number>
  }
}

export interface DirectoryStatus {
  version: number
  clusters: ClusterStatus[]
  placements: PlacementStatus[]
}

export interface Capability { value: boolean; known: boolean }

export interface ClusterView extends ClusterStatus {
  capabilities: { conditional_write: Capability; conditional_delete: Capability }
  probe: { reachable: boolean; latency_ms: number; error?: string; checked_at: string }
  // secret: where a rotation of the cluster's secret stands, when the control plane holds it.
  // Installed is not drained: a request begun before the install may still sign with the old one.
  secret?: { generation: string; installed: string[]; pending: string[]; silent: string[] }
}

export interface ClusterProbeResult {
  cluster: ClusterStatus
  reachable: boolean
  profile: 'measured' | 'assumed'
  capabilities: { conditional_write: Capability; conditional_delete: Capability }
}

export interface FenceStatus { version: number; held: boolean; proxies: number; waiting_on: string[]; silent: string[] }
export interface PlacementView extends PlacementStatus {
  fence: FenceStatus
  operations: string[]
  clusters: Record<string, ClusterStatus>
  source_uploads_in_flight: number | null
  source_uploads_error?: string
}

export interface MoverRange {
  name: string
  cursor?: string
  done: number
  total?: number
  complete: boolean
}

export interface MoverProgress {
  source: string
  primary: string
  pass: number
  copied: number
  skipped: number
  vanished: number
  failed: number
  bytes: number
  last_key?: string
  done: boolean
  converged: boolean
  updated_at: string
  ranges?: MoverRange[]
}

export interface Operation {
  id: string
  kind: string
  placement?: string
  cluster?: string
  actor: string
  // pending, running and blocked are unfinished; a refusal after the record exists ends failed
  // with error.code 'refused' (ADR-0021).
  status: 'pending' | 'running' | 'blocked' | 'succeeded' | 'failed' | 'cancelled'
  effect_state?: 'none' | 'committed' | 'uncertain'
  identity?: { cluster_id: string; epoch: string }
  scope?: { resource: string; generation: number; clusters?: string[] }
  node?: string
  request_id?: string
  created?: string
  updated?: string
  sequence?: number
  // allowed_actions are what the server accepts now: cancel after the hold is durable and before
  // its commit, resume once its owner is gone. blockers name what a blocked operation waits on;
  // barrier is the drain barrier it runs (ADR-0021 D2).
  allowed_actions?: string[]
  blockers?: Blocker[]
  blocker_count?: number
  barrier?: { id: string; scope: string; kind: string; hold_version?: number; generation?: number; committed?: boolean; commit_version?: number }
  owner_term?: number
  phase?: string
  waiting_on?: string[]
  silent?: string[]
  progress?: { done: number; total: number; unit: string }
  version?: number
  result?: unknown
  error?: { code: string; message: string }
}

export interface Blocker { code: string; proxy_id?: string; incarnation?: string; count?: number; message?: string }

// blockerText is a blocker list in one line.
export function blockerText(blockers: Blocker[] | undefined): string {
  return (blockers ?? []).map((b) => `${b.code}${b.proxy_id ? ` ${b.proxy_id}` : ''}${b.count ? ` (${b.count})` : ''}`).join(', ')
}

// unfinished reports whether an operation has not ended yet: poll it, and keep its scope's actions
// disabled.
export function unfinished(op: Pick<Operation, 'status'> | null | undefined): boolean {
  return op?.status === 'pending' || op?.status === 'running' || op?.status === 'blocked'
}

export interface RemoveDryRun { allowed: boolean; reason?: string; name: string; references: string[]; secret_files: number; token?: string; expires_at?: string }
export interface PurgeDryRun {
  allowed: boolean
  reason?: string
  key: string
  source?: string
  bucket?: string
  objects: number
  bytes: number
  uploads_in_flight: number
  missing: string[]
  version: number
  token?: string
  expires_at?: string
}

export interface LedgerEntry {
  at: string
  key: string
  size: number
  src_etag: string
  dst_etag?: string
  parts?: number
  mode: string
  result: string
  detail?: string
  took?: string
}

export interface AuditChange {
  ts: string
  actor: string
  op: string
  key?: string
  version: number
}

export interface TelemetryPoint {
  start: string
  end: string
  series: string
  op: string
  p50_us?: number
  p90_us?: number
  p99_us?: number
  p999_us?: number
  max_us?: number
  count?: number
  value?: number
  // cluster is the backend cluster of a point in a bucket scope, which answers one per cluster.
  cluster?: string
  // code is the status key of a status_per_second point: a tracked code such as "503", "4xx" or
  // "5xx" for the others, "0" for no response, "not_found" for a read answered 404.
  code?: string
}

export interface StreamEvent {
  id: string
  type: string
  data: unknown
}

export class APIError extends Error {
  constructor(public readonly status: number, message: string) {
    super(message)
  }
}

// newRequestKey is a new Idempotency-Key: 128 random bits in hex. crypto.getRandomValues, not
// crypto.randomUUID, because the UI is often reached over plain http from another host, which is
// not a secure context, and randomUUID exists only in one.
export function newRequestKey(): string {
  const bytes = crypto.getRandomValues(new Uint8Array(16))
  return Array.from(bytes, (b) => b.toString(16).padStart(2, '0')).join('')
}

async function request<T>(path: string, token: string, init?: RequestInit): Promise<T> {
  const headers = new Headers(init?.headers)
  headers.set('Authorization', `Bearer ${token}`)
  headers.set('Accept', 'application/json')
  // Every change is a new request with its own key (ADR-0021); the UI never retries one itself.
  if ((init?.method ?? 'GET') !== 'GET' && !headers.has('Idempotency-Key')) headers.set('Idempotency-Key', newRequestKey())
  const response = await fetch(path, { ...init, headers })
  if (!response.ok) {
    let message = `${response.status} ${response.statusText}`
    try {
      const body = (await response.json()) as { message?: string }
      if (body.message) message = body.message
    } catch {
      // A proxy or interrupted node may answer plain text. The status is still actionable.
    }
    throw new APIError(response.status, message)
  }
  return (await response.json()) as T
}

export const getControl = (token: string) => request<ControlStatus>('/v1/control', token)
export const getFleet = (token: string) => request<FleetStatus>('/v1/fleet', token)
// ProxyDiagnostics is GET /v1/fleet/{id}: one proxy's install state and, in words, what is off.
export interface ProxyDiagnostics extends ProxyMember {
  directory: number
  lineage: boolean
  behind: number
  backpressure: boolean
  secrets: { cluster: string; want: string; have: string; current: boolean }[]
  // lease is the member's lease as the control plane grants it: granted is the TTL in nanoseconds,
  // seq the last heartbeat recorded, age how long ago it arrived (nanoseconds, the control node's
  // clock). The member measures its own staleness from its send time.
  lease: { granted: number; seq: number; seen?: string; age?: number; live: boolean }
  problems: string[]
}
export const getProxyDiagnostics = (token: string, id: string) => request<ProxyDiagnostics>(`/v1/fleet/${encodeURIComponent(id)}`, token)
export interface MemberResult { member: ProxyMember; retire_requested?: boolean }
// retireProxy asks a proxy to retire (drain, record its retirement, stop); resolveProxy records an
// operator's attestation that an unretired incarnation's backend work has ended; forgetProxy removes
// a member that is gone, which the server refuses while an incarnation did not retire.
export const retireProxy = (token: string, id: string) => request<MemberResult>(`/v1/fleet/${encodeURIComponent(id)}/retire`, token, json({}))
export const resolveProxy = (token: string, id: string, incarnation: string, attestation: string) => request<MemberResult>(`/v1/fleet/${encodeURIComponent(id)}/resolve`, token, json({ incarnation, attestation }))
export const forgetProxy = (token: string, id: string) => request<{ forgotten: string }>(`/v1/fleet/${encodeURIComponent(id)}`, token, { method: 'DELETE' })
export const getStatus = (token: string) => request<DirectoryStatus>('/v1/status?all=1', token)
export const getClusterView = (token: string, name: string) => request<ClusterView>(`/v1/clusters/${encodeURIComponent(name)}/view`, token)
export const getPlacementView = (token: string, tenant: string, bucket: string) => request<PlacementView>(`/v1/placements/${encodeURIComponent(tenant)}/${encodeURIComponent(bucket)}/view`, token)
export const getTelemetrySeries = (token: string, query: URLSearchParams) => request<{ points: TelemetryPoint[] }>(`/v1/telemetry/series?${query}`, token)

const json = (body: unknown): RequestInit => ({ method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) })
const apiRoot = '/v1'

export interface ClusterInput {
  name: string
  cluster: { type?: string; scheme: string; region?: string; endpoints: string[]; credentials: { access_key: string; secret_ref?: string } }
  secret?: string
}

export const probeCluster = (token: string, input: ClusterInput) => request<ClusterProbeResult>(`${apiRoot}/clusters/probe`, token, json(input))
export const addCluster = (token: string, input: ClusterInput) => request<ClusterStatus>(`${apiRoot}/clusters`, token, json(input))
// CredentialsResult is a rotation's answer: generation is the secret generation it started, which
// the cluster view's secret reports proxies installing; empty for an env: or file: secret_ref.
export interface CredentialsResult { name: string; version: number; generation?: string; cluster: ClusterStatus }
export const rotateCredentials = (token: string, name: string, body: { access_key?: string; secret?: string; secret_ref?: string }) => request<CredentialsResult>(`${apiRoot}/clusters/${encodeURIComponent(name)}/credentials`, token, json(body))
export const adoptBucket = (token: string, tenant: string, bucket: string, body: { cluster: string; name?: string; keys?: { access_key: string; secret: string; buckets?: string[] }[] }) => request<PlacementStatus>(`${apiRoot}/placements/${encodeURIComponent(tenant)}/${encodeURIComponent(bucket)}/adopt`, token, json(body))
export const createBackendBucket = (token: string, tenant: string, bucket: string, body: { cluster: string; name: string; keys?: { access_key: string; secret: string; buckets?: string[] }[]; legs?: { cluster: string; name?: string }[] }) => request<PlacementStatus>(`${apiRoot}/placements/${encodeURIComponent(tenant)}/${encodeURIComponent(bucket)}/create-backend`, token, json(body))
export const expandBucket = (token: string, tenant: string, bucket: string, to: string, name?: string, acceptExisting = false) => request<{ key: string; target: string; name: string; version: number }>(`${apiRoot}/placements/${encodeURIComponent(tenant)}/${encodeURIComponent(bucket)}/expand`, token, json({ to, name, create: true, accept_existing_objects: acceptExisting || undefined }))
export const clearTarget = (token: string, tenant: string, bucket: string) => request<{ key: string; target?: string; name?: string; retired?: { id: string; cluster: string; bucket: string }[]; version: number }>(`${apiRoot}/placements/${encodeURIComponent(tenant)}/${encodeURIComponent(bucket)}/target`, token, { method: 'DELETE' })
// A read-only change runs as an operation (ADR-0021 D2): switching on is desired at once and
// effective only when every proxy has drained; the operation record says which.
export const setClusterReadOnly = (token: string, name: string, readOnly: boolean, reject = false) => request<Operation>(`${apiRoot}/operations`, token, json({ kind: 'cluster-read-only', cluster: name, args: { read_only: readOnly, reject } }))
export const setBucketWatch = (token: string, tenant: string, bucket: string, watch: boolean) => request(`${apiRoot}/placements/${encodeURIComponent(tenant)}/${encodeURIComponent(bucket)}/watch`, token, json({ watch }))
export const setPlacementReadOnly = (token: string, tenant: string, bucket: string, readOnly: boolean, reject = false) => request<Operation>(`${apiRoot}/operations`, token, json({ kind: 'placement-read-only', placement: `${tenant}/${bucket}`, args: { read_only: readOnly, reject } }))
export const resumeOperation = (token: string, id: string) => request<Operation>(`${apiRoot}/operations/${encodeURIComponent(id)}/resume`, token, json({}))
export const cancelOperation = (token: string, id: string) => request<Operation>(`${apiRoot}/operations/${encodeURIComponent(id)}/cancel`, token, json({}))
export const removeClusterDryRun = (token: string, name: string) => request<RemoveDryRun>(`${apiRoot}/clusters/${encodeURIComponent(name)}?dry_run=1`, token, { method: 'DELETE' })
export const removeCluster = (token: string, name: string, confirmation: string) => request(`${apiRoot}/clusters/${encodeURIComponent(name)}`, token, { ...json({ token: confirmation }), method: 'DELETE' })
export const startOperation = (token: string, kind: string, placement: string, args?: unknown) => request<Operation>(`${apiRoot}/operations`, token, json({ kind, placement, args }))
export const getOperation = (token: string, id: string) => request<Operation>(`${apiRoot}/operations/${encodeURIComponent(id)}`, token)
export const listOperations = (token: string, limit = 50) => request<{ operations: Operation[] }>(`${apiRoot}/operations?limit=${limit}`, token)
export const purgeSourceDryRun = (token: string, tenant: string, bucket: string) => request<PurgeDryRun>(`${apiRoot}/placements/${encodeURIComponent(tenant)}/${encodeURIComponent(bucket)}/purge-source`, token, json({ dry_run: true, wait: '30s' }))
export const getMoverLedger = (token: string, tenant: string, bucket: string) => request<{ entries: LedgerEntry[] }>(`${apiRoot}/placements/${encodeURIComponent(tenant)}/${encodeURIComponent(bucket)}/mover-ledger?limit=20`, token)
export interface ClientKeyResult { access_key: string; tenant: string; checked?: string }
export const importClientKey = (token: string, tenant: string, body: { access_key: string; secret: string; cluster?: string; buckets?: string[] }) => request<ClientKeyResult>(`${apiRoot}/tenants/${encodeURIComponent(tenant)}/client-keys`, token, json(body))
export const setTenantDefault = (token: string, tenant: string, cluster: string) => request(`${apiRoot}/tenants/${encodeURIComponent(tenant)}/default-cluster`, token, json({ cluster }))
export const getAudit = (token: string, limit = 100) => request<{ changes: AuditChange[] }>(`${apiRoot}/audit?limit=${limit}`, token)

export function parseSSEFrame(frame: string): StreamEvent | null {
  let id = ''
  let type = 'message'
  const data: string[] = []
  for (const line of frame.replaceAll('\r', '').split('\n')) {
    if (line.startsWith('id:')) id = line.slice(3).trimStart()
    else if (line.startsWith('event:')) type = line.slice(6).trimStart()
    else if (line.startsWith('data:')) data.push(line.slice(5).trimStart())
  }
  if (data.length === 0) return null
  const raw = data.join('\n')
  let parsed: unknown = raw
  try {
    parsed = JSON.parse(raw)
  } catch {
    // SSE permits text data; retain it for forward compatibility.
  }
  return { id, type, data: parsed }
}

export async function streamEvents(
  token: string,
  lastEventID: string,
  signal: AbortSignal,
  onEvent: (event: StreamEvent) => void,
): Promise<void> {
  const headers = new Headers({ Accept: 'text/event-stream', Authorization: `Bearer ${token}` })
  if (lastEventID) headers.set('Last-Event-ID', lastEventID)
  const response = await fetch('/v1/events', { headers, signal })
  if (!response.ok) throw new APIError(response.status, `${response.status} ${response.statusText}`)
  if (!response.body) throw new Error('the event stream has no body')
  const reader = response.body.pipeThrough(new TextDecoderStream()).getReader()
  let buffered = ''
  for (;;) {
    const { value, done } = await reader.read()
    if (done) break
    buffered += value.replaceAll('\r\n', '\n')
    let boundary = buffered.indexOf('\n\n')
    while (boundary >= 0) {
      const event = parseSSEFrame(buffered.slice(0, boundary))
      buffered = buffered.slice(boundary + 2)
      if (event) onEvent(event)
      boundary = buffered.indexOf('\n\n')
    }
  }
}
