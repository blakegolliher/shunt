export interface ControlMember {
  name: string
  id: string
  peer_urls: string[]
  leader: boolean
  learner?: boolean
  started: boolean
}

export interface ProxyMember {
  id: string
  live: boolean
  applied: number
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
}

export interface PlacementStatus {
  key: string
  state: string
  primary: string
  source?: string
  target?: string
  names: Record<string, string>
  read_only: boolean
  reject_writes: boolean
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
  scope?: { resource: string; generation: number; clusters?: string[] }
  sequence?: number
  allowed_actions?: string[]
  blockers?: { code: string; proxy_id?: string; message?: string }[]
  phase?: string
  waiting_on?: string[]
  silent?: string[]
  progress?: { done: number; total: number; unit: string }
  version?: number
  result?: unknown
  error?: { code: string; message: string }
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

async function request<T>(path: string, token: string, init?: RequestInit): Promise<T> {
  const headers = new Headers(init?.headers)
  headers.set('Authorization', `Bearer ${token}`)
  headers.set('Accept', 'application/json')
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
export const adoptBucket = (token: string, tenant: string, bucket: string, body: { cluster: string; name?: string; keys?: { access_key: string; secret: string; buckets?: string[] }[] }) => request<PlacementStatus>(`${apiRoot}/placements/${encodeURIComponent(tenant)}/${encodeURIComponent(bucket)}/adopt`, token, json(body))
export const createBackendBucket = (token: string, tenant: string, bucket: string, body: { cluster: string; name: string; keys?: { access_key: string; secret: string; buckets?: string[] }[]; legs?: { cluster: string; name?: string }[] }) => request<PlacementStatus>(`${apiRoot}/placements/${encodeURIComponent(tenant)}/${encodeURIComponent(bucket)}/create-backend`, token, json(body))
export const expandBucket = (token: string, tenant: string, bucket: string, to: string, name?: string, acceptExisting = false) => request<{ key: string; target: string; name: string; version: number }>(`${apiRoot}/placements/${encodeURIComponent(tenant)}/${encodeURIComponent(bucket)}/expand`, token, json({ to, name, create: true, accept_existing_objects: acceptExisting || undefined }))
export const clearTarget = (token: string, tenant: string, bucket: string) => request<{ key: string; target?: string; name?: string; retired?: { id: string; cluster: string; bucket: string }[]; version: number }>(`${apiRoot}/placements/${encodeURIComponent(tenant)}/${encodeURIComponent(bucket)}/target`, token, { method: 'DELETE' })
export const setClusterReadOnly = (token: string, name: string, readOnly: boolean, reject = false) => request(`${apiRoot}/clusters/${encodeURIComponent(name)}/read-only`, token, json({ read_only: readOnly, reject }))
export const setPlacementReadOnly = (token: string, tenant: string, bucket: string, readOnly: boolean, reject = false) => request(`${apiRoot}/placements/${encodeURIComponent(tenant)}/${encodeURIComponent(bucket)}/read-only`, token, json({ read_only: readOnly, reject }))
export const removeClusterDryRun = (token: string, name: string) => request<RemoveDryRun>(`${apiRoot}/clusters/${encodeURIComponent(name)}?dry_run=1`, token, { method: 'DELETE' })
export const removeCluster = (token: string, name: string, confirmation: string) => request(`${apiRoot}/clusters/${encodeURIComponent(name)}`, token, { ...json({ token: confirmation }), method: 'DELETE' })
export const startOperation = (token: string, kind: string, placement: string, args?: unknown) => request<Operation>(`${apiRoot}/operations`, token, json({ kind, placement, args }))
export const getOperation = (token: string, id: string) => request<Operation>(`${apiRoot}/operations/${encodeURIComponent(id)}`, token)
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
