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
