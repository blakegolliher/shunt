import { useEffect, useState } from 'react'
import { getAudit } from '../api/client'
import type { AuditChange } from '../api/client'
import { Card } from '../components/Card'
import { useStore } from '../store'

export function Audit() {
  const { token, lastEvent } = useStore()
  const [changes, setChanges] = useState<AuditChange[]>([])
  const [error, setError] = useState('')
  const directoryEvent = lastEvent?.type === 'directory' ? lastEvent.id : ''

  useEffect(() => {
    let live = true
    const timer = window.setTimeout(() => {
      void getAudit(token).then((result) => { if (live) { setChanges(result.changes); setError('') } })
        .catch((cause) => { if (live) setError(cause instanceof Error ? cause.message : String(cause)) })
    }, 0)
    return () => { live = false; window.clearTimeout(timer) }
  }, [directoryEvent, token])

  return <Card eyebrow="Directory changes" title="Audit tail">
    {error ? <p role="alert" className="rounded-lg border border-red-600 bg-red-950/40 p-3 text-sm text-red-100">{error}</p>
      : changes.length === 0 ? <p className="text-sm text-muted">No directory changes have been recorded yet.</p>
        : <div className="overflow-x-auto"><table className="w-full text-left text-sm"><thead className="text-xs uppercase tracking-wider text-muted"><tr><th className="pb-3">Version</th><th className="pb-3">Time</th><th className="pb-3">Actor</th><th className="pb-3">Action</th><th className="pb-3">Record</th></tr></thead><tbody className="divide-y divide-ink-700">{changes.map((change) => <tr key={`${change.version}-${change.op}-${change.key ?? ''}`}><td className="py-3 font-mono">{change.version}</td><td className="py-3 text-muted">{new Date(change.ts).toLocaleString()}</td><td className="py-3 font-mono text-xs text-muted">{change.actor}</td><td className="py-3 text-ember-300">{change.op}</td><td className="py-3 font-mono">{change.key ?? '—'}</td></tr>)}</tbody></table></div>}
  </Card>
}
