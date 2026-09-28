import { useEffect, useRef, useState } from 'react'
import { cancelOperation, cancelOutcome, forgetProxy, getOperation, getProxyDiagnostics, joinCommand, memberHealth, newRequestKey, removeMember, removeMemberDryRun, requestJoin, resolveProxy, resumeOperation, retireProxy, unfinished } from '../api/client'
import type { MemberRemoveDryRun, Operation, ProxyDiagnostics, ProxyMember } from '../api/client'
import { Card } from '../components/Card'
import { ConfirmDrawer } from '../components/ConfirmDrawer'
import { CopyLine } from '../components/CopyLine'
import { Drawer } from '../components/Drawer'
import { Stat } from '../components/Stat'
import { useStore } from '../store'

// ago says an observation's age in whole seconds.
function ago(ms?: number): string {
  return `${Math.round((ms ?? 0) / 1000)}s`
}

function bytes(value: number) {
  if (!value) return '0 B'
  if (value < 1048576) return `${(value / 1024).toFixed(1)} KiB`
  return `${(value / 1048576).toFixed(1)} MiB`
}

// memberState is one word for where a proxy stands (ADR-0021 D2): a process that did not retire
// cleanly blocks every barrier until an operator resolves it, which comes before live or silent.
function memberState(m: ProxyMember): { text: string; tone: string } {
  if (m.unresolved?.length) return { text: `unresolved (${m.unresolved.length})`, tone: 'text-red-300' }
  if (m.incarnation?.state === 'retired') return { text: 'retired', tone: 'text-muted' }
  if (!m.live) return { text: 'silent', tone: 'text-red-300' }
  if (m.retire_requested) return { text: 'retiring', tone: 'text-amber-200' }
  return { text: 'live', tone: 'text-emerald-300' }
}

// A membership change (ADR-0021 D4, H3e) is a join or a member removal: one operation at a time
// holds the control plane's membership, and the screen follows it by its record.
type MembershipArgs = { name?: string; peer_url?: string; id?: string }

function membershipTitle(op: Operation): string {
  const args = (op.args ?? {}) as MembershipArgs
  return op.kind === 'control-join' ? `Join of ${args.name ?? 'a control node'}` : `Removal of member ${args.id ?? ''}`
}

// membershipOutcome says how a membership change ended, from its record: never before it ended.
function membershipOutcome(op: Operation): string {
  const args = (op.args ?? {}) as MembershipArgs
  const removed = (op.result as { name?: string } | undefined)?.name || args.id
  switch (op.status) {
    case 'succeeded': return op.kind === 'control-join' ? `${args.name} joined: it is a voting member` : `member ${removed} removed`
    case 'cancelled': return `${membershipTitle(op)} cancelled; the membership is as it was`
    default: return `${membershipTitle(op)} ${op.status}: ${op.error?.message ?? 'no reason recorded'}`
  }
}

// MembershipProgress is a membership change as its record stands: its phase and what it waits on,
// the command a join's new host runs while the join waits for it, and the actions the record allows.
function MembershipProgress({ op, template, busy, onCancel, onResume }: { op: Operation; template: string; busy: boolean; onCancel: () => void; onResume: () => void }) {
  const args = (op.args ?? {}) as MembershipArgs
  const running = unfinished(op)
  const waitingForHost = op.kind === 'control-join' && running && !op.cancel_request && op.phase !== 'catching_up' && op.phase !== 'learner_remove'
  return <div className="space-y-3 text-sm">
    <p><span className="font-semibold">{membershipTitle(op)}</span><span className="ml-2 font-mono text-xs text-muted">{op.id}</span></p>
    <p role="status" className={running ? 'text-amber-100' : op.status === 'succeeded' ? 'text-emerald-300' : 'text-red-200'}>{running ? `${op.status} · phase ${op.phase ?? 'queued'}` : membershipOutcome(op)}</p>
    {op.member && <p className="text-muted">Learner <span className="font-mono text-xs text-paper">{op.member.id}</span> at {op.member.peer_url}</p>}
    {running && (op.blockers ?? []).length > 0 && <ul className="grid gap-1 text-muted">{(op.blockers ?? []).map((b) => <li key={b.code}>{b.message ?? b.code}</li>)}</ul>}
    {running && op.cancel_request && <p className="text-muted">Cancellation requested by {op.cancel_request.actor}: the join's owner removes its learner, then it ends cancelled.</p>}
    {waitingForHost && <div className="space-y-2"><p>Run this exact command on the new host. It fetches the join's bootstrap there, so the encryption key never passes through this browser.</p><CopyLine value={joinCommand(template, args.name ?? '', args.peer_url ?? '', op.id)} /></div>}
    {running && <div className="flex flex-wrap gap-2">
      {op.allowed_actions?.includes('cancel') && <button type="button" disabled={busy} onClick={onCancel} className="rounded-lg border border-red-500 px-3 py-1.5 text-xs font-semibold text-red-100 disabled:opacity-50">{op.kind === 'control-join' ? 'Cancel join' : 'Cancel removal'}</button>}
      {op.allowed_actions?.includes('resume') && <button type="button" disabled={busy} onClick={onResume} className="rounded-lg border border-ember-500 px-3 py-1.5 text-xs font-semibold disabled:opacity-50">Resume on this node</button>}
    </div>}
  </div>
}

export function ControlPlane() {
  const { control, fleet, loading, error, refresh, token, notify } = useStore()
  const [shown, setShown] = useState<string | null>(null) // the proxy whose install diagnostics the drawer shows
  const [diagnostics, setDiagnostics] = useState<ProxyDiagnostics | null>(null)
  const [diagnosticsError, setDiagnosticsError] = useState('')
  const [attestation, setAttestation] = useState('')
  const [busy, setBusy] = useState(false)
  useEffect(() => {
    if (!shown) return
    let live = true
    getProxyDiagnostics(token, shown).then((d) => { if (live) { setDiagnostics(d); setDiagnosticsError('') } }).catch((err: unknown) => { if (live) setDiagnosticsError(err instanceof Error ? err.message : String(err)) })
    return () => { live = false }
  }, [shown, token, fleet])
  const act = async (fn: () => Promise<unknown>, success: string) => {
    setBusy(true)
    try { await fn(); notify(success); await refresh(); return true } catch (err) { notify(err instanceof Error ? err.message : String(err), 'danger'); return false } finally { setBusy(false) }
  }
  const [adding, setAdding] = useState(false)
  const [name, setName] = useState('')
  const [peerURL, setPeerURL] = useState('http://host:2380')
  // The join's intent keeps one Idempotency-Key until it is answered, so a retry after a lost
  // answer finds the same operation; a changed name or peer URL is a new intent with a new key.
  const intent = useRef<{ key: string; body: string } | null>(null)
  const [removal, setRemoval] = useState<MemberRemoveDryRun | null>(null)
  // followed is the membership change started from this screen, whose end it announces; otherwise
  // the screen follows the change the control node reports as active.
  const [followed, setFollowed] = useState<string | null>(null)
  const [change, setChange] = useState<Operation | null>(null)
  const watch = followed ?? control?.active_membership_operation ?? null
  useEffect(() => {
    if (!watch || !token) return
    let live = true
    let timer: ReturnType<typeof setTimeout> | undefined
    const poll = async () => {
      try {
        const op = await getOperation(token, watch)
        if (!live) return
        setChange(op)
        if (unfinished(op)) { timer = setTimeout(() => void poll(), 2000); return }
        if (watch === followed) notify(membershipOutcome(op), op.status === 'succeeded' ? 'success' : op.status === 'cancelled' ? 'neutral' : 'danger')
        void refresh()
      } catch {
        if (live) timer = setTimeout(() => void poll(), 2000)
      }
    }
    void poll()
    return () => { live = false; clearTimeout(timer) }
  }, [watch, followed, token, notify, refresh])
  const shownChange = change && change.id === watch ? change : null
  const changing = Boolean(control?.active_membership_operation) || unfinished(shownChange)
  const requestJoinIntent = async () => {
    const body = { name: name.trim(), peer_url: peerURL.trim() }
    const key = JSON.stringify(body)
    if (intent.current?.body !== key) intent.current = { key: newRequestKey(), body: key }
    setBusy(true)
    try {
      const op = await requestJoin(token, body, intent.current.key)
      intent.current = null
      setChange(op)
      setFollowed(op.id)
    } catch (err) {
      notify(err instanceof Error ? err.message : String(err), 'danger')
    } finally { setBusy(false) }
  }
  const planRemoval = (id: string) => {
    setBusy(true)
    removeMemberDryRun(token, id).then(setRemoval).catch((err: unknown) => notify(err instanceof Error ? err.message : String(err), 'danger')).finally(() => setBusy(false))
  }
  const confirmRemoval = async () => {
    if (!removal?.token) return
    setBusy(true)
    try {
      const answer = await removeMember(token, removal.member.id, removal.token)
      if ('status' in answer) { setChange(answer); setFollowed(answer.id) } else {
        notify(`member ${answer.name || answer.member_id} removed; ${answer.voters} voting member(s) left${answer.warning ? `: ${answer.warning}` : ''}`)
        await refresh()
      }
    } catch (err) {
      notify(err instanceof Error ? err.message : String(err), 'danger')
    } finally { setBusy(false); setRemoval(null) }
  }
  const changeAction = (fn: () => Promise<Operation>, said: (op: Operation) => string) => () => {
    setBusy(true)
    fn().then((op) => { setChange(op); notify(said(op), 'neutral') }).catch((err: unknown) => notify(err instanceof Error ? err.message : String(err), 'danger')).finally(() => setBusy(false))
  }
  const progress = shownChange && control ? <MembershipProgress op={shownChange} template={control.join} busy={busy}
    onCancel={changeAction(() => cancelOperation(token, shownChange.id), cancelOutcome)}
    onResume={changeAction(() => resumeOperation(token, shownChange.id), (op) => `${op.kind} resumed on ${op.node ?? 'this node'} (${op.status}, phase ${op.phase ?? 'queued'})`)} /> : null

  if (!control) return <Card title="Control plane"><p role={error ? 'alert' : 'status'} className="text-muted">{error || (loading ? 'Loading control state…' : 'No control state yet.')}</p></Card>
  const members = fleet?.members ?? control.fleet ?? []
  const stale = members.filter((member) => !member.live)
  const observed = memberHealth(control)
  const voters = observed.filter((m) => m.role === 'voter').length
  const quorumMath = `${voters} voting · ${control.cluster.quorum} needed for writes`
  const quorumState = control.quorum?.state ?? 'unknown'
  const quorumDetail = control.quorum?.observed_at
    ? `${quorumState === 'unavailable' ? `${control.quorum.error_code ?? 'error'}, ` : ''}read ${ago(control.quorum.age_ms)} ago by ${control.quorum.observer || control.node} · ${quorumMath}`
    : `no recent observation · ${quorumMath}`
  return <div className="space-y-5">
    {error && <div role="alert" className="rounded-lg border border-red-600/60 bg-red-950/40 p-3 text-sm text-red-200">{error}</div>}
    <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
      <Card><Stat label="Quorum" value={quorumState === 'reachable' ? 'Reachable' : quorumState === 'unavailable' ? 'Unavailable' : 'Unknown'} detail={quorumDetail} /></Card>
      <Card><Stat label="Fleet" value={members.length} detail={`${members.filter((member) => member.live).length} live`} /></Card>
      <Card><Stat label="Directory" value={control.directory} detail={`etcd revision ${control.cluster.revision}`} /></Card>
      <Card><Stat label="Database" value={bytes(control.cluster.db_in_use_bytes)} detail={`${bytes(control.cluster.db_bytes)} / ${bytes(control.cluster.quota_bytes)} quota`} /></Card>
    </div>
    <Card eyebrow="Consensus" title="Control members" action={<div className="flex gap-2"><button type="button" onClick={() => setAdding(true)} className="rounded-md bg-ember-600 px-3 py-1.5 text-xs font-semibold text-white hover:bg-ember-500">Add node</button><button type="button" onClick={() => void refresh()} className="rounded-md border border-ink-700 px-3 py-1.5 text-xs font-semibold text-muted hover:border-ember-500 hover:text-paper">Refresh</button></div>}>
      <p className="mb-3 text-sm text-muted">Leader: <span className="text-paper">{control.cluster.leader || 'none'}</span> · {quorumMath}</p>
      {control.partial && <p role="status" className="mb-3 rounded-lg border border-amber-500/60 bg-amber-950/30 p-2 text-xs text-amber-100">Partial: quorum is not reachable or a member's health is not known, so this view may be incomplete or stale.</p>}
      {progress && (unfinished(shownChange) || shownChange?.id === followed) && <section aria-label="Membership change" className="mb-3 rounded-lg border border-ember-500/60 bg-ink-950 p-3">
        {progress}
        {!unfinished(shownChange) && <button type="button" onClick={() => setFollowed(null)} className="mt-2 text-xs text-muted underline">Dismiss</button>}
      </section>}
      <div className="divide-y divide-ink-700">
        {observed.map((member) => {
          const joining = unfinished(shownChange) && shownChange?.member?.id === member.id
          const self = member.name !== undefined && member.name !== '' && member.name === control.node
          return <div key={member.id} data-member={member.id} data-highlighted={joining || undefined} className={`grid gap-2 rounded-lg px-2 py-3 text-sm sm:grid-cols-[1fr_auto_auto_auto] sm:items-center ${joining ? 'bg-ember-600/20 ring-1 ring-ember-400' : ''}`}>
            <div><span className="font-semibold">{member.name || '(unnamed)'}</span><span className="ml-2 font-mono text-xs text-muted">{member.id}</span><span className="ml-2 text-muted">{member.peer_urls[0]}</span>{member.reason && <p className="text-xs text-muted">{member.reason}</p>}</div>
            <span className="text-muted">{member.leader ? 'leader' : member.role === 'learner' ? 'learner · not voting' : 'follower'}{joining && shownChange?.phase ? ` · ${shownChange.phase}` : ''}</span>
            <span title={member.observed_at ? `observed ${ago(member.age_ms)} ago by ${member.observer ?? control.node}` : 'not observed yet'} className={member.health === 'healthy' ? 'text-emerald-300' : member.health === 'unreachable' ? 'text-red-300' : 'text-amber-200'}>{member.health}{member.observed_at ? ` · ${ago(member.age_ms)} ago` : ''}</span>
            {self ? <span className="text-xs text-muted">this node</span> : <button type="button" aria-label={`Remove member ${member.name || member.id}`} disabled={busy || changing} title={changing ? 'a membership change is in progress' : undefined} onClick={() => planRemoval(member.id)} className="rounded-md border border-red-600/70 px-2 py-1 text-xs font-semibold text-red-200 disabled:opacity-40">Remove</button>}
          </div>
        })}
      </div>
    </Card>
    <Card eyebrow="Data plane" title="Proxy fleet">
      {members.length === 0 ? <p className="text-sm text-muted">No proxies have joined.</p> : <div className="overflow-x-auto"><table className="w-full text-left text-sm">
        <thead className="text-xs uppercase tracking-wider text-muted"><tr><th className="pb-3">Proxy</th><th className="pb-3">Host</th><th className="pb-3">Applied</th><th className="pb-3">Durable</th><th className="pb-3">State</th><th className="pb-3">Last heartbeat</th><th className="pb-3">Version</th></tr></thead>
        <tbody className="divide-y divide-ink-700">{members.map((member) => { const state = memberState(member); return <tr key={member.id} className="cursor-pointer hover:bg-ink-800/60" onClick={() => setShown(member.id)}><td className="py-3 font-medium text-ember-300">{member.id}</td><td className="py-3 text-muted">{member.host || '—'}</td><td className="py-3 font-mono">{member.applied} / {fleet?.version ?? control.directory}</td><td className={`py-3 font-mono ${(member.durable ?? 0) < member.applied ? 'text-amber-200' : ''}`}>{member.durable ?? '—'}{(member.durable ?? 0) < member.applied ? ' (not durable)' : ''}</td><td className={`py-3 ${state.tone}`}>{state.text}</td><td className="py-3 text-muted">{member.seen ? new Date(member.seen).toLocaleTimeString() : '—'}</td><td className="py-3 text-muted">{member.version || '—'}</td></tr> })}</tbody>
      </table></div>}
      {stale.length > 0 && <p className="mt-4 rounded-lg border border-red-600/60 bg-red-950/40 p-3 text-sm text-red-200">Silent proxies: {stale.map((member) => member.id).join(', ')}. A silent proxy's process may still be running: every barrier waits on it until it is back, retires, or an operator resolves its incarnation.</p>}
    </Card>
    <Drawer open={Boolean(shown)} title={shown ?? ''} eyebrow="Proxy install" onClose={() => { setShown(null); setDiagnostics(null); setDiagnosticsError('') }}>
      {diagnosticsError ? <p role="alert" className="text-red-300">{diagnosticsError}</p> : diagnostics && diagnostics.id === shown ? <div className="space-y-5">
        <dl className="grid grid-cols-[max-content_1fr] gap-x-4 gap-y-2 text-sm">
          <dt className="text-muted">Directory</dt><dd className="font-mono">{diagnostics.directory}</dd>
          <dt className="text-muted">Requests use</dt><dd className="font-mono">{diagnostics.applied}{diagnostics.lineage ? '' : ' (other lineage)'}</dd>
          <dt className="text-muted">Installed</dt><dd className="font-mono">{Math.max(diagnostics.installed ?? 0, diagnostics.applied)}</dd>
          <dt className="text-muted">Restart cache</dt><dd className="font-mono">{diagnostics.durable ?? 0}</dd>
          <dt className="text-muted">Lease</dt><dd>{diagnostics.lease?.seq ? <>grants {Math.round(diagnostics.lease.granted / 1e9)} s per heartbeat; heartbeat {diagnostics.lease.seq} seen {((diagnostics.lease.age ?? 0) / 1e9).toFixed(1)} s ago; <span className={diagnostics.lease.live ? 'text-emerald-300' : 'text-red-300'}>{diagnostics.lease.live ? 'live' : 'expired'}</span></> : <span className="text-muted">no heartbeat recorded</span>}</dd>
        </dl>
        {diagnostics.secrets.length > 0 && <Card title="Cluster secrets"><dl className="grid grid-cols-[max-content_1fr] gap-x-4 gap-y-1 text-sm">{diagnostics.secrets.map((sec) => <div key={sec.cluster} className="contents"><dt className="text-muted">{sec.cluster}</dt><dd className={`font-mono ${sec.current ? '' : 'text-amber-200'}`}>generation {sec.have || 'none'}{sec.current ? '' : ` (control holds ${sec.want})`}</dd></div>)}</dl></Card>}
        {diagnostics.problems.length === 0 ? <p className="text-sm text-emerald-300">Nothing is off with this proxy's install.</p> : <ul className="grid gap-2 text-sm text-amber-200">{diagnostics.problems.map((problem) => <li key={problem}>{problem}</li>)}</ul>}
        <Card title="Incarnation" eyebrow={diagnostics.incarnation ? `${diagnostics.incarnation.state}${diagnostics.retire_requested ? ', retirement requested' : ''}` : 'none running'}>
          {diagnostics.incarnation ? <p className="font-mono text-xs">{diagnostics.incarnation.id}<span className="ml-2 text-muted">since {diagnostics.incarnation.started ? new Date(diagnostics.incarnation.started).toLocaleString() : '—'}{diagnostics.incarnation.ended ? `, ended ${new Date(diagnostics.incarnation.ended).toLocaleString()}` : ''}; {diagnostics.uncertain ?? 0} backend outcomes unknown</span></p> : <p className="text-sm text-muted">No process of this proxy is running or retired; its last one was resolved.</p>}
          {(diagnostics.unresolved ?? []).map((inc) => <div key={inc.id} className="mt-3 rounded-lg border border-red-600/60 bg-red-950/30 p-3 text-sm">
            <p className="text-red-100">Incarnation <span className="font-mono text-xs">{inc.id}</span> did not retire cleanly ({inc.uncertain ?? 0} outcomes unknown{inc.ended ? `, ended ${new Date(inc.ended).toLocaleString()}` : ''}). Every barrier waits on it until you record that the process is stopped and its backend work has ended.</p>
            <label className="mt-2 block text-xs text-muted">Attestation: how that was established<input aria-label={`Attestation for ${inc.id}`} value={attestation} onChange={(event) => setAttestation(event.target.value)} className="mt-1 w-full rounded-lg border border-ink-700 bg-ink-950 px-3 py-2 text-paper" placeholder="host decommissioned; backend shows no request from it" /></label>
            <button type="button" disabled={busy || !attestation.trim()} onClick={() => void act(() => resolveProxy(token, diagnostics.id, inc.id, attestation.trim()), `Incarnation ${inc.id} of ${diagnostics.id} resolved`).then((ok) => { if (ok) setAttestation('') })} className="mt-2 rounded-lg border border-red-500 px-3 py-1.5 text-xs font-semibold text-red-100 disabled:opacity-50">Resolve incarnation</button>
          </div>)}
          <div className="mt-4 flex flex-wrap gap-3">
            {diagnostics.incarnation?.state === 'active' && diagnostics.live && <button type="button" disabled={busy || diagnostics.retire_requested} onClick={() => void act(() => retireProxy(token, diagnostics.id), `${diagnostics.id}: retirement requested; it drains and stops at its next heartbeat`)} className="rounded-lg border border-ember-500 px-4 py-2 text-sm font-semibold disabled:opacity-50">Retire proxy</button>}
            {!diagnostics.live && <button type="button" disabled={busy} onClick={() => void act(() => forgetProxy(token, diagnostics.id), `${diagnostics.id} forgotten`).then((ok) => { if (ok) setShown(null) })} className="rounded-lg border border-ink-600 px-4 py-2 text-sm font-semibold text-muted hover:text-paper disabled:opacity-50">Forget proxy</button>}
          </div>
        </Card>
      </div> : <p className="text-muted">Loading proxy install…</p>}
    </Drawer>
    <Drawer open={adding} title="Join a control member" eyebrow="Control plane" onClose={() => setAdding(false)} footer={shownChange?.kind === 'control-join' && shownChange.id === followed ? undefined : <button type="button" disabled={busy || changing || !name.trim() || !peerURL.trim()} onClick={() => void requestJoinIntent()} className="w-full rounded-lg bg-ember-600 px-4 py-2 font-semibold text-white disabled:opacity-50">Request join</button>}>
      {shownChange?.kind === 'control-join' && shownChange.id === followed ? progress : <div className="grid gap-4">
        <label className="text-sm font-medium">New member name<input value={name} onChange={(event) => setName(event.target.value)} className="mt-2 w-full rounded-lg border border-ink-700 bg-ink-950 px-3 py-2" /></label>
        <label className="text-sm font-medium">Peer URL<input value={peerURL} onChange={(event) => setPeerURL(event.target.value)} className="mt-2 w-full rounded-lg border border-ink-700 bg-ink-950 px-3 py-2" /></label>
        <p className="text-sm text-muted">Request join records the join and adds the new node as a learner, which does not vote and does not count toward quorum. The next step shows the command to run on the new host; the join promotes it to a voter once it has caught up, and you can cancel it until then.</p>
        {changing && <p role="status" className="text-sm text-amber-200">A membership change is in progress; one runs at a time.</p>}
      </div>}
    </Drawer>
    <ConfirmDrawer open={Boolean(removal)} destructive busy={busy} disabled={!removal?.allowed || !removal.token} title={removal ? `Remove ${removal.member.name || '(unnamed)'} (${removal.member.id})` : ''} onClose={() => setRemoval(null)} onConfirm={() => void confirmRemoval()}
      summary={removal && <dl className="grid grid-cols-[max-content_1fr] gap-x-4 gap-y-2">
        <dt>Member ID</dt><dd className="font-mono text-paper">{removal.member.id}</dd>
        <dt>Name</dt><dd className="text-paper">{removal.member.name || '(unnamed: it never started)'}</dd>
        <dt>Role</dt><dd className="text-paper">{removal.member.role}{removal.member.started ? '' : ', never started'}</dd>
        <dt>Peer URL</dt><dd className="text-paper">{removal.member.peer_urls.join(', ')}</dd>
        <dt>Quorum after</dt><dd className="text-paper">{removal.member.role === 'learner' ? `a learner does not vote: ${removal.voters_after} voting member(s) either way` : `${removal.voters_after} voting member(s); writes need ${Math.floor(removal.voters_after / 2) + 1}`}</dd>
      </dl>}>
      {removal?.warning && <p role="status" className="rounded-lg border border-amber-500/60 bg-amber-950/30 p-3 text-sm text-amber-100">{removal.warning}</p>}
      {removal && !removal.allowed && <p role="alert" className="rounded-lg border border-red-600/60 bg-red-950/40 p-3 text-sm text-red-200">{removal.reason}</p>}
      {removal?.allowed && <p className="mt-3 text-xs text-muted">This confirmation names member {removal.member.id} and the member list as it is now: if either changes before you confirm, the removal is refused.</p>}
    </ConfirmDrawer>
  </div>
}
