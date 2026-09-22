export function FenceStatus({ held, waitingOn, version, phase }: { held: boolean; waitingOn: string[]; version: number; phase?: string }) {
  if (held || waitingOn.length > 0) {
    return <div role="status" className="rounded-lg border border-ember-500/50 bg-ember-500/10 p-3 text-sm text-paper">
      <span className="font-semibold text-ember-300">Fence pending</span>
      <span className="ml-2">{phase ? `${phase}: ` : ''}waiting on {waitingOn.length ? waitingOn.join(', ') : 'the held step'}</span>
    </div>
  }
  if (phase) {
    return <div role="status" className="rounded-lg border border-ember-500/50 bg-ember-500/10 p-3 text-sm text-paper">
      <span className="font-semibold text-ember-300">Operation pending</span>
      <span className="ml-2">{phase}</span>
    </div>
  }
  return <div role="status" className="rounded-lg border border-ink-700 bg-ink-800 p-3 text-sm text-muted">
    Applied at directory revision <span className="font-mono text-paper">{version}</span>
  </div>
}
