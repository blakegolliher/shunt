import type { PropsWithChildren, ReactNode } from 'react'

export function ConfirmDrawer({ open, title, summary, destructive = false, busy = false, disabled = false, onClose, onConfirm, children }:
  PropsWithChildren<{ open: boolean; title: string; summary: ReactNode; destructive?: boolean; busy?: boolean; disabled?: boolean; onClose: () => void; onConfirm: () => void }>) {
  if (!open) return null
  return <div className="fixed inset-0 z-40 flex justify-end bg-black/70" role="presentation" onMouseDown={(event) => { if (event.target === event.currentTarget) onClose() }}>
    <section role="dialog" aria-modal="true" aria-labelledby="confirm-title" className="h-full w-full max-w-lg overflow-y-auto border-l border-ink-700 bg-ink-900 p-6 shadow-2xl">
      <p className="eyebrow">Review and confirm</p>
      <h2 id="confirm-title" className="mt-2 text-2xl font-semibold">{title}</h2>
      <div className="mt-5 rounded-lg border border-ink-700 bg-ink-950 p-4 text-sm text-muted">{summary}</div>
      <div className="mt-5">{children}</div>
      <div className="mt-8 flex justify-end gap-3">
        <button type="button" className="rounded-lg border border-ink-700 px-4 py-2 text-sm" onClick={onClose}>Cancel</button>
        <button type="button" disabled={busy || disabled} className={`rounded-lg px-4 py-2 text-sm font-semibold text-white disabled:opacity-50 ${destructive ? 'bg-red-600 hover:bg-red-500' : 'bg-ember-600 hover:bg-ember-500'}`} onClick={onConfirm}>{busy ? 'Working…' : 'Confirm'}</button>
      </div>
    </section>
  </div>
}
