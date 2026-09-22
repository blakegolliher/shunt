import type { PropsWithChildren, ReactNode } from 'react'

export function Drawer({ open, title, eyebrow, onClose, footer, children }: PropsWithChildren<{
  open: boolean
  title: string
  eyebrow?: string
  onClose: () => void
  footer?: ReactNode
}>) {
  if (!open) return null
  return <div className="fixed inset-0 z-40 flex justify-end bg-black/70" role="presentation" onMouseDown={(event) => { if (event.target === event.currentTarget) onClose() }}>
    <section role="dialog" aria-modal="true" aria-labelledby="drawer-title" className="flex h-full w-full max-w-xl flex-col border-l border-ink-700 bg-ink-900 shadow-2xl">
      <div className="flex items-start justify-between border-b border-ink-700 p-6">
        <div><p className="eyebrow">{eyebrow ?? 'Operator action'}</p><h2 id="drawer-title" className="mt-2 text-2xl font-semibold">{title}</h2></div>
        <button type="button" onClick={onClose} className="rounded-md border border-ink-700 px-3 py-1.5 text-sm text-muted hover:text-paper" aria-label="Close">Close</button>
      </div>
      <div className="flex-1 overflow-y-auto p-6">{children}</div>
      {footer && <div className="border-t border-ink-700 p-6">{footer}</div>}
    </section>
  </div>
}
