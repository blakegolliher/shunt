import type { PropsWithChildren, ReactNode } from 'react'

export function Card({ title, eyebrow, action, children, className = '' }: PropsWithChildren<{ title?: string; eyebrow?: string; action?: ReactNode; className?: string }>) {
  return <section className={`rounded-xl border border-ink-700 bg-ink-900 p-5 shadow-ember ${className}`}>
    {(title || eyebrow || action) && <header className="mb-4 flex items-start justify-between gap-4">
      <div>{eyebrow && <p className="eyebrow mb-1">{eyebrow}</p>}{title && <h2 className="text-lg font-semibold">{title}</h2>}</div>
      {action}
    </header>}
    {children}
  </section>
}
