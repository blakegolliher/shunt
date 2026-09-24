import { Component } from 'react'
import type { ErrorInfo, PropsWithChildren } from 'react'

// ErrorBoundary keeps one screen's render failure to that screen: it shows the error where the
// screen was, and the navigation still works, instead of React unmounting the whole app to a blank
// page. resetKey (the screen's name) clears the error when the operator moves to another screen.
export class ErrorBoundary extends Component<PropsWithChildren<{ resetKey: string }>, { error: Error | null; key: string }> {
  state = { error: null as Error | null, key: this.props.resetKey }

  static getDerivedStateFromError(error: Error) {
    return { error }
  }

  static getDerivedStateFromProps(props: { resetKey: string }, state: { error: Error | null; key: string }) {
    return props.resetKey !== state.key ? { error: null, key: props.resetKey } : null
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    console.error('screen failed to render', error, info.componentStack)
  }

  render() {
    if (this.state.error) {
      return <div role="alert" className="rounded-xl border border-red-600 bg-red-950/40 p-5 text-sm text-red-100">
        <p className="font-semibold">This screen failed to render: {this.state.error.message}</p>
        <p className="mt-2 text-red-200/80">The rest of the control surface still works. Choose another screen, or reload the page; please report this with the message above.</p>
      </div>
    }
    return this.props.children
  }
}
