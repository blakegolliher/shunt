import { createContext, useCallback, useContext, useEffect, useMemo, useReducer } from 'react'
import type { PropsWithChildren } from 'react'
import { getControl, getFleet, getStatus, streamEvents } from './api/client'
import type { ControlStatus, DirectoryStatus, FleetStatus, StreamEvent } from './api/client'

const tokenKey = 'shunt.control.token'

export const toastMillis = 5000
let toastSeq = 0

export interface Toast {
  id: number
  tone: 'success' | 'danger' | 'neutral'
  message: string
}

interface State {
  token: string
  control: ControlStatus | null
  fleet: FleetStatus | null
  directory: DirectoryStatus | null
  loading: boolean
  error: string
  lastEvent: StreamEvent | null
  toasts: Toast[]
}

type Action =
  | { type: 'token'; token: string }
  | { type: 'loading' }
  | { type: 'loaded'; control: ControlStatus; fleet: FleetStatus; directory: DirectoryStatus }
  | { type: 'error'; message: string }
  | { type: 'event'; event: StreamEvent }
  | { type: 'toast'; toast: Toast }
  | { type: 'dismiss'; id: number }

const initialState = (): State => ({
  token: sessionStorage.getItem(tokenKey) ?? '',
  control: null,
  fleet: null,
  directory: null,
  loading: false,
  error: '',
  lastEvent: null,
  toasts: [],
})

function reducer(state: State, action: Action): State {
  switch (action.type) {
    case 'token': return { ...state, token: action.token, control: null, fleet: null, directory: null, error: '' }
    case 'loading': return { ...state, loading: true, error: '' }
    case 'loaded': return { ...state, loading: false, error: '', control: action.control, fleet: action.fleet, directory: action.directory }
    case 'error': return { ...state, loading: false, error: action.message }
    case 'event': return { ...state, lastEvent: action.event }
    case 'toast': return { ...state, toasts: [...state.toasts.slice(-3), action.toast] }
    case 'dismiss': return { ...state, toasts: state.toasts.filter((toast) => toast.id !== action.id) }
  }
}

interface StoreValue extends State {
  setToken: (token: string) => void
  clearToken: () => void
  refresh: () => Promise<void>
  dismissToast: (id: number) => void
  notify: (message: string, tone?: Toast['tone']) => void
}

const Store = createContext<StoreValue | null>(null)

export function StoreProvider({ children }: PropsWithChildren) {
  const [state, dispatch] = useReducer(reducer, undefined, initialState)

  const setToken = useCallback((token: string) => {
    const clean = token.trim()
    sessionStorage.setItem(tokenKey, clean)
    dispatch({ type: 'token', token: clean })
  }, [])

  const clearToken = useCallback(() => {
    sessionStorage.removeItem(tokenKey)
    dispatch({ type: 'token', token: '' })
  }, [])

  const dismissToast = useCallback((id: number) => dispatch({ type: 'dismiss', id }), [])
  // A success or neutral toast closes itself; a danger toast carries refusal text the operator
  // must be able to read, so it stays until clicked.
  const notify = useCallback((message: string, tone: Toast['tone'] = 'success') => {
    const id = ++toastSeq
    dispatch({ type: 'toast', toast: { id, message, tone } })
    if (tone !== 'danger') setTimeout(() => dispatch({ type: 'dismiss', id }), toastMillis)
  }, [])

  const refresh = useCallback(async () => {
    if (!state.token) return
    dispatch({ type: 'loading' })
    try {
      const [control, fleet, directory] = await Promise.all([getControl(state.token), getFleet(state.token), getStatus(state.token)])
      dispatch({ type: 'loaded', control, fleet, directory })
    } catch (error) {
      dispatch({ type: 'error', message: error instanceof Error ? error.message : String(error) })
    }
  }, [state.token])

  useEffect(() => { void refresh() }, [refresh])

  useEffect(() => {
    if (!state.token) return
    const controller = new AbortController()
    let lastEventID = ''
    let stopped = false
    const follow = async () => {
      while (!stopped) {
        try {
          await streamEvents(state.token, lastEventID, controller.signal, (event) => {
            if (event.id) lastEventID = event.id
            dispatch({ type: 'event', event })
            if (event.type === 'reset' || event.type === 'directory' || event.type === 'fleet' || event.type === 'fence') void refresh()
            if (event.type === 'fence' && typeof event.data === 'object' && event.data !== null) {
              const record = event.data as { status?: string; kind?: string; error?: string }
              if (record.status === 'succeeded' || record.status === 'failed' || record.status === 'cancelled') {
                notify(record.error || `${record.kind ?? 'operation'} ${record.status}`, record.status === 'succeeded' ? 'success' : 'danger')
              }
            }
          })
        } catch (error) {
          if (controller.signal.aborted) break
          dispatch({ type: 'error', message: error instanceof Error ? error.message : String(error) })
        }
        await new Promise((resolve) => setTimeout(resolve, 500))
      }
    }
    void follow()
    return () => { stopped = true; controller.abort() }
  }, [notify, refresh, state.token])

  const value = useMemo<StoreValue>(() => ({ ...state, setToken, clearToken, refresh,
    dismissToast,
    notify,
  }),
    [state, setToken, clearToken, refresh, dismissToast, notify])
  return <Store.Provider value={value}>{children}</Store.Provider>
}

// The provider and its hook deliberately share the private context in this small store module.
// eslint-disable-next-line react-refresh/only-export-components
export function useStore(): StoreValue {
  const value = useContext(Store)
  if (!value) throw new Error('useStore must be inside StoreProvider')
  return value
}
