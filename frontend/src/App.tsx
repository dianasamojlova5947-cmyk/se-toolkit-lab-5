import { useEffect, useReducer, useState, type FormEvent } from 'react'
import Dashboard from './Dashboard'
import './App.css'

const STORAGE_KEY = 'api_key'

interface Item {
  id: number
  type: string
  title: string
  created_at: string
}

type FetchState =
  | { status: 'idle' }
  | { status: 'loading' }
  | { status: 'success'; items: Item[] }
  | { status: 'error'; message: string }

type FetchAction =
  | { type: 'fetch_start' }
  | { type: 'fetch_success'; data: Item[] }
  | { type: 'fetch_error'; message: string }

type Page = 'items' | 'dashboard'

function fetchReducer(_state: FetchState, action: FetchAction): FetchState {
  switch (action.type) {
    case 'fetch_start':
      return { status: 'loading' }
    case 'fetch_success':
      return { status: 'success', items: action.data }
    case 'fetch_error':
      return { status: 'error', message: action.message }
    default:
      return _state
  }
}

function App() {
  const [token, setToken] = useState(
    () => localStorage.getItem(STORAGE_KEY) ?? '',
  )
  const [draft, setDraft] = useState('')
  const [page, setPage] = useState<Page>('items')
  const [fetchState, dispatch] = useReducer(fetchReducer, { status: 'idle' })

  useEffect(() => {
    if (!token) return

    dispatch({ type: 'fetch_start' })

    fetch('/items/', {
      headers: { Authorization: `Bearer ${token}` },
    })
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`)
        return res.json()
      })
      .then((data: Item[]) => dispatch({ type: 'fetch_success', data }))
      .catch((err: Error) =>
        dispatch({ type: 'fetch_error', message: err.message }),
      )
  }, [token])

  function handleConnect(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    const trimmed = draft.trim()
    if (!trimmed) return

    localStorage.setItem(STORAGE_KEY, trimmed)
    setToken(trimmed)
    setPage('items')
  }

  function handleDisconnect() {
    localStorage.removeItem(STORAGE_KEY)
    setToken('')
    setDraft('')
    setPage('items')
  }

  if (!token) {
    return (
      <main className="auth-shell">
        <form className="token-form card" onSubmit={handleConnect}>
          <p className="eyebrow">Learning management service</p>
          <h1>Connect your API key</h1>
          <p className="muted">
            Enter the bearer token stored in the backend API_KEY setting.
          </p>
          <label className="field">
            <span>API key</span>
            <input
              type="password"
              placeholder="Token"
              value={draft}
              onChange={(event) => setDraft(event.target.value)}
            />
          </label>
          <button type="submit" className="primary-button">
            Connect
          </button>
        </form>
      </main>
    )
  }

  return (
    <div className="app-shell">
      <header className="app-header card">
        <div>
          <p className="eyebrow">Learning management service</p>
          <h1>{page === 'items' ? 'Items' : 'Analytics Dashboard'}</h1>
        </div>

        <div className="header-actions">
          <nav className="page-switcher" aria-label="Page navigation">
            <button
              type="button"
              className={page === 'items' ? 'chip active' : 'chip'}
              onClick={() => setPage('items')}
            >
              Items
            </button>
            <button
              type="button"
              className={page === 'dashboard' ? 'chip active' : 'chip'}
              onClick={() => setPage('dashboard')}
            >
              Dashboard
            </button>
          </nav>

          <button className="secondary-button" onClick={handleDisconnect}>
            Disconnect
          </button>
        </div>
      </header>

      <main className="page-body">
        {page === 'items' ? (
          <section className="card content-card">
            <div className="section-heading">
              <div>
                <p className="eyebrow">Catalog</p>
                <h2>Items</h2>
              </div>
              <p className="muted">
                These records come from the authenticated `/items/` endpoint.
              </p>
            </div>

            {fetchState.status === 'loading' && (
              <p className="status-message">Loading items...</p>
            )}
            {fetchState.status === 'error' && (
              <p className="status-message error">
                Error loading items: {fetchState.message}
              </p>
            )}

            {fetchState.status === 'success' && (
              <div className="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>ID</th>
                      <th>Type</th>
                      <th>Title</th>
                      <th>Created at</th>
                    </tr>
                  </thead>
                  <tbody>
                    {fetchState.items.map((item) => (
                      <tr key={item.id}>
                        <td>{item.id}</td>
                        <td>{item.type}</td>
                        <td>{item.title}</td>
                        <td>{item.created_at}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </section>
        ) : (
          <Dashboard token={token} />
        )}
      </main>
    </div>
  )
}

export default App
