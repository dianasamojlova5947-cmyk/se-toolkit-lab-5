import { useEffect, useState } from 'react'
import {
  BarElement,
  CategoryScale,
  Chart as ChartJS,
  Filler,
  Legend,
  LinearScale,
  LineElement,
  PointElement,
  Title,
  Tooltip,
} from 'chart.js'
import type { ChartData, ChartOptions } from 'chart.js'
import { Bar, Line } from 'react-chartjs-2'

ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  LineElement,
  PointElement,
  Title,
  Tooltip,
  Legend,
  Filler,
)

interface Item {
  id: number
  type: string
  title: string
  created_at: string
}

interface LabOption {
  id: string
  title: string
}

interface ScoreBucket {
  bucket: string
  count: number
}

interface TimelinePoint {
  date: string
  submissions: number
}

interface PassRateRow {
  task: string
  avg_score: number | null
  attempts: number
}

interface DashboardProps {
  token: string
}

const SCORE_BUCKETS = ['0-25', '26-50', '51-75', '76-100'] as const

function fetchJson<T>(path: string, token: string): Promise<T> {
  return fetch(path, {
    headers: { Authorization: `Bearer ${token}` },
  }).then(async (response) => {
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`)
    }

    return (await response.json()) as T
  })
}

function extractLabId(title: string, fallbackId: number): string {
  const match = title.match(/Lab\s+(\d+)/i)
  if (match?.[1]) {
    return `lab-${match[1].padStart(2, '0')}`
  }

  return `lab-${fallbackId}`
}

function sortLabs(left: LabOption, right: LabOption): number {
  return left.title.localeCompare(right.title)
}

function Dashboard({ token }: DashboardProps) {
  const [labs, setLabs] = useState<LabOption[]>([])
  const [selectedLab, setSelectedLab] = useState('')
  const [labsLoading, setLabsLoading] = useState(true)
  const [labsError, setLabsError] = useState<string | null>(null)

  const [scores, setScores] = useState<ScoreBucket[]>([])
  const [timeline, setTimeline] = useState<TimelinePoint[]>([])
  const [passRates, setPassRates] = useState<PassRateRow[]>([])
  const [analyticsLoading, setAnalyticsLoading] = useState(false)
  const [analyticsError, setAnalyticsError] = useState<string | null>(null)

  useEffect(() => {
    let cancelled = false

    setLabsLoading(true)
    setLabsError(null)
    setSelectedLab('')

    fetchJson<Item[]>('/items/', token)
      .then((items) => {
        if (cancelled) return

        const nextLabs = items
          .filter((item) => item.type === 'lab')
          .map((item) => ({
            id: extractLabId(item.title, item.id),
            title: item.title,
          }))
          .sort(sortLabs)

        setLabs(nextLabs)
        setSelectedLab(
          nextLabs.find((lab) => lab.id === 'lab-04')?.id ?? nextLabs[0]?.id ?? '',
        )
      })
      .catch((error: Error) => {
        if (!cancelled) {
          setLabsError(error.message)
        }
      })
      .finally(() => {
        if (!cancelled) {
          setLabsLoading(false)
        }
      })

    return () => {
      cancelled = true
    }
  }, [token])

  useEffect(() => {
    if (!selectedLab) return

    let cancelled = false

    setAnalyticsLoading(true)
    setAnalyticsError(null)

    Promise.all([
      fetchJson<ScoreBucket[]>(
        `/analytics/scores?lab=${encodeURIComponent(selectedLab)}`,
        token,
      ),
      fetchJson<TimelinePoint[]>(
        `/analytics/timeline?lab=${encodeURIComponent(selectedLab)}`,
        token,
      ),
      fetchJson<PassRateRow[]>(
        `/analytics/pass-rates?lab=${encodeURIComponent(selectedLab)}`,
        token,
      ),
    ])
      .then(([nextScores, nextTimeline, nextPassRates]) => {
        if (cancelled) return

        setScores(nextScores)
        setTimeline(nextTimeline)
        setPassRates(nextPassRates)
      })
      .catch((error: Error) => {
        if (!cancelled) {
          setAnalyticsError(error.message)
        }
      })
      .finally(() => {
        if (!cancelled) {
          setAnalyticsLoading(false)
        }
      })

    return () => {
      cancelled = true
    }
  }, [selectedLab, token])

  const scoreData: ChartData<'bar', number[], string> = {
    labels: [...SCORE_BUCKETS],
    datasets: [
      {
        label: 'Submissions',
        data: SCORE_BUCKETS.map(
          (bucket) => scores.find((entry) => entry.bucket === bucket)?.count ?? 0,
        ),
        backgroundColor: 'rgba(74, 144, 226, 0.8)',
        borderRadius: 10,
      },
    ],
  }

  const timelineData: ChartData<'line', number[], string> = {
    labels: timeline.map((point) => point.date),
    datasets: [
      {
        label: 'Submissions',
        data: timeline.map((point) => point.submissions),
        borderColor: 'rgba(238, 108, 77, 1)',
        backgroundColor: 'rgba(238, 108, 77, 0.18)',
        fill: true,
        tension: 0.35,
        pointRadius: 4,
        pointHoverRadius: 6,
      },
    ],
  }

  const chartOptions: ChartOptions<'bar'> = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: {
        display: false,
      },
    },
    scales: {
      y: {
        beginAtZero: true,
        ticks: {
          precision: 0,
        },
      },
    },
  }

  const lineOptions: ChartOptions<'line'> = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: {
        display: false,
      },
    },
    scales: {
      y: {
        beginAtZero: true,
        ticks: {
          precision: 0,
        },
      },
    },
  }

  const totalSubmissions = timeline.reduce(
    (sum, point) => sum + point.submissions,
    0,
  )
  const scoredRows = passRates.filter(
    (row): row is PassRateRow & { avg_score: number } => row.avg_score !== null,
  )
  const averageScore = scoredRows.length
    ? scoredRows.reduce((sum, row) => sum + row.avg_score, 0) / scoredRows.length
    : 0
  const averageScoreLabel = scoredRows.length ? averageScore.toFixed(1) : '—'

  return (
    <section className="dashboard-grid">
      <div className="card content-card dashboard-controls">
        <div className="section-heading">
          <div>
            <p className="eyebrow">Analytics</p>
            <h2>Dashboard</h2>
          </div>
          <p className="muted">
            Explore score buckets, submission trends, and pass rates by lab.
          </p>
        </div>

        <label className="field lab-select">
          <span>Lab</span>
          <select
            value={selectedLab}
            onChange={(event) => setSelectedLab(event.target.value)}
            disabled={labsLoading || labs.length === 0}
          >
            {labs.map((lab) => (
              <option key={lab.id} value={lab.id}>
                {lab.title} ({lab.id})
              </option>
            ))}
          </select>
        </label>

        {labsLoading && <p className="status-message">Loading labs...</p>}
        {labsError && (
          <p className="status-message error">Error loading labs: {labsError}</p>
        )}
        {!labsLoading && labs.length === 0 && !labsError && (
          <p className="status-message">No labs were found for this account.</p>
        )}

        <div className="summary-grid">
          <article className="summary-card">
            <span className="summary-label">Selected lab</span>
            <strong>{selectedLab || 'None'}</strong>
          </article>
          <article className="summary-card">
            <span className="summary-label">Submissions</span>
            <strong>{totalSubmissions}</strong>
          </article>
          <article className="summary-card">
            <span className="summary-label">Average pass rate</span>
            <strong>{averageScoreLabel}</strong>
          </article>
        </div>

        {analyticsLoading && (
          <p className="status-message">Loading analytics...</p>
        )}
        {analyticsError && (
          <p className="status-message error">
            Error loading analytics: {analyticsError}
          </p>
        )}
      </div>

      <article className="card content-card chart-card">
        <div className="section-heading">
          <div>
            <p className="eyebrow">Distribution</p>
            <h3>Score buckets</h3>
          </div>
        </div>
        <div className="chart-frame">
          <Bar data={scoreData} options={chartOptions} />
        </div>
      </article>

      <article className="card content-card chart-card">
        <div className="section-heading">
          <div>
            <p className="eyebrow">Timeline</p>
            <h3>Submissions per day</h3>
          </div>
        </div>
        <div className="chart-frame">
          <Line data={timelineData} options={lineOptions} />
        </div>
      </article>

      <article className="card content-card table-card">
        <div className="section-heading">
          <div>
            <p className="eyebrow">Table</p>
            <h3>Pass rates per task</h3>
          </div>
        </div>

        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Task</th>
                <th>Average score</th>
                <th>Attempts</th>
              </tr>
            </thead>
            <tbody>
              {passRates.map((row) => (
                <tr key={row.task}>
                  <td>{row.task}</td>
                  <td>{row.avg_score === null ? '—' : row.avg_score.toFixed(1)}</td>
                  <td>{row.attempts}</td>
                </tr>
              ))}
              {passRates.length === 0 && (
                <tr>
                  <td colSpan={3} className="empty-cell">
                    No task analytics available for this lab.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </article>
    </section>
  )
}

export default Dashboard
