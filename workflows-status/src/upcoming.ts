import { LitElement, css, html, nothing } from 'lit'
import { customElement, state } from 'lit/decorators.js'
import { repeat } from 'lit/directives/repeat.js'
import {
  DEFAULT_BRANCH,
  DEFAULT_OWNER,
  DEFAULT_REPO,
  RateLimitError,
  SCHEDULER_INTERVAL_MIN,
  describeCron,
  detectRepoFromLocation,
  loadWorkspaces,
  nextCron,
  relativeTime,
  type Workspace,
} from './lib.ts'

const TICK_MS = 30_000

type Upcoming = {
  workspace: Workspace
  next: Date
  scheduledDispatch: Date
}

// Snap a target UTC time up to the next scheduler tick. A workspace's real
// dispatch lands on the first scheduler poll at or after its cron fire time.
function snapToScheduler(target: Date): Date {
  const d = new Date(target)
  const min = d.getUTCMinutes()
  const remainder = min % SCHEDULER_INTERVAL_MIN
  if (remainder !== 0) {
    d.setUTCMinutes(min + (SCHEDULER_INTERVAL_MIN - remainder), 0, 0)
  } else {
    d.setUTCSeconds(0, 0)
  }
  return d
}

function formatUtc(d: Date): string {
  const pad = (n: number) => String(n).padStart(2, '0')
  return (
    `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())} ` +
    `${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())} UTC`
  )
}

@customElement('upcoming-schedule')
export class UpcomingSchedule extends LitElement {
  @state() private owner = DEFAULT_OWNER
  @state() private repo = DEFAULT_REPO
  @state() private branch = DEFAULT_BRANCH
  @state() private workspaces: Workspace[] = []
  @state() private now = new Date()
  @state() private loading = false
  @state() private error = ''
  @state() private rateLimitedUntil: Date | null = null
  @state() private rateLimitCountdownMs = 0
  private _tick?: number
  private _rateTick?: number

  connectedCallback() {
    super.connectedCallback()
    const params = new URLSearchParams(location.search)
    const detected = detectRepoFromLocation()
    if (detected) {
      this.owner = detected.owner
      this.repo = detected.repo
    }
    if (params.get('owner')) this.owner = params.get('owner')!
    if (params.get('repo')) this.repo = params.get('repo')!
    if (params.get('branch')) this.branch = params.get('branch')!
    this.load()
    this._tick = window.setInterval(() => (this.now = new Date()), TICK_MS)
  }

  disconnectedCallback() {
    super.disconnectedCallback()
    if (this._tick) window.clearInterval(this._tick)
    if (this._rateTick) window.clearInterval(this._rateTick)
  }

  private handleRateLimit(err: RateLimitError) {
    this.rateLimitedUntil = err.resetAt
    this.rateLimitCountdownMs = err.resetAt.getTime() - Date.now()
    this.error = ''
    if (this._rateTick) window.clearInterval(this._rateTick)
    this._rateTick = window.setInterval(() => {
      const remaining = err.resetAt.getTime() - Date.now()
      this.rateLimitCountdownMs = Math.max(0, remaining)
      if (remaining <= 0) {
        window.clearInterval(this._rateTick!)
        this._rateTick = undefined
        this.rateLimitedUntil = null
        window.setTimeout(() => this.load(), 2000)
      }
    }, 1000)
  }

  private async load() {
    this.loading = true
    this.error = ''
    try {
      this.workspaces = await loadWorkspaces(this.owner, this.repo, this.branch)
    } catch (e) {
      if (e instanceof RateLimitError) this.handleRateLimit(e)
      else this.error = (e as Error).message
    } finally {
      this.loading = false
    }
  }

  private upcoming(): Upcoming[] {
    const rows: Upcoming[] = []
    for (const w of this.workspaces) {
      const schedule = w.manifest.schedule
      if (!schedule) continue
      const next = nextCron(schedule, this.now)
      if (!next) continue
      rows.push({ workspace: w, next, scheduledDispatch: snapToScheduler(next) })
    }
    rows.sort((a, b) => a.next.getTime() - b.next.getTime())
    return rows
  }

  private renderRateLimitBanner() {
    if (!this.rateLimitedUntil) return nothing
    const totalSec = Math.max(0, Math.ceil(this.rateLimitCountdownMs / 1000))
    const m = Math.floor(totalSec / 60)
    const s = totalSec % 60
    const pretty =
      m > 0 ? `${m} min ${String(s).padStart(2, '0')} sec` : `${s} sec`
    const resetLocal = this.rateLimitedUntil.toLocaleTimeString()
    return html`
      <div class="ratelimit" role="status">
        <div class="ratelimit-head">
          <span class="ratelimit-dot"></span>
          <strong>GitHub API rate limit hit</strong>
        </div>
        <p class="ratelimit-msg">
          Retries pause for
          <strong class="ratelimit-count">${pretty}</strong>
          (resumes at <code>${resetLocal}</code>).
        </p>
      </div>
    `
  }

  render() {
    const rows = this.upcoming()
    return html`
      <header class="hero">
        <nav class="nav">
          <a href="./">← Watch Center</a>
        </nav>
        <div class="title">
          <h1>Upcoming schedule</h1>
        </div>
        <p class="sub">
          Next cron firing per workspace, in UTC. The scheduler polls every
          ${SCHEDULER_INTERVAL_MIN} minutes, so actual dispatch snaps up to the
          next
          <code>${`*/${SCHEDULER_INTERVAL_MIN} * * * *`}</code>
          tick after the cron fires.
        </p>
      </header>

      ${this.rateLimitedUntil
        ? this.renderRateLimitBanner()
        : this.error
          ? html`<p class="error">${this.error}</p>`
          : nothing}
      ${this.loading && rows.length === 0
        ? html`<p class="muted center">Loading…</p>`
        : nothing}

      <table>
        <thead>
          <tr>
            <th>Workspace</th>
            <th>Schedule</th>
            <th>Next cron fire</th>
            <th>Dispatched</th>
            <th>In</th>
          </tr>
        </thead>
        <tbody>
          ${repeat(
            rows,
            (r) => r.workspace.name,
            (r) => html`
              <tr>
                <td>
                  <strong>${r.workspace.name}</strong>
                  <span class="muted">${r.workspace.manifest.schema ?? ''}</span>
                </td>
                <td>
                  <span title=${r.workspace.manifest.schedule ?? ''}
                    >${describeCron(r.workspace.manifest.schedule ?? '')}</span
                  >
                </td>
                <td>
                  <code>${formatUtc(r.next)}</code>
                </td>
                <td>
                  <code>${formatUtc(r.scheduledDispatch)}</code>
                </td>
                <td>${relativeTime(r.scheduledDispatch)}</td>
              </tr>
            `,
          )}
        </tbody>
      </table>

      <p class="footnote muted">
        Scheduler itself runs every
        ${SCHEDULER_INTERVAL_MIN} minutes
        (<code>*/${SCHEDULER_INTERVAL_MIN} * * * *</code>); maintenance runs
        Sunday 03:00 UTC; catalog merge runs on extract completion plus a
        10-minute cron backstop.
      </p>
    `
  }

  static styles = css`
    :host {
      --fg: #0b0d12;
      --muted: #6b7280;
      --border: #e5e7eb;
      --card: #fff;
      --accent: #7c3aed;
      --failure: #b91c1c;
      --failure-bg: #fee2e2;
      display: block;
      width: 100%;
      max-width: 1100px;
      margin: 0 auto;
      padding: 24px;
      box-sizing: border-box;
      color: var(--fg);
      font: 14px/1.5 system-ui, -apple-system, sans-serif;
      font-variant-numeric: tabular-nums;
    }

    @media (prefers-color-scheme: dark) {
      :host {
        --fg: #e5e7eb;
        --muted: #9ca3af;
        --border: #1f2937;
        --card: #11141b;
        --failure: #ef4444;
        --failure-bg: rgba(239, 68, 68, 0.15);
      }
    }

    h1 {
      font-size: 24px;
      margin: 0 0 8px;
    }

    .hero {
      border-bottom: 1px solid var(--border);
      padding-bottom: 16px;
      margin-bottom: 20px;
    }

    .nav a {
      color: var(--accent);
      text-decoration: none;
      font-size: 13px;
    }

    .nav a:hover {
      text-decoration: underline;
    }

    .sub {
      color: var(--muted);
      margin: 8px 0 0;
    }

    .muted {
      color: var(--muted);
      font-size: 12px;
    }

    .center {
      text-align: center;
      padding: 40px 0;
    }

    code {
      font: 12px/1.4 ui-monospace, Menlo, monospace;
      background: rgba(127, 127, 127, 0.12);
      padding: 1px 6px;
      border-radius: 4px;
    }

    .error {
      padding: 12px;
      border-radius: 6px;
      background: var(--failure-bg);
      color: var(--failure);
    }

    table {
      width: 100%;
      border-collapse: collapse;
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 10px;
      overflow: hidden;
    }

    th,
    td {
      padding: 10px 14px;
      text-align: left;
      border-bottom: 1px solid var(--border);
    }

    th {
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      color: var(--muted);
      background: rgba(127, 127, 127, 0.04);
    }

    tbody tr:last-child td {
      border-bottom: none;
    }

    td strong {
      display: block;
    }

    td .muted {
      display: block;
    }

    .footnote {
      margin-top: 16px;
    }
  `
}

declare global {
  interface HTMLElementTagNameMap {
    'upcoming-schedule': UpcomingSchedule
  }
}
