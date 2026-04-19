import { parse as parseToml } from 'smol-toml'

export type RegistryManifest = {
  description?: string
  schedule?: string
  timeout?: number
  tags?: string[]
  schema?: string
  table?: string
  tables?: string[]
  mode?: 'append' | 'replace' | 'upsert'
  storage?: string | string[]
  runner?: { backend?: string; flavor?: string; image?: string }
  license?: {
    code?: string
    data?: string
    data_source?: string
    mixed?: boolean
  }
}

export type Workspace = {
  name: string
  manifest: RegistryManifest
}

export const DEFAULT_OWNER = 'walkthru-earth'
export const DEFAULT_REPO = 'ai-data-registry'
export const DEFAULT_BRANCH = 'main'

// Scheduler polls every 15 minutes. A due workspace fires at the first
// scheduler tick on/after its cron time, so real dispatch can lag by up to
// this many minutes.
export const SCHEDULER_CRON = '*/15 * * * *'
export const SCHEDULER_INTERVAL_MIN = 15

export function detectRepoFromLocation(): {
  owner: string
  repo: string
} | null {
  const host = location.hostname
  const segs = location.pathname.split('/').filter(Boolean)
  const ghPages = host.match(/^([^.]+)\.github\.io$/)
  if (ghPages) {
    const owner = ghPages[1]
    const repo = segs[0] || `${owner}.github.io`
    return { owner, repo }
  }
  const meta = document.querySelector<HTMLMetaElement>(
    'meta[name="registry:repo"]',
  )
  if (meta?.content?.includes('/')) {
    const [owner, repo] = meta.content.split('/')
    if (owner && repo) return { owner, repo }
  }
  return null
}

export class RateLimitError extends Error {
  readonly resetAt: Date
  readonly limit: number
  constructor(resetAt: Date, limit: number) {
    super(
      `GitHub API rate limit exhausted (${limit}/hr). Resumes at ${resetAt.toLocaleTimeString()}.`,
    )
    this.name = 'RateLimitError'
    this.resetAt = resetAt
    this.limit = limit
  }
}

export async function ghJson<T>(url: string): Promise<T> {
  const res = await fetch(url, {
    headers: {
      Accept: 'application/vnd.github+json',
      'X-GitHub-Api-Version': '2022-11-28',
    },
  })
  if (res.status === 403 || res.status === 429) {
    const remaining = res.headers.get('x-ratelimit-remaining')
    const reset = res.headers.get('x-ratelimit-reset')
    if (remaining === '0' && reset) {
      const resetAt = new Date(Number(reset) * 1000)
      const limit = Number(res.headers.get('x-ratelimit-limit') ?? 60)
      throw new RateLimitError(resetAt, limit)
    }
  }
  if (!res.ok) throw new Error(`${res.status} ${res.statusText} on ${url}`)
  return res.json() as Promise<T>
}

export async function loadWorkspaces(
  owner: string,
  repo: string,
  branch: string,
): Promise<Workspace[]> {
  const entries = await ghJson<Array<{ name: string; type: string }>>(
    `https://api.github.com/repos/${owner}/${repo}/contents/workspaces?ref=${branch}`,
  )
  const dirs = entries.filter((e) => e.type === 'dir').map((e) => e.name)
  const results = await Promise.all(
    dirs.map(async (name): Promise<Workspace | null> => {
      try {
        const res = await fetch(
          `https://raw.githubusercontent.com/${owner}/${repo}/${branch}/workspaces/${name}/pixi.toml`,
        )
        if (!res.ok) return null
        const parsed = parseToml(await res.text()) as {
          tool?: { registry?: RegistryManifest }
        }
        const manifest = parsed.tool?.registry
        if (!manifest) return null
        return { name, manifest }
      } catch {
        return null
      }
    }),
  )
  return results.filter((w): w is Workspace => w !== null)
}

// --- Cron parsing and next-run calculation (UTC, GitHub Actions semantics) ---

function parseCronField(spec: string, min: number, max: number): Set<number> {
  const set = new Set<number>()
  for (const part of spec.split(',')) {
    let step = 1
    let body = part
    const slash = body.indexOf('/')
    if (slash >= 0) {
      step = Number(body.slice(slash + 1)) || 1
      body = body.slice(0, slash)
    }
    let lo: number, hi: number
    if (body === '*' || body === '') {
      lo = min
      hi = max
    } else if (body.includes('-')) {
      const [a, b] = body.split('-').map(Number)
      lo = a
      hi = b
    } else {
      lo = hi = Number(body)
    }
    for (let v = lo; v <= hi; v += step) set.add(v)
  }
  return set
}

export function nextCron(expr: string, from: Date = new Date()): Date | null {
  const parts = expr.trim().split(/\s+/)
  if (parts.length !== 5) return null
  let minutes: Set<number>, hours: Set<number>, doms: Set<number>
  let months: Set<number>, dows: Set<number>
  try {
    minutes = parseCronField(parts[0], 0, 59)
    hours = parseCronField(parts[1], 0, 23)
    doms = parseCronField(parts[2], 1, 31)
    months = parseCronField(parts[3], 1, 12)
    dows = parseCronField(parts[4], 0, 6)
  } catch {
    return null
  }
  const d = new Date(from)
  d.setUTCSeconds(0, 0)
  d.setUTCMinutes(d.getUTCMinutes() + 1)
  const domRestricted = parts[2] !== '*'
  const dowRestricted = parts[4] !== '*'
  const maxIter = 366 * 24 * 60
  for (let i = 0; i < maxIter; i++) {
    if (!months.has(d.getUTCMonth() + 1)) {
      d.setUTCDate(1)
      d.setUTCMonth(d.getUTCMonth() + 1)
      d.setUTCHours(0, 0, 0, 0)
      continue
    }
    const domOk = doms.has(d.getUTCDate())
    const dowOk = dows.has(d.getUTCDay())
    const dateOk =
      domRestricted && dowRestricted
        ? domOk || dowOk
        : domRestricted
          ? domOk
          : dowRestricted
            ? dowOk
            : true
    if (!dateOk) {
      d.setUTCDate(d.getUTCDate() + 1)
      d.setUTCHours(0, 0, 0, 0)
      continue
    }
    if (!hours.has(d.getUTCHours())) {
      d.setUTCHours(d.getUTCHours() + 1)
      d.setUTCMinutes(0)
      continue
    }
    if (!minutes.has(d.getUTCMinutes())) {
      d.setUTCMinutes(d.getUTCMinutes() + 1)
      continue
    }
    return d
  }
  return null
}

const DAYS = [
  'Sunday',
  'Monday',
  'Tuesday',
  'Wednesday',
  'Thursday',
  'Friday',
  'Saturday',
]

export function describeCron(expr: string): string {
  if (!expr) return '—'
  const parts = expr.trim().split(/\s+/)
  if (parts.length !== 5) return expr
  const [min, hour, dom, month, dow] = parts
  const everyMin = min.match(/^\*\/(\d+)$/)
  if (everyMin && hour === '*' && dom === '*' && month === '*' && dow === '*')
    return `every ${everyMin[1]} minutes`
  const everyHour = hour.match(/^\*\/(\d+)$/)
  if (min === '0' && everyHour && dom === '*' && month === '*' && dow === '*')
    return `every ${everyHour[1]} hours`
  if (/^\d+$/.test(min) && hour === '*' && dom === '*' && month === '*' && dow === '*')
    return min === '0' ? 'every hour' : `hourly at :${min.padStart(2, '0')}`
  const time =
    /^\d+$/.test(hour) && /^\d+$/.test(min)
      ? `${hour.padStart(2, '0')}:${min.padStart(2, '0')}`
      : null
  if (time && dom === '*' && month === '*' && dow === '*') return `daily at ${time} UTC`
  if (time && dom === '*' && month === '*' && /^[0-6]$/.test(dow))
    return `every ${DAYS[+dow]} at ${time} UTC`
  if (time && /^\d+$/.test(dom) && month === '*' && dow === '*')
    return `monthly on day ${dom} at ${time} UTC`
  return expr
}

export const RELATIVE = new Intl.RelativeTimeFormat(undefined, {
  numeric: 'auto',
})

export function relativeTime(iso: string | Date): string {
  const ts = typeof iso === 'string' ? new Date(iso).getTime() : iso.getTime()
  const diffSec = Math.round((Date.now() - ts) / 1000)
  const divs: [number, number, Intl.RelativeTimeFormatUnit][] = [
    [60, 1, 'second'],
    [3600, 60, 'minute'],
    [86400, 3600, 'hour'],
    [86400 * 7, 86400, 'day'],
    [86400 * 30, 86400 * 7, 'week'],
    [86400 * 365, 86400 * 30, 'month'],
    [Infinity, 86400 * 365, 'year'],
  ]
  for (const [limit, div, unit] of divs) {
    if (Math.abs(diffSec) < limit) {
      return RELATIVE.format(-Math.round(diffSec / div), unit)
    }
  }
  return new Date(ts).toLocaleString()
}
