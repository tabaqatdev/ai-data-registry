---
name: playwright-skill
description: >
  Complete browser automation with Playwright. Auto-detects dev servers, writes
  clean test scripts to the project .tmp/ directory. Test pages, fill forms,
  take screenshots, check responsive design, validate UX, test login flows,
  check links, automate any browser task. Use when user wants to test websites,
  automate browser interactions, validate web functionality, or perform any
  browser-based testing.
allowed-tools: Bash, Read, Write, Glob
---

**Path Resolution:**
The skill directory is `.claude/skills/playwright-skill` in this project.
Set: `SKILL_DIR=".claude/skills/playwright-skill"`

**Environment:** Node.js is provided by pixi (`pixi run node`). Playwright runs via the skill's `run.js` executor.

**Temp directory:** Use `.tmp/` at the project root for all scratch files (scripts, screenshots). This directory is gitignored and works cross-platform.

# Playwright Browser Automation

General-purpose browser automation skill. Write custom Playwright code for any automation task and execute it via the universal executor.

## Setup (First Time)

```bash
# Ensure Node.js is available via pixi (root pixi.toml has nodejs)
pixi run node --version

# Create project temp directory
pixi run python -c "import pathlib; pathlib.Path('.tmp').mkdir(exist_ok=True)"

# Install Playwright and Chromium
cd .claude/skills/playwright-skill && pixi run pnpm run setup
```

## CRITICAL WORKFLOW

1. **Auto-detect dev servers** — For localhost testing, ALWAYS run server detection FIRST:
   ```bash
   cd $SKILL_DIR && pixi run node -e "require('./lib/helpers').detectDevServers().then(servers => console.log(JSON.stringify(servers)))"
   ```
   - If **1 server found**: Use it automatically
   - If **multiple servers found**: Ask user which one
   - If **no servers found**: Ask for URL or offer to help start dev server

2. **Write scripts to `.tmp/`** — NEVER write test files to skill directory; always use `.tmp/playwright-test-*.js`

3. **Use visible browser by default** — Always use `headless: false` unless user specifically requests headless

4. **Parameterize URLs** — Always make URLs configurable via constant at top of script

## Execution Pattern

```bash
# Step 1: Detect dev servers
cd $SKILL_DIR && pixi run node -e "require('./lib/helpers').detectDevServers().then(s => console.log(JSON.stringify(s)))"

# Step 2: Ensure .tmp/ exists and write test script there
pixi run python -c "import pathlib; pathlib.Path('.tmp').mkdir(exist_ok=True)"

# Step 3: Execute from skill directory
cd $SKILL_DIR && pixi run node run.js ../../.tmp/playwright-test-page.js
```

## Common Patterns

### Test a Page
```javascript
// Save to: .tmp/playwright-test-page.js
const { chromium } = require('playwright');
const path = require('path');
const TARGET_URL = 'http://localhost:3001';
const TMP_DIR = path.resolve(__dirname, '..', '..', '.tmp');

(async () => {
  const browser = await chromium.launch({ headless: false });
  const page = await browser.newPage();
  await page.goto(TARGET_URL);
  console.log('Page loaded:', await page.title());
  await page.screenshot({ path: path.join(TMP_DIR, 'screenshot.png'), fullPage: true });
  await browser.close();
})();
```

### Responsive Design Test
```javascript
const { chromium } = require('playwright');
const path = require('path');
const TARGET_URL = 'http://localhost:3001';
const TMP_DIR = path.resolve(__dirname, '..', '..', '.tmp');

(async () => {
  const browser = await chromium.launch({ headless: false, slowMo: 100 });
  const page = await browser.newPage();
  const viewports = [
    { name: 'Desktop', width: 1920, height: 1080 },
    { name: 'Tablet', width: 768, height: 1024 },
    { name: 'Mobile', width: 375, height: 667 },
  ];
  for (const vp of viewports) {
    await page.setViewportSize({ width: vp.width, height: vp.height });
    await page.goto(TARGET_URL);
    await page.screenshot({ path: path.join(TMP_DIR, `${vp.name.toLowerCase()}.png`), fullPage: true });
    console.log(`${vp.name}: done`);
  }
  await browser.close();
})();
```

### Login Flow Test
```javascript
const { chromium } = require('playwright');
const TARGET_URL = 'http://localhost:3001';

(async () => {
  const browser = await chromium.launch({ headless: false });
  const page = await browser.newPage();
  await page.goto(`${TARGET_URL}/login`);
  await page.fill('input[name="email"]', 'test@example.com');
  await page.fill('input[name="password"]', 'password123');
  await page.click('button[type="submit"]');
  await page.waitForURL('**/dashboard');
  console.log('Login successful');
  await browser.close();
})();
```

### Form Submission
```javascript
const { chromium } = require('playwright');
const TARGET_URL = 'http://localhost:3001';

(async () => {
  const browser = await chromium.launch({ headless: false, slowMo: 50 });
  const page = await browser.newPage();
  await page.goto(`${TARGET_URL}/contact`);
  await page.fill('input[name="name"]', 'John Doe');
  await page.fill('input[name="email"]', 'john@example.com');
  await page.fill('textarea[name="message"]', 'Test message');
  await page.click('button[type="submit"]');
  await page.waitForSelector('.success-message');
  console.log('Form submitted');
  await browser.close();
})();
```

### Broken Link Checker
```javascript
const { chromium } = require('playwright');
(async () => {
  const browser = await chromium.launch({ headless: false });
  const page = await browser.newPage();
  await page.goto('http://localhost:3000');
  const links = await page.locator('a[href^="http"]').all();
  const results = { working: 0, broken: [] };
  for (const link of links) {
    const href = await link.getAttribute('href');
    try {
      const response = await page.request.head(href);
      response.ok() ? results.working++ : results.broken.push({ url: href, status: response.status() });
    } catch (e) {
      results.broken.push({ url: href, error: e.message });
    }
  }
  console.log(`Working: ${results.working}, Broken:`, results.broken);
  await browser.close();
})();
```

## Inline Execution (Simple Tasks)

```bash
cd $SKILL_DIR && pixi run node run.js "
const path = require('path');
const browser = await chromium.launch({ headless: false });
const page = await browser.newPage();
await page.goto('http://localhost:3001');
await page.screenshot({ path: path.resolve('../../.tmp/quick.png'), fullPage: true });
await browser.close();
"
```

## Available Helpers

```javascript
const helpers = require('./lib/helpers');
const servers = await helpers.detectDevServers();
await helpers.safeClick(page, 'button.submit', { retries: 3 });
await helpers.safeType(page, '#username', 'testuser');
await helpers.takeScreenshot(page, 'test-result');
await helpers.handleCookieBanner(page);
const data = await helpers.extractTableData(page, 'table.results');
```

## Custom HTTP Headers

```bash
PW_HEADER_NAME=X-Automated-By PW_HEADER_VALUE=playwright-skill \
  cd $SKILL_DIR && pixi run node run.js ../../.tmp/my-script.js
```

## Configuration
- **Headless:** `false` (visible browser by default)
- **Slow Motion:** `100ms` for visibility
- **Timeout:** `30s`
- **Screenshots:** Saved to `.tmp/` (gitignored)

## Advanced Usage
See @API_REFERENCE.md for comprehensive documentation on selectors, network interception, authentication, visual regression, mobile emulation, and performance testing.

## Troubleshooting

```bash
# Playwright not installed
cd $SKILL_DIR && pixi run pnpm run setup

# Module not found — always run from skill directory via run.js
cd $SKILL_DIR && pixi run node run.js ../../.tmp/my-test.js

# Install all browsers (not just Chromium)
cd $SKILL_DIR && pixi run pnpm run install-all-browsers
```

## Cross-references
- Use the **env-check** skill to verify Node.js is available via pixi
- Use the **data-explorer** agent to profile data before building browser-based dashboards
- Use the **geoparquet** skill when testing map-based web apps with spatial data
