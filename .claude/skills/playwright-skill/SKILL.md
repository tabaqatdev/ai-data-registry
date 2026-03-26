---
name: playwright-skill
description: >
  Browser automation with Playwright. Use when testing websites, taking screenshots,
  checking responsive design, filling forms, testing login flows, checking links, or
  any browser-based task. Auto-detects dev servers and writes scripts to .tmp/.
allowed-tools: Bash, Read, Write, Glob
---

**Skill dir**: `.claude/skills/playwright-skill`
**Temp dir**: `.tmp/` (gitignored, all scripts/screenshots go here)
**Env vars** (inside `pixi run`): `PIXI_PROJECT_ROOT`, `INIT_CWD`, `PIXI_PROJECT_NAME`

## Setup (first time)
```bash
pixi run node --version
mkdir -p .tmp
cd .claude/skills/playwright-skill && pixi run pnpm run setup
```

## Workflow

1. **Detect dev servers**:
   ```bash
   cd .claude/skills/playwright-skill && pixi run node -e "require('./lib/helpers').detectDevServers().then(s => console.log(JSON.stringify(s)))"
   ```
   1 server → use it. Multiple → ask user. None → ask for URL.

2. **Write script** to `.tmp/playwright-test-*.js` (never to skill dir)

3. **Execute**:
   ```bash
   pixi run node .claude/skills/playwright-skill/run.js .tmp/my-script.js
   ```

## Script template
```javascript
const { chromium } = require('playwright');
const path = require('path');
const TARGET_URL = 'http://localhost:3001';
const TMP = path.join(process.env.PIXI_PROJECT_ROOT || process.cwd(), '.tmp');

(async () => {
  const browser = await chromium.launch({ headless: false });
  const page = await browser.newPage();
  await page.goto(TARGET_URL);

  // Your automation here...

  await page.screenshot({ path: path.join(TMP, 'result.png'), fullPage: true });
  await browser.close();
})();
```

## Patterns
- **Responsive**: loop viewports `[{w:1920,h:1080}, {w:768,h:1024}, {w:375,h:667}]`, screenshot each
- **Login**: `page.fill('input[name="email"]', ...)` → `page.click('button[type="submit"]')` → `page.waitForURL`
- **Form**: fill inputs → submit → `waitForSelector('.success')`
- **Link check**: `page.locator('a[href^="http"]').all()` → HEAD request each

## Helpers
```javascript
const helpers = require('./lib/helpers');
await helpers.safeClick(page, 'button.submit', { retries: 3 });
await helpers.safeType(page, '#username', 'testuser');
await helpers.takeScreenshot(page, 'result');
await helpers.handleCookieBanner(page);
const data = await helpers.extractTableData(page, 'table.results');
```

## Custom headers
```bash
PW_HEADER_NAME=X-Test PW_HEADER_VALUE=true pixi run node .claude/skills/playwright-skill/run.js .tmp/test.js
```

See `API_REFERENCE.md` for selectors, network interception, auth, visual regression, mobile emulation.
