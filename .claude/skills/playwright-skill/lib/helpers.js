// playwright-helpers.js
// Reusable utility functions for Playwright automation

const { chromium, firefox, webkit } = require('playwright');

function getExtraHeadersFromEnv() {
  const headerName = process.env.PW_HEADER_NAME;
  const headerValue = process.env.PW_HEADER_VALUE;
  if (headerName && headerValue) return { [headerName]: headerValue };

  const headersJson = process.env.PW_EXTRA_HEADERS;
  if (headersJson) {
    try {
      const parsed = JSON.parse(headersJson);
      if (typeof parsed === 'object' && parsed !== null && !Array.isArray(parsed)) return parsed;
    } catch (e) {}
  }
  return null;
}

async function launchBrowser(browserType = 'chromium', options = {}) {
  const defaultOptions = {
    headless: process.env.HEADLESS !== 'false',
    slowMo: process.env.SLOW_MO ? parseInt(process.env.SLOW_MO) : 0,
    args: ['--no-sandbox', '--disable-setuid-sandbox']
  };
  const browsers = { chromium, firefox, webkit };
  const browser = browsers[browserType];
  if (!browser) throw new Error(`Invalid browser type: ${browserType}`);
  return await browser.launch({ ...defaultOptions, ...options });
}

async function createPage(context, options = {}) {
  const page = await context.newPage();
  if (options.viewport) await page.setViewportSize(options.viewport);
  page.setDefaultTimeout(options.timeout || 30000);
  return page;
}

async function waitForPageReady(page, options = {}) {
  try {
    await page.waitForLoadState(options.waitUntil || 'networkidle', { timeout: options.timeout || 30000 });
  } catch (e) {
    console.warn('Page load timeout, continuing...');
  }
  if (options.waitForSelector) {
    await page.waitForSelector(options.waitForSelector, { timeout: options.timeout });
  }
}

async function safeClick(page, selector, options = {}) {
  const maxRetries = options.retries || 3;
  const retryDelay = options.retryDelay || 1000;
  for (let i = 0; i < maxRetries; i++) {
    try {
      await page.waitForSelector(selector, { state: 'visible', timeout: options.timeout || 5000 });
      await page.click(selector, { force: options.force || false, timeout: options.timeout || 5000 });
      return true;
    } catch (e) {
      if (i === maxRetries - 1) throw e;
      await page.waitForTimeout(retryDelay);
    }
  }
}

async function safeType(page, selector, text, options = {}) {
  await page.waitForSelector(selector, { state: 'visible', timeout: options.timeout || 10000 });
  if (options.clear !== false) await page.fill(selector, '');
  if (options.slow) {
    await page.type(selector, text, { delay: options.delay || 100 });
  } else {
    await page.fill(selector, text);
  }
}

async function extractTexts(page, selector) {
  await page.waitForSelector(selector, { timeout: 10000 });
  return await page.$$eval(selector, elements => elements.map(el => el.textContent?.trim()).filter(Boolean));
}

async function takeScreenshot(page, name, options = {}) {
  const fs = require('fs');
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
  // Use project .tmp/ directory (gitignored, cross-platform)
  const tmpDir = path.resolve(__dirname, '..', '..', '..', '.tmp');
  if (!fs.existsSync(tmpDir)) fs.mkdirSync(tmpDir, { recursive: true });
  const filename = path.join(tmpDir, `${name}-${timestamp}.png`);
  await page.screenshot({ path: filename, fullPage: options.fullPage !== false, ...options });
  console.log(`Screenshot saved: ${filename}`);
  return filename;
}

async function scrollPage(page, direction = 'down', distance = 500) {
  switch (direction) {
    case 'down': await page.evaluate(d => window.scrollBy(0, d), distance); break;
    case 'up': await page.evaluate(d => window.scrollBy(0, -d), distance); break;
    case 'top': await page.evaluate(() => window.scrollTo(0, 0)); break;
    case 'bottom': await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight)); break;
  }
  await page.waitForTimeout(500);
}

async function extractTableData(page, tableSelector) {
  await page.waitForSelector(tableSelector);
  return await page.evaluate((selector) => {
    const table = document.querySelector(selector);
    if (!table) return null;
    const headers = Array.from(table.querySelectorAll('thead th')).map(th => th.textContent?.trim());
    const rows = Array.from(table.querySelectorAll('tbody tr')).map(tr => {
      const cells = Array.from(tr.querySelectorAll('td'));
      if (headers.length > 0) {
        return cells.reduce((obj, cell, index) => { obj[headers[index] || `column_${index}`] = cell.textContent?.trim(); return obj; }, {});
      }
      return cells.map(cell => cell.textContent?.trim());
    });
    return { headers, rows };
  }, tableSelector);
}

async function handleCookieBanner(page, timeout = 3000) {
  const selectors = [
    'button:has-text("Accept")', 'button:has-text("Accept all")',
    'button:has-text("OK")', 'button:has-text("Got it")',
    'button:has-text("I agree")', '.cookie-accept', '#cookie-accept'
  ];
  for (const selector of selectors) {
    try {
      const el = await page.waitForSelector(selector, { timeout: timeout / selectors.length, state: 'visible' });
      if (el) { await el.click(); return true; }
    } catch (e) {}
  }
  return false;
}

async function createContext(browser, options = {}) {
  const envHeaders = getExtraHeadersFromEnv();
  const mergedHeaders = { ...envHeaders, ...options.extraHTTPHeaders };
  const defaultOptions = {
    viewport: { width: 1280, height: 720 },
    locale: options.locale || 'en-US',
    timezoneId: options.timezoneId || 'America/New_York',
    ...(Object.keys(mergedHeaders).length > 0 && { extraHTTPHeaders: mergedHeaders })
  };
  return await browser.newContext({ ...defaultOptions, ...options });
}

async function detectDevServers(customPorts = []) {
  const http = require('http');
  const commonPorts = [3000, 3001, 3002, 5173, 8080, 8000, 4200, 5000, 9000, 1234];
  const allPorts = [...new Set([...commonPorts, ...customPorts])];
  const detected = [];

  for (const port of allPorts) {
    try {
      await new Promise((resolve) => {
        const req = http.request({ hostname: 'localhost', port, path: '/', method: 'HEAD', timeout: 500 }, (res) => {
          if (res.statusCode < 500) {
            detected.push(`http://localhost:${port}`);
          }
          resolve();
        });
        req.on('error', () => resolve());
        req.on('timeout', () => { req.destroy(); resolve(); });
        req.end();
      });
    } catch (e) {}
  }
  return detected;
}

module.exports = {
  launchBrowser, createPage, waitForPageReady, safeClick, safeType,
  extractTexts, takeScreenshot, scrollPage, extractTableData,
  handleCookieBanner, createContext, detectDevServers, getExtraHeadersFromEnv
};
