#!/usr/bin/env node
/**
 * Universal Playwright Executor for Claude Code
 *
 * Executes Playwright automation code from:
 * - File path: node run.js script.js
 * - Inline code: node run.js 'await page.goto("...")'
 * - Stdin: cat script.js | node run.js
 *
 * Ensures proper module resolution by running from skill directory.
 */

const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');

// Change to skill directory for proper module resolution
process.chdir(__dirname);

function checkPlaywrightInstalled() {
  try {
    require.resolve('playwright');
    return true;
  } catch (e) {
    return false;
  }
}

function installPlaywright() {
  console.log('Playwright not found. Installing...');
  try {
    execSync('pnpm install', { stdio: 'inherit', cwd: __dirname });
    execSync('pnpx playwright install chromium', { stdio: 'inherit', cwd: __dirname });
    console.log('Playwright installed successfully');
    return true;
  } catch (e) {
    console.error('Failed to install Playwright:', e.message);
    console.error('Please run manually: cd', __dirname, '&& pnpm run setup');
    return false;
  }
}

function getCodeToExecute() {
  const args = process.argv.slice(2);

  if (args.length > 0 && fs.existsSync(args[0])) {
    const filePath = path.resolve(args[0]);
    console.log(`Executing file: ${filePath}`);
    return fs.readFileSync(filePath, 'utf8');
  }

  if (args.length > 0) {
    console.log('Executing inline code');
    return args.join(' ');
  }

  if (!process.stdin.isTTY) {
    console.log('Reading from stdin');
    return fs.readFileSync(0, 'utf8');
  }

  console.error('No code to execute');
  console.error('Usage:');
  console.error('  node run.js script.js          # Execute file');
  console.error('  node run.js "code here"        # Execute inline');
  console.error('  cat script.js | node run.js    # Execute from stdin');
  process.exit(1);
}

function cleanupOldTempFiles() {
  try {
    const files = fs.readdirSync(__dirname);
    const tempFiles = files.filter(f => f.startsWith('.temp-execution-') && f.endsWith('.js'));
    tempFiles.forEach(file => {
      try { fs.unlinkSync(path.join(__dirname, file)); } catch (e) {}
    });
  } catch (e) {}
}

function wrapCodeIfNeeded(code) {
  const hasRequire = code.includes('require(');
  const hasAsyncIIFE = code.includes('(async () => {') || code.includes('(async()=>{');

  if (hasRequire && hasAsyncIIFE) return code;

  if (!hasRequire) {
    return `
const { chromium, firefox, webkit, devices } = require('playwright');
const helpers = require('./lib/helpers');

const __extraHeaders = helpers.getExtraHeadersFromEnv();

function getContextOptionsWithHeaders(options = {}) {
  if (!__extraHeaders) return options;
  return {
    ...options,
    extraHTTPHeaders: { ...__extraHeaders, ...(options.extraHTTPHeaders || {}) }
  };
}

(async () => {
  try {
    ${code}
  } catch (error) {
    console.error('Automation error:', error.message);
    if (error.stack) console.error(error.stack);
    process.exit(1);
  }
})();
`;
  }

  if (!hasAsyncIIFE) {
    return `
(async () => {
  try {
    ${code}
  } catch (error) {
    console.error('Automation error:', error.message);
    if (error.stack) console.error(error.stack);
    process.exit(1);
  }
})();
`;
  }

  return code;
}

async function main() {
  console.log('Playwright Skill - Universal Executor\n');

  cleanupOldTempFiles();

  if (!checkPlaywrightInstalled()) {
    if (!installPlaywright()) process.exit(1);
  }

  const rawCode = getCodeToExecute();
  const code = wrapCodeIfNeeded(rawCode);
  const tempFile = path.join(__dirname, `.temp-execution-${Date.now()}.js`);

  try {
    fs.writeFileSync(tempFile, code, 'utf8');
    console.log('Starting automation...\n');
    require(tempFile);
  } catch (error) {
    console.error('Execution failed:', error.message);
    if (error.stack) console.error('\nStack trace:', error.stack);
    process.exit(1);
  }
}

main().catch(error => {
  console.error('Fatal error:', error.message);
  process.exit(1);
});
