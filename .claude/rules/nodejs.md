---
paths:
  - "**/*.js"
  - "**/*.ts"
  - "**/*.tsx"
  - "**/*.jsx"
  - "**/package.json"
---
# Node.js & pnpm Rules

- **Always use pnpm** — never npm or yarn. pnpm is available via `pixi run pnpm`
- Install packages: `pixi run pnpm install` or `pixi run pnpm add <pkg>`
- Run scripts: `pixi run pnpm run <script>`
- Execute binaries: `pixi run pnpx <command>`
- Node.js is provided by pixi: `pixi run node`

## Workspace Context
- From root: `pixi run pnpm ...` uses root Node.js/pnpm
- From a workspace: `cd <workspace> && pixi run pnpm ...` uses that workspace's deps
- From root targeting workspace: `pixi run --manifest-path <workspace>/pixi.toml pnpm ...`

## Playwright Skill
- Skill directory: `.claude/skills/playwright-skill/`
- Setup: `cd .claude/skills/playwright-skill && pixi run pnpm run setup`
- Execute: `cd .claude/skills/playwright-skill && pixi run node run.js ../../.tmp/test.js`
- Temp files go in `.tmp/` (gitignored, cross-platform)
