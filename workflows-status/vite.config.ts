import { defineConfig } from 'vite'
import { resolve } from 'node:path'

// Relative base so the built app works under any GitHub Pages subpath
// (e.g. https://<owner>.github.io/<repo>/) without per-repo configuration.
// CI overrides this with `--base=/<repo>/workflows-status/` at build time.
export default defineConfig({
  base: './',
  build: {
    rollupOptions: {
      input: {
        main: resolve(__dirname, 'index.html'),
        upcoming: resolve(__dirname, 'upcoming.html'),
      },
    },
  },
})
