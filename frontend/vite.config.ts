import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  build: {
    outDir: 'dist',
    sourcemap: true,
  },
  esbuild: {
    supported: {
      'top-level-await': true,
    },
  },
  server: {
    headers: {
      'Content-Security-Policy': "script-src 'self' 'unsafe-inline'; object-src 'none';",
    },
  },
})
