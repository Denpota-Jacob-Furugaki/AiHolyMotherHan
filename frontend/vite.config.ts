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
      'Content-Security-Policy': [
        "script-src 'self' 'unsafe-inline' https://accounts.google.com https://apis.google.com",
        "style-src 'self' 'unsafe-inline' https://accounts.google.com https://fonts.googleapis.com",
        "font-src 'self' https://fonts.gstatic.com",
        "img-src 'self' data: https://lh3.googleusercontent.com",
        "connect-src 'self' https://accounts.google.com https://*.execute-api.ap-northeast-1.amazonaws.com",
        "frame-src https://accounts.google.com",
        "object-src 'none'",
      ].join('; '),
    },
  },
})
