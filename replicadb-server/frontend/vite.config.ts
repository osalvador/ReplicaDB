import { defineConfig } from 'vitest/config';
import react from '@vitejs/plugin-react';

export default defineConfig({
  plugins: [react()],
  build: {
    outDir: '../src/main/resources/static',
    emptyOutDir: true
  },
  server: {
    proxy: {
      '/api/v1': {
        target: 'http://localhost:8080',
        changeOrigin: true
      },
      '/v3/api-docs': {
        target: 'http://localhost:8080',
        changeOrigin: true
      }
    }
  },
  test: {
    environment: 'jsdom',
    setupFiles: './src/test/setup.ts',
    include: ['src/**/*.{test,spec}.?(c|m)[jt]s?(x)', 'vite.config.test.ts'],
    exclude: ['**/node_modules/**', '**/dist/**']
  }
});
