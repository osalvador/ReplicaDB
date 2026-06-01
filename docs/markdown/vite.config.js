import { defineConfig } from 'vite';

export default defineConfig({
  root: '.',
  base: './',
  build: {
    outDir: 'assets',
    emptyOutDir: false,
    sourcemap: false,
    rollupOptions: {
      input: 'converter-app.js',
      output: {
        format: 'es',
        entryFileNames: 'app.js',
        chunkFileNames: 'chunks/[name]-[hash].js',
        assetFileNames: 'chunks/[name]-[hash][extname]'
      }
    }
  }
});
