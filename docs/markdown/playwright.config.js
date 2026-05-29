import { defineConfig, devices } from '@playwright/test';

const PORT = 4173;

export default defineConfig({
  testDir: './tests/e2e',
  fullyParallel: true,
  reporter: 'list',
  use: {
    baseURL: `http://127.0.0.1:${PORT}`,
    trace: 'on-first-retry'
  },
  projects: [
    { name: 'chromium', use: { ...devices['Desktop Chrome'] } }
  ],
  webServer: {
    command: `node tests/static-server.mjs ${PORT}`,
    url: `http://127.0.0.1:${PORT}/converter.html`,
    reuseExistingServer: !process.env.CI,
    timeout: 30000
  }
});
