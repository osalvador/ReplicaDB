import { defineConfig, devices } from '@playwright/test';

export default defineConfig({
  testDir: './tests/e2e',
  fullyParallel: true,
  reporter: 'html',
  webServer: { command: 'node scripts/serve-dist.mjs', url: 'http://127.0.0.1:4177/ReplicaDB/', reuseExistingServer: true, timeout: 30_000 },
  use: { baseURL: 'http://127.0.0.1:4177/ReplicaDB/', channel: process.env.PLAYWRIGHT_CHANNEL ?? 'chrome', locale: 'en-US', timezoneId: 'UTC', colorScheme: 'light', deviceScaleFactor: 1, trace: 'retain-on-failure' },
  projects: [
    { name: 'desktop', use: { ...devices['Desktop Chrome'], channel: process.env.PLAYWRIGHT_CHANNEL ?? 'chrome', viewport: { width: 1440, height: 900 } } },
    { name: 'mobile', use: { ...devices['Desktop Chrome'], channel: process.env.PLAYWRIGHT_CHANNEL ?? 'chrome', isMobile: true, viewport: { width: 390, height: 844 } } }
  ]
});