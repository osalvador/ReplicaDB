import { defineConfig, devices } from '@playwright/test';

export default defineConfig({
  testDir: './e2e',
  testMatch: '**/{docs-screenshots,visual-regression}.spec.ts',
  fullyParallel: false,
  workers: 1,
  reporter: 'html',
  use: {
    baseURL: process.env.PLAYWRIGHT_BASE_URL ?? 'http://localhost:15173',
    channel: process.env.PLAYWRIGHT_CHANNEL ?? 'chrome',
    trace: 'retain-on-failure',
    locale: 'en-US',
    timezoneId: 'UTC',
    colorScheme: 'light',
    deviceScaleFactor: 1,
    reducedMotion: 'reduce'
  },
  projects: [{ name: 'docs-chromium', use: { ...devices['Desktop Chrome'] } }]
});