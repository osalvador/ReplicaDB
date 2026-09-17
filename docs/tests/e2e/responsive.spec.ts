import { expect, test } from '@playwright/test';

test('representative pages stay contained at desktop and mobile widths', async ({ page }) => {
  for (const route of ['./', './server/dashboard/', './server/datasources/', './architecture/overview/', './api/']) {
    await page.goto(route);
    const size = page.viewportSize();
    const scrollWidth = await page.evaluate(() => document.documentElement.scrollWidth);
    expect(scrollWidth, `${route} overflow`).toBeLessThanOrEqual(size!.width);
    await expect(page.locator('main')).toBeVisible();
  }
});