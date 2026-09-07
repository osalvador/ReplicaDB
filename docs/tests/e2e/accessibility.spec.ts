import { expect, test } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';

test('homepage and API reference have no serious or critical accessibility violations', async ({ page }) => {
  for (const route of ['./', './api/', './server/dashboard/']) {
    await page.goto(route);
    const results = await new AxeBuilder({ page }).withTags(['wcag2a', 'wcag2aa', 'wcag22aa']).analyze();
    expect(results.violations.filter((violation) => ['serious', 'critical'].includes(violation.impact ?? ''))).toEqual([]);
  }
});