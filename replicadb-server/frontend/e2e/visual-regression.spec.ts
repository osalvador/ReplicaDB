import { expect, test } from '@playwright/test';
import { assertDocsSurface, clickSeededJob, enableLocalRunSeeding, settleDocsSurface, signIn, waitForHeading } from './support/docsScreenshotFixtures';

test.describe('exhaustive control-plane visual regression', () => {
  test('dashboard and seeded job detail desktop baseline', async ({ page }) => {
    await enableLocalRunSeeding(page);
    await signIn(page);
    await waitForHeading(page, 'Dashboard');
    await settleDocsSurface(page);
    await assertDocsSurface(page, 'desktop');
    await expect(page).toHaveScreenshot('visual-regression/dashboard-desktop.png', { animations: 'disabled', caret: 'hide' });

    await clickSeededJob(page);
    await waitForHeading(page, /Develop \/ PostgreSQL source/);
    await settleDocsSurface(page);
    await assertDocsSurface(page, 'desktop');
    await expect(page).toHaveScreenshot('visual-regression/job-detail-desktop.png', { animations: 'disabled', caret: 'hide' });
  });

  test('dashboard mobile baseline stays contained', async ({ page }) => {
    await page.setViewportSize({ width: 390, height: 844 });
    await enableLocalRunSeeding(page);
    await signIn(page);
    await waitForHeading(page, 'Dashboard');
    await settleDocsSurface(page);
    await assertDocsSurface(page, 'mobile');
    await expect(page).toHaveScreenshot('visual-regression/dashboard-mobile.png', { animations: 'disabled', caret: 'hide' });
  });
});
