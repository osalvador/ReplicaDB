import { expect, test, type Page } from '@playwright/test';

const username = process.env.REPLICADB_BOOTSTRAP_ADMIN_USERNAME;
const password = process.env.REPLICADB_BOOTSTRAP_ADMIN_PASSWORD;
const seededJobName = 'Develop / PostgreSQL source';

const viewports = [
  { name: 'desktop', width: 1440, height: 900 },
  { name: 'mobile', width: 390, height: 844 }
] as const;

async function signIn(page: Page) {
  expect(username, 'REPLICADB_BOOTSTRAP_ADMIN_USERNAME must be set').toBeTruthy();
  expect(password, 'REPLICADB_BOOTSTRAP_ADMIN_PASSWORD must be set').toBeTruthy();

  await page.goto('/');
  await expect(page).toHaveURL(/\/login$/);
  await page.getByLabel('Username').fill(username!);
  await page.getByLabel('Password').fill(password!);
  await page.getByRole('button', { name: 'Sign in' }).click();
  await expect(page).toHaveURL(/\/$/);
}

async function enableLocalRunSeeding(page: Page) {
  await page.route('**/api/v1/jobs/*/runs**', async route => {
    if (route.request().method() !== 'POST') {
      await route.continue();
      return;
    }

    await route.continue({
      headers: {
        ...route.request().headers(),
        'x-replicadb-local-seed': 'true'
      }
    });
  });
}

async function expectContained(page: Page) {
  const viewport = page.viewportSize();
  const scrollWidth = await page.evaluate(() => document.documentElement.scrollWidth);
  expect(scrollWidth).toBeLessThanOrEqual(viewport!.width);
}

for (const viewport of viewports) {
  test.describe(`${viewport.name} control-plane smoke`, () => {
    test.use({ viewport: { width: viewport.width, height: viewport.height } });

    test('covers the seeded operational flow', async ({ page }) => {
      await enableLocalRunSeeding(page);
      await signIn(page);

      await expect(page.getByRole('heading', { name: 'Dashboard' })).toBeVisible();
      await expect(page.getByRole('link', { name: 'Open jobs' })).toBeVisible();
      await expect(page.getByText(/Complete mode clears the sink/)).not.toBeVisible();
      await expectContained(page);

      await page.getByRole('link', { name: 'Open jobs' }).click();
      await expect(page).toHaveURL(/\/jobs$/);
      await expect(page.getByRole('link', { name: 'New job' })).toBeVisible();
      await page.getByRole('link', { name: 'New job' }).click();
      await expect(page).toHaveURL(/\/jobs\/new$/);
      await expect(page.getByRole('heading', { name: 'New job' })).toBeVisible();
      await expect(page.getByRole('heading', { name: 'Basics' })).toBeVisible();
      await expect(page.getByRole('heading', { name: 'Source', exact: true })).toBeVisible();
      await expect(page.getByRole('heading', { name: 'Sink', exact: true })).toBeVisible();
      await expect(page.getByRole('button', { name: 'Create job' })).toBeVisible();
      await expectContained(page);

      await page.goto('/');
      await page.getByRole('link', { name: 'Open jobs' }).click();
      const seededJob = page.getByRole('link', { name: seededJobName });
      await expect(seededJob, `the seeded job ${seededJobName} must exist`).toBeVisible();
      await seededJob.click();

      await expect(page).toHaveURL(/\/jobs\/[^/]+$/);
      await expect(page.getByRole('heading', { name: seededJobName })).toBeVisible();
      await expect(page.getByRole('heading', { name: 'Source', exact: true })).toBeVisible();
      await expect(page.getByRole('heading', { name: 'Sink', exact: true })).toBeVisible();
      await expect(page.getByRole('heading', { name: 'Execution' })).toBeVisible();
      await expect(page.getByRole('heading', { name: 'Lifecycle' })).toBeVisible();
      await expect(page.getByRole('alert').filter({ hasText: 'Complete mode clears the sink' }))
        .toContainText('Complete mode clears the sink');
      await expectContained(page);

      await page.getByRole('link', { name: 'Edit' }).click();
      await expect(page).toHaveURL(/\/jobs\/[^/]+\/edit$/);
      await expect(page.getByRole('heading', { name: 'Edit job' })).toBeVisible();
      await expect(page.getByRole('button', { name: 'Save changes' })).toBeVisible();
      await expectContained(page);

      await page.goto('/');
      await page.getByRole('link', { name: 'Open jobs' }).click();
      await page.getByRole('link', { name: seededJobName }).click();
      await expect(page).toHaveURL(/\/jobs\/[^/]+$/);
      await page.getByRole('button', { name: 'Trigger run' }).click();
      await page.waitForURL(/\/runs\/[^/]+$/);
      await expect(page.getByRole('heading', { name: 'Run detail' })).toBeVisible();
      await expect(page.getByRole('heading', { name: 'Run metrics' })).toBeVisible();
      await expect(page.getByRole('heading', { name: 'Detailed log' })).toBeVisible();
      await expect(page.getByRole('status', { name: /Run status:/ })).toBeVisible();
      await expectContained(page);

      await page.getByRole('link', { name: 'Back to job' }).click();
      await expect(page).toHaveURL(/\/jobs\/[^/]+$/);
      await page.getByRole('link', { name: 'Back to jobs' }).click();
      await expect(page).toHaveURL(/\/$/);
      await expect(page.getByRole('heading', { name: 'Dashboard' })).toBeVisible();
    });
  });
}
