import { expect, test, type Locator, type Page } from '@playwright/test';

const username = process.env.REPLICADB_BOOTSTRAP_ADMIN_USERNAME;
const password = process.env.REPLICADB_BOOTSTRAP_ADMIN_PASSWORD;
const seededJobName = 'Develop / PostgreSQL source';

const viewports = [
  { name: 'desktop', width: 1440, height: 900 },
  { name: 'mobile', width: 390, height: 844 }
] as const;

async function assertNoPageOverflow(page: Page) {
  const fits = await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth + 1);
  expect(fits, 'the document must not overflow the viewport').toBe(true);
}

async function assertInsideViewport(page: Page, locator: Locator) {
  await locator.scrollIntoViewIfNeeded();
  const box = await locator.boundingBox();
  expect(box, 'the inspected element must be visible').not.toBeNull();
  const viewport = page.viewportSize();
  expect(viewport).not.toBeNull();
  expect(box!.x).toBeGreaterThanOrEqual(0);
  expect(box!.y).toBeGreaterThanOrEqual(0);
  expect(box!.x + box!.width).toBeLessThanOrEqual(viewport!.width);
  expect(box!.y + box!.height).toBeLessThanOrEqual(viewport!.height);
}

function overlaps(first: { x: number; y: number; width: number; height: number }, second: { x: number; y: number; width: number; height: number }) {
  return first.x < second.x + second.width && first.x + first.width > second.x
    && first.y < second.y + second.height && first.y + first.height > second.y;
}

async function assertHeaderDoesNotOverlap(page: Page) {
  const brandBox = await page.getByRole('link', { name: 'ReplicaDB' }).boundingBox();
  const identityBox = await page.getByRole('group', { name: 'Signed-in identity' }).boundingBox();
  expect(brandBox).not.toBeNull();
  expect(identityBox).not.toBeNull();
  expect(overlaps(brandBox!, identityBox!)).toBe(false);
}

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

for (const viewport of viewports) {
  test.describe(`${viewport.name} control-plane layout`, () => {
    test.use({ viewport: { width: viewport.width, height: viewport.height } });

    test('keeps the login surface contained', async ({ page }) => {
      expect(username, 'REPLICADB_BOOTSTRAP_ADMIN_USERNAME must be set').toBeTruthy();
      expect(password, 'REPLICADB_BOOTSTRAP_ADMIN_PASSWORD must be set').toBeTruthy();

      await page.goto('/');
      await expect(page).toHaveURL(/\/login$/);
      await expect(page.getByRole('heading', { name: 'Sign in' })).toBeVisible();
      await assertNoPageOverflow(page);
      await assertInsideViewport(page, page.getByRole('form', { name: 'Sign-in form' }));
      await assertInsideViewport(page, page.getByRole('button', { name: 'Sign in' }));
    });

    test('keeps authenticated operational screens contained', async ({ page }) => {
      await enableLocalRunSeeding(page);
      await signIn(page);

      await expect(page.getByRole('heading', { name: 'Dashboard' })).toBeVisible();
      await assertNoPageOverflow(page);
      await assertHeaderDoesNotOverlap(page);
      await assertInsideViewport(page, page.getByRole('banner'));
      await assertInsideViewport(page, page.getByRole('heading', { name: 'Dashboard' }));
      await assertInsideViewport(page, page.getByRole('link', { name: 'New job' }));

      const jobsTable = page.getByRole('table', { name: 'Jobs' });
      const jobsTableContainer = jobsTable.locator('..');
      const tableLayout = await jobsTableContainer.evaluate(element => ({
        clientWidth: element.clientWidth,
        scrollWidth: element.scrollWidth,
        overflowX: getComputedStyle(element).overflowX
      }));
      expect(tableLayout.overflowX).toBe('auto');
      expect(tableLayout.scrollWidth).toBeGreaterThanOrEqual(tableLayout.clientWidth);

      await page.getByRole('link', { name: 'New job' }).click();
      await expect(page).toHaveURL(/\/jobs\/new$/);
      await expect(page.getByRole('heading', { name: 'New job' })).toBeVisible();
      await assertNoPageOverflow(page);
      await assertInsideViewport(page, page.getByRole('heading', { name: 'New job' }));
      await assertInsideViewport(page, page.getByRole('button', { name: 'Create job' }));

      await page.goto('/');
      const seededJob = page.getByRole('link', { name: seededJobName });
      await expect(seededJob, `the seeded job ${seededJobName} must exist`).toBeVisible();
      await seededJob.click();
      await expect(page).toHaveURL(/\/jobs\/[^/]+$/);
      await expect(page.getByRole('heading', { name: seededJobName })).toBeVisible();
      await assertNoPageOverflow(page);
      await assertInsideViewport(page, page.getByRole('heading', { name: seededJobName }));
      await assertInsideViewport(page, page.getByRole('button', { name: 'Trigger run' }));
      await assertInsideViewport(page, page.getByRole('link', { name: 'Edit' }));

      await page.getByRole('link', { name: 'Edit' }).click();
      await expect(page).toHaveURL(/\/jobs\/[^/]+\/edit$/);
      await expect(page.getByRole('heading', { name: 'Edit job' })).toBeVisible();
      await assertNoPageOverflow(page);
      await assertInsideViewport(page, page.getByRole('heading', { name: 'Edit job' }));
      await assertInsideViewport(page, page.getByRole('button', { name: 'Save changes' }));

      await page.goto('/');
      await page.getByRole('link', { name: seededJobName }).click();
      await expect(page).toHaveURL(/\/jobs\/[^/]+$/);
      await page.getByRole('button', { name: 'Trigger run' }).click();
      await page.waitForURL(/\/runs\/[^/]+$/);
      await expect(page.getByRole('heading', { name: 'Run detail' })).toBeVisible();
      await expect(page.getByRole('heading', { name: 'Detailed log' })).toBeVisible();
      await assertNoPageOverflow(page);
      await assertInsideViewport(page, page.getByRole('heading', { name: 'Run detail' }));
      await assertInsideViewport(page, page.getByRole('heading', { name: 'Detailed log' }));

      const logSection = page.locator('section').filter({ has: page.getByRole('heading', { name: 'Detailed log' }) });
      const logContent = logSection.locator('pre');
      if (await logContent.count()) {
        expect(await logContent.evaluate(element => getComputedStyle(element).overflowX)).toBe('auto');
      }
    });
  });
}
