import { expect, test, type Page } from '@playwright/test';

const username = process.env.REPLICADB_BOOTSTRAP_ADMIN_USERNAME;
const password = process.env.REPLICADB_BOOTSTRAP_ADMIN_PASSWORD;
const sourceDatasourceName = 'Develop / PostgreSQL source datasource';
const sinkDatasourceName = 'Develop / PostgreSQL sink datasource';

async function signIn(page: Page) {
  test.skip(!username || !password, 'REPLICADB_BOOTSTRAP_ADMIN_USERNAME and REPLICADB_BOOTSTRAP_ADMIN_PASSWORD must be set');
  await page.goto('/');
  await expect(page).toHaveURL(/\/login$/);
  await page.getByLabel('Username').fill(username!);
  await page.getByLabel('Password').fill(password!);
  await page.getByRole('button', { name: 'Sign in' }).click();
  await expect(page).toHaveURL(/\/$/);
}

async function createTestJob(page: Page, jobName: string) {
  await page.goto('/');
  await page.getByRole('link', { name: 'Open jobs' }).click();
  await expect(page).toHaveURL(/\/jobs$/);
  await page.getByRole('link', { name: 'New job' }).click();
  await expect(page).toHaveURL(/\/jobs\/new$/);
  await page.getByLabel('Name').fill(jobName);
  await page.getByRole('combobox', { name: 'Source datasource' }).fill(sourceDatasourceName);
  await page.getByRole('option', { name: new RegExp(sourceDatasourceName) }).click();
  await page.getByRole('combobox', { name: 'Sink datasource' }).fill(sinkDatasourceName);
  await page.getByRole('option', { name: new RegExp(sinkDatasourceName) }).click();
  await page.getByRole('textbox', { name: 'Table', exact: true }).fill('orders');
  await page.getByRole('textbox', { name: 'Columns', exact: true }).fill('id, payload');
  await page.getByRole('textbox', { name: 'Sink table', exact: true }).fill('orders_copy');
  await page.getByRole('textbox', { name: 'Sink columns', exact: true }).fill('id, payload');
  await page.getByRole('button', { name: 'Create job' }).click();
  await expect(page).toHaveURL(/\/jobs\/[^/]+$/);
}

test('admin can review and delete a job from the catalog', async ({ page }) => {
  await signIn(page);
  const jobName = `Playwright deletion ${Date.now()}`;
  await createTestJob(page, jobName);

  await page.goto('/');
  await page.getByRole('link', { name: 'Open jobs' }).click();
  const jobRow = page.getByRole('row', { name: new RegExp(jobName) });
  await expect(jobRow).toBeVisible();
  await jobRow.getByText('orders', { exact: true }).click();
  await expect(page).toHaveURL(/\/jobs\/[^/]+$/);
  await expect(page.getByRole('heading', { name: jobName })).toBeVisible();

  await page.getByRole('button', { name: 'Delete job' }).click();
  const dialog = page.getByRole('dialog', { name: 'Delete job' });
  await expect(dialog).toContainText(jobName);
  await dialog.getByRole('button', { name: 'Cancel' }).click();
  await expect(dialog).not.toBeVisible();

  await page.getByRole('button', { name: 'Delete job' }).click();
  await page.route('**/api/v1/jobs/*', async route => {
    if (route.request().method() === 'DELETE') {
      await route.fulfill({
        status: 409,
        contentType: 'application/problem+json',
        body: JSON.stringify({ title: 'Conflict', detail: 'This job has an active run.' })
      });
      return;
    }
    await route.continue();
  });
  await page.getByRole('dialog', { name: 'Delete job' }).getByRole('button', { name: 'Delete job' }).click();
  await expect(page.getByRole('dialog', { name: 'Delete job' })).toContainText('This job has an active run.');
  await page.unroute('**/api/v1/jobs/*');

  await page.getByRole('dialog', { name: 'Delete job' }).getByRole('button', { name: 'Delete job' }).click();
  await expect(page).toHaveURL(/\/jobs$/);
  await expect(page.getByRole('row', { name: new RegExp(jobName) })).not.toBeVisible();
});
