import { expect, test, type Page } from '@playwright/test';

const username = process.env.REPLICADB_BOOTSTRAP_ADMIN_USERNAME;
const password = process.env.REPLICADB_BOOTSTRAP_ADMIN_PASSWORD;
const sourceDatasourceName = 'Develop / PostgreSQL source datasource';

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

test('admin can inspect datasource catalog, detail, editor, and ACL screens', async ({ page }) => {
  await signIn(page);

  await page.getByRole('link', { name: 'Datasources' }).click();
  await expect(page).toHaveURL(/\/datasources$/);
  await expect(page.getByRole('heading', { name: 'Datasources' })).toBeVisible();
  const source = page.getByRole('link', { name: sourceDatasourceName, exact: true });
  await expect(source).toBeVisible();

  await source.click();
  await expect(page).toHaveURL(/\/datasources\/[^/]+$/);
  await expect(page.getByRole('heading', { name: sourceDatasourceName })).toBeVisible();
  await expect(page.getByText(/jdbc:postgresql:\/\/localhost:\d+\/replicadb/)).toBeVisible();
  await expect(page.getByText('Secrets are never returned by the server.')).toBeVisible();
  await page.getByRole('link', { name: 'Edit datasource' }).click();
  await expect(page).toHaveURL(/\/datasources\/[^/]+\/edit$/);
  await expect(page.getByRole('heading', { name: 'Edit datasource' })).toBeVisible();
  await expect(page.getByLabel('Datasource password')).toHaveValue('');
  await page.getByRole('link', { name: 'Back to datasources' }).click();

  await page.getByRole('link', { name: sourceDatasourceName, exact: true }).click();
  await expect(page).toHaveURL(/\/datasources\/[^/]+$/);
  await page.getByRole('link', { name: 'Manage permissions' }).click();
  await expect(page).toHaveURL(/\/datasources\/[^/]+\/permissions$/);
  await expect(page.getByRole('heading', { name: `${sourceDatasourceName} permissions` })).toBeVisible();
  await expect(page.getByRole('button', { name: 'Grant access' })).toBeVisible();
});
