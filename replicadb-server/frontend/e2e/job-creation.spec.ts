import { expect, test } from '@playwright/test';

const username = process.env.REPLICADB_BOOTSTRAP_ADMIN_USERNAME;
const password = process.env.REPLICADB_BOOTSTRAP_ADMIN_PASSWORD;

test('admin can create a PostgreSQL job from datasource selectors', async ({ page }) => {
  expect(username, 'REPLICADB_BOOTSTRAP_ADMIN_USERNAME must be set').toBeTruthy();
  expect(password, 'REPLICADB_BOOTSTRAP_ADMIN_PASSWORD must be set').toBeTruthy();

  const jobName = `Playwright job ${Date.now()}`;
  await page.goto('/');
  await expect(page).toHaveURL(/\/login$/);
  await page.getByLabel('Username').fill(username!);
  await page.getByLabel('Password').fill(password!);
  await page.getByRole('button', { name: 'Sign in' }).click();

  await expect(page).toHaveURL(/\/$/);
  await page.getByRole('link', { name: 'New job' }).click();
  await expect(page).toHaveURL(/\/jobs\/new$/);

  await page.getByLabel('Name').fill(jobName);
  await page.getByRole('combobox', { name: 'Source datasource' }).fill('Develop / PostgreSQL source datasource');
  await page.getByRole('option', { name: /Develop \/ PostgreSQL source datasource/ }).click();
  await page.getByRole('combobox', { name: 'Sink datasource' }).fill('Develop / PostgreSQL sink datasource');
  await page.getByRole('option', { name: /Develop \/ PostgreSQL sink datasource/ }).click();
  await page.getByRole('textbox', { name: 'Table', exact: true }).fill('orders');
  await page.getByRole('textbox', { name: 'Columns', exact: true }).fill('id, payload');
  await page.getByRole('textbox', { name: 'Sink table', exact: true }).fill('orders_copy');
  await page.getByRole('textbox', { name: 'Sink columns', exact: true }).fill('id, payload');
  await page.getByRole('button', { name: 'Create job' }).click();

  await expect(page).toHaveURL(/\/jobs\/[^/]+$/);
  await expect(page.getByRole('heading', { name: jobName })).toBeVisible();
  await expect(page.getByText('orders', { exact: true })).toBeVisible();
  await expect(page.getByText('orders_copy')).toBeVisible();
  await expect(page.getByText('id, payload').first()).toBeVisible();
});
