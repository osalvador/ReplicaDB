import { randomUUID } from 'node:crypto';
import { expect, test, type Page } from '@playwright/test';

const username = process.env.REPLICADB_BOOTSTRAP_ADMIN_USERNAME;
const password = process.env.REPLICADB_BOOTSTRAP_ADMIN_PASSWORD;
const seededJobName = 'Develop / PostgreSQL source';

async function signIn(page: Page) {
  test.skip(!username || !password, 'REPLICADB_BOOTSTRAP_ADMIN_USERNAME and REPLICADB_BOOTSTRAP_ADMIN_PASSWORD must be set');

  await page.goto('/');
  await page.getByRole('link', { name: 'Open jobs' }).click();
  await expect(page).toHaveURL(/\/login$/);
  await page.getByLabel('Username').fill(username!);
  await page.getByLabel('Password').fill(password!);
  await page.getByRole('button', { name: 'Sign in' }).click();
  await expect(page).toHaveURL(/\/$/);
}

test('admin can create a user and grant permissions for a job', async ({ page }) => {
  await signIn(page);

  const managedUsername = `admin-e2e-${Date.now()}`;
  const managedPassword = `admin-e2e-${randomUUID()}`;
  await page.getByRole('link', { name: 'Users' }).click();
  await expect(page).toHaveURL(/\/users$/);
  await page.getByRole('button', { name: 'Create user' }).click();

  const createDialog = page.getByRole('dialog', { name: 'Create user' });
  await createDialog.getByLabel('Username').fill(managedUsername);
  await createDialog.getByLabel('Password').fill(managedPassword);
  await createDialog.getByRole('combobox', { name: 'Role' }).click();
  await page.getByRole('option', { name: 'OPERATOR' }).click();
  await createDialog.getByRole('button', { name: 'Create' }).click();
  await expect(createDialog).not.toBeVisible();
  await expect(page.getByRole('table', { name: 'Users' })).toContainText(managedUsername);

  await page.goto('/');
  const seededJob = page.getByRole('link', { name: seededJobName });
  await expect(seededJob, `the seeded job ${seededJobName} must exist`).toBeVisible();
  await seededJob.click();
  await expect(page.getByRole('heading', { name: seededJobName })).toBeVisible();
  await page.getByRole('link', { name: 'Manage permissions' }).click();
  await expect(page).toHaveURL(/\/jobs\/[^/]+\/permissions$/);

  await page.getByRole('button', { name: 'Grant access' }).click();
  const grantDialog = page.getByRole('dialog', { name: 'Grant job access' });
  await grantDialog.getByRole('combobox', { name: 'User' }).fill(managedUsername);
  await page.getByRole('option', { name: managedUsername }).click();
  await grantDialog.getByRole('checkbox', { name: 'VIEW' }).check();
  await grantDialog.getByRole('checkbox', { name: 'EXECUTE' }).check();
  await grantDialog.getByRole('button', { name: 'Grant' }).click();
  await expect(grantDialog).not.toBeVisible();
  await expect(page.getByRole('table', { name: 'Job permissions' })).toContainText(managedUsername);
});
