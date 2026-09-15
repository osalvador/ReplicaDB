import { readFile } from 'node:fs/promises';
import { expect, test } from '@playwright/test';

const smokeUrl = process.env.REPLICADB_PUBLIC_SMOKE_URL;
const smokeUsername = process.env.REPLICADB_PUBLIC_SMOKE_USERNAME;
const smokePasswordFile = process.env.REPLICADB_PUBLIC_SMOKE_PASSWORD_FILE;

test.describe('public Cloud Run frontend', () => {
  test.skip(!smokeUrl, 'REPLICADB_PUBLIC_SMOKE_URL is not set');
  test.use({ baseURL: smokeUrl });

  test('serves the SPA shell while protecting dashboard data', async ({ page, request }) => {
    await page.goto('/');
    await expect(page).toHaveTitle('ReplicaDB Control Plane');

    const protectedResponse = await request.get('/api/v1/jobs');
    expect(protectedResponse.status()).toBe(401);
    expect(protectedResponse.headers()['content-type']).toContain('application/problem+json');

    await page.goto('/jobs');
    await expect(page).toHaveURL(/\/login$/);
    await expect(page.getByRole('heading', { name: 'Sign in' })).toBeVisible();
    await page.reload();
    await expect(page).toHaveURL(/\/login$/);
  });

  test('supports the opt-in authenticated browser flow', async ({ page }) => {
    test.skip(!smokeUsername || !smokePasswordFile, 'credential smoke inputs are not set');
    const smokePassword = (await readFile(smokePasswordFile!, 'utf8')).trim();

    await page.goto('/login');
    await page.getByLabel('Username').fill(smokeUsername!);
    await page.getByLabel('Password').fill(smokePassword);
    await page.getByRole('button', { name: 'Sign in' }).click();
    await expect(page).toHaveURL(/\/$/);

    await page.goto('/jobs');
    await expect(page).toHaveURL(/\/jobs$/);
  });
});
