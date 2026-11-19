import { expect, test } from '@playwright/test';

test('both quickstarts lead from the homepage to the shared orders example', async ({ page }) => {
  await page.goto('./');
  const main = page.getByRole('main');
  const diagram = main.getByRole('img', { name: /ReplicaDB bulk-loading/ });
  await expect(diagram).toBeVisible();
  await expect.poll(() => diagram.evaluate((image: HTMLImageElement) => image.naturalWidth)).toBeGreaterThan(0);

  for (const title of ['CLI quickstart', 'Server quickstart']) {
    await page.goto('./');
    await main.getByRole('link', { name: title, exact: true }).click();
    await expect(main.getByRole('heading', { name: title })).toBeVisible();
    await expect(main.getByRole('row', { name: '1 pending 10' })).toBeVisible();
    await main.getByRole('link', { name: 'Sample data', exact: true }).click();
    await expect(main.getByRole('heading', { name: 'Create the tables' })).toBeVisible();
    await page.goBack();
    await main.getByRole('link', { name: 'replication modes', exact: true }).click();
    await expect(main.getByRole('row', { name: '9 archived 8' })).toBeVisible();
  }
});

test('search, theme, keyboard navigation, generated API, tools, and 404 work', async ({ page }) => {
  await page.goto('./');
  await expect(page.locator('main')).toBeVisible();
  await page.getByRole('button', { name: /search/i }).first().click();
  await page.getByPlaceholder(/search/i).fill('datasource');
  await expect(page.locator('body')).toContainText(/datasource/i);
  const theme = page.getByRole('button', { name: /theme/i }).first();
  if (await theme.count()) await theme.click();
  await page.keyboard.press('Tab');
  await expect(page.locator(':focus')).toBeVisible();
  await page.goto('./api/');
  await expect(page.locator('main')).toContainText(/OpenAPI|API/i);
  await page.goto('./api-introduction/');
  await expect(page.getByRole('heading', { name: 'Establish a session' })).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Trigger a run idempotently' })).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Handle responses and problems' })).toBeVisible();
  await expect(page.locator('main form')).toHaveCount(0);
  for (const route of [
    './api/operations/tags/authentication/',
    './api/operations/tags/jobs/',
    './api/operations/tags/runs/',
    './api/operations/login/',
    './api/operations/triggerjobrun/'
  ]) {
    await page.goto(route);
    await expect(page.locator('main')).toBeVisible();
  }
  await page.goto('./wizard/index.html', { waitUntil: 'commit', timeout: 10_000 });
  await expect(page).toHaveTitle(/Configuration Wizard|ReplicaDB/i);
  await page.goto('./markdown/converter.html', { waitUntil: 'commit', timeout: 10_000 });
  await expect(page).toHaveTitle(/Markup Forge|Markdown/i);
  const notFound = await page.goto('./missing-route/');
  expect(notFound?.status()).toBe(404);
  await expect(page.locator('main')).toContainText(/not found|404/i);
});
