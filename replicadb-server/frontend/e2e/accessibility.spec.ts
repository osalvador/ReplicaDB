import { expect, test, type Locator, type Page } from '@playwright/test';

const username = process.env.REPLICADB_BOOTSTRAP_ADMIN_USERNAME;
const password = process.env.REPLICADB_BOOTSTRAP_ADMIN_PASSWORD;
const seededJobName = 'Develop / PostgreSQL source';

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

async function expectFocusedElementVisible(page: Page) {
  const active = page.locator(':focus');
  await expect(active).toHaveCount(1);
  const box = await active.boundingBox();
  expect(box).not.toBeNull();
  expect(box!.width).toBeGreaterThan(0);
  expect(box!.height).toBeGreaterThan(0);
}

async function expectContrast(page: Page, foreground: string, background: string, minimum: number) {
  const ratio = await page.evaluate(({ foreground: first, background: second }) => {
    const toLinear = (channel: number) => channel <= 0.03928 ? channel / 12.92 : ((channel + 0.055) / 1.055) ** 2.4;
    const luminance = (color: string) => {
      const channels = color.match(/\d+(?:\.\d+)?/g)?.map(value => Number(value) / 255) ?? [];
      const linear = channels.map(toLinear);
      return 0.2126 * linear[0] + 0.7152 * linear[1] + 0.0722 * linear[2];
    };
    const firstLuminance = luminance(first);
    const secondLuminance = luminance(second);
    return (Math.max(firstLuminance, secondLuminance) + 0.05)
      / (Math.min(firstLuminance, secondLuminance) + 0.05);
  }, { foreground, background });
  expect(ratio).toBeGreaterThanOrEqual(minimum);
}

test('authenticated controls expose keyboard and semantic accessibility contracts', async ({ page }) => {
  await signIn(page);

  await expect(page.getByRole('banner')).toBeVisible();
  await expect(page.getByRole('group', { name: 'Signed-in identity' })).toBeVisible();
  await expect(page.getByRole('link', { name: 'New job' })).toBeVisible();
  await expect(page.getByRole('table', { name: 'Jobs' })).toBeVisible();

  const focusableControls: Locator[] = [
    page.getByRole('link', { name: 'ReplicaDB' }),
    page.getByRole('button', { name: 'Logout' }),
    page.getByRole('link', { name: 'New job' })
  ];
  for (const control of focusableControls) {
    await control.focus();
    await expectFocusedElementVisible(page);
  }

  await page.getByRole('link', { name: 'New job' }).click();
  await expect(page.getByRole('heading', { name: 'New job' })).toBeVisible();
  await expect(page.getByRole('tab', { name: 'Options' })).toBeVisible();
  await expect(page.getByRole('tab', { name: 'Query' })).toBeVisible();
  await expect(page.getByRole('tab', { name: 'Schema' })).toBeVisible();
  await expect(page.getByRole('tab', { name: 'Table' })).toBeVisible();

  await page.getByRole('tab', { name: 'Query' }).focus();
  await expectFocusedElementVisible(page);
  await page.getByRole('tab', { name: 'Query' }).press('Enter');
  await expect(page.getByRole('tabpanel')).toHaveAttribute('aria-labelledby', /query-tab/);

  await page.getByRole('combobox', { name: 'Source data source type' }).click();
  await page.getByRole('option', { name: 'SQL Server' }).click();
  const disclosure = page.getByRole('button', { name: 'Microsoft Entra Authentication' });
  await expect(disclosure).toHaveAttribute('aria-expanded', 'false');
  await disclosure.click();
  await expect(disclosure).toHaveAttribute('aria-expanded', 'true');
  await expect(page.getByRole('region', { name: 'Microsoft Entra Authentication' })).toBeVisible();

  await expectContrast(page, 'rgb(11, 110, 105)', 'rgb(255, 255, 255)', 4.5);
  await expectContrast(page, 'rgb(177, 92, 56)', 'rgb(255, 255, 255)', 4.5);

  await page.goto('/');
  const seededJob = page.getByRole('link', { name: seededJobName });
  await expect(seededJob, `the seeded job ${seededJobName} must exist`).toBeVisible();
  await seededJob.click();
  await expect(page.getByRole('heading', { name: seededJobName })).toBeVisible();
  await page.getByRole('button', { name: 'Create schedule' }).click();
  await expect(page.getByRole('dialog', { name: 'Create schedule' })).toBeVisible();
  await expect(page.getByRole('textbox', { name: 'Cron expression' })).toBeVisible();
  await page.getByRole('button', { name: 'Cancel' }).click();
  await expect(page.getByRole('dialog', { name: 'Create schedule' })).not.toBeVisible();
});
