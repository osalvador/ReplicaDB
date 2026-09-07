import { expect, type Locator, type Page } from '@playwright/test';

export const DOCS_CAPTURE_PREFIX = 'Docs /';
export const SEEDED_JOB_NAME = 'Develop / PostgreSQL source';
export const SEEDED_DATASOURCE_NAME = 'Develop / PostgreSQL source datasource';

export const FIXED_IDS = {
  runningRun: '00000000-0000-4000-8000-000000000101',
  failedRun: '00000000-0000-4000-8000-000000000102',
  conflictJob: '00000000-0000-4000-8000-000000000103'
} as const;

export type DocsViewport = 'desktop' | 'mobile';

export type DocsScreenshotDefinition = {
  key: string;
  filename: string;
  viewport: DocsViewport;
  state: string;
  alt: string;
  caption: string;
  guide: string;
};

export const CURATED_SCREENSHOTS: DocsScreenshotDefinition[] = [
  ['login', 'login.png', 'desktop', 'empty sign-in form', 'ReplicaDB sign-in form', 'The authenticated entry point for the managed control plane.', 'server/sign-in-and-profile'],
  ['dashboard', 'dashboard.png', 'desktop', 'seeded operational summary', 'ReplicaDB dashboard with run metrics', 'The dashboard summarizes jobs, active runs, outcomes, rows, duration, and queue latency.', 'server/dashboard'],
  ['profile', 'profile.png', 'desktop', 'authenticated profile', 'ReplicaDB profile page', 'The profile page shows the current identity and role without offering secret self-service.', 'server/sign-in-and-profile'],
  ['jobs', 'jobs.png', 'desktop', 'seeded job catalog', 'ReplicaDB jobs catalog', 'The jobs catalog lists definitions available to the signed-in operator.', 'server/jobs'],
  ['job-new', 'job-new.png', 'desktop', 'new job form', 'ReplicaDB new job form', 'A new job binds datasources and defines the replication contract.', 'server/jobs'],
  ['job-detail', 'job-detail.png', 'desktop', 'complete-mode warning and run history', 'ReplicaDB job detail with complete-mode warning', 'Job detail keeps the destructive complete-mode warning beside the actions it affects.', 'server/jobs'],
  ['job-edit', 'job-edit.png', 'desktop', 'edit job form', 'ReplicaDB edit job form', 'The edit form preserves datasource references and retry policy controls.', 'server/jobs'],
  ['run-detail', 'run-detail.png', 'desktop', 'terminal run diagnostics', 'ReplicaDB run detail and bounded diagnostics', 'Run detail shows status, metrics, and bounded operational logs.', 'server/runs-and-diagnostics'],
  ['datasources', 'datasources.png', 'desktop', 'datasource catalog', 'ReplicaDB datasource catalog', 'Datasource profiles expose safe metadata and permission flags.', 'server/datasources'],
  ['datasource-new', 'datasource-new.png', 'desktop', 'new datasource form', 'ReplicaDB new datasource form', 'Datasource creation separates technical settings from protected security values.', 'server/datasources'],
  ['datasource-detail', 'datasource-detail.png', 'desktop', 'redacted datasource detail', 'ReplicaDB datasource detail with redacted connection', 'Datasource detail never rehydrates stored credentials.', 'server/datasources'],
  ['datasource-edit', 'datasource-edit.png', 'desktop', 'secret-preserving edit form', 'ReplicaDB edit datasource form', 'Blank security fields preserve encrypted values until explicitly cleared.', 'server/datasources'],
  ['datasource-permissions', 'datasource-permissions.png', 'desktop', 'grant dialog', 'ReplicaDB datasource permissions', 'Admins grant VIEW, USE, and EDIT resource permissions.', 'server/permissions'],
  ['job-permissions', 'job-permissions.png', 'desktop', 'job permission matrix', 'ReplicaDB job permissions', 'Admins grant VIEW, EDIT, EXECUTE, and CANCEL permissions.', 'server/permissions'],
  ['users', 'users.png', 'desktop', 'admin user management', 'ReplicaDB user management', 'Admins manage roles, enabled state, and password resets.', 'server/users'],
  ['audit', 'audit.png', 'desktop', 'filtered audit history', 'ReplicaDB audit history', 'Admins filter and inspect durable audit events.', 'server/audit'],
  ['unauthorized', 'unauthorized.png', 'desktop', 'unauthorized route state', 'ReplicaDB unauthorized page', 'Route visibility does not replace backend authorization.', 'server/errors-and-empty-states'],
  ['login-mobile', 'login-mobile.png', 'mobile', 'mobile sign-in layout', 'ReplicaDB mobile sign-in form', 'The sign-in flow remains readable on a narrow viewport.', 'server/sign-in-and-profile'],
  ['dashboard-mobile', 'dashboard-mobile.png', 'mobile', 'mobile dashboard layout', 'ReplicaDB mobile dashboard', 'Dashboard metrics stack without horizontal overflow on mobile.', 'server/dashboard'],
  ['jobs-mobile', 'jobs-mobile.png', 'mobile', 'mobile job catalog', 'ReplicaDB mobile jobs catalog', 'The jobs catalog remains usable on a narrow viewport.', 'server/jobs'],
  ['datasources-mobile', 'datasources-mobile.png', 'mobile', 'mobile datasource catalog', 'ReplicaDB mobile datasource catalog', 'Datasource rows remain contained on a narrow viewport.', 'server/datasources'],
  ['run-detail-mobile', 'run-detail-mobile.png', 'mobile', 'mobile run diagnostics', 'ReplicaDB mobile run detail', 'Run diagnostics remain inspectable on mobile.', 'server/runs-and-diagnostics']
].map(([key, filename, viewport, state, alt, caption, guide]) => ({ key, filename, viewport: viewport as DocsViewport, state, alt, caption, guide }));

export const REDACTION_PATTERNS = [
  /bootstrap\s*(?:admin|password|username)?\s*[:=]/i,
  /(?:password|secret|access.?key|private.?key|lease.?token|encrypted.?security)\s*[:=]/i,
  /jdbc:[^\s"']+:\/\/[^\s"']*:[^\s"']*@/i
];

export async function assertDocsSurface(page: Page, viewport: DocsViewport) {
  const size = page.viewportSize();
  expect(size).toBeTruthy();
  await expect.poll(() => page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(size!.width);
  const visibleText = await page.locator('body').innerText();
  for (const pattern of REDACTION_PATTERNS) expect(visibleText).not.toMatch(pattern);

  const regions = page.locator('[role="main"], main, header, nav, [role="dialog"]');
  const boxes = await regions.evaluateAll((elements, viewportWidth) => elements.map(element => {
    const box = element.getBoundingClientRect();
    return { left: box.left, top: box.top, right: box.right, bottom: box.bottom, label: element.getAttribute('aria-label') ?? element.tagName };
  }).filter(box => box.right > 0 && box.left < viewportWidth), size!.width);
  for (const box of boxes) {
    expect(box.left, `${box.label} left edge`).toBeGreaterThanOrEqual(-1);
    expect(box.right, `${box.label} right edge`).toBeLessThanOrEqual(size!.width + 1);
  }

  return visibleText;
}

export async function signIn(page: Page) {
  const username = process.env.REPLICADB_BOOTSTRAP_ADMIN_USERNAME;
  const password = process.env.REPLICADB_BOOTSTRAP_ADMIN_PASSWORD;
  expect(username, 'REPLICADB_BOOTSTRAP_ADMIN_USERNAME must be set').toBeTruthy();
  expect(password, 'REPLICADB_BOOTSTRAP_ADMIN_PASSWORD must be set').toBeTruthy();
  await page.goto('/');
  await expect(page).toHaveURL(/\/login$/);
  await page.getByLabel('Username').fill(username!);
  await page.getByLabel('Password').fill(password!);
  await page.getByRole('button', { name: 'Sign in' }).click();
  await expect(page).toHaveURL(/\/$/);
}

export async function enableLocalRunSeeding(page: Page) {
  await page.route('**/api/v1/jobs/*/runs**', async route => {
    if (route.request().method() === 'POST') {
      await route.continue({ headers: { ...route.request().headers(), 'x-replicadb-local-seed': 'true' } });
    } else {
      await route.continue();
    }
  });
}

export async function clickSeededJob(page: Page): Promise<string> {
  await page.goto('/jobs');
  const job = page.getByRole('link', { name: SEEDED_JOB_NAME });
  await expect(job).toBeVisible();
  const href = await job.getAttribute('href');
  expect(href).toMatch(/^\/jobs\//);
  await job.click();
  await expect(page).toHaveURL(/\/jobs\/[^/]+$/);
  return href!.replace(/^\/jobs\//, '');
}

export async function clickSeededDatasource(page: Page): Promise<string> {
  await page.goto('/datasources');
  const datasource = page.getByRole('link', { name: SEEDED_DATASOURCE_NAME });
  await expect(datasource).toBeVisible();
  const href = await datasource.getAttribute('href');
  expect(href).toMatch(/^\/datasources\//);
  await datasource.click();
  await expect(page).toHaveURL(/\/datasources\/[^/]+$/);
  return href!.replace(/^\/datasources\//, '');
}

export async function waitForHeading(page: Page, name: string | RegExp) {
  await expect(page.getByRole('heading', { name }).first()).toBeVisible();
}

export async function settleDocsSurface(page: Page) {
  await page.evaluate(async () => {
    await document.fonts.ready;
    const activeElement = document.activeElement;
    if (activeElement instanceof HTMLElement) activeElement.blur();
    document.documentElement.getBoundingClientRect();
    document.querySelectorAll('*').forEach(element => {
      const node = element as HTMLElement;
      node.style.setProperty('animation-duration', '0s', 'important');
      node.style.setProperty('transition-duration', '0s', 'important');
    });
  });
  await page.waitForTimeout(1000);
}

export function firstVisible(locator: Locator) {
  return locator.filter({ visible: true }).first();
}