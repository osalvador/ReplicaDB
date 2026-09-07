import { expect, test } from '@playwright/test';
import { createHash } from 'node:crypto';
import { mkdir, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { PNG } from 'pngjs';
import { CURATED_SCREENSHOTS, assertDocsSurface, clickSeededDatasource, clickSeededJob, enableLocalRunSeeding, settleDocsSurface, signIn, waitForHeading } from './support/docsScreenshotFixtures';

const outputDir = join(process.cwd(), '../../docs/src/assets/screenshots/server');

test.describe('curated documentation screenshots', () => {
  test.beforeAll(async () => {
    await mkdir(outputDir, { recursive: true });
  });

  for (const definition of CURATED_SCREENSHOTS) {
    test(`${definition.key} is deterministic and redacted`, async ({ page }) => {
      const viewport = definition.viewport === 'mobile' ? { width: 390, height: 844 } : { width: 1440, height: 900 };
      await page.setViewportSize(viewport);
      await page.emulateMedia({ reducedMotion: 'reduce', colorScheme: 'light' });
      await page.clock.install({ time: new Date('2026-01-01T00:00:00.000Z') });
      await mkdir(outputDir, { recursive: true });

      if (definition.key === 'login' || definition.key === 'login-mobile') {
        await page.goto('/login');
        await waitForHeading(page, 'Sign in');
      } else {
        await enableLocalRunSeeding(page);
        await signIn(page);
        if (definition.key.startsWith('dashboard')) {
          await page.goto('/');
          await waitForHeading(page, 'Dashboard');
        } else if (definition.key === 'profile') {
          await page.goto('/profile');
          await waitForHeading(page, 'My profile');
        } else if (definition.key === 'jobs') {
          await page.goto('/jobs');
          await waitForHeading(page, 'Jobs');
        } else if (definition.key === 'job-new') {
          await page.goto('/jobs/new');
          await waitForHeading(page, 'New job');
        } else if (definition.key === 'job-detail' || definition.key === 'job-edit') {
          await clickSeededJob(page);
          if (definition.key === 'job-edit') {
            await page.getByRole('link', { name: 'Edit' }).click();
            await waitForHeading(page, 'Edit job');
          } else {
            await waitForHeading(page, /Develop \/ PostgreSQL source/);
          }
        } else if (definition.key.startsWith('run-detail')) {
          await clickSeededJob(page);
          await page.getByRole('button', { name: 'Trigger run' }).click();
          await page.waitForURL(/\/runs\/[^/]+$/);
          await waitForHeading(page, 'Run detail');
        } else if (definition.key === 'datasources') {
          await page.goto('/datasources');
          await waitForHeading(page, 'Datasources');
        } else if (definition.key === 'jobs-mobile') {
          await page.goto('/jobs');
          await waitForHeading(page, 'Jobs');
        } else if (definition.key === 'datasources-mobile') {
          await page.goto('/datasources');
          await waitForHeading(page, 'Datasources');
        } else if (definition.key === 'datasource-new') {
          await page.goto('/datasources/new');
          await waitForHeading(page, 'New datasource');
        } else if (definition.key === 'datasource-detail' || definition.key === 'datasource-edit') {
          await clickSeededDatasource(page);
          if (definition.key === 'datasource-edit') {
            await page.getByRole('link', { name: 'Edit datasource' }).click();
            await waitForHeading(page, 'Edit datasource');
          }
        } else if (definition.key === 'datasource-permissions') {
          const datasourceId = await clickSeededDatasource(page);
          await page.goto(`/datasources/${datasourceId}/permissions`);
          await waitForHeading(page, /permissions/);
        } else if (definition.key === 'job-permissions') {
          const jobId = await clickSeededJob(page);
          await page.goto(`/jobs/${jobId}/permissions`);
          await waitForHeading(page, /permissions/);
        } else if (definition.key === 'unauthorized') {
          await page.route('**/api/v1/auth/me', async route => {
            await route.fulfill({
              status: 200,
              contentType: 'application/json',
              body: JSON.stringify({ id: 'docs-viewer', username: 'docs-viewer', role: 'VIEWER' })
            });
          });
          await page.reload();
          await page.goto('/users');
          await waitForHeading(page, /not authorized|permission/i);
        } else if (definition.key.startsWith('datasources')) {
          await clickSeededDatasource(page);
        } else if (definition.key === 'users') {
          await page.goto('/users');
          await waitForHeading(page, 'Users');
        } else if (definition.key === 'audit') {
          await page.goto('/audit');
          await page.getByRole('button', { name: 'Apply filters' }).click();
        }
      }

      await settleDocsSurface(page);
      await assertDocsSurface(page, definition.viewport);
      const target = join(outputDir, definition.filename);
      const first = await page.screenshot({
        animations: 'disabled',
        caret: 'hide',
        scale: 'css'
      });
      const second = await page.screenshot({
        animations: 'disabled',
        caret: 'hide',
        scale: 'css'
      });
      const canonicalize = (buffer: Buffer) => {
        const image = PNG.sync.read(buffer);
        for (let index = 0; index < image.data.length; index += 4) {
          const maximum = Math.max(image.data[index], image.data[index + 1], image.data[index + 2]);
          const minimum = Math.min(image.data[index], image.data[index + 1], image.data[index + 2]);
          if (maximum - minimum <= 10 && maximum >= 160 && maximum <= 250) {
            const value = Math.min(255, Math.round(((image.data[index] + image.data[index + 1] + image.data[index + 2]) / 3) / 32) * 32);
            image.data[index] = value;
            image.data[index + 1] = value;
            image.data[index + 2] = value;
          }
        }
        return PNG.sync.write(image, { colorType: 6 });
      };
      const canonicalFirst = canonicalize(first);
      const canonicalSecond = canonicalize(second);
      const digest = (buffer: Buffer) => createHash('sha256').update(buffer).digest('hex');
      expect(digest(canonicalFirst), `${definition.key} must be deterministic`).toBe(digest(canonicalSecond));
      await writeFile(target, canonicalFirst);
    });
  }
});