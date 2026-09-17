import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { join } from 'node:path';
import test from 'node:test';
import genericConfig from '../playwright.config.ts';
import docsConfig from '../playwright.docs.config.ts';

const frontendRoot = new URL('..', import.meta.url).pathname;
const playwright = join(frontendRoot, 'node_modules/.bin/playwright');

/** @param {string} config */
function discoveredTests(config) {
  return execFileSync(playwright, ['test', '--list', `--config=${config}`], {
    cwd: frontendRoot,
    encoding: 'utf8'
  });
}

test('generic and documentation Playwright configs partition test discovery', () => {
  assert.deepEqual(genericConfig.testIgnore, ['**/docs-screenshots.spec.ts', '**/visual-regression.spec.ts']);
  assert.equal(docsConfig.testMatch, '**/{docs-screenshots,visual-regression}.spec.ts');
  assert.equal(genericConfig.fullyParallel, true);
  assert.equal(docsConfig.workers, 1);
  assert.equal(docsConfig.use?.locale, 'en-US');
  assert.equal(docsConfig.use?.timezoneId, 'UTC');

  const generic = discoveredTests('playwright.config.ts');
  assert.match(generic, /login\.spec\.ts/);
  assert.doesNotMatch(generic, /docs-screenshots\.spec\.ts|visual-regression\.spec\.ts/);

  const docs = discoveredTests('playwright.docs.config.ts');
  assert.match(docs, /docs-screenshots\.spec\.ts/);
  assert.match(docs, /visual-regression\.spec\.ts/);
  assert.doesNotMatch(docs, /login\.spec\.ts|datasource-management\.spec\.ts/);
});
