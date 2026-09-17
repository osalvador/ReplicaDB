import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import test from 'node:test';

const repoRoot = new URL('../..', import.meta.url).pathname;
const workflow = readFileSync(join(repoRoot, '.github/workflows/docs-pages.yml'), 'utf8');
const checksWorkflow = readFileSync(join(repoRoot, '.github/workflows/docs.yml'), 'utf8');
const astroConfiguration = readFileSync(join(repoRoot, 'docs/astro.config.mjs'), 'utf8');
const cutover = readFileSync(join(repoRoot, 'docs/PAGES_CUTOVER.md'), 'utf8');

test('Pages workflow is preview-safe and gated for production deployment', () => {
  assert.match(workflow, /node-version: ['"]22['"]/);
  assert.match(workflow, /npm run check && npm run build && npm run validate && npm test/);
  assert.match(workflow, /actions\/configure-pages@v5/);
  assert.match(workflow, /actions\/upload-pages-artifact@v3/);
  assert.match(workflow, /path:\s*docs\/dist/);
  assert.match(workflow, /actions\/deploy-pages@v4/);
  assert.match(workflow, /needs:\s*build/);
  assert.match(workflow, /DOCS_PAGES_SOURCE == ['"]actions['"]/);
  assert.match(workflow, /event_name != ['"]pull_request['"]/);
  assert.match(workflow, /github-pages/);
  assert.match(workflow, /cancel-in-progress: false/);
  assert.match(checksWorkflow, /java-version:\s*['"]17['"]/);
  assert.match(astroConfiguration, /base:\s*['"]\/ReplicaDB['"]/);
  assert.match(astroConfiguration, /format:\s*['"]directory['"]/);
});

test('cutover checklist preserves the rollback and legacy URL contracts', () => {
  for (const text of ['GitHub Actions', 'github-pages', 'DOCS_PAGES_SOURCE', '/server.html', '/docs/docs.html', '/docs/user-guide.html', '/wizard/', '/markdown/', 'last-known-good']) {
    assert.match(cutover, new RegExp(text.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')), text);
  }
});
