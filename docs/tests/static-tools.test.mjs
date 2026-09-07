import assert from 'node:assert/strict';
import { existsSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import test from 'node:test';
import { assertRelativeAssetReference, markdownFiles, wizardDirectories, wizardFiles } from '../scripts/stage-static-tools.mjs';

const docsRoot = new URL('..', import.meta.url).pathname;
const distRoot = join(docsRoot, 'dist');

test('accepts relative runtime references and rejects root or URL references', () => {
  assert.equal(assertRelativeAssetReference('./assets/app.js'), './assets/app.js');
  assert.throws(() => assertRelativeAssetReference('/assets/app.js'), /must be relative/);
  assert.throws(() => assertRelativeAssetReference('https://example.test/app.js'), /must be relative/);
});

test('stages only the allowlisted runtime files and preserves relative tool contracts', { skip: !existsSync(distRoot) }, () => {
  for (const path of wizardFiles) assert.ok(existsSync(join(distRoot, 'wizard', path)), path);
  for (const path of wizardDirectories) assert.ok(existsSync(join(distRoot, 'wizard', path)), path);
  for (const path of markdownFiles) assert.ok(existsSync(join(distRoot, 'markdown', path)), path);
  assert.ok(existsSync(join(distRoot, 'markdown', 'assets/app.js')));

  const converter = readFileSync(join(distRoot, 'markdown/converter.html'), 'utf8');
  const converterOld = readFileSync(join(distRoot, 'markdown/converter-old.html'), 'utf8');
  const manifest = readFileSync(join(distRoot, 'markdown/manifest.webmanifest'), 'utf8');
  const serviceWorker = readFileSync(join(distRoot, 'markdown/sw.js'), 'utf8');
  assert.match(converter, /src="\.\/assets\/app\.js"/);
  assert.match(converterOld, /serviceWorker\.register\('\.\/sw\.js'/);
  assert.match(manifest, /"start_url":\s*"\.\/converter\.html"/);
  assert.match(manifest, /"scope":\s*"\.\/"/);
  assert.match(serviceWorker, /'\.\/assets\/app\.js'/);

  for (const excluded of [
    'node_modules',
    'tests',
    'test-results',
    'playwright-report',
    'context.md',
    'roadmap.md',
    'jira_style.md',
    'package.json',
    'package-lock.json',
    'vite.config.js',
    'vitest.config.js'
  ]) {
    assert.equal(existsSync(join(distRoot, 'markdown', excluded)), false, excluded);
  }
});