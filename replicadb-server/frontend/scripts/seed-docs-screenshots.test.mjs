import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import test from 'node:test';
import { buildDocsFixtureNames, DOCS_FIXED_IDS, DOCS_FIXTURE_PREFIX, docsFixturePlan, validateDocsFixturePlan } from './seed-docs-screenshots.mjs';

test('uses a deterministic docs prefix and fixed fixture identifiers', () => {
  assert.equal(DOCS_FIXTURE_PREFIX, 'Docs /');
  assert.match(DOCS_FIXED_IDS.runningRun, /^00000000-0000-4000-8000-/);
  assert.equal(validateDocsFixturePlan(), true);
  assert.equal(new Set(docsFixturePlan.map(fixture => fixture.key)).size, docsFixturePlan.length);
});

test('fixture names are human-readable and contain no security fields', () => {
  const names = buildDocsFixtureNames();
  assert.ok(names.jobs.every(name => name.startsWith(DOCS_FIXTURE_PREFIX)));
  assert.ok(names.users.every(name => name.startsWith(DOCS_FIXTURE_PREFIX)));
  assert.doesNotMatch(JSON.stringify(names), /password|secret|token|leaseToken|encryptedSecurity/i);
});

test('fixture generator source does not contain resolved security assignments', () => {
  const source = readFileSync(join(import.meta.dirname, 'seed-docs-screenshots.mjs'), 'utf8');
  assert.doesNotMatch(source, /(?:password|secret|token|leaseToken|encryptedSecurity)\s*[:=]\s*['"][^$<{]/i);
});