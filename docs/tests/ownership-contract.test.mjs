import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import test from 'node:test';

const repoRoot = new URL('../..', import.meta.url).pathname;
/** @param {string} path */
const read = (path) => readFileSync(join(repoRoot, path), 'utf8');

test('repository entry points identify the canonical portal and local ownership boundaries', () => {
  assert.match(read('README.md'), /https:\/\/osalvador\.github\.io\/ReplicaDB\//);
  assert.match(read('replicadb-server/README.md'), /documentation portal/);
  assert.match(read('replicadb-server/frontend/README.develop.md'), /canonical user-facing server documentation/);
  assert.match(read('docs/CONTRIBUTING.md'), /update-openapi\.sh/);
  assert.match(read('docs/CONTRIBUTING.md'), /test:e2e:docs/);
  assert.match(read('docs/CONTRIBUTING.md'), /connector-capabilities\.json/);
  assert.match(read('docs/CONTRIBUTING.md'), /architecture\/operations page/);
});

test('entry points do not retain the old monolithic Jekyll URL as their canonical docs link', () => {
  for (const path of ['README.md', 'replicadb-server/README.md', 'replicadb-server/frontend/README.develop.md']) {
    assert.doesNotMatch(read(path), /osalvador\.github\.io\/ReplicaDB\/docs\/docs\.html/);
  }
});
