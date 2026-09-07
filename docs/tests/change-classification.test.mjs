import assert from 'node:assert/strict';
import test from 'node:test';
import { classifyChanges } from '../scripts/classify-changes.mjs';

test('classifies docs-only changes without API or frontend drift', () => {
  assert.deepEqual(classifyChanges(['docs/src/content/docs/server/jobs.mdx', 'README.md']), {
    files: ['docs/src/content/docs/server/jobs.mdx', 'README.md'], base: true, api: false, frontend: false, docsOnly: true
  });
});

test('classifies API and frontend changes independently', () => {
  const result = classifyChanges([
    'replicadb-server/src/main/java/org/replicadb/server/job/api/JobController.java',
    'replicadb-server/frontend/src/pages/JobDetailPage.tsx',
    'docs/openapi/replicadb-server.json'
  ]);
  assert.equal(result.base, true);
  assert.equal(result.api, true);
  assert.equal(result.frontend, true);
  assert.equal(result.docsOnly, false);
});

test('ignores unrelated repository paths', () => {
  assert.deepEqual(classifyChanges(['src/main/java/org/replicadb/ReplicaDB.java']), {
    files: ['src/main/java/org/replicadb/ReplicaDB.java'], base: false, api: false, frontend: false, docsOnly: false
  });
});