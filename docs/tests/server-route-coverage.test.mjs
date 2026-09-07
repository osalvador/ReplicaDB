import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import test from 'node:test';

const repoRoot = new URL('../..', import.meta.url).pathname;
const docsRoot = new URL('..', import.meta.url).pathname;
const routeSource = readFileSync(join(repoRoot, 'replicadb-server/frontend/src/router/routes.tsx'), 'utf8');
const navigationSource = readFileSync(join(repoRoot, 'replicadb-server/frontend/src/layout/AppLayout.tsx'), 'utf8');
const pageSources = [
  'dashboard', 'datasources', 'jobs', 'schedules', 'runs-and-diagnostics',
  'users', 'permissions', 'audit', 'sign-in-and-profile', 'errors-and-empty-states'
].map((name) => readFileSync(join(docsRoot, `src/content/docs/server/${name}.md`), 'utf8')).join('\n');

/** @type {Record<string, string>} */
const guideByRoute = {
  '/login': 'sign-in-and-profile',
  '/': 'dashboard',
  'profile': 'sign-in-and-profile',
  'jobs': 'jobs',
  'datasources': 'datasources',
  'datasources/:id': 'datasources',
  'datasources/:id/edit': 'datasources',
  'jobs/new': 'jobs',
  'jobs/:id/edit': 'jobs',
  'jobs/:id': 'jobs',
  'runs/:id': 'runs-and-diagnostics',
  'audit': 'audit',
  'datasources/new': 'datasources',
  'datasources/:id/permissions': 'permissions',
  'users': 'users',
  'jobs/:id/permissions': 'permissions'
};

test('maps every frontend route and ADMIN boundary to a server guide', () => {
  const routes = [...routeSource.matchAll(/path:\s*['"]([^'"]+)['"]/g)].map((match) => match[1]);
  for (const route of routes) {
    assert.ok(guideByRoute[route], `unmapped route: ${route}`);
    assert.ok(guideByRoute[route], `missing guide for ${route}`);
  }
  assert.match(routeSource, /RequireRole role="ADMIN"/);
  for (const guide of ['users', 'permissions', 'audit']) {
    assert.match(readFileSync(join(docsRoot, `src/content/docs/server/${guide}.md`), 'utf8'), /ADMIN/);
  }
});

test('keeps navigation and visible actions aligned with the frontend', () => {
  for (const label of ['Dashboard', 'Jobs', 'Datasources', 'Audit', 'Users', 'My profile', 'Logout']) {
    assert.match(navigationSource, new RegExp(label.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')));
  }
  for (const label of ['Trigger run', 'Edit', 'Manage permissions', 'Delete', 'New job', 'New datasource', 'Create schedule', 'Cancel run', 'Retry run', 'Create user', 'Reset password']) {
    assert.match(pageSources, new RegExp(label.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')));
  }
});

test('covers ACL vocabulary, error states, and secret-preserving behavior', () => {
  for (const word of ['VIEW', 'USE', 'EDIT', 'EXECUTE', 'CANCEL', 'RFC 7807', 'clearSecurityKeys', 'complete-atomic', 'truncated', 'conflict']) {
    assert.match(pageSources, new RegExp(word.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'i'), word);
  }
  for (const prohibited of ['leaseToken', 'encryptedSecurity', 'sourcePassword', 'sinkPassword']) {
    assert.doesNotMatch(pageSources, new RegExp(prohibited));
  }
  assert.doesNotMatch(pageSources, /jdbc:[^\s"']+:\/\/[^\s"']*:[^\s"']*@/i);
});