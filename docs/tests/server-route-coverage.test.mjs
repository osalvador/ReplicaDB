import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { assertUniqueContentSlugs, collectContentOwners } from '../scripts/check-content-slugs.mjs';

const repoRoot = new URL('../..', import.meta.url).pathname;
const docsRoot = new URL('..', import.meta.url).pathname;
const contentRoot = join(docsRoot, 'src/content/docs');
const routeSource = readFileSync(join(repoRoot, 'replicadb-server/frontend/src/router/routes.tsx'), 'utf8');
const navigationSource = readFileSync(join(repoRoot, 'replicadb-server/frontend/src/layout/AppLayout.tsx'), 'utf8');
const contentOwners = collectContentOwners(contentRoot);

/** @param {string} name */
function readServerGuide(name) {
  const owners = contentOwners.get(`server/${name}`) ?? [];
  assert.equal(owners.length, 1, `server/${name} must have one canonical source entry`);
  return readFileSync(owners[0], 'utf8');
}

const pageSources = [
  'dashboard', 'datasources', 'jobs', 'schedules', 'runs-and-diagnostics',
  'users', 'permissions', 'audit', 'sign-in-and-profile', 'errors-and-empty-states'
].map(readServerGuide).join('\n');
const serverGuides = [
  readFileSync(join(contentRoot, 'server/index.md'), 'utf8'),
  ...[
  'installation', 'dashboard', 'datasources', 'jobs', 'schedules',
  'runs-and-diagnostics', 'users', 'permissions', 'audit',
  'sign-in-and-profile', 'errors-and-empty-states'
].map(readServerGuide)
].join('\n');

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
    assert.match(readServerGuide(guide), /ADMIN/);
  }
});

test('requires exactly one source entry for every documentation slug', () => {
  assert.doesNotThrow(() => assertUniqueContentSlugs(contentRoot));
});

test('rejects a duplicate content slug before building', (context) => {
  const fixtureRoot = mkdtempSync(join(tmpdir(), 'replicadb-content-'));
  context.after(() => rmSync(fixtureRoot, { recursive: true, force: true }));
  writeFileSync(join(fixtureRoot, 'duplicate.md'), '---\ntitle: First\n---\n');
  writeFileSync(join(fixtureRoot, 'duplicate.mdx'), '---\ntitle: Second\n---\n');

  assert.throws(() => assertUniqueContentSlugs(fixtureRoot), /Duplicate documentation slugs:[\s\S]*duplicate/);
});

test('keeps navigation and visible actions aligned with the frontend', () => {
  for (const label of ['Dashboard', 'Jobs', 'Datasources', 'Audit', 'Users', 'My profile', 'Logout']) {
    assert.match(navigationSource, new RegExp(label.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')));
  }
  for (const label of ['Trigger run', 'Edit', 'Manage permissions', 'Delete', 'New job', 'New datasource', 'Create schedule', 'Cancel run', 'Retry run', 'Create user', 'Reset password']) {
    assert.match(pageSources, new RegExp(label.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')));
  }
});

test('maps server workflows to their instructional captures', () => {
  const capturesByGuide = {
    dashboard: ['dashboard.png'],
    datasources: ['datasources.png', 'datasource-new.png', 'datasource-detail.png', 'datasource-edit.png'],
    jobs: ['jobs.png', 'job-new.png', 'job-detail.png', 'job-edit.png'],
    schedules: ['schedule.png'],
    'runs-and-diagnostics': ['run-detail.png'],
    users: ['users.png'],
    permissions: ['datasource-permissions.png', 'job-permissions.png'],
    audit: ['audit.png'],
    'sign-in-and-profile': ['login.png', 'profile.png'],
    'errors-and-empty-states': ['unauthorized.png']
  };

  for (const [guide, captures] of Object.entries(capturesByGuide)) {
    const content = readServerGuide(guide);
    for (const capture of captures) assert.match(content, new RegExp(capture.replace('.', '\\.')));
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

test('explains workflow effects, validation, and operational recovery', () => {
  for (const required of [
    'Follow an operator workflow', 'Create and verify a profile',
    'Define and validate execution intent', 'Configure and verify recurring work',
    'Trigger, inspect, and decide', 'Manage access deliberately',
    'Change access without changing history', 'Investigate a durable change',
    'Recover without losing context', '/ReplicaDB/operations/troubleshooting/',
    '/ReplicaDB/operations/health-and-metrics/'
  ]) {
    assert.match(serverGuides, new RegExp(required.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'i'), required);
  }
});
