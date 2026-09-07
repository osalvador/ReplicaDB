import assert from 'node:assert/strict';
import { readFileSync, existsSync } from 'node:fs';
import { join } from 'node:path';
import test from 'node:test';

const repoRoot = new URL('../..', import.meta.url).pathname;
const docsRoot = new URL('..', import.meta.url).pathname;
/** @typedef {{ id: string, displayName: string, schemes: string[], roles: string[], modes: string[], page: string, caveats: string[], bandwidthThrottling: boolean, staging: boolean }} Capability */
/** @type {Capability[]} */
const capabilities = JSON.parse(readFileSync(join(docsRoot, 'src/data/connector-capabilities.json'), 'utf8'));
const supportedManagers = readFileSync(join(repoRoot, 'src/main/java/org/replicadb/manager/SupportedManagers.java'), 'utf8');
const jdbcDrivers = readFileSync(join(repoRoot, 'src/main/java/org/replicadb/manager/JdbcDrivers.java'), 'utf8');
const managerFactory = readFileSync(join(repoRoot, 'src/main/java/org/replicadb/manager/ManagerFactory.java'), 'utf8');
const managerCapabilities = readFileSync(join(repoRoot, 'src/main/java/org/replicadb/manager/ManagerCapabilities.java'), 'utf8');

/** @param {string} name */
function managerId(name) {
  return {
    POSTGRES: 'postgresql',
    SQLSERVER: 'sql-server',
    DB2_AS400: 'db2-as400',
    JTDS_SQLSERVER: 'jtds-sql-server',
    MONGODBSRV: 'mongodb-srv',
    S3: 'amazon-s3'
  }[name] || name.toLowerCase();
}

/** @param {string} route */
function sourcePage(route) {
  const stem = route.replace(/^\//, '').replace(/\/$/, '');
  for (const candidate of [`${stem}.md`, `${stem}.mdx`, `${stem}/index.md`, `${stem}/index.mdx`]) {
    const path = join(docsRoot, 'src/content/docs', candidate);
    if (existsSync(path)) return path;
  }
  throw new Error(`Missing source page for ${route}`);
}

test('validates capability shape, pages, and safe caveats', () => {
  const ids = new Set();
  const validModes = new Set(['complete', 'complete-atomic', 'incremental']);
  const requiredFields = /** @type {(keyof Capability)[]} */ (['id', 'displayName', 'schemes', 'roles', 'modes', 'bandwidthThrottling', 'staging', 'page', 'caveats']);
  for (const connector of capabilities) {
    for (const field of requiredFields) {
      assert.ok(connector[field] !== undefined, `${connector.id}: ${field}`);
    }
    assert.equal(ids.has(connector.id), false, connector.id);
    ids.add(connector.id);
    assert.ok(connector.schemes.length > 0, connector.id);
    assert.ok(connector.roles.every((role) => role === 'source' || role === 'sink'), connector.id);
    assert.ok(connector.modes.length > 0 && connector.modes.every((mode) => validModes.has(mode)), connector.id);
    assert.ok(existsSync(sourcePage(connector.page)), connector.page);
    assert.doesNotMatch(JSON.stringify(connector), /(?:password|secret|token)\s*[:=]\s*["'][^$<{\n]+["']/i);
  }
});

test('matches every SupportedManagers enum entry and factory scheme', () => {
  const managerNames = [...supportedManagers.matchAll(/\b([A-Z][A-Z0-9_]+)\(/g)].map((match) => match[1]);
  const expectedIds = managerNames.map(managerId).sort();
  const actualIds = capabilities.map((connector) => connector.id).sort();
  assert.deepEqual(actualIds, expectedIds);
  for (const connector of capabilities) {
    for (const scheme of connector.schemes) {
      assert.match(jdbcDrivers, new RegExp(`\\"${scheme.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\"`), `${connector.id}: ${scheme}`);
    }
  }
  for (const managerName of managerNames) {
    const genericManagers = new Set(['HSQLDB', 'CUBRID', 'JTDS_SQLSERVER', 'NETEZZA']);
    if (genericManagers.has(managerName)) {
      assert.match(managerFactory, /StandardJDBCManager/);
    } else {
      assert.match(managerFactory, new RegExp(`\\b${managerName}\\b`), managerName);
    }
    assert.match(managerCapabilities, new RegExp(`\\b${managerName}\\b`), managerName);
  }
});

test('keeps capability pages free from duplicate matrix ownership and unsafe examples', () => {
  const index = readFileSync(join(docsRoot, 'src/content/docs/connectors/index.mdx'), 'utf8');
  assert.equal((index.match(/<SupportMatrix/g) || []).length, 1);
  const pages = capabilities.map((connector) => readFileSync(sourcePage(connector.page), 'utf8')).join('\n');
  assert.doesNotMatch(pages, /(?:password|secret|token)\s*[:=]\s*["'][^$<{\n]+["']/i);
  assert.doesNotMatch(pages, /coming soon/i);
});