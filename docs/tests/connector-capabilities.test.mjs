import assert from 'node:assert/strict';
import { readFileSync, existsSync } from 'node:fs';
import { join } from 'node:path';
import test from 'node:test';

const repoRoot = new URL('../..', import.meta.url).pathname;
const docsRoot = new URL('..', import.meta.url).pathname;
/** @typedef {{ id: string, displayName: string, schemes: string[], roles: string[], sourceModes: string[], sinkModes: string[], sourceQuery: boolean, singleJobOnly: boolean, page: string, caveats: string[], bandwidthThrottling: boolean, staging: boolean }} Capability */
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
  const requiredFields = /** @type {(keyof Capability)[]} */ (['id', 'displayName', 'schemes', 'roles', 'sourceModes', 'sinkModes', 'sourceQuery', 'singleJobOnly', 'bandwidthThrottling', 'staging', 'page', 'caveats']);
  for (const connector of capabilities) {
    for (const field of requiredFields) {
      assert.ok(connector[field] !== undefined, `${connector.id}: ${field}`);
    }
    assert.equal(ids.has(connector.id), false, connector.id);
    ids.add(connector.id);
    assert.ok(connector.schemes.length > 0, connector.id);
    assert.ok(connector.roles.every((role) => role === 'source' || role === 'sink'), connector.id);
    assert.ok([...connector.sourceModes, ...connector.sinkModes].every((mode) => validModes.has(mode)), connector.id);
    assert.equal(connector.roles.includes('source'), connector.sourceModes.length > 0, `${connector.id}: source role`);
    assert.equal(connector.roles.includes('sink'), connector.sinkModes.length > 0, `${connector.id}: sink role`);
    assert.ok(existsSync(sourcePage(connector.page)), connector.page);
    assert.doesNotMatch(JSON.stringify(connector), /(?:password|secret|token)\s*[:=]\s*["'][^$<{\n]+["']/i);
  }
});

test('matches the role-specific ManagerCapabilities contract', () => {
  const allModes = ['complete', 'complete-atomic', 'incremental'];
  const completeOnly = ['complete'];
  const completeIncremental = ['complete', 'incremental'];
  const expected = new Map();
  for (const id of ['mysql', 'mariadb', 'postgresql', 'oracle', 'sql-server', 'db2', 'db2-as400']) {
    expected.set(id, [allModes, allModes, true, false]);
  }
  for (const id of ['hsqldb', 'cubrid', 'jtds-sql-server', 'netezza']) {
    expected.set(id, [completeOnly, completeOnly, true, true]);
  }
  expected.set('denodo', [allModes, [], true, false]);
  expected.set('kafka', [[], completeOnly, false, false]);
  expected.set('amazon-s3', [[], completeOnly, false, false]);
  expected.set('file', [allModes, completeIncremental, false, true]);
  expected.set('sqlite', [allModes, completeIncremental, true, false]);
  expected.set('mongodb', [allModes, completeIncremental, true, false]);
  expected.set('mongodb-srv', [allModes, completeIncremental, true, false]);

  for (const connector of capabilities) {
    const [sourceModes, sinkModes, sourceQuery, singleJobOnly] = expected.get(connector.id);
    assert.deepEqual(connector.sourceModes, sourceModes, `${connector.id}: source modes`);
    assert.deepEqual(connector.sinkModes, sinkModes, `${connector.id}: sink modes`);
    assert.equal(connector.sourceQuery, sourceQuery, `${connector.id}: source query`);
    assert.equal(connector.singleJobOnly, singleJobOnly, `${connector.id}: single-job limit`);
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

test('documents relational connector roles and implementation caveats in their owning pages', () => {
  const topicsByPage = {
    '/connectors/oracle/': ['source and sink', 'Flashback', 'LOB', 'staging'],
    '/connectors/postgresql/': ['source or sink', 'binary COPY', 'TEXT COPY', 'SSL'],
    '/connectors/mysql-mariadb/': ['source or sink', 'LOAD DATA LOCAL INFILE', 'MariaDB JDBC', 'staging'],
    '/connectors/sql-server/': ['source and sink', 'XML', 'bandwidth-throttling', 'staging'],
    '/connectors/db2/': ['source and sink', 'ROW_NUMBER', 'LOB', 'IBM i'],
    '/connectors/sqlite/': ['source or sink', 'does not support complete-atomic', 'file locking'],
    '/connectors/denodo/': ['source-only', 'cannot receive sink writes', 'query pushdown'],
    '/connectors/generic-jdbc/': ['source and sink', 'complete', 'single-job', 'driver class']
  };

  for (const [page, topics] of Object.entries(topicsByPage)) {
    const content = readFileSync(sourcePage(page), 'utf8');
    for (const topic of topics) {
      const pattern = topic.split(/\s+/).map((word) => word.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')).join('\\s+');
      assert.match(content, new RegExp(pattern, 'i'), `${page}: ${topic}`);
    }
  }
});

test('documents non-table connector formats, layouts, and delivery limits', () => {
  const topicsByPage = {
    '/connectors/csv/': ['CSV and ORC', 'single-job', 'format.quoteMode', 'ORC limits', 'not an atomic replacement'],
    '/connectors/amazon-s3/': ['sink-only', 'object-per-row', 'keyFileName', 'no transaction', 'environment-managed secret'],
    '/connectors/mongodb/': ['source modes', 'aggregation pipeline', 'normalized', 'unique-index', 'does not propagate source deletes'],
    '/connectors/kafka/': ['sink-only', 'sink.connect.parameter.topic', 'partition', 'producer key', 'does not retract messages']
  };

  for (const [page, topics] of Object.entries(topicsByPage)) {
    const content = readFileSync(sourcePage(page), 'utf8');
    for (const topic of topics) {
      const pattern = topic.split(/\s+/).map((word) => word.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')).join('\\s+');
      assert.match(content, new RegExp(pattern, 'i'), `${page}: ${topic}`);
    }
  }
});
