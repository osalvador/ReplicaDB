import assert from 'node:assert/strict';
import { existsSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import test from 'node:test';

const repoRoot = new URL('../..', import.meta.url).pathname;
const docsRoot = new URL('..', import.meta.url).pathname;
const operationsRoot = join(docsRoot, 'src/content/docs/operations');
const docs = [
  ...['index', 'local-server', 'distributed-deployment', 'configuration', 'capacity-planning', 'health-and-metrics', 'security-and-tls', 'key-management', 'backups-and-restore', 'upgrades', 'failure-recovery', 'troubleshooting'].map((name) => join(operationsRoot, `${name}.md`)),
  join(docsRoot, 'src/content/docs/reference/environment-variables.md')
].map((path) => readFileSync(path, 'utf8')).join('\n');

test('covers deployment, health, security, recovery, and metric interpretations', () => {
  for (const required of [
    'local', 'api', 'worker', '8080', '9091', '/actuator/health/liveness', '/actuator/health/readiness',
    '/actuator/metrics', '/actuator/prometheus', 'liveness', 'readiness', 'DEGRADED',
    'replicadb.managed.claims', 'replicadb.managed.lease.renewals', 'replicadb.worker.admission.events',
    '5 failed attempts', '15-minute', 'AES', 'keyring', 'point-in-time', 'V1 through V21', '256 KiB',
    'never resumes', 'truncated', 'previous_run_id', 'watermark advances only', 'shutdown-timeout',
    '30 seconds', 'UUID order', '1,024', '250 ms', 'first 75%', 'last 25%',
    '[TRUNCATED: middle omitted]', 'replicadb.worker.listener.connected', 'replicadb.managed.polling.lag',
    'server.ssl.*', 'PKCS12'
  ]) {
    assert.match(docs, new RegExp(required.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'i'), required);
  }
});

test('keeps environment documentation aligned with the maintained example', () => {
  const examplePath = join(repoRoot, 'replicadb-server/conf/replicadb-server.env.example');
  const example = readFileSync(examplePath, 'utf8');
  for (const match of example.matchAll(/^([A-Z][A-Z0-9_]+)=/gm)) {
    assert.match(docs, new RegExp(`\\b${match[1]}\\b`), match[1]);
  }
  assert.ok(existsSync(join(repoRoot, 'replicadb-server/src/main/java/org/replicadb/server/observability/ManagedRuntimeMetrics.java')));
});

test('does not put resolved credentials or high-risk material in operational docs', () => {
  assert.doesNotMatch(docs, /jdbc:[^\s"']+:\/\/[^\s"']*:[^\s"']*@/i);
  assert.doesNotMatch(docs, /-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----/i);
  assert.doesNotMatch(docs, /(?:password|secret|token)\s*[:=]\s*["'][^$<{\n]+["']/i);
});
