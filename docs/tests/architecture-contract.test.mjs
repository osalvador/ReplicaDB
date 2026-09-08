import assert from 'node:assert/strict';
import { existsSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import test from 'node:test';

const repoRoot = new URL('../..', import.meta.url).pathname;
const docsRoot = new URL('..', import.meta.url).pathname;
const architectureRoot = join(docsRoot, 'src/content/docs/architecture');
const docs = ['overview.mdx', 'core-and-server-boundaries.md', 'distributed-topology.mdx', 'run-lifecycle.mdx', 'dispatch-and-recovery.md', 'concurrency-and-fencing.md', 'scheduling-and-ha.md', 'scaling-and-fairness.md', 'security-boundaries.md']
  .map((name) => readFileSync(join(architectureRoot, name), 'utf8')).join('\n');

test('covers the durable execution invariants and limitations', () => {
  for (const required of [
    'PENDING', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCEL_REQUESTED', 'CANCELLED', 'RETRY_SCHEDULED',
    'FOR UPDATE SKIP LOCKED', 'lease token', 'token matches', 'watermark', 'notification', 'polling',
    'Quartz', 'approximate fairness', 'not round-robin', 'worker instances * concurrent runs per worker * jobs per run',
    'replicadb.worker.max-concurrent-runs', 'jobs per run', 'datasource', 'fencing',
    'claim time', 'UUID order', 'immutable for the active attempt', 're-resolves the current datasource',
    'DIRECTED', 'FALLBACK', 'GENERIC', '100 ms', '250 ms', '25 ms', '2 s', '30 s decay half-life'
  ]) {
    assert.match(docs, new RegExp(required.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'i'), required);
  }
});

test('binds claims to real source/test paths and worker configuration defaults', () => {
  for (const path of [
    'replicadb-server/src/main/java/org/replicadb/server/job/persistence/JobRunRepository.java',
    'replicadb-server/src/main/java/org/replicadb/server/job/application/RunLeaseService.java',
    'replicadb-server/src/main/java/org/replicadb/server/job/application/RunFinalizationService.java',
    'replicadb-server/src/main/java/org/replicadb/server/job/execution/HeartbeatService.java',
    'replicadb-server/src/main/java/org/replicadb/server/job/execution/WorkerDispatchCoordinator.java',
    'replicadb-server/src/main/java/org/replicadb/server/job/execution/ScheduleReconciler.java',
    'replicadb-server/src/main/java/org/replicadb/server/job/domain/JobRunStatus.java',
    'replicadb-server/src/main/java/org/replicadb/server/job/config/WorkerRuntimeProperties.java',
    'replicadb-server/src/test/java/org/replicadb/server/job/execution/HeartbeatServiceTest.java',
    'replicadb-server/src/test/java/org/replicadb/server/job/execution/WorkerDispatchCoordinatorTest.java'
  ]) assert.ok(existsSync(join(repoRoot, path)), path);
  const workerConfig = readFileSync(join(repoRoot, 'replicadb-server/src/main/resources/application-worker.yml'), 'utf8');
  for (const setting of ['max-concurrent-runs: 1', 'lease-duration: 5m', 'heartbeat-interval: 30s', 'poll-interval: 30s', 'poll-batch-size: 100', 'directed-queue-capacity: 1024']) {
    assert.match(workerConfig, new RegExp(setting.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')), setting);
  }
});

test('each architecture diagram has a text alternative', () => {
  const diagramCount = (docs.match(/<ArchitectureDiagram/g) || []).length;
  const fallbackCount = (docs.match(/fallback=\{/g) || []).length;
  assert.equal(diagramCount, 3);
  assert.equal(fallbackCount, diagramCount);
});

test('explains architectural ownership, failure boundaries, and operating handoffs', () => {
  for (const required of [
    'The replication core stays reusable', 'The server owns durable coordination',
    'Responsibilities and failure boundaries', 'Creation, claim, and terminal outcome',
    'Retry and recovery lineage', 'Claim-time consistency', 'Fence stale processes',
    'Schedule intent and scheduler mechanics', 'Approximate fairness by design',
    'Data crosses explicit boundaries', 'Identity and execution are different authorities',
    '/ReplicaDB/operations/distributed-deployment/',
    '/ReplicaDB/operations/failure-recovery/', '/ReplicaDB/operations/capacity-planning/'
  ]) {
    assert.match(docs, new RegExp(required.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'i'), required);
  }
});
