import assert from 'node:assert/strict';
import test from 'node:test';
import {
  buildJobPayload,
  getMissingRunCount,
  MINIMUM_RUNS_PER_JOB,
  seedJobRunHistory,
  SOURCE_FIXTURES
} from './seed-local-jobs.mjs';

function jsonResponse(body, status = 200) {
  return {
    ok: status >= 200 && status < 300,
    status,
    headers: {
      getSetCookie: () => []
    },
    text: async () => JSON.stringify(body)
  };
}

test('covers every source type exposed by the configuration wizard', () => {
  assert.deepEqual(
    SOURCE_FIXTURES.map(fixture => fixture.key),
    ['oracle', 'mysql', 'mariadb', 'postgres', 'db2', 'db2i', 'sqlite', 'sqlserver', 'denodo', 'file']
  );
});

test('covers all replication modes', () => {
  assert.deepEqual(
    new Set(SOURCE_FIXTURES.map(fixture => fixture.mode)),
    new Set(['complete', 'complete-atomic', 'incremental'])
  );
});

test('adds watermark fields only to incremental fixtures and never embeds credentials', () => {
  for (const fixture of SOURCE_FIXTURES) {
    const payload = buildJobPayload(fixture);
    assert.equal(payload.sourceUser, undefined);
    assert.equal(payload.sourcePassword, undefined);
    assert.equal(payload.sinkPassword, undefined);
    if (fixture.mode === 'incremental') {
      assert.equal(payload.incrementalWatermarkColumn, 'updated_at');
      assert.equal(payload.initialWatermarkValue, '0');
    } else {
      assert.equal(payload.incrementalWatermarkColumn, undefined);
      assert.equal(payload.initialWatermarkValue, undefined);
    }
  }
});

test('keeps at least five run history entries per job', () => {
  assert.equal(MINIMUM_RUNS_PER_JOB, 5);
  assert.equal(getMissingRunCount(0), 5);
  assert.equal(getMissingRunCount(3), 2);
  assert.equal(getMissingRunCount(5), 0);
  assert.equal(getMissingRunCount(8), 0);
});

test('creates only the missing local terminal runs without executing or cancelling a source run', async () => {
  const calls = [];
  let triggerCount = 0;
  const fetchImpl = async (url, options = {}) => {
    calls.push({ url, options });
    if (url.endsWith('/api/v1/jobs/job-1/runs?page=0&size=100')) {
      return jsonResponse({ content: [{ id: 'run-existing-1' }], totalElements: 2 });
    }
    if (url.endsWith('/api/v1/jobs/job-1/runs')) {
      triggerCount += 1;
      return jsonResponse({ id: `run-new-${triggerCount}`, status: 'CANCELLED' }, 202);
    }
    throw new Error(`Unexpected request: ${url}`);
  };

  const created = await seedJobRunHistory({
    fetchImpl,
    cookieJar: new Map(),
    apiUrl: 'http://localhost:8080',
    jobId: 'job-1',
    csrfToken: 'csrf-token'
  });

  assert.equal(created, 3);
  assert.equal(calls.filter(call => call.options.method === 'POST' && call.url.endsWith('/runs')).length, 3);
  assert.equal(calls.filter(call => call.options.method === 'POST' && call.url.includes('/cancel')).length, 0);
  assert.equal(calls
    .filter(call => call.options.method === 'POST' && call.url.endsWith('/runs'))
    .every(call => call.options.headers.get('X-ReplicaDB-Local-Seed') === 'true'), true);
  assert.equal(calls.some(call => call.url.includes('/source')), false);
});

test('waits for an active run left by an earlier seed attempt before creating local history', async () => {
  const calls = [];
  let statusReads = 0;
  let triggerCount = 0;
  const fetchImpl = async (url, options = {}) => {
    calls.push({ url, options });
    if (url.endsWith('/api/v1/jobs/job-2/runs?page=0&size=100')) {
      return jsonResponse({ content: [{ id: 'run-stuck', status: 'PENDING' }], totalElements: 1 });
    }
    if (url.endsWith('/api/v1/runs/run-stuck')) {
      statusReads += 1;
      return jsonResponse({ id: 'run-stuck', status: statusReads === 1 ? 'PENDING' : 'FAILED' });
    }
    if (url.endsWith('/api/v1/jobs/job-2/runs')) {
      triggerCount += 1;
      return jsonResponse({ id: `run-new-${triggerCount}`, status: 'CANCELLED' }, 202);
    }
    throw new Error(`Unexpected request: ${url}`);
  };

  const created = await seedJobRunHistory({
    fetchImpl,
    cookieJar: new Map(),
    apiUrl: 'http://localhost:8080',
    jobId: 'job-2',
    csrfToken: 'csrf-token',
    minimumRuns: 2
  });

  assert.equal(created, 1);
  assert.equal(calls.some(call => call.url.includes('/cancel')), false);
  assert.equal(calls.findIndex(call => call.url.endsWith('/api/v1/runs/run-stuck')) < calls.findIndex(call => call.url.endsWith('/api/v1/jobs/job-2/runs')), true);
});

test('rejects a normal trigger response when local seeding is disabled on the API', async () => {
  const calls = [];
  const fetchImpl = async (url, options = {}) => {
    calls.push({ url, options });
    if (url.endsWith('/api/v1/jobs/job-3/runs?page=0&size=100')) {
      return jsonResponse({ content: [], totalElements: 0 });
    }
    if (url.endsWith('/api/v1/jobs/job-3/runs')) {
      return jsonResponse({ id: 'run-not-local', status: 'PENDING' }, 202);
    }
    throw new Error(`Unexpected request: ${url}`);
  };

  await assert.rejects(
    seedJobRunHistory({
      fetchImpl,
      cookieJar: new Map(),
      apiUrl: 'http://localhost:8080',
      jobId: 'job-3',
      csrfToken: 'csrf-token',
      minimumRuns: 1
    }),
    /local seed API did not create a terminal run/
  );
});
