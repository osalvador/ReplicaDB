import assert from 'node:assert/strict';
import test from 'node:test';
import {
  buildDatasourcePayload,
  buildJobPayload,
  getMissingRunCount,
  MINIMUM_RUNS_PER_JOB,
  PG2PG_FIXTURE,
  runRealIntegration,
  seedLocalJobs,
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

test('builds datasource profiles with connector metadata and transient connection security', () => {
  const payload = buildDatasourcePayload(SOURCE_FIXTURES[0]);

  assert.deepEqual(payload, {
    name: 'Develop / Oracle source datasource',
    connectorType: 'oracle',
    technicalParams: {},
    security: { connect: 'jdbc:oracle:thin:@//localhost:1521/ORCL' },
    clearSecurityKeys: []
  });
});

test('builds the isolated local PostgreSQL datasource and job payload', () => {
  const datasource = buildDatasourcePayload(PG2PG_FIXTURE);
  const job = buildJobPayload(PG2PG_FIXTURE, 'pglocal-id', 'pglocal-id');

  assert.equal(datasource.name, 'Pglocal');
  assert.equal(datasource.connectorType, 'postgres');
  assert.equal(datasource.security.user, 'postgres');
  assert.equal(datasource.security.password, undefined);
  assert.equal(job.name, 'Develop / pg2pg source');
  assert.equal(job.sourceDatasourceId, 'pglocal-id');
  assert.equal(job.sinkDatasourceId, 'pglocal-id');
  assert.equal(job.sourceTable, 'pg2pg.pg2pg_source_orders');
  assert.equal(job.sinkTable, 'pg2pg.pg2pg_destination_orders');
  assert.equal(job.sourceColumns, 'id, payload');
  assert.equal(job.sinkColumns, 'id, payload');
  assert.equal(job.mode, 'complete');
  assert.equal(job.jobs, 1);
});

test('creates datasource profiles before datasource-only jobs', async () => {
  const calls = [];
  let datasourceCount = 0;
  let jobCount = 0;
  let realJobId;
  let realRunReads = 0;
  const fetchImpl = async (url, options = {}) => {
    calls.push({ url, options });
    if (url.endsWith('/api/v1/auth/csrf')) {
      return jsonResponse({ token: 'csrf-token' });
    }
    if (url.endsWith('/api/v1/auth/login')) {
      return jsonResponse({});
    }
    if (url.endsWith('/api/v1/datasources?page=0&size=100')) {
      return jsonResponse({ content: [], totalElements: 0 });
    }
    if (url.endsWith('/api/v1/datasources')) {
      const payload = JSON.parse(options.body);
      datasourceCount += 1;
      return jsonResponse({
        id: `datasource-${datasourceCount}`,
        name: payload.name,
        connectorType: payload.connectorType
      }, 201);
    }
    if (url.endsWith('/api/v1/jobs?page=0&size=100')) {
      return jsonResponse({ content: [], totalElements: 0 });
    }
    if (url.endsWith('/api/v1/jobs')) {
      jobCount += 1;
      const payload = JSON.parse(options.body);
      if (payload.name === 'Develop / pg2pg source') {
        realJobId = `job-${jobCount}`;
      }
      return jsonResponse({ id: `job-${jobCount}`, name: payload.name }, 201);
    }
    if (url.includes('/runs?page=0&size=100')) {
      return jsonResponse({ content: [], totalElements: 0 });
    }
    if (url.endsWith('/runs')) {
      if (!options.headers.get('X-ReplicaDB-Local-Seed')) {
        return jsonResponse({ id: 'run-real', status: 'PENDING' }, 202);
      }
      return jsonResponse({ id: `seed-run-${calls.length}`, status: 'CANCELLED' }, 202);
    }
    if (url.endsWith('/api/v1/runs/run-real')) {
      realRunReads += 1;
      return jsonResponse({ id: 'run-real', status: realRunReads === 1 ? 'RUNNING' : 'SUCCEEDED' });
    }
    throw new Error(`Unexpected request: ${url}`);
  };

  const result = await seedLocalJobs({
    apiUrl: 'http://localhost:8080',
    username: 'admin',
    password: 'synthetic-test-password',
    fetchImpl,
    minimumRuns: 0
  });

  const datasourcePosts = calls.filter(call => call.options.method === 'POST'
    && call.url.endsWith('/api/v1/datasources'));
  const jobPosts = calls.filter(call => call.options.method === 'POST'
    && call.url.endsWith('/api/v1/jobs'));
  assert.equal(result.datasourcesCreated, SOURCE_FIXTURES.length + 2);
  assert.equal(result.created, SOURCE_FIXTURES.length);
  assert.equal(result.integrationJobCreated, true);
  assert.equal(result.integrationRunId, 'run-real');
  assert.equal(result.runsCreated, 0);
  assert.equal(datasourcePosts.length, SOURCE_FIXTURES.length + 2);
  assert.equal(jobPosts.length, SOURCE_FIXTURES.length + 1);
  assert.equal(calls.findIndex(call => call.url.endsWith('/api/v1/datasources'))
    < calls.findIndex(call => call.url.endsWith('/api/v1/jobs')), true);
  for (const call of jobPosts) {
    const payload = JSON.parse(call.options.body);
    assert.equal(payload.sourceConnect, undefined);
    assert.equal(payload.sinkConnect, undefined);
    assert.match(payload.sourceDatasourceId, /^datasource-/);
    assert.match(payload.sinkDatasourceId, /^datasource-/);
  }
});

test('adds watermark fields only to incremental fixtures and never embeds credentials', () => {
  for (const fixture of SOURCE_FIXTURES) {
    const payload = buildJobPayload(fixture, `source-${fixture.key}`, 'sink-postgres');
    assert.equal(payload.sourceDatasourceId, `source-${fixture.key}`);
    assert.equal(payload.sinkDatasourceId, 'sink-postgres');
    assert.equal(payload.sourceConnect, undefined);
    assert.equal(payload.sinkConnect, undefined);
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

test('triggers a real integration run without the synthetic seed header', async () => {
  const calls = [];
  let statusReads = 0;
  const fetchImpl = async (url, options = {}) => {
    calls.push({ url, options });
    if (url.endsWith('/api/v1/jobs/job-real/runs')) {
      return jsonResponse({ id: 'run-real', status: 'PENDING' }, 202);
    }
    if (url.endsWith('/api/v1/runs/run-real')) {
      statusReads += 1;
      return jsonResponse({ id: 'run-real', status: statusReads === 1 ? 'RUNNING' : 'SUCCEEDED' });
    }
    throw new Error(`Unexpected request: ${url}`);
  };

  const run = await runRealIntegration({
    fetchImpl,
    cookieJar: new Map(),
    apiUrl: 'http://localhost:8080',
    jobId: 'job-real',
    csrfToken: 'csrf-token',
    pollIntervalMs: 0,
    timeoutSeconds: 1
  });

  assert.equal(run.status, 'SUCCEEDED');
  const trigger = calls.find(call => call.options.method === 'POST');
  assert.equal(trigger.options.headers.get('X-ReplicaDB-Local-Seed'), null);
  assert.match(trigger.options.headers.get('Idempotency-Key'), /^local-integration-job-real-/);
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
