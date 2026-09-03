import { pathToFileURL } from 'node:url';

const DEFAULT_API_URL = 'http://localhost:8080';
const DEFAULT_TABLE = 'sample_orders';
const DEFAULT_POSTGRES_PORT = process.env.REPLICADB_POSTGRES_PORT ?? '5432';
export const MINIMUM_RUNS_PER_JOB = 5;
const RUN_CLEANUP_ATTEMPTS = 40;
const RUN_CLEANUP_DELAY_MS = 100;
const LOCAL_INTEGRATION_POLL_INTERVAL_MS = Number.parseInt(
  process.env.REPLICADB_LOCAL_INTEGRATION_POLL_INTERVAL_MS ?? '500', 10
);
const LOCAL_INTEGRATION_TIMEOUT_SECONDS = Number.parseInt(
  process.env.REPLICADB_LOCAL_INTEGRATION_TIMEOUT_SECONDS ?? '120', 10
);
const ACTIVE_RUN_STATUSES = new Set(['PENDING', 'RUNNING', 'CANCEL_REQUESTED']);
const TERMINAL_RUN_STATUSES = new Set(['SUCCEEDED', 'FAILED', 'CANCELLED', 'RETRY_SCHEDULED']);
export const PG2PG_FIXTURE = {
  key: 'pg2pg',
  label: 'pg2pg',
  datasourceName: 'Pglocal',
  connectorType: 'postgres',
  connect: `jdbc:postgresql://localhost:${DEFAULT_POSTGRES_PORT}/replicadb?currentSchema=pg2pg`,
  user: 'postgres',
  sourceTable: 'pg2pg.pg2pg_source_orders',
  sinkTable: 'pg2pg.pg2pg_destination_orders',
  columns: 'id, payload',
  mode: 'complete',
  expectedRows: 3
};

export const SOURCE_FIXTURES = [
  {
    key: 'oracle',
    label: 'Oracle',
    connectorType: 'oracle',
    connect: 'jdbc:oracle:thin:@//localhost:1521/ORCL',
    mode: 'complete'
  },
  {
    key: 'mysql',
    label: 'MySQL',
    connectorType: 'mysql',
    connect: 'jdbc:mysql://localhost:3306/replicadb',
    mode: 'complete-atomic'
  },
  {
    key: 'mariadb',
    label: 'MariaDB',
    connectorType: 'mariadb',
    connect: 'jdbc:mariadb://localhost:3306/replicadb',
    mode: 'incremental'
  },
  {
    key: 'postgres',
    label: 'PostgreSQL',
    connectorType: 'postgres',
    connect: `jdbc:postgresql://localhost:${DEFAULT_POSTGRES_PORT}/replicadb`,
    mode: 'complete'
  },
  {
    key: 'db2',
    label: 'DB2 LUW',
    connectorType: 'db2',
    connect: 'jdbc:db2://localhost:50000/replicadb',
    mode: 'complete-atomic'
  },
  {
    key: 'db2i',
    label: 'DB2 for i',
    connectorType: 'db2-as400',
    connect: 'jdbc:as400://localhost:446/replicadb',
    mode: 'incremental'
  },
  {
    key: 'sqlite',
    label: 'SQLite',
    connectorType: 'sqlite',
    connect: 'jdbc:sqlite:/tmp/replicadb-develop-source.db',
    mode: 'complete'
  },
  {
    key: 'sqlserver',
    label: 'SQL Server',
    connectorType: 'sqlserver',
    connect: 'jdbc:sqlserver://localhost:1433;database=replicadb',
    mode: 'complete-atomic'
  },
  {
    key: 'denodo',
    label: 'Denodo',
    connectorType: 'denodo',
    connect: 'jdbc:vdb://localhost:9999/replicadb',
    mode: 'incremental'
  },
  {
    key: 'file',
    label: 'File',
    connectorType: 'file',
    connect: 'file:///tmp/replicadb-develop-source.csv',
    mode: 'complete'
  }
];

export const SINK_FIXTURE = {
  key: 'postgres-sink',
  label: 'PostgreSQL',
  connectorType: 'postgres',
  connect: `jdbc:postgresql://localhost:${DEFAULT_POSTGRES_PORT}/replicadb`
};

export function buildDatasourceName(fixture, role = 'source') {
  if (fixture.datasourceName) {
    return fixture.datasourceName;
  }
  return `Develop / ${fixture.label} ${role} datasource`;
}

export function buildDatasourcePayload(fixture, role = 'source') {
  return {
    name: buildDatasourceName(fixture, role),
    connectorType: fixture.connectorType,
    technicalParams: {},
    security: {
      connect: fixture.connect,
      ...(fixture.user ? { user: fixture.user } : {})
    },
    clearSecurityKeys: []
  };
}

export function buildJobPayload(fixture, sourceDatasourceId, sinkDatasourceId) {
  if (!sourceDatasourceId || !sinkDatasourceId) {
    throw new Error(`Datasource IDs are required to seed the ${fixture.label} job.`);
  }

  const payload = {
    name: fixture.jobName ?? `Develop / ${fixture.label} source`,
    sourceDatasourceId,
    sourceDatasourceUseEnabled: true,
    sourceTable: fixture.sourceTable ?? DEFAULT_TABLE,
    sourceColumns: fixture.columns ?? 'id, payload',
    sinkDatasourceId,
    sinkDatasourceUseEnabled: true,
    sinkTable: fixture.sinkTable ?? `sample_${fixture.key}_orders`,
    sinkColumns: fixture.columns ?? 'id, payload',
    ...(fixture.stagingSchema ? { sinkStagingSchema: fixture.stagingSchema } : {}),
    mode: fixture.mode,
    jobs: 1,
    fetchSize: 100,
    bandwidthThrottling: 0,
    verbose: false
  };

  if (fixture.mode === 'incremental') {
    payload.incrementalWatermarkColumn = 'updated_at';
    payload.initialWatermarkValue = '0';
  }

  return payload;
}

function updateCookies(cookieJar, response) {
  const setCookies = typeof response.headers.getSetCookie === 'function'
    ? response.headers.getSetCookie()
    : [response.headers.get('set-cookie')].filter(Boolean);

  for (const setCookie of setCookies) {
    const separator = setCookie.indexOf(';');
    const pair = separator < 0 ? setCookie : setCookie.slice(0, separator);
    const equals = pair.indexOf('=');
    if (equals > 0) {
      cookieJar.set(pair.slice(0, equals), pair.slice(equals + 1));
    }
  }
}

function cookieHeader(cookieJar) {
  return [...cookieJar.entries()].map(([name, value]) => `${name}=${value}`).join('; ');
}

async function readResponseBody(response) {
  const text = await response.text();
  if (!text) {
    return undefined;
  }
  try {
    return JSON.parse(text);
  } catch {
    return { detail: text };
  }
}

async function requestJson(fetchImpl, cookieJar, apiUrl, path, options = {}, csrfToken) {
  const headers = new Headers(options.headers);
  const cookies = cookieHeader(cookieJar);
  if (cookies) {
    headers.set('cookie', cookies);
  }
  if (csrfToken) {
    headers.set('X-XSRF-TOKEN', csrfToken);
  }

  const response = await fetchImpl(`${apiUrl}${path}`, {
    ...options,
    headers
  });
  updateCookies(cookieJar, response);
  const body = await readResponseBody(response);
  if (!response.ok) {
    const detail = body?.detail ?? body?.title ?? `HTTP ${response.status}`;
    throw new Error(`${options.method ?? 'GET'} ${path} failed: ${detail}`);
  }
  return body;
}

async function wait(milliseconds) {
  await new Promise(resolve => setTimeout(resolve, milliseconds));
}

async function authenticate(fetchImpl, cookieJar, apiUrl, username, password) {
  const csrf = await requestJson(fetchImpl, cookieJar, apiUrl, '/api/v1/auth/csrf');
  const csrfToken = csrf?.token;
  if (!csrfToken) {
    throw new Error('The API did not return a CSRF token.');
  }

  let lastError;
  for (let attempt = 1; attempt <= 30; attempt += 1) {
    try {
      await requestJson(fetchImpl, cookieJar, apiUrl, '/api/v1/auth/login', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ username, password })
      }, csrfToken);
      return csrfToken;
    } catch (error) {
      lastError = error;
      if (attempt < 30) {
        await wait(250);
      }
    }
  }

  throw lastError;
}

export function getMissingRunCount(totalRuns, minimumRuns = MINIMUM_RUNS_PER_JOB) {
  return Math.max(0, minimumRuns - totalRuns);
}

function isActiveRun(run) {
  return ACTIVE_RUN_STATUSES.has(run?.status);
}

function isActiveRunConflict(error) {
  return error instanceof Error && error.message.includes('already has an active run');
}

async function listJobRuns(fetchImpl, cookieJar, apiUrl, jobId) {
  return requestJson(fetchImpl, cookieJar, apiUrl, `/api/v1/jobs/${jobId}/runs?page=0&size=100`);
}

async function ensureDatasource({
  fetchImpl,
  cookieJar,
  apiUrl,
  existingDatasources,
  fixture,
  role,
  csrfToken
}) {
  const payload = buildDatasourcePayload(fixture, role);
  const existing = existingDatasources.get(payload.name);
  if (existing?.id) {
    return { datasource: existing, created: false };
  }

  const datasource = await requestJson(fetchImpl, cookieJar, apiUrl, '/api/v1/datasources', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(payload)
  }, csrfToken);
  if (!datasource?.id) {
    throw new Error(`The API did not return a datasource ID for ${payload.name}.`);
  }
  existingDatasources.set(payload.name, datasource);
  return { datasource, created: true };
}

async function ensureJob({
  fetchImpl,
  cookieJar,
  apiUrl,
  existingJobs,
  fixture,
  sourceDatasourceId,
  sinkDatasourceId,
  csrfToken
}) {
  const payload = buildJobPayload(fixture, sourceDatasourceId, sinkDatasourceId);
  const existing = existingJobs.get(payload.name);
  if (existing?.id) {
    return { job: existing, created: false };
  }

  const job = await requestJson(fetchImpl, cookieJar, apiUrl, '/api/v1/jobs', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(payload)
  }, csrfToken);
  if (!job?.id) {
    throw new Error(`The API did not return a job ID for ${payload.name}.`);
  }
  existingJobs.set(payload.name, job);
  return { job, created: true };
}

async function waitForRunToBecomeTerminal({
  fetchImpl,
  cookieJar,
  apiUrl,
  jobId,
  runId
}) {
  for (let attempt = 0; attempt < RUN_CLEANUP_ATTEMPTS; attempt += 1) {
    const run = await requestJson(fetchImpl, cookieJar, apiUrl, `/api/v1/runs/${runId}`);
    if (TERMINAL_RUN_STATUSES.has(run?.status)) {
      return run;
    }

    if (!isActiveRun(run)) {
      throw new Error(`The API returned an unexpected seeded run status: ${run?.status ?? 'unknown'} for job ${jobId}.`);
    }

    await wait(RUN_CLEANUP_DELAY_MS);
  }

  throw new Error(`Run ${runId} for job ${jobId} did not reach a terminal status.`);
}

async function waitForActiveRunsToFinish({ fetchImpl, cookieJar, apiUrl, jobId }, page) {
  for (const run of page?.content ?? []) {
    if (isActiveRun(run)) {
      await waitForRunToBecomeTerminal({
        fetchImpl,
        cookieJar,
        apiUrl,
        jobId,
        runId: run.id
      });
    }
  }
}

async function triggerSeededRun({ fetchImpl, cookieJar, apiUrl, jobId, csrfToken, index }) {
  for (let attempt = 0; attempt < RUN_CLEANUP_ATTEMPTS; attempt += 1) {
    try {
      return await requestJson(fetchImpl, cookieJar, apiUrl, `/api/v1/jobs/${jobId}/runs`, {
        method: 'POST',
        headers: {
          'Idempotency-Key': `local-seed-${jobId}-${Date.now()}-${index}-${attempt}`,
          'X-ReplicaDB-Local-Seed': 'true'
        }
      }, csrfToken);
    } catch (error) {
      if (!isActiveRunConflict(error)) {
        throw error;
      }

      const page = await listJobRuns(fetchImpl, cookieJar, apiUrl, jobId);
  await waitForActiveRunsToFinish({ fetchImpl, cookieJar, apiUrl, jobId }, page);
    }
  }

  throw new Error(`Could not create a seeded run for job ${jobId} because another run stayed active.`);
}

export async function runRealIntegration({
  fetchImpl,
  cookieJar,
  apiUrl,
  jobId,
  csrfToken,
  pollIntervalMs = LOCAL_INTEGRATION_POLL_INTERVAL_MS,
  timeoutSeconds = LOCAL_INTEGRATION_TIMEOUT_SECONDS
}) {
  const run = await requestJson(fetchImpl, cookieJar, apiUrl, `/api/v1/jobs/${jobId}/runs`, {
    method: 'POST',
    headers: {
      'Idempotency-Key': `local-integration-${jobId}-${Date.now()}`
    }
  }, csrfToken);
  if (!run?.id) {
    throw new Error(`The API did not return a run ID for integration job ${jobId}.`);
  }

  const attempts = Math.max(1, Math.ceil((timeoutSeconds * 1000) / pollIntervalMs));
  for (let attempt = 0; attempt < attempts; attempt += 1) {
    const current = await requestJson(fetchImpl, cookieJar, apiUrl, `/api/v1/runs/${run.id}`);
    if (current?.status === 'SUCCEEDED') {
      return current;
    }
    if (TERMINAL_RUN_STATUSES.has(current?.status)) {
      const detail = current.errorMessage ? `: ${current.errorMessage}` : '.';
      let logDetail = '';
      try {
        const log = await requestJson(fetchImpl, cookieJar, apiUrl, `/api/v1/runs/${run.id}/log`);
        const excerpt = typeof log?.excerpt === 'string' ? log.excerpt : log?.content;
        if (excerpt) {
          logDetail = ` Log: ${excerpt.slice(-2000)}`;
        }
      } catch {}
      throw new Error(`Integration run ${run.id} for job ${jobId} finished with ${current.status}${detail}${logDetail}`);
    }
    await wait(pollIntervalMs);
  }

  throw new Error(`Integration run ${run.id} for job ${jobId} did not succeed within ${timeoutSeconds} seconds.`);
}

export async function seedJobRunHistory({
  fetchImpl,
  cookieJar,
  apiUrl,
  jobId,
  csrfToken,
  minimumRuns = MINIMUM_RUNS_PER_JOB
}) {
  const page = await listJobRuns(fetchImpl, cookieJar, apiUrl, jobId);
  await waitForActiveRunsToFinish({ fetchImpl, cookieJar, apiUrl, jobId }, page);
  const totalRuns = Number.isInteger(page?.totalElements)
    ? page.totalElements
    : (page?.content ?? []).length;
  const missingRuns = getMissingRunCount(totalRuns, minimumRuns);

  for (let index = 0; index < missingRuns; index += 1) {
    const run = await triggerSeededRun({ fetchImpl, cookieJar, apiUrl, jobId, csrfToken, index });

    if (!run?.id) {
      throw new Error(`The API did not return a run ID for job ${jobId}.`);
    }
    if (run.status !== 'CANCELLED') {
      throw new Error(`The local seed API did not create a terminal run for job ${jobId}: ${run.status ?? 'unknown'}.`);
    }
  }

  return missingRuns;
}

export async function seedLocalJobs({
  apiUrl = process.env.REPLICADB_API_URL ?? DEFAULT_API_URL,
  username = process.env.REPLICADB_BOOTSTRAP_ADMIN_USERNAME,
  password = process.env.REPLICADB_BOOTSTRAP_ADMIN_PASSWORD,
  fetchImpl = globalThis.fetch,
  minimumRuns = MINIMUM_RUNS_PER_JOB
} = {}) {
  if (!username || !password) {
    throw new Error('REPLICADB_BOOTSTRAP_ADMIN_USERNAME and REPLICADB_BOOTSTRAP_ADMIN_PASSWORD must be set.');
  }
  if (typeof fetchImpl !== 'function') {
    throw new Error('A fetch implementation is required.');
  }

  const cookieJar = new Map();
  const csrfToken = await authenticate(fetchImpl, cookieJar, apiUrl, username, password);
  const datasourcePage = await requestJson(fetchImpl, cookieJar, apiUrl, '/api/v1/datasources?page=0&size=100');
  const existingDatasources = new Map((datasourcePage?.content ?? []).map(datasource => [datasource.name, datasource]));
  let datasourcesCreated = 0;

  const sinkResult = await ensureDatasource({
    fetchImpl,
    cookieJar,
    apiUrl,
    existingDatasources,
    fixture: SINK_FIXTURE,
    role: 'sink',
    csrfToken
  });
  datasourcesCreated += sinkResult.created ? 1 : 0;

  const localDatasourceResult = await ensureDatasource({
    fetchImpl,
    cookieJar,
    apiUrl,
    existingDatasources,
    fixture: PG2PG_FIXTURE,
    role: 'local',
    csrfToken
  });
  datasourcesCreated += localDatasourceResult.created ? 1 : 0;

  const sourceDatasources = new Map();
  for (const fixture of SOURCE_FIXTURES) {
    const result = await ensureDatasource({
      fetchImpl,
      cookieJar,
      apiUrl,
      existingDatasources,
      fixture,
      role: 'source',
      csrfToken
    });
    sourceDatasources.set(fixture.key, result.datasource);
    datasourcesCreated += result.created ? 1 : 0;
  }

  const page = await requestJson(fetchImpl, cookieJar, apiUrl, '/api/v1/jobs?page=0&size=100');
  const existingJobs = new Map((page?.content ?? []).map(job => [job.name, job]));
  const jobs = [];
  let created = 0;
  let skipped = 0;

  for (const fixture of SOURCE_FIXTURES) {
    const sourceDatasource = sourceDatasources.get(fixture.key);
    const result = await ensureJob({
      fetchImpl,
      cookieJar,
      apiUrl,
      existingJobs,
      fixture,
      sourceDatasourceId: sourceDatasource?.id,
      sinkDatasourceId: sinkResult.datasource.id,
      csrfToken
    });
    jobs.push(result.job);
    if (result.created) {
      created += 1;
    } else {
      skipped += 1;
    }
  }

  let runsCreated = 0;
  for (const job of jobs) {
    if (!job?.id) {
      throw new Error(`Cannot seed run history without a job ID for ${job?.name ?? 'unknown job'}.`);
    }
    runsCreated += await seedJobRunHistory({
      fetchImpl,
      cookieJar,
      apiUrl,
      jobId: job.id,
      csrfToken,
      minimumRuns
    });
  }

  const integrationJobResult = await ensureJob({
    fetchImpl,
    cookieJar,
    apiUrl,
    existingJobs,
    fixture: PG2PG_FIXTURE,
    sourceDatasourceId: localDatasourceResult.datasource.id,
    sinkDatasourceId: localDatasourceResult.datasource.id,
    csrfToken
  });
  const integrationRun = await runRealIntegration({
    fetchImpl,
    cookieJar,
    apiUrl,
    jobId: integrationJobResult.job.id,
    csrfToken
  });

  return {
    created,
    skipped,
    runsCreated,
    datasourcesCreated,
    total: SOURCE_FIXTURES.length,
    integrationJobCreated: integrationJobResult.created,
    integrationRunId: integrationRun.id
  };
}

async function main() {
  const result = await seedLocalJobs();
  console.log(`Local datasource/job fixtures ready: ${result.datasourcesCreated} datasources created, ${result.created} jobs created, ${result.skipped} jobs already present, ${result.runsCreated} run history entries created.`);
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch(error => {
    console.error(`Could not seed local job fixtures: ${error.message}`);
    process.exitCode = 1;
  });
}
