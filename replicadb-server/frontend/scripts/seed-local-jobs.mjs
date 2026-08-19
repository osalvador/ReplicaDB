import { pathToFileURL } from 'node:url';

const DEFAULT_API_URL = 'http://localhost:8080';
const DEFAULT_TABLE = 'sample_orders';
const DEFAULT_POSTGRES_PORT = process.env.REPLICADB_POSTGRES_PORT ?? '5432';

export const SOURCE_FIXTURES = [
  {
    key: 'oracle',
    label: 'Oracle',
    connect: 'jdbc:oracle:thin:@//localhost:1521/ORCL',
    mode: 'complete'
  },
  {
    key: 'mysql',
    label: 'MySQL',
    connect: 'jdbc:mysql://localhost:3306/replicadb',
    mode: 'complete-atomic'
  },
  {
    key: 'mariadb',
    label: 'MariaDB',
    connect: 'jdbc:mariadb://localhost:3306/replicadb',
    mode: 'incremental'
  },
  {
    key: 'postgres',
    label: 'PostgreSQL',
    connect: 'jdbc:postgresql://localhost:5432/replicadb',
    mode: 'complete'
  },
  {
    key: 'db2',
    label: 'DB2 LUW',
    connect: 'jdbc:db2://localhost:50000/replicadb',
    mode: 'complete-atomic'
  },
  {
    key: 'db2i',
    label: 'DB2 for i',
    connect: 'jdbc:as400://localhost:446/replicadb',
    mode: 'incremental'
  },
  {
    key: 'sqlite',
    label: 'SQLite',
    connect: 'jdbc:sqlite:/tmp/replicadb-develop-source.db',
    mode: 'complete'
  },
  {
    key: 'sqlserver',
    label: 'SQL Server',
    connect: 'jdbc:sqlserver://localhost:1433;database=replicadb',
    mode: 'complete-atomic'
  },
  {
    key: 'denodo',
    label: 'Denodo',
    connect: 'jdbc:vdb://localhost:9999/replicadb',
    mode: 'incremental'
  },
  {
    key: 'file',
    label: 'File',
    connect: 'file:///tmp/replicadb-develop-source.csv',
    mode: 'complete'
  }
];

export function buildJobPayload(fixture) {
  const payload = {
    name: `Develop / ${fixture.label} source`,
    sourceConnect: fixture.connect,
    sourceTable: DEFAULT_TABLE,
    sourceColumns: 'id, payload',
    sinkConnect: `jdbc:postgresql://localhost:${DEFAULT_POSTGRES_PORT}/replicadb`,
    sinkTable: `sample_${fixture.key}_orders`,
    sinkColumns: 'id, payload',
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

export async function seedLocalJobs({
  apiUrl = process.env.REPLICADB_API_URL ?? DEFAULT_API_URL,
  username = process.env.REPLICADB_BOOTSTRAP_ADMIN_USERNAME,
  password = process.env.REPLICADB_BOOTSTRAP_ADMIN_PASSWORD,
  fetchImpl = globalThis.fetch
} = {}) {
  if (!username || !password) {
    throw new Error('REPLICADB_BOOTSTRAP_ADMIN_USERNAME and REPLICADB_BOOTSTRAP_ADMIN_PASSWORD must be set.');
  }
  if (typeof fetchImpl !== 'function') {
    throw new Error('A fetch implementation is required.');
  }

  const cookieJar = new Map();
  const csrfToken = await authenticate(fetchImpl, cookieJar, apiUrl, username, password);
  const page = await requestJson(fetchImpl, cookieJar, apiUrl, '/api/v1/jobs?page=0&size=100');
  const existingNames = new Set((page?.content ?? []).map(job => job.name).filter(Boolean));
  let created = 0;
  let skipped = 0;

  for (const fixture of SOURCE_FIXTURES) {
    const payload = buildJobPayload(fixture);
    if (existingNames.has(payload.name)) {
      skipped += 1;
      continue;
    }

    await requestJson(fetchImpl, cookieJar, apiUrl, '/api/v1/jobs', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(payload)
    }, csrfToken);
    created += 1;
  }

  return { created, skipped, total: SOURCE_FIXTURES.length };
}

async function main() {
  const result = await seedLocalJobs();
  console.log(`Local job fixtures ready: ${result.created} created, ${result.skipped} already present.`);
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch(error => {
    console.error(`Could not seed local job fixtures: ${error.message}`);
    process.exitCode = 1;
  });
}
