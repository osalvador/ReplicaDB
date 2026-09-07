import assert from 'node:assert/strict';
import { existsSync, mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import test from 'node:test';

const docsRoot = new URL('..', import.meta.url).pathname;
const repoRoot = new URL('../..', import.meta.url).pathname;
const schemaPath = join(docsRoot, 'openapi/replicadb-server.json');
const apiGuide = readFileSync(join(docsRoot, 'src/content/docs/api/index.md'), 'utf8');
const astroConfiguration = readFileSync(join(docsRoot, 'astro.config.mjs'), 'utf8');
const openApiConfiguration = readFileSync(join(repoRoot, 'replicadb-server/src/main/java/org/replicadb/server/security/config/OpenApiConfiguration.java'), 'utf8');
const securityConfiguration = readFileSync(join(repoRoot, 'replicadb-server/src/main/java/org/replicadb/server/security/config/SecurityConfig.java'), 'utf8');
const exceptionHandler = readFileSync(join(repoRoot, 'replicadb-server/src/main/java/org/replicadb/server/job/api/GlobalExceptionHandler.java'), 'utf8');

/** @param {any} value @returns {any} */
function canonicalize(value) {
  if (Array.isArray(value)) return value.map(canonicalize);
  if (value && typeof value === 'object') return Object.fromEntries(Object.keys(value).sort().map((key) => [key, canonicalize(value[key])]));
  return value;
}

/** @param {any} value @param {string[]} output @returns {string[]} */
function strings(value, output = []) {
  if (typeof value === 'string') output.push(value);
  else if (Array.isArray(value)) value.forEach((item) => strings(item, output));
  else if (value && typeof value === 'object') Object.entries(value).forEach(([key, item]) => { output.push(key); strings(item, output); });
  return output;
}

/** @param {any} value @param {string[]} output @returns {string[]} */
function references(value, output = []) {
  if (Array.isArray(value)) value.forEach((item) => references(item, output));
  else if (value && typeof value === 'object') {
    if (typeof value.$ref === 'string') output.push(value.$ref);
    Object.values(value).forEach((item) => references(item, output));
  }
  return output;
}

/** @param {any} schema @param {string} reference */
function resolveReference(schema, reference) {
  assert.match(reference, /^#\//, reference);
  return reference.slice(2).split('/').reduce((value, segment) => value?.[segment.replaceAll('~1', '/').replaceAll('~0', '~')], schema);
}

/** @param {any} schema @param {string} root */
function assertGeneratedOperationPages(schema, root) {
  const missing = [];
  for (const pathItem of Object.values(schema.paths)) {
    for (const operation of Object.values(pathItem)) {
      if (!operation?.operationId) continue;
      const path = `api/operations/${operation.operationId.toLowerCase()}/index.html`;
      if (!existsSync(join(root, path))) missing.push(path);
    }
  }
  assert.deepEqual(missing, [], `missing generated OpenAPI pages: ${missing.join(', ')}`);
}

test('committed OpenAPI JSON is canonical and grouped contract data exists', () => {
  assert.ok(existsSync(schemaPath));
  const source = readFileSync(schemaPath, 'utf8');
  const schema = JSON.parse(source);
  assert.equal(source, `${JSON.stringify(canonicalize(schema), null, 2)}\n`);
  assert.ok(schema.info);
  assert.ok(schema.servers);
  for (const path of ['/api/v1/jobs', '/api/v1/runs', '/api/v1/dashboard/summary', '/api/v1/datasources', '/api/v1/audit']) {
    assert.ok(schema.paths[path], path);
  }
  for (const group of ['job', 'run', 'dashboard', 'datasource', 'security', 'audit']) {
    assert.ok(strings(schema).some((value) => value.toLowerCase().includes(group)), group);
  }
});

test('schema contains no implementation-only or sensitive fields', () => {
  const schema = JSON.parse(readFileSync(schemaPath, 'utf8'));
  const allStrings = strings(schema).join('\n');
  for (const prohibited of ['leaseToken', 'encryptedSecurity', 'sourcePassword', 'sinkPassword', 'org.replicadb.server', 'X-ReplicaDB-Local-Seed', 'x-replicadb-local-seed']) {
    assert.equal(allStrings.includes(prohibited), false, prohibited);
  }
  assert.doesNotMatch(allStrings, /jdbc:[^\s"']+:\/\/[^\s"']*:[^\s"']*@/i);
  assert.doesNotMatch(allStrings, /-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----/i);
});

test('committed schema exposes 37 intentional, resolvable public operations', () => {
  const schema = JSON.parse(readFileSync(schemaPath, 'utf8'));
  const methods = new Set(['get', 'post', 'put', 'delete', 'patch']);
  const domainTags = new Set(['Authentication', 'Dashboard', 'Datasources', 'Datasource permissions', 'Jobs', 'Job permissions', 'Schedules', 'Runs', 'Users', 'Audit']);
  const publicOperations = new Set(['login', 'getCsrfToken']);
  const operations = [];

  for (const [path, pathItem] of Object.entries(schema.paths)) {
    for (const [method, operation] of Object.entries(pathItem)) {
      if (!methods.has(method)) continue;
      operations.push(operation);
      assert.ok(operation.operationId, `${method} ${path}: operationId`);
      assert.doesNotMatch(operation.operationId, /_\d+$/, operation.operationId);
      assert.ok(operation.summary?.trim(), `${operation.operationId}: summary`);
      assert.ok(operation.description?.trim(), `${operation.operationId}: description`);
      assert.equal(operation.tags?.length, 1, `${operation.operationId}: tag count`);
      assert.ok(domainTags.has(operation.tags[0]), `${operation.operationId}: ${operation.tags[0]}`);
      assert.ok(Object.keys(operation.responses ?? {}).length > 0, `${operation.operationId}: responses`);
      if (publicOperations.has(operation.operationId)) {
        assert.equal(operation.security, undefined, `${operation.operationId}: public`);
      } else {
        assert.ok(operation.security?.[0]?.sessionCookie, `${operation.operationId}: sessionCookie`);
        if (['post', 'put', 'delete', 'patch'].includes(method)) {
          assert.ok(operation.security[0].csrfHeader, `${operation.operationId}: csrfHeader`);
        }
      }
    }
  }
  assert.equal(operations.length, 37);

  for (const reference of references(schema)) {
    assert.ok(resolveReference(schema, reference), `unresolved reference: ${reference}`);
  }

  assert.ok(schema.paths['/api/v1/datasources'].post.responses['201']);
  assert.ok(schema.paths['/api/v1/jobs/{jobDefinitionId}/runs'].post.responses['202']);
  assert.ok(schema.paths['/api/v1/jobs/{id}'].delete.responses['204']);
  assert.ok(schema.paths['/api/v1/jobs/{id}'].delete.responses['409']);
  assert.equal(schema.components.schemas.LoginRequest.properties.password.writeOnly, true);
  assert.equal(schema.components.schemas.DatasourceRequest.properties.security.writeOnly, true);
  assert.ok(schema.components.schemas.JobDefinitionRequest.properties.mode.description);
  assert.ok(schema.components.schemas.JobRunResponse.properties.status.enum.length > 0);
});

test('server source owns shared OpenAPI metadata, security, and RFC 7807 components', () => {
  assert.match(openApiConfiguration, /title\("ReplicaDB Server API"\)/);
  assert.match(openApiConfiguration, /version\("v1"\)/);
  for (const tag of [
    'Authentication', 'Dashboard', 'Datasources', 'Datasource permissions', 'Jobs',
    'Job permissions', 'Schedules', 'Runs', 'Users', 'Audit'
  ]) {
    assert.match(openApiConfiguration, new RegExp(`tag\\("${tag}"`), tag);
  }
  for (const component of [
    'sessionCookie', 'csrfHeader', 'JSESSIONID', 'X-XSRF-TOKEN', 'ProblemDetail',
    'BadRequestProblem', 'UnauthorizedProblem', 'ForbiddenProblem', 'NotFoundProblem',
    'ConflictProblem', 'TooManyRequestsProblem', 'InternalServerErrorProblem'
  ]) {
    assert.match(openApiConfiguration, new RegExp(component), component);
  }
  assert.match(openApiConfiguration, /application\/problem\+json|APPLICATION_PROBLEM_JSON_VALUE/);
  assert.match(securityConfiguration, /"\/v3\/api-docs\/\*\*"\)\.permitAll\(\)/);
  assert.match(securityConfiguration, /\.anyRequest\(\)\.authenticated\(\)/);
  assert.match(exceptionHandler, /CredentialRedactor\.redactMessage/);
});

test('human API guide covers cross-cutting integration and links every generated domain', () => {
  for (const topic of [
    '/api/v1', 'JSESSIONID', 'X-XSRF-TOKEN', 'ADMIN', 'VIEW', 'page', 'size',
    'UTC ISO-8601', 'Idempotency-Key', '202', '204', 'application/problem+json', 'RFC 7807'
  ]) {
    assert.match(apiGuide, new RegExp(topic.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'i'), topic);
  }
  for (const tag of [
    'authentication', 'dashboard', 'datasources', 'datasource-permissions', 'jobs',
    'job-permissions', 'schedules', 'runs', 'users', 'audit'
  ]) {
    assert.match(apiGuide, new RegExp(`/ReplicaDB/api/operations/tags/${tag}/`), tag);
  }
  assert.match(astroConfiguration, /operations:\s*\{\s*labels:\s*'summary',\s*sort:\s*'document'\s*\}/);
  assert.match(astroConfiguration, /tags:\s*\{\s*sort:\s*'document'\s*\}/);
  assert.doesNotMatch(apiGuide, /Swagger UI|Try it out|interactive request/i);
});

test('built reference contains domain overviews and stable operation pages', { skip: !existsSync(join(docsRoot, 'dist')) }, () => {
  const schema = JSON.parse(readFileSync(schemaPath, 'utf8'));
  assertGeneratedOperationPages(schema, join(docsRoot, 'dist'));
  for (const path of [
    'api/operations/tags/authentication/index.html',
    'api/operations/tags/jobs/index.html',
    'api/operations/tags/runs/index.html',
    'api/operations/login/index.html',
    'api/operations/triggerjobrun/index.html',
    'api/operations/listauditevents/index.html'
  ]) {
    assert.ok(existsSync(join(docsRoot, 'dist', path)), path);
  }
  assert.match(readFileSync(join(docsRoot, 'dist/api/operations/tags/jobs/index.html'), 'utf8'), /<title>Jobs API \|/);
});

test('rejects a missing generated OpenAPI operation page fixture', (context) => {
  const fixtureRoot = mkdtempSync(join(tmpdir(), 'replicadb-openapi-pages-'));
  context.after(() => rmSync(fixtureRoot, { recursive: true, force: true }));
  const schema = { paths: { '/items': { get: { operationId: 'listItems' } } } };
  assert.throws(() => assertGeneratedOperationPages(schema, fixtureRoot), /missing generated OpenAPI pages:[\s\S]*listitems/);
});
