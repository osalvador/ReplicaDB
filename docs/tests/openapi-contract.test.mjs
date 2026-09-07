import assert from 'node:assert/strict';
import { readFileSync, existsSync } from 'node:fs';
import { join } from 'node:path';
import test from 'node:test';

const docsRoot = new URL('..', import.meta.url).pathname;
const schemaPath = join(docsRoot, 'openapi/replicadb-server.json');

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
  for (const prohibited of ['leaseToken', 'encryptedSecurity', 'sourcePassword', 'sinkPassword', 'org.replicadb.server']) {
    assert.equal(allStrings.includes(prohibited), false, prohibited);
  }
  assert.doesNotMatch(allStrings, /jdbc:[^\s"']+:\/\/[^\s"']*:[^\s"']*@/i);
  assert.doesNotMatch(allStrings, /-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----/i);
});