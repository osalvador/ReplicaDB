import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import test from 'node:test';
import { generateApiTypes, generationOptions } from './generate-api-types.mjs';

const fixture = JSON.stringify({
  openapi: '3.0.3',
  info: { title: 'Fixture API', version: 'v1' },
  paths: { '/items': { get: { operationId: 'listItems', responses: { 200: { description: 'OK' } } } } }
});

test('prefers an explicit schema file and output path', () => {
  const options = generationOptions(['--input-file', './fixture.json', '--output', './generated.ts'], {});
  assert.equal(options.input.endsWith('/fixture.json'), true);
  assert.equal(options.output.endsWith('/generated.ts'), true);
});

test('generates byte-identical TypeScript from the same schema file', (context) => {
  const directory = mkdtempSync(join(tmpdir(), 'replicadb-api-types-'));
  context.after(() => rmSync(directory, { recursive: true, force: true }));
  const input = join(directory, 'openapi.json');
  const output = join(directory, 'schema.ts');
  writeFileSync(input, fixture);

  generateApiTypes({ input, output, stdio: 'ignore' });
  const first = readFileSync(output, 'utf8');
  generateApiTypes({ input, output, stdio: 'ignore' });
  assert.equal(readFileSync(output, 'utf8'), first);
  assert.match(first, /listItems/);
});

test('does not replace existing output when generation fails', (context) => {
  const directory = mkdtempSync(join(tmpdir(), 'replicadb-api-types-failure-'));
  context.after(() => rmSync(directory, { recursive: true, force: true }));
  const output = join(directory, 'schema.ts');
  writeFileSync(output, 'existing output\n');

  assert.throws(() => generateApiTypes({ input: join(directory, 'missing.json'), output, stdio: 'ignore' }), /exited with status/);
  assert.equal(readFileSync(output, 'utf8'), 'existing output\n');
});
