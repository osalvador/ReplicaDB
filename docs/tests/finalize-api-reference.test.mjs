import assert from 'node:assert/strict';
import { mkdtemp, mkdir, readFile, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';
import test from 'node:test';
import { finalizeApiReference } from '../scripts/finalize-api-reference.mjs';

test('finalizes generated API titles and keyboard-accessible code regions', async () => {
  const root = await mkdtemp(join(tmpdir(), 'replicadb-api-reference-'));
  const directory = join(root, 'api/operations/tags/jobs');
  const page = join(directory, 'index.html');
  const introductionDirectory = join(root, 'api-introduction');
  const introduction = join(introductionDirectory, 'index.html');
  await mkdir(directory, { recursive: true });
  await mkdir(introductionDirectory, { recursive: true });
  await writeFile(page, '<!doctype html><html><head><title>Overview | Docs</title></head>'
    + '<body><h1>Overview</h1><pre data-language="bash">long command</pre></body></html>');
  await writeFile(introduction, '<!doctype html><html><body><pre data-language="json">problem</pre></body></html>');

  await finalizeApiReference(pathToFileURL(`${root}/`));

  const result = await readFile(page, 'utf8');
  assert.match(result, /<title>Jobs API \| Docs<\/title>/);
  assert.match(result, /<h1>Jobs API<\/h1>/);
  assert.match(result, /<pre data-language="bash" tabindex="0">/);
  assert.match(await readFile(introduction, 'utf8'), /<pre data-language="json" tabindex="0">/);
});
