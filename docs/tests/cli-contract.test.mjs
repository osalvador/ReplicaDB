import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { readFileSync, readdirSync } from 'node:fs';
import { join } from 'node:path';
import test from 'node:test';

const repoRoot = new URL('../..', import.meta.url).pathname;
const docsRoot = new URL('..', import.meta.url).pathname;
const cliRoot = join(docsRoot, 'src/content/docs/cli');
const referenceRoot = join(docsRoot, 'src/content/docs/reference');

/** @param {string} directory @returns {string[]} */
function filesUnder(directory) {
  return readdirSync(directory, { withFileTypes: true }).flatMap((entry) => {
    const path = join(directory, entry.name);
    return entry.isDirectory() ? filesUnder(path) : [path];
  });
}

/** @param {string} directory */
function readFiles(directory) {
  return filesUnder(directory).map((path) => readFileSync(path, 'utf8')).join('\n');
}

const docs = `${readFiles(cliRoot)}\n${readFiles(referenceRoot)}`;
const toolOptions = readFileSync(join(repoRoot, 'src/main/java/org/replicadb/cli/ToolOptions.java'), 'utf8');
const sampleConfig = readFileSync(join(repoRoot, 'conf/_replicadb.conf'), 'utf8');
const compatibilityGate = readFileSync(join(repoRoot, 'scripts/phase3-cli-compatibility.sh'), 'utf8');

const documentedDeprecationAllowlist = new Set();

test('documents only long options present in the CLI evidence set', () => {
  const evidenceOptions = new Set([
    ...toolOptions.matchAll(/\.longOpt\("([a-z0-9-]+)"\)/g)
  ].map((match) => `--${match[1]}`));
  evidenceOptions.add('--help');
  evidenceOptions.add('--jobs');
  evidenceOptions.add('--verbose');

  const documentedOptions = new Set(docs.match(/--[a-z][a-z0-9-]*/g) || []);
  const unsupported = [...documentedOptions].filter((option) => !evidenceOptions.has(option) && !documentedDeprecationAllowlist.has(option));
  assert.deepEqual(unsupported, []);
  assert.match(compatibilityGate, /--options-file/);
});

test('documents properties present in the maintained options-file evidence', () => {
  const evidenceProperties = new Set([
    ...sampleConfig.matchAll(/^\s*#?\s*([a-z][a-z0-9]*(?:\.[a-z0-9{}_-]+)*)\s*=/gim),
    ...toolOptions.matchAll(/getProperty\("([a-z][a-z0-9]*(?:\.[a-z0-9{}_-]+)*)"\)/g)
  ].map((match) => match[1]));
  const documentedProperties = new Set((docs.match(/\b(?:mode|jobs|verbose|fetch\.size|bandwidth\.throttling|quoted\.identifiers|source|sink|incremental|replication|sentry)(?:\.[a-z0-9{}_-]+)+\b/g) || []));
  const unsupported = [...documentedProperties].filter((property) => {
    if (property.startsWith('source.connect.parameter') || property.startsWith('sink.connect.parameter')) return false;
    if (property === 'source.auth' || property === 'sink.auth' || property === 'replication.table') return false;
    if (property.startsWith('replication.table.')) return ![...evidenceProperties].some((evidence) => evidence.startsWith('replication.table.'));
    return !evidenceProperties.has(property);
  });
  assert.deepEqual(unsupported, []);
});

test('preserves the CLI behavior sections and contract limits', () => {
  for (const required of [
    'complete',
    'complete-atomic',
    'incremental',
    'complete-atomic',
    'replication.table.1.source',
    'incremental-watermark-column',
    'code 1',
    'code 2'
  ]) {
    assert.match(docs, new RegExp(required.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'i'), required);
  }
  assert.match(docs, /Command-line\s+arguments override/i);
});

test('keeps CLI guide examples free of resolved credentials', () => {
  assert.doesNotMatch(docs, /(?:password|secret|token)\s*=\s*["'][^$<{\n]+["']/i);
  assert.doesNotMatch(docs, /jdbc:[^\s"']+:\/\/[^\s"']*:[^\s"']*@/i);
});

test('CLI bash examples parse after placeholders are made inert', () => {
  for (const match of docs.matchAll(/```bash\n([\s\S]*?)```/g)) {
    const inert = match[1].replace(/<[^>]+>/g, 'placeholder');
    execFileSync('bash', ['-n'], { input: inert, encoding: 'utf8' });
  }
});