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

/** @param {string} phrase */
function phrasePattern(phrase) {
  const escapedWords = phrase.split(/\s+/).map((word) => word.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'));
  return new RegExp(escapedWords.join('\\s+'), 'i');
}

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
  const propertyDocs = docs.replace(/^import .* from .*;$/gm, '');
  const documentedProperties = new Set((propertyDocs.match(/\b(?:mode|jobs|verbose|fetch\.size|bandwidth\.throttling|quoted\.identifiers|source|sink|incremental|replication|sentry)(?:\.[a-z0-9{}_-]+)+\b/g) || []));
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

test('covers the complete CLI workflow in its owning guides', () => {
  const requiredTopics = {
    'index.md': ['one invocation', 'external scheduler', 'change-data-capture'],
    'installation.md': ['Java 17', 'replicadb.cmd', 'external JDBC driver'],
    'configuration.md': ['applying explicit flags', 'defaults to 4', 'sentry.dsn'],
    'parallelism.md': ['per worker', '40,960 KB/s', 'connections'],
    'filtering-and-queries.md': ['source.where', 'source.query', 'sink.columns'],
    'multi-table.md': ['sequentially', 'first failure', 'not rolled back'],
    'incremental-watermarks.md': ['strict greater-than', 'monotonically increasing', 'code 2'],
    'performance.md': ['source plan', 'sink write strategy', 'lock waits'],
    'troubleshooting.md': ['verbose=DEBUG', 'conversion failure', 'staging failure']
  };

  for (const [file, topics] of Object.entries(requiredTopics)) {
    const guide = readFileSync(join(cliRoot, file), 'utf8');
    for (const topic of topics) {
      assert.match(guide, phrasePattern(topic), `${file}: ${topic}`);
    }
  }
});

test('documents mode-specific flow and interruption consequences', () => {
  const modes = readFileSync(join(cliRoot, 'replication-modes.mdx'), 'utf8');
  for (const behavior of [
    'clearing the sink',
    'empty or partially repopulated',
    'staging table',
    'atomic replacement transaction',
    'merge matched keys',
    'does not infer deleted source rows',
    'retry starts the complete flow again'
  ]) {
    assert.match(modes, phrasePattern(behavior), behavior);
  }
});

test('documents file formats, defaults, precedence, and safe optional telemetry', () => {
  const options = readFileSync(join(referenceRoot, 'cli-options.md'), 'utf8');
  const examples = readFileSync(join(referenceRoot, 'example-options-files.md'), 'utf8');
  for (const format of ['csv', 'json', 'avro', 'parquet', 'orc']) {
    assert.match(options, new RegExp(`source file format:[^\\n]*${format}`, 'i'));
    assert.match(options, new RegExp(`sink file format:[^\\n]*${format}`, 'i'));
  }
  assert.match(options, /mode=complete[\s\S]*jobs=4[\s\S]*fetch\.size=100/i);
  assert.match(examples, /--jobs 2[\s\S]*wins over `jobs=1`/i);
  assert.match(examples, /sentry\.dsn=\$\{SENTRY_DSN\}/);
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
