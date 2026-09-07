import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import test from 'node:test';

const docsRoot = new URL('..', import.meta.url).pathname;
const contentRoot = join(docsRoot, 'src/content/docs/getting-started');

/** @param {string} name */
function readGuide(name) {
  return readFileSync(join(contentRoot, name), 'utf8');
}

/** @param {string} text */
function shellBlocks(text) {
  return [...text.matchAll(/```(?:bash|sh)\n([\s\S]*?)```/g)].map((match) => match[1]);
}

test('contains the product decision path, quickstarts, and glossary terms', () => {
  const homepage = readFileSync(join(docsRoot, 'src/content/docs/index.mdx'), 'utf8');
  const choice = readGuide('choose-cli-or-server.mdx');
  const concepts = readGuide('concepts.md');

  for (const required of ['Standalone CLI', 'Managed Server', 'product comparison', 'CLI quickstart', 'Server quickstart']) {
    assert.match(`${homepage}\n${choice}`, new RegExp(required, 'i'));
  }
  for (const term of ['Source', 'Sink', 'Job', 'Task', 'Run', 'Attempt', 'Datasource', 'Watermark', 'API', 'Worker']) {
    assert.match(concepts, new RegExp(`^## ${term}$`, 'm'));
  }
});

test('quickstarts link to installation, configuration, troubleshooting, and security follow-ups', () => {
  for (const name of ['cli-quickstart.md', 'server-quickstart.md']) {
    const guide = readGuide(name);
    for (const path of ['installation', 'configuration', 'troubleshooting', 'security-and-tls']) {
      assert.match(guide, new RegExp(`/ReplicaDB/(?:cli|server|operations)/[^)]*${path}/`), `${name}: ${path}`);
    }
  }
});

test('shell examples parse after documentation placeholders are made inert', () => {
  const guides = ['cli-quickstart.md', 'server-quickstart.md'].map(readGuide);
  for (const guide of guides) {
    for (const block of shellBlocks(guide)) {
      const inert = block.replace(/<[^>]+>/g, 'placeholder');
      execFileSync('bash', ['-n'], { input: inert, encoding: 'utf8' });
    }
  }
});

test('quickstarts do not contain literal credential or secret assignments', () => {
  const quickstarts = ['cli-quickstart.md', 'server-quickstart.md'].map(readGuide).join('\n');
  assert.doesNotMatch(quickstarts, /(?:password|secret|token)\s*=\s*["'][^$<{\n]+["']/i);
  assert.doesNotMatch(quickstarts, /(?:password|secret|token)\s*:\s*["'][^$<{\n]+["']/i);
});