import assert from 'node:assert/strict';
import { mkdtemp, mkdir, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import test from 'node:test';
import { prohibitedPatterns, validateBuiltDocs } from '../scripts/validate-docs.mjs';

async function fixture() {
  const root = await mkdtemp(join(tmpdir(), 'replicadb-docs-validator-'));
  for (const directory of ['pagefind', 'server', 'api', 'cli', 'connectors', 'operations', 'reference', 'wizard', 'markdown']) await mkdir(join(root, directory), { recursive: true });
  for (const [path, title] of [['index.html', 'Home'], ['server/index.html', 'Server'], ['404.html', '404'], ['api/index.html', 'API index'], ['cli/index.html', 'CLI'], ['connectors/index.html', 'Connectors'], ['operations/index.html', 'Operations'], ['reference/index.html', 'Reference'], ['wizard/index.html', 'Wizard'], ['markdown/converter.html', 'Markup']]) await writeFile(join(root, path), `<title>${title}</title>`);
  for (const path of ['pagefind/pagefind-ui.js', 'sitemap-index.xml', 'robots.txt', '.nojekyll']) await writeFile(join(root, path), '');
  return root;
}

test('validator accepts a complete fixture and rejects broken links', async () => {
  const root = await fixture();
  assert.equal(validateBuiltDocs(root).pages, 10);
  await writeFile(join(root, 'index.html'), '<title>Home</title><a href="/ReplicaDB/missing/">broken</a>');
  assert.throws(() => validateBuiltDocs(root), /Broken built links/);
});

test('validator rejects injected secret fixtures', async () => {
  const root = await fixture();
  await writeFile(join(root, 'index.html'), '<title>Home</title><p>password="resolved"</p>');
  assert.throws(() => validateBuiltDocs(root), /Prohibited content/);
  assert.ok(prohibitedPatterns.length >= 4);
});