import assert from 'node:assert/strict';
import { existsSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import test from 'node:test';

const root = new URL('..', import.meta.url).pathname;
const css = readFileSync(join(root, 'src/styles/custom.css'), 'utf8');
const homepage = readFileSync(join(root, 'src/content/docs/index.mdx'), 'utf8');
const builtHomepagePath = join(root, 'dist/index.html');

test('maps the Engineering Ledger palette to Starlight dark and light tokens', () => {
  for (const token of [
    '--replicadb-brand-teal: #0B6E69',
    '--replicadb-terracotta: #B15C38',
    '--replicadb-page-green: #F3F6F4',
    '--replicadb-paper: #FFFFFF',
    '--replicadb-mist-green: #E8F0ED',
    '--replicadb-ink: #1B2926',
    '--replicadb-muted-ink: #50625D',
    '--sl-color-accent-low: #113B38',
    '--sl-color-accent: #57C2B7',
    '--sl-color-accent-high: #D7F3EF',
    '--sl-color-black: #17211F'
  ]) {
    assert.match(css, new RegExp(token.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')));
  }

  assert.match(css, /\[data-theme='light'\][\s\S]*--sl-color-accent: #0B6E69/);
  assert.match(css, /--sl-font: "Avenir Next", "Helvetica Neue", sans-serif/);
  assert.match(css, /font-family: Georgia, "Times New Roman", serif/);
});

test('keeps component color and radius literals inside token scopes', () => {
  const outsideTokens = css
    .replace(/:root\s*\{[\s\S]*?\n\}/, '')
    .replace(/\[data-theme='light'\]\s*\{[\s\S]*?\n\}/, '');
  assert.doesNotMatch(outsideTokens, /#[0-9A-Fa-f]{6}/);
  assert.doesNotMatch(outsideTokens, /border-radius:\s*\d+px/);
});

test('requires accessible component content and stable image dimensions', () => {
  assert.match(homepage, /<ProductChoice/);
  assert.match(homepage, /<ArchitectureDiagram/);
  assert.match(homepage, /<SupportMatrix/);
  assert.match(homepage, /<ScreenshotFrame/);
  assert.match(homepage, /alt="[^"]+"/);
  assert.match(homepage, /caption="[^"]+"/);
  assert.match(homepage, /width=\{\d+\}/);
  assert.match(homepage, /height=\{\d+\}/);
  assert.match(readFileSync(join(root, 'src/components/ArchitectureDiagram.astro'), 'utf8'), /data-mermaid-source/);
  assert.match(readFileSync(join(root, 'src/components/ArchitectureDiagram.astro'), 'utf8'), /data-diagram-fallback/);
  assert.match(readFileSync(join(root, 'src/components/ArchitectureDiagram.astro'), 'utf8'), /mermaid\.render/);
  assert.match(readFileSync(join(root, 'src/components/ProductChoice.astro'), 'utf8'), /aria-labelledby/);
  assert.ok(existsSync(join(root, 'src/assets/brand/replicadb-logo.png')));
});

test('built light and dark surfaces retain responsive, readable component contracts', { skip: !existsSync(builtHomepagePath) }, () => {
  const builtHomepage = readFileSync(builtHomepagePath, 'utf8');

  assert.match(builtHomepage, /data-theme="dark"/);
  assert.match(builtHomepage, /data-mermaid-source/);
  assert.match(builtHomepage, /data-diagram-fallback/);
  assert.match(builtHomepage, /<header\b/);
  assert.match(builtHomepage, /<main\b/);
  assert.match(builtHomepage, /<a href="\/ReplicaDB\//);
  assert.match(builtHomepage, /aspect-ratio: 323 \/ 153/);
  assert.match(css, /@media \(max-width: 40rem\)[\s\S]*?grid-template-columns: 1fr/);
  assert.match(css, /\.screenshot-frame__viewport/);
  assert.match(css, /\[data-theme='light'\][\s\S]*--sl-color-accent-high: #064A47/);
});