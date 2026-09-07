import assert from 'node:assert/strict';
import { existsSync, readFileSync } from 'node:fs';
import { execFileSync } from 'node:child_process';
import { join, relative } from 'node:path';
import test from 'node:test';
import { legacyRoutes, preservedStaticRoutes } from '../src/data/legacy-routes.ts';

const repoRoot = new URL('../..', import.meta.url).pathname;
const docsRoot = new URL('..', import.meta.url).pathname;
const distRoot = join(docsRoot, 'dist');
const trackedExtensions = new Set(['.md', '.mdx', '.html', '.js', '.mjs', '.ts', '.tsx', '.yaml', '.yml', '.sh']);
const routeByPath = new Map(legacyRoutes.map((route) => [route.path, route]));
const canonicalPaths = new Set([
  '/',
  ...legacyRoutes.flatMap((route) => [route.canonicalPath, ...Object.values(route.fragments)])
]);

function trackedFiles() {
  return execFileSync('git', ['ls-files'], { cwd: repoRoot, encoding: 'utf8' })
    .trim()
    .split('\n')
    .filter(Boolean)
    .filter((path) => trackedExtensions.has(path.slice(path.lastIndexOf('.'))));
}

/** @param {string} text */
function publicReferences(text) {
  const references = [];
  const absolutePattern = /https:\/\/osalvador\.github\.io\/ReplicaDB([^\s"'<>)]*)/g;
  for (const match of text.matchAll(absolutePattern)) references.push(match[1] || '/');
  const knownRootPattern = /(?:href|src)=["']\/(server\.html|docs\/docs\.html(?:#[^"']+)?|docs\/user-guide\.html|wizard\/|markdown\/)[^"']*["']/g;
  for (const match of text.matchAll(knownRootPattern)) references.push(`/${match[1]}`);
  return references.map((reference) => reference.replace(/[.,]+$/, ''));
}

/** @param {string} reference */
function splitReference(reference) {
  const [pathname, fragment] = reference.split('#');
  return { pathname, fragment: fragment || '' };
}

test('maps every tracked repository-owned public path and fragment', () => {
  const missing = [];
  for (const path of trackedFiles()) {
    if (!existsSync(join(repoRoot, path))) continue;
    const text = readFileSync(join(repoRoot, path), 'utf8');
    for (const reference of publicReferences(text)) {
      const { pathname, fragment } = splitReference(reference);
      const route = routeByPath.get(pathname);
      const isPreserved = preservedStaticRoutes.some((routePath) => pathname === routePath || pathname.startsWith(`${routePath.slice(0, -'index.html'.length)}`));
      const isCanonical = [...canonicalPaths].some((canonicalPath) => pathname === canonicalPath || pathname.startsWith(`${canonicalPath.replace(/\/$/, '')}/`));
      if (isCanonical || isPreserved) continue;
      if (!route || (fragment && !Object.hasOwn(route.fragments, fragment))) {
        missing.push(`${relative(repoRoot, path)} -> ${reference}`);
      }
    }
  }
  assert.deepEqual(missing, []);
});

test('emits compatibility pages with canonical metadata and fragment navigation', { skip: !existsSync(distRoot) }, () => {
  for (const route of legacyRoutes) {
    const outputPath = join(distRoot, route.path.replace(/^\//, ''));
    assert.ok(existsSync(outputPath), route.path);
    const html = readFileSync(outputPath, 'utf8');
    assert.match(html, /rel="canonical"/);
    assert.match(html, /http-equiv="refresh"/);
    assert.match(html, /window\.location\.replace/);
    assert.match(html, /JavaScript is disabled/);
  }
  for (const route of preservedStaticRoutes) {
    assert.ok(existsSync(join(distRoot, route.replace(/^\//, ''))), route);
  }
});
