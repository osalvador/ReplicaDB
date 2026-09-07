import { readFileSync } from 'node:fs';

/** @param {string[]} paths */
export function classifyChanges(paths) {
  const files = [...paths].filter(Boolean);
  const base = files.some((path) => path === 'README.md'
    || path === 'DEPLOYMENT.md'
    || path === 'RELEASE_GUIDE.md'
    || path === 'replicadb-server/README.md'
    || path === 'replicadb-server/frontend/README.develop.md'
    || path === 'scripts/check-phase3-docs.sh'
    || path.startsWith('docs/'));
  const api = files.some((path) => path.startsWith('replicadb-server/src/main/java/')
    || path === 'replicadb-server/pom.xml'
    || path === 'replicadb-server/src/test/java/org/replicadb/server/OpenApiSpecificationIT.java'
    || path === 'docs/openapi/replicadb-server.json');
  const frontend = files.some((path) => path.startsWith('replicadb-server/frontend/src/')
    || path.startsWith('replicadb-server/frontend/e2e/')
    || path.startsWith('replicadb-server/frontend/scripts/seed-docs-screenshots')
    || path === 'replicadb-server/frontend/package.json'
    || path === 'replicadb-server/frontend/package-lock.json'
    || path === 'replicadb-server/frontend/playwright.docs.config.ts'
    || path === 'docs/src/data/screenshots.ts'
    || path.startsWith('docs/src/assets/screenshots/'));
  return { files, base, api, frontend, docsOnly: base && !api && !frontend };
}

if (process.argv[1]?.endsWith('classify-changes.mjs')) {
  const input = readFileSync(0, 'utf8').split(/\r?\n/);
  process.stdout.write(`${JSON.stringify(classifyChanges(input))}\n`);
}