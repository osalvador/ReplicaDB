import { writeFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';

export const DOCS_FIXTURE_PREFIX = 'Docs /';
export const DOCS_FIXED_IDS = Object.freeze({
  runningRun: '00000000-0000-4000-8000-000000000101',
  failedRun: '00000000-0000-4000-8000-000000000102',
  conflictJob: '00000000-0000-4000-8000-000000000103'
});

export const docsFixturePlan = Object.freeze([
  { key: 'running-run', status: 'RUNNING', id: DOCS_FIXED_IDS.runningRun },
  { key: 'failed-run', status: 'FAILED', id: DOCS_FIXED_IDS.failedRun },
  { key: 'conflict-job', status: 'CONFLICT', id: DOCS_FIXED_IDS.conflictJob },
  { key: 'truncated-log', status: 'FAILED', marker: '[TRUNCATED: middle omitted]' },
  { key: 'empty-collection', status: 'EMPTY' },
  { key: 'grant-dialog', status: 'DIALOG' }
]);

export function buildDocsFixtureNames() {
  return {
    jobs: [`${DOCS_FIXTURE_PREFIX} Running job`, `${DOCS_FIXTURE_PREFIX} Failed job`, `${DOCS_FIXTURE_PREFIX} Conflict job`],
    users: [`${DOCS_FIXTURE_PREFIX} Viewer`, `${DOCS_FIXTURE_PREFIX} Operator`],
    datasources: [`${DOCS_FIXTURE_PREFIX} Source`, `${DOCS_FIXTURE_PREFIX} Sink`]
  };
}

export function validateDocsFixturePlan(plan = docsFixturePlan) {
  const keys = new Set();
  for (const fixture of plan) {
    if (keys.has(fixture.key)) throw new Error(`Duplicate docs fixture: ${fixture.key}`);
    keys.add(fixture.key);
    if (JSON.stringify(fixture).match(/(?:password|secret|token|leaseToken|encryptedSecurity)/i)) {
      throw new Error(`Prohibited field in docs fixture: ${fixture.key}`);
    }
  }
  return true;
}

export async function writeFixturePlan(outputPath) {
  validateDocsFixturePlan();
  await writeFile(outputPath, `${JSON.stringify({ prefix: DOCS_FIXTURE_PREFIX, fixedIds: DOCS_FIXED_IDS, fixtures: docsFixturePlan }, null, 2)}\n`);
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  await writeFixturePlan(process.argv[2] ?? 'docs-screenshot-fixtures.json');
}