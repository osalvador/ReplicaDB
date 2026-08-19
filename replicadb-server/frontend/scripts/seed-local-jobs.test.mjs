import assert from 'node:assert/strict';
import test from 'node:test';
import { buildJobPayload, SOURCE_FIXTURES } from './seed-local-jobs.mjs';

test('covers every source type exposed by the configuration wizard', () => {
  assert.deepEqual(
    SOURCE_FIXTURES.map(fixture => fixture.key),
    ['oracle', 'mysql', 'mariadb', 'postgres', 'db2', 'db2i', 'sqlite', 'sqlserver', 'denodo', 'file']
  );
});

test('covers all replication modes', () => {
  assert.deepEqual(
    new Set(SOURCE_FIXTURES.map(fixture => fixture.mode)),
    new Set(['complete', 'complete-atomic', 'incremental'])
  );
});

test('adds watermark fields only to incremental fixtures and never embeds credentials', () => {
  for (const fixture of SOURCE_FIXTURES) {
    const payload = buildJobPayload(fixture);
    assert.equal(payload.sourceUser, undefined);
    assert.equal(payload.sourcePassword, undefined);
    assert.equal(payload.sinkPassword, undefined);
    if (fixture.mode === 'incremental') {
      assert.equal(payload.incrementalWatermarkColumn, 'updated_at');
      assert.equal(payload.initialWatermarkValue, '0');
    } else {
      assert.equal(payload.incrementalWatermarkColumn, undefined);
      assert.equal(payload.initialWatermarkValue, undefined);
    }
  }
});
