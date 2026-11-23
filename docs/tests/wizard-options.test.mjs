import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import { fieldHelp } from '../wizard/js/help.js';
import { advancedFields, compatibility, endpointFields, generateConfiguration, genericJdbc, modes } from '../wizard/js/options.js';

const connectors = JSON.parse(readFileSync(new URL('../src/data/connector-capabilities.json', import.meta.url), 'utf8'));
const catalog = [...connectors, genericJdbc];
/** @param {string} id */
const get = id => catalog.find(connector => connector.id === id);
const postgres = get('postgresql');
const postgresSettings = {
  mode: 'complete',
  scope: 'table',
  'source.connect': 'jdbc:postgresql://localhost/source',
  'source.table': 'public.orders',
  'sink.connect': 'jdbc:postgresql://localhost/sink',
  'sink.table': 'public.orders'
};

test('previews an example configuration for every supported connector pairing and mode', () => {
  for (const source of catalog.filter(connector => connector.roles.includes('source'))) {
    for (const sink of catalog.filter(connector => connector.roles.includes('sink'))) {
      for (const mode of modes) {
        const label = `${source.id} → ${sink.id} (${mode})`;
        const values = { mode, scope: 'table' };
        const result = generateConfiguration(source, sink, values);
        const supported = source.sourceModes.includes(mode) && sink.sinkModes.includes(mode);
        assert.equal(Boolean(result.text), supported, `${label}: ${result.errors.join(' ')}`);
        if (supported && (source.singleJobOnly || sink.singleJobOnly)) assert.match(result.text, /^jobs=1$/m, label);
      }
    }
  }
});

test('exposes every property loaded by ToolOptions in its relevant workflow', () => {
  const java = readFileSync(new URL('../../src/main/java/org/replicadb/cli/ToolOptions.java', import.meta.url), 'utf8');
  const expected = [...new Set([...java.matchAll(/prop.getProperty\("([^"]+)"\)/g)].map(match => match[1]))].sort();
  const actual = new Set(['mode']);
  for (const connector of catalog) {
    for (const mode of modes) {
      for (const scope of ['table', 'query', 'catalog']) {
        const settings = {
          mode, scope,
          'source.auth.mode': 'ActiveDirectoryServicePrincipalCertificate',
          'sink.auth.mode': 'ActiveDirectoryServicePrincipalCertificate'
        };
        for (const side of connector.roles) {
          const [source, sink] = side === 'source' ? [connector, postgres] : [postgres, connector];
          const fields = [
            ...endpointFields(side, connector, source, sink, settings),
            ...advancedFields(source, sink, settings)
          ];
          for (const field of fields) {
            assert.ok(fieldHelp(field.key, connector)?.[1], field.key);
            if (!field.key.includes('.connect.parameter.')) actual.add(field.key);
          }
        }
      }
    }
  }
  for (const side of ['source', 'sink']) {
    const sql = get('sql-server');
    for (const field of endpointFields(side, sql, sql, sql, { 'source.auth.mode': 'ActiveDirectoryInteractive', 'sink.auth.mode': 'ActiveDirectoryInteractive' })) {
      actual.add(field.key);
    }
  }
  assert.deepEqual([...actual].sort(), expected);
});

test('provides help for conditional fields and keeps intentionally cleared examples empty', () => {
  const scenarios = [
    { sink: get('amazon-s3'), values: { 'sink.connect.parameter.row.isObject': 'true' } },
    { sink: get('file'), values: { 'sink.file.format': 'orc' } }
  ];
  for (const { sink, values } of scenarios) {
    for (const field of endpointFields('sink', sink, postgres, sink, { mode: 'complete', ...values })) {
      assert.ok(fieldHelp(field.key, sink)?.[1], field.key);
    }
  }
  const result = generateConfiguration(postgres, get('kafka'), {
    mode: 'complete', scope: 'table', 'sink.connect.parameter.topic': ''
  });
  assert.equal(result.text, '');
  assert.match(result.errors.join(' '), /enter topic/i);
});

test('does not serialize inactive options and preserves Java properties escaping', () => {
  const result = generateConfiguration(postgres, get('file'), {
    ...postgresSettings,
    mode: 'incremental',
    jobs: '9',
    'sink.connect': 'file:///tmp/orders.csv',
    'sink.file.format': 'csv',
    'sink.connect.parameter.format.delimiter': '\t',
    'sink.connect.parameter.format.firstRecordAsHeader': 'false',
    'sink.connect.parameter.format.recordSeparator': '\r\n',
    'sink.staging.schema': 'ignored',
    'source.password': '${SOURCE_PASSWORD}',
    'source.extra': 'sslmode=verify-full\nsslrootcert=C:\\certs\\ca.pem'
  });
  assert.deepEqual(result.errors, []);
  assert.match(result.text, /^jobs=1$/m);
  assert.ok(result.text.includes('source.password=${SOURCE_PASSWORD}\n'));
  assert.ok(result.text.includes('format.delimiter=\\t\n'));
  assert.ok(result.text.includes('format.recordSeparator=\\r\\n\n'));
  assert.ok(result.text.includes('format.firstRecordAsHeader=false\n'));
  assert.ok(result.text.includes('sslrootcert=C:\\\\certs\\\\ca.pem\n'));
  assert.doesNotMatch(result.text, /sink\.(?:table|staging)/);
});

test('serializes contiguous table pairs without stale single-table settings', () => {
  const result = generateConfiguration(postgres, postgres, {
    ...postgresSettings,
    mode: 'incremental',
    scope: 'catalog',
    'source.query': 'select * from not_used',
    'incremental.watermark.column': 'ignored',
    'sink.staging.table': 'ignored',
    'sink.staging.schema': 'stage'
  }, [
    { source: 'shop.orders', sink: 'reports.orders' },
    { source: 'shop.customers', sink: 'reports.customers' }
  ]);
  assert.deepEqual(result.errors, []);
  assert.match(result.text, /^sink.staging.schema=stage$/m);
  assert.match(result.text, /^replication.table.2.sink=reports.customers$/m);
  assert.doesNotMatch(result.text, /^(?:source\.(?:table|query)|incremental\.watermark\.column|sink\.staging\.table)=/m);
});

test('checks required connector settings and disallows conflicting fields', () => {
  const azure = get('sql-server');
  const settings = {
    ...postgresSettings,
    'source.connect': 'jdbc:sqlserver://localhost;databaseName=orders',
    'source.auth.mode': 'ActiveDirectoryInteractive',
    'source.password': 'ignored',
    mode: 'incremental',
    'incremental.watermark.value': '5',
    'sink.staging.table': 'public.stage',
    'sink.staging.schema': 'public'
  };
  assert.equal(compatibility(azure, postgres, settings).singleJob, true);
  const invalid = generateConfiguration(azure, postgres, settings);
  assert.equal(invalid.text, '');
  assert.match(invalid.errors.join(' '), /watermark column/);
  assert.match(invalid.errors.join(' '), /staging schema/);

  const s3 = generateConfiguration(postgres, get('amazon-s3'), {
    ...postgresSettings,
    'sink.connect': 's3://s3.eu-west-1.amazonaws.com/bucket',
    'sink.connect.parameter.accessKey': '${AWS_ACCESS_KEY_ID}',
    'sink.connect.parameter.secretKey': '${AWS_SECRET_ACCESS_KEY}',
    'sink.connect.parameter.row.isObject': 'true',
    'sink.connect.parameter.row.keyColumn': 'id',
    'sink.connect.parameter.row.contentColumn': 'payload',
    'sink.file.format': 'orc'
  });
  assert.deepEqual(s3.errors, []);
  assert.doesNotMatch(s3.text, /sink.file.format=/);
  assert.match(s3.text, /sink.connect.parameter.row.contentColumn=payload/);

  const kafka = generateConfiguration(postgres, get('kafka'), {
    ...postgresSettings,
    'sink.connect': 'kafka://localhost:9092',
    'sink.connect.parameter.topic': 'destination',
    'sink.extra': 'topic=override'
  });
  assert.equal(kafka.text, '');
  assert.match(kafka.errors.join(' '), /sink.connect.parameter.topic/);
});
