import { describe, expect, it } from 'vitest';
import {
  buildConnectString,
  parseConnectString,
  type ConnectionFields,
  type DbType
} from './connectionBuilder';

describe('connectionBuilder', () => {
  const cases: Array<{ type: Exclude<DbType, 'custom'>; fields: ConnectionFields }> = [
    { type: 'oracle', fields: { host: 'oracle.example', port: '1521', database: 'ORCL', oracleFormat: 'service' } },
    { type: 'mysql', fields: { host: 'mysql.example', port: '3306', database: 'app' } },
    { type: 'mariadb', fields: { host: 'maria.example', port: '3306', database: 'app' } },
    { type: 'postgres', fields: { host: 'postgres.example', port: '5432', database: 'app' } },
    { type: 'db2', fields: { host: 'db2.example', port: '50000', database: 'app' } },
    { type: 'db2i', fields: { host: 'as400.example', port: '446', database: 'app' } },
    { type: 'sqlite', fields: { sqliteFilePath: 'var/app.db' } },
    { type: 'sqlserver', fields: { host: 'sqlserver.example', port: '1433', database: 'app' } },
    { type: 'denodo', fields: { host: 'denodo.example', port: '9999', database: 'app' } },
    { type: 'file', fields: { filePath: '/var/data/input.csv' } },
    { type: 'kafka', fields: { kafkaBootstrapServers: 'broker-1:9092,broker-2:9092' } },
    { type: 's3', fields: { host: 's3.example', port: '443', bucket: 'replicadb', prefix: 'exports' } },
    { type: 'mongodb', fields: { mongoUri: 'mongodb://host/catalog' } },
    { type: 'mongodb+srv', fields: { mongoUri: 'mongodb+srv://cluster.example/catalog' } }
  ];

  it.each(cases)('round-trips $type connections', ({ type, fields }) => {
    const connect = buildConnectString(type, fields);
    expect(parseConnectString(connect)).toMatchObject({ type, ...fields });
  });

  it('builds and parses Oracle SID connections', () => {
    const fields = { host: 'oracle.example', port: '1521', database: 'ORCL', oracleFormat: 'sid' as const };

    expect(buildConnectString('oracle', fields)).toBe('jdbc:oracle:thin:@oracle.example:1521:ORCL');
    expect(parseConnectString('jdbc:oracle:thin:@oracle.example:1521:ORCL')).toMatchObject({
      type: 'oracle',
      ...fields
    });
  });

  it('keeps custom connections intact', () => {
    const raw = 'jdbc:custom://host/database?option=value';

    expect(parseConnectString(raw)).toEqual({ type: 'custom', raw });
    expect(buildConnectString('custom', { raw })).toBe(raw);
  });

  it('parses connections without a port', () => {
    const connect = buildConnectString('postgres', { host: 'localhost', database: 'app' });

    expect(connect).toBe('jdbc:postgresql://localhost/app');
    expect(parseConnectString(connect)).toMatchObject({
      type: 'postgres',
      host: 'localhost',
      database: 'app'
    });
    expect(parseConnectString(connect).port).toBeUndefined();
  });

  it('parses bracketed IPv6 hosts', () => {
    const connect = buildConnectString('postgres', { host: '[::1]', port: '5432', database: 'app' });

    expect(parseConnectString(connect)).toMatchObject({
      type: 'postgres',
      host: '[::1]',
      port: '5432',
      database: 'app'
    });
  });
});
