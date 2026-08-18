import { spawnSync } from 'node:child_process';

const command = process.platform === 'win32' ? 'openapi-typescript.cmd' : 'openapi-typescript';
const output = process.env.OPENAPI_SCHEMA_OUTPUT ?? 'src/api/schema.ts';
const schemaUrl = process.env.OPENAPI_SCHEMA_URL ?? 'http://localhost:8080/v3/api-docs';
const result = spawnSync(command, [
  schemaUrl,
  '-o',
  output
], {
  stdio: 'inherit'
});

if (result.error) {
  throw result.error;
}

if (result.status !== 0) {
  process.exit(result.status ?? 1);
}
