import { spawnSync } from 'node:child_process';

const command = process.platform === 'win32' ? 'openapi-typescript.cmd' : 'openapi-typescript';
const output = process.env.OPENAPI_SCHEMA_OUTPUT ?? 'src/api/schema.ts';
const result = spawnSync(command, [
  'http://localhost:8080/v3/api-docs',
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
