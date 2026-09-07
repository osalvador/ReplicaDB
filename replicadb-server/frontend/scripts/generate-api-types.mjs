import { spawnSync } from 'node:child_process';
import { mkdirSync, mkdtempSync, renameSync, rmSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const defaultCommand = process.platform === 'win32' ? 'openapi-typescript.cmd' : 'openapi-typescript';

/**
 * @param {string[]} argv
 * @param {NodeJS.ProcessEnv} env
 */
export function generationOptions(argv = process.argv.slice(2), env = process.env) {
  const inputFileIndex = argv.indexOf('--input-file');
  const outputIndex = argv.indexOf('--output');
  const inputFile = inputFileIndex >= 0 ? argv[inputFileIndex + 1] : env.OPENAPI_SCHEMA_FILE;
  const input = inputFile
    ? resolve(inputFile)
    : env.OPENAPI_SCHEMA_URL ?? 'http://localhost:8080/v3/api-docs';
  const output = resolve(outputIndex >= 0 ? argv[outputIndex + 1] : env.OPENAPI_SCHEMA_OUTPUT ?? 'src/api/schema.ts');
  if ((inputFileIndex >= 0 && !argv[inputFileIndex + 1]) || (outputIndex >= 0 && !argv[outputIndex + 1])) {
    throw new Error('--input-file and --output require a path');
  }
  return { input, output };
}

/**
 * @param {{ input: string, output: string, command?: string, stdio?: import('node:child_process').StdioOptions }} options
 */
export function generateApiTypes({ input, output, command = defaultCommand, stdio = 'inherit' }) {
  mkdirSync(dirname(output), { recursive: true });
  const temporaryDirectory = mkdtempSync(join(dirname(output), '.openapi-types-'));
  const temporaryOutput = join(temporaryDirectory, 'schema.ts');
  try {
    const result = spawnSync(command, [input, '-o', temporaryOutput], { stdio });
    if (result.error) throw result.error;
    if (result.status !== 0) throw new Error(`openapi-typescript exited with status ${result.status ?? 1}`);
    renameSync(temporaryOutput, output);
  } finally {
    rmSync(temporaryDirectory, { recursive: true, force: true });
  }
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  generateApiTypes(generationOptions());
}
