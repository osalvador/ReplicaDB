import { cp, mkdir, readFile, rm, writeFile } from 'node:fs/promises';
import { dirname, join, relative, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const docsRoot = resolve(fileURLToPath(new URL('..', import.meta.url)));

export const wizardFiles = ['index.html'];

export const wizardDirectories = ['css', 'js', 'vendor'];

export const markdownFiles = [
  'converter.html',
  'converter-old.html',
  'converter-app.js',
  'converter-core.js',
  'editor-cm.js',
  'preview-sync.js',
  'sw.js',
  'manifest.webmanifest',
  'markdown-copy-lab-icon.svg',
  'markdown-copy-lab-icon-192.png',
  'markdown-copy-lab-icon-512.png',
  'apple-touch-icon.png',
  'favicon-32.png'
];

export const flattenedLegacyPages = [
  'server.html',
  'docs/docs.html',
  'docs/user-guide.html'
];

/** @param {string} reference */
export function assertRelativeAssetReference(reference) {
  if (/^(?:\/|[a-z][a-z\d+.-]*:)/i.test(reference)) {
    throw new Error(`Static tool asset must be relative: ${reference}`);
  }
  return reference;
}

/** @param {string} sourceRoot @param {string} outputRoot @param {string} path */
async function copyFile(sourceRoot, outputRoot, path) {
  const source = join(sourceRoot, path);
  const destination = join(outputRoot, path);
  await mkdir(dirname(destination), { recursive: true });
  await cp(source, destination);
}

/** @param {string} sourceRoot @param {string} outputRoot @param {string} path */
async function copyDirectory(sourceRoot, outputRoot, path) {
  const source = join(sourceRoot, path);
  const destination = join(outputRoot, path);
  await mkdir(dirname(destination), { recursive: true });
  await cp(source, destination, { recursive: true });
}

/** @param {string} outputRoot @param {string} path */
async function flattenLegacyPage(outputRoot, path) {
  const directory = join(outputRoot, path);
  const source = join(directory, 'index.html');
  const destination = join(outputRoot, path);
  const html = await readFile(source, 'utf8');
  await rm(directory, { recursive: true, force: true });
  await writeFile(destination, html, 'utf8');
}

export async function stageStaticTools({ sourceRoot = docsRoot, outputRoot = join(docsRoot, 'dist') } = {}) {
  const wizardRoot = join(sourceRoot, 'wizard');
  const markdownRoot = join(sourceRoot, 'markdown');
  const wizardOutput = join(outputRoot, 'wizard');
  const markdownOutput = join(outputRoot, 'markdown');

  await Promise.all([
    ...wizardFiles.map((path) => copyFile(wizardRoot, wizardOutput, path)),
    ...wizardDirectories.map((path) => copyDirectory(wizardRoot, wizardOutput, path)),
    ...markdownFiles.map((path) => copyFile(markdownRoot, markdownOutput, path)),
    copyDirectory(markdownRoot, markdownOutput, 'assets')
  ]);

  await Promise.all(flattenedLegacyPages.map((path) => flattenLegacyPage(outputRoot, path)));

  return {
    wizard: wizardFiles.concat(wizardDirectories),
    markdown: markdownFiles.concat('assets'),
    legacyPages: flattenedLegacyPages
  };
}

const invokedPath = process.argv[1] ? pathToFileURL(resolve(process.argv[1])).href : '';
if (invokedPath === import.meta.url) {
  await stageStaticTools();
  process.stdout.write(`Staged static tools under ${relative(docsRoot, join(docsRoot, 'dist'))}\n`);
}