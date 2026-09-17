import { readdir, readFile, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { parse, serialize } from 'parse5';

/** @type {Record<string, string>} */
const apiTagTitles = {
  authentication: 'Authentication API',
  dashboard: 'Dashboard API',
  datasources: 'Datasources API',
  'datasource-permissions': 'Datasource permissions API',
  jobs: 'Jobs API',
  'job-permissions': 'Job permissions API',
  schedules: 'Schedules API',
  runs: 'Runs API',
  users: 'Users API',
  audit: 'Audit API',
  keyring: 'Keyring API'
};

/** @param {string} directory @returns {Promise<string[]>} */
async function htmlFiles(directory) {
  const entries = await readdir(directory, { withFileTypes: true });
  const files = await Promise.all(entries.map((entry) => {
    const path = join(directory, entry.name);
    return entry.isDirectory() ? htmlFiles(path) : Promise.resolve(entry.name.endsWith('.html') ? [path] : []);
  }));
  return files.flat();
}

/** @param {any} node @param {(node: any) => void} visitor */
function visit(node, visitor) {
  visitor(node);
  for (const child of node.childNodes ?? []) visit(child, visitor);
}

/** @param {any} node @param {string} value */
function replaceFirstText(node, value) {
  for (const child of node.childNodes ?? []) {
    if (child.nodeName === '#text') {
      child.value = value;
      return;
    }
  }
}

/** @param {any} node @param {string} name */
function hasAttribute(node, name) {
  for (const attribute of node.attrs ?? []) {
    if (attribute.name === name) return true;
  }
  return false;
}

/** @param {URL} outputDirectory */
export async function finalizeApiReference(outputDirectory) {
  const root = fileURLToPath(outputDirectory);
  const files = await htmlFiles(join(root, 'api'));
  files.push(join(root, 'api-introduction', 'index.html'));
  for (const file of files) {
    const source = await readFile(file, 'utf8');
    const tagMatch = file.match(/[/\\]api[/\\]operations[/\\]tags[/\\]([^/\\]+)[/\\]index\.html$/);
    const title = tagMatch?.[1] ? apiTagTitles[tagMatch[1]] : undefined;
    if (!title && !source.includes('<pre data-language=')) continue;

    const document = parse(source);
    visit(document, (node) => {
      if (title && node.tagName === 'title') {
        for (const child of node.childNodes ?? []) {
          if (child.nodeName === '#text' && child.value.startsWith('Overview |')) {
            child.value = child.value.replace('Overview |', `${title} |`);
            break;
          }
        }
      }
      if (title && node.tagName === 'h1') replaceFirstText(node, title);
      if (node.tagName === 'pre' && hasAttribute(node, 'data-language') && !hasAttribute(node, 'tabindex')) {
        node.attrs.push({ name: 'tabindex', value: '0' });
      }
    });
    await writeFile(file, serialize(document));
  }
}
