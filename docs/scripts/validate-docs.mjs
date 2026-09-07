import { existsSync, readFileSync, readdirSync } from 'node:fs';
import { join, relative } from 'node:path';

export const prohibitedPatterns = [
  /-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----/i,
  /jdbc:[^\s"']+:\/\/[^\s"']*:[^\s"']*@/i,
  /(?:password|secret|token)\s*[:=]\s*["'][^$<{\n]+["']/i,
  /(?:leaseToken|encryptedSecurity|sourcePassword|sinkPassword)/
];

const docsRoot = new URL('..', import.meta.url).pathname;
export const distRoot = join(docsRoot, 'dist');
/** @param {string} directory @returns {string[]} */
function filesUnder(directory) {
  if (!existsSync(directory)) return [];
  return readdirSync(directory, { withFileTypes: true }).flatMap((entry) => {
    const path = join(directory, entry.name);
    return entry.isDirectory() ? filesUnder(path) : [path];
  });
}
/** @param {string} root */
function htmlFiles(root = distRoot) { return filesUnder(root).filter((path) => path.endsWith('.html')); }

export function validateBuiltDocs(root = distRoot) {
  const pages = htmlFiles(root);
  if (pages.length < 10) throw new Error(`Expected a built docs site, found ${pages.length} HTML files`);
  const titles = new Map();
  for (const page of pages) {
    const html = readFileSync(page, 'utf8');
    const title = html.match(/<title>([^<]+)<\/title>/i)?.[1]?.trim();
    if (!title) throw new Error(`Missing title: ${relative(root, page)}`);
    if (titles.has(title)) throw new Error(`Duplicate title "${title}" in ${relative(root, page)} and ${titles.get(title)}`);
    titles.set(title, relative(root, page));
    for (const pattern of prohibitedPatterns) if (pattern.test(html)) throw new Error(`Prohibited content in ${relative(root, page)}: ${pattern}`);
  }
  for (const path of ['index.html', '404.html', 'api/index.html', 'server/index.html', 'wizard/index.html', 'markdown/converter.html', 'pagefind/pagefind-ui.js', 'sitemap-index.xml', 'robots.txt', '.nojekyll']) {
    if (!existsSync(join(root, path))) throw new Error(`Missing built docs artifact: ${path}`);
  }
  const internalTargets = new Set(pages.map((page) => `/${relative(root, page).replace(/\\/g, '/')}`));
  const missingLinks = [];
  for (const page of pages) {
    const html = readFileSync(page, 'utf8');
    for (const match of html.matchAll(/(?:href|src)="(\/ReplicaDB\/[^"#?]+)/g)) {
      const target = match[1].replace(/^\/ReplicaDB/, '') || '/';
      if (target.endsWith('/')) {
        if (!existsSync(join(root, target.slice(1), 'index.html'))) missingLinks.push(`${relative(root, page)} -> ${target}`);
      } else if (!existsSync(join(root, target.slice(1))) && !internalTargets.has(target)) missingLinks.push(`${relative(root, page)} -> ${target}`);
    }
  }
  if (missingLinks.length) throw new Error(`Broken built links:\n${missingLinks.join('\n')}`);
  return { pages: pages.length, titles: titles.size };
}

if (process.argv[1]?.endsWith('validate-docs.mjs')) {
  const result = validateBuiltDocs();
  process.stdout.write(`Docs validated: ${result.pages} pages, ${result.titles} unique titles\n`);
}