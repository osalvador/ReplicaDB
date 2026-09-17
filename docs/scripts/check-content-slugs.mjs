import { readFileSync, readdirSync } from 'node:fs';
import { extname, relative, sep } from 'node:path';
import { pathToFileURL } from 'node:url';

/**
 * @param {string} directory
 * @returns {string[]}
 */
function contentFiles(directory) {
  return readdirSync(directory, { withFileTypes: true }).flatMap((entry) => {
    const path = `${directory}/${entry.name}`;
    return entry.isDirectory()
      ? contentFiles(path)
      : ['.md', '.mdx'].includes(extname(entry.name))
        ? [path]
        : [];
  });
}

/** @param {string} contentRoot */
export function collectContentOwners(contentRoot) {
  /** @type {Map<string, string[]>} */
  const owners = new Map();
  for (const path of contentFiles(contentRoot)) {
    const source = readFileSync(path, 'utf8');
    const frontmatter = source.match(/^---\s*\n([\s\S]*?)\n---/);
    const explicitSlug = frontmatter?.[1].match(/^slug:\s*['"]?([^'"\n]+)['"]?\s*$/m)?.[1];
    const derivedSlug = relative(contentRoot, path)
      .split(sep)
      .join('/')
      .replace(/\.(?:md|mdx)$/, '')
      .replace(/(?:^|\/)index$/, '');
    const slug = explicitSlug ?? derivedSlug;
    owners.set(slug, [...(owners.get(slug) ?? []), path]);
  }
  return owners;
}

/** @param {string} contentRoot */
export function assertUniqueContentSlugs(contentRoot) {
  const duplicates = [...collectContentOwners(contentRoot)]
    .filter(([, paths]) => paths.length > 1)
    .map(([slug, paths]) => `${slug || '/'}: ${paths.join(', ')}`);
  if (duplicates.length > 0) {
    throw new Error(`Duplicate documentation slugs:\n${duplicates.join('\n')}`);
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const contentRoot = new URL('../src/content/docs', import.meta.url).pathname;
  assertUniqueContentSlugs(contentRoot);
  console.log('Documentation content slugs are unique.');
}
