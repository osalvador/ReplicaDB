import assert from 'node:assert/strict';
import { existsSync, readFileSync, readdirSync, statSync } from 'node:fs';
import { extname, join, relative } from 'node:path';
import test from 'node:test';
import sharp from 'sharp';
import { screenshotDefinitions } from '../src/data/screenshots.ts';

const root = new URL('..', import.meta.url).pathname;
const contentRoot = join(root, 'src/content/docs');
const maximumImageBytes = 500 * 1024;
const requiredMedia = [
  {
    path: 'src/assets/media/replication-modes/complete.png',
    owner: 'src/content/docs/cli/replication-modes.mdx',
    width: 1023,
    height: 680
  },
  {
    path: 'src/assets/media/replication-modes/complete-atomic.png',
    owner: 'src/content/docs/cli/replication-modes.mdx',
    width: 1023,
    height: 830
  },
  {
    path: 'src/assets/media/replication-modes/incremental.png',
    owner: 'src/content/docs/cli/replication-modes.mdx',
    width: 1023,
    height: 718
  },
  {
    path: 'src/assets/media/connector-examples/amazon-s3.png',
    owner: 'src/content/docs/connectors/amazon-s3.mdx',
    width: 1070,
    height: 420
  },
  {
    path: 'src/assets/media/connector-examples/amazon-s3-csv.png',
    owner: 'src/content/docs/connectors/amazon-s3.mdx',
    width: 1128,
    height: 436
  }
];

/**
 * @param {string} directory
 * @returns {string[]}
 */
function contentFiles(directory) {
  return readdirSync(directory, { withFileTypes: true }).flatMap((entry) => {
    const path = join(directory, entry.name);
    return entry.isDirectory()
      ? contentFiles(path)
      : ['.md', '.mdx'].includes(extname(entry.name))
        ? [path]
        : [];
  });
}

const guides = contentFiles(contentRoot).map((path) => ({
  path: relative(root, path),
  content: readFileSync(path, 'utf8')
}));

/** @param {string[]} registeredFiles @param {string[]} storedFiles */
function assertScreenshotInventory(registeredFiles, storedFiles) {
  assert.deepEqual(registeredFiles.toSorted(), storedFiles.toSorted(), 'registered screenshots must exactly match stored PNG files');
}

test('keeps recovered instructional media within its verified image contract', async () => {
  for (const media of requiredMedia) {
    const absolutePath = join(root, media.path);
    assert.ok(existsSync(absolutePath), `${media.path} must exist`);
    assert.ok(statSync(absolutePath).size <= maximumImageBytes, `${media.path} exceeds 500 KiB`);

    const metadata = await sharp(absolutePath).metadata();
    assert.equal(metadata.format, 'png', `${media.path} must remain a PNG`);
    assert.equal(metadata.width, media.width, `${media.path} width changed`);
    assert.equal(metadata.height, media.height, `${media.path} height changed`);

    const filename = media.path.slice(media.path.lastIndexOf('/') + 1);
    const owners = guides.filter((guide) => guide.content.includes(filename)).map((guide) => guide.path);
    assert.deepEqual(owners, [media.owner], `${media.path} must have exactly one owning guide`);
  }
});

test('does not publish legacy-hosted media or the unverified animation', () => {
  const documentation = guides.map((guide) => guide.content).join('\n');
  assert.doesNotMatch(documentation, /https?:\/\/[^\s)'\"]*(?:ReplicaDB-Mode_|AWS-S3-Screenshot)/i);
  assert.doesNotMatch(documentation, /ReplicaDB-Ora2PG\.gif/i);
  assert.equal(existsSync(join(root, 'src/assets/media/ReplicaDB-Ora2PG.gif')), false);
});

test('keeps every curated server screenshot registered and owned by one MDX guide', async () => {
  const screenshotRoot = join(root, 'src/assets/screenshots/server');
  const registeredFiles = screenshotDefinitions.map((definition) => definition.filename).sort();
  const storedFiles = readdirSync(screenshotRoot).filter((name) => name.endsWith('.png')).sort();
  assertScreenshotInventory(registeredFiles, storedFiles);
  assert.equal(new Set(registeredFiles).size, registeredFiles.length);

  for (const definition of screenshotDefinitions) {
    const absolutePath = join(screenshotRoot, definition.filename);
    assert.ok(existsSync(absolutePath), `${definition.filename} must exist`);
    const metadata = await sharp(absolutePath).metadata();
    const expected = { width: 1440, height: 900 };
    assert.equal(metadata.width, expected.width, `${definition.filename} width`);
    assert.equal(metadata.height, expected.height, `${definition.filename} height`);

    const owners = guides.filter((guide) => guide.content.includes(definition.filename));
    assert.equal(owners.length, 1, `${definition.filename} must have one owning guide`);
    assert.equal(owners[0].path, `src/content/docs/${definition.guide}.mdx`);
    assert.match(owners[0].content, /<ScreenshotFrame\b/);
    assert.match(owners[0].content, /alt="[^"]+"/);
    assert.match(owners[0].content, /caption="[^"]+"/);
    assert.match(owners[0].content, new RegExp(`width=\\{${expected.width}\\}`));
    assert.match(owners[0].content, new RegExp(`height=\\{${expected.height}\\}`));
  }
});

test('rejects an orphan screenshot fixture', () => {
  assert.throws(
    () => assertScreenshotInventory(['registered.png'], ['registered.png', 'orphan.png']),
    /registered screenshots must exactly match stored PNG files/
  );
});
