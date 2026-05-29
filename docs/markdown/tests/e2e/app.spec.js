import { test, expect } from '@playwright/test';

// Fail the test if the app logs any console error or throws a page error.
function trackErrors(page) {
  const errors = [];
  page.on('console', msg => { if (msg.type() === 'error') errors.push(msg.text()); });
  page.on('pageerror', err => errors.push(String(err)));
  return errors;
}

test.beforeEach(async ({ page }) => {
  await page.addInitScript(() => {
    try { localStorage.clear(); } catch { /* ignore */ }
  });
});

test('loads without console errors and renders the Teams preview', async ({ page }) => {
  const errors = trackErrors(page);
  await page.goto('/converter.html');

  // The default sample is loaded into the editor.
  await expect(page.locator('#editor')).not.toHaveValue('');

  // The Teams preview renders the sent-message view (bubble), not a composer.
  await expect(page.locator('.teams-sent-message')).toBeVisible();
  await expect(page.locator('.teams-content')).not.toBeEmpty();
  await expect(page.locator('.teams-sent-meta')).toBeVisible();
  // The composer chrome (format bar / compose footer) must not be shown.
  await expect(page.locator('.teams-format-bar')).toHaveCount(0);
  await expect(page.locator('.teams-compose-footer')).toHaveCount(0);

  expect(errors).toEqual([]);
});

test('typing markdown updates the Teams preview live', async ({ page }) => {
  const errors = trackErrors(page);
  await page.goto('/converter.html');

  const editor = page.locator('#editor');
  await editor.fill('# Hello Teams\n\nThis is **bold**.');

  const content = page.locator('.teams-content');
  // The preview renders the same Teams-friendly HTML that gets pasted: the
  // heading becomes an x-large span (not an <h1>), so preview == paste.
  await expect(content.locator('span[style*="x-large"]')).toHaveText('Hello Teams');
  await expect(content.locator('h1')).toHaveCount(0);
  await expect(content.locator('strong')).toHaveText('bold');

  expect(errors).toEqual([]);
});

test('Teams preview matches the pasted HTML structure (fidelity)', async ({ page }) => {
  await page.goto('/converter.html');
  await page.locator('#editor').fill('# Title\n\n> quote\n\n| Area | Status |\n| --- | --- |\n| IDE | Ready |');

  const content = page.locator('.teams-content');
  // Headings -> x-large span, quote -> blockquote>p, table -> figure.table.
  await expect(content.locator('span[style*="x-large"]')).toHaveText('Title');
  await expect(content.locator('blockquote p')).toHaveText('quote');
  await expect(content.locator('figure.table table')).toBeVisible();
  await expect(content.locator('figure.table td').first()).toHaveText('Area');
});

test('Teams preview table spans the full width of the message bubble', async ({ page }) => {
  await page.goto('/converter.html');
  await page.locator('#editor').fill('| Area | Status |\n| --- | --- |\n| Markdown IDE | Ready |\n| Local preview | Ready |');

  const table = page.locator('.teams-content figure.table table');
  await expect(table).toBeVisible();

  // A generic `.preview table { display: block }` rule (used for horizontal
  // scrolling) makes the table report a full-width bounding box while its
  // columns shrink to content. Assert the actual table formatting context and
  // that the last cell reaches the right edge of the content area.
  const result = await page.evaluate(() => {
    const content = document.querySelector('.teams-content');
    const tbl = document.querySelector('.teams-content figure.table table');
    const cells = tbl.querySelectorAll('tr:first-child td');
    const lastCell = cells[cells.length - 1];
    return {
      display: getComputedStyle(tbl).display,
      contentRight: Math.round(content.getBoundingClientRect().right),
      lastCellRight: Math.round(lastCell.getBoundingClientRect().right)
    };
  });

  expect(result.display).toBe('table');
  // The rightmost cell must reach (almost) the content's right edge.
  expect(result.lastCellRight).toBeGreaterThan(result.contentRight - 4);
});

test('switching output tabs changes preview and copy label', async ({ page }) => {
  const errors = trackErrors(page);
  await page.goto('/converter.html');
  await page.locator('#editor').fill('# Title\n\n```js\nconst x = 1;\n```');

  // Teams is the default tab.
  await expect(page.locator('#copyBtnLabel')).toHaveText('Copy for Teams');

  // Jira tab -> dark code output, Jira copy label.
  await page.getByRole('tab', { name: 'Jira' }).click();
  await expect(page.locator('#copyBtnLabel')).toHaveText('Copy Jira');
  await expect(page.locator('.preview-output-code')).toContainText('h1. Title');
  await expect(page.locator('.preview-output-code')).toContainText('{code:language=js}');

  // HTML tab -> dark HTML output, HTML copy label.
  await page.getByRole('tab', { name: 'HTML' }).click();
  await expect(page.locator('#copyBtnLabel')).toHaveText('Copy HTML');
  await expect(page.locator('.preview-output-code')).toContainText('<!doctype html>');

  // Back to Teams.
  await page.getByRole('tab', { name: 'Teams' }).click();
  await expect(page.locator('.teams-sent-message')).toBeVisible();

  expect(errors).toEqual([]);
});

test('output profile dropdown is removed (always default profile)', async ({ page }) => {
  await page.goto('/converter.html');
  await expect(page.locator('#outputProfile')).toHaveCount(0);
});

test('view modes toggle the editor and preview panels', async ({ page }) => {
  await page.goto('/converter.html');
  const workspace = page.locator('#workspace');

  await page.getByRole('button', { name: 'Editor', exact: true }).click();
  await expect(workspace).toHaveClass(/editor-only/);
  await expect(page.locator('.preview-panel')).toBeHidden();

  await page.getByRole('button', { name: 'Preview', exact: true }).click();
  await expect(workspace).toHaveClass(/preview-only/);
  await expect(page.locator('.editor-panel')).toBeHidden();

  await page.getByRole('button', { name: 'Split', exact: true }).click();
  await expect(workspace).toHaveClass(/split/);
  await expect(page.locator('.editor-panel')).toBeVisible();
  await expect(page.locator('.preview-panel')).toBeVisible();
});

test('header does not overlap the workspace', async ({ page }) => {
  await page.goto('/converter.html');
  const topbar = await page.locator('.topbar').boundingBox();
  const workspace = await page.locator('#workspace').boundingBox();
  expect(topbar).not.toBeNull();
  expect(workspace).not.toBeNull();
  // The workspace must start at or below the bottom of the header.
  expect(workspace.y).toBeGreaterThanOrEqual(topbar.y + topbar.height - 1);
});

test('Copy for Teams writes rich HTML and clean text to the clipboard', async ({ page, context }) => {
  await context.grantPermissions(['clipboard-read', 'clipboard-write']);
  await page.goto('/converter.html');
  await page.locator('#editor').fill('# Update\n\nThis is **bold**.');

  await page.locator('#copyBtn').click();

  const clip = await page.evaluate(async () => {
    const items = await navigator.clipboard.read();
    const out = { html: '', text: '' };
    for (const item of items) {
      if (item.types.includes('text/html')) out.html = await (await item.getType('text/html')).text();
      if (item.types.includes('text/plain')) out.text = await (await item.getType('text/plain')).text();
    }
    return out;
  });

  // HTML flavour carries Teams-ready formatting (not raw Markdown, no <h1>).
  expect(clip.html).toContain('<!--StartFragment-->');
  expect(clip.html).toContain('<span style="font-size:x-large;">Update</span>');
  expect(clip.html).toContain('<strong>bold</strong>');
  expect(clip.html).not.toContain('# Update');
  expect(clip.html).not.toContain('<h1>');
  // Plain-text fallback is clean text, not Markdown.
  expect(clip.text).toContain('This is bold.');
  expect(clip.text).not.toContain('**');
});

test('preview keeps position while typing instead of jumping to top', async ({ page }) => {
  await page.goto('/converter.html');

  // Build a long document so both editor and preview overflow.
  const longDoc = Array.from({ length: 120 }, (_, i) => `Line number ${i + 1} with some words.`).join('\n\n');
  await page.locator('#editor').fill(longDoc);

  // The preview scroller is either the inner Teams content or the outer preview.
  const previewScrollTop = () => page.evaluate(() => {
    const tc = document.querySelector('.teams-content');
    const preview = document.getElementById('preview');
    return Math.max(tc ? tc.scrollTop : 0, preview ? preview.scrollTop : 0);
  });

  // Scroll the editor near the bottom; the preview should follow proportionally.
  await page.locator('#editor').evaluate(el => { el.scrollTop = el.scrollHeight; });
  await page.waitForTimeout(150);
  expect(await previewScrollTop()).toBeGreaterThan(0);

  // Typing at the caret must not reset the preview scroll back to the top.
  await page.locator('#editor').press('End');
  await page.locator('#editor').type(' extra');
  await page.waitForTimeout(150);
  expect(await previewScrollTop()).toBeGreaterThan(0);
});
