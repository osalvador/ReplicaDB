import { test, expect } from '@playwright/test';

const editorValue = page => page.evaluate(() => window.__markupForgeEditor?.getValue?.() || '');

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
  await expect.poll(() => editorValue(page)).not.toBe('');

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

  // Jira tab -> defaults to Visual (iframe). Switch to Text to see raw markup.
  await page.getByRole('tab', { name: 'Jira' }).click();
  await expect(page.locator('#copyBtnLabel')).toHaveText('Copy Jira');
  await expect(page.locator('.jira-visual-frame')).toBeVisible();
  await page.getByRole('button', { name: 'Text', exact: true }).click();
  await expect(page.locator('.preview-output-code')).toContainText('h1. Title');
  await expect(page.locator('.preview-output-code')).toContainText('{code:language=js}');

  // HTML tab -> rendered navigable artifact in a sandboxed iframe by default,
  // with a Preview/Source toggle. HTML copy label.
  await page.getByRole('tab', { name: 'HTML' }).click();
  await expect(page.locator('#copyBtnLabel')).toHaveText('Copy HTML');
  await expect(page.locator('.html-preview-frame')).toBeVisible();
  // Switching to Source shows the raw HTML document.
  await page.getByRole('button', { name: 'Source', exact: true }).click();
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

test('split handle resizes the source and preview panels horizontally', async ({ page }) => {
  await page.goto('/converter.html');
  const handle = page.locator('#splitHandle');
  await expect(handle).toBeVisible();

  const before = await page.evaluate(() => {
    const editorPanel = document.querySelector('.editor-panel');
    const previewPanel = document.querySelector('.preview-panel');
    return {
      editorWidth: editorPanel.getBoundingClientRect().width,
      previewWidth: previewPanel.getBoundingClientRect().width
    };
  });

  const box = await handle.boundingBox();
  expect(box).not.toBeNull();
  await page.mouse.move(box.x + box.width / 2, box.y + box.height / 2);
  await page.mouse.down();
  await page.mouse.move(box.x + box.width / 2 + 120, box.y + box.height / 2, { steps: 8 });
  await page.mouse.up();

  const after = await page.evaluate(() => {
    const editorPanel = document.querySelector('.editor-panel');
    const previewPanel = document.querySelector('.preview-panel');
    return {
      editorWidth: editorPanel.getBoundingClientRect().width,
      previewWidth: previewPanel.getBoundingClientRect().width
    };
  });

  expect(after.editorWidth).toBeGreaterThan(before.editorWidth + 40);
  expect(after.previewWidth).toBeLessThan(before.previewWidth - 40);
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

test('preview follows the caret when typing at the bottom after scrolling preview up', async ({ page }) => {
  await page.goto('/converter.html');

  const longDoc = Array.from({ length: 120 }, (_, i) => `Line number ${i + 1} with some words.`).join('\n\n');
  await page.locator('#editor').fill(longDoc);
  await page.waitForTimeout(150);

  const previewScrollTop = () => page.evaluate(() => {
    const tc = document.querySelector('.teams-content');
    const preview = document.getElementById('preview');
    return Math.max(tc ? tc.scrollTop : 0, preview ? preview.scrollTop : 0);
  });

  // Manually scroll the preview back to the top while leaving the caret/editor at the end.
  await page.evaluate(() => {
    const tc = document.querySelector('.teams-content');
    const preview = document.getElementById('preview');
    if (tc) tc.scrollTop = 0;
    if (preview) preview.scrollTop = 0;
  });
  expect(await previewScrollTop()).toBe(0);

  // Move the caret to the very end and type: the preview must follow the caret to the bottom.
  await page.locator('#editor').focus();
  await page.locator('#editor').press('Control+End');
  await page.locator('#editor').type(' bottom-edit');
  await page.waitForTimeout(200);
  expect(await previewScrollTop()).toBeGreaterThan(0);
});

test('typing stays centered in Teams preview even with tall tables above', async ({ page }) => {
  await page.goto('/converter.html');

  const tallDoc = [
    '# Report',
    '',
    '| Area | Status | Notes |',
    '| --- | --- | --- |',
    ...Array.from({ length: 60 }, (_, i) => `| Row ${i + 1} | Ready | Cell ${i + 1} with extra content to make the preview much taller than the source line |`),
    '',
    ...Array.from({ length: 80 }, (_, i) => `Editable line ${i + 1}`)
  ].join('\n');

  await page.locator('#editor').fill(tallDoc);
  await page.locator('#editor').focus();
  await page.locator('#editor').press('Control+End');
  await page.locator('#editor').type(' still-editing');
  await page.waitForTimeout(250);

  const result = await page.evaluate(() => {
    const scroller = document.querySelector('.teams-sent-scroll') || document.getElementById('preview');
    const target = document.querySelector('.preview-map-block[data-src-start="63"][data-src-end="63"], .preview-map-block[data-src-start="64"][data-src-end="64"]')
      || Array.from(document.querySelectorAll('.preview-map-block')).at(-1);
    const scrollerRect = scroller.getBoundingClientRect();
    const targetRect = target.getBoundingClientRect();
    return {
      targetTopOffset: targetRect.top - scrollerRect.top,
      targetBottomOffset: targetRect.bottom - scrollerRect.top,
      scrollerHeight: scroller.clientHeight,
      scrollTop: scroller.scrollTop
    };
  });

  // The edited line block must stay well inside the viewport, not lost above or below.
  expect(result.targetTopOffset).toBeGreaterThan(result.scrollerHeight * 0.15);
  expect(result.targetBottomOffset).toBeLessThan(result.scrollerHeight * 0.85);
});

test('typing keeps the active block visible across Teams, Jira Text, and HTML Source', async ({ page }) => {
  await page.goto('/converter.html');
  const doc = [
    ...Array.from({ length: 80 }, (_, i) => `| Row ${i + 1} | Value ${i + 1} |`),
    '',
    ...Array.from({ length: 80 }, (_, i) => `Tail line ${i + 1}`)
  ].join('\n');
  await page.locator('#editor').fill(doc);

  const assertPreviewMoved = async () => {
    const value = await page.evaluate(() => {
      if (document.querySelector('.html-preview-frame:not([hidden])')) {
        const frame = document.querySelector('.html-preview-frame');
        const doc = frame && frame.contentDocument;
        const scroller = doc && (doc.scrollingElement || doc.documentElement || doc.body);
        return scroller ? scroller.scrollTop : 0;
      }
      if (document.querySelector('.jira-visual-frame:not([hidden])')) {
        const frame = document.querySelector('.jira-visual-frame');
        const doc = frame && frame.contentDocument;
        const scroller = doc && (doc.scrollingElement || doc.documentElement || doc.body);
        return scroller ? scroller.scrollTop : 0;
      }
      const code = document.querySelector('.preview-output-code:not([hidden])');
      if (code) {
        const preview = document.getElementById('preview');
        if (preview && preview.scrollHeight > preview.clientHeight) return preview.scrollTop;
        return code.scrollTop;
      }
      const teams = document.querySelector('.teams-sent-scroll');
      if (teams && teams.scrollHeight > teams.clientHeight) return teams.scrollTop;
      const teamsContent = document.querySelector('.teams-content');
      if (teamsContent && teamsContent.scrollHeight > teamsContent.clientHeight) return teamsContent.scrollTop;
      const preview = document.getElementById('preview');
      return preview ? preview.scrollTop : 0;
    });
    expect(value).toBeGreaterThan(0);
  };

  await page.locator('#editor').focus();
  await page.locator('#editor').press('Control+End');
  await page.locator('#editor').type(' end');
  await page.waitForTimeout(250);
  await assertPreviewMoved();

  await page.getByRole('tab', { name: 'HTML' }).click();
  await page.waitForTimeout(250);
  await page.locator('#editor').focus();
  await page.locator('#editor').press('Control+End');
  await page.locator('#editor').type(' visual');
  await page.waitForTimeout(350);
  const htmlVisualScroll = await page.evaluate(() => {
    const frame = document.querySelector('.html-preview-frame');
    const doc = frame && frame.contentDocument;
    const scroller = doc && (doc.scrollingElement || doc.documentElement || doc.body);
    return scroller ? scroller.scrollTop : 0;
  });
  expect(htmlVisualScroll).toBeGreaterThan(0);

  await page.getByRole('tab', { name: 'Jira' }).click();
  await page.waitForTimeout(250);
  await page.locator('#editor').focus();
  await page.locator('#editor').press('Control+End');
  await page.locator('#editor').type(' visual');
  await page.waitForTimeout(350);
  const jiraVisualScroll = await page.evaluate(() => {
    const frame = document.querySelector('.jira-visual-frame');
    const doc = frame && frame.contentDocument;
    const scroller = doc && (doc.scrollingElement || doc.documentElement || doc.body);
    return scroller ? scroller.scrollTop : 0;
  });
  expect(jiraVisualScroll).toBeGreaterThan(0);

  await page.getByRole('button', { name: 'Text', exact: true }).click();
  await page.locator('#editor').focus();
  await page.locator('#editor').press('Control+End');
  await page.locator('#editor').type(' more');
  await page.waitForTimeout(250);
  await assertPreviewMoved();

  await page.getByRole('tab', { name: 'HTML' }).click();
  await page.getByRole('button', { name: 'Source', exact: true }).click();
  await page.locator('#editor').focus();
  await page.locator('#editor').press('Control+End');
  await page.locator('#editor').type(' again');
  await page.waitForTimeout(250);
  await assertPreviewMoved();
});

test('HTML and Jira visual previews do not drift to the bottom during repeated edits away from the end', async ({ page }) => {
  await page.goto('/converter.html');
  const doc = Array.from({ length: 180 }, (_, i) => `Paragraph line ${i + 1}`).join('\n\n');
  await page.evaluate(text => {
    globalThis.__markupForgeEditor.setValue(text);
  }, doc);

  const typeNearTopRepeatedly = async () => {
    for (let i = 0; i < 4; i++) {
      await page.locator('#editor').focus();
      await page.locator('#editor').press('Home');
      await page.locator('#editor').type('X');
      await page.waitForTimeout(200);
    }
  };

  await page.getByRole('tab', { name: 'HTML' }).click();
  await page.waitForTimeout(300);
  await typeNearTopRepeatedly();
  const htmlVisual = await page.evaluate(() => {
    const frame = document.querySelector('.html-preview-frame');
    const doc = frame && frame.contentDocument;
    const scroller = doc && (doc.scrollingElement || doc.documentElement || doc.body);
    if (!scroller) return null;
    const max = Math.max(0, scroller.scrollHeight - scroller.clientHeight);
    return { top: scroller.scrollTop, max };
  });
  expect(htmlVisual).not.toBeNull();
  expect(htmlVisual.top).toBeLessThan(htmlVisual.max * 0.35);

  await page.getByRole('tab', { name: 'Jira' }).click();
  await page.waitForTimeout(300);
  await typeNearTopRepeatedly();
  const jiraVisual = await page.evaluate(() => {
    const frame = document.querySelector('.jira-visual-frame');
    const doc = frame && frame.contentDocument;
    const scroller = doc && (doc.scrollingElement || doc.documentElement || doc.body);
    if (!scroller) return null;
    const max = Math.max(0, scroller.scrollHeight - scroller.clientHeight);
    return { top: scroller.scrollTop, max };
  });
  expect(jiraVisual).not.toBeNull();
  expect(jiraVisual.top).toBeLessThan(jiraVisual.max * 0.35);
});

test('rapid typing keeps HTML and Jira visual previews stable near the edited region', async ({ page }) => {
  await page.goto('/converter.html');
  const doc = Array.from({ length: 180 }, (_, i) => `Paragraph line ${i + 1}`).join('\n\n');
  await page.evaluate(text => {
    globalThis.__markupForgeEditor.setValue(text);
  }, doc);

  const burstTypeNearTop = async () => {
    await page.locator('#editor').focus();
    await page.locator('#editor').press('Home');
    await page.locator('#editor').type('XXXX');
    await page.waitForTimeout(160);
  };

  await page.getByRole('tab', { name: 'HTML' }).click();
  await page.waitForTimeout(250);
  await burstTypeNearTop();
  const htmlVisual = await page.evaluate(() => {
    const frame = document.querySelector('.html-preview-frame');
    const doc = frame && frame.contentDocument;
    const scroller = doc && (doc.scrollingElement || doc.documentElement || doc.body);
    if (!scroller) return null;
    return { top: scroller.scrollTop, max: Math.max(0, scroller.scrollHeight - scroller.clientHeight) };
  });
  expect(htmlVisual).not.toBeNull();
  expect(htmlVisual.top).toBeLessThan(htmlVisual.max * 0.3);

  await page.getByRole('tab', { name: 'Jira' }).click();
  await page.waitForTimeout(250);
  await burstTypeNearTop();
  const jiraVisual = await page.evaluate(() => {
    const frame = document.querySelector('.jira-visual-frame');
    const doc = frame && frame.contentDocument;
    const scroller = doc && (doc.scrollingElement || doc.documentElement || doc.body);
    if (!scroller) return null;
    return { top: scroller.scrollTop, max: Math.max(0, scroller.scrollHeight - scroller.clientHeight) };
  });
  expect(jiraVisual).not.toBeNull();
  expect(jiraVisual.top).toBeLessThan(jiraVisual.max * 0.3);
});

test('manual editor scroll does not push HTML and Jira visual previews to the bottom', async ({ page }) => {
  await page.goto('/converter.html');
  const doc = [
    ...Array.from({ length: 40 }, (_, i) => `## Objective ${i + 1}`),
    '',
    ...Array.from({ length: 140 }, (_, i) => `Paragraph ${i + 1} with enough content to make the preview tall and easy to drift.`)
  ].join('\n\n');
  await page.evaluate(text => {
    globalThis.__markupForgeEditor.setValue(text);
  }, doc);

  const stepScroll = async () => {
    await page.evaluate(() => {
      const ed = globalThis.__markupForgeEditor;
      ed.scrollDOM.scrollTop += 450;
      ed.scrollDOM.dispatchEvent(new Event('scroll', { bubbles: true }));
    });
    await page.waitForTimeout(180);
  };

  await page.getByRole('tab', { name: 'HTML' }).click();
  await page.waitForTimeout(300);
  for (let i = 0; i < 5; i++) await stepScroll();
  const htmlState = await page.evaluate(() => {
    const ed = globalThis.__markupForgeEditor;
    const frame = document.querySelector('.html-preview-frame');
    const doc = frame && frame.contentDocument;
    const scroller = doc && (doc.scrollingElement || doc.documentElement || doc.body);
    const editorMax = ed.scrollDOM.scrollHeight - ed.scrollDOM.clientHeight;
    const previewMax = scroller ? scroller.scrollHeight - scroller.clientHeight : 0;
    return scroller ? {
      editorRatio: editorMax > 0 ? ed.scrollDOM.scrollTop / editorMax : 0,
      previewRatio: previewMax > 0 ? scroller.scrollTop / previewMax : 0
    } : null;
  });
  expect(htmlState).not.toBeNull();
  expect(htmlState.previewRatio).toBeLessThan(htmlState.editorRatio + 0.18);

  await page.getByRole('tab', { name: 'Jira' }).click();
  await page.waitForTimeout(300);
  for (let i = 0; i < 5; i++) await stepScroll();
  const jiraState = await page.evaluate(() => {
    const ed = globalThis.__markupForgeEditor;
    const frame = document.querySelector('.jira-visual-frame');
    const doc = frame && frame.contentDocument;
    const scroller = doc && (doc.scrollingElement || doc.documentElement || doc.body);
    const editorMax = ed.scrollDOM.scrollHeight - ed.scrollDOM.clientHeight;
    const previewMax = scroller ? scroller.scrollHeight - scroller.clientHeight : 0;
    return scroller ? {
      editorRatio: editorMax > 0 ? ed.scrollDOM.scrollTop / editorMax : 0,
      previewRatio: previewMax > 0 ? scroller.scrollTop / previewMax : 0
    } : null;
  });
  expect(jiraState).not.toBeNull();
  expect(jiraState.previewRatio).toBeLessThan(jiraState.editorRatio + 0.18);
});

test('HTML and Jira visual previews track editor scrolling without drifting too far ahead', async ({ page }) => {
  await page.goto('/converter.html');
  const doc = [
    ...Array.from({ length: 50 }, (_, i) => `## Heading ${i + 1}`),
    '',
    ...Array.from({ length: 180 }, (_, i) => `Paragraph ${i + 1} with enough content to make the preview tall.`)
  ].join('\n\n');
  await page.evaluate(text => {
    globalThis.__markupForgeEditor.setValue(text);
  }, doc);
  await page.waitForTimeout(250);

  const stepEditorScroll = async () => {
    await page.evaluate(() => {
      const ed = globalThis.__markupForgeEditor;
      ed.scrollDOM.scrollTop += 360;
      ed.scrollDOM.dispatchEvent(new Event('scroll', { bubbles: true }));
    });
    await page.waitForTimeout(180);
  };

  await page.getByRole('tab', { name: 'HTML' }).click();
  await page.waitForTimeout(250);
  for (let i = 0; i < 4; i++) await stepEditorScroll();
  const html = await page.evaluate(() => {
    const ed = globalThis.__markupForgeEditor;
    const frame = document.querySelector('.html-preview-frame');
    const doc = frame && frame.contentDocument;
    const scroller = doc && (doc.scrollingElement || doc.documentElement || doc.body);
    if (!scroller) return null;
    const editorMax = ed.scrollDOM.scrollHeight - ed.scrollDOM.clientHeight;
    const previewMax = scroller.scrollHeight - scroller.clientHeight;
    return {
      editorRatio: editorMax > 0 ? ed.scrollDOM.scrollTop / editorMax : 0,
      previewRatio: previewMax > 0 ? scroller.scrollTop / previewMax : 0
    };
  });
  expect(html).not.toBeNull();
  expect(html.previewRatio).toBeLessThan(html.editorRatio + 0.22);

  await page.getByRole('tab', { name: 'Jira' }).click();
  await page.waitForTimeout(250);
  for (let i = 0; i < 4; i++) await stepEditorScroll();
  const jira = await page.evaluate(() => {
    const ed = globalThis.__markupForgeEditor;
    const frame = document.querySelector('.jira-visual-frame');
    const doc = frame && frame.contentDocument;
    const scroller = doc && (doc.scrollingElement || doc.documentElement || doc.body);
    if (!scroller) return null;
    const editorMax = ed.scrollDOM.scrollHeight - ed.scrollDOM.clientHeight;
    const previewMax = scroller.scrollHeight - scroller.clientHeight;
    return {
      editorRatio: editorMax > 0 ? ed.scrollDOM.scrollTop / editorMax : 0,
      previewRatio: previewMax > 0 ? scroller.scrollTop / previewMax : 0
    };
  });
  expect(jira).not.toBeNull();
  expect(jira.previewRatio).toBeLessThan(jira.editorRatio + 0.22);
});

test('manual preview scroll moves the editor without creating a bounce loop', async ({ page }) => {
  await page.goto('/converter.html');
  const doc = [
    ...Array.from({ length: 30 }, (_, i) => `## Section ${i + 1}`),
    '',
    ...Array.from({ length: 160 }, (_, i) => `Paragraph ${i + 1} with enough content to make both editor and preview scroll.`)
  ].join('\n\n');
  await page.evaluate(text => {
    globalThis.__markupForgeEditor.setValue(text);
  }, doc);
  await page.waitForTimeout(300);

  const editorScrollTop = () => page.evaluate(() => globalThis.__markupForgeEditor.scrollDOM.scrollTop);

  await page.evaluate(() => {
    globalThis.__markupForgeEditor.scrollDOM.scrollTop = 0;
  });

  await page.evaluate(() => {
    const teams = document.querySelector('.teams-sent-scroll');
    const scroller = teams && teams.scrollHeight > teams.clientHeight + 2 ? teams : document.getElementById('preview');
    scroller.scrollTop = 800;
    scroller.dispatchEvent(new Event('scroll', { bubbles: true }));
  });
  await page.waitForTimeout(250);
  const teamsEditorTop = await editorScrollTop();
  expect(teamsEditorTop).toBeGreaterThan(0);
  await page.waitForTimeout(200);
  expect(await editorScrollTop()).toBe(teamsEditorTop);

  await page.getByRole('tab', { name: 'HTML' }).click();
  await page.waitForTimeout(400);
  await page.evaluate(() => {
    const frame = document.querySelector('.html-preview-frame');
    const doc = frame && frame.contentDocument;
    const scroller = doc && (doc.scrollingElement || doc.documentElement || doc.body);
    if (!scroller) return;
    scroller.scrollTop = 1200;
    scroller.dispatchEvent(new Event('scroll', { bubbles: true }));
  });
  await page.waitForTimeout(300);
  const htmlEditorTop = await editorScrollTop();
  expect(htmlEditorTop).toBeGreaterThan(0);
  await page.waitForTimeout(200);
  expect(await editorScrollTop()).toBe(htmlEditorTop);

  await page.getByRole('tab', { name: 'Jira' }).click();
  await page.waitForTimeout(400);
  await page.evaluate(() => {
    const frame = document.querySelector('.jira-visual-frame');
    const doc = frame && frame.contentDocument;
    const scroller = doc && (doc.scrollingElement || doc.documentElement || doc.body);
    if (!scroller) return;
    scroller.scrollTop = 1200;
    scroller.dispatchEvent(new Event('scroll', { bubbles: true }));
  });
  await page.waitForTimeout(300);
  const jiraEditorTop = await editorScrollTop();
  expect(jiraEditorTop).toBeGreaterThan(0);
  await page.waitForTimeout(200);
  expect(await editorScrollTop()).toBe(jiraEditorTop);
});

test('switching HTML Source and Jira Text does not change scroll position', async ({ page }) => {
  await page.goto('/converter.html');
  const doc = [
    ...Array.from({ length: 70 }, (_, i) => `## Heading ${i + 1}`),
    '',
    ...Array.from({ length: 180 }, (_, i) => `Paragraph ${i + 1} with enough content to make the preview tall.`)
  ].join('\n\n');
  await page.evaluate(text => {
    globalThis.__markupForgeEditor.setValue(text);
  }, doc);
  await page.waitForTimeout(350);

  const getScrollState = () => page.evaluate(() => ({
    editor: globalThis.__markupForgeEditor.scrollDOM.scrollTop,
    preview: document.getElementById('preview').scrollTop
  }));

  await page.getByRole('tab', { name: 'HTML' }).click();
  await page.waitForTimeout(250);
  await page.evaluate(() => {
    const frame = document.querySelector('.html-preview-frame');
    const doc = frame && frame.contentDocument;
    const scroller = doc && (doc.scrollingElement || doc.documentElement || doc.body);
    if (!scroller) return;
    scroller.scrollTop = 900;
    scroller.dispatchEvent(new Event('scroll', { bubbles: true }));
  });
  await page.waitForTimeout(250);
  const beforeHtml = await getScrollState();
  await page.getByRole('button', { name: 'Source', exact: true }).click();
  await page.waitForTimeout(250);
  const afterHtml = await getScrollState();
  expect(afterHtml.preview).toBe(beforeHtml.preview);

  await page.getByRole('tab', { name: 'Jira' }).click();
  await page.waitForTimeout(250);
  await page.evaluate(() => {
    const frame = document.querySelector('.jira-visual-frame');
    const doc = frame && frame.contentDocument;
    const scroller = doc && (doc.scrollingElement || doc.documentElement || doc.body);
    if (!scroller) return;
    scroller.scrollTop = 900;
    scroller.dispatchEvent(new Event('scroll', { bubbles: true }));
  });
  await page.waitForTimeout(250);
  const beforeJira = await getScrollState();
  await page.getByRole('button', { name: 'Text', exact: true }).click();
  await page.waitForTimeout(250);
  const afterJira = await getScrollState();
  expect(afterJira.preview).toBe(beforeJira.preview);
});

test('HTML Source and Jira Text toggles stay pinned while content scrolls', async ({ page }) => {
  await page.goto('/converter.html');
  const doc = [
    ...Array.from({ length: 80 }, (_, i) => `## Heading ${i + 1}`),
    '',
    ...Array.from({ length: 220 }, (_, i) => `Paragraph ${i + 1} with enough content to make the source text tall.`)
  ].join('\n\n');
  await page.evaluate(text => {
    globalThis.__markupForgeEditor.setValue(text);
  }, doc);
  await page.waitForTimeout(400);

  const assertPinned = async (tabName, buttonName) => {
    await page.getByRole('tab', { name: tabName }).click();
    await page.waitForTimeout(250);
    await page.getByRole('button', { name: buttonName, exact: true }).click();
    await page.waitForTimeout(250);
    const before = await page.evaluate(() => {
      const button = document.querySelector('.html-view-btn.active, .jira-view-btn.active');
      const preview = document.getElementById('preview');
      return {
        buttonTop: button ? button.getBoundingClientRect().top : 0,
        buttonBottom: button ? button.getBoundingClientRect().bottom : 0,
        previewTop: preview.getBoundingClientRect().top,
        previewScrollTop: preview.scrollTop
      };
    });
    await page.locator('#preview').evaluate(el => { el.scrollTop = el.scrollHeight; el.dispatchEvent(new Event('scroll', { bubbles: true })); });
    await page.waitForTimeout(250);
    const after = await page.evaluate(() => {
      const button = document.querySelector('.html-view-btn.active, .jira-view-btn.active');
      const preview = document.getElementById('preview');
      return {
        buttonTop: button ? button.getBoundingClientRect().top : 0,
        buttonBottom: button ? button.getBoundingClientRect().bottom : 0,
        previewTop: preview.getBoundingClientRect().top,
        previewScrollTop: preview.scrollTop
      };
    });
    expect(after.buttonTop).toBeCloseTo(before.buttonTop, 0);
    expect(after.buttonBottom).toBeCloseTo(before.buttonBottom, 0);
    expect(after.previewTop).toBeCloseTo(before.previewTop, 0);
    expect(after.previewScrollTop).toBeGreaterThan(before.previewScrollTop);
  };

  await assertPinned('HTML', 'Source');
  await assertPinned('Jira', 'Text');
});

test('HTML and Jira preview shells stretch to fill the preview panel height', async ({ page }) => {
  await page.goto('/converter.html');
  await page.locator('#editor').fill('# Title\n\nBody');

  await page.getByRole('tab', { name: 'HTML' }).click();
  const htmlHeights = await page.evaluate(() => {
    const preview = document.getElementById('preview');
    const shell = document.querySelector('.html-preview-shell');
    return {
      preview: preview.getBoundingClientRect().height,
      shell: shell.getBoundingClientRect().height
    };
  });
  expect(htmlHeights.shell).toBeGreaterThan(htmlHeights.preview - 40);

  await page.getByRole('tab', { name: 'Jira' }).click();
  const jiraHeights = await page.evaluate(() => {
    const preview = document.getElementById('preview');
    const shell = document.querySelector('.jira-preview-shell');
    return {
      preview: preview.getBoundingClientRect().height,
      shell: shell.getBoundingClientRect().height
    };
  });
  expect(jiraHeights.shell).toBeGreaterThan(jiraHeights.preview - 40);
});

test('editor mode skips preview rendering updates for large documents', async ({ page }) => {
  await page.goto('/converter.html');
  await page.getByRole('button', { name: 'Editor', exact: true }).click();
  const big = Array.from({ length: 800 }, (_, i) => `Line ${i + 1}`).join('\n');
  await page.locator('#editor').fill(big);
  await page.waitForTimeout(150);
  const classes = await page.locator('#githubEditor').getAttribute('class');
  expect(classes).toContain('performance-lite');
});

test('large documents keep preview active in Split view and keep editor scrolling', async ({ page }) => {
  await page.goto('/converter.html');
  const big = Array.from({ length: 1600 }, (_, i) => `Line ${i + 1}`).join('\n');

  await page.evaluate(text => {
    globalThis.__markupForgeEditor.setValue(text);
  }, big);
  await page.waitForTimeout(250);

  await expect(page.locator('.preview-output-code')).toHaveCount(0);
  await expect(page.locator('.teams-content')).toBeVisible();

  const before = await page.evaluate(() => {
    const ed = globalThis.__markupForgeEditor;
    return {
      scrollTop: ed.scrollDOM.scrollTop,
      clientHeight: ed.scrollDOM.clientHeight,
      scrollHeight: ed.scrollDOM.scrollHeight
    };
  });
  expect(before.scrollHeight).toBeGreaterThan(before.clientHeight);

  await page.evaluate(() => {
    const ed = globalThis.__markupForgeEditor;
    ed.scrollDOM.scrollTop = ed.scrollDOM.scrollHeight;
    ed.scrollDOM.dispatchEvent(new Event('scroll', { bubbles: true }));
  });
  await page.waitForTimeout(100);

  const after = await page.evaluate(() => globalThis.__markupForgeEditor.scrollDOM.scrollTop);
  expect(after).toBeGreaterThan(0);
});

test('pasting rich Teams HTML converts it to Markdown in the editor', async ({ page }) => {
  await page.goto('/converter.html');

  // Pre-existing content must be preserved; paste inserts at the caret.
  await page.locator('#editor').fill('Existing line.\n');
  await page.locator('#editor').focus();
  await page.locator('#editor').press('End');

  // Simulate pasting HTML copied from Teams (heading, bold, link, table).
  await page.locator('#editor').evaluate(el => {
    const html = '<h2>Weekly</h2><p><b>Bold</b> and <a href="https://ms.com">MS</a></p>'
      + '<table><tr><td>Area</td><td>Status</td></tr><tr><td>IDE</td><td>Ready</td></tr></table>';
    const dt = new DataTransfer();
    dt.setData('text/html', html);
    dt.setData('text/plain', 'Weekly Bold and MS');
    el.dispatchEvent(new ClipboardEvent('paste', { clipboardData: dt, bubbles: true, cancelable: true }));
  });

  await expect.poll(() => editorValue(page)).toContain('## Weekly');
  const value = await editorValue(page);
  expect(value).toContain('Existing line.'); // existing content not erased
  expect(value).toContain('**Bold**');
  expect(value).toContain('[MS](https://ms.com)');
  expect(value).toContain('| Area | Status |');
});

test('pasting a data-URI image renders an <img> in the Teams preview', async ({ page }) => {
  await page.goto('/converter.html');
  const dataUri = 'data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==';
  await page.locator('#editor').fill(`![](${dataUri})`);
  const img = page.locator('.teams-content img');
  await expect(img).toBeVisible();
  await expect(img).toHaveAttribute('src', dataUri);
});

test('Teams "Copy image" does not produce a duplicate image (binary + HTML data URI)', async ({ page }) => {
  await page.goto('/converter.html');
  const dataUri = 'data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==';
  await page.locator('#editor').fill('');
  await page.locator('#editor').focus();

  // Teams ships the picture both as a binary image file AND inline in the HTML.
  await page.locator('#editor').evaluate((el, uri) => {
    const bin = atob(uri.split(',')[1]);
    const bytes = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
    const file = new File([bytes], 'image.png', { type: 'image/png' });
    const dt = new DataTransfer();
    dt.setData('text/html', `<html><body><img src="${uri}" alt=""></body></html>`);
    dt.items.add(file);
    el.dispatchEvent(new ClipboardEvent('paste', { clipboardData: dt, bubbles: true, cancelable: true }));
  }, dataUri);

  await expect.poll(() => editorValue(page)).toContain('![](data:image/png');
  const value = await editorValue(page);
  const imageCount = (value.match(/!\[\]\(data:image/g) || []).length;
  expect(imageCount).toBe(1); // exactly one image, no duplicate
});

test('Paste as Markdown preserves Word data URIs and replaces file:// images with clipboard binaries', async ({ page }) => {
  await page.goto('/converter.html');
  const dataUri = 'data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==';
  const html = `<html><body><p><img alt="inline" src="${dataUri}"></p><p><img alt="local" src="file:///Users/me/AppData/Local/Temp/msohtmlclip/clip_image001.png"></p></body></html>`;

  await page.evaluate(([uri, htmlText]) => {
    const bin = atob(uri.split(',')[1]);
    const bytes = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
    navigator.clipboard.read = async () => [{
      types: ['text/html', 'image/png'],
      getType: async type => new Blob([type === 'text/html' ? htmlText : bytes], { type: type === 'text/html' ? 'text/html' : 'image/png' }),
    }];
    navigator.clipboard.readText = async () => 'inline local';
  }, [dataUri, html]);

  await page.locator('#editor').fill('');
  await page.locator('#editor').focus();
  await page.locator('#pasteMarkdownBtn').click();

  // pasteFromTeams is async; poll until the content is available.
  await expect.poll(() => editorValue(page)).toContain('![inline](data:image/png');
  const value = await editorValue(page);
  expect(value).toContain('![local](data:image/png');
  expect(value).not.toContain('file:///Users/me/AppData/Local/Temp/msohtmlclip/clip_image001.png');
  expect((value.match(/!\[[^\]]*\]\(data:image/g) || []).length).toBe(2);
});

test('Paste as Markdown detects TSV table from clipboard', async ({ page }) => {
  await page.goto('/converter.html');

  // Mock the async clipboard with a TSV table (e.g. copied from a spreadsheet).
  await page.evaluate(() => {
    const tsv = 'Area\tStatus\nIDE\tReady\nPaste\tDone';
    navigator.clipboard.read = async () => [{
      types: ['text/plain'],
      getType: async () => new Blob([tsv], { type: 'text/plain' }),
    }];
    navigator.clipboard.readText = async () => tsv;
  });

  await page.locator('#editor').fill('');
  await page.locator('#editor').focus();
  await page.locator('#pasteMarkdownBtn').click();

  await expect.poll(() => editorValue(page)).toContain('| Area | Status |');
  const value = await editorValue(page);
  expect(value).toContain('| IDE | Ready |');
  expect(value).toContain('| --- | --- |');
});

test('Paste as Markdown detects CSV table from clipboard', async ({ page }) => {
  await page.goto('/converter.html');

  // Mock the async clipboard with a CSV table (comma-separated, no HTML).
  await page.evaluate(() => {
    const csv = 'Name,Role\nAlice,Engineer\nBob,Designer';
    navigator.clipboard.read = async () => [{
      types: ['text/plain'],
      getType: async () => new Blob([csv], { type: 'text/plain' }),
    }];
    navigator.clipboard.readText = async () => csv;
  });

  await page.locator('#editor').fill('');
  await page.locator('#editor').focus();
  await page.locator('#pasteMarkdownBtn').click();

  await expect.poll(() => editorValue(page)).toContain('| Name | Role |');
  const value = await editorValue(page);
  expect(value).toContain('| Alice | Engineer |');
  expect(value).toContain('| --- | --- |');
});

test('Paste as Markdown converts rich HTML clipboard without errors', async ({ page }) => {
  const errors = trackErrors(page);
  await page.goto('/converter.html');

  // Mock the async clipboard with a rich text/html flavour.
  await page.evaluate(() => {
    const html = '<h2>Weekly</h2><p>Hello <strong>world</strong></p>';
    navigator.clipboard.read = async () => [{
      types: ['text/html'],
      getType: async () => new Blob([html], { type: 'text/html' }),
    }];
    navigator.clipboard.readText = async () => 'Weekly\nHello world';
  });

  await page.locator('#editor').fill('');
  await page.locator('#editor').focus();
  await page.locator('#pasteMarkdownBtn').click();

  await expect.poll(() => editorValue(page)).toContain('## Weekly');
  expect(await editorValue(page)).toContain('**world**');
  expect(errors).toEqual([]);
});

test('Paste as Markdown converts Microsoft Word HTML clipboard', async ({ page }) => {
  await page.goto('/converter.html');
  await page.locator('#editor').fill('');
  await page.locator('#editor').focus();

  await page.evaluate(() => {
    const html = '<html><body><p class="MsoHeading1" style="mso-outline-level:1">Word Title</p><p class="MsoListParagraph" style="mso-list:l0 level1 lfo1"><span style="font-weight:bold">•</span> Item one</p><p class="MsoListParagraph" style="mso-list:l0 level1 lfo1"><span style="font-weight:bold">•</span> Item two</p><p><span style="font-weight:bold">Bold</span> and <span style="font-style:italic">Italic</span></p></body></html>';
    navigator.clipboard.read = async () => [{
      types: ['text/html', 'text/plain'],
      getType: async type => new Blob([type === 'text/html' ? html : 'Word Title'], { type })
    }];
    navigator.clipboard.readText = async () => 'Word Title';
  });

  await page.locator('#pasteMarkdownBtn').click();

  const editor = page.locator('.cm-content');
  await expect(editor).toContainText('# Word Title');
  await expect(editor).toContainText('- Item one');
  await expect(editor).toContainText('- Item two');
  await expect(editor).toContainText('**Bold**');
  await expect(editor).toContainText('*Italic*');
});

test('Paste as Markdown converts Jira wiki clipboard text', async ({ page }) => {
  await page.goto('/converter.html');
  await page.locator('#editor').fill('');
  await page.locator('#editor').focus();

  await page.evaluate(() => {
    const jira = 'h2. Jira Title\n\n* Bullet\n# Number\n\n{code:language=js}\nconst x = 1;\n{code}';
    navigator.clipboard.read = async () => [{
      types: ['text/plain'],
      getType: async () => new Blob([jira], { type: 'text/plain' })
    }];
    navigator.clipboard.readText = async () => jira;
  });

  await page.locator('#pasteMarkdownBtn').click();

  const editor = page.locator('.cm-content');
  await expect(editor).toContainText('## Jira Title');
  await expect(editor).toContainText('- Bullet');
  await expect(editor).toContainText('1. Number');
  await expect(editor).toContainText('```js');
  await expect(editor).toContainText('const x = 1;');
});

test('Paste as Markdown converts Jira HTML clipboard', async ({ page }) => {
  await page.goto('/converter.html');
  await page.locator('#editor').fill('');
  await page.locator('#editor').focus();

  await page.evaluate(() => {
    const html = '<div data-node-type="paragraph"><h2>Jira HTML</h2><p><strong>Bold</strong> and <em>Italic</em></p><table><tr><td>A</td><td>B</td></tr><tr><td>1</td><td>2</td></tr></table></div>';
    navigator.clipboard.read = async () => [{
      types: ['text/html', 'text/plain'],
      getType: async type => new Blob([type === 'text/html' ? html : 'Jira HTML'], { type })
    }];
    navigator.clipboard.readText = async () => 'Jira HTML';
  });

  await page.locator('#pasteMarkdownBtn').click();

  const editor = page.locator('.cm-content');
  await expect(editor).toContainText('## Jira HTML');
  await expect(editor).toContainText('**Bold**');
  await expect(editor).toContainText('*Italic*');
  await expect(editor).toContainText('| A | B |');
});

test('Export Source + Artifact downloads a ZIP bundle', async ({ page }) => {
  await page.goto('/converter.html');
  await page.locator('#editor').fill('# Release\n\nNotes body.');

  const [download] = await Promise.all([
    page.waitForEvent('download'),
    page.locator('#exportBundleBtn').click(),
  ]);

  expect(download.suggestedFilename()).toMatch(/source-artifact\.zip$/);

  const path = await download.path();
  const fs = await import('node:fs/promises');
  const bytes = await fs.readFile(path);
  // ZIP local file header magic "PK\x03\x04".
  expect(bytes[0]).toBe(0x50);
  expect(bytes[1]).toBe(0x4b);
  expect(bytes[2]).toBe(0x03);
  expect(bytes[3]).toBe(0x04);
  // Entry names are stored uncompressed, so they appear verbatim.
  const text = bytes.toString('latin1');
  expect(text).toContain('source.md');
  expect(text).toContain('index.html');
  expect(text).toContain('README.md');
});

test('editor shows CodeMirror line numbers and Markdown syntax highlighting', async ({ page }) => {
  await page.goto('/converter.html');
  await page.locator('#editor').fill('# Heading\n\nA **bold** word.\n- item');

  const gutter = page.locator('.cm-gutters');
  await expect(gutter).toBeVisible();
  await expect(gutter).toContainText('4');

  // CodeMirror renders syntax-highlighted spans directly in the editor content.
  await expect(page.locator('.cm-content')).toContainText('# Heading');
  const highlightedSpanCount = await page.locator('.cm-content span').count();
  expect(highlightedSpanCount).toBeGreaterThan(0);
});

test('keyboard shortcuts wrap the selection (Cmd/Ctrl+B, I, K)', async ({ page }) => {
  await page.goto('/converter.html');
  const editor = page.locator('#editor');
  const mod = process.platform === 'darwin' ? 'Meta' : 'Control';

  // Bold.
  await editor.fill('word');
  await page.keyboard.press(`${mod}+a`);
  await page.keyboard.press(`${mod}+b`);
  await expect.poll(() => editorValue(page)).toBe('**word**');

  // Italic.
  await editor.fill('word');
  await page.keyboard.press(`${mod}+a`);
  await page.keyboard.press(`${mod}+i`);
  await expect.poll(() => editorValue(page)).toBe('*word*');

  // Link.
  await editor.fill('word');
  await page.keyboard.press(`${mod}+a`);
  await page.keyboard.press(`${mod}+k`);
  await expect.poll(() => editorValue(page)).toContain('[word](');
});

test('slash command menu inserts a snippet', async ({ page }) => {
  await page.goto('/converter.html');
  const editor = page.locator('#editor');
  await editor.fill('');
  await editor.focus();

  // Typing "/" opens the menu listing the commands.
  await page.keyboard.type('/');
  const menu = page.locator('#slashMenu');
  await expect(menu).toBeVisible();
  await expect(menu.locator('.slash-item')).toHaveCount(5);

  // Narrow to the table command and accept with Enter.
  await page.keyboard.type('table');
  await expect(menu.locator('.slash-item')).toHaveCount(1);
  await page.keyboard.press('Enter');

  await expect(menu).toBeHidden();
  const value = await editorValue(page);
  expect(value).toContain('| Column A | Column B |');
  expect(value).toContain('| --- | --- |');
  expect(value).not.toContain('/table');
});

test('slash command menu can be dismissed with Escape', async ({ page }) => {
  await page.goto('/converter.html');
  const editor = page.locator('#editor');
  await editor.fill('');
  await editor.focus();
  await page.keyboard.type('/cal');
  await expect(page.locator('#slashMenu')).toBeVisible();
  await page.keyboard.press('Escape');
  await expect(page.locator('#slashMenu')).toBeHidden();
  // The typed text remains; nothing was inserted.
  expect(await editorValue(page)).toBe('/cal');
});

test('importing a .csv file converts it to a Markdown table', async ({ page }) => {
  const errors = trackErrors(page);
  await page.goto('/converter.html');
  await page.locator('#editor').fill('');
  await page.locator('#fileInput').setInputFiles({
    name: 'data.csv',
    mimeType: 'text/csv',
    buffer: Buffer.from('name,role\nAda,Engineer\nGrace,Admiral')
  });
  await expect.poll(() => editorValue(page)).toContain('| name | role |');
  const value = await editorValue(page);
  expect(value).toContain('| --- | --- |');
  expect(value).toContain('| Ada | Engineer |');
  expect(errors).toEqual([]);
});

test('importing an .html file converts it to Markdown', async ({ page }) => {
  await page.goto('/converter.html');
  await page.locator('#editor').fill('');
  await page.locator('#fileInput').setInputFiles({
    name: 'page.html',
    mimeType: 'text/html',
    buffer: Buffer.from('<h1>Imported</h1><p>Hello <strong>world</strong></p>')
  });
  await expect.poll(() => editorValue(page)).toContain('# Imported');
  expect(await editorValue(page)).toContain('**world**');
});

test('dragging a file over the workspace shows the drop cue', async ({ page }) => {
  await page.goto('/converter.html');
  const workspace = page.locator('#workspace');
  await expect(workspace).not.toHaveClass(/drag-over/);
  // Dispatch synthetic drag events carrying a "Files" dataTransfer in-page,
  // since Playwright cannot serialise a fake DataTransfer across the boundary.
  const fire = type => page.evaluate(eventType => {
    const ev = new Event(eventType, { bubbles: true, cancelable: true });
    Object.defineProperty(ev, 'dataTransfer', { value: { types: ['Files'], files: [] } });
    document.getElementById('workspace').dispatchEvent(ev);
  }, type);
  await fire('dragenter');
  await expect(workspace).toHaveClass(/drag-over/);
  await fire('dragleave');
  await expect(workspace).not.toHaveClass(/drag-over/);
});

test('dropping a .md file on the editor surface loads it exactly once (no duplicate content)', async ({ page }) => {
  // Regression: CodeMirror's built-in drop handler was inserting the raw file
  // text inline AND the workspace drop listener was also calling loadFile() →
  // the document ended up with the content duplicated and format broken.
  // The fix intercepts the drop in capture phase on the editor contentDOM,
  // suppressing CM's native handler and calling loadFile() once.
  await page.goto('/converter.html');
  await page.locator('#editor').fill('');

  const content = '# Hello\n\nThis is the file content.';

  // Simulate a file drop directly on the editor's contentDOM (the CM editable
  // div), which is the element that triggers the CM native file-drop handler.
  // We use a Blob URL so the File object has real readable content.
  await page.evaluate(src => {
    const file = new File([src], 'test.md', { type: 'text/markdown' });
    const dt = new DataTransfer();
    dt.items.add(file);
    const editor = document.querySelector('.cm-content') || document.getElementById('editor');
    if (!editor) throw new Error('Editor contentDOM not found');
    editor.dispatchEvent(new DragEvent('drop', {
      bubbles: true,
      cancelable: true,
      dataTransfer: dt,
    }));
  }, content);

  // Content appears exactly once — no duplicate
  await expect.poll(() => editorValue(page)).toContain('# Hello');
  const value = await editorValue(page);
  const headingCount = (value.match(/# Hello/g) || []).length;
  expect(headingCount).toBe(1);
  expect(value).toContain('This is the file content.');
  // Should not contain raw content duplicated
  const bodyCount = (value.match(/This is the file content\./g) || []).length;
  expect(bodyCount).toBe(1);
});

test('dropping a .md file on the workspace (outside editor) also loads exactly once', async ({ page }) => {
  await page.goto('/converter.html');
  await page.locator('#editor').fill('');

  const content = '# Workspace Drop\n\nDropped on workspace.';

  await page.evaluate(src => {
    const file = new File([src], 'ws.md', { type: 'text/markdown' });
    const dt = new DataTransfer();
    dt.items.add(file);
    document.getElementById('workspace').dispatchEvent(new DragEvent('drop', {
      bubbles: true,
      cancelable: true,
      dataTransfer: dt,
    }));
  }, content);

  await expect.poll(() => editorValue(page)).toContain('# Workspace Drop');
  const value = await editorValue(page);
  const count = (value.match(/# Workspace Drop/g) || []).length;
  expect(count).toBe(1);
});

test('frontmatter is excluded from the preview and surfaces the title', async ({ page }) => {
  const errors = trackErrors(page);
  await page.goto('/converter.html');
  await page.locator('#editor').fill('---\ntitle: Release Notes\ntags: [ide, markdown]\n---\n# Heading\n\nBody text.');
  const preview = page.locator('#previewInner');
  // The Markdown body renders, but the YAML metadata does not leak in.
  await expect(preview).toContainText('Heading');
  await expect(preview).toContainText('Body text.');
  await expect(preview).not.toContainText('title: Release Notes');
  await expect(preview).not.toContainText('tags:');
  // The frontmatter title is surfaced in the document info line.
  await expect(page.locator('#docInfo')).toContainText('Release Notes');
  expect(errors).toEqual([]);
});

test('filename can be edited and is reflected in document info', async ({ page }) => {
  await page.goto('/converter.html');
  await page.locator('#filenameInput').fill('quarterly-report.md');
  await page.locator('#filenameInput').blur();
  await expect(page.locator('#filenameInput')).toHaveValue('quarterly-report.md');
  await expect(page.locator('#docInfo')).toContainText('quarterly-report.md');
});

test('status bar shows estimated tokens', async ({ page }) => {
  await page.goto('/converter.html');
  await page.locator('#editor').fill('Hello world '.repeat(20));
  const tokens = Number(await page.locator('#tokenCount').textContent());
  expect(tokens).toBeGreaterThan(0);
});

test('source panel shows a visible drag and drop hint', async ({ page }) => {
  await page.goto('/converter.html');
  await expect(page.locator('.drop-hint')).toContainText('Drag & drop files here');
});

test('HTML output uses the frontmatter title for the document', async ({ page }) => {
  await page.goto('/converter.html');
  await page.locator('#editor').fill('---\ntitle: My Artifact\n---\n# Hi');
  await page.getByRole('tab', { name: 'HTML' }).click();
  await page.getByRole('button', { name: 'Source', exact: true }).click();
  await expect(page.locator('#previewInner .preview-output-code')).toContainText('<title>My Artifact</title>');
});

test('Jira tab shows Visual/Text toggle defaulting to Visual', async ({ page }) => {
  const errors = trackErrors(page);
  await page.goto('/converter.html');
  await page.locator('#editor').fill('# Hello\n\nSome **bold** text.');
  await page.getByRole('tab', { name: 'Jira' }).click();
  // Toggle buttons exist
  await expect(page.getByRole('button', { name: 'Visual', exact: true })).toBeVisible();
  await expect(page.getByRole('button', { name: 'Text', exact: true })).toBeVisible();
  // Default is Visual: iframe is visible, code block shell stays hidden.
  await expect(page.locator('#previewInner .jira-visual-frame')).toBeVisible();
  await expect(page.locator('#previewInner .preview-output-code')).toBeHidden();
  expect(errors).toEqual([]);
});

test('Jira tab Text view shows raw Jira wiki markup', async ({ page }) => {
  const errors = trackErrors(page);
  await page.goto('/converter.html');
  await page.locator('#editor').fill('# Hello\n\nSome **bold** text.');
  await page.getByRole('tab', { name: 'Jira' }).click();
  await page.getByRole('button', { name: 'Text', exact: true }).click();
  // iframe shell remains mounted but hidden; code block is visible with Jira markup
  await expect(page.locator('#previewInner .jira-visual-frame')).toBeHidden();
  await expect(page.locator('#previewInner .preview-output-code')).toBeVisible();
  const code = await page.locator('#previewInner .preview-output-code').textContent();
  expect(code).toContain('h1.');
  expect(code).toContain('*bold*');
  expect(errors).toEqual([]);
});

test('Jira tab Visual view renders HTML in the iframe', async ({ page }) => {
  const errors = trackErrors(page);
  await page.goto('/converter.html');
  await page.locator('#editor').fill('# Hello\n\nSome **bold** text.');
  await page.getByRole('tab', { name: 'Jira' }).click();
  // Switch to Text then back to Visual
  await page.getByRole('button', { name: 'Text', exact: true }).click();
  await page.getByRole('button', { name: 'Visual', exact: true }).click();
  await expect(page.locator('#previewInner .jira-visual-frame')).toBeVisible();
  await expect(page.locator('#previewInner .preview-output-code')).toBeHidden();
  expect(errors).toEqual([]);
});
