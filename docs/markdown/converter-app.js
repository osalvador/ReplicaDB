// Markup Forge - UI/DOM controller for the Markdown IDE (converter.html).
// This module wires the editor, toolbar, preview and clipboard behaviour to the
// pure conversion logic exported by converter-core.js. It is browser-only and
// runs as a deferred ES module, so the DOM is fully parsed before it executes.
import {
  htmlEscape,
  convertMarkdownToJira,
  jiraToHtml,
  renderMarkdown,
  renderMarkdownForTeams,
  renderNavigableHtml,
  parseMarkdownBlocks,
  buildTeamsClipboard,
  smartPasteToMarkdown,
  buildZip,
  buildSourceArtifactBundle,
  filterSlashCommands,
  importFileToMarkdown,
  parseFrontmatter,
  stripFrontmatter
} from './converter-core.js';
import { createMarkdownEditor } from './editor-cm.js';

const STORAGE_KEY = 'markup-forge.ide.markdown.v1';
const FILE_KEY = 'markup-forge.ide.filename.v1';

const DEFAULT_FILENAME = 'markup-forge-document.md';
const SAMPLE = `# Weekly update

**Summary**

- Item one
- Item two with *emphasis*
- A link to [Microsoft](https://www.microsoft.com)

> This is a quote that should be easy to read in the preview.

\`inline code\` works too.

\`\`\`json
{
  "eventId": "0",
  "eventType": "SecurityNotification"
}
\`\`\`

## Table

| Area | Status |
| --- | --- |
| Markdown IDE | Ready |
| Local preview | Ready |
`;

const state = {
  filename: localStorage.getItem(FILE_KEY) || DEFAULT_FILENAME,
  viewMode: 'split',
  htmlView: 'preview',
  jiraView: 'visual',
  splitPercent: 50,
  renderTimer: 0,
  lastInput: 0,
  syncSource: '',
  skipNextPasteEvent: false
};

const CARET_SYNC_WINDOW_MS = 140;

let editor = document.getElementById('editor');
const lineNumbers = document.getElementById('lineNumbers');
const editorHighlight = document.getElementById('editorHighlight');
const slashMenu = document.getElementById('slashMenu');
const githubEditor = document.getElementById('githubEditor');
const preview = document.getElementById('preview');
const previewInner = document.getElementById('previewInner');
const fileInput = document.getElementById('fileInput');
const filenameInput = document.getElementById('filenameInput');
const copyBtn = document.getElementById('copyBtn');
const copyBtnLabel = document.getElementById('copyBtnLabel');
const pasteMarkdownBtn = document.getElementById('pasteMarkdownBtn');
const smartPasteBtn = null; // removed – "Paste As Markdown" now handles all smart conversion
const outputTabs = Array.from(document.querySelectorAll('[data-output-format]'));
const outputFormat = { value: 'teams' };
const downloadBtn = document.getElementById('downloadBtn');
const exportBundleBtn = document.getElementById('exportBundleBtn');
const clearBtn = document.getElementById('clearBtn');
const insertSampleBtn = document.getElementById('insertSampleBtn');
const workspace = document.getElementById('workspace');
const splitHandle = document.getElementById('splitHandle');
const docInfo = document.getElementById('docInfo');
const toast = document.getElementById('toast');
const statsEls = {
  words: document.getElementById('wordCount'),
  tokens: document.getElementById('tokenCount'),
  chars: document.getElementById('charCount'),
  lines: document.getElementById('lineCount'),
  headings: document.getElementById('headingCount')
};
const modeButtons = {
  split: document.getElementById('modeSplit'),
  editor: document.getElementById('modeEditor'),
  preview: document.getElementById('modePreview')
};

const appRoot = document.querySelector('.app');

const MIN_SPLIT_PERCENT = 25;
const MAX_SPLIT_PERCENT = 75;
const LARGE_DOC_LINE_THRESHOLD = 600;
const LARGE_DOC_CHAR_THRESHOLD = 120000;
const editorHost = editor;
let markdownEditor = null;

// The document title used for HTML output: a frontmatter `title:` when present,
// otherwise the current filename.
function documentTitle() {
  const { attributes } = parseFrontmatter(editor.value);
  const title = attributes && typeof attributes.title === 'string' && attributes.title.trim();
  return title || state.filename || DEFAULT_FILENAME;
}

function normalizeFilename(value) {
  const next = String(value || '').trim();
  return next || DEFAULT_FILENAME;
}

function getCurrentOutput() {
  const markdown = stripFrontmatter(editor.value);
  const format = outputFormat.value;
  if (format === 'html') {
    const html = renderNavigableHtml(markdown, { title: documentTitle() });
    return { text: html, html, label: 'HTML document' };
  }
  if (format === 'jira') return { text: convertMarkdownToJira(markdown), label: 'Jira markup' };
  const teams = buildTeamsClipboard(markdown);
  return { text: teams.text, html: teams.html, bodyHtml: teams.body, label: 'Teams HTML' };
}

function syncOutputProfileOptions() {
  /* Output profiles removed: each format has a single canonical form. */
}

function updateCopyLabel() {
  if (!copyBtnLabel) return;
  const labels = {
    teams: 'Copy for Teams',
    html: 'Copy HTML',
    jira: 'Copy Jira'
  };
  copyBtnLabel.textContent = labels[outputFormat.value] || 'Copy Output';
  outputTabs.forEach(tab => {
    const active = tab.dataset.outputFormat === outputFormat.value;
    tab.classList.toggle('active', active);
    tab.setAttribute('aria-selected', String(active));
  });
}

function clampSplitPercent(value) {
  return Math.min(MAX_SPLIT_PERCENT, Math.max(MIN_SPLIT_PERCENT, value));
}

function estimateTokens(markdown) {
  const chars = String(markdown || '').trim().length;
  if (!chars) return 0;
  return Math.ceil(chars / 4);
}

function shouldUsePerformanceLite(markdown) {
  const text = String(markdown || '');
  const lines = text.split('\n').length;
  return lines >= LARGE_DOC_LINE_THRESHOLD || text.length >= LARGE_DOC_CHAR_THRESHOLD;
}

function shouldRenderPreview(markdown = editor.value) {
  if (state.viewMode === 'editor') return false;
  return true;
}

function getCurrentSplitPercent() {
  return clampSplitPercent(state.splitPercent || 50);
}

function updateSplitHandleAria(percent) {
  if (!splitHandle) return;
  splitHandle.setAttribute('aria-valuenow', String(Math.round(percent)));
  splitHandle.setAttribute('aria-valuetext', `${Math.round(percent)} percent source width`);
}

function setSplitPercent(percent, { persist = true } = {}) {
  const next = clampSplitPercent(percent);
  state.splitPercent = next;
  workspace.style.setProperty('--left-panel-width', `${next}fr`);
  workspace.style.setProperty('--right-panel-width', `${100 - next}fr`);
  updateSplitHandleAria(next);
}

function updateSplitFromPointer(clientX) {
  const rect = workspace.getBoundingClientRect();
  if (!rect.width) return;
  const usableLeft = clientX - rect.left;
  const percent = (usableLeft / rect.width) * 100;
  setSplitPercent(percent);
}

function startSplitResize(startEvent) {
  if (!splitHandle || state.viewMode !== 'split' || window.innerWidth <= 980) return;
  startEvent.preventDefault();
  workspace.classList.add('resizing');
  document.body.classList.add('col-resize-active');
  const move = event => updateSplitFromPointer(event.clientX);
  const stop = () => {
    workspace.classList.remove('resizing');
    document.body.classList.remove('col-resize-active');
    window.removeEventListener('pointermove', move);
    window.removeEventListener('pointerup', stop);
    window.removeEventListener('pointercancel', stop);
  };
  window.addEventListener('pointermove', move);
  window.addEventListener('pointerup', stop);
  window.addEventListener('pointercancel', stop);
  updateSplitFromPointer(startEvent.clientX);
}

function updateLineNumbers() {
  if (lineNumbers) lineNumbers.hidden = true;
}

// Paint the syntax-highlight overlay that sits behind the transparent
// textarea. A trailing newline keeps the overlay at least as tall as the
// textarea so the final line stays aligned while scrolling.
function updateHighlight() {
  if (editorHighlight) editorHighlight.hidden = true;
}

// Mirror the textarea's scroll position onto the overlay and line gutter so
// all three layers stay locked together.
function syncEditorScroll() {
  if (slashState.open) positionSlashMenu();
}

function updateStats(markdown) {
  const trimmed = markdown.trim();
  const words = trimmed ? (trimmed.match(/\b[\p{L}\p{N}_'-]+\b/gu) || []).length : 0;
  const tokens = estimateTokens(markdown);
  const chars = markdown.length;
  const lines = Math.max(1, markdown.split('\n').length);
  const headings = (markdown.match(/^#{1,6}\s+/gm) || []).length;
  statsEls.words.textContent = words;
  statsEls.tokens.textContent = tokens;
  statsEls.chars.textContent = chars;
  statsEls.lines.textContent = lines;
  statsEls.headings.textContent = headings;
}

function setDocInfo() {
  const { attributes } = parseFrontmatter(editor.value);
  const title = attributes && typeof attributes.title === 'string' && attributes.title.trim()
    ? attributes.title.trim()
    : null;
  const name = title || state.filename || DEFAULT_FILENAME;
  const meta = title ? 'Frontmatter title' : 'Autosaved locally';
  docInfo.textContent = `${name} · ${meta} · Browser-only`;
}

function syncFilenameInput() {
  if (!filenameInput) return;
  if (filenameInput.value !== state.filename) filenameInput.value = state.filename;
}

const previewSyncTargets = new WeakSet();
let previewScrollSyncRaf = 0;

function previewScroller() {
  const inner = previewInner.querySelector('.teams-sent-scroll');
  if (inner && inner.scrollHeight - inner.clientHeight > 2) return inner;
  return preview;
}

function withSyncSource(source, fn) {
  const prev = state.syncSource;
  state.syncSource = source;
  try {
    return fn();
  } finally {
    requestAnimationFrame(() => {
      if (state.syncSource === source) state.syncSource = prev;
    });
  }
}

function shouldFollowCaret() {
  return Date.now() - (state.lastInput || 0) < CARET_SYNC_WINDOW_MS;
}

function previewSourceContext() {
  const raw = String(editor.value || '').replace(/\r\n?/g, '\n');
  const { body } = parseFrontmatter(raw);
  const offset = raw.length - body.length;
  const caretOffset = Math.max(0, Math.min(body.length, editor.selectionStart - offset));
  const caretLine = body.slice(0, caretOffset).split('\n').length - 1;
  return { body, caretLine };
}

function activePreviewContext() {
  if (outputFormat.value === 'teams') {
    return { root: previewInner, scroller: previewScroller() };
  }
  if (outputFormat.value === 'html' && state.htmlView === 'preview') {
    const frame = previewInner.querySelector('.html-preview-frame');
    const doc = frame && frame.contentDocument;
    const scroller = doc && (doc.scrollingElement || doc.documentElement || doc.body);
    if (doc && scroller) return { root: doc, scroller };
  }
  if (outputFormat.value === 'html' && state.htmlView === 'source') {
    const code = previewInner.querySelector('.html-source-scroll');
    if (code) return { root: previewInner, scroller: code };
  }
  if (outputFormat.value === 'jira' && state.jiraView === 'visual') {
    const frame = previewInner.querySelector('.jira-visual-frame');
    const doc = frame && frame.contentDocument;
    const scroller = doc && (doc.scrollingElement || doc.documentElement || doc.body);
    if (doc && scroller) return { root: doc, scroller };
  }
  if (outputFormat.value === 'jira' && state.jiraView === 'text') {
    const code = previewInner.querySelector('.jira-text-scroll');
    if (code) return { root: previewInner, scroller: code };
  }
  return null;
}

function supportsMappedViewportSync() {
  return (outputFormat.value === 'html' && state.htmlView === 'preview')
    || (outputFormat.value === 'jira' && state.jiraView === 'visual');
}

function findMappedPreviewTarget(root, caretLine) {
  const blocks = Array.from(root.querySelectorAll('.preview-map-block[data-src-start][data-src-end]'));
  if (!blocks.length) return null;
  let best = null;
  let bestDistance = Number.POSITIVE_INFINITY;
  for (const element of blocks) {
    const startLine = Number(element.getAttribute('data-src-start'));
    const endLine = Number(element.getAttribute('data-src-end'));
    if (!Number.isFinite(startLine) || !Number.isFinite(endLine)) continue;
    if (caretLine >= startLine && caretLine <= endLine) {
      const lineCount = Math.max(1, endLine - startLine + 1);
      const localRatio = Math.min(1, Math.max(0, (caretLine - startLine + 0.5) / lineCount));
      return { element, localRatio };
    }
    const distance = Math.min(Math.abs(caretLine - startLine), Math.abs(caretLine - endLine));
    if (distance < bestDistance) {
      bestDistance = distance;
      best = { element, localRatio: caretLine < startLine ? 0 : 1 };
    }
  }
  return best;
}

// Keep the preview aligned with the editor. While the user is typing we
// follow the caret line (so the part being edited stays visible even with
// images, whose one-line data URI maps to a tall block in the preview).
// When the editor is merely scrolled, we mirror its pixel scroll ratio.
function previewRatioFromCaret() {
  const value = editor.value;
  const totalLines = Math.max(1, value.split('\n').length - 1);
  const caretLine = value.slice(0, editor.selectionStart).split('\n').length - 1;
  return totalLines > 0 ? caretLine / totalLines : 0;
}

function previewRatioFromScroll() {
  const editorMax = editor.scrollHeight - editor.clientHeight;
  return editorMax > 0 ? editor.scrollTop / editorMax : 0;
}

function previewAnchorLineFromViewport() {
  if (!markdownEditor || !markdownEditor.getVisibleLineRange) return null;
  const range = markdownEditor.getVisibleLineRange();
  if (!range) return null;
  const span = Math.max(0, range.to - range.from);
  return Math.max(range.from, Math.min(range.to, Math.round(range.from + span * 0.2)));
}

function previewCenterLineFromScrollContext() {
  const ctx = activePreviewContext();
  if (!ctx || !supportsMappedViewportSync()) return null;
  const scroller = ctx.scroller;
  const centerY = scroller.clientHeight / 2;
  const candidates = Array.from(ctx.root.querySelectorAll('.preview-map-block[data-src-start][data-src-end]'));
  let best = null;
  let bestDistance = Number.POSITIVE_INFINITY;

  for (const element of candidates) {
    const startLine = Number(element.getAttribute('data-src-start'));
    const endLine = Number(element.getAttribute('data-src-end'));
    if (!Number.isFinite(startLine) || !Number.isFinite(endLine)) continue;
    const rect = element.getBoundingClientRect();
    const doc = scroller.ownerDocument;
    const isRootScroller = !!doc && (scroller === doc.scrollingElement || scroller === doc.documentElement || scroller === doc.body);
    const top = isRootScroller ? rect.top : rect.top - scroller.getBoundingClientRect().top;
    const bottom = top + rect.height;
    let distance = 0;
    let localRatio = 0.5;

    if (centerY < top) {
      distance = top - centerY;
      localRatio = 0;
    } else if (centerY > bottom) {
      distance = centerY - bottom;
      localRatio = 1;
    } else {
      const height = Math.max(1, rect.height);
      localRatio = (centerY - top) / height;
      const lineCount = Math.max(1, endLine - startLine + 1);
      return Math.max(startLine, Math.min(endLine, Math.round(startLine + localRatio * (lineCount - 1))));
    }

    if (distance < bestDistance) {
      bestDistance = distance;
      best = { startLine, endLine, localRatio };
    }
  }

  if (!best) return null;
  const lineCount = Math.max(1, best.endLine - best.startLine + 1);
  return Math.max(best.startLine, Math.min(best.endLine, Math.round(best.startLine + best.localRatio * (lineCount - 1))));
}

function handlePreviewScroll() {
  if (!shouldRenderPreview(editor.value)) return;
  if (state.syncSource === 'editor') return;
  if (!markdownEditor) return;

  const ctx = activePreviewContext();
  if (!ctx) return;

  withSyncSource('preview', () => {
    if (supportsMappedViewportSync()) {
      const centerLine = previewCenterLineFromScrollContext();
      if (centerLine != null && markdownEditor.scrollToLine) {
        markdownEditor.scrollToLine(centerLine);
        return;
      }
    }

    const scroller = ctx.scroller;
    const max = scroller.scrollHeight - scroller.clientHeight;
    const ratio = max > 0 ? scroller.scrollTop / max : 0;
    if (markdownEditor.scrollToRatio) markdownEditor.scrollToRatio(ratio);
  });
}

function schedulePreviewScrollSync() {
  if (state.syncSource === 'editor') return;
  if (previewScrollSyncRaf) return;
  previewScrollSyncRaf = requestAnimationFrame(() => {
    previewScrollSyncRaf = 0;
    handlePreviewScroll();
  });
}

function bindPreviewScrollTarget(target) {
  if (!target || typeof target.addEventListener !== 'function' || previewSyncTargets.has(target)) return;
  previewSyncTargets.add(target);
  target.addEventListener('scroll', schedulePreviewScrollSync, { passive: true });
  target.addEventListener('wheel', schedulePreviewScrollSync, { passive: true });
}

function bindActivePreviewScroller() {
  bindPreviewScrollTarget(preview);
  const teams = previewInner.querySelector('.teams-sent-scroll');
  if (teams) bindPreviewScrollTarget(teams);
  const htmlSource = previewInner.querySelector('.html-source-scroll');
  if (htmlSource) bindPreviewScrollTarget(htmlSource);
  const htmlFrame = previewInner.querySelector('.html-preview-frame');
  if (htmlFrame) bindPreviewScrollTarget(htmlFrame);
  if (htmlFrame && htmlFrame.contentDocument) {
    const doc = htmlFrame.contentDocument;
    bindPreviewScrollTarget(doc);
    bindPreviewScrollTarget(doc.scrollingElement || doc.documentElement || doc.body);
    if (doc.defaultView) bindPreviewScrollTarget(doc.defaultView);
  }
  const jiraText = previewInner.querySelector('.jira-text-scroll');
  if (jiraText) bindPreviewScrollTarget(jiraText);
  const jiraFrame = previewInner.querySelector('.jira-visual-frame');
  if (jiraFrame) bindPreviewScrollTarget(jiraFrame);
  if (jiraFrame && jiraFrame.contentDocument) {
    const doc = jiraFrame.contentDocument;
    bindPreviewScrollTarget(doc);
    bindPreviewScrollTarget(doc.scrollingElement || doc.documentElement || doc.body);
    if (doc.defaultView) bindPreviewScrollTarget(doc.defaultView);
  }
}

function previewCenteredRatioFromCaret(scroller) {
  const { body, caretLine } = previewSourceContext();
  const totalLines = Math.max(1, body.split('\n').length - 1);
  const ratio = totalLines > 0 ? caretLine / totalLines : 0;
  const viewportFraction = scroller.scrollHeight > 0 ? scroller.clientHeight / scroller.scrollHeight : 0;
  return Math.max(0, Math.min(1, ratio - (viewportFraction / 2)));
}

function keepPreviewTargetVisible(scroller, element, localRatio = 0.5) {
  const targetRect = element.getBoundingClientRect();
  const viewportHeight = scroller.clientHeight;
  const upperBand = viewportHeight * 0.2;
  const lowerBand = viewportHeight * 0.8;
  const doc = scroller.ownerDocument;
  const isRootScroller = !!doc && (scroller === doc.scrollingElement || scroller === doc.documentElement || scroller === doc.body);
  const anchorY = isRootScroller
    ? targetRect.top + targetRect.height * localRatio
    : (targetRect.top - scroller.getBoundingClientRect().top) + targetRect.height * localRatio;
  let next = scroller.scrollTop;

  if (anchorY < upperBand) {
    next += anchorY - upperBand;
  } else if (anchorY > lowerBand) {
    next += anchorY - lowerBand;
  } else {
    return true;
  }

  const max = Math.max(0, scroller.scrollHeight - scroller.clientHeight);
  scroller.scrollTop = Math.max(0, Math.min(max, next));
  return true;
}

function syncPreviewScroll(useCaret = false) {
  const ctx = activePreviewContext();
  if (!useCaret && ctx && supportsMappedViewportSync()) {
    const anchorLine = previewAnchorLineFromViewport();
    if (anchorLine != null) {
      const target = findMappedPreviewTarget(ctx.root, anchorLine);
      if (target && keepPreviewTargetVisible(ctx.scroller, target.element, target.localRatio)) {
        return;
      }
    }
  }
  if (useCaret) {
    const { caretLine } = previewSourceContext();
    if (ctx) {
      const target = findMappedPreviewTarget(ctx.root, caretLine);
      if (target && keepPreviewTargetVisible(ctx.scroller, target.element, target.localRatio)) {
        return;
      }
    }
  }
  const scroller = ctx ? ctx.scroller : previewScroller();
  const ratio = useCaret ? previewCenteredRatioFromCaret(scroller) : previewRatioFromScroll();
  const scrollerMax = scroller.scrollHeight - scroller.clientHeight;
  scroller.scrollTop = ratio * scrollerMax;
}

function syncPlainPreviewToCaret() {
  if (!(outputFormat.value === 'jira' && state.jiraView === 'text') && !(outputFormat.value === 'html' && state.htmlView === 'source')) return;
  const ctx = activePreviewContext();
  const scroller = ctx ? ctx.scroller : preview;
  const ratio = previewCenteredRatioFromCaret(scroller);
  const max = Math.max(0, scroller.scrollHeight - scroller.clientHeight);
  scroller.scrollTop = ratio * max;
}

function updateIframeDocument(frame, html) {
  if (!frame) return;
  if (frame.dataset.doc === html) return;
  const doc = frame.contentDocument;
  if (!doc || !doc.documentElement) {
    frame.srcdoc = html;
    frame.dataset.doc = html;
    return;
  }
  const parsed = new DOMParser().parseFromString(html, 'text/html');
  doc.head.innerHTML = parsed.head.innerHTML;
  doc.body.innerHTML = parsed.body.innerHTML;
  doc.title = parsed.title;
  frame.dataset.doc = html;
}

function bindHtmlShell(shell) {
  shell.querySelectorAll('[data-html-view]').forEach(btn => {
    btn.addEventListener('click', () => {
      const editorScrollTop = markdownEditor ? markdownEditor.getScrollTop() : 0;
      state.htmlView = btn.dataset.htmlView;
      renderPreview(editor.value, { syncScroll: false });
      requestAnimationFrame(() => {
        if (markdownEditor && typeof markdownEditor.scrollToRatio === 'function') {
          const max = Math.max(0, markdownEditor.scrollDOM.scrollHeight - markdownEditor.scrollDOM.clientHeight);
          const ratio = max > 0 ? editorScrollTop / max : 0;
          markdownEditor.scrollToRatio(ratio);
        }
      });
    });
  });
}

function ensureHtmlShell() {
  let shell = previewInner.querySelector('.html-preview-shell');
  if (shell) return shell;
  shell = document.createElement('div');
  shell.className = 'html-preview-shell';
  shell.innerHTML = `<div class="html-view-toggle" role="group" aria-label="HTML preview mode">`
    + `<button type="button" class="html-view-btn" data-html-view="preview">Preview</button>`
    + `<button type="button" class="html-view-btn" data-html-view="source">Source</button>`
    + `</div>`
    + `<iframe class="html-preview-frame" sandbox="allow-same-origin" title="Rendered HTML preview"></iframe>`
    + `<div class="html-source-scroll" hidden><pre class="preview-output-code"><code></code></pre></div>`;
  previewInner.replaceChildren(shell);
  bindHtmlShell(shell);
  shell.querySelector('.html-preview-frame').addEventListener('load', () => {
    bindActivePreviewScroller();
    if (outputFormat.value !== 'html' || state.htmlView !== 'preview') return;
    requestAnimationFrame(() => syncPreviewScroll(shouldFollowCaret()));
  });
  return shell;
}

function renderJiraMappedBody(markdown, { sourceMap = false } = {}) {
  const blocks = parseMarkdownBlocks(markdown);
  return blocks.map(block => {
    const jiraText = convertMarkdownToJira(block.text);
    const html = jiraToHtml(jiraText) || '<p>&nbsp;</p>';
    if (!sourceMap) return html;
    return `<div class="preview-map-block" data-src-start="${block.startLine}" data-src-end="${block.endLine}">${html}</div>`;
  }).join('\n');
}

function bindJiraShell(shell) {
  shell.querySelectorAll('[data-jira-view]').forEach(btn => {
    btn.addEventListener('click', () => {
      const editorScrollTop = markdownEditor ? markdownEditor.getScrollTop() : 0;
      state.jiraView = btn.dataset.jiraView;
      renderPreview(editor.value, { syncScroll: false });
      requestAnimationFrame(() => {
        if (markdownEditor && typeof markdownEditor.scrollToRatio === 'function') {
          const max = Math.max(0, markdownEditor.scrollDOM.scrollHeight - markdownEditor.scrollDOM.clientHeight);
          const ratio = max > 0 ? editorScrollTop / max : 0;
          markdownEditor.scrollToRatio(ratio);
        }
      });
    });
  });
}

function ensureJiraShell() {
  let shell = previewInner.querySelector('.jira-preview-shell');
  if (shell) return shell;
  shell = document.createElement('div');
  shell.className = 'jira-preview-shell';
  shell.innerHTML = `<div class="jira-view-toggle" role="group" aria-label="Jira preview mode">`
    + `<button type="button" class="jira-view-btn" data-jira-view="visual">Visual</button>`
    + `<button type="button" class="jira-view-btn" data-jira-view="text">Text</button>`
    + `</div>`
    + `<iframe class="jira-visual-frame" sandbox="allow-same-origin" title="Jira visual preview"></iframe>`
    + `<div class="jira-text-scroll" hidden><pre class="preview-output-code"><code></code></pre></div>`;
  previewInner.replaceChildren(shell);
  bindJiraShell(shell);
  shell.querySelector('.jira-visual-frame').addEventListener('load', () => {
    bindActivePreviewScroller();
    if (outputFormat.value !== 'jira' || state.jiraView !== 'visual') return;
    requestAnimationFrame(() => syncPreviewScroll(shouldFollowCaret()));
  });
  return shell;
}

// Render the HTML output tab. In "preview" mode the navigable artifact is
// shown rendered inside a sandboxed iframe (no allow-scripts, so embedded JS
// cannot run); in "source" mode the raw HTML is shown as code. A small toggle
// switches between the two.
function renderHtmlOutput(markdown) {
  const doc = renderNavigableHtml(markdown, { title: documentTitle(), softBreaks: true, sourceMap: true });
  const shell = ensureHtmlShell();
  const frame = shell.querySelector('.html-preview-frame');
  const source = shell.querySelector('.preview-output-code');
  const sourceScroll = shell.querySelector('.html-source-scroll');
  const code = source.querySelector('code');
  shell.querySelectorAll('[data-html-view]').forEach(btn => {
    const active = btn.dataset.htmlView === state.htmlView;
    btn.classList.toggle('active', active);
    btn.setAttribute('aria-pressed', String(active));
  });
  frame.hidden = state.htmlView !== 'preview';
  source.hidden = state.htmlView !== 'source';
  if (sourceScroll) sourceScroll.hidden = state.htmlView !== 'source';
  if (state.htmlView === 'source') {
    code.textContent = doc;
  } else {
    updateIframeDocument(frame, doc);
  }
}

// Render the Jira output tab. In "visual" mode the Markdown is rendered as
// HTML using Atlassian-flavoured CSS inside a sandboxed iframe; in "text" mode
// the raw Jira wiki markup is shown as code. A small toggle switches between
// the two — mirroring the toggle already present in the HTML tab.
function buildJiraVisualHtml(markdown, options = {}) {
  const body = renderJiraMappedBody(markdown, { sourceMap: !!options.sourceMap });
  return `<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<style>
  body {
    margin: 0; padding: 18px 22px;
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu,
      'Fira Sans', 'Droid Sans', 'Helvetica Neue', sans-serif;
    font-size: 14px; line-height: 1.42857143;
    color: #172b4d; background: #fff;
  }
  h1 { font-size: 24px; font-weight: 700; margin: 0 0 12px; }
  h2 { font-size: 20px; font-weight: 700; margin: 16px 0 10px; }
  h3 { font-size: 16px; font-weight: 700; margin: 14px 0 8px; }
  h4,h5,h6 { font-size: 14px; font-weight: 700; margin: 12px 0 6px; }
  p { margin: 0 0 10px; }
  a { color: #0052cc; text-decoration: none; }
  a:hover { text-decoration: underline; }
  code { font-family: 'SFMono-Regular', Consolas, 'Liberation Mono', Menlo, monospace;
    font-size: 12px; background: #f4f5f7; border-radius: 3px; padding: 1px 4px; }
  pre { background: #f4f5f7; border-radius: 3px; padding: 12px 16px; overflow-x: auto; }
  pre code { background: none; padding: 0; font-size: 13px; }
  blockquote { border-left: 3px solid #dfe1e5; margin: 0 0 10px 0;
    padding: 4px 0 4px 14px; color: #5e6c84; }
  table { border-collapse: collapse; margin: 0 0 12px; }
  th, td { border: 1px solid #dfe1e5; padding: 7px 10px; text-align: left; }
  th { background: #f4f5f7; font-weight: 700; }
  ul, ol { margin: 0 0 10px; padding-left: 24px; }
  li { margin-bottom: 4px; }
  hr { border: none; border-top: 1px solid #dfe1e5; margin: 16px 0; }
  .hljs-attr    { color: #0065ff; }
  .hljs-string  { color: #00875a; }
  .hljs-number  { color: #ff5630; }
  .hljs-keyword { color: #6554c0; }
  .hljs-comment { color: #5e6c84; font-style: italic; }
  .preview-map-block:empty::before { content: '\\00a0'; }
</style></head><body>${body}</body></html>`;
}

function renderJiraOutput(markdown) {
  const jiraText = convertMarkdownToJira(markdown);
  const shell = ensureJiraShell();
  const frame = shell.querySelector('.jira-visual-frame');
  const source = shell.querySelector('.preview-output-code');
  const sourceScroll = shell.querySelector('.jira-text-scroll');
  const code = source.querySelector('code');
  shell.querySelectorAll('[data-jira-view]').forEach(btn => {
    const active = btn.dataset.jiraView === state.jiraView;
    btn.classList.toggle('active', active);
    btn.setAttribute('aria-pressed', String(active));
  });
  frame.hidden = state.jiraView !== 'visual';
  source.hidden = state.jiraView !== 'text';
  if (sourceScroll) sourceScroll.hidden = state.jiraView !== 'text';
  if (state.jiraView === 'text') {
    code.textContent = jiraText;
  } else {
    updateIframeDocument(frame, buildJiraVisualHtml(markdown, { sourceMap: true }));
  }
}

function renderPreview(markdown, { syncScroll = true } = {}) {
  if (!shouldRenderPreview(markdown)) {
    updateCopyLabel();
    return;
  }
  syncOutputProfileOptions();
  const format = outputFormat.value;
  const source = stripFrontmatter(markdown);
  previewInner.className = `preview-inner mode-${format}`;
  preview.classList.toggle('preview-no-scroll', false);
  if (format === 'html') {
    renderHtmlOutput(source);
  } else if (format === 'jira') {
    renderJiraOutput(source);
  } else {
    previewInner.innerHTML = renderMarkdownForTeams(source, { sourceMap: true });
  }
  updateCopyLabel();
  bindActivePreviewScroller();
  if (syncScroll) {
    syncPreviewScroll(shouldFollowCaret());
    syncPlainPreviewToCaret();
  }
}

function scheduleUpdate() {
  clearTimeout(state.renderTimer);
  state.renderTimer = setTimeout(() => {
    const markdown = editor.value;
    if (githubEditor) githubEditor.classList.toggle('performance-lite', shouldUsePerformanceLite(markdown));
    localStorage.setItem(STORAGE_KEY, markdown);
    localStorage.setItem(FILE_KEY, normalizeFilename(state.filename));
    updateLineNumbers();
    updateHighlight();
    updateStats(markdown);
    renderPreview(markdown);
    syncFilenameInput();
    setDocInfo();
  }, 80);
}

function setViewMode(mode) {
  state.viewMode = mode;
  workspace.className = `workspace ${mode === 'split' ? 'split' : mode === 'editor' ? 'editor-only' : 'preview-only'}`;
  Object.entries(modeButtons).forEach(([key, btn]) => btn.classList.toggle('active', key === mode));
  if (mode === 'split' && window.innerWidth > 980) setSplitPercent(50, { persist: false });
  renderPreview(markdownEditor.getValue());
}


// Generation counter used by loadFile to discard results from superseded calls.
// If loadFile is triggered twice in rapid succession (e.g. a race between the
// workspace drop listener and the editor onDrop hook), only the latest call
// commits its result to the editor.
let loadFileGeneration = 0;

async function loadFile(file) {
  const myGeneration = ++loadFileGeneration;
  if (markdownEditor.getValue() && !confirm(`Replace current content with "${file.name}"?`)) return;
  try {
    const text = await file.text();
    if (myGeneration !== loadFileGeneration) return; // superseded by a newer call
    markdownEditor.setValue(importFileToMarkdown(text, file.name || ''));
    state.filename = normalizeFilename(file.name || DEFAULT_FILENAME);
    scheduleUpdate();
    showToast(`Opened ${state.filename}`);
  } catch (error) {
    console.error(error);
    showToast('Could not open file');
  }
}

function downloadMarkdown(markdown, filename) {
  const blob = new Blob([markdown], { type: 'text/markdown;charset=utf-8' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  const safeName = (filename || DEFAULT_FILENAME).replace(/\.(txt|markdown)$/i, '.md');
  a.download = /\.md$/i.test(safeName) ? safeName : `${safeName}.md`;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

// Export a "Source + Artifact" bundle: a ZIP containing the Markdown source,
// a navigable standalone HTML artifact and a README. Built entirely in the
// browser with no dependencies (see buildZip / buildSourceArtifactBundle).
function exportSourceArtifact(markdown, filename) {
  const safeName = (filename || DEFAULT_FILENAME).replace(/\.[^./]*$/, '') || 'document';
  const files = buildSourceArtifactBundle(markdown, filename || DEFAULT_FILENAME);
  const zipBytes = buildZip(files);
  const blob = new Blob([zipBytes], { type: 'application/zip' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = `${safeName}-source-artifact.zip`;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
  showToast('Exported Source + Artifact bundle');
}

// Copy rich HTML to the clipboard by selecting a hidden contenteditable
// element and firing the synchronous copy command. This works even when the
// async Clipboard API is unavailable (e.g. file:// or other insecure
// contexts) and is what lets Microsoft Teams paste with formatting instead
// of falling back to plain text.
function copyHtmlViaExecCommand(bodyHtml, plainText) {
  const holder = document.createElement('div');
  holder.contentEditable = 'true';
  holder.setAttribute('aria-hidden', 'true');
  holder.style.position = 'fixed';
  holder.style.left = '-9999px';
  holder.style.top = '0';
  holder.style.whiteSpace = 'pre-wrap';
  holder.innerHTML = bodyHtml;
  document.body.appendChild(holder);
  const selection = window.getSelection();
  const previous = selection.rangeCount ? selection.getRangeAt(0) : null;
  const range = document.createRange();
  range.selectNodeContents(holder);
  selection.removeAllRanges();
  selection.addRange(range);
  let ok = false;
  try { ok = document.execCommand('copy'); } catch (error) { ok = false; }
  selection.removeAllRanges();
  if (previous) selection.addRange(previous);
  holder.remove();
  if (!ok && plainText !== undefined) {
    const ta = document.createElement('textarea');
    ta.value = plainText;
    ta.style.position = 'fixed';
    ta.style.left = '-9999px';
    document.body.appendChild(ta);
    ta.select();
    try { ok = document.execCommand('copy'); } catch (error) { ok = false; }
    ta.remove();
  }
  return ok;
}

async function copyMarkdown() {
  const output = getCurrentOutput();
  const isTeams = outputFormat.value === 'teams';
  try {
    if (isTeams && navigator.clipboard && navigator.clipboard.write && window.ClipboardItem) {
      await navigator.clipboard.write([new ClipboardItem({
        'text/html': new Blob([output.html], { type: 'text/html' }),
        'text/plain': new Blob([output.text], { type: 'text/plain' })
      })]);
    } else if (isTeams) {
      // Insecure context (no async Clipboard API): copy rich HTML via execCommand.
      if (!copyHtmlViaExecCommand(output.bodyHtml, output.text)) throw new Error('Rich copy failed');
    } else {
      if (!navigator.clipboard || !navigator.clipboard.writeText) throw new Error('Clipboard API unavailable');
      await navigator.clipboard.writeText(output.text);
    }
    showToast(`Copied ${output.label}`);
  } catch (error) {
    console.error(error);
    if (isTeams && copyHtmlViaExecCommand(output.bodyHtml, output.text)) {
      showToast(`Copied ${output.label}`);
      return;
    }
    const helper = document.createElement('textarea');
    helper.value = output.text;
    helper.setAttribute('readonly', '');
    helper.style.position = 'fixed';
    helper.style.opacity = '0';
    document.body.appendChild(helper);
    helper.select();
    document.execCommand('copy');
    helper.remove();
    showToast(`Copied ${output.label} using fallback`);
  }
}

function replaceWholeDocument(nextValue, nextFilename, message) {
  markdownEditor.replaceDocument(nextValue);
  state.filename = normalizeFilename(nextFilename || DEFAULT_FILENAME);
  scheduleUpdate();
  showToast(message);
}

function clearDocument() {
  if (markdownEditor.getValue() && !confirm('Clear the current Markdown document?')) return;
  replaceWholeDocument('', DEFAULT_FILENAME, 'Document cleared');
}

function blobToDataUrl(blob) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(reader.result);
    reader.onerror = () => reject(reader.error);
    reader.readAsDataURL(blob);
  });
}

// Insert text at the current caret position without destroying the rest of
// the document. Uses execCommand so the change is part of the native undo
// stack and the textarea keeps focus.
function insertTextAtCursor(text) {
  markdownEditor.replaceSelection(text);
  scheduleUpdate();
}

// Detect images that Teams referenced in the HTML but did NOT ship as a
// binary clipboard flavour (e.g. real picture messages, which Teams keeps
// internal). Emoji and data URIs are already self-contained, so ignore them.
function countMissingImages(html) {
  if (!html) return 0;
  try {
    const doc = new DOMParser().parseFromString(html, 'text/html');
    return Array.from(doc.querySelectorAll('img')).filter(img => {
      const itemtype = img.getAttribute('itemtype') || '';
      if (/schema\.skype\.com\/Emoji/i.test(itemtype)) return false;
      const src = img.getAttribute('src') || '';
      const targetSrc = img.getAttribute('target-src') || '';
      // Usable if we have a network/data URL; local file:/blob: refs still need
      // clipboard data to become portable in the app.
      const usable = [src, targetSrc].some(u => /^(?:https?:|data:)/i.test(u));
      return !usable;
    }).length;
  } catch {
    return 0;
  }
}

function reconcileClipboardImages(markdown, imageDataUrls = []) {
  const source = String(markdown || '');
  if (!imageDataUrls.length) return source;

  const remaining = [...imageDataUrls];
  const imagePattern = /!\[([^\]]*)\]\(([^)\s]+)(?:\s+"([^"]+)")?\)/g;
  let rebuilt = '';
  let lastIndex = 0;
  let matched = false;

  for (const match of source.matchAll(imagePattern)) {
    matched = true;
    const [full, alt, url, title] = match;
    rebuilt += source.slice(lastIndex, match.index);
    let nextUrl = url;
    if (/^(?:blob:|file:)/i.test(url) && remaining.length) {
      nextUrl = remaining.shift();
    }
    rebuilt += `![${alt}](${nextUrl}${title ? ` "${title}"` : ''})`;
    lastIndex = match.index + full.length;
  }

  if (!matched) {
    const suffix = remaining.map(url => `![](${url})`).join('\n\n');
    if (!suffix) return source.trim();
    const prefix = source.replace(/\s+$/, '');
    return prefix ? `${prefix}\n\n${suffix}` : suffix;
  }

  rebuilt += source.slice(lastIndex);
  return rebuilt.replace(/\n{3,}/g, '\n\n').trim();
}

// Turn rich clipboard content (HTML, plain text and/or binary images copied
// from the clipboard) into Markdown and insert it at the caret. Images that ship as
// binary clipboard flavours are embedded as data URIs so they stay offline.
// `replace` truncates the document first (used by the toolbar button).
async function importRichClipboard({ html, plain, imageDataUrls = [] }, { replace = false } = {}) {
  // Use smart conversion: HTML → faithful rich Markdown; plain-only →
  // auto-detect TSV/CSV tables, Jira wiki markup, code blocks, or clean prose.
  let markdown = smartPasteToMarkdown({ html: html || '', plain: plain || '' });
  markdown = reconcileClipboardImages(markdown, imageDataUrls);
  if (!markdown.trim()) {
    showToast('Clipboard is empty');
    return;
  }
  // Always end with a single newline so the caret lands on a fresh line after
  // pasting. A double newline (\n\n) was used before but produced an unwanted
  // blank line at the end of every paste, which the user had to manually delete.
  const markdownWithBreak = `${markdown.replace(/\s+$/, '')}\n`;
  if (replace) {
    replaceWholeDocument(markdownWithBreak, state.filename, 'Pasted as Markdown');
  } else {
    const selection = markdownEditor.getSelection();
    markdownEditor.replaceRange(selection.from, selection.to, markdownWithBreak, {
      anchor: selection.from + markdownWithBreak.length
    });
    scheduleUpdate();
  }
  // Warn when the clipboard referenced an image it did not actually copy.
  const missing = Math.max(0, countMissingImages(html) - imageDataUrls.length);
  if (missing > 0) {
    showToast('Image not copied by the source app — copy the image again and paste once more');
  } else {
    showToast(replace ? 'Pasted as Markdown' : 'Pasted as Markdown at cursor');
  }
}

async function pasteFromTeams() {
  if (!navigator.clipboard || !navigator.clipboard.read) {
    showToast('Rich clipboard read is not available here');
    return;
  }
  try {
    const items = await navigator.clipboard.read();
    let html = '';
    let plain = '';
    const types = [];
    const imageDataUrls = [];
    for (const item of items) {
      item.types.forEach(t => { if (!types.includes(t)) types.push(t); });
      if (!html && item.types.includes('text/html')) html = await (await item.getType('text/html')).text();
      if (!plain && item.types.includes('text/plain')) plain = await (await item.getType('text/plain')).text();
      const imageType = item.types.find(type => type.startsWith('image/'));
      if (imageType) {
        try { imageDataUrls.push(await blobToDataUrl(await item.getType(imageType))); } catch { /* ignore */ }
      }
    }
    if (PASTE_DEBUG) { capturePasteDiagnostics({ html, plain, types, imageDataUrls }); return; }
    await importRichClipboard({ html, plain, imageDataUrls });
  } catch (error) {
    console.warn(error);
    showToast('Could not read the clipboard');
  }
}

// Diagnostic capture: dump the RAW clipboard flavours (text/html, text/plain,
// image/*) so we can inspect exactly what an app like Teams writes. Enabled
// with ?debugpaste in the URL or by Alt+clicking the paste button. The raw
// dump is written into the editor so it can be copied and shared verbatim.
const PASTE_DEBUG = /[?&]debugpaste\b/.test(location.search);

function buildPasteDiagnostics({ html, plain, types = [], imageDataUrls = [] }) {
  const lines = [];
  lines.push('# Clipboard diagnostics');
  lines.push('');
  lines.push(`- Flavours: ${types.length ? types.join(', ') : '(unknown)'}`);
  lines.push(`- Images captured: ${imageDataUrls.length}`);
  lines.push('');
  lines.push('## text/plain');
  lines.push('```');
  lines.push(plain || '(empty)');
  lines.push('```');
  lines.push('');
  lines.push('## text/html (raw)');
  lines.push('```html');
  lines.push(html || '(empty)');
  lines.push('```');
  imageDataUrls.forEach((url, i) => {
    lines.push('');
    lines.push(`## image ${i + 1} (truncated)`);
    lines.push('```');
    lines.push(url.slice(0, 120) + (url.length > 120 ? `… (${url.length} chars total)` : ''));
    lines.push('```');
  });
  return lines.join('\n');
}

function capturePasteDiagnostics(payload) {
  const dump = buildPasteDiagnostics(payload);
  window.__lastPaste = payload;
  console.log('[Markup Forge] RAW clipboard capture:', payload);
  replaceWholeDocument(dump, state.filename, 'Captured raw clipboard (debug)');
}

// Intercept native paste so rich HTML or images become Markdown instead of
// the browser's default plain-text/raw paste.
function handleEditorPaste(event) {
  const data = event.clipboardData;
  if (!data) return false;
  if (state.skipNextPasteEvent) {
    state.skipNextPasteEvent = false;
    // Return true so the paste event is suppressed (preventDefault + stopImmediatePropagation
    // via the editor-cm.js listener). pasteFromTeams() called from onKeydown handles the
    // actual insertion via navigator.clipboard.read().
    return true;
  }
  const html = data.getData('text/html');
  const imageFiles = Array.from(data.files || data.items || [])
    .filter(entry => entry && entry.type && entry.type.startsWith('image/'));
  if (!html && !imageFiles.length && !PASTE_DEBUG) return false;
  event.preventDefault();
  event.stopImmediatePropagation();
  const plain = data.getData('text/plain');
  void (async () => {
    const imageDataUrls = [];
    for (const entry of imageFiles) {
      const blob = typeof entry.getAsFile === 'function' ? entry.getAsFile() : entry;
      if (blob) {
        try { imageDataUrls.push(await blobToDataUrl(blob)); } catch { /* ignore */ }
      }
    }
    if (PASTE_DEBUG) {
      capturePasteDiagnostics({ html, plain, types: Array.from(data.types || []), imageDataUrls });
      return;
    }
    await importRichClipboard({ html, plain, imageDataUrls });
  })();
  return true;
}

// Intercept file drops on the editor surface (capture phase, runs before
// CodeMirror's own drop handler). Without this, CM reads the raw file text and
// inserts it inline; the workspace drop listener also fires → double load.
// Returning true causes editor-cm.js to call preventDefault + stopImmediatePropagation,
// which blocks CM's built-in file-drop path AND stops the event from bubbling to
// the workspace drop listener. We therefore replicate the workspace side-effects
// (drag visual state reset) and call loadFile ourselves.
function handleEditorDrop(event) {
  const dt = event.dataTransfer;
  if (!dt) return false;
  const types = dt.types;
  if (!types || Array.prototype.indexOf.call(types, 'Files') === -1) return false;
  // Reset workspace drag-over visual state — the workspace 'drop' listener won't
  // fire because stopImmediatePropagation prevents the event from bubbling to it.
  dragDepth = 0;
  workspace.classList.remove('drag-over');
  const file = dt.files && dt.files[0];
  if (file) void loadFile(file);
  return true; // tell editor-cm.js: preventDefault + stopImmediatePropagation
}


function showToast(message) {
  toast.textContent = message;
  toast.classList.add('show');
  clearTimeout(showToast.timer);
  showToast.timer = setTimeout(() => toast.classList.remove('show'), 1800);
}


function replaceSelection(before, after = '', placeholder = '') {
  markdownEditor.wrapSelection(before, after, placeholder);
  scheduleUpdate();
}

function prefixSelectedLines(prefixFactory) {
  markdownEditor.replaceLines(prefixFactory);
  scheduleUpdate();
}

function insertLink() {
  markdownEditor.insertLink();
  scheduleUpdate();
}

function runToolbarAction(action) {
  if (action === 'heading') return prefixSelectedLines(() => '## ');
  if (action === 'bold') return replaceSelection('**', '**', 'strong text');
  if (action === 'italic') return replaceSelection('*', '*', 'emphasized text');
  if (action === 'quote') return prefixSelectedLines(() => '> ');
  if (action === 'code') return replaceSelection('`', '`', 'code');
  if (action === 'link') return insertLink();
  if (action === 'ul') return prefixSelectedLines(() => '- ');
  if (action === 'ol') return prefixSelectedLines(index => `${index + 1}. `);
  if (action === 'task') return prefixSelectedLines(() => '- [ ] ');
  if (action === 'codeblock') return replaceSelection('```\n', '\n```', 'code');
}

// --- Slash command menu (T1c) ----------------------------------------------
const slashState = { open: false, start: 0, items: [], active: 0 };

function closeSlashMenu() {
  if (!slashState.open) return;
  slashState.open = false;
  if (slashMenu) { slashMenu.hidden = true; slashMenu.innerHTML = ''; }
}

function positionSlashMenu() {
  if (!slashMenu) return;
  const coords = markdownEditor && markdownEditor.coordsAtPos(markdownEditor.getCursorOffset());
  const hostRect = markdownEditor && markdownEditor.dom.getBoundingClientRect();
  if (!coords || !hostRect) return;
  slashMenu.style.top = `${Math.max(4, coords.bottom - hostRect.top + 4)}px`;
  slashMenu.style.left = `${Math.max(8, coords.left - hostRect.left)}px`;
}

function renderSlashMenu() {
  if (!slashMenu) return;
  slashMenu.innerHTML = slashState.items.map((cmd, i) => (
    `<button type="button" class="slash-item${i === slashState.active ? ' active' : ''}" role="option" data-cmd="${cmd.name}" aria-selected="${i === slashState.active}">`
    + `<span class="slash-name">/${cmd.name}</span>`
    + `<span class="slash-desc">${cmd.description}</span>`
    + `</button>`
  )).join('');
  slashMenu.hidden = false;
  positionSlashMenu();
}

function detectSlashTrigger() {
  if (!slashMenu) return;
  if (!markdownEditor) return closeSlashMenu();
  const { from, to } = markdownEditor.getSelection();
  if (from !== to) return closeSlashMenu();
  const pos = markdownEditor.getCursorOffset();
  const lineStart = markdownEditor.lineStartAt(pos);
  const before = markdownEditor.textBetween(lineStart, pos);
  const match = before.match(/(^|\s)\/(\w*)$/);
  if (!match) return closeSlashMenu();
  const query = match[2];
  const items = filterSlashCommands(query);
  if (!items.length) return closeSlashMenu();
  slashState.open = true;
  slashState.start = pos - query.length - 1; // index of the "/"
  slashState.items = items;
  slashState.active = 0;
  renderSlashMenu();
}

function applySlashCommand(cmd) {
  if (!cmd) return;
  closeSlashMenu();
  const current = markdownEditor.getCursorOffset();
  markdownEditor.replaceSelection(cmd.snippet, {
    from: slashState.start,
    to: current,
    selection: { anchor: slashState.start + (typeof cmd.caret === 'number' ? cmd.caret : cmd.snippet.length) }
  });
  const caretPos = slashState.start + (typeof cmd.caret === 'number' ? cmd.caret : cmd.snippet.length);
  markdownEditor.setSelection(caretPos, caretPos);
  scheduleUpdate();
}
if (slashMenu) {
  slashMenu.addEventListener('mousedown', event => {
    const btn = event.target.closest('.slash-item');
    if (!btn) return;
    event.preventDefault(); // keep focus in the editor
    const cmd = slashState.items.find(c => c.name === btn.dataset.cmd);
    applySlashCommand(cmd);
  });
}
let scrollSyncRaf = 0;
function handleEditorScroll() {
  if (state.syncSource === 'preview') return;
  syncEditorScroll();
  if (!shouldRenderPreview(editor.value)) return;
  if (scrollSyncRaf) return;
  scrollSyncRaf = requestAnimationFrame(() => {
    scrollSyncRaf = 0;
    // If the scroll was triggered by typing (editor auto-scrolls to keep the
    // caret visible), follow the caret instead of the raw pixel ratio.
    withSyncSource('editor', () => {
      syncPreviewScroll(shouldFollowCaret());
      syncPlainPreviewToCaret();
    });
  });
}
document.querySelectorAll('[data-md-action]').forEach(button => {
  button.addEventListener('click', () => runToolbarAction(button.dataset.mdAction));
});
fileInput.addEventListener('change', event => {
  const file = event.target.files && event.target.files[0];
  if (file) loadFile(file);
  event.target.value = '';
});
// Drag & drop import (T1d): drop a .md/.markdown/.txt/.html/.csv file onto the
// workspace to import it as Markdown. A visual cue marks the active drop zone.
let dragDepth = 0;
function isFileDrag(event) {
  const types = event.dataTransfer && event.dataTransfer.types;
  return !!types && Array.prototype.indexOf.call(types, 'Files') !== -1;
}
workspace.addEventListener('dragenter', event => {
  if (!isFileDrag(event)) return;
  event.preventDefault();
  dragDepth++;
  workspace.classList.add('drag-over');
});
workspace.addEventListener('dragover', event => {
  if (!isFileDrag(event)) return;
  event.preventDefault();
  event.dataTransfer.dropEffect = 'copy';
});
workspace.addEventListener('dragleave', event => {
  if (!isFileDrag(event)) return;
  dragDepth = Math.max(0, dragDepth - 1);
  if (dragDepth === 0) workspace.classList.remove('drag-over');
});
workspace.addEventListener('drop', event => {
  if (!isFileDrag(event)) return;
  event.preventDefault();
  dragDepth = 0;
  workspace.classList.remove('drag-over');
  const file = event.dataTransfer.files && event.dataTransfer.files[0];
  if (file) loadFile(file);
});
copyBtn.addEventListener('click', () => copyMarkdown());
downloadBtn.addEventListener('click', () => downloadMarkdown(markdownEditor.getValue(), state.filename));
if (exportBundleBtn) exportBundleBtn.addEventListener('click', () => exportSourceArtifact(markdownEditor.getValue(), state.filename));
clearBtn.addEventListener('click', clearDocument);
if (pasteMarkdownBtn) pasteMarkdownBtn.addEventListener('click', () => pasteFromTeams());
insertSampleBtn.addEventListener('click', () => {
  if (markdownEditor.getValue() && !confirm('Replace current content with the sample document?')) return;
  replaceWholeDocument(SAMPLE, 'sample-markdown-document.md', 'Sample loaded');
});
if (filenameInput) {
  syncFilenameInput();
  filenameInput.addEventListener('input', () => {
    state.filename = normalizeFilename(filenameInput.value);
    localStorage.setItem(FILE_KEY, state.filename);
    setDocInfo();
  });
  filenameInput.addEventListener('blur', () => {
    state.filename = normalizeFilename(filenameInput.value);
    syncFilenameInput();
    localStorage.setItem(FILE_KEY, state.filename);
    setDocInfo();
  });
}
modeButtons.split.addEventListener('click', () => setViewMode('split'));
modeButtons.editor.addEventListener('click', () => setViewMode('editor'));
modeButtons.preview.addEventListener('click', () => setViewMode('preview'));
outputTabs.forEach(tab => tab.addEventListener('click', () => { outputFormat.value = tab.dataset.outputFormat; renderPreview(markdownEditor.getValue(), { syncScroll: false, preserveEditorScroll: true }); }));
if (splitHandle) {
  splitHandle.addEventListener('pointerdown', startSplitResize);
  splitHandle.addEventListener('keydown', event => {
    if (state.viewMode !== 'split') return;
    const step = event.shiftKey ? 10 : 5;
    const current = getCurrentSplitPercent();
    if (event.key === 'ArrowLeft') {
      event.preventDefault();
      setSplitPercent(current - step);
    } else if (event.key === 'ArrowRight') {
      event.preventDefault();
      setSplitPercent(current + step);
    } else if (event.key === 'Home') {
      event.preventDefault();
      setSplitPercent(MIN_SPLIT_PERCENT);
    } else if (event.key === 'End') {
      event.preventDefault();
      setSplitPercent(MAX_SPLIT_PERCENT);
    }
  });
}

window.addEventListener('resize', () => {
  if (state.viewMode === 'split' && window.innerWidth > 980) {
    setSplitPercent(getCurrentSplitPercent(), { persist: false });
  }
});

window.addEventListener('keydown', event => {
  const mod = event.metaKey || event.ctrlKey;
  if (mod && event.key.toLowerCase() === 's') {
    event.preventDefault();
    downloadMarkdown(markdownEditor.getValue(), state.filename);
  }
});

const saved = localStorage.getItem(STORAGE_KEY);
markdownEditor = createMarkdownEditor(editorHost, {
  initialValue: saved || SAMPLE,
  onChange(value) {
    state.lastInput = Date.now();
    updateStats(value);
    detectSlashTrigger();
    scheduleUpdate();
  },
  onScroll() {
    handleEditorScroll();
  },
  onBlur() {
    setTimeout(closeSlashMenu, 120);
  },
  onBeforeInput(event) {
    // Block native paste insertion — pasteFromTeams handles it via clipboard API.
    if (event.inputType === 'insertFromPaste') return true;
    // Block browser rich-text formatting events (formatBold, formatItalic, etc.).
    // Our capture-phase keydown handler already applied the equivalent Markdown
    // wrapping via wrapSelection(), so the browser's own formatting must not run.
    if (event.inputType.startsWith('format')) return true;
    // Block Enter (insertParagraph) when the slash command menu is open — the
    // capture-phase keydown handler closes the menu and applies the snippet, so
    // CM's default newline insertion must not run concurrently.
    if (event.inputType === 'insertParagraph' && slashState.open) return true;
    return false;
  },
  onPaste(event) {
    return handleEditorPaste(event);
  },
  onDrop(event) {
    return handleEditorDrop(event);
  },
  onKeydown(event) {
    if (slashState.open) {
      if (event.key === 'ArrowDown') {
        event.preventDefault();
        slashState.active = (slashState.active + 1) % slashState.items.length;
        renderSlashMenu();
        return true;
      }
      if (event.key === 'ArrowUp') {
        event.preventDefault();
        slashState.active = (slashState.active - 1 + slashState.items.length) % slashState.items.length;
        renderSlashMenu();
        return true;
      }
      if (event.key === 'Enter' || event.key === 'Tab') {
        event.preventDefault();
        applySlashCommand(slashState.items[slashState.active]);
        return true;
      }
      if (event.key === 'Escape') {
        event.preventDefault();
        closeSlashMenu();
        return true;
      }
    }
    if ((event.metaKey || event.ctrlKey) && !event.altKey) {
      const key = event.key.toLowerCase();
      if (key === 'v') {
        event.preventDefault();
        event.stopPropagation();
        state.skipNextPasteEvent = true;
        void pasteFromTeams();
        return true;
      }
      const action = key === 'b' ? 'bold' : key === 'i' ? 'italic' : key === 'k' ? 'link' : null;
      if (action) {
        event.preventDefault();
        runToolbarAction(action);
        return true;
      }
    }
    return false;
  }
});
editor = markdownEditor.inputDom;
// Capture-phase keydown listener on the editor's contentDOM.
// Must use capture (true) + stopPropagation so this fires before CodeMirror's
// own keymap handlers (which run in the bubbling phase on view.dom). Without
// this, CM's defaultKeymap processes Enter and inserts a newline before our
// slash-command handler can intercept it, and onChange fires closing the slash
// menu so our domEventHandlers.keydown handler never sees it open.
markdownEditor.inputDom.addEventListener('keydown', event => {
  if ((event.metaKey || event.ctrlKey) && !event.altKey) {
    const key = event.key.toLowerCase();
    if (key === 'v') {
      event.preventDefault();
      event.stopPropagation();
      state.skipNextPasteEvent = true;
      void pasteFromTeams();
      return;
    }
    const action = key === 'b' ? 'bold' : key === 'i' ? 'italic' : key === 'k' ? 'link' : null;
    if (action) {
      event.preventDefault();
      event.stopPropagation();
      runToolbarAction(action);
      return;
    }
  }
  if (!slashState.open) return;
  if (event.key === 'ArrowDown') {
    event.preventDefault();
    event.stopPropagation();
    slashState.active = (slashState.active + 1) % slashState.items.length;
    renderSlashMenu();
  } else if (event.key === 'ArrowUp') {
    event.preventDefault();
    event.stopPropagation();
    slashState.active = (slashState.active - 1 + slashState.items.length) % slashState.items.length;
    renderSlashMenu();
  } else if (event.key === 'Enter' || event.key === 'Tab') {
    event.preventDefault();
    event.stopPropagation();
    applySlashCommand(slashState.items[slashState.active]);
  } else if (event.key === 'Escape') {
    event.preventDefault();
    event.stopPropagation();
    closeSlashMenu();
  }
}, true);
Object.assign(globalThis, { __markupForgeEditor: markdownEditor });
state.filename = normalizeFilename(localStorage.getItem(FILE_KEY) || state.filename);
syncFilenameInput();
setSplitPercent(50, { persist: false });
setViewMode('split');
syncOutputProfileOptions();
scheduleUpdate();
