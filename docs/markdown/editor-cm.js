import { EditorState, EditorSelection } from '@codemirror/state';
import { EditorView, drawSelection, highlightActiveLine, highlightActiveLineGutter, keymap, lineNumbers } from '@codemirror/view';
import { defaultKeymap, history, historyKeymap, indentWithTab } from '@codemirror/commands';
import { HighlightStyle, indentOnInput, syntaxHighlighting } from '@codemirror/language';
import { markdown } from '@codemirror/lang-markdown';
import { autocompletion, closeBrackets, closeBracketsKeymap, completionKeymap } from '@codemirror/autocomplete';
import { searchKeymap } from '@codemirror/search';
import { tags } from '@lezer/highlight';

function clampPos(view, pos) {
  return Math.max(0, Math.min(pos, view.state.doc.length));
}

function selectionText(view) {
  const { from, to } = view.state.selection.main;
  return view.state.sliceDoc(from, to);
}

function lineRangeForSelection(view) {
  const { from, to } = view.state.selection.main;
  const doc = view.state.doc;
  const startLine = doc.lineAt(from);
  const endAnchor = Math.max(from, to - (to > from ? 1 : 0));
  const endLine = doc.lineAt(endAnchor);
  return { startLine, endLine };
}

function setSelection(view, from, to = from) {
  view.dispatch({ selection: EditorSelection.range(clampPos(view, from), clampPos(view, to)), scrollIntoView: true });
}

function replaceRange(view, from, to, insert, selection) {
  view.dispatch({
    changes: { from, to, insert },
    selection,
    scrollIntoView: true,
    userEvent: 'input'
  });
}

export function createMarkdownEditor(root, { initialValue = '', onChange, onScroll, onFocus, onBlur, onKeydown, onPaste, onDrop, onBeforeInput } = {}) {
  const parent = root.parentElement;
  const host = document.createElement('div');
  host.className = 'cm-editor-host';
  host.style.flex = '1 1 auto';
  host.style.height = '100%';
  host.style.minHeight = '0';
  host.style.display = 'flex';
  host.style.overflow = 'hidden';
  host.style.position = 'relative';
  if (parent) parent.replaceChild(host, root);

  const updateListener = EditorView.updateListener.of(update => {
    if (update.docChanged && onChange) onChange(update.state.doc.toString(), update);
    if (update.focusChanged) {
      if (update.view.hasFocus && onFocus) onFocus(update);
      if (!update.view.hasFocus && onBlur) onBlur(update);
    }
    if (update.viewportChanged && onScroll) onScroll(update);
  });

  const domHandlers = EditorView.domEventHandlers({
    scroll: (_event, view) => {
      if (onScroll) onScroll({ view, viewportChanged: true, selectionSet: false, docChanged: false });
    },
    keydown: (event, view) => {
      if (!onKeydown) return false;
      return !!onKeydown(event, view);
    }
  });

  const theme = EditorView.theme({
    '&': {
      position: 'absolute',
      inset: '0',
      width: '100%',
      height: '100%',
      minHeight: '0',
      color: '#f0f6fc',
      backgroundColor: '#0d1117',
      fontFamily: 'var(--font-mono)',
      fontSize: '14px',
      display: 'flex',
      flexDirection: 'column',
      overflow: 'hidden'
    },
    '.cm-scroller': {
      flex: '1 1 auto',
      minHeight: '0',
      overflow: 'auto',
      fontFamily: 'var(--font-mono)',
      lineHeight: '1.65'
    },
    '.cm-content, .cm-gutter': {
      minHeight: '100%',
      paddingTop: '20px',
      paddingBottom: '20px'
    },
    '.cm-content': {
      caretColor: '#58a6ff',
      paddingLeft: '22px',
      paddingRight: '22px'
    },
    '.cm-gutters': {
      minHeight: '100%',
      backgroundColor: '#10151d',
      color: '#a7b0c0',
      borderRight: '1px solid var(--line)'
    },
    '.cm-lineNumbers .cm-gutterElement': {
      paddingLeft: '14px',
      paddingRight: '10px'
    },
    '.cm-line': {
      color: '#e5edf7'
    },
    '.cm-activeLine': {
      backgroundColor: 'rgba(255,255,255,.02)'
    },
    '.cm-activeLineGutter': {
      backgroundColor: 'rgba(255,255,255,.03)'
    },
    '.cm-selectionBackground, &.cm-focused .cm-selectionBackground, & ::selection': {
      backgroundColor: 'rgba(88, 166, 255, .28)'
    },
    '&.cm-focused': {
      outline: 'none'
    },
    '.cm-cursor, &.cm-focused .cm-cursor': {
      borderLeftColor: '#58a6ff'
    },
    '.cm-panels': {
      backgroundColor: '#161b22',
      color: '#f0f6fc'
    }
  });

  const markdownHighlightStyle = HighlightStyle.define([
    { tag: tags.heading, color: '#7dd3fc', fontWeight: '700' },
    { tag: tags.strong, color: '#ffd166', fontWeight: '700' },
    { tag: tags.emphasis, color: '#f8fafc', fontStyle: 'italic', textDecoration: 'underline' },
    { tag: [tags.monospace, tags.literal], color: '#c4b5fd' },
    { tag: [tags.url, tags.link], color: '#60a5fa', textDecoration: 'underline' },
    { tag: tags.quote, color: '#94d2bd' },
    { tag: [tags.list, tags.separator], color: '#ffb703', fontWeight: '700' },
    { tag: [tags.comment, tags.meta], color: '#94a3b8' },
    { tag: tags.processingInstruction, color: '#f59e0b' },
    { tag: tags.string, color: '#fca5a5' },
    { tag: tags.number, color: '#67e8f9' },
    { tag: tags.keyword, color: '#c084fc' }
  ]);

  const state = EditorState.create({
    doc: initialValue,
    extensions: [
      lineNumbers(),
      highlightActiveLineGutter(),
      history(),
      drawSelection(),
      highlightActiveLine(),
      indentOnInput(),
      closeBrackets(),
      autocompletion({ activateOnTyping: false }),
      markdown(),
      syntaxHighlighting(markdownHighlightStyle, { fallback: true }),
      keymap.of([
        indentWithTab,
        ...closeBracketsKeymap,
        ...defaultKeymap,
        ...historyKeymap,
        ...completionKeymap,
        ...searchKeymap
      ]),
      theme,
      updateListener,
      domHandlers
    ]
  });

  const view = new EditorView({ state, parent: host });
  view.scrollDOM.classList.add('gh-editor-scroll');
  view.contentDOM.id = 'editor';
  view.contentDOM.setAttribute('aria-label', 'Markdown source editor');
  if (onBeforeInput) {
    view.contentDOM.addEventListener('beforeinput', event => {
      if (!onBeforeInput(event, view)) return;
      event.preventDefault();
      event.stopImmediatePropagation();
    }, true);
  }
  if (onPaste) {
    view.contentDOM.addEventListener('paste', event => {
      if (!onPaste(event, view)) return;
      event.preventDefault();
      event.stopImmediatePropagation();
    }, true);
  }
  // Intercept file drops on the editor surface so they are handled by the app's
  // import pipeline (loadFile → importFileToMarkdown) rather than CodeMirror's
  // built-in file-drop handler, which reads the raw text and inserts it inline.
  // The capture phase ensures we run before CM's own domEventHandlers.drop.
  if (onDrop) {
    view.contentDOM.addEventListener('drop', event => {
      if (!onDrop(event, view)) return;
      event.preventDefault();
      event.stopImmediatePropagation();
    }, true);
  }

  Object.defineProperties(view.contentDOM, {
    value: {
      get() { return view.state.doc.toString(); },
      set(value) {
        const insert = String(value ?? '');
        view.dispatch({ changes: { from: 0, to: view.state.doc.length, insert }, selection: { anchor: insert.length }, userEvent: 'input' });
      }
    },
    selectionStart: {
      get() { return view.state.selection.main.from; },
      set(value) { setSelection(view, value, view.state.selection.main.to); }
    },
    selectionEnd: {
      get() { return view.state.selection.main.to; },
      set(value) { setSelection(view, view.state.selection.main.from, value); }
    },
    scrollTop: {
      get() { return view.scrollDOM.scrollTop; },
      set(value) { view.scrollDOM.scrollTop = value; }
    },
    scrollHeight: {
      get() { return view.scrollDOM.scrollHeight; }
    },
    clientHeight: {
      get() { return view.scrollDOM.clientHeight; }
    }
  });
  view.contentDOM.setSelectionRange = (start, end = start) => setSelection(view, start, end);

  return {
    view,
    dom: view.dom,
    inputDom: view.contentDOM,
    scrollDOM: view.scrollDOM,
    focus() {
      view.focus();
    },
    getValue() {
      return view.state.doc.toString();
    },
    setValue(value) {
      const current = view.state.doc.toString();
      if (current === value) return;
      view.dispatch({
        changes: { from: 0, to: view.state.doc.length, insert: value },
        selection: { anchor: Math.min(view.state.selection.main.head, value.length) },
        userEvent: 'input'
      });
    },
    setSelection(from, to = from) {
      setSelection(view, from, to);
    },
    getSelection() {
      const { from, to } = view.state.selection.main;
      return { from, to, text: view.state.sliceDoc(from, to) };
    },
    getCursorOffset() {
      return view.state.selection.main.head;
    },
    getCursorLine() {
      return view.state.doc.lineAt(view.state.selection.main.head).number - 1;
    },
    getVisibleLineRange() {
      const top = view.lineBlockAtHeight(view.scrollDOM.scrollTop);
      const bottom = view.lineBlockAtHeight(view.scrollDOM.scrollTop + view.scrollDOM.clientHeight);
      return {
        from: view.state.doc.lineAt(top.from).number - 1,
        to: view.state.doc.lineAt(bottom.to).number - 1
      };
    },
    getScrollRatio() {
      const max = view.scrollDOM.scrollHeight - view.scrollDOM.clientHeight;
      return max > 0 ? view.scrollDOM.scrollTop / max : 0;
    },
    getScrollTop() {
      return view.scrollDOM.scrollTop;
    },
    scrollToRatio(ratio) {
      const max = Math.max(0, view.scrollDOM.scrollHeight - view.scrollDOM.clientHeight);
      view.scrollDOM.scrollTop = Math.max(0, Math.min(max, ratio * max));
    },
    scrollToLine(lineIndex) {
      const line = view.state.doc.line(Math.max(1, Math.min(view.state.doc.lines, lineIndex + 1)));
      view.dispatch({ effects: EditorView.scrollIntoView(line.from, { y: 'center' }) });
    },
    replaceSelection(insert, { from, to, selection } = {}) {
      const main = view.state.selection.main;
      replaceRange(view, from ?? main.from, to ?? main.to, insert, selection ?? { anchor: (from ?? main.from) + insert.length });
    },
    replaceRange(from, to, insert, selection) {
      replaceRange(view, from, to, insert, selection);
    },
    replaceDocument(value) {
      // Full-document replacements must keep the selection inside the *old*
      // document range; otherwise CodeMirror can throw when the replacement is
      // larger than the current content (for example, debugpaste dumps).
      const anchor = Math.min(view.state.selection.main.head, value.length);
      replaceRange(view, 0, view.state.doc.length, value, { anchor });
    },
    replaceLines(prefixFactory) {
      const { startLine, endLine } = lineRangeForSelection(view);
      const lines = [];
      for (let index = startLine.number; index <= endLine.number; index++) {
        lines.push(view.state.doc.line(index).text);
      }
      const next = lines.map((line, index) => `${prefixFactory(index)}${line || ''}`).join('\n');
      replaceRange(view, startLine.from, endLine.to, next, { anchor: startLine.from, head: startLine.from + next.length });
    },
    wrapSelection(before, after = '', placeholder = '') {
      const { from, to } = view.state.selection.main;
      const selected = selectionText(view) || placeholder;
      const insert = `${before}${selected}${after}`;
      replaceRange(view, from, to, insert, {
        anchor: from + before.length,
        head: from + before.length + selected.length
      });
    },
    insertLink() {
      const { from, to } = view.state.selection.main;
      const selected = selectionText(view) || 'link text';
      const insert = `[${selected}](https://example.com)`;
      replaceRange(view, from, to, insert, {
        anchor: from + 1,
        head: from + 1 + selected.length
      });
    },
    dispatch(spec) {
      view.dispatch(spec);
    },
    hasFocus() {
      return view.hasFocus;
    },
    coordsAtPos(pos) {
      return view.coordsAtPos(clampPos(view, pos));
    },
    lineStartAt(pos) {
      return view.state.doc.lineAt(clampPos(view, pos)).from;
    },
    textBetween(from, to) {
      return view.state.sliceDoc(from, to);
    }
  };
}
