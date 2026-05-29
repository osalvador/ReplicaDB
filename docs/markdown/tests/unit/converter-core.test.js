import { describe, it, expect } from 'vitest';
import {
  htmlEscape,
  renderInline,
  renderMarkdown,
  convertMarkdownToJira,
  renderMarkdownForTeams,
  getHtmlDocument,
  getHtmlOutput,
  compactMarkdown,
  markdownForOutput,
  buildClipboardHtmlFragment,
  htmlToPlainText,
  buildTeamsClipboard,
  convertMarkdownToTeamsHtml,
  inlineTeamsHtml
} from '../../converter-core.js';

describe('htmlEscape', () => {
  it('escapes HTML-significant characters', () => {
    expect(htmlEscape('<a href="x">&\'</a>'))
      .toBe('&lt;a href=&quot;x&quot;&gt;&amp;&#39;&lt;/a&gt;');
  });
});

describe('renderInline', () => {
  it('renders bold, italic and strikethrough', () => {
    expect(renderInline('**b** _i_ ~~s~~'))
      .toBe('<strong>b</strong> <em>i</em> <del>s</del>');
  });

  it('renders links with safe attributes', () => {
    expect(renderInline('[t](https://e.com)'))
      .toContain('<a href="https://e.com" target="_blank" rel="noopener noreferrer">t</a>');
  });

  it('escapes content inside inline code', () => {
    expect(renderInline('`<x>`')).toBe('<code>&amp;lt;x&amp;gt;</code>');
  });
});

describe('renderMarkdown', () => {
  it('renders headings', () => {
    expect(renderMarkdown('# Title')).toBe('<h1>Title</h1>');
  });

  it('renders unordered lists', () => {
    expect(renderMarkdown('- a\n- b')).toBe('<ul><li>a</li><li>b</li></ul>');
  });

  it('renders ordered lists', () => {
    expect(renderMarkdown('1. a\n2. b')).toBe('<ol><li>a</li><li>b</li></ol>');
  });

  it('renders blockquotes', () => {
    expect(renderMarkdown('> quoted')).toBe('<blockquote>quoted</blockquote>');
  });

  it('renders fenced code blocks with language class', () => {
    const html = renderMarkdown('```js\nconst x = 1;\n```');
    expect(html).toContain('<pre><code class="language-js">');
    expect(html).toContain('hljs-number');
  });

  it('renders tables', () => {
    const html = renderMarkdown('| a | b |\n| --- | --- |\n| 1 | 2 |');
    expect(html).toContain('<table>');
    expect(html).toContain('<th>a</th>');
    expect(html).toContain('<td>1</td>');
  });

  it('collapses soft-wrapped paragraph lines', () => {
    expect(renderMarkdown('one\ntwo')).toBe('<p>one two</p>');
  });

  it('shows a placeholder for empty input', () => {
    expect(renderMarkdown('')).toContain('Start writing Markdown');
  });
});

describe('convertMarkdownToJira', () => {
  it('converts headings to Jira notation', () => {
    expect(convertMarkdownToJira('## Hello')).toBe('h2. Hello');
  });

  it('converts bold and inline code', () => {
    expect(convertMarkdownToJira('**b** `c`')).toBe('*b* {{c}}');
  });

  it('converts fenced code blocks to {code}', () => {
    const out = convertMarkdownToJira('```js\nx\n```');
    expect(out).toContain('{code:language=js}');
    expect(out).toContain('{code}');
  });

  it('converts lists', () => {
    expect(convertMarkdownToJira('- a')).toBe('* a');
    expect(convertMarkdownToJira('1. a')).toBe('# a');
  });
});

describe('renderMarkdownForTeams', () => {
  it('renders the sent-message view (bubble), not a composer', () => {
    const html = renderMarkdownForTeams('# Update');
    expect(html).toContain('teams-sent-message');
    expect(html).toContain('teams-content');
    expect(html).toContain('teams-sent-meta');
    // No composer chrome in the sent-message preview.
    expect(html).not.toContain('teams-format-bar');
    expect(html).not.toContain('teams-compose-footer');
    // Preview uses the same Teams-friendly markup that gets pasted (no <h1>).
    expect(html).toContain('<span style="font-size:x-large;">Update</span>');
    expect(html).not.toContain('<h1>');
  });

  it('renders fenced code blocks as Teams code cards', () => {
    const html = renderMarkdownForTeams('```js\nconst x = 1;\n```');
    expect(html).toContain('teams-code-card');
    expect(html).toContain('teams-code-line-number');
  });
});

describe('getHtmlDocument / getHtmlOutput', () => {
  it('builds a full HTML document with the title', () => {
    const doc = getHtmlDocument('# Hi', 'notes.md');
    expect(doc).toContain('<!doctype html>');
    expect(doc).toContain('<title>notes</title>');
    expect(doc).toContain('<h1>Hi</h1>');
  });

  it('returns only the fragment for the html-fragment profile', () => {
    expect(getHtmlOutput('# Hi', 'html-fragment', 'notes.md')).toBe('<h1>Hi</h1>');
  });

  it('returns the full document for the html-document profile', () => {
    expect(getHtmlOutput('# Hi', 'html-document', 'notes.md')).toContain('<!doctype html>');
  });
});

describe('compactMarkdown / markdownForOutput', () => {
  it('collapses soft-wrapped lines within a paragraph', () => {
    expect(compactMarkdown('line one\nline two')).toBe('line one line two');
  });

  it('keeps paragraph separation on blank lines', () => {
    expect(compactMarkdown('a\nb\n\nc\nd')).toBe('a b\n\nc d');
  });

  it('preserves lists, headings and quotes', () => {
    const input = '# Title\n- one\n- two\n> quote';
    expect(compactMarkdown(input)).toBe(input);
  });

  it('preserves fenced code blocks verbatim', () => {
    const input = '```js\nconst a = 1;\nconst b = 2;\n```';
    expect(compactMarkdown(input)).toBe(input);
  });

  it('markdownForOutput compacts only for teams-compact profile', () => {
    expect(markdownForOutput('a\nb', 'teams-compact')).toBe('a b');
    expect(markdownForOutput('a\nb', 'teams-rich')).toBe('a\nb');
    expect(markdownForOutput('a\nb', 'jira-wiki')).toBe('a\nb');
  });
});

describe('Teams clipboard payload', () => {
  it('wraps HTML in a clipboard fragment with StartFragment markers', () => {
    const fragment = buildClipboardHtmlFragment('<p>hi</p>');
    expect(fragment).toContain('<!--StartFragment-->');
    expect(fragment).toContain('<!--EndFragment-->');
    expect(fragment).toContain('<p>hi</p>');
    expect(fragment.startsWith('<!doctype html>')).toBe(true);
  });

  it('derives readable plain text from rendered HTML', () => {
    expect(htmlToPlainText('<h1>Title</h1><p>Hello <strong>world</strong></p>'))
      .toBe('Title\nHello world');
  });

  it('converts list items to bullet text', () => {
    expect(htmlToPlainText('<ul><li>one</li><li>two</li></ul>'))
      .toBe('\u2022 one\n\u2022 two');
  });

  it('buildTeamsClipboard returns rich HTML fragment and plain-text fallback', () => {
    const payload = buildTeamsClipboard('# Update\n\nThis is **bold**.');
    // The HTML flavour must carry Teams-friendly formatting, not raw Markdown.
    expect(payload.html).toContain('<!--StartFragment-->');
    expect(payload.html).toContain('<span style="font-size:x-large;">Update</span>');
    expect(payload.html).toContain('<strong>bold</strong>');
    expect(payload.html).not.toContain('# Update');
    expect(payload.html).not.toContain('<h1>');
    // The unwrapped body is exposed for the contenteditable copy fallback.
    expect(payload.body).toContain('<span style="font-size:x-large;">Update</span>');
    expect(payload.body).not.toContain('<!doctype');
    // The plain-text flavour must be clean text, not Markdown syntax.
    expect(payload.text).toContain('Update');
    expect(payload.text).toContain('This is bold.');
    expect(payload.text).not.toContain('**');
    expect(payload.text).not.toContain('#');
  });
});

describe('convertMarkdownToTeamsHtml', () => {
  it('maps headings to styled paragraphs Teams accepts', () => {
    expect(convertMarkdownToTeamsHtml('# Title'))
      .toBe('<p><span style="font-size:x-large;">Title</span></p>');
    expect(convertMarkdownToTeamsHtml('## Subtitle'))
      .toBe('<p><strong>Subtitle</strong></p>');
  });

  it('preserves the author blank line between heading and table (or omits it)', () => {
    // No blank line in the source -> no spacer paragraph.
    const tight = convertMarkdownToTeamsHtml('## Table\n| A | B |\n| --- | --- |\n| 1 | 2 |');
    expect(tight.startsWith('<p><strong>Table</strong></p><figure class="table">')).toBe(true);
    expect(tight).not.toContain('<p>&nbsp;</p>');

    // A blank line in the source -> keep the spacer paragraph.
    const spaced = convertMarkdownToTeamsHtml('## Table\n\n| A | B |\n| --- | --- |\n| 1 | 2 |');
    expect(spaced.startsWith('<p><strong>Table</strong></p><p>&nbsp;</p><figure class="table">')).toBe(true);
  });

  it('uses <i> for emphasis and <strong> for bold', () => {
    expect(inlineTeamsHtml('*em* **bold**')).toBe('<i>em</i> <strong>bold</strong>');
  });

  it('renders links as anchors', () => {
    expect(inlineTeamsHtml('[MS](https://www.microsoft.com)'))
      .toBe('<a href="https://www.microsoft.com">MS</a>');
  });

  it('wraps quotes in a paragraph inside blockquote', () => {
    expect(convertMarkdownToTeamsHtml('> hi'))
      .toBe('<blockquote spellcheck="false"><p>hi</p></blockquote>');
  });

  it('renders unordered and ordered lists', () => {
    expect(convertMarkdownToTeamsHtml('- a\n- b')).toBe('<ul><li>a</li><li>b</li></ul>');
    expect(convertMarkdownToTeamsHtml('1. a\n2. b')).toBe('<ol><li>a</li><li>b</li></ol>');
  });

  it('renders tables as a figure table', () => {
    const html = convertMarkdownToTeamsHtml('| Area | Status |\n| --- | --- |\n| IDE | Ready |');
    expect(html).toContain('<figure class="table"><table><tbody>');
    expect(html).toContain('data-is-tablecell-container="true"');
    expect(html).toContain('Area');
    expect(html).toContain('Ready');
  });

  it('renders fenced code blocks with the Skype CodeBlockEditor marker', () => {
    const html = convertMarkdownToTeamsHtml('```json\n{\n  "a": 1\n}\n```');
    expect(html).toContain('itemtype="http://schema.skype.com/CodeBlockEditor"');
    expect(html).toContain('<pre class="language-json skipProofing"');
    expect(html).toContain('<br>');
  });

  it('escapes inline code content once', () => {
    expect(inlineTeamsHtml('`<x>`')).toBe('<code>&lt;x&gt;</code>');
  });
});
