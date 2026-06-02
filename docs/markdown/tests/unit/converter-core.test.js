import { describe, it, expect } from 'vitest';
import { JSDOM } from 'jsdom';
import {
  htmlEscape,
  renderInline,
  renderMarkdown,
  convertMarkdownToJira,
  renderMarkdownForTeams,
  getHtmlDocument,
  slugify,
  renderNavigableHtml,
  buildClipboardHtmlFragment,
  htmlToPlainText,
  buildTeamsClipboard,
  convertMarkdownToTeamsHtml,
  inlineTeamsHtml,
  convertHtmlToMarkdown,
  tsvToMarkdownTable,
  guessCodeLanguage,
  looksLikeCode,
  detectAndWrapCode,
  cleanPlainTextToMarkdown,
  smartPasteToMarkdown,
  looksLikeCsv,
  crc32,
  buildZip,
  buildSourceArtifactBundle,
  highlightMarkdownSource,
  SLASH_COMMANDS,
  filterSlashCommands,
  parseCsv,
  csvToMarkdownTable,
  importFileToMarkdown,
  parseFrontmatter,
  stripFrontmatter,
  highlightCode,
  highlightCodeForPreview,
  looksLikeSpaceTable,
  spaceTableToMarkdown,
  jiraToHtml
} from '../../converter-core.js';

// jsdom's DOMParser so convertHtmlToMarkdown can run under Vitest's node env.
const DomParser = new JSDOM('').window.DOMParser;

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

  it('converts Markdown images to Jira image syntax !url|alt=alt!', () => {
    // Regression: images must not be discarded as plain text; they must use
    // Jira image syntax so that jiraToHtml can render them as <img> in the
    // Visual preview.
    expect(convertMarkdownToJira('![photo](data:image/png;base64,AAAB)'))
      .toBe('!data:image/png;base64,AAAB|alt=photo!');
    // Image with no alt text
    expect(convertMarkdownToJira('![](https://example.com/img.png)'))
      .toBe('!https://example.com/img.png!');
  });

  it('converts a GFM table to Jira table syntax (|| headers ||, | cells |)', () => {
    const md = '| Tier | Score |\n| --- | --- |\n| T0 | < 20 |\n| T1 | 20-39 |';
    const jira = convertMarkdownToJira(md);
    // Header row uses || delimiters
    expect(jira).toContain('|| Tier || Score ||');
    // Data rows use | delimiters
    expect(jira).toContain('| T0 | < 20 |');
    expect(jira).toContain('| T1 | 20-39 |');
    // Separator row must NOT appear in output
    expect(jira).not.toContain('---');
  });

  it('converts a GFM table with bold headers to Jira syntax', () => {
    const md = '| **Name** | **Value** |\n| --- | --- |\n| foo | bar |';
    const jira = convertMarkdownToJira(md);
    // Bold in Jira = *text*
    expect(jira).toContain('*Name*');
    expect(jira).toContain('*Value*');
    expect(jira).toContain('| foo | bar |');
  });
});

describe('jiraToHtml', () => {
  it('renders a heading', () => {
    expect(jiraToHtml('h2. Hello')).toBe('<h2>Hello</h2>');
  });

  it('renders bold and italic inline', () => {
    const html = jiraToHtml('*bold* and _italic_');
    expect(html).toContain('<strong>bold</strong>');
    expect(html).toContain('<em>italic</em>');
  });

  it('renders monospace {{...}}', () => {
    expect(jiraToHtml('see {{foo()}}')).toContain('<code>foo()</code>');
  });

  it('renders a Jira link [label|url]', () => {
    expect(jiraToHtml('[Jira|https://jira.example.com]')).toContain('<a href="https://jira.example.com">Jira</a>');
  });

  it('preserves single newlines within a paragraph as <br> (regression: Teams reference list)', () => {
    // Regression: reference codes pasted from Teams arrive as soft-break lines
    // within the same paragraph block. The visual Jira preview must render them
    // on separate lines, not collapsed into one line separated by spaces.
    const jira = '05802014400-V2026,\n 03074119080-V2026,\n 04391501644-I2026,\n 08784860250-V2026';
    const html = jiraToHtml(jira);
    expect(html).toContain('<br>');
    expect(html).not.toMatch(/V2026,\s+0\d/); // must NOT be collapsed with space
  });

  it('renders blank-line-separated paragraphs as distinct <p> elements', () => {
    const html = jiraToHtml('First paragraph\n\nSecond paragraph');
    expect(html).toContain('<p>First paragraph</p>');
    expect(html).toContain('<p>Second paragraph</p>');
    // Two separate paragraphs — not a single one
    const matches = html.match(/<p>/g);
    expect(matches).toHaveLength(2);
  });

  it('renders a {code} block as <pre><code>', () => {
    const html = jiraToHtml('{code:language=js}\nconsole.log("hi");\n{code}');
    expect(html).toContain('<pre>');
    expect(html).toContain('<code');
    expect(html).toContain('console.log');
  });

  it('renders {code:xml} shorthand (no language= prefix) with language class', () => {
    const html = jiraToHtml('{code:xml}\n<root/>\n{code}');
    expect(html).toContain('class="language-xml"');
    expect(html).toContain('&lt;root/&gt;');
  });

  it('renders {noformat} block as <pre><code> without language', () => {
    const html = jiraToHtml('{noformat}\nplain preformatted text\n{noformat}');
    expect(html).toContain('<pre>');
    expect(html).toContain('plain preformatted text');
    expect(html).not.toContain('class="language-');
  });

  it('renders bq. as a blockquote', () => {
    const html = jiraToHtml('bq. Important note here');
    expect(html).toContain('<blockquote>');
    expect(html).toContain('Important note here');
  });

  it('renders unordered list (* item)', () => {
    const html = jiraToHtml('* Alpha\n* Beta');
    expect(html).toContain('<ul>');
    expect(html).toContain('<li>Alpha</li>');
    expect(html).toContain('<li>Beta</li>');
  });

  it('renders ordered list (# item)', () => {
    const html = jiraToHtml('# One\n# Two');
    expect(html).toContain('<ol>');
    expect(html).toContain('<li>One</li>');
    expect(html).toContain('<li>Two</li>');
  });

  it('renders \\\\ as explicit <br>', () => {
    expect(jiraToHtml('line one\\\\line two')).toContain('<br>');
  });

  it('does not convert hyphenated identifiers to strikethrough', () => {
    // REF-2026-style codes must not be wrapped in <del>
    const html = jiraToHtml('Ref: 05802014400-V2026, 03074119080-V2026');
    expect(html).not.toContain('<del>');
    expect(html).toContain('05802014400-V2026');
  });

  it('escapes HTML special chars in code blocks', () => {
    const html = jiraToHtml('{code}\n<script>alert(1)</script>\n{code}');
    expect(html).toContain('&lt;script&gt;');
    expect(html).not.toContain('<script>');
  });

  it('renders Jira image syntax !url|alt=alt! as <img> (regression: Jira visual images)', () => {
    // Images pasted from Teams survive the pipeline:
    //   Markdown ![photo](data:...) → inlineToJira → !data:...|alt=photo!
    //   → jiraToHtml → <img src="data:..." alt="photo">
    const html = jiraToHtml('!data:image/png;base64,AAAB|alt=photo!');
    expect(html).toContain('<img');
    expect(html).toContain('src="data:image/png;base64,AAAB"');
    expect(html).toContain('alt="photo"');
  });

  it('renders Jira image syntax without params !url! as <img>', () => {
    const html = jiraToHtml('!https://example.com/img.png!');
    expect(html).toContain('<img');
    expect(html).toContain('src="https://example.com/img.png"');
  });

  it('renders a Jira table (|| header ||) as <table><tr><th>', () => {
    const jira = '|| Tier || Score ||\n| T0 | < 20 |\n| T1 | 20-39 |';
    const html = jiraToHtml(jira);
    expect(html).toContain('<table');
    expect(html).toContain('<th>Tier</th>');
    expect(html).toContain('<th>Score</th>');
    expect(html).toContain('<td>T0</td>');
    expect(html).toContain('<td>T1</td>');
    // < in a cell must be escaped
    expect(html).toContain('&lt; 20');
    expect(html).not.toContain('<20');
  });

  it('table does not bleed into the following paragraph', () => {
    const jira = '|| A || B ||\n| 1 | 2 |\n\nParagraph after';
    const html = jiraToHtml(jira);
    expect(html).toContain('<table');
    expect(html).toContain('<p>Paragraph after</p>');
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

  it('two consecutive fenceless code blocks each render their own content', () => {
    const md = '```\nfirst block content\n```\n\n```\nsecond block content\n```';
    const html = renderMarkdownForTeams(md);
    // Both blocks must appear as separate code cards with their own content
    expect(html).toContain('first block content');
    expect(html).toContain('second block content');
    // Must have two code cards, not one swallowing both
    const cardCount = (html.match(/teams-code-card/g) || []).length;
    expect(cardCount).toBeGreaterThanOrEqual(2);
  });

  it('first line of a fenceless code block is NOT used as the language label', () => {
    const md = '```\nArchive → extract → enrich\n```';
    const html = renderMarkdownForTeams(md);
    // The content line should appear in the code body, not as a card header label
    expect(html).toContain('Archive → extract → enrich');
    // Card header should say "Text" (the default), not the first content line
    const headerMatch = html.match(/teams-code-header[^>]*>([\s\S]*?)<\/div>/);
    if (headerMatch) {
      expect(headerMatch[1]).not.toContain('Archive');
    }
  });

  it('fenced code blocks with a language tag still work correctly after the fix', () => {
    const md = '```yaml\nkey: value\n```\n\n```js\nconst x = 1;\n```';
    const html = renderMarkdownForTeams(md);
    // Content is syntax-highlighted so check for key tokens rather than verbatim strings
    expect(html).toContain('key');
    expect(html).toContain('value');
    expect(html).toContain('const');
    const cardCount = (html.match(/teams-code-card/g) || []).length;
    expect(cardCount).toBeGreaterThanOrEqual(2);
  });
});

describe('getHtmlDocument', () => {
  it('builds a full HTML document with the title', () => {
    const doc = getHtmlDocument('# Hi', 'notes.md');
    expect(doc).toContain('<!doctype html>');
    expect(doc).toContain('<title>notes</title>');
    expect(doc).toContain('<h1>Hi</h1>');
  });
});


describe('slugify', () => {
  it('creates url-safe slugs from heading text', () => {
    expect(slugify('Hello World!')).toBe('hello-world');
    expect(slugify('  Spaced  &  Symbols % ')).toBe('spaced-symbols');
  });

  it('de-duplicates slugs using a shared used set', () => {
    const used = new Set();
    expect(slugify('Title', used)).toBe('title');
    expect(slugify('Title', used)).toBe('title-1');
    expect(slugify('Title', used)).toBe('title-2');
  });

  it('falls back to "section" for empty input', () => {
    expect(slugify('   ')).toBe('section');
  });
});

describe('renderNavigableHtml', () => {
  it('produces a self-contained document with title and embedded styles', () => {
    const doc = renderNavigableHtml('# Hi', { title: 'notes.md' });
    expect(doc).toContain('<!doctype html>');
    expect(doc).toContain('<title>notes</title>');
    expect(doc).toContain('<style>');
  });

  it('builds a table of contents linking to heading anchors', () => {
    const doc = renderNavigableHtml('## Alpha\n\ntext\n\n## Beta', {});
    expect(doc).toContain('class="toc"');
    expect(doc).toContain('href="#alpha"');
    expect(doc).toContain('id="alpha"');
    expect(doc).toContain('href="#beta"');
  });

  it('gives duplicate headings unique anchor ids', () => {
    const doc = renderNavigableHtml('## Setup\n\n## Setup', {});
    expect(doc).toContain('id="setup"');
    expect(doc).toContain('id="setup-1"');
  });

  it('renders GitHub-style callouts', () => {
    const doc = renderNavigableHtml('> [!WARNING]\n> Be careful', {});
    expect(doc).toContain('callout callout-warning');
    expect(doc).toContain('class="callout-title"');
    expect(doc).toContain('Be careful');
  });

  it('wraps tables for responsive scrolling', () => {
    const doc = renderNavigableHtml('| a | b |\n| --- | --- |\n| 1 | 2 |', {});
    expect(doc).toContain('table-wrap');
    expect(doc).toContain('<table>');
  });

  it('makes H2 sections collapsible without JavaScript', () => {
    const doc = renderNavigableHtml('## Section\n\nbody', {});
    expect(doc).toContain('<details');
    expect(doc).toContain('<summary>');
  });

  it('never embeds scripts or external resources in the chrome', () => {
    const doc = renderNavigableHtml('# Title\n\nSome **content** and a list:\n\n- one\n- two', {});
    expect(doc).not.toContain('<script');
    expect(doc).not.toContain('http://');
    expect(doc).not.toContain('https://');
  });

  it('collapses soft line breaks to spaces by default (standard Markdown semantics)', () => {
    const doc = renderNavigableHtml('line one\nline two', {});
    // Default: single newlines are soft breaks → space, no <br>
    expect(doc).toContain('line one line two');
    expect(doc).not.toContain('line one<br>');
  });

  it('preserves soft line breaks as <br> when softBreaks:true (regression: HTML preview reference lists)', () => {
    // When pasting Teams content, reference codes arrive as separate soft-break
    // lines. The HTML preview must render them on separate visual lines.
    const md = '05802014400-V2026,\n 03074119080-V2026,\n 04391501644-I2026';
    const doc = renderNavigableHtml(md, { softBreaks: true });
    expect(doc).toContain('<br>');
    expect(doc).not.toMatch(/V2026,\s+0\d/); // must NOT be collapsed with space
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

  it('renders images as <img> so the preview shows the picture', () => {
    expect(inlineTeamsHtml('![shot](data:image/png;base64,AAAB)'))
      .toBe('<img src="data:image/png;base64,AAAB" alt="shot" />');
  });

  it('wraps quotes in a paragraph inside blockquote', () => {
    expect(convertMarkdownToTeamsHtml('> hi'))
      .toBe('<blockquote spellcheck="false"><p>hi</p></blockquote>');
  });

  it('merges consecutive blockquote lines into a single blockquote', () => {
    // Regression: each "> line" used to emit its own <blockquote>, producing
    // N separate boxes with gaps. Consecutive lines must be collected and joined
    // with <br> inside one unified blockquote.
    expect(convertMarkdownToTeamsHtml('> line one\n> line two\n> line three'))
      .toBe('<blockquote spellcheck="false"><p>line one<br>line two<br>line three</p></blockquote>');
  });

  it('splits blockquotes separated by a blank line into distinct blocks', () => {
    const result = convertMarkdownToTeamsHtml('> first\n\n> second');
    // A blank line between quote blocks produces a <p>&nbsp;</p> spacer — that
    // is fine; the important thing is that each group gets its own blockquote.
    expect(result).toContain('<blockquote spellcheck="false"><p>first</p></blockquote>');
    expect(result).toContain('<blockquote spellcheck="false"><p>second</p></blockquote>');
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

  it('table cell with < value does not eat subsequent rows (stripHtml regression)', () => {
    // "< 20" must NOT be treated as an HTML tag that swallows everything up to
    // the next ">" character elsewhere in the document.
    const md = [
      '| Tier | Score |',
      '| --- | --- |',
      '| T0 | < 20 |',
      '| T1 | 20-39 |',
      '| T2 | ≥ 40 |',
    ].join('\n');
    const html = convertMarkdownToTeamsHtml(md);
    // All three data rows must be present
    expect(html).toContain('T0');
    expect(html).toContain('T1');
    expect(html).toContain('T2');
    // The literal < must be escaped, not stripped
    expect(html).toContain('&lt; 20');
  });

  it('content after a table with a < cell is not swallowed', () => {
    // Paragraph that follows the table must still appear in the output.
    const md = '| X | Y |\n| --- | --- |\n| a | < 5 |\n\nNext section heading';
    const html = convertMarkdownToTeamsHtml(md);
    expect(html).toContain('Next section heading');
    expect(html).toContain('&lt; 5');
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

describe('convertHtmlToMarkdown (paste from Teams)', () => {
  const toMd = html => convertHtmlToMarkdown(html, '', DomParser);

  it('falls back to plain text when there is no HTML', () => {
    expect(convertHtmlToMarkdown('', 'just text', DomParser)).toBe('just text');
  });

  it('accepts a DOMParser instance as well as a constructor', () => {
    // Regression: passing `new DOMParser()` must not throw "Parser is not a
    // constructor". parseHtmlBody should detect the instance and use it.
    const instance = new DomParser();
    expect(convertHtmlToMarkdown('<h2>Hi</h2>', '', instance)).toBe('## Hi');
  });

  it('converts bold, italic, strikethrough and inline code', () => {
    expect(toMd('<p><b>bold</b> <i>em</i> <s>gone</s> <code>x</code></p>'))
      .toBe('**bold** *em* ~~gone~~ `x`');
  });

  it('converts links and headings', () => {
    expect(toMd('<h2>Title</h2>')).toBe('## Title');
    expect(toMd('<p><a href="https://ms.com">MS</a></p>')).toBe('[MS](https://ms.com)');
  });

  it('keeps image-extension links (e.g. SharePoint uploads) as regular links — not images', () => {
    // Teams shares screenshots as SharePoint links. These require auth so they
    // cannot be embedded; keep them as [text](url) hyperlinks, not ![text](url).
    const sharepoint = 'https://grupoinditex-my.sharepoint.com/personal/user/Documents/Microsoft%20Teams%20Chat%20Files/Screenshot%202026-05-29%20at%2013.27.35.png';
    expect(toMd(`<a href="${sharepoint}">Screenshot 2026-05-29 at 13.27.35.png</a>`))
      .toBe(`[Screenshot 2026-05-29 at 13.27.35.png](${sharepoint})`);
  });

  it('keeps all remote links as regular Markdown links regardless of extension', () => {
    expect(toMd('<a href="https://cdn.example.com/photo.jpg">photo</a>'))
      .toBe('[photo](https://cdn.example.com/photo.jpg)');
    expect(toMd('<a href="https://example.com/page">click here</a>'))
      .toBe('[click here](https://example.com/page)');
    expect(toMd('<a href="https://example.com/doc.pdf">doc</a>'))
      .toBe('[doc](https://example.com/doc.pdf)');
  });

  it('converts unordered and ordered lists', () => {
    expect(toMd('<ul><li>one</li><li>two</li></ul>')).toBe('- one\n- two');
    expect(toMd('<ol><li>one</li><li>two</li></ol>')).toBe('1. one\n2. two');
  });

  it('converts tables to GitHub-flavoured Markdown', () => {
    const html = '<table><tr><td>Area</td><td>Status</td></tr><tr><td>IDE</td><td>Ready</td></tr></table>';
    expect(toMd(html)).toBe('| Area | Status |\n| --- | --- |\n| IDE | Ready |');
  });

  it('converts <br>-separated values inside table cells to HTML line-breaks', () => {
    // Regression: td.textContent was used, which concatenated values without any
    // separator — "value1value2". The fix recurses into the cell and maps <br>→<br>
    // (literal HTML in the GFM cell), preserving intentional line breaks.
    const html = '<table><tr><td>Environment</td><td>Values</td></tr>'
      + '<tr><td>PRE<br>PRO</td><td>alpha<br>beta<br>gamma</td></tr></table>';
    expect(toMd(html)).toBe(
      '| Environment | Values |\n| --- | --- |\n| PRE<br>PRO | alpha<br>beta<br>gamma |'
    );
  });

  it('handles <strong>/<em> inside table cells', () => {
    const html = '<table><tr><td><strong>Header</strong></td><td>Value</td></tr></table>';
    expect(toMd(html)).toBe('| **Header** | Value |\n| --- | --- |');
  });

  it('handles a Teams-style HTML table with multiline cells (paste regression)', () => {
    // Simulates HTML Teams pastes where a table has <br> for multiple values per cell.
    const html = '<table><tbody>'
      + '<tr><td>Env</td><td>Slots</td></tr>'
      + '<tr><td>Development<br>Integration</td><td>slot1<br>slot2</td></tr>'
      + '</tbody></table>';
    const result = toMd(html);
    expect(result).toBe(
      '| Env | Slots |\n| --- | --- |\n| Development<br>Integration | slot1<br>slot2 |'
    );
  });

  it('converts images to Markdown, keeping the source (incl. data URIs)', () => {
    expect(toMd('<p><img src="https://ms.com/a.png" alt="logo"></p>'))
      .toBe('![logo](https://ms.com/a.png)');
    expect(toMd('<p><img src="data:image/png;base64,AAAB" title="shot"></p>'))
      .toBe('![shot](data:image/png;base64,AAAB)');
  });

  it('converts fenced code blocks with language', () => {
    expect(toMd('<pre class="language-json"><code>{"a":1}</code></pre>'))
      .toBe('```json\n{"a":1}\n```');
  });

  it('renders Teams emoji images as their unicode alt text', () => {
    const html = '<p>hi <span class="animated-emoticon-20-dance"><img itemtype="http://schema.skype.com/Emoji" itemid="dance" alt="🕺" src="https://cdn/x.png"></span></p>';
    expect(toMd(html)).toBe('hi 🕺');
  });

  it('embeds a Teams picture message (AMSImage) inlined as a data URI', () => {
    const html = '<p>note</p><span itemtype="http://schema.skype.com/AMSImage">'
      + '<img alt="imagen" itemtype="http://schema.skype.com/AMSImage" '
      + 'target-src="blob:https://teams.microsoft.com/abc" src="data:image/jpeg;base64,AAAB"></span>';
    expect(toMd(html)).toBe('note\n\n![imagen](data:image/jpeg;base64,AAAB)');
  });

    it('does not emit a broken link when a Teams image is only a blob reference', () => {
      const html = '<p>note</p><img alt="imagen" itemtype="http://schema.skype.com/AMSImage" src="blob:https://teams.microsoft.com/abc">';
      expect(toMd(html)).toBe('note\n\n*imagen*');
    });

    it('keeps Word file:// image references so the app layer can reconcile them with clipboard binaries', () => {
      const html = '<p><img alt="inline" src="data:image/png;base64,AAAB"></p><p><img alt="local" src="file:///Users/me/AppData/Local/Temp/msohtmlclip/clip_image001.png"></p>';
      const out = smartPasteToMarkdown({ html, plain: '' }, DomParser);
      expect(out).toContain('![inline](data:image/png;base64,AAAB)');
      expect(out).toContain('![local](file:///Users/me/AppData/Local/Temp/msohtmlclip/clip_image001.png)');
    });

    it('adds a blank line after an AMSImage so following text is not concatenated', () => {
      // Regression: without the trailing \n\n on the img handler, the paragraph
      // immediately after the span was glued onto the image line.
    const html = '<span itemtype="http://schema.skype.com/AMSImage">'
      + '<img alt="photo" src="data:image/png;base64,AAAB"></span>'
      + '<p>Caption text</p>';
    expect(toMd(html)).toBe('![photo](data:image/png;base64,AAAB)\n\nCaption text');
  });

  it('does not introduce extra blank lines when an image is wrapped in a <p>', () => {
    // The <p> handler calls child().trim(), so the trailing \n\n from the img
    // handler is absorbed and the output is identical to before the fix.
    const html = '<p><img alt="chart" src="data:image/png;base64,AAAB"></p><p>After</p>';
    expect(toMd(html)).toBe('![chart](data:image/png;base64,AAAB)\n\nAfter');
  });

  it('converts Teams reply quotes to author + preview blockquotes', () => {
    const html = '<blockquote itemtype="http://schema.skype.com/Reply" itemid="1">'
      + '<strong itemprop="mri" itemid="8:orgid:abc">Jorge Vicente</strong>'
      + '<span itemprop="time" itemid="1"></span>'
      + '<p itemprop="preview">Buenas, al final…</p></blockquote>';
    expect(toMd(html)).toBe('> **Jorge Vicente**\n> Buenas, al final…');
  });

  it('keeps stacked Teams messages separated and mentions as text', () => {
    const html = '<span data-teams="true">'
      + '<span><span style="font-size:14px"><p><span itemtype="http://schema.skype.com/Mention" itemid="0">Jorge</span>&nbsp;hello</p><p>&nbsp;</p></span></span>'
      + '<span><span style="font-size:14px"><p>second message</p></span></span>'
      + '</span>';
    expect(toMd(html)).toBe('Jorge hello\n\nsecond message');
  });

  it('strips leading whitespace after <br> inside a paragraph (regression: Teams reference codes)', () => {
    // Teams HTML often emits "<br> next line" where the space is formatting
    // noise. The pasted Markdown must NOT start continuation lines with a space.
    const html = '<p>05802014400-V2026,<br> 03074119080-V2026,<br> 04391501644-I2026,<br> 08784860250-V2026</p>';
    const md = toMd(html);
    // Every line must start without a leading space
    md.split('\n').filter(l => l.length > 0).forEach(line => {
      expect(line).not.toMatch(/^ /);
    });
    expect(md).toContain('03074119080-V2026');
  });

  it('strips aria-hidden anchor links from headings (generic HTML from the web)', () => {
    // GitHub, Markup Forge and many static site generators inject
    // <a class="anchor" href="#section" aria-hidden="true">#</a> into headings.
    // These must be silently dropped so headings stay clean.
    const html = '<h2><a href="#intro" aria-hidden="true">#</a>Introduction</h2><p>Body text.</p>';
    const md = toMd(html);
    expect(md).toContain('## Introduction');
    expect(md).not.toContain('[#]');
    expect(md).not.toContain('localhost');
  });

  it('converts <details>/<summary> sections from generic HTML', () => {
    // Markup Forge's own HTML preview uses <details class="section"> with
    // <summary> holding the heading and <div class="section-body"> for content.
    const html = '<details>'
      + '<summary><h2><a href="#s1" aria-hidden="true">#</a>Section One</h2></summary>'
      + '<div class="section-body"><p>Content of section one.</p></div>'
      + '</details>'
      + '<details>'
      + '<summary><h2><a href="#s2" aria-hidden="true">#</a>Section Two</h2></summary>'
      + '<div class="section-body"><p>Content of section two.</p></div>'
      + '</details>';
    const md = toMd(html);
    expect(md).toContain('## Section One');
    expect(md).toContain('## Section Two');
    expect(md).toContain('Content of section one.');
    expect(md).toContain('Content of section two.');
    expect(md).not.toContain('[#]');
  });

  it('strips GitHub permalink anchors (class=anchor, aria-label=Permalink)', () => {
    // GitHub renders headings with an adjacent <a class="anchor" aria-label="Permalink: …">
    // containing an SVG icon. Without stripping these produce noise:
    // "# Title[https://github.com/…#title](https://github.com/…#title)"
    const html = '<div class="markdown-heading">'
      + '<h1 class="heading-element">Sender RFC</h1>'
      + '<a id="user-content-sender-rfc" class="anchor" aria-label="Permalink: Sender RFC"'
      + ' href="https://github.com/org/repo/blob/main/README.md#sender-rfc">'
      + '<svg aria-hidden="true"><path d="m7.775 3.275..."></path></svg>'
      + '</a>'
      + '</div>'
      + '<p>Introduction text.</p>';
    const md = toMd(html);
    expect(md).toContain('# Sender RFC');
    expect(md).toContain('Introduction text.');
    expect(md).not.toContain('https://github.com');
    expect(md).not.toContain('[https://');
  });

  it('handles <markdown-accessiblity-table> GitHub custom element as a block container', () => {
    // GitHub wraps tables in a custom element for accessibility. The table inside
    // must keep its trailing blank line so the next heading does not merge into it.
    const html = '<h2>Meta</h2>'
      + '<markdown-accessiblity-table>'
      + '<table><tr><th>Key</th><th>Value</th></tr><tr><td>Author</td><td>Alice</td></tr></table>'
      + '</markdown-accessiblity-table>'
      + '<h2>Details</h2>'
      + '<p>Body text.</p>';
    const md = toMd(html);
    expect(md).toContain('## Meta');
    expect(md).toContain('| Key | Value |');
    expect(md).toContain('| Author | Alice |');
    expect(md).toContain('## Details');
    expect(md).toContain('Body text.');
    // heading must not be glued onto the last table row (no | on the same line as ##)
    expect(md).not.toMatch(/\|[^\n]*## Details/);
  });

  it('respects align= attributes on <th> cells for table column alignment', () => {
    const html = '<table>'
      + '<tr><th align="left">Name</th><th align="right">Score</th><th align="center">Grade</th><th>Notes</th></tr>'
      + '<tr><td>Alice</td><td>95</td><td>A</td><td>top</td></tr>'
      + '</table>';
    const md = toMd(html);
    expect(md).toContain(':---');   // left-aligned
    expect(md).toContain('---:');   // right-aligned
    expect(md).toContain(':---:');  // center-aligned
    // neutral column (no align attr) gets plain ---
    expect(md).toMatch(/\| --- \|/);
  });

  it('converts a GitHub RFC page HTML fragment (integration test)', () => {
    // Realistic paste from a GitHub-rendered markdown RFC with heading + table + paragraph.
    const html = '<div class="markdown-heading">'
      + '<h1 class="heading-element">Sender Web Push Integration</h1>'
      + '<a class="anchor" aria-label="Permalink: Sender Web Push Integration"'
      + ' href="https://github.com/org/repo#sender-web-push-integration">'
      + '<svg aria-hidden="true"><path d="m1 1 1 1"></path></svg></a>'
      + '</div>'
      + '<markdown-accessiblity-table>'
      + '<table>'
      + '<thead><tr><th align="left">Status</th><th align="left">Draft</th></tr></thead>'
      + '<tbody>'
      + '<tr><td><strong>Author(s)</strong></td><td>'
      + 'Alice (<a href="mailto:alice@example.com">alice@example.com</a>)<br>'
      + 'Bob (<a href="mailto:bob@example.com">bob@example.com</a>)'
      + '</td></tr>'
      + '</tbody>'
      + '</table>'
      + '</markdown-accessiblity-table>'
      + '<div class="markdown-heading">'
      + '<h2 class="heading-element">Objective</h2>'
      + '<a class="anchor" aria-label="Permalink: Objective"'
      + ' href="https://github.com/org/repo#objective">'
      + '<svg aria-hidden="true"><path d="m1 1 1 1"></path></svg></a>'
      + '</div>'
      + '<p>This RFC describes <strong>Web Push</strong> notifications.</p>';
    const md = toMd(html);
    // heading structure
    expect(md).toContain('# Sender Web Push Integration');
    expect(md).toContain('## Objective');
    // no permalink noise
    expect(md).not.toContain('https://github.com');
    // table with left-aligned separators
    expect(md).toContain('| Status | Draft |');
    expect(md).toContain(':---');
    // multi-author cell uses <br> and email links are rendered as Markdown links
    expect(md).toContain('Alice ([alice@example.com](mailto:alice@example.com))<br>Bob ([bob@example.com](mailto:bob@example.com))');
    // paragraph content with bold
    expect(md).toContain('**Web Push**');
    // heading must follow the table with proper separation (not glued)
    const headingIdx = md.indexOf('## Objective');
    const tableEnd = md.lastIndexOf('|', headingIdx);
    expect(md.slice(tableEnd, headingIdx)).toMatch(/\n\n/);
  });
});

describe('smart paste helpers', () => {
  describe('tsvToMarkdownTable', () => {
    it('converts a tab-separated block into a Markdown table', () => {
      const tsv = 'Name\tRole\nAlice\tDev\nBob\tQA';
      expect(tsvToMarkdownTable(tsv)).toBe(
        '| Name | Role |\n| --- | --- |\n| Alice | Dev |\n| Bob | QA |'
      );
    });

    it('pads short rows and escapes pipe characters', () => {
      const tsv = 'A\tB\tC\n1\t2';
      expect(tsvToMarkdownTable(tsv)).toBe('| A | B | C |\n| --- | --- | --- |\n| 1 | 2 |  |');
      expect(tsvToMarkdownTable('a|b\tc')).toContain('a\\|b');
    });

    it('returns null when the text is not tabular', () => {
      expect(tsvToMarkdownTable('just a sentence')).toBeNull();
      expect(tsvToMarkdownTable('single\tcolumnlessrow')).not.toBeNull();
      expect(tsvToMarkdownTable('nocolumns')).toBeNull();
    });
  });

  describe('guessCodeLanguage', () => {
    it('detects common languages', () => {
      expect(guessCodeLanguage('{ "a": 1 }')).toBe('json');
      expect(guessCodeLanguage('SELECT id FROM users WHERE x = 1')).toBe('sql');
      expect(guessCodeLanguage('const x = () => 1;')).toBe('javascript');
      expect(guessCodeLanguage('<div class="x">hi</div>')).toBe('xml');
      expect(guessCodeLanguage('git status\ncd repo')).toBe('bash');
    });

    it('returns empty string for prose', () => {
      expect(guessCodeLanguage('This is a normal sentence.')).toBe('');
    });
  });

  describe('looksLikeCode / detectAndWrapCode', () => {
    it('recognises code and wraps it in a fenced block', () => {
      const code = 'function add(a, b) {\n  return a + b;\n}';
      expect(looksLikeCode(code)).toBe(true);
      expect(detectAndWrapCode(code)).toBe('```javascript\n' + code + '\n```');
    });

    it('leaves prose alone', () => {
      expect(looksLikeCode('Hello there, this is a normal paragraph of text.')).toBe(false);
      expect(detectAndWrapCode('Hello there, this is normal text.')).toBeNull();
    });
  });

  describe('cleanPlainTextToMarkdown', () => {
    it('normalises bullets, ordered markers and blank lines', () => {
      const input = 'Title  \n\n\n\n• one\n• two\n1) first\n2) second';
      expect(cleanPlainTextToMarkdown(input)).toBe(
        'Title\n\n- one\n- two\n1. first\n2. second'
      );
    });
  });

  describe('smartPasteToMarkdown', () => {
    it('prefers rich HTML when present', () => {
      const out = smartPasteToMarkdown({ html: '<h2>Hi</h2>', plain: 'Hi' }, DomParser);
      expect(out).toBe('## Hi');
    });

    it('cleans Microsoft Word HTML into Markdown', () => {
      const html = '<html><body><p class="MsoHeading1" style="mso-outline-level:1">Word Title</p><p class="MsoListParagraph" style="mso-list:l0 level1 lfo1"><span style="font-weight:bold">•</span> Item</p><p><span style="font-weight:bold">Bold</span> and <span style="font-style:italic">Italic</span></p></body></html>';
      const out = smartPasteToMarkdown({ html, plain: '' }, DomParser);
      expect(out).toContain('# Word Title');
      expect(out).toContain('- Item');
      expect(out).toContain('**Bold**');
      expect(out).toContain('*Italic*');
    });

    it('converts Jira wiki markup from the clipboard into Markdown', () => {
      const out = smartPasteToMarkdown({ plain: 'h2. Jira Title\n\n* Bullet\n# Number\n\n|| A || B ||\n| 1 | 2 |' }, DomParser);
      expect(out).toContain('## Jira Title');
      expect(out).toContain('- Bullet');
      expect(out).toContain('1. Number');
      expect(out).toContain('| A | B |');
    });

    it('converts Jira {code:language=xml} to a fenced code block', () => {
      const plain = 'Ejemplo de dependencia Maven:\n\n{code:language=xml}\n<dependency>\n    <groupId>org.apache.tika</groupId>\n</dependency>\n{code}\n\nNOTA: Elegir el scope adecuado.';
      const out = smartPasteToMarkdown({ plain }, DomParser);
      expect(out).toContain('```xml');
      expect(out).toContain('<dependency>');
      expect(out).toContain('org.apache.tika');
      expect(out).toContain('NOTA:');
    });

    it('converts Jira {code:xml} shorthand to a fenced code block', () => {
      const plain = '{code:xml}\n<root/>\n{code}';
      const out = smartPasteToMarkdown({ plain }, DomParser);
      expect(out).toContain('```xml');
      expect(out).toContain('<root/>');
    });

    it('converts Jira {noformat} block to a fenced code block', () => {
      const plain = 'Some text:\n\n{noformat}\nplain preformatted\n{noformat}';
      const out = smartPasteToMarkdown({ plain }, DomParser);
      expect(out).toContain('```');
      expect(out).toContain('plain preformatted');
    });

    it('normalises Jira HTML into Markdown', () => {
      const html = '<div data-node-type="paragraph"><h2>Jira Title</h2><p><strong>Bold</strong> and <em>Italic</em></p><table><tr><td>A</td><td>B</td></tr><tr><td>1</td><td>2</td></tr></table></div>';
      const out = smartPasteToMarkdown({ html, plain: '' }, DomParser);
      expect(out).toContain('## Jira Title');
      expect(out).toContain('**Bold**');
      expect(out).toContain('*Italic*');
      expect(out).toContain('| A | B |');
    });

    it('preserves indentation inside Jira code panels', () => {
      const html = String.raw`<div class="code panel"><div class="codeContent panelContent"><pre class="code-bash" style="white-space: pre-wrap; overflow-wrap: normal;">curl --request GET \
  <span class="code-quote-red">'https://example.com'</span> \
  --header <span class="code-quote-red">'Authorization: Bearer [TOKEN]'</span>
</pre></div></div>`;
      const out = smartPasteToMarkdown({ html, plain: '' }, DomParser);
      expect(out).toContain('```bash');
      expect(out).toContain('curl --request GET \\');
      expect(out).toContain("  'https://example.com'");
      expect(out).toContain("  --header 'Authorization: Bearer [TOKEN]'");
    });

    it('converts tabular plain text to a table', () => {
      const out = smartPasteToMarkdown({ plain: 'A\tB\n1\t2' });
      expect(out).toBe('| A | B |\n| --- | --- |\n| 1 | 2 |');
    });

    it('wraps code-like plain text in a fence', () => {
      const out = smartPasteToMarkdown({ plain: 'const x = () => 1;\nif (x) { return; }' });
      expect(out.startsWith('```javascript')).toBe(true);
    });

    it('cleans prose otherwise', () => {
      const out = smartPasteToMarkdown({ plain: '• alpha\n• beta' });
      expect(out).toBe('- alpha\n- beta');
    });

    it('converts CSV plain text to a table', () => {
      const out = smartPasteToMarkdown({ plain: 'Name,Role\nAlice,Engineer\nBob,Designer' });
      expect(out).toBe('| Name | Role |\n| --- | --- |\n| Alice | Engineer |\n| Bob | Designer |');
    });

    it('converts semicolon-separated CSV to a table', () => {
      const out = smartPasteToMarkdown({ plain: 'a;b;c\n1;2;3\n4;5;6' });
      expect(out).toBe('| a | b | c |\n| --- | --- | --- |\n| 1 | 2 | 3 |\n| 4 | 5 | 6 |');
    });

    it('does not convert prose with commas to a table', () => {
      const out = smartPasteToMarkdown({ plain: 'Hello, world.\nThis is a sentence, right?' });
      expect(out).not.toContain('|');
    });

    it('passes through an already-formatted Markdown table unchanged', () => {
      const table = '| Name | Age |\n| --- | --- |\n| Alice | 28 |';
      expect(smartPasteToMarkdown({ plain: table })).toBe(table);
    });

    it('converts each section independently when blocks are separated by blank lines', () => {
      const csv   = 'Name,Role\nAlice,Engineer';
      const tsv   = 'A\tB\n1\t2';
      const mixed = `${csv}\n\n${tsv}`;
      const out   = smartPasteToMarkdown({ plain: mixed });
      // CSV block → Markdown table
      expect(out).toContain('| Name | Role |');
      expect(out).toContain('| Alice | Engineer |');
      // TSV block → Markdown table
      expect(out).toContain('| A | B |');
      expect(out).toContain('| 1 | 2 |');
    });

    it('does not mix CSV and TSV blocks into a single mangled table', () => {
      const csv = 'First,Last\nJohn,Doe\nJane,Smith';
      const tsv = 'Name\tAge\nAlice\t28';
      const out = smartPasteToMarkdown({ plain: `${csv}\n\n${tsv}` });
      // CSV columns must appear as real columns, not single-cell rows
      expect(out).toContain('| First | Last |');
      expect(out).toContain('| Name | Age |');
      expect(out).not.toMatch(/First,Last/); // no raw CSV lines
    });

    it('passes through an already-formatted Markdown table in a mixed paste', () => {
      const md  = '| X | Y |\n| --- | --- |\n| 1 | 2 |';
      const csv = 'A,B\n3,4';
      const out = smartPasteToMarkdown({ plain: `${md}\n\n${csv}` });
      expect(out).toContain('| X | Y |');
      expect(out).toContain('| A | B |');
      // The already-formatted table must not have its pipes escaped
      expect(out).not.toContain('\\|');
    });
  });
});

describe('looksLikeCsv', () => {
  it('detects comma-separated data', () => {
    expect(looksLikeCsv('a,b\n1,2\n3,4')).toBe(true);
  });
  it('detects semicolon-separated data', () => {
    expect(looksLikeCsv('a;b\n1;2')).toBe(true);
  });
  it('returns false for TSV (tabs present)', () => {
    expect(looksLikeCsv('a\tb\n1\t2')).toBe(false);
  });
  it('returns false for single-row data', () => {
    expect(looksLikeCsv('a,b,c')).toBe(false);
  });
  it('returns false for prose with commas', () => {
    expect(looksLikeCsv('Hello, world.\nGoodbye, friend.')).toBe(false);
  });
  it('returns false for empty string', () => {
    expect(looksLikeCsv('')).toBe(false);
  });
});

describe('source + artifact bundle (T4)', () => {
  const readUint32LE = (bytes, at) =>
    (bytes[at] | (bytes[at + 1] << 8) | (bytes[at + 2] << 16) | (bytes[at + 3] << 24)) >>> 0;
  const readUint16LE = (bytes, at) => bytes[at] | (bytes[at + 1] << 8);

  describe('crc32', () => {
    it('matches the known CRC-32 of "123456789"', () => {
      const bytes = new TextEncoder().encode('123456789');
      expect(crc32(bytes)).toBe(0xCBF43926);
    });

    it('returns 0 for empty input', () => {
      expect(crc32(new Uint8Array(0))).toBe(0);
    });
  });

  describe('buildZip', () => {
    it('produces a valid ZIP signature and end-of-central-directory record', () => {
      const zip = buildZip([{ name: 'a.txt', content: 'hello' }]);
      expect(zip).toBeInstanceOf(Uint8Array);
      // Local file header signature PK\x03\x04.
      expect(readUint32LE(zip, 0)).toBe(0x04034b50);
      // End of central directory signature PK\x05\x06 appears near the end.
      const eocdSig = 0x06054b50;
      let found = -1;
      for (let i = zip.length - 22; i >= 0; i--) {
        if (readUint32LE(zip, i) === eocdSig) { found = i; break; }
      }
      expect(found).toBeGreaterThanOrEqual(0);
      // Total entry count in EOCD.
      expect(readUint16LE(zip, found + 10)).toBe(1);
    });

    it('stores file data uncompressed (method 0) so content is recoverable', () => {
      const content = 'plain stored text';
      const zip = buildZip([{ name: 'note.md', content }]);
      // Method field is at offset 8 of the local header.
      expect(readUint16LE(zip, 8)).toBe(0);
      // The raw bytes should appear verbatim somewhere in the archive.
      const text = new TextDecoder().decode(zip);
      expect(text).toContain(content);
      expect(text).toContain('note.md');
    });

    it('records every file in the central directory', () => {
      const zip = buildZip([
        { name: 'one.txt', content: '1' },
        { name: 'two.txt', content: '22' },
        { name: 'three.txt', content: '333' },
      ]);
      const eocdSig = 0x06054b50;
      let eocd = -1;
      for (let i = zip.length - 22; i >= 0; i--) {
        if (readUint32LE(zip, i) === eocdSig) { eocd = i; break; }
      }
      expect(readUint16LE(zip, eocd + 10)).toBe(3);
    });
  });

  describe('buildSourceArtifactBundle', () => {
    it('returns source.md, index.html and README.md', () => {
      const files = buildSourceArtifactBundle('# Title\n\nBody', 'my-doc.md');
      expect(files.map(f => f.name)).toEqual(['source.md', 'index.html', 'README.md']);
    });

    it('keeps the Markdown source verbatim', () => {
      const md = '# Title\n\n- a\n- b';
      const files = buildSourceArtifactBundle(md, 'doc.md');
      expect(files[0].content).toBe(md);
    });

    it('renders a navigable HTML artifact with an embedded title', () => {
      const files = buildSourceArtifactBundle('# Hello\n\nWorld', 'release-notes.md');
      const html = files[1].content;
      expect(html.toLowerCase()).toContain('<!doctype html>');
      expect(html).toContain('release notes'); // derived title from filename
      expect(html).not.toContain('<script');   // offline / no scripts
    });

    it('documents the bundle contents in the README', () => {
      const files = buildSourceArtifactBundle('# X', 'x.md');
      const readme = files[2].content;
      expect(readme).toContain('source.md');
      expect(readme).toContain('index.html');
      expect(readme).toContain('Markup Forge');
    });

    it('produces a zippable bundle', () => {
      const files = buildSourceArtifactBundle('# X\n\ntext', 'x.md');
      const zip = buildZip(files);
      expect(readUint32LE(zip, 0)).toBe(0x04034b50);
    });
  });
});

describe('highlightMarkdownSource (T1a)', () => {
  // Strip span tags and unescape entities to recover the original plain text.
  const recover = html => html
    .replace(/<[^>]+>/g, '')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&amp;/g, '&');

  it('preserves the source character-for-character (only adds spans)', () => {
    const md = '# Title\n\nSome **bold**, *em*, `code` and [a](http://x).\n- one\n> quote\n```js\nconst x = 1;\n```';
    expect(recover(highlightMarkdownSource(md))).toBe(md);
  });

  it('wraps headings', () => {
    expect(highlightMarkdownSource('## Hello')).toContain('<span class="tok-heading">## Hello</span>');
  });

  it('wraps bold, italic, inline code and links', () => {
    const out = highlightMarkdownSource('a **b** _c_ `d` [e](f)');
    expect(out).toContain('tok-strong');
    expect(out).toContain('tok-em');
    expect(out).toContain('tok-code-inline');
    expect(out).toContain('tok-link');
  });

  it('marks list markers and blockquotes', () => {
    expect(highlightMarkdownSource('- item')).toContain('<span class="tok-list">- </span>');
    expect(highlightMarkdownSource('> quote')).toContain('tok-quote');
  });

  it('highlights fenced code blocks including the fences', () => {
    const out = highlightMarkdownSource('```\ncode line\n```');
    expect(out).toContain('tok-fence');
    expect(out).toContain('<span class="tok-code-block">code line</span>');
  });

  it('does not treat code-fence content as markdown', () => {
    const out = highlightMarkdownSource('```\n# not a heading\n```');
    expect(out).not.toContain('tok-heading');
  });

  it('escapes HTML-significant characters', () => {
    const out = highlightMarkdownSource('<script>alert(1)</script>');
    expect(out).toContain('&lt;script&gt;');
    expect(out).not.toContain('<script>');
  });

  it('keeps the same number of lines as the source', () => {
    const md = 'a\nb\nc\n\nd';
    expect(highlightMarkdownSource(md).split('\n').length).toBe(md.split('\n').length);
  });
});

describe('slash commands (T1c)', () => {
  it('exposes the expected commands', () => {
    expect(SLASH_COMMANDS.map(c => c.name)).toEqual(['table', 'code', 'callout', 'jira', 'email']);
  });

  it('every command has a non-empty snippet and a caret within bounds', () => {
    for (const cmd of SLASH_COMMANDS) {
      expect(cmd.snippet.length).toBeGreaterThan(0);
      expect(cmd.caret).toBeGreaterThanOrEqual(0);
      expect(cmd.caret).toBeLessThanOrEqual(cmd.snippet.length);
    }
  });

  it('returns all commands for an empty query', () => {
    expect(filterSlashCommands('')).toHaveLength(SLASH_COMMANDS.length);
    expect(filterSlashCommands('/')).toHaveLength(SLASH_COMMANDS.length);
  });

  it('filters by name prefix', () => {
    expect(filterSlashCommands('tab').map(c => c.name)).toEqual(['table']);
    expect(filterSlashCommands('/cod').map(c => c.name)).toEqual(['code']);
  });

  it('filters by title substring', () => {
    expect(filterSlashCommands('status').map(c => c.name)).toContain('jira');
  });

  it('returns nothing for an unknown query', () => {
    expect(filterSlashCommands('zzz')).toHaveLength(0);
  });

  it('produces a valid table snippet', () => {
    const table = SLASH_COMMANDS.find(c => c.name === 'table');
    expect(table.snippet).toContain('| --- | --- |');
  });
});

describe('parseCsv (T1d)', () => {
  it('parses simple rows', () => {
    expect(parseCsv('a,b,c\n1,2,3')).toEqual([['a', 'b', 'c'], ['1', '2', '3']]);
  });

  it('handles quoted fields with commas', () => {
    expect(parseCsv('name,note\n"Doe, John",hi')).toEqual([
      ['name', 'note'],
      ['Doe, John', 'hi']
    ]);
  });

  it('handles escaped quotes', () => {
    expect(parseCsv('a\n"say ""hi"""')).toEqual([['a'], ['say "hi"']]);
  });

  it('handles newlines inside quotes', () => {
    expect(parseCsv('a,b\n"line1\nline2",x')).toEqual([
      ['a', 'b'],
      ['line1\nline2', 'x']
    ]);
  });

  it('drops a trailing empty row from a final newline', () => {
    expect(parseCsv('a,b\n1,2\n')).toEqual([['a', 'b'], ['1', '2']]);
  });

  it('respects a custom delimiter', () => {
    expect(parseCsv('a;b\n1;2', ';')).toEqual([['a', 'b'], ['1', '2']]);
  });
});

describe('csvToMarkdownTable (T1d)', () => {
  it('builds a GFM table from CSV', () => {
    expect(csvToMarkdownTable('name,age\nAda,36')).toBe(
      '| name | age |\n| --- | --- |\n| Ada | 36 |'
    );
  });

  it('auto-detects semicolon delimiter', () => {
    expect(csvToMarkdownTable('a;b\n1;2')).toBe('| a | b |\n| --- | --- |\n| 1 | 2 |');
  });

  it('auto-detects tab delimiter', () => {
    expect(csvToMarkdownTable('a\tb\n1\t2')).toBe('| a | b |\n| --- | --- |\n| 1 | 2 |');
  });

  it('escapes pipes in cells', () => {
    expect(csvToMarkdownTable('a,b\nx|y,z')).toContain('x\\|y');
  });

  it('pads short rows to the column count', () => {
    expect(csvToMarkdownTable('a,b,c\n1,2')).toBe(
      '| a | b | c |\n| --- | --- | --- |\n| 1 | 2 |  |'
    );
  });

  it('returns null for single-column input', () => {
    expect(csvToMarkdownTable('just one\nline')).toBeNull();
  });

  it('returns null for empty input', () => {
    expect(csvToMarkdownTable('   ')).toBeNull();
  });
});

describe('importFileToMarkdown (T1d)', () => {
  it('passes Markdown through unchanged', () => {
    expect(importFileToMarkdown('# Hi\n\ntext', 'doc.md')).toBe('# Hi\n\ntext');
  });

  it('treats .txt as Markdown source', () => {
    expect(importFileToMarkdown('plain text', 'notes.txt')).toBe('plain text');
  });

  it('converts .csv into a Markdown table', () => {
    expect(importFileToMarkdown('a,b\n1,2', 'data.csv')).toBe(
      '| a | b |\n| --- | --- |\n| 1 | 2 |'
    );
  });

  it('falls back to raw text when CSV is not tabular', () => {
    expect(importFileToMarkdown('single', 'data.csv')).toBe('single');
  });

  it('converts .html into Markdown', () => {
    expect(importFileToMarkdown('<h1>Title</h1>', 'page.html', DomParser)).toContain('# Title');
  });

  it('handles a missing/unknown extension as Markdown', () => {
    expect(importFileToMarkdown('content', 'README')).toBe('content');
  });
});

describe('parseFrontmatter (T1f)', () => {
  it('returns empty attributes and the original body when absent', () => {
    const r = parseFrontmatter('# Hello\n\nNo frontmatter here.');
    expect(r.attributes).toEqual({});
    expect(r.body).toBe('# Hello\n\nNo frontmatter here.');
  });

  it('parses scalar key/value pairs and strips the block from the body', () => {
    const r = parseFrontmatter('---\ntitle: Weekly Update\ndraft: false\nversion: 3\n---\n# Body\n\nText');
    expect(r.attributes.title).toBe('Weekly Update');
    expect(r.attributes.draft).toBe(false);
    expect(r.attributes.version).toBe(3);
    expect(r.body).toBe('# Body\n\nText');
  });

  it('strips surrounding quotes from values', () => {
    const r = parseFrontmatter('---\ntitle: "Quoted: Title"\n---\nbody');
    expect(r.attributes.title).toBe('Quoted: Title');
  });

  it('parses inline arrays', () => {
    const r = parseFrontmatter('---\ntags: [alpha, beta, gamma]\n---\nbody');
    expect(r.attributes.tags).toEqual(['alpha', 'beta', 'gamma']);
  });

  it('parses block lists', () => {
    const r = parseFrontmatter('---\ntags:\n  - one\n  - two\n---\nbody');
    expect(r.attributes.tags).toEqual(['one', 'two']);
  });

  it('only matches frontmatter at the very start of the document', () => {
    const md = 'intro\n---\ntitle: Nope\n---\nbody';
    const r = parseFrontmatter(md);
    expect(r.attributes).toEqual({});
    expect(r.body).toBe(md);
  });

  it('supports the "..." terminator', () => {
    const r = parseFrontmatter('---\ntitle: Done\n...\nbody');
    expect(r.attributes.title).toBe('Done');
    expect(r.body).toBe('body');
  });

  it('handles an empty document', () => {
    expect(parseFrontmatter('')).toEqual({ attributes: {}, body: '' });
  });
});

describe('stripFrontmatter (T1f)', () => {
  it('removes the frontmatter block', () => {
    expect(stripFrontmatter('---\ntitle: X\n---\n# Heading')).toBe('# Heading');
  });

  it('leaves content without frontmatter unchanged', () => {
    expect(stripFrontmatter('# Heading\n\ntext')).toBe('# Heading\n\ntext');
  });
});

// ---------------------------------------------------------------------------
// highlightCode / highlightCodeForPreview — regression: span attribute
// values must never be corrupted by subsequent regex passes (T-hljs-safe)
// ---------------------------------------------------------------------------
describe('highlightCode (T-hljs-safe)', () => {
  it('does not corrupt span attributes when highlighting JSON', () => {
    const out = highlightCode('{ "eventId": "0", "ok": true }', 'json');
    // class= from generated spans must not appear as text
    expect(out).not.toMatch(/^class=/m);
    expect(out).not.toMatch(/>\s*class=/);
    // Well-formed: every opened span has a matching close
    const opens = (out.match(/<span /g) || []).length;
    const closes = (out.match(/<\/span>/g) || []).length;
    expect(opens).toBe(closes);
  });

  it('does not corrupt span attributes when highlighting Java', () => {
    const javaCode = 'public class Main {\n  String name = "Alice";\n  int x = 42;\n}';
    const out = highlightCode(javaCode, 'java');
    // The word "class" inside a span attribute must not be double-wrapped
    expect(out).not.toContain('<span <span');
    const opens = (out.match(/<span /g) || []).length;
    const closes = (out.match(/<\/span>/g) || []).length;
    expect(opens).toBe(closes);
  });

  it('wraps Java keywords correctly without breaking tag structure', () => {
    const out = highlightCode('public class Main {', 'java');
    expect(out).toContain('<span class="hljs-keyword">public</span>');
    expect(out).toContain('<span class="hljs-keyword">class</span>');
    // "class" in the attribute value of a span must not itself be wrapped
    expect(out).not.toContain('<span <span');
  });
});

describe('highlightCodeForPreview (T-hljs-safe)', () => {
  it('does not corrupt span attributes for Java strings', () => {
    const out = highlightCodeForPreview('  String name = "Alice";', 'java');
    expect(out).not.toContain('<span <span');
    const opens = (out.match(/<span /g) || []).length;
    const closes = (out.match(/<\/span>/g) || []).length;
    expect(opens).toBe(closes);
  });

  it('does not wrap "class" inside span attribute values for Java', () => {
    const out = highlightCodeForPreview('public class Main {', 'java');
    // "class" keyword should be wrapped once as hljs-keyword
    expect(out).toContain('<span class="hljs-keyword">class</span>');
    // but the attribute value "class" in class="hljs-keyword" must stay intact
    expect(out).not.toContain('<span <span');
  });

  it('produces well-formed spans for JSON', () => {
    const out = highlightCodeForPreview('  "eventId": "0",', 'json');
    expect(out).not.toContain('<span <span');
    const opens = (out.match(/<span /g) || []).length;
    const closes = (out.match(/<\/span>/g) || []).length;
    expect(opens).toBe(closes);
  });
});

// ---------------------------------------------------------------------------
// tsvToMarkdownTable — multiline cell support (T-tsv-multiline)
// ---------------------------------------------------------------------------
describe('tsvToMarkdownTable multiline cells (T-tsv-multiline)', () => {
  it('joins no-tab continuation lines into the last non-empty cell with ", "', () => {
    const tsv = 'APPKEY\tSERVIDOR\nAMANDA\tSRV1\nSRV2\nSRV3';
    const out = tsvToMarkdownTable(tsv);
    expect(out).toContain('| AMANDA | SRV1, SRV2, SRV3 |');
  });

  it('skips all-empty tab rows (row-end separators)', () => {
    const tsv = 'APPKEY\tSERVIDOR\nAMANDA\tSRV1\t \t \nBOB\tSRV2';
    const out = tsvToMarkdownTable(tsv);
    // All-empty row produces no extra Markdown row
    const rows = out.split('\n').filter(l => l.startsWith('|') && !l.includes('---'));
    expect(rows).toHaveLength(3); // header + AMANDA + BOB
    expect(out).toContain('| AMANDA | SRV1 |');
    expect(out).toContain('| BOB | SRV2 |');
  });

  it('treats single-cell + trailing tabs as final continuation + row end', () => {
    const tsv = 'APPKEY\tUSER\tSERVIDOR\nFOO\tU1\tSRV1\nSRV2\t \t \nBAR\tU2\tSRV3';
    const out = tsvToMarkdownTable(tsv);
    expect(out).toContain('| FOO | U1 | SRV1, SRV2 |');
    expect(out).toContain('| BAR | U2 | SRV3 |');
  });

  it('still converts a plain TSV without multiline cells', () => {
    expect(tsvToMarkdownTable('Name\tRole\nAlice\tDev\nBob\tQA'))
      .toBe('| Name | Role |\n| --- | --- |\n| Alice | Dev |\n| Bob | QA |');
  });

  it('handles the real-world DB2 permissions table excerpt', () => {
    const tsv = [
      'APPKEY\tUSUARIO\tSERVIDOR',
      'AMANDA\tUAMANDA\tSRV1',
      'SRV2',
      'SRV3\t \t ',
      'BCSUPSHIPM\tUICACMAS\tSRVA',
      'SRVB\t \t ',
    ].join('\n');
    const out = tsvToMarkdownTable(tsv);
    expect(out).toContain('| AMANDA | UAMANDA | SRV1, SRV2, SRV3 |');
    expect(out).toContain('| BCSUPSHIPM | UICACMAS | SRVA, SRVB |');
    // Should be header + 2 data rows, no spurious rows
    const dataRows = out.split('\n').filter(l => l.startsWith('|') && !l.includes('---'));
    expect(dataRows).toHaveLength(3); // header + AMANDA + BCSUPSHIPM
  });
});

// ---------------------------------------------------------------------------
// looksLikeSpaceTable / spaceTableToMarkdown (T-space-table)
// ---------------------------------------------------------------------------
describe('looksLikeSpaceTable (T-space-table)', () => {
  it('detects a 2-column space-separated table', () => {
    expect(looksLikeSpaceTable('Repository  Usernames\nfoo/bar  USER1\nfoo/baz  USER2')).toBe(true);
  });

  it('returns false for plain prose', () => {
    expect(looksLikeSpaceTable('Hello world this is some text')).toBe(false);
  });

  it('returns false when column counts differ by more than 1', () => {
    // Line 1 has 2 cols, lines 2-3 have 4 cols → difference > 1
    expect(looksLikeSpaceTable('A  B\nC  D  E  F\nG  H  I  J')).toBe(false);
  });

  it('returns false for a single line', () => {
    expect(looksLikeSpaceTable('Repo  User')).toBe(false);
  });
});

describe('spaceTableToMarkdown (T-space-table)', () => {
  it('converts a 2-column space-separated table to GFM', () => {
    const input = 'Repository  Usernames Found\nfoo/bar  USER1\nfoo/baz  USER2, USER3';
    const out = spaceTableToMarkdown(input);
    expect(out).toContain('| Repository | Usernames Found |');
    expect(out).toContain('| --- | --- |');
    expect(out).toContain('| foo/bar | USER1 |');
    expect(out).toContain('| foo/baz | USER2, USER3 |');
  });

  it('returns null for non-space-table text', () => {
    expect(spaceTableToMarkdown('just prose text')).toBeNull();
  });

  it('escapes pipe characters in cells', () => {
    const out = spaceTableToMarkdown('A|B  C\nval1  val2');
    expect(out).toContain('A\\|B');
  });
});
