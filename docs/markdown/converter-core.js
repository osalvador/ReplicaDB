// Markup Forge - pure conversion/rendering logic.
// This module is browser-only (ES module) and dependency-free so it can be
// imported both by converter.html and by the Vitest unit tests.

export function htmlEscape(value) {
  return String(value)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

export function escapeRegExp(value) {
  return String(value).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

// --- Editor syntax highlighting (T1a) --------------------------------------
// Produce an HTML string that mirrors the Markdown source character-for-
// character (only <span> wrappers are inserted, never text changes) so it can
// be layered behind a transparent <textarea> as a highlight overlay. Because
// no visible characters are added or removed, the overlay stays perfectly
// aligned with the textarea caret and selection.

const HL_INLINE_RE = /(`[^`\n]+`)|(\*\*[^\n]+?\*\*|__[^\n]+?__)|(\[[^\]\n]+\]\([^)\n]+\))|(\*[^*\n]+?\*|_[^_\n]+?_)/g;

function highlightInline(escaped) {
  return escaped.replace(HL_INLINE_RE, (match, code, strong, link, em) => {
    if (code) return `<span class="tok-code-inline">${code}</span>`;
    if (strong) return `<span class="tok-strong">${strong}</span>`;
    if (link) return `<span class="tok-link">${link}</span>`;
    if (em) return `<span class="tok-em">${em}</span>`;
    return match;
  });
}

export function highlightMarkdownSource(md) {
  const source = String(md == null ? '' : md);
  const lines = source.split('\n');
  let inFence = false;
  const out = lines.map(line => {
    const escaped = htmlEscape(line);
    if (/^\s*(```|~~~)/.test(line)) {
      inFence = !inFence;
      return `<span class="tok-fence">${escaped}</span>`;
    }
    if (inFence) return `<span class="tok-code-block">${escaped}</span>`;
    if (/^#{1,6}\s/.test(line)) return `<span class="tok-heading">${escaped}</span>`;
    if (/^\s*>/.test(line)) return `<span class="tok-quote">${escaped}</span>`;
    if (/^\s*([-*_])(\s*\1){2,}\s*$/.test(line)) return `<span class="tok-hr">${escaped}</span>`;
    const listMatch = line.match(/^(\s*)([-*+]|\d+[.)])(\s+)/);
    if (listMatch) {
      const prefixLen = listMatch[0].length;
      const marker = htmlEscape(line.slice(0, prefixLen));
      const rest = highlightInline(htmlEscape(line.slice(prefixLen)));
      return `<span class="tok-list">${marker}</span>${rest}`;
    }
    return highlightInline(escaped);
  });
  return out.join('\n');
}

export function protectCodeSpans(text) {
  const stash = [];
  const replaced = text.replace(/`([^`]+)`/g, (_, code) => {
    const id = stash.push(code) - 1;
    return `@@CODE${id}@@`;
  });
  return { replaced, stash };
}

export function renderInline(text) {
  const { replaced, stash } = protectCodeSpans(htmlEscape(text));
  let s = replaced;
  s = s.replace(/!\[([^\]]*)\]\(([^)\s]+)(?:\s+"([^"]+)")?\)/g, '<img alt="$1" src="$2" title="$3" />');
  s = s.replace(/\[([^\]]+)\]\(([^)\s]+)(?:\s+"([^"]+)")?\)/g, '<a href="$2" target="_blank" rel="noopener noreferrer">$1</a>');
  s = s.replace(/\*\*([^*\n]+)\*\*/g, '<strong>$1</strong>');
  s = s.replace(/__([^_\n]+)__/g, '<strong>$1</strong>');
  s = s.replace(/(?<!\*)\*([^*\n]+)\*(?!\*)/g, '<em>$1</em>');
  s = s.replace(/(?<!_)_([^_\n]+)_(?!_)/g, '<em>$1</em>');
  s = s.replace(/~~([^~\n]+)~~/g, '<del>$1</del>');
  s = s.replace(/@@CODE(\d+)@@/g, (_, n) => `<code>${htmlEscape(stash[Number(n)] || '')}</code>`);
  return s;
}

// Apply a regex replacement only to text nodes (the segments between HTML tags),
// leaving already-generated <span> and other tags completely untouched.
// This prevents a later pass from matching text that appears inside tag attribute
// values (e.g. the word "class" in class="hljs-string") and corrupting the markup.
function applyToTextOnly(html, pattern, replacement) {
  return html.replace(/(<[^>]*>)|([^<]+)/g, (m, tag, text) => {
    if (tag) return tag;        // leave tags intact
    return text.replace(pattern, replacement);
  });
}

export function highlightCode(code, lang) {
  let s = htmlEscape(code);
  if (/^(json|js|javascript|ts|typescript)$/i.test(lang)) {
    s = applyToTextOnly(s, /(&quot;[^&]*?&quot;)(\s*:)/g, '<span class="hljs-attr">$1</span>$2');
    s = applyToTextOnly(s, /(:\s*)(&quot;[^&]*?&quot;)/g, '$1<span class="hljs-string">$2</span>');
    s = applyToTextOnly(s, /\b(true|false|null)\b/g, '<span class="hljs-keyword">$1</span>');
    s = applyToTextOnly(s, /\b(-?\d+(?:\.\d+)?)\b/g, '<span class="hljs-number">$1</span>');
  } else if (/^(java|bash|sh|yaml|yml)$/i.test(lang)) {
    s = applyToTextOnly(s, /(\/\/.*$|#.*$)/gm, '<span class="hljs-comment">$1</span>');
    s = applyToTextOnly(s, /(&quot;.*?&quot;|'.*?')/g, '<span class="hljs-string">$1</span>');
    s = applyToTextOnly(s, /\b(public|private|class|return|new|if|else|try|catch|final|static|void|boolean|int|long|String)\b/g, '<span class="hljs-keyword">$1</span>');
    s = applyToTextOnly(s, /\b(-?\d+(?:\.\d+)?)\b/g, '<span class="hljs-number">$1</span>');
  }
  return s;
}

export function parseTable(lines, i) {
  if (i + 1 >= lines.length || !/\|/.test(lines[i])) return null;
  const separator = lines[i + 1];
  if (!/^\s*\|?\s*:?-{3,}:?\s*(\|\s*:?-{3,}:?\s*)+\|?\s*$/.test(separator)) return null;
  const split = line => line.replace(/^\s*\|?|\|?\s*$/g, '').split('|').map(cell => cell.trim());
  const header = split(lines[i]);
  const rows = [];
  let j = i + 2;
  while (j < lines.length && /\|/.test(lines[j]) && lines[j].trim()) {
    rows.push(split(lines[j]));
    j += 1;
  }
  const html = `<table><thead><tr>${header.map(h => `<th>${renderInline(h)}</th>`).join('')}</tr></thead><tbody>${rows.map(row => `<tr>${header.map((_, idx) => `<td>${renderInline(row[idx] || '')}</td>`).join('')}</tr>`).join('')}</tbody></table>`;
  return { html, next: j };
}

export function parseMarkdownBlocks(markdown) {
  const lines = String(markdown || '').replace(/\r\n?/g, '\n').split('\n');
  const blocks = [];
  let i = 0;
  while (i < lines.length) {
    const start = i;
    const raw = lines[i];
    const trimmed = raw.trim();
    if (!trimmed) {
      blocks.push({ type: 'blank', startLine: i, endLine: i, text: raw });
      i += 1;
      continue;
    }

    const fence = trimmed.match(/^```\s*([^`]*)$/);
    if (fence) {
      i += 1;
      while (i < lines.length && !/^```\s*$/.test(lines[i].trim())) i += 1;
      if (i < lines.length) i += 1;
      blocks.push({ type: 'code', startLine: start, endLine: i - 1, text: lines.slice(start, i).join('\n') });
      continue;
    }

    const table = parseTable(lines, i);
    if (table) {
      blocks.push({ type: 'table', startLine: start, endLine: table.next - 1, text: lines.slice(start, table.next).join('\n') });
      i = table.next;
      continue;
    }

    if (/^(#{1,6})\s+/.test(trimmed)) {
      blocks.push({ type: 'heading', startLine: i, endLine: i, text: raw });
      i += 1;
      continue;
    }

    if (/^(---|\*\*\*|___)$/.test(trimmed)) {
      blocks.push({ type: 'hr', startLine: i, endLine: i, text: raw });
      i += 1;
      continue;
    }

    if (/^>\s?/.test(trimmed)) {
      while (i < lines.length && /^>\s?/.test(lines[i].trim())) i += 1;
      blocks.push({ type: 'quote', startLine: start, endLine: i - 1, text: lines.slice(start, i).join('\n') });
      continue;
    }

    if (/^[-*+]\s+/.test(trimmed)) {
      while (i < lines.length && /^[-*+]\s+/.test(lines[i].trim())) i += 1;
      blocks.push({ type: 'ul', startLine: start, endLine: i - 1, text: lines.slice(start, i).join('\n') });
      continue;
    }

    if (/^\d+\.\s+/.test(trimmed)) {
      while (i < lines.length && /^\d+\.\s+/.test(lines[i].trim())) i += 1;
      blocks.push({ type: 'ol', startLine: start, endLine: i - 1, text: lines.slice(start, i).join('\n') });
      continue;
    }

    while (i < lines.length && lines[i].trim()) {
      if (/^(#{1,6}\s+|```|>\s?|[-*+]\s+|\d+\.\s+|---$|\*\*\*$|___$)/.test(lines[i].trim())) break;
      if (parseTable(lines, i)) break;
      i += 1;
    }
    blocks.push({ type: 'paragraph', startLine: start, endLine: i - 1, text: lines.slice(start, i).join('\n') });
  }
  return blocks;
}

export function renderMarkdown(markdown) {
  const lines = String(markdown || '').replace(/\r\n?/g, '\n').split('\n');
  const out = [];
  let i = 0;
  while (i < lines.length) {
    const raw = lines[i];
    const trimmed = raw.trim();
    if (!trimmed) { i += 1; continue; }

    const fence = trimmed.match(/^```\s*([^`]*)$/);
    if (fence) {
      const lang = (fence[1] || '').trim();
      const code = [];
      i += 1;
      while (i < lines.length && !/^```\s*$/.test(lines[i].trim())) {
        code.push(lines[i]);
        i += 1;
      }
      if (i < lines.length) i += 1;
      out.push(`<pre><code class="language-${htmlEscape(lang || 'text')}">${highlightCode(code.join('\n'), lang)}</code></pre>`);
      continue;
    }

    const table = parseTable(lines, i);
    if (table) { out.push(table.html); i = table.next; continue; }

    const heading = trimmed.match(/^(#{1,6})\s+(.*)$/);
    if (heading) {
      const level = heading[1].length;
      out.push(`<h${level}>${renderInline(heading[2])}</h${level}>`);
      i += 1;
      continue;
    }

    if (/^(---|\*\*\*|___)$/.test(trimmed)) { out.push('<hr>'); i += 1; continue; }

    const quote = trimmed.match(/^>\s?(.*)$/);
    if (quote) {
      const parts = [];
      while (i < lines.length) {
        const m = lines[i].trim().match(/^>\s?(.*)$/);
        if (!m) break;
        parts.push(m[1]);
        i += 1;
      }
      out.push(`<blockquote>${parts.map(renderInline).join('<br>')}</blockquote>`);
      continue;
    }

    const ul = trimmed.match(/^[-*+]\s+(.*)$/);
    if (ul) {
      const items = [];
      while (i < lines.length) {
        const m = lines[i].trim().match(/^[-*+]\s+(.*)$/);
        if (!m) break;
        items.push(m[1]);
        i += 1;
      }
      out.push(`<ul>${items.map(item => `<li>${renderInline(item)}</li>`).join('')}</ul>`);
      continue;
    }

    const ol = trimmed.match(/^\d+\.\s+(.*)$/);
    if (ol) {
      const items = [];
      while (i < lines.length) {
        const m = lines[i].trim().match(/^\d+\.\s+(.*)$/);
        if (!m) break;
        items.push(m[1]);
        i += 1;
      }
      out.push(`<ol>${items.map(item => `<li>${renderInline(item)}</li>`).join('')}</ol>`);
      continue;
    }

    const para = [];
    while (i < lines.length && lines[i].trim() && !/^(#{1,6}\s+|```|>\s?|[-*+]\s+|\d+\.\s+|---$|\*\*\*$|___$)/.test(lines[i].trim())) {
      if (parseTable(lines, i)) break;
      para.push(lines[i].trim());
      i += 1;
    }
    out.push(`<p>${renderInline(para.join(' '))}</p>`);
  }
  return out.join('\n') || '<p style="color:#6b7280">Start writing Markdown to see the preview.</p>';
}

export function inlineToJira(text) {
  const { replaced, stash } = protectCodeSpans(text);
  const bold = [];
  let s = replaced;
  // Images → Jira image syntax  !url! or !url|alt=alt!
  // This preserves enough information for jiraToHtml to render <img> in the
  // Visual preview, and produces valid Jira wiki markup for external images.
  s = s.replace(/!\\?\[([^\]]*)\]\(([^)]+)\)/g, (_, alt, url) =>
    `!${url}${alt ? `|alt=${alt}` : ''}!`);
  s = s.replace(/\[([^\]]+)\]\(([^)]+)\)/g, (_, label, url) => `[${label}|${url}]`);
  s = s.replace(/\*\*([^*\n]+)\*\*/g, (_, value) => `@@BOLD${bold.push(value) - 1}@@`);
  s = s.replace(/__([^_\n]+)__/g, (_, value) => `@@BOLD${bold.push(value) - 1}@@`);
  s = s.replace(/(?<!\*)\*([^*\n]+)\*(?!\*)/g, '_$1_');
  s = s.replace(/(?<!_)_([^_\n]+)_(?!_)/g, '_$1_');
  s = s.replace(/@@BOLD(\d+)@@/g, (_, n) => `*${bold[Number(n)]}*`);
  s = s.replace(/~~([^~\n]+)~~/g, '-$1-');
  s = s.replace(/@@CODE(\d+)@@/g, (_, n) => `{{${stash[Number(n)] || ''}}}`);
  return s;
}

export function convertMarkdownToJira(markdown) {
  const lines = String(markdown || '').replace(/\r\n?/g, '\n').split('\n');
  const out = [];
  let i = 0;
  while (i < lines.length) {
    const line = lines[i];
    const trimmed = line.trim();
    if (!trimmed) { out.push(''); i += 1; continue; }
    const fence = trimmed.match(/^```\s*([^`]*)$/);
    if (fence) {
      const lang = (fence[1] || '').trim();
      const code = [];
      i += 1;
      while (i < lines.length && !/^```\s*$/.test(lines[i].trim())) { code.push(lines[i]); i += 1; }
      if (i < lines.length) i += 1;
      out.push(`{code${lang ? ':language=' + lang : ''}}`);
      out.push(code.join('\n'));
      out.push('{code}');
      continue;
    }
    const heading = trimmed.match(/^(#{1,6})\s+(.*)$/);
    if (heading) { out.push(`h${Math.min(6, heading[1].length)}. ${inlineToJira(heading[2])}`); i += 1; continue; }
    // GFM table: header row followed by a separator row (| --- | --- |)
    if (/\|/.test(trimmed) && i + 1 < lines.length &&
        /^\s*\|?\s*:?-{3,}:?\s*(\|\s*:?-{3,}:?\s*)+\|?\s*$/.test(lines[i + 1].trim())) {
      const header = line.replace(/^\s*\|?|\|?\s*$/g, '').split('|').map(s => s.trim());
      // Jira header row uses || col || col ||
      out.push(`||${header.map(h => ` ${inlineToJira(h)} `).join('||')}||`);
      i += 2; // skip header + separator
      while (i < lines.length && /\|/.test(lines[i]) && lines[i].trim()) {
        const cells = lines[i].replace(/^\s*\|?|\|?\s*$/g, '').split('|').map(s => s.trim());
        out.push(`|${cells.map(c => ` ${inlineToJira(c)} `).join('|')}|`);
        i += 1;
      }
      continue;
    }
    const quote = trimmed.match(/^>\s?(.*)$/);
    if (quote) { out.push(`bq. ${inlineToJira(quote[1])}`); i += 1; continue; }
    const ul = trimmed.match(/^[-*+]\s+(.*)$/);
    if (ul) { out.push(`* ${inlineToJira(ul[1])}`); i += 1; continue; }
    const ol = trimmed.match(/^\d+\.\s+(.*)$/);
    if (ol) { out.push(`# ${inlineToJira(ol[1])}`); i += 1; continue; }
    out.push(inlineToJira(line));
    i += 1;
  }
  return out.join('\n');
}

/**
 * Convert Jira wiki markup text to HTML for the Visual preview iframe.
 *
 * Key behaviours:
 *  - Blank lines separate paragraphs.
 *  - Single newlines *within* a paragraph become <br> so soft-break lists
 *    (e.g. reference codes pasted from Teams) are displayed line-by-line.
 *  - `\\` (Jira explicit line break) → <br>.
 *  - Block-level constructs: h1.-h6., bq., {quote}…{quote}, {code}…{code},
 *    unordered (*) and ordered (#) lists, ---- horizontal rules.
 *  - Inline: *bold*, _italic_, {{monospace}}, [label|url].
 *
 * This intentionally avoids the `-strikethrough-` Jira pattern because it
 * would produce false positives on hyphenated identifiers like "REF-2026".
 */
export function jiraToHtml(jiraText) {
  const text = String(jiraText || '').replace(/\r\n?/g, '\n');

  /** Escape HTML special chars (for text nodes / code blocks). */
  function esc(s) {
    return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  }

  /** Apply inline Jira → HTML conversions to a single line of text. */
  function inlineJira(s) {
    s = esc(s);
    // \\ explicit line break
    s = s.replace(/\\\\/g, '<br>');
    // Jira image syntax: !url! or !url|params! (only for real URLs / data URIs)
    // inlineToJira emits !url|alt=alt! for Markdown images.
    s = s.replace(/!((?:https?:|data:|ftp:)[^!\s]*?)(?:\|([^!]*))?!/g, (_, url, params) => {
      const altMatch = params ? params.match(/(?:^|,)alt=([^,]+)/) : null;
      const alt = altMatch ? altMatch[1] : '';
      return `<img src="${url}" alt="${alt}" style="max-width:100%;height:auto;vertical-align:middle;">`;
    });
    // {{monospace}}
    s = s.replace(/\{\{([^}]+)\}\}/g, (_, c) => `<code>${c}</code>`);
    // *bold* (not inside words — require non-word boundary or start/end)
    s = s.replace(/(^|[\s([{])\*(\S[^*\n]*\S|\S)\*($|[\s)\]},!?.])/g, '$1<strong>$2</strong>$3');
    // _italic_
    s = s.replace(/(^|[\s([{])_(\S[^_\n]*\S|\S)_($|[\s)\]},!?.])/g, '$1<em>$2</em>$3');
    // [label|url] — Jira link syntax
    s = s.replace(/\[([^\]|]+)\|([^\]]+)\]/g, (_, label, url) => `<a href="${esc(url)}">${label}</a>`);
    return s;
  }

  const lines = text.split('\n');
  const out = [];
  let i = 0;

  while (i < lines.length) {
    const line = lines[i];
    const trimmed = line.trim();

    // Blank line: just advance (paragraph boundaries are handled below)
    if (!trimmed) { i += 1; continue; }

    // {code} … {code}  — supports:
    //   {code}                   → no language
    //   {code:language=xml}      → explicit language= prefix
    //   {code:xml}               → shorthand without language= prefix
    if (/^\{code/.test(trimmed)) {
      const langMatch = trimmed.match(/\{code(?::(?:language=)?(\w+))?\}/);
      const lang = langMatch ? (langMatch[1] || '') : '';
      const code = [];
      i += 1;
      while (i < lines.length && !/^\{code\}\s*$/.test(lines[i].trim())) {
        code.push(esc(lines[i]));
        i += 1;
      }
      if (i < lines.length) i += 1; // consume closing {code}
      out.push(`<pre><code${lang ? ` class="language-${lang}"` : ''}>${code.join('\n')}</code></pre>`);
      continue;
    }

    // {noformat} … {noformat}  — preformatted plain text, no syntax highlight
    if (trimmed === '{noformat}') {
      const code = [];
      i += 1;
      while (i < lines.length && lines[i].trim() !== '{noformat}') {
        code.push(esc(lines[i]));
        i += 1;
      }
      if (i < lines.length) i += 1; // consume closing {noformat}
      out.push(`<pre><code>${code.join('\n')}</code></pre>`);
      continue;
    }

    // {quote} … {quote} (recursive)
    if (trimmed === '{quote}') {
      const quoted = [];
      i += 1;
      while (i < lines.length && lines[i].trim() !== '{quote}') {
        quoted.push(lines[i]);
        i += 1;
      }
      if (i < lines.length) i += 1;
      out.push(`<blockquote>${jiraToHtml(quoted.join('\n'))}</blockquote>`);
      continue;
    }

    // h1. … h6.
    const heading = trimmed.match(/^h([1-6])\.\s+(.*)$/);
    if (heading) {
      out.push(`<h${heading[1]}>${inlineJira(heading[2])}</h${heading[1]}>`);
      i += 1;
      continue;
    }

    // bq. single-line blockquote
    const bq = trimmed.match(/^bq\.\s+(.*)$/);
    if (bq) {
      out.push(`<blockquote><p>${inlineJira(bq[1])}</p></blockquote>`);
      i += 1;
      continue;
    }

    // ---- horizontal rule
    if (/^-{4,}$/.test(trimmed)) {
      out.push('<hr>');
      i += 1;
      continue;
    }

    // Unordered list (* item, ** nested)
    if (/^\*+\s/.test(trimmed)) {
      const items = [];
      while (i < lines.length && /^\*+\s/.test(lines[i].trim())) {
        const depth = (lines[i].trim().match(/^\*+/) || [''])[0].length;
        const text = lines[i].trim().replace(/^\*+\s+/, '');
        items.push({ depth, text });
        i += 1;
      }
      // Simple flat rendering (nesting would require stack — keep it simple)
      out.push(`<ul>${items.map(it => `<li>${inlineJira(it.text)}</li>`).join('')}</ul>`);
      continue;
    }

    // Ordered list (# item, ## nested)
    if (/^#+\s/.test(trimmed)) {
      const items = [];
      while (i < lines.length && /^#+\s/.test(lines[i].trim())) {
        const text = lines[i].trim().replace(/^#+\s+/, '');
        items.push(text);
        i += 1;
      }
      out.push(`<ol>${items.map(t => `<li>${inlineJira(t)}</li>`).join('')}</ol>`);
      continue;
    }

    // Jira table rows: || header || or | cell |
    if (/^\|/.test(trimmed)) {
      const rows = [];
      while (i < lines.length && /^\|/.test(lines[i].trim())) {
        rows.push(lines[i].trim());
        i += 1;
      }
      const htmlRows = rows.map(row => {
        if (row.startsWith('||')) {
          // Header row: ||col1||col2||  →  split on '||', drop empty first/last
          const cells = row.replace(/^\|\||\|\|$/g, '').split('||').map(s => s.trim());
          return `<tr>${cells.map(c => `<th>${inlineJira(c)}</th>`).join('')}</tr>`;
        } else {
          // Data row: |col1|col2|  →  split on '|', drop empty first/last
          const cells = row.replace(/^\||\|$/g, '').split('|').map(s => s.trim());
          return `<tr>${cells.map(c => `<td>${inlineJira(c)}</td>`).join('')}</tr>`;
        }
      });
      out.push(`<table style="border-collapse:collapse;width:100%">${htmlRows.join('')}</table>`);
      continue;
    }

    // Paragraph: collect consecutive non-special, non-blank lines.
    // Single newlines within the paragraph become <br>.
    const paraLines = [];
    while (i < lines.length) {
      const l = lines[i];
      const t = l.trim();
      if (!t) break;
      if (/^(h[1-6]\.\s|bq\.\s|\{code|\{quote\}|\*+\s|#+\s|-{4,}$|\|)/.test(t)) break;
      paraLines.push(inlineJira(l.trimEnd()));
      i += 1;
    }
    if (paraLines.length) out.push(`<p>${paraLines.join('<br>')}</p>`);
  }

  return out.join('\n');
}

export function languageLabel(language) {
  const raw = String(language || '').trim().toLowerCase();
  if (!raw) return 'Text';
  const labels = { js: 'JavaScript', javascript: 'JavaScript', ts: 'TypeScript', java: 'Java', json: 'JSON', yaml: 'YAML', yml: 'YAML', bash: 'Bash', sh: 'Shell', html: 'HTML', css: 'CSS', xml: 'XML', sql: 'SQL', text: 'Text' };
  return labels[raw] || raw.charAt(0).toUpperCase() + raw.slice(1);
}

export function highlightCodeForPreview(code, language) {
  let s = htmlEscape(code);
  const lang = String(language || '').trim().toLowerCase();
  if (lang === 'json') {
    s = applyToTextOnly(s, /(&quot;[^&]*?&quot;)(\s*:)/g, '<span class="hljs-attr">$1</span>$2');
    s = applyToTextOnly(s, /(:\s*)(&quot;[^&]*?&quot;)/g, '$1<span class="hljs-string">$2</span>');
    s = applyToTextOnly(s, /\b(true|false|null)\b/g, '<span class="hljs-keyword">$1</span>');
    s = applyToTextOnly(s, /\b(-?\d+(?:\.\d+)?)\b/g, '<span class="hljs-number">$1</span>');
    return s;
  }
  if (lang === 'java' || lang === 'js' || lang === 'javascript' || lang === 'ts') {
    s = applyToTextOnly(s, /(&quot;.*?&quot;|'.*?')/g, '<span class="hljs-string">$1</span>');
    s = applyToTextOnly(s, /\b(public|private|class|return|new|const|let|var|if|else|try|catch|throw|void|static|final|import|package)\b/g, '<span class="hljs-keyword">$1</span>');
    s = applyToTextOnly(s, /(\/\/.*$)/gm, '<span class="hljs-comment">$1</span>');
    s = applyToTextOnly(s, /\b(-?\d+(?:\.\d+)?)\b/g, '<span class="hljs-number">$1</span>');
    return s;
  }
  if (lang === 'yaml' || lang === 'yml') {
    s = applyToTextOnly(s, /^([\s-]*)([A-Za-z0-9_.-]+)(\s*:)/gm, '$1<span class="hljs-attr">$2</span>$3');
    s = applyToTextOnly(s, /(:\s*)([^\n#]+)/g, '$1<span class="hljs-string">$2</span>');
    s = applyToTextOnly(s, /(#.*$)/gm, '<span class="hljs-comment">$1</span>');
    return s;
  }
  return s;
}

export function renderTeamsCodeBlock(code, language) {
  const lines = String(code || '').replace(/\r\n?/g, '\n').split('\n');
  // Join with '' (no newline): each line is a block-level grid row, so a literal
  // newline inside the <pre> would render as an extra blank line and double the
  // vertical spacing.
  const body = lines.map((line, index) => `<span class="teams-code-line"><span class="teams-code-line-number">${index + 1}</span><span>${highlightCodeForPreview(line, language) || '&nbsp;'}</span></span>`).join('');
  return `<div class="teams-code-card"><div class="teams-code-head"><small>${languageLabel(language)}</small><span aria-hidden="true">•••</span></div><pre><code>${body}</code></pre></div>`;
}

function wrapPreviewMappedBlock(block, html, sourceMap) {
  if (!sourceMap) return html;
  return `<div class="preview-map-block" data-src-start="${block.startLine}" data-src-end="${block.endLine}">${html}</div>`;
}

export function renderMarkdownForTeams(markdown, options = {}) {
  const sourceMap = !!options.sourceMap;
  const blocks = parseMarkdownBlocks(markdown).map(block => {
    if (block.type === 'code') {
      const lines = block.text.split('\n');
      const fence = (lines[0] || '').trim().match(/^```(.*)$/);
      const language = fence ? (fence[1] || '').trim() : '';
      const code = lines.slice(1, -1).join('\n');
      return wrapPreviewMappedBlock(block, renderTeamsCodeBlock(code, language), sourceMap);
    }
    const html = convertMarkdownToTeamsHtml(block.text).replace(/<p>&nbsp;<\/p>/g, '<p class="teams-blank">&nbsp;</p>');
    return wrapPreviewMappedBlock(block, html, sourceMap);
  });
  const body = blocks.join('');
  return `<div class="teams-sent-scroll"><div class="teams-sent-message"><div class="teams-content">${body}</div><div class="teams-sent-meta"><span class="teams-sent-time">Just now</span><span class="teams-sent-status" aria-hidden="true">✓</span></div></div></div>`;
}

export function getHtmlDocument(markdown, title) {
  const safeTitle = htmlEscape(String(title || 'document').replace(/\.md$/i, ''));
  return `<!doctype html>\n<html>\n<head>\n<meta charset="utf-8">\n<title>${safeTitle}</title>\n</head>\n<body>\n${renderMarkdown(markdown)}\n</body>\n</html>`;
}

// ---------------------------------------------------------------------------
// Safe, navigable HTML artifact.
// renderNavigableHtml turns Markdown into a single self-contained HTML document
// designed to be opened/shared as an artifact: it has a table of contents,
// slugged heading anchors, collapsible H2 sections (using <details>, no JS),
// GitHub-style callouts, responsive tables and print styles. It embeds all of
// its CSS and emits NO <script> tags and NO external resource references in its
// chrome, so it is safe to preview inside a sandboxed <iframe srcdoc>.
// ---------------------------------------------------------------------------

const CALLOUT_LABELS = {
  NOTE: 'Note',
  TIP: 'Tip',
  IMPORTANT: 'Important',
  WARNING: 'Warning',
  CAUTION: 'Caution'
};

// Turn arbitrary heading text into a URL-safe slug. When a `used` Set is given,
// duplicate slugs get a numeric suffix (-1, -2, ...) so every anchor is unique.
export function slugify(text, used) {
  const base = String(text || '')
    .toLowerCase()
    .replace(/<[^>]+>/g, '')
    .replace(/[`*_~]/g, '')
    .trim()
    .replace(/[^\w\s-]/g, '')
    .replace(/\s+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-+|-+$/g, '') || 'section';
  if (!used) return base;
  let slug = base;
  let n = 1;
  while (used.has(slug)) { slug = `${base}-${n}`; n += 1; }
  used.add(slug);
  return slug;
}

// Strip inline Markdown/HTML down to plain text for use in the table of
// contents (where we only want readable labels, not nested markup).
function inlineToText(text) {
  return renderInline(text).replace(/<[^>]+>/g, '');
}

// Parse Markdown into an ordered list of rendered blocks plus the heading
// outline (used to build the TOC and collapsible sections).
function renderNavigableBlocks(markdown, { softBreaks = false } = {}) {
  const lines = String(markdown || '').replace(/\r\n?/g, '\n').split('\n');
  const blocks = [];
  const headings = [];
  const usedSlugs = new Set();
  let i = 0;
  while (i < lines.length) {
    const raw = lines[i];
    const trimmed = raw.trim();
    if (!trimmed) { i += 1; continue; }

    const fence = trimmed.match(/^```\s*([^`]*)$/);
    if (fence) {
      const start = i;
      const lang = (fence[1] || '').trim();
      const code = [];
      i += 1;
      while (i < lines.length && !/^```\s*$/.test(lines[i].trim())) { code.push(lines[i]); i += 1; }
      if (i < lines.length) i += 1;
      blocks.push({ type: 'other', startLine: start, endLine: i - 1, html: `<pre><code class="language-${htmlEscape(lang || 'text')}">${highlightCode(code.join('\n'), lang)}</code></pre>` });
      continue;
    }

    const table = parseTable(lines, i);
    if (table) { blocks.push({ type: 'other', startLine: i, endLine: table.next - 1, html: `<div class="table-wrap">${table.html}</div>` }); i = table.next; continue; }

    const heading = trimmed.match(/^(#{1,6})\s+(.*)$/);
    if (heading) {
      const start = i;
      const level = heading[1].length;
      const text = heading[2];
      const slug = slugify(text, usedSlugs);
      headings.push({ level, slug, text });
      const html = `<h${level} id="${slug}"><a class="anchor" href="#${slug}" aria-hidden="true">#</a>${renderInline(text)}</h${level}>`;
      blocks.push({ type: 'heading', level, slug, startLine: start, endLine: i, html });
      i += 1;
      continue;
    }

    if (/^(---|\*\*\*|___)$/.test(trimmed)) { blocks.push({ type: 'other', startLine: i, endLine: i, html: '<hr>' }); i += 1; continue; }

    const quote = trimmed.match(/^>\s?(.*)$/);
    if (quote) {
      const start = i;
      const parts = [];
      while (i < lines.length) {
        const m = lines[i].trim().match(/^>\s?(.*)$/);
        if (!m) break;
        parts.push(m[1]);
        i += 1;
      }
      const callout = (parts[0] || '').match(/^\[!(NOTE|TIP|IMPORTANT|WARNING|CAUTION)\]\s*(.*)$/i);
      if (callout) {
        const type = callout[1].toUpperCase();
        const bodyLines = [];
        if (callout[2]) bodyLines.push(callout[2]);
        bodyLines.push(...parts.slice(1));
        const bodyHtml = bodyLines.length ? `<p>${bodyLines.map(renderInline).join('<br>')}</p>` : '';
        blocks.push({ type: 'other', startLine: start, endLine: i - 1, html: `<div class="callout callout-${type.toLowerCase()}"><p class="callout-title">${CALLOUT_LABELS[type]}</p>${bodyHtml}</div>` });
      } else {
        blocks.push({ type: 'other', startLine: start, endLine: i - 1, html: `<blockquote>${parts.map(renderInline).join('<br>')}</blockquote>` });
      }
      continue;
    }

    const ul = trimmed.match(/^[-*+]\s+(.*)$/);
    if (ul) {
      const start = i;
      const items = [];
      while (i < lines.length) {
        const m = lines[i].trim().match(/^[-*+]\s+(.*)$/);
        if (!m) break;
        items.push(m[1]);
        i += 1;
      }
      blocks.push({ type: 'other', startLine: start, endLine: i - 1, html: `<ul>${items.map(item => `<li>${renderInline(item)}</li>`).join('')}</ul>` });
      continue;
    }

    const ol = trimmed.match(/^\d+\.\s+(.*)$/);
    if (ol) {
      const start = i;
      const items = [];
      while (i < lines.length) {
        const m = lines[i].trim().match(/^\d+\.\s+(.*)$/);
        if (!m) break;
        items.push(m[1]);
        i += 1;
      }
      blocks.push({ type: 'other', startLine: start, endLine: i - 1, html: `<ol>${items.map(item => `<li>${renderInline(item)}</li>`).join('')}</ol>` });
      continue;
    }

    const start = i;
    const para = [];
    while (i < lines.length && lines[i].trim() && !/^(#{1,6}\s+|```|>\s?|[-*+]\s+|\d+\.\s+|---$|\*\*\*$|___$)/.test(lines[i].trim())) {
      if (parseTable(lines, i)) break;
      para.push(lines[i].trim());
      i += 1;
    }
    blocks.push({ type: 'other', startLine: start, endLine: i - 1, html: `<p>${softBreaks ? para.map(renderInline).join('<br>') : renderInline(para.join(' '))}</p>` });
  }
  return { blocks, headings };
}

// Build the table-of-contents nav from the heading outline (H1–H3 only, so the
// TOC stays scannable on long documents).
function buildToc(headings) {
  const items = headings
    .filter(h => h.level >= 1 && h.level <= 3)
    .map(h => `<li class="toc-l${h.level}"><a href="#${h.slug}">${inlineToText(h.text)}</a></li>`)
    .join('');
  if (!items) return '';
  return `<nav class="toc" aria-label="Table of contents"><p class="toc-title">Contents</p><ul>${items}</ul></nav>`;
}

// Assemble rendered blocks, wrapping each H2 (and everything until the next H2)
// in an open <details> element so readers can collapse sections without JS.
function assembleNavigable(blocks, options = {}) {
  const sourceMap = !!options.sourceMap;
  const out = [];
  let inSection = false;
  const closeSection = () => { if (inSection) { out.push('</div></details>'); inSection = false; } };
  const wrap = block => wrapPreviewMappedBlock(block, block.html, sourceMap);
  for (const block of blocks) {
    if (block.type === 'heading' && block.level === 2) {
      closeSection();
      out.push(`<details class="section" open><summary>${wrap(block)}</summary><div class="section-body">`);
      inSection = true;
      continue;
    }
    out.push(wrap(block));
  }
  closeSection();
  return out.join('\n');
}

const NAVIGABLE_CSS = `
:root { color-scheme: light dark; }
* { box-sizing: border-box; }
body { margin: 0; background: #f6f8fa; color: #1f2328; font: 16px/1.6 -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif; }
.doc { max-width: 920px; margin: 0 auto; padding: 32px 24px 80px; }
.doc-header { margin-bottom: 24px; }
.doc-title { margin: 0; font-size: 30px; line-height: 1.25; }
.toc { margin: 0 0 28px; padding: 16px 20px; background: #fff; border: 1px solid #d0d7de; border-radius: 12px; }
.toc-title { margin: 0 0 8px; font-size: 13px; font-weight: 700; text-transform: uppercase; letter-spacing: .04em; color: #57606a; }
.toc ul { list-style: none; margin: 0; padding: 0; }
.toc li { margin: 2px 0; }
.toc a { color: #0969da; text-decoration: none; }
.toc a:hover { text-decoration: underline; }
.toc-l2 { padding-left: 14px; }
.toc-l3 { padding-left: 28px; font-size: 14px; }
.doc-body { background: #fff; border: 1px solid #d0d7de; border-radius: 12px; padding: 28px 32px; }
h1, h2, h3, h4, h5, h6 { line-height: 1.3; margin: 1.4em 0 .6em; scroll-margin-top: 16px; }
h1 { font-size: 26px; } h2 { font-size: 22px; } h3 { font-size: 18px; }
.doc-body > :first-child, .section-body > :first-child, summary > :first-child { margin-top: 0; }
.anchor { float: left; margin-left: -20px; padding-right: 4px; color: #afb8c1; text-decoration: none; opacity: 0; }
h1:hover .anchor, h2:hover .anchor, h3:hover .anchor, h4:hover .anchor { opacity: 1; }
a { color: #0969da; }
p { margin: 0 0 1em; }
code { background: rgba(175,184,193,.2); padding: .15em .4em; border-radius: 6px; font: .88em/1.5 "SFMono-Regular", Consolas, "Liberation Mono", Menlo, monospace; }
pre { background: #0d1117; color: #e6edf3; padding: 16px; border-radius: 10px; overflow: auto; }
pre code { background: transparent; padding: 0; color: inherit; }
blockquote { margin: 0 0 1em; padding: .2em 1em; color: #57606a; border-left: 4px solid #d0d7de; }
.table-wrap { overflow-x: auto; margin: 0 0 1em; }
table { border-collapse: collapse; width: 100%; }
th, td { border: 1px solid #d0d7de; padding: 8px 12px; text-align: left; }
th { background: #f6f8fa; }
hr { border: 0; border-top: 1px solid #d0d7de; margin: 24px 0; }
img { max-width: 100%; height: auto; }
.callout { margin: 0 0 1em; padding: 12px 16px; border-radius: 8px; border-left: 4px solid #57606a; background: #f6f8fa; }
.callout p { margin: 0; }
.callout p + p { margin-top: .4em; }
.callout-title { font-weight: 700; margin-bottom: .3em !important; }
.callout-note { border-left-color: #0969da; background: #ddf4ff; }
.callout-tip { border-left-color: #1a7f37; background: #dafbe1; }
.callout-important { border-left-color: #8250df; background: #fbefff; }
.callout-warning { border-left-color: #9a6700; background: #fff8c5; }
.callout-caution { border-left-color: #cf222e; background: #ffebe9; }
details.section { margin: 0 0 .5em; }
details.section > summary { cursor: pointer; list-style: none; }
details.section > summary::-webkit-details-marker { display: none; }
details.section > summary { position: relative; }
details.section > summary::before { content: "\\25B8"; position: absolute; left: -18px; top: .55em; color: #8c959f; transition: transform .15s ease; }
details.section[open] > summary::before { transform: rotate(90deg); }
.section-body { padding-left: 2px; }
.empty { color: #8c959f; }
@media (prefers-color-scheme: dark) {
  body { background: #0d1117; color: #e6edf3; }
  .toc, .doc-body { background: #161b22; border-color: #30363d; }
  th { background: #21262d; }
  .toc-title, blockquote { color: #8b949e; }
  th, td, hr, blockquote { border-color: #30363d; }
  .callout { background: #161b22; }
}
@media print {
  body { background: #fff; }
  .doc { max-width: none; padding: 0; }
  .toc, .doc-body { border: 0; }
  details.section > summary::before { display: none; }
  details.section[open] > summary { cursor: default; }
  a { color: inherit; text-decoration: none; }
}
`.trim();

// Render a complete, safe, navigable HTML artifact from Markdown.
export function renderNavigableHtml(markdown, options = {}) {
  const title = htmlEscape(String(options.title || 'Document').replace(/\.md$/i, '') || 'Document');
  const { blocks, headings } = renderNavigableBlocks(markdown, { softBreaks: options.softBreaks || false });
  const toc = buildToc(headings);
  const body = assembleNavigable(blocks, { sourceMap: !!options.sourceMap }) || '<p class="empty">No content yet — start writing Markdown.</p>';
  return `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>${title}</title>
<style>${NAVIGABLE_CSS}</style>
</head>
<body>
<main class="doc">
<header class="doc-header"><h1 class="doc-title">${title}</h1></header>
${toc}
<article class="doc-body">
${body}
</article>
</main>
</body>
</html>`;
}

// Wrap rendered HTML in a clipboard fragment. The StartFragment/EndFragment
// markers are what let rich targets (Microsoft Teams, Outlook, Word) recognise
// and paste the HTML with formatting instead of falling back to plain text.
export function buildClipboardHtmlFragment(html) {
  return `<!doctype html><html><head><meta charset="utf-8"></head><body><!--StartFragment-->${html || ''}<!--EndFragment--></body></html>`;
}

// Derive a readable plain-text fallback from rendered HTML (used as the
// text/plain clipboard flavour so non-rich targets still get clean text
// instead of raw Markdown).
export function htmlToPlainText(html) {
  return String(html || '')
    .replace(/<br\s*\/?\s*>/gi, '\n')
    .replace(/<li[^>]*>/gi, '\u2022 ')
    .replace(/<\/(p|h[1-6]|li|blockquote|pre|tr|div|ul|ol)>/gi, '\n')
    .replace(/<[^>]+>/g, '')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&nbsp;/g, ' ')
    .replace(/&amp;/g, '&')
    .replace(/[ \t]+\n/g, '\n')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
}

// Build the rich payload copied for the Teams output: a fragment-wrapped HTML
// flavour plus a clean plain-text fallback, both derived from the rendered
// message body (not the composer chrome).
export function buildTeamsClipboard(markdown) {
  const body = convertMarkdownToTeamsHtml(markdown);
  return { body, html: buildClipboardHtmlFragment(body), text: htmlToPlainText(body) };
}

// --- Teams-friendly HTML for the clipboard -------------------------------
// Microsoft Teams' compose box is a constrained contenteditable that ignores
// tags such as <h1>/<h2>/<em> and falls back to plain text when it cannot map
// the pasted markup. To paste with formatting we must emit the same paragraph
// based structure Teams produces itself: headings become styled paragraphs,
// emphasis uses <i>/<strong>/<s>, quotes wrap a <p>, tables use <figure> and
// code blocks use Skype's CodeBlockEditor marker + <pre>.

function stripHtml(md) {
  return String(md)
    .replace(/<script[\s\S]*?<\/script>/gi, '')
    .replace(/<style[\s\S]*?<\/style>/gi, '')
    // Only strip actual HTML tags (tag name must start with a letter or '/').
    // Bare comparisons like "< 20" or "x > y" are left untouched so they
    // are not misread as tag delimiters.
    .replace(/<\/?[a-z][^>]*>/gi, '');
}

export function inlineTeamsHtml(text) {
  const { replaced, stash } = protectCodeSpans(String(text || ''));
  let s = htmlEscape(replaced);
  s = s.replace(/!\[([^\]]*)\]\(([^)]+)\)/g, (_, alt, url) => `<img src="${htmlEscape(url)}" alt="${htmlEscape(alt)}" />`);
  s = s.replace(/\[([^\]]+)\]\(([^)]+)\)/g, (_, label, url) => `<a href="${htmlEscape(url)}">${htmlEscape(label)}</a>`);
  s = s.replace(/\*\*([^*\n]+)\*\*/g, '<strong>$1</strong>');
  s = s.replace(/__([^_\n]+)__/g, '<strong>$1</strong>');
  s = s.replace(/(?<!\*)\*([^*\n]+)\*(?!\*)/g, '<i>$1</i>');
  s = s.replace(/(?<!_)_([^_\n]+)_(?!_)/g, '<i>$1</i>');
  s = s.replace(/~~([^~\n]+)~~/g, '<s>$1</s>');
  s = s.replace(/@@CODE(\d+)@@/g, (_, n) => `<code>${htmlEscape(stash[Number(n)] || '')}</code>`);
  return s;
}

function escapeCodeText(code) {
  return htmlEscape(String(code || '').replace(/\r\n?/g, '\n')).replace(/ /g, '&nbsp;').replace(/\n/g, '<br>');
}

let teamsBlockSeq = 0;
function teamsCodeBlock(code, lang) {
  const language = String(lang || 'text').toLowerCase().replace(/[^a-z0-9_-]/g, '') || 'text';
  const blockId = `codeBlockEditor-${Date.now().toString(36)}-${(teamsBlockSeq++).toString(36)}`;
  const highlighted = escapeCodeText(code) || '&nbsp;';
  return `<p itemtype="http://schema.skype.com/CodeBlockEditor" id="${blockId}">&nbsp;</p><pre class="language-${language} skipProofing" itemid="${blockId}" spellcheck="false" corrected="true"><code>${highlighted}</code></pre>`;
}

export function convertMarkdownToTeamsHtml(markdown) {
  const md = stripHtml(String(markdown || '').replace(/\r\n?/g, '\n'));
  const lines = md.split('\n');
  const out = [];
  let i = 0;
  while (i < lines.length) {
    const line = lines[i].replace(/\s+$/, '');
    const trimmed = line.trim();
    if (!trimmed) { out.push('<p>&nbsp;</p>'); i += 1; continue; }
    if (/^(?:-{3,}|\*{3,}|_{3,})$/.test(trimmed)) { out.push('<p>──────────</p>'); i += 1; continue; }

    const fence = trimmed.match(/^```(.*)$/);
    if (fence) {
      const lang = (fence[1] || '').trim();
      const codeLines = [];
      i += 1;
      while (i < lines.length && !/^```\s*$/.test(lines[i].trim())) { codeLines.push(lines[i].replace(/\s+$/, '')); i += 1; }
      if (i < lines.length && /^```\s*$/.test(lines[i].trim())) i += 1;
      out.push(teamsCodeBlock(codeLines.join('\n'), lang));
      continue;
    }

    if (/\|/.test(line) && i + 1 < lines.length && /^\s*\|?\s*:?-{3,}:?\s*(\|\s*:?-{3,}:?\s*)+\|?\s*$/.test(lines[i + 1])) {
      const header = line.replace(/^\s*\|?|\|?\s*$/g, '').split('|').map(s => s.trim());
      let j = i + 2;
      const rows = [];
      while (j < lines.length && /\|/.test(lines[j]) && lines[j].trim()) {
        rows.push(lines[j].replace(/^\s*\|?|\|?\s*$/g, '').split('|').map(s => s.trim()));
        j += 1;
      }
      const body = [header, ...rows]
        .map(row => `<tr>${row.map(cell => `<td><p data-is-tablecell-container="true">${inlineTeamsHtml(cell)}</p></td>`).join('')}</tr>`)
        .join('');
      out.push(`<figure class="table"><table><tbody>${body}</tbody></table></figure>`);
      i = j;
      continue;
    }

    const heading = trimmed.match(/^(#{1,6})\s+(.*)$/);
    if (heading) {
      const level = Math.min(6, heading[1].length);
      const inner = inlineTeamsHtml(heading[2].trim());
      out.push(level === 1
        ? `<p><span style="font-size:x-large;">${inner}</span></p>`
        : `<p><strong>${inner}</strong></p>`);
      i += 1;
      continue;
    }

    const quote = trimmed.match(/^>\s?(.*)$/);
    if (quote) {
      const quoteParts = [];
      while (i < lines.length) {
        const m = lines[i].trim().match(/^>\s?(.*)$/);
        if (!m) break;
        quoteParts.push(inlineTeamsHtml(m[1].trim()));
        i += 1;
      }
      out.push(`<blockquote spellcheck="false"><p>${quoteParts.join('<br>')}</p></blockquote>`);
      continue;
    }

    const ul = trimmed.match(/^[-*+]\s+(.*)$/);
    if (ul) {
      const items = [];
      while (i < lines.length) { const m = lines[i].trim().match(/^[-*+]\s+(.*)$/); if (!m) break; items.push(m[1].trim()); i += 1; }
      out.push(`<ul>${items.map(item => `<li>${inlineTeamsHtml(item)}</li>`).join('')}</ul>`);
      continue;
    }

    const ol = trimmed.match(/^\d+\.\s+(.*)$/);
    if (ol) {
      const items = [];
      while (i < lines.length) { const m = lines[i].trim().match(/^\d+\.\s+(.*)$/); if (!m) break; items.push(m[1].trim()); i += 1; }
      out.push(`<ol>${items.map(item => `<li>${inlineTeamsHtml(item)}</li>`).join('')}</ol>`);
      continue;
    }

    out.push(`<p>${inlineTeamsHtml(line)}</p>`);
    i += 1;
  }
  return out.join('');
}


// ---------------------------------------------------------------------------
// Rich clipboard -> Markdown (the reverse direction: paste from Teams).
// Converts the HTML flavour copied from Teams/email/web pages into clean
// Markdown so it can be dropped straight into the editor. Image elements are
// preserved as Markdown images (data URIs are kept as-is for offline use).
// ---------------------------------------------------------------------------

const NODE_TYPE_TEXT = 3;
const NODE_TYPE_ELEMENT = 1;

function parseInlineStyle(style) {
  const out = {};
  String(style || '').split(';').forEach(part => {
    const idx = part.indexOf(':');
    if (idx < 0) return;
    const key = part.slice(0, idx).trim().toLowerCase();
    const value = part.slice(idx + 1).trim().toLowerCase();
    if (key) out[key] = value;
  });
  return out;
}

function sanitizeClipboardHtml(html) {
  return String(html || '')
    .replace(/<\?xml[\s\S]*?\?>/gi, '')
    .replace(/<script\b[\s\S]*?<\/script>/gi, '')
    .replace(/<style\b[\s\S]*?<\/style>/gi, '')
    .replace(/<!--([\s\S]*?)-->/g, '')
    .replace(/<!\[if[\s\S]*?<!\[endif\]>/gi, '')
    .replace(/<\/?o:p\b[^>]*>/gi, '');
}

function isWordHtml(html) {
  const text = String(html || '');
  return /\bMso[a-zA-Z0-9_-]*\b/.test(text)
    || /\bmso-[a-z-]+:/i.test(text)
    || /\bxmlns:(?:o|w|m)=/i.test(text)
    || /Microsoft Word/i.test(text)
    || /<\/?o:p\b/i.test(text);
}

function isJiraHtml(html) {
  const text = String(html || '');
  return /\bdata-node-type\b/i.test(text)
    || /\bdata-layout\b/i.test(text)
    || /\bak-renderer\b/i.test(text)
    || /\bAtlassian\b/i.test(text)
    || /\bac:[a-z-]+\b/i.test(text)
    || /\bri:[a-z-]+\b/i.test(text);
}

function looksLikeJiraWiki(text) {
  const value = String(text || '').replace(/\r\n?/g, '\n');
  if (!value.trim()) return false;
  return /^\s*h[1-6]\.\s+\S+/m.test(value)
    || /^\s*bq\.\s+\S+/m.test(value)
    || /^\s*\|\|.*\|\|\s*$/m.test(value)
    // {code}, {code:language=xml}, {code:xml} (shorthand without language= prefix)
    || /\{code(?::[^}]*)?\}/.test(value)
    // {noformat} preformatted blocks
    || /\{noformat\}/.test(value)
    || /\[[^\]\n]+\|[^\]\n]+\]/.test(value)
    || /!https?:\/\/[^!\s]+!/.test(value)
    || /!data:[^!\s]+!/.test(value)
    || /\{\{[^}\n]+\}\}/.test(value)
    || /\\\\/.test(value);
}

function detectWordHeadingLevel(node, innerText) {
  const className = String(node.getAttribute('class') || '');
  const classMatch = className.match(/\bMsoHeading([1-6])\b/i) || className.match(/\bHeading([1-6])\b/i);
  if (classMatch) return Number(classMatch[1]);
  if (/\bMsoTitle\b/i.test(className)) return 1;
  if (/\bMsoSubtitle\b/i.test(className)) return 2;

  const style = parseInlineStyle(node.getAttribute('style') || '');
  const outline = style['mso-outline-level'] || '';
  const outlineMatch = outline.match(/(\d+)/);
  if (outlineMatch) return Math.min(6, Math.max(1, Number(outlineMatch[1]) + 1));

  const text = String(innerText || '').replace(/\s+/g, ' ').trim();
  if (!text || text.length > 180) return 0;

  const candidates = [node, ...Array.from(node.querySelectorAll('span, font'))];
  for (const el of candidates) {
    const styles = parseInlineStyle(el.getAttribute('style') || '');
    const fontSize = parseFloat(String(styles['font-size'] || '').replace(/[^\d.]/g, ''));
    const weight = styles['font-weight'] || styles['mso-bidi-font-weight'] || '';
    if (!Number.isFinite(fontSize)) continue;
    if (!/(bold|[7-9]00)/.test(weight)) continue;
    if (fontSize >= 28) return 1;
    if (fontSize >= 24) return 2;
    if (fontSize >= 20) return 3;
    if (fontSize >= 18) return 4;
    if (fontSize >= 16) return 5;
  }
  return 0;
}

function detectWordListInfo(node, innerText) {
  const className = String(node.getAttribute('class') || '');
  const style = parseInlineStyle(node.getAttribute('style') || '');
  if (!/MsoListParagraph/i.test(className) && !String(node.getAttribute('style') || '').match(/mso-list:/i)) return null;
  const levelMatch = String(node.getAttribute('style') || '').match(/level(\d+)/i);
  const level = Math.max(1, Number(levelMatch ? levelMatch[1] : 1) || 1);
  const clean = String(innerText || '').replace(/\u00a0/g, ' ').trim();
  const ordered = clean.match(/^((?:\(?\d+[\.)])|(?:[a-zA-Z][\.)])|(?:[ivxlcdm]+[\.)]))\s+(.*)$/i);
  if (ordered) return { level, marker: '1.', text: ordered[2].trim() };
  const bullet = clean.match(/^([•◦▪·‣–—*+]+)\s+(.*)$/);
  if (bullet) return { level, marker: '-', text: bullet[2].trim() };
  const styleWeight = style['font-weight'] || style['mso-bidi-font-weight'] || '';
  if (/mso-list/i.test(String(node.getAttribute('style') || '')) || /bold|[7-9]00/.test(styleWeight)) {
    return { level, marker: '-', text: clean.replace(/^([•◦▪·‣–—*+]+)\s*/, '').trim() };
  }
  return { level, marker: '-', text: clean };
}

function htmlNodeToMarkdown(node, source = 'generic') {
  if (node.nodeType === NODE_TYPE_TEXT) return node.textContent.replace(/\s+/g, ' ');
  if (node.nodeType !== NODE_TYPE_ELEMENT) return '';
  const tag = node.tagName.toLowerCase();
  const child = () => Array.from(node.childNodes).map(childNode => htmlNodeToMarkdown(childNode, source)).join('').trim();
  if (tag === 'br') return '\n';
  if (tag === 'strong' || tag === 'b') return `**${child()}**`;
  if (tag === 'em' || tag === 'i') return `*${child()}*`;
  if (tag === 's' || tag === 'strike' || tag === 'del') return `~~${child()}~~`;
  if (tag === 'code') return '`' + child().replace(/`/g, '\\`') + '`';
  if (source === 'jira') {
    if (tag === 'ri:url') return node.getAttribute('ri:value') || node.getAttribute('value') || child();
    if (tag === 'ri:page') return node.getAttribute('ri:content-title') || node.getAttribute('content-title') || child();
    if (tag === 'ri:issue') return node.getAttribute('ri:key') || node.getAttribute('key') || child();
    if (tag === 'ri:user') return node.getAttribute('ri:account-id') || node.getAttribute('account-id') || child();
    if (tag.startsWith('ac:') || tag.startsWith('ri:')) return child();
  }
  if (tag === 'pre') {
    const codeEl = node.querySelector('code');
    const classes = [...Array.from(node.classList || []), ...Array.from((codeEl && codeEl.classList) || [])];
    const langClass = classes.find(c => c.startsWith('language-')) || classes.find(c => /^code-[a-z0-9_-]+$/i.test(c)) || '';
    const lang = langClass.replace(/^(language-|code-)/i, '') || 'text';
    return '\n```' + lang + '\n' + node.textContent.replace(/^\n+|\n+$/g, '') + '\n```\n\n';
  }
  if (tag === 'img') {
    const itemtype = node.getAttribute('itemtype') || '';
    const alt = (node.getAttribute('alt') || node.getAttribute('title') || '').trim();
    // Teams emoji are <img itemtype=".../Emoji" alt="🕺"> from a public CDN.
    // Represent them by their unicode alt text, not as a Markdown image.
    if (/schema\.skype\.com\/Emoji/i.test(itemtype)) return alt;
    // Picture messages (AMSImage) are inlined as a data: URI in `src` when the
    // copy happens from within the message. Prefer whichever attribute carries
    // the usable data URI; ignore `blob:` references (only valid inside Teams).
    const src = node.getAttribute('src') || '';
    const targetSrc = node.getAttribute('target-src') || '';
    const usable = [src, targetSrc].find(u => u && !u.startsWith('blob:'));
    if (!usable) return alt ? `*${alt}*` : '';
    // Images are block-level elements in Teams messages; add a blank-line
    // separator so following text is not concatenated onto the same line.
    // Any parent <p> / <div> that calls child().trim() neutralises the extra
    // newlines in purely inline contexts, so existing tests are unaffected.
    return `![${alt}](${usable})\n\n`;
  }
  if (tag === 'a') {
    // Skip decorative anchor links — two common patterns:
    // 1. aria-hidden="true": Markup Forge / generic sites inject `<a aria-hidden="true">#</a>`
    //    next to headings as a copy-link affordance visible only on hover.
    // 2. class="anchor" with aria-label="Permalink: …": GitHub's heading anchors contain
    //    an invisible SVG icon; converting them produces noise like
    //    "## [https://github.com/…#section](https://github.com/…#section)Title".
    if (node.getAttribute('aria-hidden') === 'true') return '';
    if (node.classList.contains('anchor') &&
        (node.getAttribute('aria-label') || '').startsWith('Permalink')) return '';
    const href = node.getAttribute('href') || '';
    const text = child() || href;
    return href ? `[${text}](${href})` : text;
  }
  // <details> is a collapsible section: treat it as a block container so
  // sibling sections are separated. <summary> is the visible heading/title
  // row — pass its children through transparently.
  // IMPORTANT: do NOT use child() here — its .trim() strips the structural
  // \n\n that heading/block handlers append, causing heading+content to
  // merge onto the same line ("## TitleParagraph" instead of "## Title\n\nParagraph").
  if (tag === 'details') {
    const raw = Array.from(node.childNodes).map(childNode => htmlNodeToMarkdown(childNode, source)).join('');
    const inner = normalizeMarkdownWhitespace(raw);
    return inner ? inner + '\n\n' : '';
  }
  if (tag === 'summary') {
    return Array.from(node.childNodes).map(childNode => htmlNodeToMarkdown(childNode, source)).join('');
  }
  if (/^h[1-6]$/.test(tag)) return '\n' + '#'.repeat(Number(tag[1])) + ' ' + child() + '\n\n';
  if (tag === 'p' || tag === 'div') {
    if (source === 'word') {
      const headingLevel = detectWordHeadingLevel(node, child());
      if (headingLevel) return `\n${'#'.repeat(headingLevel)} ${child().replace(/^\s+|\s+$/g, '')}\n\n`;
      const listInfo = detectWordListInfo(node, child());
      if (listInfo) return `${'  '.repeat(listInfo.level - 1)}${listInfo.marker} ${listInfo.text}\n`;
    }
    // Normalise whitespace injected by "<br> text" combos, but preserve fenced
    // code indentation when Jira wraps <pre> blocks in generic div containers.
    const inner = normalizeMarkdownWhitespace(child());
    return inner ? inner + '\n\n' : '';
  }
  if (tag === 'blockquote') {
    const itemtype = node.getAttribute('itemtype') || '';
    // Teams "reply" quotes embed the original author (itemprop="mri") and a
    // preview of the quoted message (itemprop="preview").
    if (/schema\.skype\.com\/Reply/i.test(itemtype)) {
      const author = node.querySelector('[itemprop="mri"]');
      const preview = node.querySelector('[itemprop="preview"]');
      const quoteLines = [];
      if (author && author.textContent.trim()) quoteLines.push('**' + author.textContent.trim() + '**');
      if (preview && preview.textContent.trim()) quoteLines.push(preview.textContent.trim());
      if (!quoteLines.length) return '';
      return quoteLines.map(l => '> ' + l).join('\n') + '\n\n';
    }
    return child().split('\n').map(l => (l.trim() ? '> ' + l.trim() : '>')).join('\n') + '\n\n';
  }
  if (tag === 'li') return '- ' + child() + '\n';
  if (tag === 'ul' || tag === 'ol') {
    return '\n' + Array.from(node.children).map((li, idx) => (
      tag === 'ol'
        ? `${idx + 1}. ${Array.from(li.childNodes).map(htmlNodeToMarkdown).join('').trim()}\n`
        : htmlNodeToMarkdown(li)
    )).join('') + '\n';
  }
  if (tag === 'table') {
    const allRows = Array.from(node.querySelectorAll('tr'));
    // Capture column alignment from the first row's cell align attributes.
    const firstRowCells = allRows.length > 0 ? Array.from(allRows[0].children) : [];
    const rows = allRows.map(tr =>
      Array.from(tr.children).map(td => {
        // Recurse into the cell so nested elements (e.g. <br>, <strong>) are
        // handled correctly. <br> elements become literal <br> tags (valid in GFM
        // table cells) rather than commas, preserving intentional line breaks such
        // as multi-author lists.
        const inner = Array.from(td.childNodes).map(htmlNodeToMarkdown).join('').trim();
        return inner
          .replace(/\n+/g, '<br>')         // newlines → HTML line-break (valid in GFM cells)
          .replace(/(<br>)+/g, '<br>')      // collapse consecutive breaks
          .replace(/^<br>|<br>$/g, '')      // strip leading/trailing breaks
          .trim();
      })
    );
    if (!rows.length) return '';
    const header = rows[0];
    const separator = header.map((_, i) => {
      const align = firstRowCells[i]?.getAttribute('align') || '';
      if (align === 'center') return ':---:';
      if (align === 'right') return '---:';
      if (align === 'left') return ':---';
      return '---';
    });
    const body = rows.slice(1);
    return '\n' + [header, separator, ...body].map(row => `| ${row.join(' | ')} |`).join('\n') + '\n\n';
  }
  // <markdown-accessiblity-table> is a GitHub custom element that wraps <table>
  // for accessibility. Treat it as a transparent block container so the table
  // inside keeps its trailing \n\n and does not run into the next heading.
  if (tag === 'markdown-accessiblity-table') {
    const inner = child();
    return inner ? inner + '\n\n' : '';
  }
  // <span> is Teams' structural wrapper (one per message) as well as an inline
  // element. Concatenate children WITHOUT trimming so paragraph breaks between
  // stacked messages survive; the final cleanup collapses excess blank lines.
  if (source === 'word' && (tag === 'span' || tag === 'font')) {
    const styles = parseInlineStyle(node.getAttribute('style') || '');
    let inner = Array.from(node.childNodes).map(childNode => htmlNodeToMarkdown(childNode, source)).join('').trim();
    const bold = /bold|[7-9]00/.test((styles['font-weight'] || '') + ' ' + (styles['mso-bidi-font-weight'] || ''));
    const italic = /italic|oblique/.test((styles['font-style'] || '') + ' ' + (styles['mso-bidi-font-style'] || ''));
    const strike = /line-through/.test((styles['text-decoration'] || '') + ' ' + (styles['text-decoration-line'] || ''));
    if (strike) inner = `~~${inner}~~`;
    if (italic) inner = `*${inner}*`;
    if (bold) inner = `**${inner}**`;
    return inner;
  }
  if (tag === 'span') return Array.from(node.childNodes).map(htmlNodeToMarkdown).join('');
  return child();
}

// Parse an HTML string into a DOM body using whichever DOMParser is available.
// `domParser` may be either a DOMParser *constructor* (e.g. jsdom's, injected in
// tests) or an already-constructed *instance*; both are accepted so callers
// can't accidentally break this by passing `new DOMParser()`.
function parseHtmlBody(html, domParser) {
  let parser = null;
  if (domParser && typeof domParser.parseFromString === 'function') {
    // An instance was passed (it already exposes parseFromString).
    parser = domParser;
  } else {
    const Parser = domParser || (typeof DOMParser !== 'undefined' ? DOMParser : null);
    if (!Parser) throw new Error('No DOMParser available to convert HTML to Markdown');
    parser = new Parser();
  }
  const doc = parser.parseFromString(html, 'text/html');
  return doc.body;
}

// Preserve whitespace inside fenced code blocks while still cleaning up soft
// break noise around normal paragraphs and quotes.
function normalizeMarkdownWhitespace(text) {
  return String(text || '')
    .split(/(```[\s\S]*?```)/g)
    .map(part => {
      if (part.startsWith('```')) return part;
      return part
        .replace(/\u00a0/g, ' ')
        .replace(/\n[ \t]+/g, '\n')
        .replace(/[ \t]+\n/g, '\n')
        .replace(/(^|\n)[ \t]+>/g, '$1>')
        .replace(/\n{3,}/g, '\n\n');
    })
    .join('');
}

// Convert rich clipboard HTML into Markdown. Falls back to the plain-text
// flavour when no HTML is present. `domParser` lets tests inject jsdom's parser.
export function convertHtmlToMarkdown(html, plainFallback = '', domParser, source = 'auto') {
  if (!html) return plainFallback || '';
  const sanitized = sanitizeClipboardHtml(html);
  const detectedSource = source === 'auto'
    ? (isWordHtml(sanitized) ? 'word' : isJiraHtml(sanitized) ? 'jira' : 'generic')
    : source;
  const body = parseHtmlBody(sanitized, domParser);
  return normalizeMarkdownWhitespace(Array.from(body.childNodes)
    .map(node => htmlNodeToMarkdown(node, detectedSource))
    .join(''))
    .trim();
}

function convertJiraWikiToMarkdown(text, domParser) {
  return convertHtmlToMarkdown(jiraToHtml(String(text || '')), '', domParser, 'jira');
}

// ---------------------------------------------------------------------------
// Smart paste: turn raw clipboard text into clean Markdown.
// These helpers detect the *kind* of plain text on the clipboard (a spreadsheet
// selection, a snippet of code, or messy prose) and convert each to tidy
// Markdown. smartPasteToMarkdown is the orchestrator the UI calls.
// ---------------------------------------------------------------------------

// Convert a tab-separated block (e.g. a copy from Excel/Sheets) into a
// GitHub-flavoured Markdown table. Returns null when the text is not tabular.
export function tsvToMarkdownTable(text) {
  const raw = String(text || '').replace(/\r\n?/g, '\n').replace(/\n+$/, '');
  if (!raw || !/\t/.test(raw)) return null;

  // Build rows while handling multiline cells (Teams/Excel copy-paste often
  // puts multiple values in one cell on separate lines, with the tab structure
  // only appearing on the first – or last – line of each logical row):
  //   • Line with NO tabs:           continuation of previous row's last
  //                                  non-empty cell, appended with ", ".
  //   • Line with tabs, ALL cells
  //     empty/whitespace:            row-end separator, discarded.
  //   • Line with tabs, exactly ONE
  //     non-empty cell (+ trailing
  //     whitespace cells):           last value of previous row's last cell,
  //                                  appended with ", ", then the row ends.
  //   • Line with tabs, ≥2 non-empty
  //     cells:                       new row.
  const rows = [];
  let current = null; // cells array for the row currently being assembled

  const lastNonEmptyIdx = arr => {
    for (let i = arr.length - 1; i >= 0; i--) {
      if (arr[i] && arr[i].trim()) return i;
    }
    return -1;
  };

  for (const line of raw.split('\n')) {
    if (!line.trim()) continue;

    if (!line.includes('\t')) {
      // No-tab line: continuation of the last non-empty cell in current row.
      if (current !== null) {
        const idx = lastNonEmptyIdx(current);
        if (idx >= 0) current[idx] += ', ' + line.trim();
      }
      continue;
    }

    // Tab-containing line.
    const cells = line.split('\t');
    const trimmed = cells.map(c => c.trim());
    const nonEmpty = trimmed.filter(Boolean);

    if (nonEmpty.length === 0) {
      // All-empty row: row-end separator. Close current row.
      current = null;
      continue;
    }

    if (nonEmpty.length === 1 && current !== null) {
      // Single non-empty cell with trailing tabs: last continuation value
      // plus implicit row end.
      const idx = lastNonEmptyIdx(current);
      if (idx >= 0) current[idx] += ', ' + nonEmpty[0];
      current = null; // row is complete
      continue;
    }

    // New logical row (≥2 non-empty cells, or the very first line).
    current = trimmed;
    rows.push(current);
  }

  if (rows.length < 1) return null;
  const cols = Math.max(...rows.map(r => r.length));
  if (cols < 2) return null;
  const norm = rows.map(r => {
    const cells = r.map(c => (c || '').replace(/\|/g, '\\|'));
    while (cells.length < cols) cells.push('');
    return cells;
  });
  const fmt = row => `| ${row.join(' | ')} |`;
  const header = norm[0];
  const separator = header.map(() => '---');
  return [fmt(header), fmt(separator), ...norm.slice(1).map(fmt)].join('\n');
}

// Best-effort language guess for a code snippet (used to tag fenced blocks).
export function guessCodeLanguage(text) {
  const t = String(text || '').trim();
  if (!t) return '';
  if (/^[[{]/.test(t) && /["']?[\w-]+["']?\s*:/.test(t)) return 'json';
  if (/\b(SELECT|INSERT\s+INTO|UPDATE|DELETE\s+FROM|CREATE\s+TABLE)\b/i.test(t) && /\b(FROM|WHERE|VALUES|SET|JOIN)\b/i.test(t)) return 'sql';
  if (/^\s*<[a-zA-Z!?]/.test(t) && /<\/?[a-zA-Z][\s\S]*>/.test(t)) return 'xml';
  if (/\b(function|const|let|var|=>|console\.log|document\.|window\.)\b/.test(t)) return 'javascript';
  if (/\b(public|private|protected)\s+(class|static|void)|\bSystem\.out\.|import\s+java\./.test(t)) return 'java';
  if (/(^|\n)\s*(#!\/|\$\s|sudo\s|apt(-get)?\s|npm\s|yarn\s|git\s|cd\s|ls\s|echo\s|curl\s|docker\s)/.test(t)) return 'bash';
  if (/^\s*(def|class)\s+\w+|\bprint\(|\bimport\s+\w+/.test(t)) return 'python';
  return '';
}

// Heuristic: does this plain text look like source code (rather than prose)?
export function looksLikeCode(text) {
  const t = String(text || '');
  if (!t.trim()) return false;
  const lines = t.split('\n');
  let score = 0;
  if (guessCodeLanguage(t)) score += 2;
  if (/[;{}]|=>|\bfunction\b|\bclass\b/.test(t)) score += 1;
  if (/^[ \t]{2,}\S/m.test(t)) score += 1;
  const codey = lines.filter(l => /[{};=<>()]|=>|^\s{2,}\S/.test(l)).length;
  if (lines.length > 1 && codey / lines.length > 0.5) score += 1;
  // Prose-like punctuation density lowers confidence.
  if (/[.!?]\s+[A-Z]/.test(t) && score < 3) score -= 1;
  return score >= 3;
}

// Wrap text that looks like code in a fenced block with a guessed language.
// Returns null when the text does not look like code.
export function detectAndWrapCode(text) {
  if (!looksLikeCode(text)) return null;
  const lang = guessCodeLanguage(text);
  const body = String(text || '').replace(/\r\n?/g, '\n').replace(/\n+$/, '');
  return '```' + lang + '\n' + body + '\n```';
}

// Tidy messy prose into readable Markdown: normalise line endings, strip
// trailing whitespace, convert unicode bullets to "- ", normalise "1)" to
// "1." ordered markers and collapse runs of blank lines.
export function cleanPlainTextToMarkdown(text) {
  return String(text || '')
    .replace(/\r\n?/g, '\n')
    .replace(/[ \t]+\n/g, '\n')
    .replace(/^[ \t]*[•◦▪·‣–—*]\s+/gm, '- ')
    .replace(/^([ \t]*)(\d+)\)\s+/gm, '$1$2. ')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
}

// Detect a block that is already a well-formed GFM table (so we pass it
// through unchanged rather than re-converting it).
function isMarkdownTable(block) {
  const lines = block.split('\n');
  return lines.length >= 2
    && /^\|.+\|/.test(lines[0])
    && /^\|[\s|:-]+\|/.test(lines[1]);
}

// Convert a single plain-text block (no blank lines inside) to the best
// Markdown representation. Already-formatted GFM tables are returned as-is.
// Detect and convert a plain-text table where columns are separated by two or
// more consecutive spaces (common in terminal output and copied text tables).
// Returns true only when the column structure is consistent across all lines.
export function looksLikeSpaceTable(text) {
  const lines = String(text || '').trim().split('\n').map(l => l.trim()).filter(Boolean);
  if (lines.length < 2) return false;
  // Every line must contain at least one gap of 2+ spaces between non-space chars.
  if (!lines.every(l => /\S {2,}\S/.test(l))) return false;
  // Split each line by 2+ spaces and check column counts are consistent (±1).
  const colCounts = lines.map(l => l.split(/\s{2,}/).length);
  const max = Math.max(...colCounts);
  const min = Math.min(...colCounts);
  return max >= 2 && max - min <= 1;
}

export function spaceTableToMarkdown(text) {
  if (!looksLikeSpaceTable(text)) return null;
  const lines = String(text || '').trim().split('\n').map(l => l.trim()).filter(Boolean);
  const rows = lines.map(l => l.split(/\s{2,}/).map(c => c.trim().replace(/\|/g, '\\|')));
  const cols = Math.max(...rows.map(r => r.length));
  const norm = rows.map(r => { while (r.length < cols) r.push(''); return r; });
  const fmt = row => `| ${row.join(' | ')} |`;
  const sep = norm[0].map(() => '---');
  return [fmt(norm[0]), fmt(sep), ...norm.slice(1).map(fmt)].join('\n');
}

function convertPlainBlock(block) {
  if (isMarkdownTable(block)) return block;
  return tsvToMarkdownTable(block)
    || (looksLikeCsv(block) ? csvToMarkdownTable(block) : null)
    || spaceTableToMarkdown(block)
    || detectAndWrapCode(block)
    || cleanPlainTextToMarkdown(block);
}

// Orchestrator: prefer rich HTML (full fidelity); otherwise split plain text
// into sections separated by blank lines and convert each section independently
// so a mixed paste (CSV + TSV + already-formatted Markdown) is handled correctly.
export function smartPasteToMarkdown({ html = '', plain = '' } = {}, domParser) {
  const normalizedHtml = String(html || '').replace(/\r\n?/g, '\n');
  const normalizedPlain = String(plain || '').replace(/\r\n?/g, '\n');
  if (normalizedHtml.trim()) {
    const md = convertHtmlToMarkdown(normalizedHtml, normalizedPlain, domParser, 'auto');
    if (md && md.trim()) return md;
  }
  const text = normalizedPlain;
  if (looksLikeJiraWiki(text)) {
    return convertJiraWikiToMarkdown(text, domParser);
  }
  // Split on runs of blank lines so each section gets its own conversion.
  const blocks = text.split(/\n{2,}/).map(b => b.trim()).filter(b => b.length > 0);
  if (blocks.length === 0) return cleanPlainTextToMarkdown(text);
  return blocks.map(b => convertPlainBlock(b)).join('\n\n');
}

// ---------------------------------------------------------------------------
// File import (T1d): turn a dropped/opened file into Markdown based on its
// extension. CSV becomes a Markdown table, HTML is converted to Markdown, and
// everything else (.md/.markdown/.txt) is treated as Markdown source as-is.
// ---------------------------------------------------------------------------

// Parse CSV text into a 2D array of strings. Handles quoted fields, escaped
// quotes ("") and embedded commas/newlines inside quotes. Accepts an optional
// delimiter (defaults to comma).
export function parseCsv(text, delimiter = ',') {
  const src = String(text || '').replace(/\r\n?/g, '\n');
  const rows = [];
  let row = [];
  let field = '';
  let inQuotes = false;
  for (let i = 0; i < src.length; i++) {
    const ch = src[i];
    if (inQuotes) {
      if (ch === '"') {
        if (src[i + 1] === '"') { field += '"'; i++; }
        else inQuotes = false;
      } else {
        field += ch;
      }
    } else if (ch === '"') {
      inQuotes = true;
    } else if (ch === delimiter) {
      row.push(field); field = '';
    } else if (ch === '\n') {
      row.push(field); field = '';
      rows.push(row); row = [];
    } else {
      field += ch;
    }
  }
  row.push(field);
  rows.push(row);
  // Drop a trailing empty row produced by a final newline.
  if (rows.length > 1 && rows[rows.length - 1].length === 1 && rows[rows.length - 1][0] === '') {
    rows.pop();
  }
  return rows;
}

// Convert CSV text into a GitHub-flavoured Markdown table. Returns null when the
// text does not look tabular (fewer than 2 columns). Auto-detects ';' or tab as
// the delimiter when a comma is not the dominant separator.
// Heuristic: does this plain text look like a CSV/semicolon-separated table
// (rather than prose that happens to have commas)? Requires at least 2 rows
// with a consistent non-zero delimiter count, and tabs must be absent (TSV
// takes priority). Returns false for single-row or single-column content.
export function looksLikeCsv(text) {
  const raw = String(text || '').replace(/\r\n?/g, '\n').replace(/\n+$/, '');
  if (!raw.trim()) return false;
  if (/\t/.test(raw)) return false; // TSV takes priority
  const lines = raw.split('\n').filter(l => l.trim().length > 0);
  if (lines.length < 2) return false;
  // Curly braces are a strong code signal — semicolons in code would otherwise
  // be misidentified as a CSV separator.
  if (lines.some(l => /[{}]/.test(l))) return false;
  // Detect delimiter from raw comma/semicolon count on the first line.
  // (Quoted fields may inflate the raw count, but it still gives the right
  // delimiter when commas >> semicolons or vice-versa.)
  const firstRaw = lines[0];
  const rawCommas = (firstRaw.match(/,/g) || []).length;
  const rawSemis  = (firstRaw.match(/;/g) || []).length;
  if (rawCommas === 0 && rawSemis === 0) return false;
  const delim = rawSemis > rawCommas ? ';' : ',';
  // Use proper CSV parsing to get true column counts (handles quoted commas).
  const colCounts = lines.map(l => {
    const parsed = parseCsv(l, delim);
    return parsed.length > 0 ? parsed[0].length : 1;
  });
  if (colCounts[0] < 2) return false;
  // ≥70% of rows must have the same column count as the first row.
  const consistent = colCounts.filter(c => c === colCounts[0]).length;
  if (consistent / lines.length < 0.7) return false;
  // If more than 40% of rows have their last parsed cell ending with
  // sentence-ending punctuation (.!?), the content is likely prose.
  const lastCells = lines.map(l => {
    const parsed = parseCsv(l, delim);
    const row = parsed.length > 0 ? parsed[0] : [''];
    return (row[row.length - 1] || '').trim();
  });
  const sentenceEnds = lastCells.filter(c => /[.!?]$/.test(c)).length;
  if (sentenceEnds / lastCells.length > 0.4) return false;
  return true;
}

export function csvToMarkdownTable(text) {
  const raw = String(text || '').replace(/\n+$/, '');
  if (!raw.trim()) return null;
  const firstLine = raw.split('\n')[0] || '';
  let delimiter = ',';
  const commas = (firstLine.match(/,/g) || []).length;
  const semis = (firstLine.match(/;/g) || []).length;
  const tabs = (firstLine.match(/\t/g) || []).length;
  if (tabs > commas && tabs >= semis) delimiter = '\t';
  else if (semis > commas) delimiter = ';';
  const rows = parseCsv(raw, delimiter).filter(r => !(r.length === 1 && r[0].trim() === ''));
  if (rows.length < 1) return null;
  const cols = Math.max(...rows.map(r => r.length));
  if (cols < 2) return null;
  const norm = rows.map(r => {
    const cells = r.map(c => c.trim().replace(/\|/g, '\\|'));
    while (cells.length < cols) cells.push('');
    return cells;
  });
  const fmt = row => `| ${row.join(' | ')} |`;
  const header = norm[0];
  const separator = header.map(() => '---');
  const body = norm.slice(1);
  return [fmt(header), fmt(separator), ...body.map(fmt)].join('\n');
}

// Dispatch a file's text content to the right importer based on its filename
// extension. domParser is only required for HTML inputs.
export function importFileToMarkdown(text, filename = '', domParser) {
  const ext = String(filename || '').toLowerCase().match(/\.([a-z0-9]+)$/);
  const kind = ext ? ext[1] : '';
  if (kind === 'html' || kind === 'htm') {
    return convertHtmlToMarkdown(String(text || ''), '', domParser);
  }
  if (kind === 'csv') {
    return csvToMarkdownTable(text) || String(text || '');
  }
  // .md / .markdown / .txt / unknown: treat as Markdown source verbatim.
  return String(text || '');
}

// ---------------------------------------------------------------------------
// YAML frontmatter (T1f): documents may begin with a "---" fenced block of
// key: value metadata. parseFrontmatter splits that metadata from the Markdown
// body so previews/outputs can omit it while the editor keeps the raw source.
// Supports a pragmatic YAML subset: scalars (string/number/boolean), quoted
// strings, inline arrays ([a, b]) and block lists (- item on following lines).
// ---------------------------------------------------------------------------

function coerceScalar(raw) {
  let v = String(raw).trim();
  if (v === '') return '';
  // Strip matching surrounding quotes.
  if ((v.startsWith('"') && v.endsWith('"')) || (v.startsWith("'") && v.endsWith("'"))) {
    return v.slice(1, -1);
  }
  if (v === 'true') return true;
  if (v === 'false') return false;
  if (v === 'null' || v === '~') return null;
  if (/^-?\d+$/.test(v)) return parseInt(v, 10);
  if (/^-?\d*\.\d+$/.test(v)) return parseFloat(v);
  return v;
}

function parseInlineArray(raw) {
  const inner = String(raw).trim().slice(1, -1).trim();
  if (!inner) return [];
  return inner.split(',').map(s => coerceScalar(s.trim()));
}

export function parseFrontmatter(markdown) {
  const text = String(markdown || '');
  // Frontmatter must be the very first thing in the document.
  const match = text.match(/^---[ \t]*\r?\n([\s\S]*?)\r?\n(?:---|\.\.\.)[ \t]*(?:\r?\n|$)/);
  if (!match) return { attributes: {}, body: text };
  const block = match[1];
  const body = text.slice(match[0].length);
  const attributes = {};
  const lines = block.split(/\r?\n/);
  let currentKey = null;
  for (const line of lines) {
    if (!line.trim()) continue;
    const listItem = line.match(/^[ \t]+-[ \t]+(.*)$/);
    if (listItem && currentKey) {
      if (!Array.isArray(attributes[currentKey])) attributes[currentKey] = [];
      attributes[currentKey].push(coerceScalar(listItem[1]));
      continue;
    }
    const kv = line.match(/^([A-Za-z0-9_.-]+):[ \t]*(.*)$/);
    if (!kv) continue;
    const key = kv[1];
    const rawValue = kv[2];
    currentKey = key;
    if (rawValue === '') {
      // Could be the start of a block list; leave undefined until items arrive.
      attributes[key] = '';
    } else if (rawValue.startsWith('[') && rawValue.endsWith(']')) {
      attributes[key] = parseInlineArray(rawValue);
    } else {
      attributes[key] = coerceScalar(rawValue);
    }
  }
  return { attributes, body };
}

// Convenience: return just the Markdown body with any frontmatter removed.
export function stripFrontmatter(markdown) {
  return parseFrontmatter(markdown).body;
}

// --- Source + Artifact bundle (T4) -----------------------------------------
// A dependency-free ZIP writer using the STORE method (no compression). This
// keeps the app local-first and avoids pulling in JSZip/pako. Good enough for
// a handful of small text files (source.md, index.html, README.md).

const CRC32_TABLE = (() => {
  const table = new Uint32Array(256);
  for (let n = 0; n < 256; n++) {
    let c = n;
    for (let k = 0; k < 8; k++) {
      c = (c & 1) ? (0xEDB88320 ^ (c >>> 1)) : (c >>> 1);
    }
    table[n] = c >>> 0;
  }
  return table;
})();

export function crc32(bytes) {
  let crc = 0xFFFFFFFF;
  for (let i = 0; i < bytes.length; i++) {
    crc = (crc >>> 8) ^ CRC32_TABLE[(crc ^ bytes[i]) & 0xFF];
  }
  return (crc ^ 0xFFFFFFFF) >>> 0;
}

function utf8Bytes(str) {
  if (typeof TextEncoder !== 'undefined') return new TextEncoder().encode(str);
  // Node fallback (tests run under jsdom where TextEncoder exists, but stay safe).
  const buf = Buffer.from(str, 'utf-8');
  return new Uint8Array(buf.buffer, buf.byteOffset, buf.byteLength);
}

// Build a ZIP archive (STORE / no compression) from a list of files.
// `files` is an array of { name, content } where content is a string.
// Returns a Uint8Array containing the raw ZIP bytes.
export function buildZip(files = []) {
  const encoder = utf8Bytes;
  const localParts = [];
  const centralParts = [];
  let offset = 0;

  const writeUint16 = (arr, value) => { arr.push(value & 0xFF, (value >>> 8) & 0xFF); };
  const writeUint32 = (arr, value) => {
    arr.push(value & 0xFF, (value >>> 8) & 0xFF, (value >>> 16) & 0xFF, (value >>> 24) & 0xFF);
  };

  files.forEach(file => {
    const nameBytes = encoder(String(file.name));
    const dataBytes = encoder(String(file.content == null ? '' : file.content));
    const crc = crc32(dataBytes);
    const size = dataBytes.length;

    // Local file header.
    const local = [];
    writeUint32(local, 0x04034b50); // signature
    writeUint16(local, 20);         // version needed
    writeUint16(local, 0x0800);     // flags: UTF-8 names
    writeUint16(local, 0);          // method: store
    writeUint16(local, 0);          // mod time
    writeUint16(local, 0);          // mod date
    writeUint32(local, crc);
    writeUint32(local, size);       // compressed size
    writeUint32(local, size);       // uncompressed size
    writeUint16(local, nameBytes.length);
    writeUint16(local, 0);          // extra length
    const localHeader = new Uint8Array(local);
    localParts.push(localHeader, nameBytes, dataBytes);

    // Central directory header.
    const central = [];
    writeUint32(central, 0x02014b50);
    writeUint16(central, 20);        // version made by
    writeUint16(central, 20);        // version needed
    writeUint16(central, 0x0800);    // flags: UTF-8
    writeUint16(central, 0);         // method
    writeUint16(central, 0);         // mod time
    writeUint16(central, 0);         // mod date
    writeUint32(central, crc);
    writeUint32(central, size);
    writeUint32(central, size);
    writeUint16(central, nameBytes.length);
    writeUint16(central, 0);         // extra length
    writeUint16(central, 0);         // comment length
    writeUint16(central, 0);         // disk number
    writeUint16(central, 0);         // internal attrs
    writeUint32(central, 0);         // external attrs
    writeUint32(central, offset);    // local header offset
    centralParts.push(new Uint8Array(central), nameBytes);

    offset += localHeader.length + nameBytes.length + dataBytes.length;
  });

  const centralStart = offset;
  let centralSize = 0;
  centralParts.forEach(part => { centralSize += part.length; });

  const end = [];
  writeUint32(end, 0x06054b50);     // end of central dir signature
  writeUint16(end, 0);              // disk number
  writeUint16(end, 0);              // central dir disk
  writeUint16(end, files.length);   // entries on this disk
  writeUint16(end, files.length);   // total entries
  writeUint32(end, centralSize);
  writeUint32(end, centralStart);
  writeUint16(end, 0);              // comment length
  const endRecord = new Uint8Array(end);

  const total = centralStart + centralSize + endRecord.length;
  const out = new Uint8Array(total);
  let pos = 0;
  localParts.forEach(part => { out.set(part, pos); pos += part.length; });
  centralParts.forEach(part => { out.set(part, pos); pos += part.length; });
  out.set(endRecord, pos);
  return out;
}

// Build the list of files for a "Source + Artifact" bundle: the raw Markdown
// source, a navigable standalone HTML artifact, and a README explaining the
// bundle. Returns an array of { name, content } ready for buildZip().
export function buildSourceArtifactBundle(markdown, filename = 'document.md') {
  const source = String(markdown == null ? '' : markdown);
  const baseName = String(filename).replace(/\.[^./]*$/, '') || 'document';
  const title = baseName.replace(/[-_]+/g, ' ').replace(/\s+/g, ' ').trim() || 'Document';
  const html = renderNavigableHtml(source, { title });
  const generated = (typeof Date !== 'undefined') ? new Date().toISOString().slice(0, 10) : '';
  const readme = [
    `# ${title} — Source + Artifact bundle`,
    '',
    'Generated by **Markup Forge** — One Markdown source. Every output you need.',
    generated ? `\nGenerated: ${generated}` : '',
    '',
    '## Contents',
    '',
    '- `source.md` — the Markdown source (the single source of truth).',
    '- `index.html` — a self-contained, navigable HTML artifact. Open it in any browser; no server or internet required.',
    '- `README.md` — this file.',
    '',
    '## How to use',
    '',
    '1. Edit `source.md` in any Markdown editor (or back in Markup Forge).',
    '2. Re-export to regenerate `index.html` from the updated source.',
    '',
    'The HTML artifact is fully offline: styles are embedded and there are no scripts or external requests.',
    '',
  ].join('\n');

  return [
    { name: 'source.md', content: source },
    { name: 'index.html', content: html },
    { name: 'README.md', content: readme },
  ];
}

// --- Slash commands (T1c) --------------------------------------------------
// Insertable Markdown snippets triggered by typing "/" at the start of a line
// (or after whitespace). Each command carries the text to insert and an
// optional caret offset (measured from the start of the snippet) so the
// cursor can land in a useful spot after insertion.

export const SLASH_COMMANDS = [
  {
    name: 'table',
    title: 'Table',
    description: 'Insert a Markdown table',
    snippet: '| Column A | Column B |\n| --- | --- |\n| Cell 1 | Cell 2 |\n| Cell 3 | Cell 4 |\n',
    caret: 2,
  },
  {
    name: 'code',
    title: 'Code block',
    description: 'Insert a fenced code block',
    snippet: '```js\n\n```\n',
    caret: 6, // inside the fence, on the empty line
  },
  {
    name: 'callout',
    title: 'Callout',
    description: 'Insert a note callout',
    snippet: '> [!NOTE]\n> Your note here.\n',
    caret: 12,
  },
  {
    name: 'jira',
    title: 'Jira status',
    description: 'Insert a Jira-friendly status block',
    snippet: '## Summary\n\n- **Status:** \n- **Owner:** \n- **Next step:** \n',
    caret: 3,
  },
  {
    name: 'email',
    title: 'Email',
    description: 'Insert an email skeleton',
    snippet: '**Subject:** \n\nHi ,\n\n\n\nBest regards,\n',
    caret: 12,
  },
];

// Filter the slash commands by a query (the text typed after "/"). Matches by
// command name prefix or a substring of the title. An empty query lists all.
export function filterSlashCommands(query) {
  const q = String(query == null ? '' : query).toLowerCase().replace(/^\//, '').trim();
  if (!q) return SLASH_COMMANDS.slice();
  return SLASH_COMMANDS.filter(cmd =>
    cmd.name.startsWith(q) || cmd.title.toLowerCase().includes(q));
}
