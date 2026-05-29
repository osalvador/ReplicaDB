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

export function highlightCode(code, lang) {
  let s = htmlEscape(code);
  if (/^(json|js|javascript|ts|typescript)$/i.test(lang)) {
    s = s.replace(/(&quot;[^&]*?&quot;)(\s*:)/g, '<span class="hljs-attr">$1</span>$2');
    s = s.replace(/(:\s*)(&quot;[^&]*?&quot;)/g, '$1<span class="hljs-string">$2</span>');
    s = s.replace(/\b(true|false|null)\b/g, '<span class="hljs-keyword">$1</span>');
    s = s.replace(/\b(-?\d+(?:\.\d+)?)\b/g, '<span class="hljs-number">$1</span>');
  } else if (/^(java|bash|sh|yaml|yml)$/i.test(lang)) {
    s = s.replace(/(\/\/.*$|#.*$)/gm, '<span class="hljs-comment">$1</span>');
    s = s.replace(/(&quot;.*?&quot;|'.*?')/g, '<span class="hljs-string">$1</span>');
    s = s.replace(/\b(public|private|class|return|new|if|else|try|catch|final|static|void|boolean|int|long|String)\b/g, '<span class="hljs-keyword">$1</span>');
    s = s.replace(/\b(-?\d+(?:\.\d+)?)\b/g, '<span class="hljs-number">$1</span>');
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
  s = s.replace(/!\\?\[([^\]]*)\]\(([^)]+)\)/g, (_, alt, url) => `${alt || url} (${url})`);
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

export function languageLabel(language) {
  const raw = String(language || '').trim().toLowerCase();
  if (!raw) return 'Text';
  const labels = { js: 'JavaScript', javascript: 'JavaScript', ts: 'TypeScript', java: 'Java', json: 'JSON', yaml: 'YAML', yml: 'YAML', bash: 'Bash', sh: 'Shell', html: 'HTML', css: 'CSS', xml: 'XML', sql: 'SQL', text: 'Text' };
  return labels[raw] || raw.charAt(0).toUpperCase() + raw.slice(1);
}

export function highlightCodeForPreview(code, language) {
  const escaped = htmlEscape(code);
  const lang = String(language || '').trim().toLowerCase();
  if (lang === 'json') {
    return escaped
      .replace(/(&quot;[^&]*?&quot;)(\s*:)/g, '<span class="hljs-attr">$1</span>$2')
      .replace(/(:\s*)(&quot;[^&]*?&quot;)/g, '$1<span class="hljs-string">$2</span>')
      .replace(/\b(true|false|null)\b/g, '<span class="hljs-keyword">$1</span>')
      .replace(/\b(-?\d+(?:\.\d+)?)\b/g, '<span class="hljs-number">$1</span>');
  }
  if (lang === 'java' || lang === 'js' || lang === 'javascript' || lang === 'ts') {
    return escaped
      .replace(/(&quot;.*?&quot;|'.*?')/g, '<span class="hljs-string">$1</span>')
      .replace(/\b(public|private|class|return|new|const|let|var|if|else|try|catch|throw|void|static|final|import|package)\b/g, '<span class="hljs-keyword">$1</span>')
      .replace(/(\/\/.*$)/gm, '<span class="hljs-comment">$1</span>')
      .replace(/\b(-?\d+(?:\.\d+)?)\b/g, '<span class="hljs-number">$1</span>');
  }
  if (lang === 'yaml' || lang === 'yml') {
    return escaped
      .replace(/^([\s-]*)([A-Za-z0-9_.-]+)(\s*:)/gm, '$1<span class="hljs-attr">$2</span>$3')
      .replace(/(:\s*)([^\n#]+)/g, '$1<span class="hljs-string">$2</span>')
      .replace(/(#.*$)/gm, '<span class="hljs-comment">$1</span>');
  }
  return escaped;
}

export function renderTeamsCodeBlock(code, language) {
  const lines = String(code || '').replace(/\r\n?/g, '\n').split('\n');
  // Join with '' (no newline): each line is a block-level grid row, so a literal
  // newline inside the <pre> would render as an extra blank line and double the
  // vertical spacing.
  const body = lines.map((line, index) => `<span class="teams-code-line"><span class="teams-code-line-number">${index + 1}</span><span>${highlightCodeForPreview(line, language) || '&nbsp;'}</span></span>`).join('');
  return `<div class="teams-code-card"><div class="teams-code-head"><small>${languageLabel(language)}</small><span aria-hidden="true">•••</span></div><pre><code>${body}</code></pre></div>`;
}

export function renderMarkdownForTeams(markdown) {
  const text = String(markdown || '').replace(/\r\n?/g, '\n');
  const blocks = [];
  let last = 0;
  const fence = /```\s*([^\n`]*)\n([\s\S]*?)\n```/g;
  let match;
  // Render the exact same HTML that gets copied to the clipboard
  // (convertMarkdownToTeamsHtml) so the preview is faithful to what Teams
  // shows after pasting. Fenced code blocks are shown as the Teams code card
  // (header + line numbers) because that is how Teams itself renders them.
  while ((match = fence.exec(text))) {
    blocks.push(convertMarkdownToTeamsHtml(text.slice(last, match.index)));
    blocks.push(renderTeamsCodeBlock(match[2], match[1]));
    last = fence.lastIndex;
  }
  blocks.push(convertMarkdownToTeamsHtml(text.slice(last)));
  // Tag the blank-line spacer paragraphs (preview only) so the CSS can give
  // them the shorter height Teams uses, without changing the copied HTML.
  const body = blocks.join('').replace(/<p>&nbsp;<\/p>/g, '<p class="teams-blank">&nbsp;</p>');
  return `<div class="teams-sent-scroll"><div class="teams-sent-message"><div class="teams-content">${body}</div><div class="teams-sent-meta"><span class="teams-sent-time">Just now</span><span class="teams-sent-status" aria-hidden="true">✓</span></div></div></div>`;
}

export function getHtmlDocument(markdown, title) {
  const safeTitle = htmlEscape(String(title || 'document').replace(/\.md$/i, ''));
  return `<!doctype html>\n<html>\n<head>\n<meta charset="utf-8">\n<title>${safeTitle}</title>\n</head>\n<body>\n${renderMarkdown(markdown)}\n</body>\n</html>`;
}

export function getHtmlOutput(markdown, profile, title) {
  return profile === 'html-fragment' ? renderMarkdown(markdown) : getHtmlDocument(markdown, title);
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
    .replace(/<[^>]+>/g, '');
}

export function inlineTeamsHtml(text) {
  const { replaced, stash } = protectCodeSpans(String(text || ''));
  let s = htmlEscape(replaced);
  s = s.replace(/!\[([^\]]*)\]\(([^)]+)\)/g, (_, alt, url) => `${htmlEscape(alt || url)} (<a href="${htmlEscape(url)}">${htmlEscape(url)}</a>)`);
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
    if (quote) { out.push(`<blockquote spellcheck="false"><p>${inlineTeamsHtml(quote[1].trim())}</p></blockquote>`); i += 1; continue; }

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


// Teams Compact: collapse soft-wrapped lines inside a paragraph into a single
// line while preserving blank-line paragraph separation, lists, headings,
// quotes, horizontal rules, tables and fenced code blocks.
export function compactMarkdown(markdown) {
  const lines = String(markdown || '').replace(/\r\n?/g, '\n').split('\n');
  const out = [];
  let para = [];
  let inFence = false;
  const flush = () => { if (para.length) { out.push(para.join(' ')); para = []; } };
  const isStructural = (t) =>
    /^#{1,6}\s+/.test(t) ||
    /^[-*+]\s+/.test(t) ||
    /^\d+\.\s+/.test(t) ||
    /^>\s?/.test(t) ||
    /^(---|\*\*\*|___)$/.test(t) ||
    /\|/.test(t);
  for (let i = 0; i < lines.length; i += 1) {
    const line = lines[i];
    const t = line.trim();
    if (/^```/.test(t)) {
      flush();
      out.push(line);
      inFence = !inFence;
      continue;
    }
    if (inFence) { out.push(line); continue; }
    if (!t) { flush(); out.push(''); continue; }
    if (isStructural(t)) { flush(); out.push(line); continue; }
    para.push(t);
  }
  flush();
  return out.join('\n').replace(/\n{3,}/g, '\n\n').trim();
}

export function markdownForOutput(markdown, profile) {
  if (profile === 'teams-compact') return compactMarkdown(markdown);
  return String(markdown || '');
}
