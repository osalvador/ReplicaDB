# CodeMirror 6 Migration Plan

## Objective

Migrate the current custom Markdown editor to CodeMirror 6 to:

- recover robust Markdown syntax highlighting
- improve performance on medium and large documents
- simplify editor behavior and state management
- make preview synchronization consistent across:
  - Teams
  - Jira Visual
  - Jira Text
  - HTML Preview
  - HTML Source
- stop layout/render regressions caused by the custom textarea + overlay approach

This migration must preserve:

- static deployment
- browser-only execution
- local-first behavior
- PWA/offline support
- current product model: one Markdown source, multiple output previews

---

## Scope

### In scope

- add a minimal build step
- integrate CodeMirror 6
- replace the current textarea editor
- migrate toolbar behavior
- migrate slash commands
- rebuild preview synchronization
- add performance gating for large documents
- preserve autosave, import/export, copy, preview tabs, PWA

### Out of scope

- Monaco migration
- backend/cloud features
- new output formats
- advanced linting/formatting
- collaborative editing
- large redesign of UI

---

## Current Problems To Solve

### Editor architecture

- custom textarea editor depends on:
  - manual overlay syntax highlight
  - manual line numbers
  - manual selection editing
  - manual scroll sync
  - manual slash menu positioning
- this is fragile and expensive to maintain

### Preview sync

- Teams works better now, but Jira/HTML remain inconsistent
- source/text outputs do not have a robust line/block mapping model
- tall blocks like tables/images still stress synchronization logic

### Performance

- input handling currently fans out into too much work:
  - stats
  - line numbers
  - highlight overlay
  - preview rendering
  - scroll sync
- large files degrade typing performance

### Layout/render stability

- Jira/HTML preview shells have already regressed in height/stretch behavior
- iframe-based previews need persistence to avoid blink and DOM churn

---

## Proposed Architecture

## 1. Keep `converter-core.js` as pure rendering logic

Responsibilities:

- Markdown parsing
- Markdown -> Teams HTML
- Markdown -> Jira text
- Markdown -> Jira visual HTML
- Markdown -> HTML preview/source artifact
- block metadata generation:
  - `startLine`
  - `endLine`

Expected outcome:

- output renderers remain testable
- source mapping becomes explicit instead of inferred in app code

---

## 2. Introduce `editor-cm.js`

New file responsible for CodeMirror integration.

Responsibilities:

- create editor instance
- expose editor API to app layer
- configure:
  - Markdown language
  - syntax highlighting
  - line numbers
  - history
  - selection behavior
  - theme
- provide cursor/viewport information

Suggested public API:

- `createMarkdownEditor(root, options)`
- `getValue()`
- `setValue(value)`
- `focus()`
- `dispatch(spec)`
- `replaceSelection(text)`
- `getCursorLine()`
- `getSelection()`
- `getVisibleLineRange()`
- `scrollToLine(line)`
- `onChange(callback)`

---

## 3. Introduce `preview-sync.js`

New file responsible for all editor/preview synchronization.

Responsibilities:

- map editor cursor line to preview block
- map editor viewport to preview viewport
- support:
  - Teams visual
  - Jira visual
  - Jira text
  - HTML preview
  - HTML source

Expected outcome:

- one synchronization model
- fewer per-format hacks inside `converter-app.js`

---

## 4. Reduce `converter-app.js` to orchestration

Responsibilities after migration:

- app state
- view mode
- preview tab selection
- file import/export
- autosave
- toolbar wiring
- slash command orchestration
- calling editor API
- calling preview API

Expected outcome:

- less editor-specific DOM code
- simpler update cycle
- easier maintenance

---

## Technical Decisions

## Decision 1. Use CodeMirror 6

Recommended: yes

Reason:

- lighter than Monaco
- better suited for static browser app
- enough performance and flexibility
- easier incremental migration

## Decision 2. Add build step

Recommended: yes

Tool:

- Vite

Reason:

- easiest path to bundle CodeMirror
- keeps app static
- works well with GitHub Pages and PWA assets

## Decision 3. Preview render policy

Recommended:

- `Split`: render preview live
- `Preview`: render preview live
- `Editor`: do not render preview while typing
- large docs: auto-open in `Editor` mode when loaded

Reason:

- best performance win with minimal product compromise

---

## Dependency Plan

Initial packages to add:

- `vite`
- `@codemirror/state`
- `@codemirror/view`
- `@codemirror/commands`
- `@codemirror/language`
- `@codemirror/lang-markdown`
- `@codemirror/search`
- `@codemirror/autocomplete`
- `@lezer/highlight`

Optional depending on exact setup:

- `@codemirror/gutter`
- `@codemirror/history`
- `@codemirror/theme-one-dark`

Do not add extra editor packages until the base migration is stable.

---

## Phase 1. Build Pipeline

## Goal

Introduce a minimal bundle step without changing behavior.

## Tasks

1. Add `vite` to `package.json`
2. Create minimal Vite config targeting `docs/markdown`
3. Define entrypoint for app bundle
4. Keep `converter.html` as the published app shell
5. Update asset references if needed
6. Ensure service worker still caches built assets
7. Add build script(s):
   - `build`
   - optional `dev`

## Deliverables

- Vite config
- updated scripts
- working static build output

## Validation

- app still opens locally
- app still deploys statically
- PWA still works
- no functional regressions

---

## Phase 2. Mount CodeMirror 6

## Goal

Replace the core editor engine while preserving current UI.

## Tasks

1. Create `editor-cm.js`
2. Add editor mount container in existing editor panel
3. Initialize CM6 with:
   - markdown language
   - dark theme
   - line numbers
   - history
4. Bind editor content to existing app state
5. Preserve autosave/localStorage restore
6. Preserve focus behavior on load
7. Keep current toolbar visually unchanged for now

## Deliverables

- working CM6 editor inside current editor shell
- syntax highlighting restored
- line numbers restored

## Validation

- typing works
- undo/redo works
- syntax highlighting visible
- line numbers visible
- no custom overlay needed

---

## Phase 3. Replace Textarea-Specific Logic

## Goal

Remove dependence on textarea APIs.

## Tasks

1. Replace all reads of:
   - `editor.value`
   - `selectionStart`
   - `selectionEnd`
   - `scrollTop`
2. Replace with CM6 equivalents
3. Update:
   - stats calculation source
   - doc info source
   - autosave source
4. Keep DOM ids stable where practical for tests

## Deliverables

- app no longer depends on textarea internals

## Validation

- no runtime use of textarea-only APIs remains in active code path
- tests still pass after adapter changes

---

## Phase 4. Toolbar Migration

## Goal

Move Markdown toolbar actions to CM6 transactions.

## Tasks

1. Reimplement:
   - heading
   - bold
   - italic
   - quote
   - inline code
   - link
   - bulleted list
   - numbered list
   - task list
   - code block
2. Use CM6 transactions instead of string surgery on textarea
3. Preserve selection where appropriate
4. Preserve undo/redo granularity

## Deliverables

- toolbar works against CM6 editor state

## Validation

- toolbar actions behave correctly on:
  - empty selection
  - inline selection
  - multiline selection
- undo/redo returns to previous state cleanly

---

## Phase 5. Slash Command Migration

## Goal

Replace manual slash positioning and state with CM6-aware behavior.

## Tasks

1. Keep current slash command list
2. Use CM6 cursor position to detect trigger
3. Use CM6 geometry to place menu
4. Preserve:
   - filtering
   - arrow navigation
   - enter/tab accept
   - escape dismiss
5. Ensure inserted snippets use CM6 transactions

## Deliverables

- slash menu backed by CM6 state

## Validation

- slash menu opens in the correct place
- snippets insert correctly
- menu navigation remains stable

---

## Phase 6. Standardize Source Mapping In Preview Renderers

## Goal

Make preview synchronization based on explicit metadata.

## Tasks

1. Ensure all visual preview outputs wrap blocks with:
   - `data-src-start`
   - `data-src-end`
2. Apply to:
   - Teams visual output
   - Jira visual output
   - HTML preview output
3. Keep source/text outputs line-based
4. Ensure `converter-core.js` is the source of truth for block mapping

## Deliverables

- consistent source-map-ready preview DOM across visual outputs

## Validation

- rendered DOM contains stable source mapping metadata
- mapping survives headings, tables, images, code blocks, lists, quotes

---

## Phase 7. Rebuild Preview Synchronization

## Goal

Make all output formats follow editing consistently.

## Tasks

1. Implement shared synchronization module in `preview-sync.js`
2. Use editor cursor line and viewport information from CM6
3. For visual outputs:
   - find mapped block by line range
   - center or keep active block comfortably visible
4. For source/text outputs:
   - use line-centered scrolling
5. Keep iframe shells persistent for:
   - Jira visual
   - HTML preview
6. Separate typing sync from scroll sync:
   - typing -> active block
   - scrolling -> viewport ratio/viewport anchor

## Deliverables

- one synchronization model across formats

## Validation

- Teams sync works
- Jira Visual sync works
- Jira Text sync works
- HTML Preview sync works
- HTML Source sync works
- tall tables/images do not break visible editing context badly

---

## Phase 8. Performance Strategy

## Goal

Reduce typing latency on medium/large files.

## Tasks

1. Render preview only when:
   - `Split`
   - `Preview`
2. Skip preview rendering in `Editor` mode
3. Split update cycle into:
   - immediate editor-state updates
   - deferred preview rendering
4. Use `requestAnimationFrame` for preview refresh
5. Use caching for unchanged output per tab if possible
6. Keep iframe documents persistent instead of recreating them
7. Remove legacy overlay/highlight work entirely
8. Define large-document thresholds:
   - lines >= 600
   - or chars >= 120000
9. On large file load:
   - default to `Editor`
   - show small UI notice that preview is paused until enabled

## Deliverables

- faster typing path
- stable large-document behavior

## Validation

- large file editing feels responsive
- preview hidden means lower work
- split mode still works when explicitly enabled

---

## Phase 9. Cleanup Legacy Editor Code

## Goal

Finish the migration and remove obsolete systems.

## Tasks

1. Remove:
   - textarea overlay highlight logic
   - manual line-number sync
   - performance-lite workaround for old textarea stack
   - redundant scroll sync branches
2. Simplify tests that depended on old DOM structure
3. Keep the outer shell/UI stable

## Deliverables

- smaller `converter-app.js`
- editor logic isolated
- preview sync logic isolated

## Validation

- no dead code paths for old editor remain
- final architecture matches intended separation

---

## Phase 10. PWA And Release Hardening

## Goal

Make the migration safe for production use.

## Tasks

1. Update `sw.js` cache name
2. Ensure bundled assets are cached
3. Verify manifest still correct
4. Run offline smoke test
5. Verify no broken asset references
6. Check GitHub Pages path compatibility

## Deliverables

- production-ready static build
- working offline behavior

## Validation

- reload works offline after first load
- no missing bundle files
- app boots correctly from published path

---

## Testing Plan

## Unit tests

Add or update tests for:

- block parsing with line ranges
- Teams render mapping
- Jira visual mapping
- HTML preview mapping
- toolbar transformation logic if isolated

## E2E tests

Add or update Playwright coverage for:

1. editor loads with syntax highlighting
2. toolbar actions work with undo/redo
3. slash commands work
4. Teams sync while typing
5. Jira Visual sync while typing
6. Jira Text sync while typing
7. HTML Preview sync while typing
8. HTML Source sync while typing
9. Jira/HTML visual previews do not blink
10. preview shells fill panel height
11. large file opens in `Editor` mode
12. `Editor` mode skips preview work
13. opening ~1000-line file remains responsive enough

---

## Success Criteria

The migration is successful when:

- Markdown syntax highlighting is restored via CodeMirror
- typing is smoother on medium/large docs
- Teams/Jira/HTML sync consistently
- Jira/HTML previews stop blinking
- preview panels use full height correctly
- `Editor` mode is the fastest path
- toolbar and slash commands still behave correctly
- undo/redo remains robust
- app remains static, local-first, and PWA-compatible

---

## Implementation Order

1. Build pipeline
2. CodeMirror mount
3. textarea API replacement
4. toolbar migration
5. slash menu migration
6. source mapping standardization
7. cross-format preview sync
8. performance gating
9. cleanup
10. PWA hardening

---

## Notes

- Do not attempt Monaco first.
- Do not keep both old editor and CM6 long-term.
- Do not continue growing the current custom overlay editor.
- Prefer small, phased PRs/changes over a giant rewrite.
