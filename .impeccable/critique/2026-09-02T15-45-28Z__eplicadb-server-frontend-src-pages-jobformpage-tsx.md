---
target: dropdown consistency on job form
total_score: 27
max_score: 40
na_heuristics: 
p0_count: 0
p1_count: 2
target_identity: "file:/Users/oscarsm/Documents/GitHub/ReplicaDB/replicadb-server/frontend/src/pages/JobFormPage.tsx"
target_fingerprint: "sha256:67dcb1008ae98ede04c2a49e36b285008d19aaf9be405040876ab2e34c0534d4"
target_path: /Users/oscarsm/Documents/GitHub/ReplicaDB/replicadb-server/frontend/src/pages/JobFormPage.tsx
timestamp: 2026-09-02T15-45-28Z
slug: eplicadb-server-frontend-src-pages-jobformpage-tsx
---
⚠️ DEGRADED: single-context (Assessment B subagent lacked terminal/browser access; required evidence was completed by the parent context fallback)

# ReplicaDB Job Form Dropdown Critique

## Design Health Score

| # | Heuristic | Score | Key Issue |
|---|---|---:|---|
| 1 | Visibility of System Status | 3 | Both controls show focus and open state, but the selected Mode row gets a stronger cue than datasource options. |
| 2 | Match System / Real World | 3 | Mode consequences and connector names are understandable; visual weight does not reflect a shared control pattern. |
| 3 | User Control and Freedom | 3 | Both menus support normal keyboard and pointer selection, with no trap observed. |
| 4 | Consistency and Standards | 2 | Equivalent dropdowns use different option typography, secondary content, and selected-state treatment. |
| 5 | Error Prevention | 3 | Labels, required state, and datasource capability filtering help; the visual mismatch can still make a critical selection easy to overlook. |
| 6 | Recognition Rather Than Recall | 3 | Descriptions and connector types provide recognition cues, but they are presented with different hierarchy rules. |
| 7 | Flexibility and Efficiency | 2 | Standard selection works, but datasource lists are long and have no shared scan or grouping treatment. |
| 8 | Aesthetic and Minimalist Design | 2 | The bold Mode options over-command attention while the datasource list recedes into its paper surface. |
| 9 | Error Recovery | 3 | Helper and error text exist; the open-list styling does not reinforce where the active/invalid choice is. |
| 10 | Help and Documentation | 3 | Mode descriptions explain consequences inline, but the datasource list offers only connector labels and no comparable selection guidance. |
| **Total** | | **27/40** | **Acceptable foundation; dropdown parity is the main visual quality gap.** |

## Design Specificity Verdict

The form is recognizably ReplicaDB through source/sink terminology, replication-mode consequences, connector names, and the Engineering Ledger visual system. The dropdown mismatch is not a loss of product identity; it is a local implementation split inside an otherwise coherent control plane.

The two controls look different for concrete reasons. The Mode select is a MUI `TextField select` whose `MenuItem` uses `ListItemText` with `primaryTypographyProps={{ fontWeight: 700 }}` in [JobFormPage.tsx](replicadb-server/frontend/src/pages/JobFormPage.tsx). It also has a two-line primary/description structure and receives MUI's selected-row background. The datasource field is a MUI `Autocomplete` in [DatasourceSelector.tsx](replicadb-server/frontend/src/components/DatasourceSelector.tsx): its `ListItemText` has no explicit primary weight, so it inherits regular body weight, and the theme defines no `MuiAutocomplete` option/paper/selected-state override. Its options therefore sit on a transparent/default list state until hover or selection.

In short: one dropdown is explicitly bold and semantically described; the other is regular-weight and largely left to MUI defaults. The user is correctly seeing a typography and state-token mismatch, not a browser rendering error.

## Deterministic and Browser Evidence

The CLI detector ran exactly once against [JobFormPage.tsx](replicadb-server/frontend/src/pages/JobFormPage.tsx), returned exit code `0`, and produced `[]` with zero source findings. A fresh browser tab was used and mutable injection succeeded. The browser overlay reported 17 runtime findings: 14 `layout property animation` annotations, 2 `cramped padding` annotations, and 1 `line length too long` annotation. The 14 layout findings are associated with runtime-generated MUI styles rather than declarations in the target source and are false positives for this issue. The padding and line-length annotations may reflect the long two-line option/copy treatment and are secondary follow-up evidence, not the root cause of the bold-versus-faint mismatch. The helper was stopped after collection.

## Overall Impression

The form is visually disciplined until either dropdown opens. Then the Mode menu shouts with 700-weight primary labels and a teal selected wash, while the datasource menu whispers with 400-weight names on an unaccented paper list. That tonal whiplash weakens trust at exactly the point where engineers are binding a job to real systems.

## What's Working

- **Shared control foundation:** Both fields use the same 40px outlined input language, Avenir body face, restrained radius, teal focus treatment, and accessible combobox/select semantics.
- **Useful secondary information:** Mode consequences and datasource connector types reduce recall when they remain readable.
- **Good interaction baseline:** The menus are keyboard-operable, labels are present, and datasource capability filtering keeps invalid source/sink choices out of the list.

## Cognitive Load

Three checklist items fail at the open-menu decision point:

- **Visual hierarchy:** Bold Mode rows imply a priority that is not part of the product model, while datasource rows appear visually unfinished.
- **Minimal choices:** The datasource list can expose many options at once without grouping or a stronger scan structure; search helps, but the list still needs a stable primary/secondary rhythm.
- **Progressive disclosure:** The mode menu reveals rich descriptions, but the datasource menu reveals only a lighter name/type pair, so equivalent decisions carry different amounts of visible context.

Single focus, grouping, one decision at a time, and working-memory demands are otherwise manageable for this form segment.

## Emotional Journey

The closed form feels calm and dependable. Opening Mode creates an impression of urgency or recommendation because every primary label is bold. Opening Source datasource creates the opposite concern: the list looks like default MUI content or a loading surface because rows have no explicit emphasis or selected/hover language. The user then recalibrates between controls instead of evaluating the actual replication choice, ending the selection step with doubt rather than confidence.

## Priority Issues

### [P1] Equivalent dropdowns use contradictory primary typography

**Why it matters:** The Mode menu sets primary option text to 700 weight while datasource names inherit 400. Engineers read the difference as meaning, even though neither control is more important than the other.

**Fix:** Establish one option typography contract: primary text at a shared moderate weight, secondary text at the shared muted body style, and the same row padding/line-height in both menus. Remove the one-off `fontWeight: 700` override from Mode or deliberately apply the same moderate weight to both controls.

**Suggested command:** `/impeccable polish`

### [P1] Autocomplete options have no ReplicaDB selected/hover surface

**Why it matters:** Mode receives a visible selected-row tint from MUI, while datasource options remain transparent/default. The datasource list can blend into the white paper and does not communicate the active row with the same confidence.

**Fix:** Add a theme-level `MuiAutocomplete` option and paper treatment using the existing tokens: Paper White surface, muted border, restrained dialog-level shadow, teal-tinted hover, and teal-tinted selected state. Keep text contrast semantic and avoid introducing a heavier decorative border.

**Suggested command:** `/impeccable polish`

### [P2] Secondary content follows two different information hierarchies

**Why it matters:** Mode shows consequence copy such as “Replaces the sink before loading,” while datasource shows only connector type. The content is useful in both cases, but the primary/secondary relationship is not visually standardized.

**Fix:** Reuse the same `ListItemText` typography contract in both renderers. Keep Mode descriptions concise and keep datasource connector type as a consistent secondary line; align its color, size, line-height, and spacing with Mode descriptions.

**Suggested command:** `/impeccable clarify`

### [P2] Long option copy creates avoidable narrow-menu pressure

**Why it matters:** The browser detector reported `cramped padding` twice and `line length too long` once. Rich descriptions improve comprehension, but they need enough width and predictable wrapping to avoid turning a short choice into a visually dense block.

**Fix:** Set a responsive minimum/max width for the menu paper, allow deliberate two-line wrapping, and keep each description to one concise sentence. Verify at 320px, 390px, and desktop widths.

**Suggested command:** `/impeccable adapt`

## Persona Red Flags

- **Alex, platform engineer:** The bold Mode menu feels like a forced recommendation and slows comparison; the faint datasource list makes a high-frequency source choice harder to scan.
- **Jordan, first-time DBA:** Jordan may interpret the visual difference as a difference in safety or importance and may assume the datasource list is incomplete or still loading.
- **Sam, accessibility-dependent operator:** Semantic labels and keyboard controls are strong, but visual state differences are not backed by one consistent focus/selection contract; verify that active option announcements match the visible row state.

## Minor Observations

- Both controls use the same outlined input surface and 40px height, so the mismatch begins inside the popup rather than at the field boundary.
- The mode selected background is a MUI default-style state, while the datasource popup has no explicit `MuiAutocomplete` theme override.
- The source list currently exposes many options; as the catalog grows, server-side filtering or grouping will matter more than additional decoration.
- The detector's 14 layout-animation findings should not be fixed in the form source without proving they are authored styles.

## Questions to Consider

- Should the shared contract use regular 400 text or a restrained 500-ish emphasis for every dropdown primary label?
- Should both menus keep secondary descriptions/types, or should Mode descriptions be shortened to match datasource density?
- Should the next fix be a shared theme-level popup treatment, or a smaller local normalization of these two renderers?
