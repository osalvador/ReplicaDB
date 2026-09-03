---
target: active job form
total_score: 28
max_score: 40
na_heuristics: 
p0_count: 0
p1_count: 2
target_identity: "file:/Users/oscarsm/Documents/GitHub/ReplicaDB/replicadb-server/frontend/src/pages/JobFormPage.tsx"
target_fingerprint: "sha256:48334ec05daf03134d0e431b52d7274987e15b8460f0acfc4b37c7c5cd192c8a"
target_path: /Users/oscarsm/Documents/GitHub/ReplicaDB/replicadb-server/frontend/src/pages/JobFormPage.tsx
timestamp: 2026-09-02T14-01-49Z
slug: eplicadb-server-frontend-src-pages-jobformpage-tsx
closed: true
---
Method: Assessment A was completed by an isolated design-review agent. Assessment B was attempted by a separate isolated agent but lacked terminal/browser access; the parent completed the required evidence pass as a fallback.

# ReplicaDB Job Form Critique

## Design Health Score

| # | Heuristic | Score | Key Issue |
|---|---|---:|---|
| 1 | Visibility of System Status | 3 | Loading, saving, validation, and datasource warnings are visible; there is no dirty-state or leave-without-saving feedback. |
| 2 | Match System / Real World | 3 | Source/sink language fits engineers, but raw replication modes and lease terminology assume prior knowledge. |
| 3 | User Control and Freedom | 2 | Back navigation exists, but staging switches can replace the other value, drafts are not recoverable, and edit-mode names are locked without explanation. |
| 4 | Consistency and Standards | 4 | MUI controls, sections, tabs, labels, focus treatment, and responsive grids are consistent. |
| 5 | Error Prevention | 3 | Required fields and numeric constraints are strong; destructive sink options and mutually exclusive staging changes lack a confirmation or explicit consequence. |
| 6 | Recognition Rather Than Recall | 3 | Helper text and sensible defaults help, but several important decisions require remembering replication semantics. |
| 7 | Flexibility and Efficiency | 2 | Keyboard navigation and mode defaults work, but advanced tuning is always exposed and there are no templates, summaries, or shortcuts. |
| 8 | Aesthetic and Minimalist Design | 3 | The visual system is clean and calm, but the long form gives many advanced controls equal prominence. |
| 9 | Error Recovery | 3 | Inline validation and mutation feedback are present; load and save failures do not consistently explain the next recovery step. |
| 10 | Help and Documentation | 2 | Field helper text exists, but modes, staging strategy, and lease expiry have no task-focused explanation or glossary path. |
| **Total** | | **28/40** | **Good foundation; clarity and progressive disclosure are the main gaps.** |

## Design Specificity Verdict

The form is recognizably ReplicaDB: the teal-led Engineering Ledger system, source/sink vocabulary, datasource ACL behavior, incremental watermark fields, and durable retry policy are specific to a managed replication control plane. It is not a generic settings form.

The main missed opportunity is semantic specificity at the decision points. The visual shell is authored, but raw enum labels and infrastructure terms make the core replication choices feel like implementation vocabulary. The result is coherent and trustworthy for an experienced operator, but less self-explanatory for an engineer encountering ReplicaDB for the first time.

## Deterministic and Browser Evidence

The CLI detector ran exactly once against `replicadb-server/frontend/src/pages/JobFormPage.tsx`, returned exit code 0, and produced `[]` with zero source findings. A fresh browser tab was used for the visual pass; DOM mutation and detector injection succeeded. The visible detector overlay reported repeated `layout property animation: transition: max-width` annotations from runtime-generated MUI styles around the autocomplete fields. These are framework-generated false positives: the target source contains no matching transition declaration. The helper was stopped after the pass.

## Overall Impression

A competent, carefully structured form that already feels safe to operate, but asks users to understand too much before they can make a confident first configuration. The single biggest opportunity is to turn advanced replication knowledge into progressive, plain-language decisions without flattening the power of the form.

## What's Working

- **Strong structural grammar:** Basics, Source, Sink, and Watermark and execution sections create a dependable scan path and preserve the committed visual system.
- **Good safety net:** Required-field validation, mode-dependent watermark fields, numeric bounds, disabled binding states, and compound checkbox explanations prevent many invalid configurations.
- **Responsive and accessible foundation:** Semantic headings/regions, labeled controls, visible focus, responsive grids, and local table/tab behavior support keyboard and narrow-screen use.

## Cognitive Load

Four of the eight checklist items fail at the form's peak decision points:

- **Single focus:** Source filtering, sink mapping, staging, destructive sink behavior, retry policy, and performance tuning share one uninterrupted creation path.
- **Chunking:** The Watermark and execution section exposes several coupled numeric and retry controls that should be grouped into essential versus advanced decisions.
- **Working memory:** Users must remember what complete, complete-atomic, incremental, schema staging, table staging, and lease expiry imply before saving.
- **Progressive disclosure:** Fetch size, bandwidth, max attempts, backoff, and automatic retry are visible to every user instead of appearing when needed.

Grouping, basic visual hierarchy, and the number of options at the mode selector itself are acceptable. The form has three mode options, but the meaning behind those options is not visible at the choice point.

## Emotional Journey

Arrival is calm: the back link, page title, and section framing make a complex task feel intentional. Datasource selection is the confidence-building moment, because it connects the job to real source and sink systems. Uncertainty peaks when the user reaches raw mode choices, staging tabs, and retry controls; the form becomes a test of prior ReplicaDB knowledge. The final Create job action is clear and successful, but there is no compact review summary before submission, so confidence depends on remembering choices made several sections earlier.

## Priority Issues

### [P1] Advanced controls compete with the first successful configuration

**Why it matters:** The form presents connection selection, filtering, sink safety, staging, retry policy, and performance tuning in one long flow. First-timers must parse operational knobs before they know which ones matter; power users must scroll past controls they may not need.

**Fix:** Keep the minimum viable job path prominent: name, source, sink, mode, and required mapping. Move fetch size, bandwidth, retry tuning, verbose logging, and optional staging details into an `Advanced` or `Resilience` disclosure with clear defaults and a short summary when collapsed. Keep all current values intact when the disclosure closes.

**Suggested command:** `/impeccable distill`

### [P1] Replication modes are raw enum labels at the most consequential choice

**Why it matters:** `complete`, `complete-atomic`, and `incremental` are implementation names. A user can select a mode without seeing whether the sink is cleared, whether the load is all-or-nothing, or whether a watermark is required.

**Fix:** Keep the wire values unchanged, but show plain-language option labels and one-line consequences: Complete (replace the sink), Complete atomic (all-or-nothing load), and Incremental (load changes from a watermark). Keep the existing warning for complete mode and make the same consequence visible on create.

**Suggested command:** `/impeccable clarify`

### [P2] Staging schema/table is a hidden destructive switch

**Why it matters:** Tabs look like views, but these two choices are mutually exclusive and switching resets the other value. Users can lose an entered staging target or fail to understand which permission model they are choosing.

**Fix:** Use an explicit radio/select choice with the consequence beside it, then show only the selected field. Warn before replacing a non-empty value, or preserve both values until save and validate the selected one.

**Suggested command:** `/impeccable clarify`

### [P2] Retry policy language assumes distributed-systems expertise

**Why it matters:** `Automatic retry after lease expiry`, `Maximum automatic attempts`, and `Retry backoff` are meaningful to platform operators but opaque to many engineers. The automatic mode default changes with replication mode without explaining why.

**Fix:** Rename the group to `Resilience and retry`, define lease expiry in one sentence, state whether attempts include the first run, and explain why incremental mode defaults to retry. Keep the current policy behavior and values.

**Suggested command:** `/impeccable clarify`

### [P2] Edit-mode name lock has no visible product explanation

**Why it matters:** The disabled Name field makes a rename look broken. If immutability is required for audit or identity reasons, users need to know that before attempting to change it; if renaming is supported by product policy, the current control blocks a legitimate maintenance task.

**Fix:** Confirm the backend contract. If names are intentionally immutable, label the field as `Job name (cannot be changed after creation)` and provide the supported rename path. If renaming is valid, allow it through the audited update contract rather than silently disabling the field.

**Suggested command:** `/impeccable clarify`

## Persona Red Flags

- **Alex, power user:** The form has useful defaults and keyboard navigation, but advanced controls are always expanded and there is no compact review or template path. The locked name field is likely to feel like an arbitrary blocker during maintenance.
- **Jordan, first-timer:** Jordan can follow the section order but cannot infer the consequences of the three mode labels or the staging tabs. Retry and watermark language arrives before Jordan has a usable mental model.
- **Sam, accessibility-dependent user:** Semantic labels, headings, focus, and responsive grids are strong. The long linear tab order and repeated numeric controls create unnecessary navigation cost; changing tabs and conditional fields should preserve a clear announcement of the active choice and its consequence.

## Minor Observations

- The form's required-field errors are specific and close to their inputs; preserve that pattern while adding higher-level guidance.
- `Source` and `Sink` are appropriate domain terms for this engineering audience, but helper copy should explain the action rather than merely repeat the label.
- The complete-mode warning currently appears in edit mode; the consequence should also be discoverable while creating a job.
- The detector's MUI `max-width` animation annotations should not be fixed in `JobFormPage.tsx`.
- The source selector's `200`-item query is acceptable for current fixtures but may need search or server-side filtering as datasource catalogs grow.

## Questions to Consider

- Should the default create path optimize for a minimal complete job, with resilience and performance controls collapsed until requested?
- Are job names intentionally immutable after creation, and what is the supported audited rename path?
- Is the priority to help first-time engineers understand modes, or to make experienced operators faster through advanced defaults and a review summary?
