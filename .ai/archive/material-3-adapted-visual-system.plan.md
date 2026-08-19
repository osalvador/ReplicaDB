# Implementation Plan: Material 3 adapted visual system for the ReplicaDB control plane

## Task Source
No JIRA ticket. Source: direct user request based on the proposed Material 3 visual direction and the planning discussion that followed it.

Acceptance criteria:
- The frontend adopts a coherent Material 3-inspired visual system across authenticated control-plane screens.
- The visual language remains appropriate for an operational database replication product: dense enough for scanning, calm enough for repeated use, and consistent across dashboard, job, run, schedule, authentication, and form workflows.
- The visual direction does not copy the proposed violet palette literally. It preserves the existing ReplicaDB teal/rust identity and adds M3-style surface, elevation, focus, state, spacing, and type-scale tokens.
- The desktop shell keeps the existing top AppBar/navigation pattern. A bottom navigation bar is out of scope because the product is a desktop-first operational control plane.
- Primary/secondary actions, fields, cards, alerts, chips, tables, dialogs, loading states, empty states, and error states use shared tokens and component behavior rather than page-specific styling.
- Existing routes, API calls, authorization behavior, CSRF/session behavior, job/run semantics, and generated OpenAPI types remain unchanged.
- The frontend remains usable at desktop and mobile widths, with keyboard-visible focus, accessible names, adequate contrast, and no overlapping or clipped content.

## Overview
ReplicaDB already uses MUI and has a working authenticated SPA, but visual decisions are currently distributed across page-level `Paper`, `Stack`, `Button`, `TextField`, `Chip`, and `Alert` usage. This plan introduces a complete frontend visual layer on top of the existing architecture: centralized tokens and MUI overrides, reusable composition primitives, and an ordered migration of the shell and operational screens.

The system borrows Material 3 interaction principles without copying its violet palette or forcing every control into stadium pills. ReplicaDB keeps its teal primary, rust secondary, serif display headings, top navigation, and information-dense tables while gaining stronger surface hierarchy, consistent action emphasis, predictable responsive behavior, and clearer state presentation.

## Architecture & Design

**Approach**: Full visual system adapted to the existing MUI control plane. This is frontend-only and consumes the existing REST/OpenAPI contract. It is split into 19 tasks so the visual foundation and each screen can be verified independently without introducing a new design framework.

**Visual decisions**:
- Preserve brand colors: primary teal `#0B6E69`, secondary rust `#B15C38`, dark text `#1B2926`, muted text `#50625D`, and light neutral background `#F3F6F4`. Add semantic success/info/warning/error tokens with sufficient contrast.
- Use layered neutral surfaces: page background, elevated shell surface, framed section surface, and subtle inset surface. Avoid a one-hue purple palette and decorative gradient blobs.
- Keep `Avenir Next`/`Helvetica Neue` for interface text and Georgia for display headings unless a separate font-loading decision is made. Do not add remote font dependencies or claim Source Sans Pro is a Roboto substitute.
- Use an M3-like type scale, 8px spacing rhythm, focus ring, state layers, and component density. Keep shape restrained: approximately 8px section corners and moderate action corners; do not make every operational control a large pill.
- Use MUI theme component overrides and reusable components instead of duplicated page-level styles. No new UI dependency is required.
- Keep the top AppBar and brand link to `/`. On narrow screens, make the header and actions wrap or compact gracefully; do not add bottom navigation.

**Architecture boundaries**:
- `src/theme` owns tokens, typography, shape, component overrides, and state-layer behavior.
- `src/components` owns reusable page headers, surfaces, status chips, section headers, tables, dialogs, and form layout helpers.
- `src/layout` owns the authenticated shell and navigation.
- `src/pages` owns screen composition only; pages continue using existing `src/api` modules and TanStack Query.
- `src/api`, `src/auth`, and `src/router` keep their current responsibilities and contracts.

**Integration points**: `src/main.tsx`/`ThemeProvider`, `src/theme/theme.ts`, `AppLayout`, `LoginPage`, `DashboardPage`, `JobDetailPage`, `JobFormPage`, `RunDetailPage`, `RunHistoryTable`, `JobScheduleCard`, `ConnectionSettingsCard`, `DataFilteringTabs`, `StagingOptionsTabs`, existing route tests, Vitest setup, and Playwright e2e flows.

**Risks and mitigations**:
- Global MUI overrides can change layout or accessible states; each high-blast-radius override gets focused tests and a full frontend regression run.
- Dense tables can overflow on mobile; browser bounding-box and overflow checks will verify the rendered layout rather than relying on jsdom.
- Markup changes can invalidate selectors; tests will use roles, labels, headings, links, and stable accessible names instead of CSS classes.
- New icons or dependencies can increase bundle size; reuse existing MUI icons and compare generated asset sizes.
- No credentials, resolved connection values, API tokens, or environment-specific URLs are added to fixtures, screenshots, source code, or documentation.

**Browser validation rule**: Testing Library covers semantic structure and interaction contracts; it is not used to claim pixel/layout correctness. Bounding boxes, viewport overflow, clipping, and overlap are verified in Playwright at explicit desktop/mobile viewports.

**Relevant learnings**:
- Generated OpenAPI schema and nullability are contract boundaries; this plan does not hand-edit or regenerate them because no API shape changes are required.
- `npm ci` must remain registry-neutral and reproducible; no package dependency is necessary.
- Existing Playwright authentication requires environment-managed credentials and explicit session/CSRF behavior; e2e additions preserve that setup and report missing credentials separately from UI failures.

## Implementation Tasks

### 1. Visual inventory and token contract
- [x] **1.1 Define the visual inventory and token contract before page migration**
  Files: `replicadb-server/frontend/src/theme/theme.ts`, `replicadb-server/frontend/src/theme/theme.test.ts` (new), `replicadb-server/frontend/src/pages/LoginPage.tsx`, `replicadb-server/frontend/src/layout/AppLayout.tsx`, `replicadb-server/frontend/src/pages/DashboardPage.tsx`, `replicadb-server/frontend/src/pages/JobDetailPage.tsx`, `replicadb-server/frontend/src/pages/JobFormPage.tsx`, `replicadb-server/frontend/src/pages/RunDetailPage.tsx`, `replicadb-server/frontend/src/components/RunHistoryTable.tsx`, `replicadb-server/frontend/src/components/JobScheduleCard.tsx`
  Changes: Define named theme tokens with concrete values before page migration: `brand.primary` `#0B6E69`, `brand.secondary` `#B15C38`, `surface.page` `#F3F6F4`, `surface.paper` `#FFFFFF`, `surface.subtle` `#E8F0ED`, `text.primary` `#1B2926`, `text.secondary` `#50625D`, semantic success/info/warning/error colors, `focus.ring`, `control.height`, `section.radius` `8`, `spacing.unit` `8`, and MUI `xs`/`sm`/`md` breakpoints. Inventory existing visual primitives in the shell, authentication, dashboard, job/detail/form, run, schedule, table, dialog, and connector controls so later tasks consume named tokens instead of ad hoc values.
  Tests: `theme.test.ts` asserts every named token exists with the specified values or constraints, primary/secondary contrast text is white, the section radius is `8`, the primary token is not purple, and semantic colors are distinct. Run the theme test plus `npm run typecheck`.
  Dependencies: None

### 2. Theme foundation
- [x] **2.1 Implement the adapted Material 3 theme foundation**
  Files: `replicadb-server/frontend/src/theme/theme.ts`, `replicadb-server/frontend/src/theme/theme.test.ts`
  Changes: Configure the palette, typography scale, shape, spacing, shadows/elevation, focus-visible styling, and `components` overrides for `MuiAppBar`, `MuiButton`, `MuiIconButton`, `MuiTextField`/`MuiOutlinedInput`, `MuiPaper`, `MuiCard`, `MuiAlert`, `MuiChip`, `MuiTable`, `MuiDialog`, `MuiTabs`, and `MuiTablePagination`. Make hover/selected/disabled/error/focus states explicit. Preserve the existing font families and avoid remote font loading.
  Tests: Extend theme tests with representative assertions for button radius/height, outlined-field focus styling, surface colors, alert spacing, chip density, and table header styling. Run the theme test plus `npm run typecheck`.
  Dependencies: Task 1.1

### 3. Shared composition primitives
- [x] **3.1 Add shared page header and surface-section primitives**
  Files: `replicadb-server/frontend/src/components/PageHeader.tsx` (new), `replicadb-server/frontend/src/components/PageHeader.test.tsx` (new), `replicadb-server/frontend/src/components/SurfaceSection.tsx` (new), `replicadb-server/frontend/src/components/SurfaceSection.test.tsx` (new)
  Changes: Add `PageHeader` with title, supporting text, optional breadcrumb/back link, and responsive action slot. Add `SurfaceSection` with title, optional description, optional action slot, semantic heading level, and consistent surface/padding/border treatment. Keep both primitives composition-only and free of server state. Do not create decorative card-within-card nesting.
  Tests: Render headers with and without actions; assert heading levels, accessible link names, action content, consistent section titles, and no layout-only description node when the description is absent.
  Dependencies: Task 2.1

- [x] **3.2 Add shared status and state primitives**
  Files: `replicadb-server/frontend/src/components/StatusChip.tsx` (new), `replicadb-server/frontend/src/components/StatusChip.test.tsx` (new), `replicadb-server/frontend/src/components/LoadingState.tsx` (new), `replicadb-server/frontend/src/components/LoadingState.test.tsx` (new), `replicadb-server/frontend/src/components/EmptyState.tsx` (new), `replicadb-server/frontend/src/components/EmptyState.test.tsx` (new), `replicadb-server/frontend/src/components/RunHistoryTable.tsx`
  Changes: Move the run-status-to-color mapping out of `RunHistoryTable` into `StatusChip`; add accessible `StatusChip`, a consistent loading state, and a compact empty state with optional action. Update `RunHistoryTable` to import the new status mapping/component in this task so there is one status authority. Keep run terminal semantics unchanged and do not add authorization logic to presentation components.
  Tests: Table-test all run statuses and assert expected chip labels/colors; assert loading and empty primitives expose stable accessible roles/labels and optional actions; assert `RunHistoryTable` consumes the shared status component without changing query/pagination behavior.
  Dependencies: Task 2.1

### 4. Application shell
- [x] **4.1 Migrate `AppLayout` to the shared shell visual system**
  Files: `replicadb-server/frontend/src/layout/AppLayout.tsx`, `replicadb-server/frontend/src/layout/AppLayout.test.tsx`, `replicadb-server/frontend/src/components/PageHeader.tsx`
  Changes: Use theme tokens and shared shell spacing for the AppBar, toolbar, brand link, identity/role display, logout action, and main content container. Keep `ReplicaDB` clickable to `/`; preserve logout behavior and make the header wrap/compact at mobile widths without introducing bottom navigation. Add subtle active/hover/focus state to the brand link and a consistent action grouping for identity/logout.
  Tests: Extend `AppLayout.test.tsx` to assert brand destination, logout/cache clearing, accessible names, and shell content. Add a narrow viewport browser assertion that identity and logout remain reachable and do not overlap.
  Dependencies: Tasks 2.1, 3.1

### 5. Authentication surface
- [x] **5.1 Redesign `LoginPage` using shared visual primitives**
  Files: `replicadb-server/frontend/src/pages/LoginPage.tsx`, `replicadb-server/frontend/src/pages/LoginPage.test.tsx`, `replicadb-server/frontend/src/components/SurfaceSection.tsx`
  Changes: Replace page-local Paper/Stack styling with the theme and a focused authentication surface: brand link, compact context text, clear form hierarchy, visible validation/error states, loading/submitting state, and responsive width/padding. Keep the existing `AuthContext` login flow, CSRF bootstrap, error mapping, and redirect unchanged.
  Tests: Preserve successful login, disabled-submit, and API-error scenarios; add assertions for heading/field/button roles, keyboard order, visible error state, and mobile-width content containment.
  Dependencies: Tasks 2.1, 3.1

### 6. Dashboard visual system
- [x] **6.1 Redesign the dashboard header and job list surface**
  Files: `replicadb-server/frontend/src/pages/DashboardPage.tsx`, `replicadb-server/frontend/src/pages/DashboardPage.test.tsx`, `replicadb-server/frontend/src/components/LoadingState.tsx`, `replicadb-server/frontend/src/components/EmptyState.tsx`
  Changes: Use `PageHeader` and `SurfaceSection` for the dashboard heading, New job action, and table container. Improve table hierarchy with a stronger header, stable row spacing, job-name links, and a mode chip/treatment only; do not invent a job status because `JobDefinitionResponse` has no status field. Use shared loading/empty/error presentation while preserving pagination and `listJobs` query behavior. Keep warning indicators out of the general list; warnings remain contextual on job detail/edit pages.
  Tests: Assert visible rows, New job link, pagination, mode rendering, shared empty/loading/error semantics, absence of contextual mode warnings in the general list, keyboard-focusable job links, and semantic table structure. Layout/overflow assertions belong to Task 13.1, not jsdom tests.
  Dependencies: Tasks 3.1, 3.2, 4.1

### 7. Job detail surface
- [x] **7.1 Redesign `JobDetailPage` as a scannable operational definition view**
  Files: `replicadb-server/frontend/src/pages/JobDetailPage.tsx`, `replicadb-server/frontend/src/pages/JobDetailPage.test.tsx`, `replicadb-server/frontend/src/components/SurfaceSection.tsx`, `replicadb-server/frontend/src/components/LoadingState.tsx`
  Changes: Recompose the detail page with a responsive `PageHeader` containing Back to jobs, Trigger run, and Edit actions; group definition rows into source, sink, execution, and lifecycle sections; keep the clearer complete-mode warning only here for the detail route. Use shared loading/error/surface presentation. Preserve API data, trigger mutation, query invalidation, schedule placement, run-history placement, redaction boundaries, and ACL-authoritative errors.
  Tests: Assert grouped headings/values, contextual complete-mode warning text, no warning for complete-atomic, trigger pending/error/navigation behavior, schedule/run-history placement, accessible action names, and shared loading/error semantics. Mobile layout measurements belong to Task 13.1.
  Dependencies: Tasks 3.1, 3.2, 4.1

### 8. Run detail surface
- [x] **8.1 Redesign `RunDetailPage` with shared status/action/error surfaces**
  Files: `replicadb-server/frontend/src/pages/RunDetailPage.tsx`, `replicadb-server/frontend/src/pages/RunDetailPage.test.tsx`, `replicadb-server/frontend/src/components/StatusChip.tsx`, `replicadb-server/frontend/src/components/SurfaceSection.tsx`, `replicadb-server/frontend/src/components/LoadingState.tsx`
  Changes: Use `PageHeader`, `StatusChip`, grouped run metrics, clear cancel/retry action hierarchy, shared warning/error/loading states, and a framed log surface. Keep the exact polling terminal contract, cancellation/retry API calls, query invalidation, navigation, and redacted error display unchanged.
  Tests: Preserve the seven-status action matrix; add assertions for status-chip semantics, grouped metric headings, pending states, warning/error placement, log content/empty state, and keyboard focus order. Browser log overflow belongs to Task 13.1.
  Dependencies: Tasks 3.1, 3.2, 7.1

### 9. Shared operational components
- [x] **9.1 Migrate `RunHistoryTable` and `JobScheduleCard` to shared surfaces/statuses**
  Files: `replicadb-server/frontend/src/components/RunHistoryTable.tsx`, `replicadb-server/frontend/src/components/RunHistoryTable.test.tsx`, `replicadb-server/frontend/src/components/JobScheduleCard.tsx`, `replicadb-server/frontend/src/components/JobScheduleCard.test.tsx`, `replicadb-server/frontend/src/components/StatusChip.tsx`, `replicadb-server/frontend/src/components/SurfaceSection.tsx`, `replicadb-server/frontend/src/components/LoadingState.tsx`, `replicadb-server/frontend/src/components/EmptyState.tsx`
  Changes: Replace page-local Paper/header/status styling with shared primitives, consistent table/dialog surfaces, responsive action wrapping, and the centralized status chip from Task 3.2. Explicitly migrate loading, empty, and error branches to shared state/error treatment; no page-local loading/empty copy remains in these components. Preserve run polling queries, pagination, schedule mutations, delete confirmation, dialog-local errors, and accessible labels.
  Tests: Preserve schedule mutation/error/delete tests and run-history status tests; assert loading/empty/error branches use shared semantics and dialog focus/close behavior. Browser layout assertions belong to Task 13.1.
  Dependencies: Tasks 3.1, 3.2, 8.1

### 10. Job form composition foundation
- [x] **10.1 Recompose `JobFormPage` around shared headers and form sections**
  Files: `replicadb-server/frontend/src/pages/JobFormPage.tsx`, `replicadb-server/frontend/src/pages/JobFormPage.test.tsx`, `replicadb-server/frontend/src/components/PageHeader.tsx`, `replicadb-server/frontend/src/components/SurfaceSection.tsx`
  Changes: Use shared page header and surface sections for Basics, Source, Sink, and Watermark/Execution. Establish consistent form section spacing, required/error presentation, and clear create/edit context. Use a normal action row at the end of the form: inline/right-aligned on desktop and full-width stacked buttons on mobile; do not introduce a sticky overlay. Preserve form state, query/mutation behavior, password helper behavior, source table/query validation, mode-gated watermarks, and payload normalization.
  Tests: Preserve create/edit/error/watermark tests; add assertions for section headings, desktop/mobile action-row structure, keyboard traversal, and save-state text. Browser measurements belong to Task 13.1.
  Dependencies: Tasks 3.1, 2.1, 7.1

### 11. Connection settings visual migration
- [x] **11.1 Apply the shared visual system to `ConnectionSettingsCard`**
  Files: `replicadb-server/frontend/src/components/ConnectionSettingsCard.tsx`, `replicadb-server/frontend/src/components/ConnectionSettingsCard.test.tsx`, `replicadb-server/frontend/src/utils/connectionBuilder.ts`
  Changes: Preserve all connector behavior while improving type selector hierarchy, generated-connection preview, host/port/database grids, Oracle format controls, SQL Server Entra disclosure, Kafka fields, password helper text, parameter editor, focus states, and responsive wrapping. Use shared section styling instead of nested decorative cards. Ensure custom connection strings remain visibly editable and generated values remain read-only.
  Tests: Preserve all connector tests; add assertions for focusable disclosure, SQL Server-only auth visibility, custom connector fallback, generated preview readability, and mobile field order.
  Dependencies: Tasks 2.1, 3.1, 10.1

### 12. Filtering, staging, and file controls
- [x] **12.1 Apply shared form patterns to filtering, file, Kafka, and staging controls**
  Files: `replicadb-server/frontend/src/components/DataFilteringTabs.tsx`, `replicadb-server/frontend/src/components/DataFilteringTabs.test.tsx`, `replicadb-server/frontend/src/components/StagingOptionsTabs.tsx`, `replicadb-server/frontend/src/components/StagingOptionsTabs.test.tsx`, `replicadb-server/frontend/src/pages/JobFormPage.tsx`
  Changes: Harmonize tabs, tab focus/selected states, helper text, file-format controls, toggles, staging schema/table controls, and sink mapping with visual tokens. Preserve table/query exclusivity, reserved connection-parameter keys, sink escape/truncate inversion, file parsing behavior, and validation errors. Avoid adding bottom navigation or a wizard stepper.
  Tests: Preserve current tab/file/staging tests; add keyboard tab navigation, selected-tab semantics, helper/error rendering, and semantic responsive-grid assertions. Browser dimensions belong to Task 13.1.
  Dependencies: Tasks 2.1, 10.1, 11.1

### 13. Responsive behavior pass
- [x] **13.1 Make all authenticated screens stable at desktop and mobile widths**
  Files: `replicadb-server/frontend/src/layout/AppLayout.tsx`, `replicadb-server/frontend/src/pages/LoginPage.tsx`, `replicadb-server/frontend/src/pages/DashboardPage.tsx`, `replicadb-server/frontend/src/pages/JobDetailPage.tsx`, `replicadb-server/frontend/src/pages/JobFormPage.tsx`, `replicadb-server/frontend/src/pages/RunDetailPage.tsx`, `replicadb-server/frontend/src/components/RunHistoryTable.tsx`, `replicadb-server/frontend/src/components/JobScheduleCard.tsx`, `replicadb-server/frontend/e2e/responsive-layout.spec.ts` (new), `replicadb-server/frontend/playwright.config.ts`
  Changes: Define exact browser viewports of `1440x900` and `390x844`; use the existing `PLAYWRIGHT_BASE_URL` and environment-managed login without adding credentials or a new server. Apply responsive constraints for header actions, page headings, tables, detail rows, form grids, dialog actions, log text, and pagination. Keep table/log overflow local to their containers. The e2e spec must assert `document.documentElement.scrollWidth <= innerWidth`, key element bounding boxes stay inside the viewport, header/action groups do not overlap, and table/log containers own their overflow.
  Tests: `responsive-layout.spec.ts` covers login, dashboard, new job, job detail, edit job, and run detail at both viewports using bounding-box comparisons. Missing credentials are handled by the existing explicit e2e guard and reported as environment-blocked.
  Dependencies: Tasks 4.1, 5.1, 6.1, 7.1, 8.1, 9.1, 10.1, 11.1, 12.1

### 14. Accessibility and interaction states
- [x] **14.1 Audit keyboard, focus, labels, contrast, and state-layer behavior**
  Files: `replicadb-server/frontend/src/theme/theme.ts`, `replicadb-server/frontend/src/theme/accessibility.test.ts` (new), `replicadb-server/frontend/src/layout/AppLayout.tsx`, `replicadb-server/frontend/src/pages/LoginPage.tsx`, `replicadb-server/frontend/src/pages/DashboardPage.tsx`, `replicadb-server/frontend/src/pages/JobDetailPage.tsx`, `replicadb-server/frontend/src/pages/JobFormPage.tsx`, `replicadb-server/frontend/src/pages/RunDetailPage.tsx`, `replicadb-server/frontend/src/components/PageHeader.tsx`, `replicadb-server/frontend/src/components/SurfaceSection.tsx`, `replicadb-server/frontend/src/components/StatusChip.tsx`, `replicadb-server/frontend/src/components/ConnectionSettingsCard.tsx`, `replicadb-server/frontend/src/components/DataFilteringTabs.tsx`, `replicadb-server/frontend/src/components/StagingOptionsTabs.tsx`, `replicadb-server/frontend/src/components/RunHistoryTable.tsx`, `replicadb-server/frontend/src/components/JobScheduleCard.tsx`, `replicadb-server/frontend/e2e/accessibility.spec.ts` (new)
  Changes: Make visible focus rings consistent, ensure all icon-only controls have labels/tooltips, associate helper/error text with fields, preserve semantic heading order, ensure tab/dialog semantics, and ensure status is not communicated by color alone. Add a deterministic contrast helper in the Playwright spec for core token combinations and enforce WCAG AA thresholds of 4.5:1 for normal text and 3:1 for large text/UI boundaries; do not add an accessibility dependency.
  Tests: `accessibility.test.ts` asserts semantic labels, focus token values, and token-level contrast pairs. `accessibility.spec.ts` logs in through the existing environment-managed flow, tabs through shell/dashboard/job form controls, asserts the active element is visible, checks accessible names/roles for actions/tabs/dialogs, and evaluates contrast thresholds for representative primary, secondary, warning, error, and muted-text surfaces.
  Dependencies: Tasks 2.1, 3.1, 3.2, 4.1, 5.1, 6.1, 7.1, 8.1, 9.1, 10.1, 11.1, 12.1, 13.1

### 15. Frontend test and selector stabilization
- [x] **15.1 Align tests with semantic UI contracts instead of generated MUI classes**
  Files: `replicadb-server/frontend/src/pages/*.test.tsx`, `replicadb-server/frontend/src/components/*.test.tsx`, `replicadb-server/frontend/src/layout/AppLayout.test.tsx`, `replicadb-server/frontend/src/router/routes.test.tsx`, `replicadb-server/frontend/src/App.test.tsx`, `replicadb-server/frontend/src/test/setup.ts`
  Changes: Replace brittle class/text-position assertions exposed by the visual migration with role, label, heading, link, tab, and accessible-name assertions. Add only stable `data-testid` values where semantic queries cannot distinguish repeated operational elements. Update `App.test.tsx` to provide a fresh QueryClient through the test wrapper rather than relying on the production singleton. Keep API mocks and router setup explicit.
  Tests: Run the complete Vitest suite and verify no test relies on MUI-generated CSS classes, screen coordinates, incidental DOM nesting, or the production QueryClient singleton.
  Dependencies: Tasks 4.1 through 14.1

### 16. Browser visual smoke coverage
- [x] **16.1 Add authenticated desktop/mobile control-plane smoke coverage**
  Files: `replicadb-server/frontend/e2e/visual-control-plane.spec.ts` (new), `replicadb-server/frontend/e2e/README.md` (existing), `replicadb-server/frontend/playwright.config.ts` only if reporter/viewport configuration is required, `replicadb-server/frontend/scripts/seed-local-jobs.mjs` (read-only dependency)
  Changes: Add environment-managed authenticated flows covering login, dashboard, New job, job detail, edit job, run detail, and back-to-home navigation. Use the stable seeded job name `Develop / PostgreSQL source` created by the local development seed; open its detail page, trigger a run, wait for the returned run route, and validate run detail even if the external database later fails. Use desktop/mobile viewports and assert section visibility, no page-level overflow, semantic headings/actions, and contextual warning placement. Do not add a backend fixture contract or hardcode credentials.
  Tests: The Playwright spec itself is the test; run it with configured bootstrap credentials and the local seed when available. If credentials or the seed are absent, fail at an explicit setup guard and classify that as environment-blocked rather than a UI assertion failure.
  Dependencies: Tasks 13.1, 14.1, 15.1

### 17. Documentation and developer workflow
- [x] **17.1 Document the adapted M3 visual rules and local hot-reload workflow**
  Files: `replicadb-server/frontend/README.develop.md`, `replicadb-server/frontend/src/theme/theme.ts` (short token comments only if needed)
  Changes: Document the intentional visual decisions: teal/rust identity, neutral surfaces, desktop top navigation, restrained radius, semantic states, and no purple/bottom-navigation default. Document that Vite HMR is the expected frontend workflow and frontend-only edits do not require restarting the API/PostgreSQL stack. Do not add a separate markdown document.
  Tests: Run a repository text check asserting the documentation mentions `npm run dev`, Vite HMR, port/proxy expectations, teal/rust identity, and the no-bottom-navigation decision; assert it contains no credential literals, resolved DSNs, or machine-specific registry URLs. Run `npm run typecheck` after the documentation edit.
  Dependencies: Tasks 2.1, 4.1, 13.1

### 18. Full frontend regression and production build
- [x] **18.1 Run the final visual-system regression matrix**
  Files: `replicadb-server/frontend/package.json`, `replicadb-server/frontend/src/**`, `replicadb-server/frontend/e2e/**`, `replicadb-server/frontend/scripts/seed-local-jobs.test.mjs`, `replicadb-server/src/main/resources/static/` (generated build output verification only)
  Changes: Run `npm ci` in a clean runner-like environment, `npm run typecheck`, `npm test`, `npm run test:seed`, `npm run build`, and `npm run test:e2e` with environment-managed credentials when available. Before and after build, record the generated static asset file list and total size; verify generated assets remain ignored or intentionally unchanged in version control, and record any bundle-size warning without silently accepting unexpected growth. Check `git diff --check`, stale API/client imports, and all routes. Fix only regressions caused by this visual-system work.
  Tests: Typecheck, all Vitest tests, seed tests, and build must pass. E2e must pass when credentials and local seed are available; otherwise the explicit credential/setup guard is the only accepted environment-blocked result and must be reported separately. Confirm no frontend file introduces credentials, resolved DSNs, machine-specific URLs, or non-registry package configuration.
  Dependencies: All previous tasks

## Technical Reference

<details>
<summary>Types and visual primitives</summary>

- Theme tokens are the source of truth for palette, typography, surfaces, spacing, shape, elevation, focus, and component state overrides.
- `PageHeader` owns title/supporting copy/action composition; pages do not duplicate header spacing rules.
- `SurfaceSection` owns framed operational sections; it must not become a card-within-card decorator.
- `StatusChip` owns run status label/color mapping and reuses the existing `JobRunStatus` union from `src/api/runsApi.ts`.
- `LoadingState` and `EmptyState` own stable asynchronous placeholders without changing TanStack Query behavior.
- Existing generated `src/api/schema.ts` types, API modules, auth context, and router contracts are reused unchanged.
</details>

## Execution Retrospective (auto-generated by /itx-code)

### Plan Accuracy
- Tasks completed as planned: 15/19 (79%)
- Tasks that required plan adjustment: 4/19 (21%)
- Test loop iterations: 8 total (first-pass 4, second-pass 4, third-pass 0)

### Gaps Encountered

#### Gap 1: Theme provider assumptions in existing route tests (Plan-to-Implementation)
- **Task**: 4.1 and 7.1 — shell and authenticated screen migrations
- **Plan assumed**: Existing component and route tests could render the migrated shell and shared surfaces without additional setup.
- **Reality**: Several tests rendered token-backed `sx` callbacks without the production `ThemeProvider`.
- **Resolution**: Updated the affected harnesses to use the shared production theme and fresh query clients where needed.
- **Learning**: Shared theme tokens require a common test wrapper whenever components consume custom theme extensions.

#### Gap 2: Build-only MUI composition typing (Plan-to-Implementation)
- **Task**: 18.1 — final production build
- **Plan assumed**: The no-emit TypeScript check and unit suite would cover the shared section composition types.
- **Reality**: `tsc -b` in the production build exposed stricter `SxProps` array and polymorphic form-handler typing than `tsc --noEmit`.
- **Resolution**: Flattened caller `sx` values in `SurfaceSection` and kept the login form wrapper explicitly typed as a form.
- **Learning**: Run the exact production build early for MUI polymorphic components and custom theme extensions.

#### Gap 3: Accessibility test extension mismatch (Plan-to-Implementation)
- **Task**: 14.1 — accessibility unit coverage
- **Plan assumed**: The planned `.ts` accessibility test could use JSX like the existing `.tsx` tests.
- **Reality**: Vite parsed the file as TypeScript and rejected JSX syntax.
- **Resolution**: Kept the planned filename and used `createElement` for the small render fixture.
- **Learning**: Match test syntax to the exact extension when a plan fixes a new test filename.

#### Gap 4: Existing documentation contained a resolved local DSN (Intent-to-Plan)
- **Task**: 17.1 — developer workflow documentation
- **Plan assumed**: Documentation security checks only needed to prevent newly introduced secrets and machine-specific URLs.
- **Reality**: The existing guide already contained a concrete local JDBC URL, which violated the task's final scan requirement.
- **Resolution**: Replaced the concrete DSN with an environment-managed placeholder while preserving setup guidance.
- **Learning**: Documentation security checks must scan the full edited file, including pre-existing examples.

### Patterns Discovered
- Shared theme wrapper: see `src/theme/theme.ts` and the authenticated page test harnesses.
- Semantic operational section: see `src/components/SurfaceSection.tsx`.
- Text-backed status presentation: see `src/components/StatusChip.tsx`.
- Local overflow ownership: see `src/components/RunHistoryTable.tsx` and `src/pages/RunDetailPage.tsx`.

<details>
<summary>Dependencies</summary>

No new runtime dependency is required. Existing MUI, MUI icons, React Router, TanStack Query, Vitest, Testing Library, and Playwright are sufficient. Do not add remote font loading, an accessibility package, a CSS framework, or a new navigation framework unless a later implementation blocker is explicitly documented and approved.
</details>

<details>
<summary>Testing strategy</summary>

- Theme/primitives: Vitest/Testing Library unit tests for token presence, semantic markup, state styles where inspectable, and accessible names.
- Pages/components: preserve existing API mocks and fresh QueryClient/router setup; use role/label/heading/link queries, not generated MUI class names.
- Responsive/accessibility behavior: Playwright at `1440x900` and `390x844` against the existing local dev/build setup, asserting no page-level overflow, bounding-box containment, no overlap, visible focus, semantic actions, and contrast thresholds.
- Auth/session: do not replace real cookie/CSRF flows with mocked authorization in e2e; use existing environment-managed bootstrap variables.
- Build: `npm ci`, `npm run typecheck`, `npm test`, `npm run test:seed`, `npm run build`, and `npm run test:e2e`; record credential/browser limitations explicitly.
</details>
