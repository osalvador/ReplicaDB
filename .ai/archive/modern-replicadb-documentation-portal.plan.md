# Implementation Plan: Modern ReplicaDB Documentation Portal

## Task Source - JIRA: none - repository documentation modernization

The source is the user request to replace the current Jekyll documentation with
a modern, comprehensive product portal. The accepted requirements are:

- Use an open-source static documentation system deployed to GitHub Pages.
- Use Astro Starlight, English-first content, and one living documentation
  version rather than release-versioned copies.
- Document the standalone CLI and the managed server as related but distinct
  products.
- Cover the complete server frontend, REST API, distributed API/worker
  architecture, scheduling, scaling, concurrency, fairness, leases, fencing,
  retries, cancellation, security, observability, deployment, and recovery.
- Publish one canonical screenshot per frontend screen and additional desktop
  or mobile variants only for pedagogically important states. Keep exhaustive
  visual regression separate from the public screenshot set.
- Preserve the public `/server.html`, `/docs/docs.html`, `/wizard/`, and
  `/markdown/` contracts while migrating the documentation implementation.
- Keep `implementation_plan.md` untouched because it is being executed in a
  separate session. This plan is intentionally stored as
  `implementation_plan_doc.md`.

## Overview

Replace the Jekyll site under `docs/` with a branded Astro Starlight portal that
turns the existing CLI reference, managed-server operations, OpenAPI contract,
and React control plane into one navigable documentation experience. The
migration is performed in place but remains reversible until the final Pages
cutover: legacy URLs and static tools are validated before Jekyll files are
removed, while committed OpenAPI and screenshot artifacts keep the public docs
build independent from Java, PostgreSQL, credentials, and a running server.

## Architecture & Design - Approach: Controlled In-Place Replacement

### Documentation architecture

`docs/` becomes an independent Node 22/Astro project. Starlight owns content,
navigation, Pagefind search, SEO, accessible reading layouts, and static output
in `docs/dist`. Content lives under `docs/src/content/docs`; reusable visual
components live under `docs/src/components`; committed images live under
`docs/src/assets`; and a local committed OpenAPI document lives under
`docs/openapi`.

The public information architecture separates task-oriented guides from
reference material:

```text
ReplicaDB Docs
|- Start here
|  |- Choose CLI or Server
|  |- CLI quickstart
|  `- Server quickstart
|- CLI
|  |- Install and configure
|  |- Replication modes and multi-table execution
|  |- Connectors
|  `- Performance and troubleshooting
|- Server
|  |- Install and sign in
|  |- Dashboard, datasources, jobs, schedules, and runs
|  `- Users, permissions, profile, and audit
|- Architecture
|  |- Core and managed boundaries
|  |- API/worker topology and run lifecycle
|  `- Concurrency, scaling, fairness, retries, and fencing
|- Operations
|  |- Topologies, configuration, capacity, health, and metrics
|  `- Security, recovery, backup, upgrades, and troubleshooting
|- API reference
`- Project reference
```

### Artifact and ownership boundaries

- `docs/openapi/replicadb-server.json` is generated from the tested Springdoc
  endpoint, committed, and consumed locally by `starlight-openapi`. A drift
  gate regenerates it to a temporary file and diffs it. The public reference is
  read-only because the API uses session cookies and CSRF; the docs must not
  proxy requests or encourage sending credentials through a third party.
- `docs/src/assets/screenshots/server/` contains the curated public image set.
  A dedicated Playwright spec recreates those files from deterministic local
  fixtures and verifies the DOM does not contain bootstrap secrets, metadata
  credentials, lease tokens, or unredacted datasource security.
- `docs/wizard/` and `docs/markdown/` remain source-owned tools. A bounded
  staging script copies only their runtime files to `docs/dist/wizard` and
  `docs/dist/markdown`; dependency directories, tests, reports, and internal
  planning files are never published.
- GitHub Pages has no server-side redirect support. Astro compatibility pages
  preserve `/server.html` and `/docs/docs.html`, retain fragments through an
  explicit legacy-anchor map, expose canonical links, and provide a visible
  fallback link when JavaScript is unavailable.
- Root `README.md`, `DEPLOYMENT.md`, `RELEASE_GUIDE.md`, and module READMEs stay
  concise repository entry points. Public product guidance is canonical in the
  Starlight site; repository docs link to it and retain only contributor or
  release-specific procedures that do not belong on the public site.

### Visual direction

The portal extends the existing "Engineering Ledger" system from `DESIGN.md`:
deep teal primary actions, terracotta secondary emphasis, cool green-neutral
surfaces, editorial serif headings, sans-serif operational detail, restrained
8px corners, flat resting surfaces, and explicit semantic state colors. It
uses Starlight's accessible structure and overrides only tokens or components
needed to establish ReplicaDB's identity. Architecture diagrams are rendered
through a small local Mermaid component with adjacent textual explanations, so
the content remains understandable if diagram JavaScript fails.

### Performance, security, and compatibility

- Generate static HTML, hashed assets, local Pagefind search, and responsive
  AVIF/WebP or optimized PNG/JPEG output where the source permits it.
- Lazy-load below-the-fold screenshots, specify stable dimensions, and enforce
  an image-size budget so the screenshot inventory does not make guides slow.
- Never embed environment values, real endpoints, passwords, tokens, keyrings,
  datasource security maps, lease tokens, or real user data in Markdown,
  OpenAPI examples, screenshots, manifests, logs, or Pages artifacts.
- Preserve Java 17 and Node 22 project baselines. The docs build itself requires
  Node only; OpenAPI and screenshot regeneration are explicit maintenance gates
  with their own Java/PostgreSQL/browser prerequisites.
- CI-invoked shell validation uses baseline POSIX tools and avoids quiet
  early-closing pipeline consumers under `pipefail`.

## Implementation Tasks

### 1. Establish the Starlight build foundation

- [x] **1.1 Create the independent Astro Starlight project in `docs/`**
  Files: `docs/package.json` (new), `docs/package-lock.json` (new),
  `docs/astro.config.mjs` (new), `docs/tsconfig.json` (new),
  `docs/src/content.config.ts` (new), `docs/src/content/docs/index.mdx` (new),
  `docs/public/robots.txt` (new), `docs/public/.nojekyll` (new), `.gitignore`
  Changes: Add pinned Astro, Starlight, MDX, OpenAPI, Mermaid, image, and test
  dependencies with Node 22-compatible scripts for `dev`, `check`, `build`,
  `preview`, and tests. Configure the production site and `/ReplicaDB` base
  path for `https://osalvador.github.io/ReplicaDB`, Pagefind search, sitemap,
  GitHub edit links, English UI, dark/light themes, and a minimal placeholder
  homepage. Keep the nested `docs/markdown/package.json` project independent
  and exclude generated `docs/dist`, Astro cache, and test output.
  Tests: Assert `node --version` has major version 22 and that `npm` and `git`
  are available; run `npm ci`, `npm run check`, and `npm run build` from `docs`; assert
  `docs/dist/index.html`, search data, sitemap, robots file, and `.nojekyll`
  exist and all generated asset URLs include the configured repository base.
  Dependencies: None

### 2. Apply the ReplicaDB documentation design system

- [x] **2.1 Implement the Engineering Ledger theme and reusable reading components**
  Files: `docs/src/styles/custom.css` (new),
  `docs/src/components/ArchitectureDiagram.astro` (new),
  `docs/src/components/ProductChoice.astro` (new),
  `docs/src/components/ScreenshotFrame.astro` (new),
  `docs/src/components/SupportMatrix.astro` (new),
  `docs/src/assets/brand/replicadb-logo.png` (new),
  `docs/tests/design-contract.test.mjs` (new), `docs/astro.config.mjs`,
  `docs/package.json`, `docs/package-lock.json`
  Changes: Define `--replicadb-brand-teal: #0B6E69`,
  `--replicadb-terracotta: #B15C38`, `--replicadb-page-green: #F3F6F4`,
  `--replicadb-paper: #FFFFFF`, `--replicadb-mist-green: #E8F0ED`,
  `--replicadb-ink: #1B2926`, and `--replicadb-muted-ink: #50625D`, plus the
  exact success/info/warning/error values from `DESIGN.md`. Use this explicit
  Starlight scale for dark mode: accent low/accent/high
  `#113B38/#57C2B7/#D7F3EF`, white `#FFFFFF`, grays 1-6
  `#E8F0ED/#C8D5D1/#91A39E/#63756F/#3D4C48/#26332F`, and black
  `#17211F`. Use this light-mode scale: accent low/accent/high
  `#DCEBE7/#0B6E69/#064A47`, white `#1B2926`, grays 1-6
  `#33423E/#50625D/#6F817B/#A4B2AE/#CFD9D6/#E8F0ED`, and black
  `#FFFFFF`. Map those values once in `:root` and `[data-theme='light']` to
  Starlight's `--sl-color-accent-low`, `--sl-color-accent`,
  `--sl-color-accent-high`, `--sl-color-white`, `--sl-color-gray-1` through
  `--sl-color-gray-6`, and `--sl-color-black`. Set `--sl-font` to the
  Avenir Next/Helvetica Neue fallback chain, set headings to Georgia/Times New
  Roman, and define radius tokens at 4px, 6px, and 8px. Permit color and radius
  literals only in these token scopes; components consume variables. Add narrowly scoped
  overrides and accessible components for CLI/server choice,
  Mermaid architecture diagrams, captioned screenshots, and connector support
  matrices. Keep diagrams responsive and provide text equivalents; avoid a
  generic marketing hero, decorative gradients, nested cards, and oversized
  headings in reference pages.
  Tests: Add Node contract tests for the exact product/Starlight token mapping,
  image dimensions, alt/caption requirements, and color/radius literals outside
  the two token scopes;
  build light and dark pages and assert Mermaid source, textual fallback,
  landmarks, focusable navigation, and stable screenshot aspect ratios are
  present in generated HTML. At 390px and 1440px, assert ProductChoice stacks
  without clipping, ScreenshotFrame stays inside its content column, and
  ArchitectureDiagram exposes readable fallback content with scripts blocked.
  Dependencies: Task 1.1

### 3. Preserve the standalone browser tools

- [x] **3.1 Stage the configuration wizard and Markup Forge into the Astro output**
  Files: `docs/scripts/stage-static-tools.mjs` (new),
  `docs/tests/static-tools.test.mjs` (new), `docs/package.json`,
  `docs/wizard/index.html`, `docs/wizard/css/**`, `docs/wizard/js/**`,
  `docs/wizard/vendor/**`, `docs/markdown/converter.html`,
  `docs/markdown/converter-old.html`, `docs/markdown/converter-app.js`,
  `docs/markdown/converter-core.js`, `docs/markdown/editor-cm.js`,
  `docs/markdown/preview-sync.js`, `docs/markdown/sw.js`,
  `docs/markdown/manifest.webmanifest`, `docs/markdown/assets/**`,
  `docs/markdown/*icon*`
  Changes: Add a post-build staging script with an explicit runtime allowlist.
  Preserve `/wizard/index.html` and `/markdown/converter.html` while excluding
  `node_modules`, package metadata, test sources/results, context notes,
  roadmaps, and migration plans from the public artifact. Keep both tools'
  relative asset paths and service-worker scope functional under `/ReplicaDB`.
  Copy each runtime subtree intact rather than rewriting its `./` URLs; update
  only the wizard's absolute documentation links through Task 4.1. Require the
  Markdown manifest `start_url` and service-worker registration to remain
  relative and scope the worker to `/ReplicaDB/markdown/`.
  Do not redesign or alter tool behavior as part of the docs migration.
  Tests: Unit-test that staging preserves a relative URL fixture and rejects an
  absolute-root asset fixture; run the existing Markup Forge Vitest and Playwright suites, build the
  docs, serve `docs/dist`, and verify both legacy tool URLs load without 404s,
  their CSS/JS/manifest assets resolve, the wizard still generates a
  configuration, the Markdown editor still previews content, and no excluded
  source/dependency/report path is present in `docs/dist`.
  Dependencies: Task 1.1

### 4. Lock public routes before moving content

- [x] **4.1 Add legacy compatibility pages and an executable route contract**
  Files: `docs/src/data/legacy-routes.ts` (new),
  `docs/src/pages/server.html.astro` (new),
  `docs/src/pages/docs/docs.html.astro` (new),
  `docs/src/components/LegacyRedirect.astro` (new),
  `docs/tests/legacy-routes.test.mjs` (new),
  `docs/wizard/index.html`, `README.md`
  Changes: Inventory every repository-owned link to the published site and map
  `/server.html`, `/docs/docs.html`, and their currently referenced fragments
  to canonical Starlight routes. Generate static compatibility HTML suitable
  for GitHub Pages with canonical metadata, fragment-preserving client
  navigation, and a no-script/manual fallback. Implement the inventory in
  `legacy-routes.test.mjs`: scan tracked Markdown, HTML, JavaScript, YAML, and
  shell files with Node, extract same-origin absolute URLs and known
  root-relative public paths, and require each discovered path/fragment in the
  typed map so future links fail until mapped. Update the wizard and root links
  only after mapped targets exist; retain compatibility pages indefinitely for
  published release and search-engine links.
  Tests: Parse all tracked repository Markdown/HTML/JS/YAML/shell files for
  `osalvador.github.io/ReplicaDB` URLs and fail when a repository-owned path is
  absent from the route map or generated output. Serve `docs/dist` and verify
  old top-level and deep-fragment URLs reach the intended canonical pages while
  `/wizard/` and `/markdown/` remain unchanged.
  Dependencies: Tasks 1.1 and 3.1

### 5. Build the product entry experience

- [x] **5.1 Replace the monolithic homepage with task-oriented getting-started guides**
  Files: `docs/src/content/docs/index.mdx`,
  `docs/src/content/docs/getting-started/choose-cli-or-server.mdx` (new),
  `docs/src/content/docs/getting-started/cli-quickstart.md` (new),
  `docs/src/content/docs/getting-started/server-quickstart.md` (new),
  `docs/src/content/docs/getting-started/concepts.md` (new),
  `docs/astro.config.mjs`, `docs/tests/content-contract.test.mjs` (new)
  Changes: Present ReplicaDB as a high-performance, non-intrusive bulk
  replication product with two artifacts: a standalone CLI requiring no
  metadata database and a managed server with authenticated durable operation.
  Add explicit decision guidance, prerequisites, secure quickstarts, next-step
  links, and a concise glossary of source, sink, job, task, run, attempt,
  datasource, watermark, API, and worker. Remove unsupported benchmark and
  roadmap claims from the entry experience.
  Tests: Validate required choice/quickstart sections and code blocks, reject
  credentials or literal secret examples, extract fenced blocks tagged `bash`
  or `sh`, replace documented `<placeholder>` values with inert shell words,
  and run `bash -n` on each block; assert each quickstart links to its
  installation, configuration, troubleshooting, and security follow-up.
  Dependencies: Tasks 2.1 and 4.1

### 6. Migrate and restructure the complete CLI reference

- [x] **6.1 Split the existing CLI manual into focused guides without changing its contract**
  Files: `docs/src/content/docs/cli/index.md` (new),
  `docs/src/content/docs/cli/installation.md` (new),
  `docs/src/content/docs/cli/configuration.md` (new),
  `docs/src/content/docs/cli/replication-modes.md` (new),
  `docs/src/content/docs/cli/parallelism.md` (new),
  `docs/src/content/docs/cli/filtering-and-queries.md` (new),
  `docs/src/content/docs/cli/multi-table.md` (new),
  `docs/src/content/docs/cli/incremental-watermarks.md` (new),
  `docs/src/content/docs/cli/performance.md` (new),
  `docs/src/content/docs/cli/troubleshooting.md` (new),
  `docs/src/content/docs/reference/cli-options.md` (new),
  `docs/src/content/docs/reference/example-options-files.md` (new),
  `docs/tests/cli-contract.test.mjs` (new)
  Changes: Migrate all supported information from `docs/docs/docs.md` and
  `README.md`, deduplicate it, and organize it by user task. Preserve exact CLI
  argument names, options-file keys, defaults, precedence, exit codes,
  multi-table sequential behavior, mode semantics, staging ownership,
  watermark commit rules, cancellation risks, and environment substitution.
  Clearly mark unsupported CDC/resume behavior and manager-specific limits.
  Use `src/main/java/org/replicadb/cli/ToolOptions.java`, assembled
  `replicadb --help`, `conf/_replicadb.conf`, focused `ToolOptions*Test` classes,
  and `scripts/phase3-cli-compatibility.sh` as the evidence set.
  `cli-contract.test.mjs` extracts documented long options and property keys,
  rejects names absent from that evidence, and accepts divergence only through
  an explicit documented-deprecation allowlist.
  Tests: Build the release-compatible CLI, capture help output, and compare it
  with documented option names and modes. Verify complete,
  complete-atomic, incremental, multi-table, watermark, exit-code, and
  precedence sections exist. Run two temporary SQLite scenarios from the
  documented examples: a legacy single-table complete copy and a sequential
  two-table catalog; assert exit code 0, destination row counts,
  CLI-over-options-file precedence, no metadata connection, and cleanup. Reuse
  the existing compatibility gate for malformed invocation exit code 1 and
  cancellation exit code 2.
  Dependencies: Task 5.1

### 7. Publish connector-specific guidance and capability truth

- [x] **7.1 Replace the duplicated compatibility table with evidence-backed connector pages**
  Files: `docs/src/content/docs/connectors/index.mdx` (new),
  `docs/src/content/docs/connectors/oracle.md` (new),
  `docs/src/content/docs/connectors/postgresql.md` (new),
  `docs/src/content/docs/connectors/mysql-mariadb.md` (new),
  `docs/src/content/docs/connectors/sql-server.md` (new),
  `docs/src/content/docs/connectors/db2.md` (new),
  `docs/src/content/docs/connectors/sqlite.md` (new),
  `docs/src/content/docs/connectors/mongodb.md` (new),
  `docs/src/content/docs/connectors/csv.md` (new),
  `docs/src/content/docs/connectors/amazon-s3.md` (new),
  `docs/src/content/docs/connectors/kafka.md` (new),
  `docs/src/content/docs/connectors/denodo.md` (new),
  `docs/src/content/docs/connectors/generic-jdbc.md` (new),
  `docs/src/data/connector-capabilities.json` (new),
  `docs/tests/connector-capabilities.test.mjs` (new),
  `src/test/java/org/replicadb/docs/DocumentationConnectorContractTest.java` (new)
  Changes: Create one structured capability source consumed by the support
  matrix and connector pages. For each implemented manager, document source
  and sink roles, supported modes, connection form, driver/runtime caveats,
  type/staging/partition behavior, security options, examples, and known
  limitations. Remove unimplemented "coming soon" stores from the capability
  graphic and ensure Azure SQL/Microsoft Entra guidance is represented under
  SQL Server without implying headless support for interactive authentication.
  Require each JSON entry to contain `id`, `displayName`, nonempty `schemes`,
  `roles` (`source` and/or `sink`), `modes` drawn from `complete`,
  `complete-atomic`, and `incremental`, `bandwidthThrottling`, `staging`,
  `page`, and `caveats`. The Java contract test loads this JSON and compares its
  complete scheme/id set with `SupportedManagers` and `ManagerFactory`, so a
  manager addition or removal forces a documentation update.
  Tests: Validate the JSON shape in Node and compare connector identifiers and modes with `SupportedManagers`,
  `ManagerFactory`, concrete manager capabilities, and maintained integration
  test families; fail on duplicate capability tables, unsupported green checks,
  missing connector pages, unsafe credential examples, or matrix/page drift.
  Dependencies: Tasks 2.1 and 6.1

### 8. Document every managed-server workflow

- [x] **8.1 Create the complete server user guide around the actual route and role model**
  Files: `docs/src/content/docs/server/index.md` (new),
  `docs/src/content/docs/server/installation.md` (new),
  `docs/src/content/docs/server/sign-in-and-profile.md` (new),
  `docs/src/content/docs/server/dashboard.md` (new),
  `docs/src/content/docs/server/datasources.md` (new),
  `docs/src/content/docs/server/jobs.md` (new),
  `docs/src/content/docs/server/schedules.md` (new),
  `docs/src/content/docs/server/runs-and-diagnostics.md` (new),
  `docs/src/content/docs/server/users.md` (new),
  `docs/src/content/docs/server/permissions.md` (new),
  `docs/src/content/docs/server/audit.md` (new),
  `docs/src/content/docs/server/errors-and-empty-states.md` (new),
  `docs/tests/server-route-coverage.test.mjs` (new)
  Changes: Document all current frontend routes and journeys from login through
  monitoring, datasource/job creation and editing, scheduling, triggering,
  cancelling, retrying, diagnostics, users, ACLs, audit, profile, unauthorized,
  loading, empty, validation, conflict, and recoverable error states. State
  role and resource-permission requirements without treating hidden frontend
  controls as authorization. Explain secret-preserving edit behavior and
  destructive complete-mode warnings: edit responses never rehydrate stored
  passwords, blank secret inputs preserve encrypted values,
  `clearSecurityKeys` is the explicit removal path, and interrupted complete
  mode may leave a truncated sink. Tie each documented action to the visible
  label in its owning page and a maintained route/action inventory.
  Tests: Parse `replicadb-server/frontend/src/router/routes.tsx` and navigation
  labels to require a mapped guide section for every route and ADMIN boundary;
  assert documented actions match available controls, RFC 7807 states and ACL
  vocabulary. Scan prohibited field names using the OpenAPI test's allowlist
  and compare documented action labels with labels extracted from owning page
  components; fail if lease tokens, resolved credentials, or an absent action
  appears in the guide.
  Dependencies: Tasks 5.1 and 7.1

### 9. Explain the distributed execution model

- [x] **9.1 Publish architecture guides grounded in durable state and runtime ownership**
  Files: `docs/src/content/docs/architecture/overview.mdx` (new),
  `docs/src/content/docs/architecture/core-and-server-boundaries.md` (new),
  `docs/src/content/docs/architecture/distributed-topology.mdx` (new),
  `docs/src/content/docs/architecture/run-lifecycle.mdx` (new),
  `docs/src/content/docs/architecture/dispatch-and-recovery.mdx` (new),
  `docs/src/content/docs/architecture/concurrency-and-fencing.mdx` (new),
  `docs/src/content/docs/architecture/scheduling-and-ha.mdx` (new),
  `docs/src/content/docs/architecture/scaling-and-fairness.mdx` (new),
  `docs/src/content/docs/architecture/security-boundaries.mdx` (new),
  `docs/tests/architecture-contract.test.mjs` (new)
  Changes: Explain CLI/core/server dependency direction; API and worker profile
  responsibilities; PostgreSQL durable state; Quartz JDBC clustering;
  notification and mandatory polling paths; atomic claims with
  `FOR UPDATE SKIP LOCKED`; lease heartbeat and token fencing; durable remote
  cancellation; retry attempt chains; overlap prevention; two-lane worker
  admission; approximate rather than round-robin fairness; and the capacity
  formula `workers * concurrent runs per worker * jobs per run`. Include run
  lifecycle and failure-recovery diagrams plus text equivalents. Explicitly
  state that notifications are wake-ups, retries restart, and stale workers
  cannot finalize or advance watermarks. Cite `JobRunRepository`,
  `RunLeaseService`, `RunFinalizationService`, `HeartbeatService`,
  `WorkerDispatchCoordinator`, `ScheduleReconciler`, and their focused tests in
  source comments. Bind capacity terms to worker-instance count,
  `replicadb.worker.max-concurrent-runs`, and the job's core `jobs` option, and
  state that datasource, database, and network limits can reduce effective
  throughput below the arithmetic maximum.
  Tests: Require every architecture invariant and limitation named above,
  render each diagram in light/dark builds, validate diagram IDs/links and text
  alternatives, and compare configuration defaults and state names against
  `application-api.yml`, `application-worker.yml`, `JobRunStatus`, and
  `WorkerRuntimeProperties`. Require every claim/lease/finalization assertion
  to reference an existing source or test path and compare all three capacity
  terms with their current configuration and DTO names.
  Dependencies: Tasks 2.1 and 8.1

### 10. Turn deployment material into operational runbooks

- [x] **10.1 Add deployment, observability, security, and recovery operations guides**
  Files: `docs/src/content/docs/operations/index.md` (new),
  `docs/src/content/docs/operations/local-server.md` (new),
  `docs/src/content/docs/operations/distributed-deployment.md` (new),
  `docs/src/content/docs/operations/configuration.md` (new),
  `docs/src/content/docs/operations/capacity-planning.md` (new),
  `docs/src/content/docs/operations/health-and-metrics.md` (new),
  `docs/src/content/docs/operations/security-and-tls.md` (new),
  `docs/src/content/docs/operations/key-management.md` (new),
  `docs/src/content/docs/operations/backups-and-restore.md` (new),
  `docs/src/content/docs/operations/upgrades.md` (new),
  `docs/src/content/docs/operations/failure-recovery.md` (new),
  `docs/src/content/docs/operations/troubleshooting.md` (new),
  `docs/src/content/docs/reference/environment-variables.md` (new)
  Changes: Reorganize `DEPLOYMENT.md` into user-facing topology and runbook
  pages for embedded local mode, external PostgreSQL, multi-API/multi-worker
  deployment, pool headroom, admission settings, probabilistic fairness,
  health/readiness, bounded metrics, listener degradation, logs, TLS, sessions,
  login throttling, encrypted datasource keyrings, rotation, backup/restore,
  Flyway/Quartz rollout, graceful shutdown, worker loss, cancellation, retry
  recovery, and upgrade boundaries. Keep worker management private and mark
  mixed RAM/JDBC scheduler ownership as prohibited.
  Tests: Extend the portable documentation gate to compare documented
  environment names/defaults with application YAML and Compose, assert every
  exposed health/metrics path has an interpretation and action, verify recovery
  runbooks distinguish liveness from readiness and notification latency from
  correctness. Probe freshly packaged API and worker processes for
  `/actuator/health`, `/actuator/health/liveness`,
  `/actuator/health/readiness`, `/actuator/metrics`, and
  `/actuator/prometheus`; compare documented endpoint availability and bounded
  claim, renewal, recovery, outcome, listener, polling, queue-age, and
  worker-capacity metric families with `ManagedRuntimeMetrics`. Run
  secret-pattern scans over source and built HTML.
  Dependencies: Task 9.1

### 11. Generate a safe, drift-checked API reference

- [x] **11.1 Export the tested Springdoc schema and render it through Starlight OpenAPI**
  Files: `replicadb-server/src/test/java/org/replicadb/server/OpenApiSpecificationIT.java`,
  `docs/openapi/replicadb-server.json` (new),
  `docs/scripts/update-openapi.sh` (new),
  `docs/tests/openapi-contract.test.mjs` (new),
  `docs/src/content/docs/api/index.md` (new), `docs/astro.config.mjs`,
  `docs/package.json`, `docs/package-lock.json`
  Changes: Extend the existing authenticated MockMvc OpenAPI integration test
  with an opt-in output property that writes the exact validated JSON to a
  requested path without weakening its contract assertions. Add a portable
  update script that generates to a temporary file, parses JSON, recursively
  sorts object keys lexicographically, preserves array order and every semantic
  field including `info`, `servers`, examples, and operation/schema order, and
  writes UTF-8 JSON with two-space indentation and one final newline. Do not
  remove timestamps, versions, URLs, or environment-dependent values; make the
  MockMvc profile deterministic and fail if one enters the contract. Validate
  canonicalization is idempotent before atomically replacing the committed
  schema. Configure `starlight-openapi` to generate grouped, searchable,
  read-only endpoint pages and code examples from the local file. Add an API
  introduction covering session/CSRF authentication, permissions, pagination,
  idempotency, RFC 7807 errors, and why the public docs do not provide a live
  request proxy.
  Tests: Run `OpenApiSpecificationIT`, canonicalize the same output twice and
  assert byte identity, regenerate to a temporary path and diff
  against the committed JSON, build every generated endpoint route, require
  jobs/runs/dashboard/datasources/security/audit groups, validate referenced
  schemas and response codes. Scan every JSON key/string with a maintained
  prohibited-name list shared with existing OpenAPI assertions: `leaseToken`,
  `encryptedSecurity`, password/bootstrap property names, JDBC/URI credential
  values, private-key material, and `org.replicadb.server` implementation class
  names must be absent.
  Dependencies: Tasks 1.1 and 8.1

### 12. Produce deterministic frontend documentation screenshots

- [x] **12.1 Add isolated fixtures and a curated Playwright screenshot capture project**
  Files: `replicadb-server/frontend/e2e/docs-screenshots.spec.ts` (new),
  `replicadb-server/frontend/e2e/visual-regression.spec.ts` (new),
  `replicadb-server/frontend/e2e/support/docsScreenshotFixtures.ts` (new),
  `replicadb-server/frontend/scripts/seed-docs-screenshots.mjs` (new),
  `replicadb-server/frontend/scripts/seed-docs-screenshots.test.mjs` (new),
  `replicadb-server/frontend/playwright.docs.config.ts` (new),
  `replicadb-server/frontend/package.json`,
  `replicadb-server/frontend/package-lock.json`,
  `docs/src/data/screenshots.ts` (new),
  `docs/src/assets/screenshots/server/*.png` (new),
  `replicadb-server/frontend/e2e/visual-regression.spec.ts-snapshots/*.png` (new)
  Changes: Create deterministic, docs-only fixture names and states using the
  existing authenticated API and local-seeding contract. Capture one canonical
  desktop image for login, dashboard, profile, jobs, new/edit/detail job, run
  detail, datasources, new/edit/detail datasource, job/datasource permissions,
  users, audit, and unauthorized access. Add selected mobile captures for login,
  dashboard, catalogs, run detail, and navigation plus selected error/empty,
  running/failed, truncated-log, grant-dialog, and conflict states. Disable
  motion; fix locale to `en-US`, timezone to `UTC`, color scheme, viewport,
  device scale 1, and a frozen clock; navigate through the SPA and wait on
  named headings/API responses rather than network idle. Use the prefix
  `Docs /` and fixed test-only UUIDs in an isolated dataset, with one Playwright
  worker for curated capture. Use the healthy API for normal screens and
  schema-typed route fixtures for pedagogical states: fixed RUNNING and FAILED
  runs, a fixed 256 KiB-truncated log response and marker, an RFC 7807 409
  conflict, empty collections, and a pre-opened grant dialog. Isolate mutable jobs/users per test, and produce a typed manifest with route,
  viewport, state, alt text, caption, and owning guide.
  Keep public capture and exhaustive regression separate:
  `docs-screenshots.spec.ts` writes only manifest-listed assets under
  `docs/src/assets`, while `visual-regression.spec.ts` uses
  `toHaveScreenshot` baselines under frontend E2E for the broader
  state/viewport matrix and never stages them into the portal.
  Tests: Unit-test fixture generation, prohibited fields, and manifest uniqueness; run the capture
  project against a freshly packaged healthy server and isolated PostgreSQL
  fixture; assert every manifest file exists with expected dimensions and a
  nonblank pixel sample. Run every mocked state twice in the same pinned browser
  and require identical SHA-256 output. Assert
  `document.documentElement.scrollWidth <= viewport.width` and compare bounding
  boxes for visible app bar/drawer/main/dialog regions, rejecting intersections
  outside intentional overlays;
  and inspect visible DOM text before every capture for bootstrap credentials,
  metadata connection values, security fields, lease tokens, random real user
  data, and unstable timestamps.
  Dependencies: Task 8.1

### 13. Integrate curated media into the guides

- [x] **13.1 Place screenshots and refreshed diagrams where they teach a workflow**
  Files: `docs/src/data/screenshots.ts`,
  `docs/src/components/ScreenshotFrame.astro`,
  `docs/src/content/docs/server/*.md`,
  `docs/src/content/docs/server/*.mdx`,
  `docs/src/content/docs/getting-started/*.md`,
  `docs/src/content/docs/architecture/*.mdx`,
  `docs/src/assets/media/replication-modes/*` (new),
  `docs/src/assets/media/connector-examples/*` (new),
  `docs/tests/media-contract.test.mjs` (new)
  Changes: Embed the canonical screenshot for every server surface and only the
  agreed high-value state/mobile variants. Replace the outdated conceptual
  image with current architecture diagrams and migrate still-accurate mode/S3
  media with descriptive captions. Optimize large raster assets, preserve
  inspectable UI detail, set stable dimensions, lazy-load noncritical images,
  and avoid duplicating screenshots that do not add instructional value. Limit
  each desktop capture to 500 KiB, each mobile capture to 300 KiB, all curated
  captures to 12 MiB total, and any guide's eagerly loaded media to 750 KiB;
  optimize non-inspection media when needed without making UI text unreadable.
  Tests: Validate that every screenshot manifest entry has exactly one owning
  guide and every server route has a canonical image; fail on orphaned files,
  missing alt/caption text, broken image imports, images above the agreed byte
  budget, outdated "coming soon" labels, or mobile/desktop media that overflows
  generated pages.
  Dependencies: Tasks 7.1, 9.1, and 12.1

### 14. Add executable documentation quality gates

- [x] **14.1 Validate content, routes, links, accessibility, search, and responsive rendering**
  Files: `docs/scripts/validate-docs.mjs` (new),
  `docs/playwright.config.ts` (new),
  `docs/tests/e2e/docs-site.spec.ts` (new),
  `docs/tests/e2e/accessibility.spec.ts` (new),
  `docs/tests/e2e/responsive.spec.ts` (new),
  `docs/package.json`, `docs/package-lock.json`
  Changes: Add a post-build validator for internal routes, fragments, images,
  canonical URLs, sidebar coverage, required metadata, duplicate titles,
  prohibited secret patterns, and legacy compatibility output. Add browser
  tests against `docs/dist` for Pagefind search, keyboard navigation, light/dark
  themes, code copy, diagrams, OpenAPI pages, screenshots, 404 handling, static
  tools, and layouts at representative desktop/mobile widths. Use
  `@axe-core/playwright` with WCAG 2.2 A/AA tags; scope any false-positive
  suppression to one documented selector/rule and fail serious or critical
  impact. Centralize secret detection in `validate-docs.mjs` with an explicit
  key-name list plus URI user-info, JDBC credential parameter, private-key
  header, bearer-token, and high-entropy assignment patterns. Scan source text,
  OpenAPI JSON, fixture DOM text, and built HTML, allowing only explicit
  placeholder syntax. Treat external
  link reachability as a separately reportable check so transient network
  failures do not hide deterministic product-doc failures.
  Tests: Run Node tests, `astro check`, production build, validator, and
  Playwright in one `npm run test:all` command; verify the validator catches an
  intentionally broken route/fragment/asset and injected secret fixture; run
  accessibility scans with no serious/critical violations and pixel/containment
  checks showing nonblank, nonoverlapping content at 1440x900 and 390x844.
  Dependencies: Tasks 3.1, 4.1, 11.1, and 13.1

### 15. Define canonical ownership and remove repository-level duplication

- [x] **15.1 Align repository entry documents with the new portal**
  Files: `README.md`, `DEPLOYMENT.md`, `RELEASE_GUIDE.md`,
  `replicadb-server/README.md`,
  `replicadb-server/frontend/README.develop.md`,
  `docs/CONTRIBUTING.md` (new), `scripts/check-phase3-docs.sh`,
  `docs/scripts/validate-docs.mjs`
  Changes: Make the portal the canonical user-facing source while keeping root
  and module files focused on repository setup, development, deployment gates,
  and release operations. Replace duplicated installation/configuration text
  with stable links where safe, retain operational details required by local
  scripts, document which source must change for each subject, and update the
  Phase 3 documentation checker to validate equivalent public pages as well as
  repository contracts. Fix stale release/version wording without changing the
  release process owned by the concurrent plan. Keep release asset creation,
  checksums, and tagging in `RELEASE_GUIDE.md`; keep contributor build/local
  topology commands in module READMEs; keep script-required Phase 3/4
  invariants in `DEPLOYMENT.md`; replace duplicated end-user installation,
  connector, UI, and troubleshooting narratives with portal links. Make
  `check-phase3-docs.sh` require the public operations pages for V15 through
  V20, key management, pool headroom, admission/fairness, RAM/JDBC handoff,
  diagnostics, and Phase 4 acceptance while retaining checks against actual
  YAML/Compose settings and stale Phase-status text.
  Tests: Run `scripts/check-phase3-docs.sh` against the repository and against
  temporary stale/secret fixtures, scan for duplicated large headings and old
  Jekyll URLs, verify all root/module links resolve in the built site, and assert
  contributor guidance names the OpenAPI, screenshot, connector-matrix, and
  architecture-default update commands.
  Dependencies: Tasks 6.1, 10.1, 11.1, and 14.1

### 16. Gate documentation changes in pull requests

- [x] **16.1 Add a focused docs CI job and cross-artifact drift checks**
  Files: `.github/workflows/docs.yml` (new),
  `.github/workflows/CT_Push.yml`, `docs/package.json`,
  `docs/scripts/classify-changes.mjs` (new),
  `docs/tests/change-classification.test.mjs` (new),
  `docs/scripts/update-openapi.sh`,
  `replicadb-server/frontend/package.json`
  Changes: Add a path-filtered pull-request/push workflow using Node 22 and npm
  caches to install, check, build, validate, and browser-test the docs. Add
  explicit jobs or steps for OpenAPI drift when server/API files change and
  screenshot drift when frontend visual/route files change, reusing freshly
  packaged server health and isolated E2E fixtures rather than an old JAR.
  Implement classification in a tested Node script fed by
  `git diff --name-only`: `docs/**`, `README.md`, `DEPLOYMENT.md`,
  `RELEASE_GUIDE.md`, `replicadb-server/README.md`,
  `replicadb-server/frontend/README.develop.md`, and
  `scripts/check-phase3-docs.sh` trigger the base docs job;
  `replicadb-server/src/main/java/**`, `replicadb-server/pom.xml`, and the
  OpenAPI integration test additionally trigger schema drift;
  `replicadb-server/frontend/src/**`, `replicadb-server/frontend/e2e/**`,
  `replicadb-server/frontend/scripts/seed-docs-screenshots*`, and frontend
  package/Playwright files additionally trigger curated capture and separate
  visual-regression checks. Generated OpenAPI, screenshot, and baseline paths
  trigger their owning drift check.
  Remove the effective docs blind spot created by `CT_Push.yml` path ignores
  without forcing the full heterogeneous database matrix for docs-only changes.
  Upload Playwright reports and generated diffs only on failure and never upload
  keyrings or bootstrap values.
  Tests: Unit-test classification with synthetic docs-only, API, frontend,
  combined, and irrelevant path lists; validate workflow syntax and execute its
  exact commands in the same Ubuntu/Node 22/JDK 17 job images. Prove a docs-only change runs the focused workflow, prove an API
  change fails on stale OpenAPI, prove a route/theme change fails on stale
  screenshot artifacts, and verify all CI shell snippets use available tools,
  strict error handling, safe matching under `pipefail`, and bounded timeouts.
  Dependencies: Tasks 11.1, 12.1, 14.1, and 15.1

### 17. Publish the static portal through GitHub Pages

- [x] **17.1 Add preview-safe build and production Pages deployment**
  Files: `.github/workflows/docs-pages.yml` (new),
  `docs/astro.config.mjs`, `docs/public/.nojekyll`,
  `docs/tests/e2e/docs-site.spec.ts`, `docs/PAGES_CUTOVER.md` (new)
  Changes: Use the official Astro/GitHub Pages artifact workflow on pushes to
  `master` and manual dispatch, with `contents: read`, `pages: write`, and
  `id-token: write`, one non-cancelling production deployment concurrency group,
  and deployment only after the same deterministic docs gates pass. Configure
  GitHub Pages output for the repository subpath, preserve a future custom-domain
  switch through one site/base setting, and keep PRs build-only with downloadable
  artifacts rather than production deployment. Add a mandatory checklist for
  Settings > Pages > Source = GitHub Actions, the `github-pages` environment,
  expected `/ReplicaDB` URL, first dry run, legacy/tool smoke, and rollback
  artifact. Gate deployment on repository variable
  `DOCS_PAGES_SOURCE=actions`; fail with the checklist path when absent, and let
  `actions/configure-pages` verify enablement before artifact upload.
  Tests: Build and inspect the uploaded artifact locally, verify no Jekyll
  processing is required, serve it under `/ReplicaDB` rather than `/`, exercise
  canonical, legacy, tool, OpenAPI, search, image, and 404 URLs, and confirm the
  deployment job cannot run for pull requests or before its build/validation
  dependency succeeds. Static workflow tests require the repository-variable
  gate and checklist; the first manual dry run records the resulting Pages URL
  before enabling the push trigger.
  Dependencies: Task 16.1

### 18. Retire Jekyll only after cutover acceptance

- [x] **18.1 Remove the obsolete theme and complete the reversible cutover**
  Files: `docs/_config.yml` (delete), `docs/Gemfile` (delete),
  `docs/jekyll-docs-theme.gemspec` (delete), `docs/_includes/**` (delete),
  `docs/_layouts/**` (delete), `docs/assets/css/**` (delete),
  `docs/assets/js/**` (delete), `docs/index.md` (delete),
  `docs/server.md` (delete), `docs/docs/docs.md` (delete),
  `docs/docs/media/**` (delete after migrated assets are verified),
  `.github/workflows/docs-pages.yml`, `docs/PAGES_CUTOVER.md`, `README.md`
  Changes: Delete Jekyll-only source and duplicated monolithic Markdown after
  the Astro build, route compatibility, content parity, static tools, OpenAPI,
  screenshots, CI, and Pages artifact all pass. Record the Pages source switch
  from branch/folder Jekyll publishing to GitHub Actions, retain a last-known-good
  artifact for rollback, and document rollback as redeploying that artifact or
  reverting the workflow/source commit rather than restoring mixed Jekyll/Astro
  ownership. Record the successful workflow run ID and downloaded artifact name
  in `PAGES_CUTOVER.md`; rollback dispatches the Pages workflow at that
  last-known-good commit, or reverts the cutover commit, and deploys through the
  same artifact path rather than restoring a generated branch.
  Tests: Run the complete docs acceptance suite from a clean checkout, verify no
  Jekyll/Bundler dependency or Liquid tag remains, compare the required legacy
  URL inventory against the deployed artifact, inspect desktop/mobile
  screenshots of the homepage, CLI guide, server guide, architecture guide,
  API reference, wizard, and Markup Forge, and run `git diff --check` plus a
  repository secret scan before enabling the Pages workflow.
  Dependencies: Tasks 4.1, 13.1, 15.1, 16.1, and 17.1

## Technical Reference

<details>
<summary>Types & Data Structures</summary>

- `LegacyRoute`: old pathname, optional fragment map, canonical target, and
  permanent compatibility-page metadata. It is also consumed by route tests.
- `ConnectorCapability`: JSON object with required `id`, `displayName`,
  `schemes`, `roles`, `modes`, `bandwidthThrottling`, `staging`, `page`, and
  `caveats`; Java and Node tests enforce complete manager coverage.
- `ScreenshotDefinition`: stable key, route, viewport, documented state,
  filename, alt text, caption, and owning guide. It must not contain credentials,
  real identities, datasource security, or unstable run identifiers.
- Local OpenAPI JSON: generated from Springdoc, normalized deterministically,
  committed, and treated as generated output. The Java contract remains the
  source of truth.
- Starlight content collections: English living documentation with validated
  frontmatter, sidebar placement, title, description, and optional page-specific
  screenshot or diagram metadata.

</details>

<details>
<summary>Dependencies</summary>

- Node 22, npm lockfiles, Astro, `@astrojs/starlight`, MDX, Mermaid, and
  `starlight-openapi` form the documentation build. All output remains static.
- Pagefind is provided by Starlight for local full-text search; no external
  search account or crawler is required.
- Existing Java 17, Springdoc, MockMvc, and `OpenApiSpecificationIT` generate
  and validate the committed API document. No production endpoint or DTO change
  is required.
- Existing frontend Playwright, local PostgreSQL fixtures, session/CSRF flow,
  and local-seeding support generate screenshots. Screenshot generation is not
  part of the lightweight docs build.
- Existing `docs/markdown` dependencies and tests remain isolated from the new
  top-level docs package; the staging script copies runtime output only.
- GitHub Actions and GitHub Pages host the final static artifact. No SaaS docs
  platform, server runtime, external search service, or new product database is
  introduced.

</details>

<details>
<summary>Testing Strategy</summary>

| Layer | Tooling | Required evidence |
| --- | --- | --- |
| Content contracts | Node test runner and source parsers | Every route/capability/configuration has an owning page; no stale or secret-bearing content |
| Static build | Astro check/build and Pagefind output | Deterministic HTML, sitemap, search, base path, canonical metadata, and no Jekyll dependency |
| Legacy compatibility | Node route inventory plus Playwright | Old pages/fragments and both browser tools remain reachable under GitHub Pages semantics |
| CLI examples | Packaged CLI and SQLite smoke | Documented keys, precedence, modes, multi-table behavior, and exit codes remain true |
| OpenAPI | JUnit Jupiter/MockMvc plus JSON drift diff | Public schema is current, generated pages build, internal/secret fields remain absent |
| Screenshots | Playwright against a fresh server and isolated PostgreSQL | Curated inventory is deterministic, nonblank, responsive, redacted, and complete |
| Docs UX | Playwright and accessibility scanner | Keyboard/search/theme/code/diagram/media behavior works at desktop and mobile sizes |
| CI/deployment | GitHub Actions validation and Pages artifact inspection | Docs-only changes are gated; deployment follows validation and serves correctly from `/ReplicaDB` |
| Final acceptance | Clean checkout build, link/secret scan, visual inspection | Jekyll is gone, all requested subject areas exist, and rollback remains possible |

</details>

## Risks, Assumptions, and Deferred Work

- The plan assumes GitHub Pages will be configured to use GitHub Actions as its
  source at cutover. Repository settings cannot be changed by source code; the
  final task must record and verify that manual setting.
- `starlight-openapi` is an actively maintained community plugin rather than an
  Astro core package. Pin its version, keep the committed JSON usable by another
  renderer, and avoid plugin-specific content in authored guides so replacement
  remains bounded.
- Screenshot diffs can change with browser/font updates. Pin Playwright through
  the frontend lockfile, use the CI Chromium channel for canonical captures,
  disable animation, and review binary updates rather than accepting them
  automatically.
- Curated documentation captures and exhaustive visual regression have separate
  ownership and outputs. `docs-screenshots.spec.ts` updates only the small
  manifest-driven public set; `visual-regression.spec.ts` owns baselines under
  frontend E2E and covers the broader state/viewport matrix. Neither command
  writes into the other's directory, and static-tool staging excludes both test
  reports and regression baselines.
- Documentation duplicates some runtime facts from YAML and Java. Contract tests
  should compare machine-readable values where practical; prose-only operational
  claims still require review when worker, scheduler, ACL, or manager behavior
  changes.
- A single living version means old releases will not receive frozen manuals.
  Compatibility pages preserve links, while upgrade guides describe behavioral
  changes. Revisit Docusaurus-style major versioning only if simultaneous
  supported majors become a real maintenance requirement.
- English is the sole authored locale in this plan. Starlight's locale-ready
  structure should not be expanded to Spanish until ownership and translation
  freshness are agreed.
- The existing configuration wizard and Markup Forge are preserved, not
  redesigned. Their modernization remains separate work.

## Phase Exit Criteria

The documentation migration is complete only when:

- A clean Node 22 checkout builds the complete Starlight site without Java,
  PostgreSQL, credentials, network-fetched content, Jekyll, or Bundler.
- CLI and Server are visibly distinct choices and all requested CLI, frontend,
  API, distributed architecture, concurrency, scaling, operations, security,
  and troubleshooting subjects have navigable English pages.
- Every frontend route has a guide and canonical screenshot; selected mobile
  and critical-state screenshots are present and reproducible.
- The committed OpenAPI document matches the tested server contract and exposes
  no lease token, datasource secret, credential, or implementation-only type.
- `/server.html`, `/docs/docs.html`, `/wizard/index.html`, and
  `/markdown/converter.html` work from the production-shaped Pages artifact,
  including mapped legacy fragments and nested assets.
- Internal links, fragments, images, search, sitemap, canonical URLs,
  accessibility, responsive layouts, and secret scans pass in CI.
- Docs-only pull requests run a focused gate, API/frontend changes detect stale
  generated docs artifacts, and production deployment occurs only after the
  validated build succeeds.
- The Jekyll theme and duplicated monolithic pages are removed only after a
  last-known-good Astro artifact and rollback procedure are available.

## Execution Retrospective (auto-generated by /itx-code)

### Plan Accuracy

- Tasks completed as planned: 18/18 (100%).
- Tasks that required plan adjustment: 5/18 (28%).
- Test loop iterations: 34 total (25 first-pass, 8 second-pass, 1 third-pass).

### Gaps Encountered

#### Gap 1: Legacy SEO alias was part of the published surface (Intent-to-Plan)

- **Task**: 4.1 — Add legacy compatibility pages and an executable route contract.
- **Plan assumed**: The primary legacy pages were `/server.html` and `/docs/docs.html`.
- **Reality**: Tracked SEO metadata also referenced `/docs/user-guide.html`.
- **Resolution**: Added a typed compatibility mapping and static page for the alias.
- **Learning**: Inventory tracked SEO, HTML, and generated-link sources before deleting a published docs implementation.

#### Gap 2: Astro output format conflicted with exact legacy files (Plan-to-Implementation)

- **Task**: 4.1 — Preserve exact legacy `.html` URLs while keeping normal portal routes.
- **Plan assumed**: Astro file output would preserve `.html` page names directly.
- **Reality**: `.html.astro` emitted `.html.html`, while file output broke normal trailing-slash portal links.
- **Resolution**: Kept directory output for normal Starlight pages and flattened only compatibility pages during staging.
- **Learning**: Treat legacy compatibility output as a post-build artifact concern when the new site uses directory routes.

#### Gap 3: Manager capability modes are aggregate source/sink unions (Plan-to-Implementation)

- **Task**: 7.1 — Align connector JSON with Java capability truth.
- **Plan assumed**: A single `modes` field represented sink behavior.
- **Reality**: `ManagerCapabilities` exposes source and sink sets, so aggregate documentation includes source-supported modes even when the sink is narrower.
- **Resolution**: Documented aggregate modes and kept sink limitations in caveats/pages; Java contract checks both roles and unions.
- **Learning**: Structured capability models need explicit role-specific fields or an unambiguous aggregate definition.

#### Gap 4: Docs-only visual capture was not isolated from frontend E2E (Plan-to-Implementation)

- **Task**: 12.1 — Add deterministic screenshot capture and separate visual regression.
- **Plan assumed**: The new capture specs would naturally be scoped to their own project.
- **Reality**: The existing broad Playwright config discovered docs specs, and CI lacked Linux baselines.
- **Resolution**: Added a dedicated docs config, excluded docs specs from the general config, and committed Linux baselines.
- **Learning**: New visual projects require both positive `testMatch` ownership and negative exclusion from existing project discovery.

#### Gap 5: Recovered workspace state dropped generated prerequisites (Plan-to-Implementation)

- **Task**: 11.1–14.1 — Continue after workspace resets.
- **Plan assumed**: Authored docs sources and generated OpenAPI/build prerequisites would remain together.
- **Reality**: External resets removed uncommitted sources and later removed the OpenAPI schema, forcing recovery from unreachable Git objects and regeneration.
- **Resolution**: Recovered authored trees from unreachable snapshots, regenerated the schema, restored Pages markers/favicon, and reran all local/remote gates.
- **Learning**: Long agentic runs need checkpoint commits or durable recovery snapshots for generated contracts and untracked plan state.

### Patterns Discovered

- **Contract-first documentation**: `docs/tests/*-contract.test.mjs` compares authored content with Java registries, frontend routes, YAML defaults, and built output.
- **Deterministic artifact ownership**: curated media lives under `docs/src/assets`; exhaustive regression baselines remain under frontend E2E snapshots.
- **Compatibility staging**: `docs/scripts/stage-static-tools.mjs` owns legacy tools and exact `.html` compatibility flattening without mixing source ownership.
