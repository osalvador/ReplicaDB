# Implementation Plan: Recover Comprehensive Product Documentation and Complete API Reference

## Task Source - JIRA: none - Restore ReplicaDB documentation depth and API clarity

Acceptance criteria are derived from the user request and planning decisions:

- Recover the useful detail, examples, and instructional visuals lost when the
  legacy documentation was replaced, while removing obsolete behavior rather
  than reproducing it verbatim.
- Keep public user documentation in English, direct, specific, and grounded in
  the current product contract.
- Explain the three replication modes with their original instructional flows,
  and give the managed server complete screenshot-backed workflow coverage.
- Publish a complete, static, read-only OpenAPI 3 reference with a concise
  human guide for authentication, CSRF, authorization, pagination,
  idempotency, and RFC 7807 errors.
- Make every generated API operation useful: a stable operation ID, a clear
  summary and description, resource-oriented tags, parameters, schemas,
  examples where safe, and the HTTP responses actually emitted at runtime.
- Preserve the Astro/Starlight Pages deployment, `/ReplicaDB` base path,
  legacy compatibility pages, generated frontend contract, redaction rules,
  and existing CLI/connector behavior contracts.

## Overview

Restore the documentation portal as the single canonical user guide instead of
adding a second legacy document. The work will recover accurate legacy content
into the portal's task-oriented pages, restore or replace instructional media,
and resolve content-source collisions that currently hide the richer MDX
guides.

The server API will remain generated from Springdoc and published as a static,
read-only OpenAPI 3 reference. The implementation will enrich the Java source
of that contract, regenerate both published JSON and frontend types, and prove
that the documented semantics match the authenticated server.

## Architecture & Design

### Approach: Recovery modular verified

The portal remains an Astro/Starlight `Read` surface. Every topic has one
canonical source entry under `docs/src/content/docs`; MDX is used where a page
imports optimized local media or visual components. The parent of migration
commit `18161f78` is historical evidence, not an alternate published source:
each claim is retained only after comparison with maintained Java behavior,
configuration, tests, and connector capability data.

```text
Historical Markdown + media
           |
           | factual review against code/tests
           v
Canonical Starlight guides ----> static GitHub Pages artifact
           |                                  |
           | imports local optimized media     `--> no live API proxy
           v
CLI / connectors / server / operations

Spring controllers + DTOs + security behavior
           |
           v
Springdoc OpenAPI 3 JSON --> committed schema --> Starlight operation pages
                              |
                              `--> generated frontend TypeScript types
```

### Content and media rules

- Preserve a clear separation between CLI, managed server, architecture,
  operations, connector reference, and API usage. Do not recreate the old
  2,561-line monolith or present it as a second source of truth.
- A retained paragraph must state what happens, required conditions, observable
  outcome, and an actionable limitation where one exists. Remove speculative
  promises, deprecated CDC instructions, and version-specific assertions that
  no longer match current behavior.
- Restore the three replication mode diagrams and the Amazon S3 examples from
  Git history. Use the current Mermaid architecture diagrams for topology that
  changed after the legacy screenshots; retain the old demo animation only if
  it still depicts a supported, current workflow.
- Use the existing `ScreenshotFrame` component for all public server captures.
  Each image must have an accurate alt text, caption, source guide, stable
  dimensions, and lazy loading unless it is the page's first instructional
  visual. Curated public images remain under `docs/src/assets/screenshots`;
  visual-regression baselines remain in frontend E2E snapshots.
- Do not add a visual redesign or new documentation framework. Preserve the
  existing Engineering Ledger tokens, responsive layout, accessibility and
  image-size safeguards.

### API contract rules

- Add a small server-side OpenAPI configuration in the existing security
  configuration package. It will declare `ReplicaDB Server API`, API version
  `v1`, domain tags, session-cookie and CSRF-header security schemes, and
  reusable RFC 7807 problem responses. It does not add a new runtime endpoint
  or alter authentication behavior.
- Decorate each of the 37 public `/api/v1` operations in the controller that
  owns its runtime behavior. Explicit annotations will supply stable,
  human-readable operation IDs, summaries, descriptions, security
  requirements, documented parameters, and only the response codes that the
  endpoint can actually emit.
- Describe the public session and CSRF contract, but do not make the GitHub
  Pages site an interactive Swagger client. Session cookies and CSRF tokens are
  same-origin deployment concerns; the static portal consumes a committed,
  tested schema and never accepts credentials.
- Preserve redaction. `leaseToken`, encrypted datasource contents, resolved
  passwords, connection strings containing credentials, keyrings, bootstrap
  values, and the test-only local-seeding mechanism must remain absent from
  OpenAPI, examples, screenshots, generated TypeScript, and built HTML.
- Preserve the separate response meanings: resource creation returns `201`,
  accepted run dispatch returns `202`, empty successful deletes/logout return
  `204`, and invalid, unauthenticated, unauthorized, missing, or conflicting
  operations use documented RFC 7807 responses.

### Delivery boundaries, compatibility, and risks

- No database migration, manager behavior, runtime security policy, or new npm
  or Maven dependency is required.
- Keep Astro directory output and `stage-static-tools.mjs` ownership intact so
  `/server.html`, `/docs/docs.html`, `/docs/user-guide.html`, `/wizard/`, and
  `/markdown/` remain compatible under GitHub Pages.
- The risk in media recovery is falsely representing a historical interface as
  current. The mitigation is to use deterministic current server captures for
  all UI workflows and recover old images only where the contract still
  matches.
- The risk in API enrichment is contract drift caused by editing generated
  artifacts. The mitigation is controller/DTO annotations as the source of
  truth, regeneration through existing scripts, and byte/shape checks on the
  committed output.
- The existing docs workflow already runs the API drift gate for server Java
  changes and the frontend drift gate for generated frontend changes. The plan
  extends their tests rather than bypassing those gates.

## Implementation Tasks

### 1. Recover instructional media with a verified ownership inventory

- [x] **1.1 Restore currently accurate legacy mode and connector visuals**
  Files: `docs/src/assets/media/replication-modes/complete.png` (new),
  `docs/src/assets/media/replication-modes/complete-atomic.png` (new),
  `docs/src/assets/media/replication-modes/incremental.png` (new),
  `docs/src/assets/media/connector-examples/amazon-s3.png` (new),
  `docs/src/assets/media/connector-examples/amazon-s3-csv.png` (new),
  `docs/tests/media-contract.test.mjs` (new)
  Changes: Recover the five named binary assets from the parent of
  `18161f78`, preserve their inspectable dimensions, and record their exact
  intended guide ownership in the media contract. Review the old conceptual
  diagram and Oracle-to-PostgreSQL animation against the current product; use
  the existing current architecture diagram when legacy topology is no longer
  factual. Do not publish the unnamed legacy animation in this recovery: it
  has no reproducible current capture, source scenario, or assertion proving
  that it remains an accurate representation of CLI behavior.
  Tests: Add Node assertions that all five required files exist, remain within
  the established public image budgets, have one owning guide, and are not
  referenced through the former GitHub-hosted legacy media URLs or the
  unverified legacy animation. Run the media contract and an Astro production
  build.
  Dependencies: None

### 2. Remove competing slugs and establish canonical MDX ownership

- [x] **2.1 Consolidate duplicate guides and convert image-owning server pages to MDX**
  Files: `docs/src/content/docs/cli/replication-modes.md` (delete),
  `docs/src/content/docs/cli/replication-modes.mdx`,
  `docs/src/content/docs/connectors/amazon-s3.md` (delete),
  `docs/src/content/docs/connectors/amazon-s3.mdx`,
  `docs/src/content/docs/server/permissions.md` (delete),
  `docs/src/content/docs/server/permissions.mdx`,
  `docs/src/content/docs/server/runs-and-diagnostics.md` (delete),
  `docs/src/content/docs/server/runs-and-diagnostics.mdx`,
  `docs/src/content/docs/server/sign-in-and-profile.md` (delete),
  `docs/src/content/docs/server/sign-in-and-profile.mdx`,
  `docs/src/content/docs/server/dashboard.md` (replace with `.mdx`),
  `docs/src/content/docs/server/datasources.md` (replace with `.mdx`),
  `docs/src/content/docs/server/jobs.md` (replace with `.mdx`),
  `docs/src/content/docs/server/users.md` (replace with `.mdx`),
  `docs/src/content/docs/server/audit.md` (replace with `.mdx`),
  `docs/src/content/docs/server/errors-and-empty-states.md` (replace with
  `.mdx`), `docs/tests/server-route-coverage.test.mjs`,
  `docs/tests/media-contract.test.mjs`
  Changes: Retain one canonical content entry for each slug, merging the
  richer MDX copy into its canonical page instead of maintaining a short Markdown
  counterpart. Convert only server pages that own a screenshot to MDX so they
  can import local media through `ScreenshotFrame`. Update source-resolution
  tests to accept the canonical extension and add a pre-build uniqueness check
  that fails on duplicate content slugs rather than relying on Astro's warning.
  Tests: Prove every sidebar and frontend route resolves to one source entry,
  inject a duplicate-slug fixture to prove the uniqueness check fails, run
  `npm run check`, and assert the resulting output contains no duplicate route
  owners.
  Dependencies: Task 1.1

### 3. Rebuild complete CLI workflow and reference documentation

- [x] **3.1 Expand CLI usage from installation through failure recovery**
  Files: `docs/src/content/docs/cli/index.md`,
  `docs/src/content/docs/cli/installation.md`,
  `docs/src/content/docs/cli/configuration.md`,
  `docs/src/content/docs/cli/replication-modes.mdx`,
  `docs/src/content/docs/cli/parallelism.md`,
  `docs/src/content/docs/cli/filtering-and-queries.md`,
  `docs/src/content/docs/cli/multi-table.md`,
  `docs/src/content/docs/cli/incremental-watermarks.md`,
  `docs/src/content/docs/cli/performance.md`,
  `docs/src/content/docs/cli/troubleshooting.md`,
  `docs/src/content/docs/reference/cli-options.md`,
  `docs/src/content/docs/reference/example-options-files.md`,
  `docs/tests/cli-contract.test.mjs`
  Changes: Recover and editorially distribute the historical explanation of
  option precedence, source/sink connection parameters, table selection,
  free-form queries, parallelism, bandwidth, multi-table sequencing,
  watermark filtering, tuning, failure exit codes, and data conversion limits.
  Expand each replication mode with its verified sequence, preconditions,
  visibility and interruption consequences, mode-specific staging and merge
  semantics, and restored mode diagram. Keep every option, property, default,
  precedence statement, and connector limitation tied to `ToolOptions`, the
  maintained sample configuration, and current manager capabilities.
  Tests: Extend the CLI contract with required topic and mode-flow assertions;
  retain checks rejecting unsupported options/properties and resolved
  credentials; parse every bash snippet; run the CLI compatibility script and
  targeted mode tests named by the maintained contract.
  Dependencies: Tasks 1.1 and 2.1

### 4. Recover connector documentation by supported behavior, not generic claims

- [x] **4.1 Expand relational and JDBC connector pages from capability evidence**
  Files: `docs/src/content/docs/connectors/oracle.md`,
  `docs/src/content/docs/connectors/postgresql.md`,
  `docs/src/content/docs/connectors/mysql-mariadb.md`,
  `docs/src/content/docs/connectors/sql-server.md`,
  `docs/src/content/docs/connectors/db2.md`,
  `docs/src/content/docs/connectors/sqlite.md`,
  `docs/src/content/docs/connectors/denodo.md`,
  `docs/src/content/docs/connectors/generic-jdbc.md`,
  `docs/src/content/docs/connectors/index.mdx`,
  `docs/src/data/connector-capabilities.json`,
  `docs/tests/connector-capabilities.test.mjs`
  Changes: Restore current Oracle URL and Flashback guidance, PostgreSQL and
  MySQL/MariaDB handling, SQL Server/XML caveats, DB2 and SQLite limitations,
  Denodo behavior, and generic JDBC prerequisites. Explain source and sink
  roles, supported replication modes, staging behavior, driver requirements,
  type or vendor caveats, and safe configuration examples per connector. Keep
  the support matrix as the one aggregate view and express role-specific
  limitations in the owning page rather than claiming that aggregate modes are
  universal sink capabilities.
  Tests: Update the capability contract to assert that each detailed page names
  its documented source/sink limits and safe caveat; run the current manager
  capability tests and reject connector claims not present in the Java evidence.
  Dependencies: Task 3.1

- [x] **4.2 Expand file, object-storage, document, and stream connector guidance**
  Files: `docs/src/content/docs/connectors/csv.md`,
  `docs/src/content/docs/connectors/amazon-s3.mdx`,
  `docs/src/content/docs/connectors/mongodb.md`,
  `docs/src/content/docs/connectors/kafka.md`,
  `docs/src/content/docs/connectors/index.mdx`,
  `docs/src/data/connector-capabilities.json`,
  `docs/tests/connector-capabilities.test.mjs`,
  `docs/tests/media-contract.test.mjs`
  Changes: Restore CSV format, quote, path, type, and ORC limitations;
  document S3 object layout, replacement semantics, permission prerequisites,
  and recovered screenshots; describe MongoDB field mapping and Kafka-specific
  sink semantics. State explicitly where file/object/document/stream sinks do
  not provide table merge or transactional staging guarantees. Use environment
  managed credentials in examples and never place keys in connection strings,
  options files, or image captions.
  Tests: Run connector capability contracts, image ownership checks, and safe
  example scans. Add assertions that S3's two restored images appear only in
  its canonical guide and that ORC and non-table restrictions are discoverable
  from the relevant connector pages.
  Dependencies: Tasks 1.1 and 2.1

> Critic note: `connector-capabilities.json` currently exposes flat aggregate
> `modes`. Before implementation, resolve source and sink modes into explicit
> role-specific data or an equally unambiguous rendered matrix. The recovery
> must not allow aggregate modes to be read or tested as sink guarantees.

### 5. Restore product orientation, architecture, and operating detail

- [x] **5.1 Expand start-here, architecture, and operations pages with current evidence**
  Files: `docs/src/content/docs/index.mdx`,
  `docs/src/content/docs/getting-started/choose-cli-or-server.mdx`,
  `docs/src/content/docs/getting-started/cli-quickstart.md`,
  `docs/src/content/docs/getting-started/server-quickstart.md`,
  `docs/src/content/docs/getting-started/concepts.md`,
  `docs/src/content/docs/architecture/overview.mdx`,
  `docs/src/content/docs/architecture/core-and-server-boundaries.mdx`,
  `docs/src/content/docs/architecture/distributed-topology.mdx`,
  `docs/src/content/docs/architecture/run-lifecycle.mdx`,
  `docs/src/content/docs/architecture/dispatch-and-recovery.md`,
  `docs/src/content/docs/architecture/concurrency-and-fencing.md`,
  `docs/src/content/docs/architecture/scheduling-and-ha.md`,
  `docs/src/content/docs/architecture/scaling-and-fairness.md`,
  `docs/src/content/docs/architecture/security-boundaries.md`,
  `docs/src/content/docs/operations/*.md`, `docs/tests/content-contract.test.mjs`,
  `docs/tests/architecture-contract.test.mjs`,
  `docs/tests/operations-contract.test.mjs`
  Changes: Reintroduce the useful legacy product rationale, data-flow detail,
  troubleshooting and performance context into the relevant current sections.
  Keep current managed-server facts authoritative: CLI and server are distinct
  artifacts; retries restart rather than resume; watermark and cancellation
  semantics are explicit; and deployment, backup, TLS, key management,
  monitoring, capacity, upgrade and recovery procedures remain consistent with
  current configuration and scripts. Retain current Mermaid diagrams with
  textual fallbacks instead of publishing obsolete topology artwork.
  Tests: Expand content, architecture, and operations contracts with required
  current concepts and links; parse shell examples; run the existing phase-3
  documentation checker and the portal build/link validator.
  Dependencies: Tasks 3.1, 4.1, and 4.2

### 6. Publish complete, workflow-oriented server documentation

- [x] **6.1 Expand server setup and each authenticated control-plane workflow**
  Files: `docs/src/content/docs/server/index.md`,
  `docs/src/content/docs/server/installation.md`,
  `docs/src/content/docs/server/sign-in-and-profile.mdx`,
  `docs/src/content/docs/server/dashboard.mdx`,
  `docs/src/content/docs/server/datasources.mdx`,
  `docs/src/content/docs/server/jobs.mdx`,
  `docs/src/content/docs/server/schedules.md`,
  `docs/src/content/docs/server/runs-and-diagnostics.mdx`,
  `docs/src/content/docs/server/users.mdx`,
  `docs/src/content/docs/server/permissions.mdx`,
  `docs/src/content/docs/server/audit.mdx`,
  `docs/src/content/docs/server/errors-and-empty-states.mdx`,
  `docs/src/data/screenshots.ts`, `docs/src/components/ScreenshotFrame.astro`,
  `docs/src/assets/screenshots/server/schedule.png` (new),
  `replicadb-server/frontend/e2e/docs-screenshots.spec.ts`,
  `replicadb-server/frontend/e2e/support/docsScreenshotFixtures.ts`,
  `docs/tests/server-route-coverage.test.mjs`,
  `docs/tests/media-contract.test.mjs`
  Changes: Recover the server distribution, local/external PostgreSQL topology,
  durable home, keyring, backup, upgrade, profile, permission, run lifecycle,
  schedule, audit, failure and recovery detail from the historical guide and
  current server behavior. Embed the curated desktop capture for every
  documented server surface and the selected mobile capture where it explains
  responsive use: dashboard, jobs, datasource catalog/detail/edit, users,
  audit, permissions, runs, login/profile, schedule configuration, and
  authorization state. Add a deterministic, curated schedule configuration
  capture to the isolated frontend screenshot fixture/spec and expand the
  screenshot registry to all generated assets. Define the route/action-to-image
  mapping so catalog, create, detail, edit, schedule, permission, diagnostics,
  identity, administration, audit and error workflows cannot be documented
  without their agreed instructional visual.
  Keep source data redacted, explain secrets-preserving edits, distinguish UI
  affordances from server authorization, and link dashboard metrics to the
  underlying job/run investigation paths.
  Tests: Assert every SPA route has exactly one canonical guide, every
  documented visual workflow has its required canonical capture, and every
  curated screenshot has one owning MDX guide, an import, alt text, caption,
  dimensions, and responsive containment. Run browser documentation checks at
  1440x900 and 390x844, confirm no horizontal overflow or secret-bearing text,
  and run server route/content contracts.
  Dependencies: Tasks 1.1, 2.1, and 5.1

### 7. Define the OpenAPI document's shared public contract

- [x] **7.1 Add server-owned OpenAPI metadata, groups, security schemes, and problem responses**
  Files: `replicadb-server/src/main/java/org/replicadb/server/security/config/OpenApiConfiguration.java` (new),
  `replicadb-server/src/main/java/org/replicadb/server/security/config/SecurityConfig.java`,
  `replicadb-server/src/main/java/org/replicadb/server/job/api/GlobalExceptionHandler.java`,
  `replicadb-server/src/test/java/org/replicadb/server/OpenApiSpecificationIT.java`,
  `docs/tests/openapi-contract.test.mjs`
  Changes: Add an API configuration using the existing Springdoc dependency,
  with API title `ReplicaDB Server API`, version `v1`, domain descriptions and
  ordered tags for Authentication, Dashboard, Datasources, Datasource
  permissions, Jobs, Job permissions, Schedules, Runs, Users, and Audit.
  Declare the established session-cookie and `X-XSRF-TOKEN` header schemes,
  reusable `application/problem+json` response components, and a public schema
  description that directs consumers to same-origin authenticated deployments.
  Align names with existing `SecurityConfig` behavior without exposing session
  values or relaxing access to API resources.
  Tests: In MockMvc, assert OpenAPI 3 output has the title, `v1`, each domain
  tag, both security schemes, reusable problem representation, and no
  credentials, implementation packages, lease tokens, encrypted security, or
  test-only headers. Confirm anonymous `/v3/api-docs` remains readable while a
  protected `/api/v1` resource still returns its RFC 7807 unauthorized result.
  Dependencies: None

### 8. Document job, run, schedule, and dashboard operations at their owners

- [x] **8.1 Enrich operational controllers with complete endpoint semantics**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/api/JobDefinitionController.java`,
  `replicadb-server/src/main/java/org/replicadb/server/job/api/JobRunController.java`,
  `replicadb-server/src/main/java/org/replicadb/server/job/api/JobScheduleController.java`,
  `replicadb-server/src/main/java/org/replicadb/server/job/api/DashboardController.java`,
  `replicadb-server/src/test/java/org/replicadb/server/job/api/JobDefinitionControllerTest.java`,
  `replicadb-server/src/test/java/org/replicadb/server/job/api/JobRunControllerTest.java`,
  `replicadb-server/src/test/java/org/replicadb/server/job/api/JobScheduleControllerTest.java`,
  `replicadb-server/src/test/java/org/replicadb/server/job/api/DashboardControllerTest.java`,
  `replicadb-server/src/test/java/org/replicadb/server/OpenApiSpecificationIT.java`
  Changes: Add controller and operation metadata for job CRUD, job run history,
  global runs, run detail/log/trigger/cancel/retry, schedule get/upsert/delete,
  and dashboard summary. Give every operation a stable identifier and a
  specific description of permissions, state preconditions, query parameters,
  side effects, and recovery behavior. Document `Idempotency-Key` as required,
  maximum 255 characters, and replay-safe only on manual run trigger. Describe
  time ranges, status filtering and pagination accurately, including the
  default page 0, default size 50, and size cap 200. Declare actual `201`
  create, `202` accepted dispatch/retry, `204` deletion, and applicable 400,
  401, 403, 404, and 409 problem responses; never expose the local E2E seed
  header as public API.
  Tests: Extend existing MockMvc tests with successful and failure response
  assertions for each status class, missing/invalid idempotency keys, replay,
  forbidden resource access, non-cancellable/retry-ineligible runs, missing
  schedules, invalid dashboard ranges, and omitted dashboard bounds that return
  the effective 24-hour window. Assert a non-admin dashboard is restricted to
  visible jobs before metrics are aggregated. Assert the generated schema has
  all expected operation IDs, parameters, response keys, and no test-seeding
  header.
  Dependencies: Task 7.1

### 9. Document datasource, ACL, identity, and audit operations at their owners

- [x] **9.1 Enrich catalog, permissions, session, user, and audit controller operations**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/api/DatasourceController.java`,
  `replicadb-server/src/main/java/org/replicadb/server/job/api/DatasourcePermissionController.java`,
  `replicadb-server/src/main/java/org/replicadb/server/job/api/JobPermissionController.java`,
  `replicadb-server/src/main/java/org/replicadb/server/security/api/AuthController.java`,
  `replicadb-server/src/main/java/org/replicadb/server/security/api/UserController.java`,
  `replicadb-server/src/main/java/org/replicadb/server/audit/api/AuditEventController.java`,
  `replicadb-server/src/test/java/org/replicadb/server/job/api/DatasourceControllerTest.java`,
  `replicadb-server/src/test/java/org/replicadb/server/job/api/DatasourcePermissionControllerTest.java`,
  `replicadb-server/src/test/java/org/replicadb/server/job/api/JobPermissionControllerTest.java`,
  `replicadb-server/src/test/java/org/replicadb/server/security/api/AuthControllerTest.java`,
  `replicadb-server/src/test/java/org/replicadb/server/security/api/UserControllerTest.java`,
  `replicadb-server/src/test/java/org/replicadb/server/audit/api/AuditEventControllerTest.java`,
  `replicadb-server/src/test/java/org/replicadb/server/OpenApiSpecificationIT.java`
  Changes: Add stable operation metadata for datasource CRUD, datasource/job
  grant listing/replacement/revocation, CSRF bootstrap, login/logout/current
  identity, user administration, and audit filtering. Mark only CSRF bootstrap
  and login as unauthenticated in the reference; require the session and CSRF
  contract for protected mutations. Describe ADMIN-only actions and resource
  permission requirements, datasource source/sink role filtering, redacted
  response behavior, blank-security preservation and explicit
  `clearSecurityKeys`, login throttling, audit filters, and accurate `201` or
  `204` responses. Use examples containing UUID and placeholder values only,
  never a real password, token, connection, or security map.
  Tests: Extend each nearest MockMvc test with allowed, unauthenticated,
  unauthorized, validation, missing-resource and conflict cases appropriate to
  its controller. Assert all operations appear under human domain tags with
  explicit public/secured requirements and status responses; run a generated
  schema scan proving secret values and implementation-only fields remain
  absent.
  Dependencies: Task 7.1

### 10. Make OpenAPI schemas readable without changing the wire contract

- [x] **10.1 Add schema descriptions, constraints, and safe examples to public DTOs**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/api/DatasourceRequest.java`,
  `replicadb-server/src/main/java/org/replicadb/server/job/api/DatasourceResponse.java`,
  `replicadb-server/src/main/java/org/replicadb/server/job/api/DatasourceSummaryResponse.java`,
  `replicadb-server/src/main/java/org/replicadb/server/job/api/DatasourceCapabilitiesResponse.java`,
  `replicadb-server/src/main/java/org/replicadb/server/job/api/JobDefinitionRequest.java`,
  `replicadb-server/src/main/java/org/replicadb/server/job/api/JobDefinitionResponse.java`,
  `replicadb-server/src/main/java/org/replicadb/server/job/api/JobRunResponse.java`,
  `replicadb-server/src/main/java/org/replicadb/server/job/api/RunLogResponse.java`,
  `replicadb-server/src/main/java/org/replicadb/server/job/api/JobScheduleRequest.java`,
  `replicadb-server/src/main/java/org/replicadb/server/job/api/JobScheduleResponse.java`,
  `replicadb-server/src/main/java/org/replicadb/server/job/api/JobPermissionRequest.java`,
  `replicadb-server/src/main/java/org/replicadb/server/job/api/JobPermissionResponse.java`,
  `replicadb-server/src/main/java/org/replicadb/server/job/api/DatasourcePermissionRequest.java`,
  `replicadb-server/src/main/java/org/replicadb/server/job/api/DatasourcePermissionResponse.java`,
  `replicadb-server/src/main/java/org/replicadb/server/job/api/DashboardSummaryResponse.java`,
  `replicadb-server/src/main/java/org/replicadb/server/job/api/PageResponse.java`,
  `replicadb-server/src/main/java/org/replicadb/server/security/api/LoginRequest.java`,
  `replicadb-server/src/main/java/org/replicadb/server/security/api/UserIdentityResponse.java`,
  `replicadb-server/src/main/java/org/replicadb/server/security/api/UserRequest.java`,
  `replicadb-server/src/main/java/org/replicadb/server/security/api/UserResponse.java`,
  `replicadb-server/src/main/java/org/replicadb/server/audit/api/AuditEventResponse.java`,
  `replicadb-server/src/test/java/org/replicadb/server/OpenApiSpecificationIT.java`
  Changes: Use Springdoc schema metadata on public request/response fields to
  explain mode, connector capabilities, datasource role, UUID references,
  timestamps, pagination, retry policy, run state, bounded diagnostics,
  cancellation warnings, permission sets, audit filters and identity fields.
  Carry existing validation bounds into the rendered schema where available.
  Keep nullability aligned with serialized JSON and expose no encryption,
  plaintext credential, key-version, or lease implementation detail. Use
  generic placeholder examples only where an example materially clarifies a
  request; annotate sensitive write-only inputs without supplying values.
  Tests: Assert representative schema descriptions, enum values, bounds,
  nullable fields and examples. Preserve the serialized JSON versus generated
  type nullability checks and negative assertions for credentials and
  `leaseToken`.
  Dependencies: Tasks 8.1 and 9.1

### 11. Regenerate and lock the public API artifacts to the enriched source

- [x] **11.1 Regenerate OpenAPI JSON and frontend types only from the running contract**
  Files: `replicadb-server/src/test/java/org/replicadb/server/OpenApiSpecificationIT.java`,
  `docs/scripts/update-openapi.sh`, `docs/openapi/replicadb-server.json`,
  `docs/tests/openapi-contract.test.mjs`,
  `replicadb-server/frontend/scripts/generate-api-types.mjs`,
  `replicadb-server/frontend/scripts/generate-api-types.test.mjs` (new),
  `replicadb-server/frontend/package.json`,
  `replicadb-server/frontend/src/api/schema.ts`,
  `replicadb-server/frontend/src/api/schema.test.ts`,
  `.github/workflows/docs.yml`
  Changes: Keep the existing MockMvc export and deterministic JSON
  canonicalization as the only way to update the committed docs schema. Extend
  its assertions to require all 37 operations to have an intentional
  operation ID, nonempty summary and description, domain tag, documented
  responses and resolvable request/response schemas. Regenerate the committed
  OpenAPI JSON through `update-openapi.sh`. Extend the generation wrapper with
  a file-input mode and regenerate TypeScript from that exact exported JSON to
  a temporary path before atomically replacing `schema.ts`; retain the URL mode
  for development only. Make the API drift CI job install the frontend tooling,
  generate types from the just-exported schema, and fail on either JSON or
  TypeScript working-tree drift. Do not hand-edit either generated file. Update
  type-level checks for the exact response-status unions and sensitive-field
  exclusions.
  Tests: Run `OpenApiSpecificationIT`, canonicalize twice and compare bytes,
  regenerate JSON to prove no working-tree drift, generate TypeScript twice
  from the exact JSON and require byte-identical temporary output, run frontend
  typecheck and `schema.test.ts`, and make the docs OpenAPI test fail for a
  generic generated operation ID, controller-name tag, absent description,
  incorrect `201`/`202`/`204` status, unresolved schema, or forbidden field.
  Dependencies: Tasks 8.1, 9.1, and 10.1

### 12. Write the human API guide and improve generated-reference navigation

- [x] **12.1 Turn the static API section into a usable integration guide plus generated detail**
  Files: `docs/src/content/docs/api/index.md`, `docs/astro.config.mjs`,
  `docs/tests/openapi-contract.test.mjs`, `docs/tests/e2e/docs-site.spec.ts`,
  `docs/tests/e2e/accessibility.spec.ts`, `docs/tests/validate-docs.test.mjs`
  Changes: Expand the API introduction with the base `/api/v1` path, the
  session/CSRF bootstrap sequence, cookie and `X-XSRF-TOKEN` handling,
  permission model, page/size rules, UTC ISO-8601 ranges, RFC 7807 error
  payloads, and the manual-run `Idempotency-Key` lifecycle. Include concise,
  safe request sequences using placeholders and explain the distinctions
  between `201`, `202`, `204`, and problem responses. Keep this guide focused
  on cross-cutting use; let Starlight-generated, tag-grouped pages own the
  endpoint-specific parameters, schemas, examples and responses. Configure the
  existing local `starlight-openapi` integration to use the new human domain
  tags and readable operation labels, without a live request proxy or public
  Swagger UI.
  Tests: Build the portal and assert the guide and representative operation
  pages expose the auth, CSRF, pagination, idempotency and RFC 7807 sections;
  exercise navigation/search from the guide to each tag group; run accessibility
  scans; and fail documentation validation for missing generated API pages,
  broken base-path links, secret-like examples, or an interactive external API
  form.
  Dependencies: Task 11.1

### 13. Strengthen portal validation and preserve the release path

- [x] **13.1 Make comprehensive documentation recovery a repeatable quality gate**
  Files: `docs/scripts/validate-docs.mjs`, `docs/tests/validate-docs.test.mjs`,
  `docs/tests/content-contract.test.mjs`, `docs/tests/server-route-coverage.test.mjs`,
  `docs/tests/media-contract.test.mjs`, `docs/tests/openapi-contract.test.mjs`,
  `docs/tests/e2e/docs-site.spec.ts`, `docs/tests/e2e/responsive.spec.ts`,
  `docs/CONTRIBUTING.md`, `.github/workflows/docs.yml`,
  `.github/workflows/docs-pages.yml`,
  `replicadb-server/frontend/playwright.config.ts`,
  `replicadb-server/frontend/playwright.docs.config.ts`,
  `replicadb-server/frontend/scripts/playwright-docs-config.test.mjs` (new),
  `replicadb-server/frontend/package.json`
  Changes: Extend source and built-artifact validation to enforce unique
  content slugs, all required legacy-topic ownership, local image imports,
  image dimensions, alt/caption text, safe examples, generated OpenAPI page
  coverage, and preserved legacy/tool routes. Keep the existing docs/API and
  frontend-drift job ownership; amend the contribution guide so documentation
  maintainers know when to rerun OpenAPI export, frontend type generation,
  screenshot capture, connector checks and portal verification. Change the
  workflows only if the updated test commands or new contract files require
  explicit paths. Assert dedicated screenshot discovery with an explicit
  positive docs match and exclude both documentation captures and visual
  baselines from the general frontend Playwright configuration. Run the new
  discovery contract in CI so a normal frontend E2E run cannot create or alter
  public documentation assets. Retain Node 22, Java 17, static Pages output and
  no runtime deployment dependency.
  Tests: From a clean checkout run `npm --prefix docs run check`, build,
  validate, Node contracts and documentation Playwright tests; run the focused
  OpenAPI Maven test and frontend typecheck/schema test; run documentation
  workflow validation. Add negative fixtures for duplicate slugs, a broken
  local asset, an orphan screenshot, a missing OpenAPI operation page and a
  prohibited secret pattern. Run the Playwright discovery test against both
  configuration files and prove that the generic suite excludes docs captures
  while the docs project selects them. Verify that `docs-pages.yml` deploys
  only after its build gate succeeds and preserves `/ReplicaDB` compatibility
  routes.
  Dependencies: Tasks 3.1, 4.1, 4.2, 5.1, 6.1, and 12.1

### 14. Perform the final cross-contract editorial acceptance pass

- [x] **14.1 Verify factual completeness, generated output, and static publication together**
  Files: `README.md`, `replicadb-server/README.md`,
  `docs/PAGES_CUTOVER.md`, `docs/dist/**` (generated; do not hand-edit),
  `docs/openapi/replicadb-server.json`, `implementation_plan.md`
  Changes: Compare every retained historical subject with its canonical portal
  location and remove any remaining legacy-only source reference. Check that
  repository entry points link to canonical portal pages without duplicating
  long end-user guides. Inspect the generated static artifact under the real
  `/ReplicaDB` base path, preserving the Pages cutover and rollback procedure.
  Record no credentials, local URLs, generated reports or temporary capture
  artifacts in documentation changes.
  Tests: Run the complete docs command suite, API schema export/drift check,
  frontend generated-schema typecheck, relevant CLI/connector contracts,
  browser smoke tests for homepage, mode diagrams, server screenshots and API
  operation pages at desktop and mobile widths, then `git diff --check` and a
  repository secret scan. Confirm legacy `/server.html`, `/docs/docs.html`,
  `/docs/user-guide.html`, wizard and Markdown-tool URLs still resolve in the
  staged artifact.
  Dependencies: Task 13.1

## Technical Reference

<details>
<summary>Content recovery map</summary>

| Historical subject | Canonical portal destination | Evidence authority |
| --- | --- | --- |
| Product rationale, direct transfer, task terms | Start here and CLI overview | Current product/CLI code and glossary |
| Complete, complete-atomic, incremental | CLI replication modes | Manager behavior, mode tests, restored diagrams |
| Options, connection parameters, selection, queries, transforms | CLI configuration, filtering, references | `ToolOptions` and `_replicadb.conf` |
| Parallelism, bandwidth, multi-table, watermarks | CLI execution guides | CLI compatibility and manager contracts |
| Vendor and non-relational behavior | Connector pages and support matrix | `SupportedManagers` and `ManagerCapabilities` |
| Installation, home, API/worker topologies, backups | Server and operations guides | Packaged launchers, configuration, deployment tests |
| UI workflows and access boundaries | Screenshot-backed server pages | React routes, E2E fixtures, server authorization |
| Legacy architecture narrative | Architecture and operations pages | Current run, scheduling, persistence and security code |
| REST endpoint specifics | Generated OpenAPI operation pages | Spring controller/DTO/security source |

</details>

<details>
<summary>API inventory and response model</summary>

| Domain | Operations | Contract notes |
| --- | ---: | --- |
| Authentication | 4 | CSRF bootstrap, login, logout, current identity |
| Dashboard | 1 | Explicit effective time window and visible-job restriction |
| Jobs | 5 | CRUD plus permission-aware definitions |
| Runs | 7 | Lists, detail, log, manual trigger, cancel and retry |
| Schedules | 3 | Get, upsert and delete one job schedule |
| Datasources | 5 | Catalog CRUD with safe display only |
| Datasource permissions | 3 | ADMIN-managed grant list, replacement and revocation |
| Job permissions | 3 | Resource EDIT/ADMIN-managed grant list, replacement and revocation |
| Users | 5 | ADMIN-managed create, list, get, role/enabled and password update |
| Audit | 1 | ADMIN-filtered, paginated durable events |

All protected mutations require the established session and CSRF contract.
`POST /api/v1/jobs/{jobDefinitionId}/runs` additionally requires an
`Idempotency-Key` and can replay a recent request. The public documentation
will not document the test-only local-run seeding header.

</details>

<details>
<summary>Validation strategy</summary>

| Layer | Evidence |
| --- | --- |
| Historical editorial recovery | Topic-to-page contract, current-code review, no obsolete CDC or unsupported claims |
| CLI and connectors | Existing source option/property and Java manager capability contracts, parsed shell examples |
| Server workflow media | Curated screenshot registry, unique guide ownership, responsive Playwright checks, redaction scan |
| OpenAPI source | MockMvc specification test, explicit controller/DTO metadata, serialized wire-contract checks |
| Generated artifacts | Canonical committed JSON drift check and generated TypeScript typecheck; neither file is hand-edited |
| Static portal | Astro check/build, link/image/base-path validator, search/navigation/accessibility/responsive E2E |
| Release compatibility | Existing docs and Pages workflows plus staged legacy/tool route smoke tests |

</details>

## Out of Scope

- A live Swagger UI or interactive request proxy on GitHub Pages.
- Changes to replication manager behavior, CLI options, server persistence,
  security policy, API versioning, or database migrations.
- Translating the portal or introducing a second documentation locale.
- Reintroducing the former monolithic page as a visible legacy reference.
