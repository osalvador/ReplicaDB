# Directory Update Log

## 2026-09-04
* **Update**: Refreshed OKF concepts for the control-plane refinement (`8a9f52a`), managed replication diagnostics (`fbdf7df`), nullable committed watermarks (`4d58116`), and the current job deletion/local-startup worktree (frontend, APIs, execution, persistence, deployment, tests).
* **Update**: Added the dashboard summary interface, current V20/V21 persistence and deletion behavior, local resource lifecycle rules, and four learnings from the archived local PostgreSQL fixture plan.

## 2026-08-25
* **Update**: Completed and archived the Phase 3.3 API high-availability and operational-hardening plan (`.ai/archive/phase-3-3-api-high-availability-and-operational-hardening.plan.md`) with its `/itx-code` execution retrospective (commits `b54fa5c`, `19096c3`, `b52e356`, `96e41f4`).
* **Update**: Added eight Phase 3.3 execution learnings covering framework probes, profile drift, Flyway fixture ordering, artifact readiness, database-observable barriers, CLI architecture limits, CI tool portability, and `pipefail`-safe matching.
* **Update**: Refreshed OKF architecture, decision, testing, redaction, deployment, and technical-debt concepts to reflect Quartz JDBC clustering, PostgreSQL login throttling, worker management, Prometheus metrics, Compose validation, and the remaining Phase 3.4 fairness scope.

## 2026-08-20
* **Update**: Added OKF learnings from dependency compatibility analysis and CI/browser validation (`c6f4a91`, `a4b9e43`, `be7009d`) (build dependencies, automation, frontend tests).
* **Update**: Dependency automation and MariaDB driver handling were hardened (`be7009d`) (build dependencies, Dependabot, CI workflows).
* **Update**: Phase 3.1 distributed state contract implemented (`0759fec`): retry policy, PostgreSQL-owned eligibility, lease claims, expiry recovery, token fencing, durable cancellation, and API/frontend contract updates (domain, persistence, execution, interfaces, tests).
* **Update**: Local run seeding was stabilized (`04ed238`) for frontend development fixtures and managed run API tests (server API, frontend tooling, tests).

## 2026-08-19
* **Update**: Rebuilt the repository AI context as an OKF 0.2 bundle. Legacy `.ai/context` concepts were replaced; archived plans were retained as historical evidence.
* **Update**: Current worktree includes the Phase 2c frontend administration slice: users, job permissions, ADMIN route guards, and an admin Playwright flow (frontend, interfaces, tests). These files remain uncommitted and are described as worktree evidence.
* **Update**: Recent commits include the Phase 2b editor and run actions (`2b04ae6`), the frontend context refresh (`d386f9f`), and tab-state preservation (`31356a5`).

## 2026-08-18
* **Update**: Frontend authentication, monitoring, OpenAPI drift handling, and registry-neutral npm configuration were completed (`5abc156`, `472adb3`, `f6535af`, `2668d99`) (frontend, interfaces, build).

## 2026-08-17
* **Update**: Authentication, global roles, job ACLs, audit events, retention, and cancellation warnings were added (`56f9243`, `a7b9225`) (security, audit, job execution).
* **Update**: Quartz scheduling and run-history indexing were added (`8d12cdc`, `b2f66ca`) (scheduling, persistence, execution).

## 2026-08-16
* **Update**: REST job/run APIs and PostgreSQL-backed state were added (`24bec4a`, `c897181`) (interfaces, persistence, execution).

## 2026-08-15
* **Update**: The Spring Boot server artifact was split from the standalone CLI (`a0d6012`) (architecture, build).

## 2026-08-14
* **Update**: Per-run execution context, cancellation plumbing, and watermark injection were added (`c228ddc`, `4dd4cb5`, `21ce791`) (core execution, adapters).
