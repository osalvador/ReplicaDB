# Directory Update Log

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
