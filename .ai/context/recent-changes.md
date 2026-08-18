## Last 10 Master Changes
| Commit | Description | Layers | Date |
| --- | --- | --- | --- |
| `2668d99` | Remove CSRF schema drift | API/frontend | 2026-08-18 |
| `f6535af` | Stabilize frontend schema CI | frontend/CI | 2026-08-18 |
| `472adb3` | Use public npm registry in CI | frontend/build | 2026-08-18 |
| `5abc156` | Add Phase 2a frontend auth/monitoring | frontend/server | 2026-08-18 |
| `a7b9225` | Persist audit events and cancellation warnings | server state/audit | 2026-08-17 |
| `56f9243` | Add authentication and per-job ACLs | server security/API | 2026-08-17 |
| `8d12cdc` | Add Quartz job scheduling | server execution | 2026-08-17 |
| `b2f66ca` | Index job-run history queries | server persistence | 2026-08-17 |
| `24bec4a` | Add REST API core | server API | 2026-08-16 |
| `c897181` | Add PostgreSQL-backed state layer | server persistence/domain | 2026-08-16 |

## Structural Changes
- The repository now has a sibling `replicadb-server` artifact around the unchanged CLI core.
- The server owns PostgreSQL/Flyway state, Quartz reconciliation, session security, ACLs, audit retention, and a static Vite SPA.
- OpenAPI is a generated protocol surface; TypeScript schema output is committed and checked in CI.

## Patterns Introduced
- Server-to-core execution is translated through `JobDefinitionEnvResolver` and `ToolOptionsArgsBuilder`.
- State transitions, active-run uniqueness, idempotency, and cancellation warnings are persisted rather than inferred from logs.
- Frontend server state uses TanStack Query; non-terminal run polling mirrors backend terminal semantics.

## Recent Learnings
- [WARNING] **API contract**: compare generated OpenAPI output to deterministic DTO serialization and real JSON nullability. Source: `phase-2a-frontend-auth-monitoring`.
- [WARNING] **Frontend build**: never commit machine-specific package registry URLs; test `npm ci` in a clean environment. Source: `phase-2a-frontend-auth-monitoring`.
- [WARNING] **Security tests**: real-port session flows need explicit cookies/CSRF; MockMvc annotations are not equivalent. Source: `phase-1c-3-security`.
- [WARNING] **Persistence**: state-index constraints must be checked against transitions that update and insert rows in one transaction. Source: `phase-1b-state-layer`.

## Known Tech Debt
| Source | Description | Impact |
| --- | --- | --- |
| `src/main/java/org/replicadb/config/Sentry.java` | Telemetry integration remains a sensitive redaction boundary | future fields must not reintroduce connection secrets |
| `src/test/java/org/replicadb/ReplicaDBTest.java` | Legacy JUnit 4 residue remains beside Jupiter tests | inconsistent discovery/style |
| `S3Manager.java`, `KafkaManager.java` | SQL staging/DDL hooks do not apply to non-SQL sinks | mode capability differs by adapter |
| `replicadb-server/.../JobRunRepository.java` | Lease and heartbeat values are set at claim time but are not renewed or recovered by distributed workers | managed execution remains single-instance until the planned worker phase |

## Gap Recurrence Candidates
None identified. The recurring-looking topics are each represented in only one or two archived retrospectives, below the 3-plan promotion threshold.
