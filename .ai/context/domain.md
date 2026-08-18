## Core Contracts
| Concept | Contract |
| --- | --- |
| Source/sink | A configured input and destination selected by `DataSourceType` and manager factories. |
| Manager | Owns connection, dialect, type mapping, partitioning, lifecycle hooks, and sink capability. |
| Task/partition | One task owns one source and sink manager pair; partitioning is manager-specific. |
| Row set | A JDBC-shaped cursor and metadata surface that lets non-JDBC inputs reuse sink logic. |
| Staging | Generated staging resources may be cleaned; user-defined staging tables are preserved. |

## Managed Server Model
| Entity | Key fields | Invariants |
| --- | --- | --- |
| `JobDefinition` | source/sink references, table pair, mode, jobs, optional watermark | one table pair; credentials are `${env:VARIABLE}` references; watermark column requires incremental mode |
| `JobRun` | status, attempt, lease, counters, watermark, warnings | state changes follow `JobRunStateMachine`; retries create a new pending row |
| `JobSchedule` | job id, Quartz cron, time zone, enabled | schedule persistence is authoritative; Quartz is reconciled from it |
| `AppUser`/`JobPermission` | identity, global role, per-job permission | backend ACL checks precede job operations; ADMIN bypass is explicit |
| `AuditEvent` | actor, action, resource, outcome, sanitized detail | audit failure is logged but does not roll back the business operation |

## Business Rules
- `complete`, `incremental`, and `complete-atomic` have different sink and retry guarantees; do not generalize them across managers.
- Managed retries re-execute from the beginning. Watermarks advance only after a successful incremental merge.
- Cancellation is immediate at the control-plane contract and can leave `complete` or an in-progress merge in an indeterminate state; the warning is persisted.
- Data movement preserves values, metadata, precision, nullability, and unsupported-conversion failures.

## Domain Vocabulary
| Term | Definition |
| --- | --- |
| Configuration reference | A stored environment reference resolved immediately before core execution, never a resolved secret. |
| Active run | `PENDING`, `RUNNING`, or `CANCEL_REQUESTED`; PostgreSQL uniqueness prevents overlap per job. |
| Committed watermark | The last successful incremental boundary, stored as text and rebound using source metadata. |

## Reference Implementations
- `src/main/java/org/replicadb/cli/ReplicationMode.java`
- `src/main/java/org/replicadb/execution/ReplicationExecutionContext.java`
- `replicadb-server/src/main/java/org/replicadb/server/job/domain/JobDefinition.java`
- `replicadb-server/src/main/java/org/replicadb/server/job/domain/JobRunStateMachine.java`
- `replicadb-server/src/main/java/org/replicadb/server/security/JobAccessService.java`

## Recent Learnings
- [WARNING] Check partial unique indexes against every multi-row state transition before changing run states; retry changes the old row before inserting its replacement. Source: `phase-1b-state-layer`.
- [WARNING] Validate secret policy against connection strings as well as password fields. Source: `phase-1b-state-layer`.
