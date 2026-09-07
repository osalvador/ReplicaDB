---
title: CLI troubleshooting
description: Diagnose malformed options, connection failures, resource pressure, and interrupted runs.
---

# CLI troubleshooting

Start with `./bin/replicadb --help`, the resolved options file, and the process
exit code. Never paste resolved connection values into a shared issue or log.

Set `verbose=DEBUG` in a protected options file when the ordinary INFO output
does not identify the failing phase. `WARN` and `ERROR` reduce output;
`verbose=true` selects DEBUG, while an invalid level falls back to INFO. Remove
diagnostic logs according to the same policy as options files. Optional
`sentry.dsn=${SENTRY_DSN}` telemetry is disabled when no DSN is configured;
enable it only under the deployment's observability and data-handling policy.

## Exit codes

| Code | Meaning | Next action |
| --- | --- | --- |
| `0` | Help, version, or replication completed | Verify destination row counts and run records. |
| `1` | Parse, validation, connection, or replication error | Read the error, check options and permissions, then retry after correction. |
| `2` | Replication cancelled | Inspect the sink and choose a deliberate recovery mode. |

## Common failures

- **Malformed value:** numeric options such as `--jobs` must parse; malformed
  invocations return code 1.
- **Missing connection:** both source and sink connections are required for a
  transfer. Confirm the options-file path and environment substitutions.
- **Permission error:** check source read rights, sink write/create rights,
  staging rights, and driver-specific connection settings.
- **Memory pressure:** reduce `fetch.size`, then reduce `jobs`; inspect the
  connector's staging and type conversion behavior.
- **Unexpected empty sink:** complete mode can expose a truncated sink during
  execution. Use complete-atomic where its connector and staging contract fit.
- **Conversion failure:** verify the projected source column order against
  `sink.columns`, then check the owning connector page for unsupported source
  or sink types. ReplicaDB does not make every vendor value portable.
- **Staging failure:** verify create/drop permissions and capacity for a
  generated `sink.staging.schema`, or the lifecycle permissions for a fixed
  staging table. User-provided staging tables are retained during cleanup.

The CLI does not automatically resume after interruption. For managed durable
recovery, use the [server operations guide](/ReplicaDB/operations/failure-recovery/).
