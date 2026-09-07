---
title: CLI troubleshooting
description: Diagnose malformed options, connection failures, resource pressure, and interrupted runs.
---

# CLI troubleshooting

Start with `./bin/replicadb --help`, the resolved options file, and the process
exit code. Never paste resolved connection values into a shared issue or log.

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

The CLI does not automatically resume after interruption. For managed durable
recovery, use the [server operations guide](/ReplicaDB/operations/failure-recovery/).