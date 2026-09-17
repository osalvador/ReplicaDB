---
type: Learning
description: PostgreSQL ANY queries need typed array metadata preserved through the final JDBC call.
sources: [{ id: plan, resource: .ai/archive/keyring-lifecycle-status-re-encryption-and-flat-environment-configuration.plan.md }]
generated: { by: itx-code, at: "2026-09-10" }
status: stable
---

Keep `MapSqlParameterSource` and `Types.ARRAY` intact for PostgreSQL `ANY(:parameter)` queries. Generic helpers accepting only `Map<String, ?>` can erase the parameter metadata needed by the driver.
