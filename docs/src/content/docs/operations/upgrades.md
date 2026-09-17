---
title: Upgrades
description: Apply forward-only migrations and move scheduler ownership safely.
---


Migrations V1 through V21 are forward-only. Apply Flyway migrations before
starting a new API/worker cluster and keep Quartz schema creation under the
managed migration path; automatic Quartz schema creation is disabled.

For the RAMJobStore to JDBC handoff, drain and stop every API using the old
store, apply the scheduler migration, verify JDBC settings, and start all API
instances with identical clustered settings. Use `instanceId=AUTO`,
PostgreSQL locking, stable scheduler names and job keys, clustered mode, and
`MISFIRE_INSTRUCTION_DO_NOTHING`.

Mixed RAM/JDBC scheduler ownership is prohibited. Back up PostgreSQL and the
keyring before a major upgrade and verify a representative job, schedule,
permission, and run after startup.