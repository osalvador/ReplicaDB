---
title: Backups and restore
description: Back up PostgreSQL state and the encryption keyring together.
---

# Backups and restore

PostgreSQL is the source of truth for jobs, schedules, permissions, sessions,
audit, leases, retry chains, watermarks, and run logs. Back up the database
and `REPLICADB_SECURITY_MASTER_KEY_FILE` as one recovery set. The PostgreSQL
bundle cache can be recreated; the keyring cannot.

Prefer point-in-time PostgreSQL recovery. Restore the metadata database and
keyring before starting API or worker processes, validate migrations and
encryption access, then start the API and workers in a controlled order.

Run logs are redacted and bounded to 256 KiB; their lifecycle follows the run.
Do not treat an unredacted application log or a keyring copy as a public
artifact.