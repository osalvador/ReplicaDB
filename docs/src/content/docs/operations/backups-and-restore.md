---
title: Backups and restore
description: Back up PostgreSQL state and the encryption keyring together.
---


PostgreSQL is the source of truth for jobs, schedules, permissions, sessions,
audit, leases, retry chains, watermarks, and run logs. Back up the database
and `REPLICADB_SECURITY_KEYRING_FILE` as one recovery set. The PostgreSQL
bundle cache can be recreated; the keyring cannot.

## Prepare a recoverable backup

Use PostgreSQL backups that support point-in-time recovery and verify their
restore procedure before an incident. Capture the matching keyring version and
record the PostgreSQL recovery point with it. Stop a local-mode server before
copying its embedded PostgreSQL data; for a distributed deployment, take
database backups through the managed PostgreSQL service rather than copying
live files from API or worker hosts.

Protect backup media with the same access controls as the database and keyring.
Run logs are redacted and bounded to 256 KiB, but remain operational data and
must not be treated as public artifacts.

Prefer point-in-time PostgreSQL recovery. Restore the metadata database and
keyring before starting API or worker processes, validate migrations and
encryption access, then start the API and workers in a controlled order.

Run logs are redacted and bounded to 256 KiB; their lifecycle follows the run.
Do not treat an unredacted application log or a keyring copy as a public
artifact.
