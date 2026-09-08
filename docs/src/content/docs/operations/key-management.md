---
title: Key management
description: Protect, rotate, and validate the managed datasource keyring.
---


The default keyring is `REPLICADB_SECURITY_MASTER_KEY_FILE` or
`/run/secrets/replicadb-master-key` in a managed container. It contains a
current version and versioned 256-bit AES keys. Startup fails when it is
missing, unreadable, malformed, or missing the current valid key.

## Prepare and distribute the keyring

Create and store the keyring in the deployment secret manager, outside
PostgreSQL. Mount the same readable keyring into every API and worker that
shares managed datasource profiles, restricting the file to the runtime user.
Before sending traffic to a new instance, verify that it starts successfully,
can read existing datasource profiles, and uses the same current key version as
the rest of the fleet.

Back up the keyring with its PostgreSQL metadata recovery set. A recreated
PostgreSQL bundle cache is harmless; a lost keyring makes existing encrypted
datasource values unrecoverable. Never place the keyring in source control,
application logs, screenshots, or a support ticket.

## Rotation

1. Add a new key version while retaining the old version.
2. Deploy the keyring to every API and worker instance.
3. Run the datasource re-encryption operation.
4. Verify every envelope uses the current version.
5. Remove the old version only after no row references it.

Perform the rollout in that order so every running API and worker can decrypt
old envelopes while re-encryption is in progress. Validate a representative
datasource read and a worker-executed run before removing the previous key. If
an instance cannot read the new keyring, stop its rollout and restore the last
keyring version that all active profiles require.

Never export plaintext security values during rotation. See [backups and restore](/ReplicaDB/operations/backups-and-restore/)
for recovery order and [distributed deployment](/ReplicaDB/operations/distributed-deployment/)
for the shared keyring boundary.
