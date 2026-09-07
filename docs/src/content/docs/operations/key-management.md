---
title: Key management
description: Protect, rotate, and validate the managed datasource keyring.
---

# Key management

The default keyring is `REPLICADB_SECURITY_MASTER_KEY_FILE` or
`/run/secrets/replicadb-master-key` in a managed container. It contains a
current version and versioned 256-bit AES keys. Startup fails when it is
missing, unreadable, malformed, or missing the current valid key.

## Rotation

1. Add a new key version while retaining the old version.
2. Deploy the keyring to every API and worker instance.
3. Run the datasource re-encryption operation.
4. Verify every envelope uses the current version.
5. Remove the old version only after no row references it.

Never export plaintext security values during rotation. Back up the keyring
with PostgreSQL metadata; losing it makes encrypted datasource values
unrecoverable.