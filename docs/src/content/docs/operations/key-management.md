---
title: Key management
description: Protect, rotate, and validate the managed datasource keyring.
---


datasource read and a worker-executed run before removing the previous key. If
ReplicaDB uses a versioned **keyring** to encrypt and decrypt managed datasource
credentials. The keyring is the cryptographic material, not a shared storage
resource: every API and worker that reads the same datasource catalog must know
the same key versions, but each instance may receive its own local projection
of those values. Different ReplicaDB installations should use independent
keyrings.

The keyring is kept outside PostgreSQL. PostgreSQL stores encrypted envelopes
and their key-version metadata; it never stores the AES key material.

## Keyring format

The canonical file source is `REPLICADB_SECURITY_KEYRING_FILE`. Its default is
`/run/secrets/replicadb-master-key`. The deprecated
`REPLICADB_SECURITY_MASTER_KEY_FILE` variable remains accepted as a migration
alias.

The file contains a current version and one or more Base64-encoded 256-bit AES
keys:

```json
{
	"currentVersion": "v2",
	"keys": {
		"v1": "BASE64_AES_256_KEY",
		"v2": "BASE64_AES_256_KEY"
	}
}
```

Every value under `keys` must decode to exactly 32 bytes, and
`currentVersion` must name one of those entries. The process validates the
keyring during startup and fails before serving traffic when it is missing,
unreadable, malformed, or incomplete.

For platforms that inject secret values as environment variables, use the flat
form instead of embedding JSON in one variable:

| Variable | Role |
| --- | --- |
| `REPLICADB_SECURITY_KEYRING_CURRENT_VERSION` | Version used for new envelopes. |
| `REPLICADB_SECURITY_KEYRING_CURRENT_KEY` | Base64-encoded 32-byte AES key for that version. |
| `REPLICADB_SECURITY_KEYRING_SECONDARY_VERSION` | Optional second version that can still decrypt existing envelopes. |
| `REPLICADB_SECURITY_KEYRING_SECONDARY_KEY` | Base64-encoded 32-byte AES key for the secondary version. |

The inline form supports one current and one secondary version by design. Use
the file form when a deployment needs more simultaneous versions. File and
inline configuration are mutually exclusive. Surrounding whitespace around an
inline Base64 value is ignored, but malformed Base64 and any decoded length
other than 32 bytes fail startup.

## Envelope encryption

Each datasource encryption creates a fresh random 32-byte data-encryption key
(DEK). AES-256-GCM encrypts the credential map, and AES Key Wrap protects the
DEK with the selected key-encryption key (KEK):

```text
												 KEK v2
									 from the keyring source
													 |
													 | AES Key Wrap
													 v
									 random DEK (32 bytes)
													 |
													 | AES-256-GCM
													 v
								 datasource credentials
```

The encrypted bundle stores the algorithm, key version, wrapped DEK, nonce,
and ciphertext. The authenticated data includes the datasource identifier, so
an envelope cannot be copied to a different datasource row and decrypted
successfully.

## Startup snapshot

ReplicaDB loads its keyring once when the process starts. Replacing a mounted
file or changing an injected environment variable does not reload a running
process. Restart or redeploy **every API and worker** after changing the
keyring. This same snapshot-at-startup behavior applies to Kubernetes,
OpenShift, Cloud Run, ECS/Fargate, App Runner, and Azure Container Apps.

## Prepare and distribute the keyring

Create and store the keyring in the deployment secret manager, outside
PostgreSQL. Give every API and worker that shares managed datasource profiles
the same key material and restrict it to the runtime user. Before sending
traffic to a new instance, wait for the platform rollout to report completion
and verify that the instance starts successfully and can read existing
datasource profiles.

Back up the keyring with its PostgreSQL metadata recovery set. A recreated
PostgreSQL bundle cache is harmless; a lost keyring makes existing encrypted
datasource values unrecoverable. Never place the keyring in source control,
application logs, screenshots, or a support ticket.

## Safe rotation

Use two completed rollout generations. The completed rollout is the
synchronization barrier; do not activate a version before every instance has
the material needed to read it.

### Generation 1: distribute

Add `v2` while keeping `v1` current and deploy this keyring to every API and
worker:

```json
{
	"currentVersion": "v1",
	"keys": {
		"v1": "BASE64_AES_256_KEY",
		"v2": "BASE64_AES_256_KEY"
	}
}
```

Wait for the deployment platform's completion signal, such as
`kubectl rollout status deployment/<name>` or the equivalent completed state in
ECS, Cloud Run, or Container Apps. Do not proceed while any API or worker is
still on the previous generation.

### Generation 2: activate

Change only `currentVersion` to `v2` and roll out every API and worker again:

```json
{
	"currentVersion": "v2",
	"keys": {
		"v1": "BASE64_AES_256_KEY",
		"v2": "BASE64_AES_256_KEY"
	}
}
```

After this rollout, new datasource writes use `v2`, while every running
instance can still read `v1` and `v2`. A rolling deployment is safe because
the first generation already gave old and new processes both versions.

### Re-encrypt and retire

The administrative endpoints operate in bounded batches. They require an
authenticated ADMIN session and the CSRF header described in the [API
reference](/ReplicaDB/api/). First inspect the counts:

```bash
curl --fail --silent --show-error \
	--cookie "$REPLICADB_COOKIE_JAR" \
	"$REPLICADB_SERVER_URL/api/v1/keyring/status"
```

Then repeat a bounded batch until `remaining` is zero and `converged` is true:

```bash
curl --fail --silent --show-error \
	--request POST \
	--cookie "$REPLICADB_COOKIE_JAR" \
	--header "X-XSRF-TOKEN: $REPLICADB_CSRF_TOKEN" \
	"$REPLICADB_SERVER_URL/api/v1/keyring/reencrypt?batchSize=200"
```

Each response reports `reencrypted`, `remaining`, and `converged`. The
operation is resumable and idempotent; a stopped client can issue another
batch later. It decrypts and re-encrypts values in memory and never exports
plaintext credentials. The per-row transaction skips a datasource currently
locked by a job claim and processes it on a later batch.

Do not remove `v1` until status reports no `v1` envelopes and a representative
datasource read plus worker-executed run succeeds. Finally remove `v1` from the
keyring and complete one last rollout. If status reports an unknown version,
stop the operation and restore a keyring containing the missing material before
retrying.

### Known limitation

ReplicaDB does not maintain a fleet registry and cannot detect a partially
completed rollout from inside the application. Follow the platform's rollout
completion signal before each generation change. If a generation was activated
too early, redeploy the previous keyring containing every required version;
`GET /api/v1/keyring/status` can then reveal any already-created envelopes that
the restored process cannot read.

## Backup and restore

Protect the keyring and PostgreSQL backup as one recovery set. Restore the
database and the matching keyring before starting API or worker processes, then
validate migrations, `GET /api/v1/keyring/status`, and a representative
datasource read. A database backup without the matching keyring cannot recover
the encrypted datasource credentials. See [backups and restore](/ReplicaDB/operations/backups-and-restore/).
for recovery order and [distributed deployment](/ReplicaDB/operations/distributed-deployment/)
for the shared keyring boundary.
