---
title: Security boundaries
description: Keep credentials, leases, sessions, and operational data in their owners.
---


Datasource security is encrypted before PostgreSQL persistence and is not
returned to the frontend. Safe connection displays, blank-preserving edits,
and explicit key clearing prevent a read-edit-save cycle from exposing or
silently deleting stored values.

## Data crosses explicit boundaries

The control plane accepts datasource security over its authenticated API,
encrypts it before persistence, and returns only redacted display metadata.
Workers decrypt resolved values only immediately before execution using the
configured keyring. A claim records stable encrypted snapshots for that
attempt, so a later datasource edit affects a later claim rather than changing
in-flight execution inputs.

Session cookies and CSRF tokens belong to the authenticated API origin. ACLs
and roles are enforced by the backend; route guards and hidden buttons are
usability signals only.

Lease tokens identify durable execution ownership and remain inside the
server job boundary. They are not frontend, REST, OpenAPI, audit, log, or
documentation fields. Run diagnostics are bounded and redacted; logs and
audit detail are still sensitive operational data.

## Identity and execution are different authorities

Session cookies and CSRF prove a browser request belongs to an authenticated
API session. Roles and job ACLs are enforced by backend services; hiding a
route or button is not authorization. Lease tokens instead fence one worker's
claim and are never a browser or REST credential. Keeping these authorities
separate prevents an authenticated user interface from becoming an execution
ownership channel.

Workers require the configured keyring and private management health. TLS,
reverse proxies, key rotation, backup, and restore must preserve these
boundaries rather than copying secrets into job definitions or deployment
examples.

## Security limits and operational handoff

Redaction reduces accidental disclosure but does not make logs, audit records,
or backups public. The architecture prevents managed plaintext values from
crossing API, frontend, OpenAPI, audit, log, and metric boundaries; it cannot
protect a value deliberately written into an external system by a user. See
[security and TLS](/ReplicaDB/operations/security-and-tls/),
[key management](/ReplicaDB/operations/key-management/), and
[backups and restore](/ReplicaDB/operations/backups-and-restore/) for the
procedures that maintain these boundaries.
