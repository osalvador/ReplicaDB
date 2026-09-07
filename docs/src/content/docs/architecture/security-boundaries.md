---
title: Security boundaries
description: Keep credentials, leases, sessions, and operational data in their owners.
---


Datasource security is encrypted before PostgreSQL persistence and is not
returned to the frontend. Safe connection displays, blank-preserving edits,
and explicit key clearing prevent a read-edit-save cycle from exposing or
silently deleting stored values.

Session cookies and CSRF tokens belong to the authenticated API origin. ACLs
and roles are enforced by the backend; route guards and hidden buttons are
usability signals only.

Lease tokens identify durable execution ownership and remain inside the
server job boundary. They are not frontend, REST, OpenAPI, audit, log, or
documentation fields. Run diagnostics are bounded and redacted; logs and
audit detail are still sensitive operational data.

Workers require the configured keyring and private management health. TLS,
reverse proxies, key rotation, backup, and restore must preserve these
boundaries rather than copying secrets into job definitions or deployment
examples.