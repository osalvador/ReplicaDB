---
title: Security and TLS
description: Secure sessions, login, API exposure, worker health, and runtime secrets.
---

# Security and TLS

Put TLS or an authenticated reverse proxy in front of an API exposed beyond a
trusted host. Production session cookies are HTTP-only, secure, and SameSite
Lax, with a 30-minute JDBC-backed session timeout. Local/Compose smoke mode
may use non-TLS cookies only on a private test network.

Terminate TLS at the trusted ingress or configure Spring Boot's standard
`server.ssl.*` properties with a mounted JKS or PKCS12 key store. Keep the key
store password in the deployment secret provider, serve the complete
certificate chain, and test renewal before the current certificate expires.
Drain or roll API instances so renewal does not interrupt all sessions at
once. The worker management listener is private by design; do not expose it as
a public TLS endpoint.

Login throttling allows 5 failed attempts per account and source address in
a rolling 15-minute window. Reservations use PostgreSQL advisory locks and
fail closed when PostgreSQL is unavailable.

Keep the worker management port private. Keep credentials, keyrings, sessions,
CSRF values, lease identity, and datasource security out of logs, metrics,
audit detail, screenshots, and deployment examples.
