---
title: Security and TLS
description: Secure sessions, login, API exposure, worker health, and runtime secrets.
---

# Security and TLS

Put TLS or an authenticated reverse proxy in front of an API exposed beyond a
trusted host. Production session cookies are HTTP-only, secure, and SameSite
Lax, with a 30-minute JDBC-backed session timeout. Local/Compose smoke mode
may use non-TLS cookies only on a private test network.

Login throttling allows 5 failed attempts per account and source address in
a rolling 15-minute window. Reservations use PostgreSQL advisory locks and
fail closed when PostgreSQL is unavailable.

Keep the worker management port private. Keep credentials, keyrings, sessions,
CSRF values, lease identity, and datasource security out of logs, metrics,
audit detail, screenshots, and deployment examples.