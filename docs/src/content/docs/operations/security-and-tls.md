---
title: Security and TLS
description: Secure sessions, login, API exposure, worker health, and runtime secrets.
---


Put TLS or an authenticated reverse proxy in front of an API exposed beyond a
trusted host. Production session cookies are HTTP-only, secure, and SameSite
Lax, with a 30-minute JDBC-backed session timeout. Local/Compose smoke mode
may use non-TLS cookies only on a private test network.

## Define the traffic boundary

Expose only API traffic through the trusted ingress. Keep PostgreSQL on the
control network and keep worker management health private. Workers have no
public product API, so publishing port `9091` does not make a worker usable by
clients and unnecessarily exposes operational information.

Before enabling external traffic, confirm that the ingress reaches healthy API
instances, redirects or rejects plaintext HTTP according to local policy, and
preserves the session cookie attributes. Use the [distributed deployment](/ReplicaDB/operations/distributed-deployment/)
topology as the network boundary reference.

## Configure and validate TLS

Terminate TLS at the trusted ingress or configure Spring Boot's standard
`server.ssl.*` properties with a mounted JKS or PKCS12 key store. Keep the key
store password in the deployment secret provider, serve the complete
certificate chain, and test renewal before the current certificate expires.
Drain or roll API instances so renewal does not interrupt all sessions at
once. The worker management listener is private by design; do not expose it as
a public TLS endpoint.

After deployment, validate the certificate chain and hostname from a client,
sign in through the public origin, and check a protected API request. Check
that session cookies are Secure and HTTP-only in production. A successful
process liveness probe alone does not prove that TLS, proxy forwarding, or
session handling are correct.

## Protect runtime secrets

Login throttling allows 5 failed attempts per account and source address in
a rolling 15-minute window. Reservations use PostgreSQL advisory locks and
fail closed when PostgreSQL is unavailable.

Keep the worker management port private. Keep credentials, keyrings, sessions,
CSRF values, lease identity, and datasource security out of logs, metrics,
audit detail, screenshots, and deployment examples.

Inject database, keyring, bootstrap, and keystore values through the deployment
secret provider. Rotate a suspected secret before reviewing logs or artifacts;
do not attempt to recover it from diagnostics. Follow [key management](/ReplicaDB/operations/key-management/)
for datasource encryption keys and [troubleshooting](/ReplicaDB/operations/troubleshooting/)
for readiness failures.
