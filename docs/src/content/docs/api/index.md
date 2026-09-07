---
title: API reference
description: Read-only reference for the authenticated ReplicaDB server API.
slug: api-introduction
---


ReplicaDB Server exposes its control plane below the `/api/v1` base path. The
endpoint pages are generated from the tested Springdoc contract and grouped by
the same domains used in the application: authentication, dashboard,
datasources, permissions, jobs, schedules, runs, users, and audit.

The public documentation does not proxy live requests. The API uses
same-origin session cookies and CSRF protection, so interactive calls should be
made from an authenticated deployment or a controlled local client rather than
sending credentials through a third-party documentation host.

## Establish a session

The API uses a server-owned `JSESSIONID` cookie. State-changing requests also
require an `X-XSRF-TOKEN` header whose value matches the `XSRF-TOKEN` cookie.
Bootstrap them in this order:

1. `GET /api/v1/auth/csrf` to initialize CSRF state and the cookie jar.
2. `POST /api/v1/auth/login` with the username and password, preserving cookies.
3. Read the `XSRF-TOKEN` cookie and send its value as `X-XSRF-TOKEN` on each
	 protected `POST`, `PUT`, `PATCH`, or `DELETE` request.
4. Use `GET /api/v1/auth/me` to verify the current identity and role.
5. End the session with `POST /api/v1/auth/logout`, including the CSRF header.

```bash
server_url="${REPLICADB_SERVER_URL:?set REPLICADB_SERVER_URL}"
cookie_jar="${REPLICADB_COOKIE_JAR:-./replicadb-session.cookies}"

curl --fail --silent --show-error \
	--cookie-jar "$cookie_jar" \
	"$server_url/api/v1/auth/csrf"

curl --fail --silent --show-error \
	--cookie "$cookie_jar" --cookie-jar "$cookie_jar" \
	--header 'Content-Type: application/json' \
	--data '{"username":"<username>","password":"<password>"}' \
	"$server_url/api/v1/auth/login"
```

Treat both cookie files as credentials. Keep them outside source control,
restrict file permissions, and remove them when the integration session ends.
Login is throttled after repeated failures; a throttled attempt returns `429`.

## Apply authorization correctly

Global roles are `ADMIN`, `OPERATOR`, and `VIEWER`. Datasources add `VIEW`,
`USE`, and `EDIT`; jobs add `VIEW`, `EDIT`, `EXECUTE`, and `CANCEL`. A route
being visible in the frontend does not grant API access. The backend applies
global and resource permissions to every request and filters list/dashboard
results before returning them.

Datasource responses contain `safeConnectDisplay` and capability flags, never
resolved security values. On datasource update, omitted or blank security
inputs preserve stored encrypted values; `clearSecurityKeys` is the explicit
removal mechanism.

## Page and filter collections

Paginated endpoints use zero-based `page` and a `size` that defaults to `50`
and is capped at `200`. Responses include `content`, effective `page`,
effective `size`, and `totalElements`.

Run and audit time filters use UTC ISO-8601 date-times. Run `status` may be
repeated and is case-insensitive. Dashboard bounds are explicit in the
response; when omitted, the server returns the effective 24-hour window ending
at current server time.

```bash
curl --fail --silent --show-error \
	--cookie "$cookie_jar" \
	"$server_url/api/v1/runs?page=0&size=50&status=FAILED&from=2026-01-01T00:00:00Z&to=2026-01-02T00:00:00Z"
```

## Trigger a run idempotently

`POST /api/v1/jobs/{jobDefinitionId}/runs` requires a non-blank
`Idempotency-Key` of at most 255 characters. The key applies only to manual run
triggering. Replaying a retained key returns the same accepted run instead of
creating a duplicate; retry and scheduled dispatch use their own lifecycle.

```bash
curl --fail --silent --show-error \
	--request POST \
	--cookie "$cookie_jar" \
	--header "X-XSRF-TOKEN: <csrf-cookie-value>" \
	--header "Idempotency-Key: <stable-request-key>" \
	"$server_url/api/v1/jobs/<job-definition-id>/runs"
```

An accepted trigger or retry returns `202` with a `Location` header for the
run. Retry starts a new attempt from the beginning; it is not resume behavior.

## Handle responses and problems

| Status | Meaning |
| --- | --- |
| `200` | Read or update completed. |
| `201` | Resource created; inspect `Location`. |
| `202` | Run dispatch accepted; poll the returned run. |
| `204` | Delete or logout completed with no response body. |
| `400` | Request syntax, validation, filter, or range is invalid. |
| `401` | Authentication is missing or credentials are invalid. |
| `403` | Session is valid but permission or CSRF validation failed. |
| `404` | Resource is absent or unavailable to the operation. |
| `409` | Request conflicts with current resource or run state. |
| `429` | Login attempts are temporarily throttled. |

Failures use `application/problem+json` with the RFC 7807 fields `type`,
`title`, `status`, `detail`, and `instance`. Dynamic detail is
credential-redacted, but clients should still treat it as operational data.

```json
{
	"type": "about:blank",
	"title": "Conflict",
	"status": 409,
	"detail": "The requested operation conflicts with current state.",
	"instance": "/api/v1/runs/<run-id>/retry"
}
```

## Browse by domain

- [Authentication](/ReplicaDB/api/operations/tags/authentication/)
- [Dashboard](/ReplicaDB/api/operations/tags/dashboard/)
- [Datasources](/ReplicaDB/api/operations/tags/datasources/)
- [Datasource permissions](/ReplicaDB/api/operations/tags/datasource-permissions/)
- [Jobs](/ReplicaDB/api/operations/tags/jobs/)
- [Job permissions](/ReplicaDB/api/operations/tags/job-permissions/)
- [Schedules](/ReplicaDB/api/operations/tags/schedules/)
- [Runs](/ReplicaDB/api/operations/tags/runs/)
- [Users](/ReplicaDB/api/operations/tags/users/)
- [Audit](/ReplicaDB/api/operations/tags/audit/)
