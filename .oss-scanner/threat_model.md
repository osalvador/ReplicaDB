# Threat model

## What this project does and where untrusted input enters

ReplicaDB copies data between JDBC databases. It runs from the command line and through `replicadb-server`, a REST service with a web UI. Treat these inputs as untrusted:

- Options files and connection strings, including `${VAR}` substitution.
- Job and datasource definitions submitted to the REST API, including `sourceTable`, `sinkTable`, column lists and `sourceWhere`.
- Rows read from source databases and written to sink databases.
- HTTP requests to `replicadb-server`, including JSON bodies and the web UI.

`sourceQuery` accepts free-form SQL. It is a privilege of authenticated users with job-creation rights (ADMIN or OPERATOR). A bypass of the role check or of the datasource capability check is in scope.

## Components that matter most / least

- Most: the `replicadb-server` REST API (authentication, role checks, CSRF, job and datasource handling) and its web UI (XSS).
- Then: SQL construction in the core CLI for table and column identifiers that come from options files or API job definitions.
- Least: connector behavior that only uses trusted, local options.

## How to exercise it

- Core: run `bin/replicadb` with an options file. The integration suites under `src/test` need live databases.
- Server: built from `replicadb-server`. Authenticate at `/api/v1/auth/login` before calling the REST API.

## How you rate severity

- Critical: unauthenticated remote code execution; authentication bypass to ADMIN; exposure of keyring credentials to users without permission.
- High: OPERATOR escalation to ADMIN or reading datasource credentials; SQL injection through job fields other than `sourceQuery`; stored XSS in the web UI that runs in an admin session.
- Medium: denial of service of the server or of job runs; disclosure of non-sensitive information; partial bypass of CSRF or login throttling.
- Low: hardening issues and verbose internal error messages.
- Not a vulnerability by design: `sourceQuery` executed by an ADMIN or OPERATOR with valid permissions.

Not yet verified by the maintainers: whether read-only roles can create or change jobs, and which concatenated SQL sites in the core receive identifiers from users. Rate findings in these areas from your own analysis.

## Anything to leave alone

- Test code, `docs/`, `target/`, and copies under `.worktrees/`.
- Known vulnerabilities in third-party dependencies, unless ReplicaDB uses them unsafely.
- `sourceQuery` SQL run by an ADMIN or OPERATOR (see severity).
