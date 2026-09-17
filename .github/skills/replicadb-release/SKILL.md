---
name: replicadb-release
description: "Use when preparing, validating, gating, tagging, or publishing a ReplicaDB release such as v1.0.0."
---

# ReplicaDB Release

Use this skill to run the controlled release workflow. Keep the release
version, Git commit, remote workflow runs, tag, and published asset set tied to
one another throughout the process.

## State Machine

```text
preflight -> validate -> prepare -> local-gates -> remote-green
remote-green -> final-confirmation -> tag -> publish-tag -> verify-release
```

Never skip a state. A failure or unexpected skipped job returns the workflow to
investigation; it does not permit the next state.

## Preflight

1. Confirm the repository is on `master` and the intended release changes are
   limited to the release allowlist.
2. Preserve unrelated untracked planning, design, and generated paths outside
   the release commit. Reject any unexpected untracked path.
3. Confirm the release version is a semantic `X.Y.Z` value. The first stable
   target is `v1.0.0`.
4. Ensure no credentials, tokens, database values, or sensitive command output
   will be written to logs or release files.
5. Keep temporary build and staging directories outside the repository.

## Prepare

Run the non-destructive contract check first:

```bash
./release.sh validate 1.0.0
```

Prepare the candidate after the read-only check:

```bash
./release.sh prepare 1.0.0
```

`prepare` updates the CLI and server POM contract and current release
naming, creates `feat(release): prepare 1.0.0`, and pushes only `master`. It
must not create or push `v1.0.0`. Record the preparation commit SHA and use
that exact SHA for every remote-gate query.

## Local Gates

Run and record the focused checks against the prepared commit before relying
on remote CI. If a gate fails, correct the release scope and prepare a new
commit before continuing:

- root CLI install, server package, archives, direct JAR, and `SHA256SUMS`
- server tests and the embedded PostgreSQL profile
- archive reproducibility, JAR identity, and absence of PostgreSQL bundles
- Docker image smoke and Compose configuration checks
- POSIX launcher, documentation, YAML parser, and `actionlint` checks
- `bash -n`, `git diff --check`, and the start-local guard

Do not stage generated archives, staging directories, design files, or
knowledge-plan files.

## Remote Green Gate

After `prepare` pushes `master`, query workflows for the recorded preparation
SHA. Require success for all of the following:

- `.github/workflows/CT_Push.yml`, including Windows launcher, frontend E2E,
  embedded matrix, server module, fairness, and multinode coverage
- CodeQL
- `pages-build-deployment`

Use the GitHub CLI without placing credentials in the command or its output:

```bash
gh run list --branch master
gh run view RUN_ID --json status,conclusion,jobs
```

The remote gate is green only when the required runs belong to the preparation
SHA and every required job is successful. Pending, failed, cancelled, or
unexpected skipped jobs block tagging. Classify infrastructure failures
separately, but keep the tag blocked until the required state is resolved.

## Final Confirmation, Tag, and Publish

Ask for final confirmation only after local gates and the exact-SHA remote
checks are green. Then create the annotated tag locally:

```bash
./release.sh tag 1.0.0 --ci-green
git show v1.0.0
```

The `--ci-green` argument is an explicit acknowledgment that the remote gate
was checked. The command requires a coherent version, a release-clean
worktree, and no existing local or remote tag. It creates no remote tag.

Publish the tag as a separate action:

```bash
./release.sh push-tag 1.0.0
```

Wait for `.github/workflows/CI_Release.yml` after the tag push. Do not upload
assets manually outside the workflow.

## Release Verification

The GitHub release must contain the complete set, not a subset:

- `ReplicaDB-1.0.0.tar.gz`
- `ReplicaDB-1.0.0.zip`
- `ReplicaDB-server-1.0.0.tar.gz`
- `ReplicaDB-server-1.0.0.zip`
- `replicadb-server-1.0.0.jar`
- `SHA256SUMS`

Verify the checksums, archive contents, direct JAR identity, Docker tags
`1.0.0` and `latest`, and the release workflow jobs
`build`, `windows_launcher`, `embedded_postgres`, and `publish`.

After publication, run the archive smoke test from clean directories: `help`,
`start local`, `status`, and `stop`. Confirm CLI state remains under
`REPLICADB_HOME`, server state remains under `REPLICADB_SERVER_HOME`, and no
Java or PostgreSQL process remains after the smoke test.

## Reruns and Recovery

If any local or remote gate fails, stop before tagging, fix the cause, create a
new preparation commit, push `master`, and repeat the exact-SHA remote gate.
Never move a tag to a different commit or delete tags automatically.

A rerun of an existing GitHub release may use `--clobber` only for the same
version and only when it regenerates and verifies the complete asset set. Keep
unrelated untracked files outside every rerun.
