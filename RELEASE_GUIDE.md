# ReplicaDB Release Guide

This guide defines the controlled publication flow for `v1.0.0` and later
releases. A release is not published until local gates and the required remote
workflows are green.

## Command Contract

`release.sh` has four separate commands:

```bash
./release.sh validate VERSION
./release.sh prepare VERSION
./release.sh tag VERSION --ci-green
./release.sh push-tag VERSION
```

- `validate` is read-only. It checks semantic version syntax, both Maven
  project versions, the server dependency on `org.replicadb:ReplicaDB`, and
  the current release artifact names in the documentation.
- `prepare` requires the `master` branch, rejects tracked changes outside the
  release allowlist and unexpected untracked files, updates the POMs and
  documentation, creates `feat(release): prepare VERSION`, and pushes only
  `master`. It never creates or pushes a tag.
- `tag` requires a clean release worktree, a coherent version, an absent local
  and remote tag, and the explicit `--ci-green` confirmation. It creates only
  the annotated local tag `vVERSION`.
- `push-tag` requires the local tag to point at `HEAD` and pushes only that
  tag.

The script permits the known untracked planning and design paths described in
the implementation plan, but never stages them. Any other untracked path
blocks `prepare`, `tag`, and `push-tag`.

## Controlled Sequence

1. Start on `master` with the intended release changes limited to the release
   allowlist.
2. Run the local checks and the read-only contract check:

   ```bash
   ./release.sh validate 1.0.0
   ```

3. Prepare and push the release candidate:

   ```bash
   ./release.sh prepare 1.0.0
   ```

4. Wait for the exact pushed commit to pass the `Only CI/CT` workflow,
   `CodeQL`, and `pages-build-deployment`. A pending, failed, cancelled, or
   unexpected skipped job blocks the tag.
5. After the remote gates are green, create the local tag:

   ```bash
   ./release.sh tag 1.0.0 --ci-green
   ```

6. Inspect the annotated tag, then publish it explicitly:

   ```bash
   git show v1.0.0
   ./release.sh push-tag 1.0.0
   ```

7. Wait for `CI_Release.yml` and verify the complete GitHub release asset set.

## Release Asset Contract

The release workflow must publish all of these assets together:

- `ReplicaDB-1.0.0.tar.gz`
- `ReplicaDB-1.0.0.zip`
- `ReplicaDB-server-1.0.0.tar.gz`
- `ReplicaDB-server-1.0.0.zip`
- `replicadb-server-1.0.0.jar`
- `SHA256SUMS`

The direct JAR must match the server archive JAR and must not contain native
PostgreSQL bundles. Docker publication uses the versioned and `latest` tags
only after the release workflow has passed its build and smoke gates.

## Local Preflight

Before `prepare`, complete the release candidate gates:

- root CLI install before the server package
- server tests and the embedded PostgreSQL profile
- reproducible CLI and server archives
- direct JAR identity and checksum verification
- Docker image and Compose smoke checks
- POSIX launcher and documentation checks
- workflow YAML and `actionlint` checks
- `git diff --check` and `bash -n`

Keep generated staging directories outside the repository and remove them
after validation. Do not put credentials, tokens, or database connection
values in logs or release files.

## Remote Gates

Check the workflow runs by the exact preparation commit, not merely by the
latest green run:

```bash
gh run list --branch master
gh run view RUN_ID --json status,conclusion,jobs
```

The CT run must include the Windows launcher, frontend E2E, embedded matrix,
server module, fairness, and multinode jobs. Infrastructure failures should be
recorded separately from code regressions; neither is permission to tag while
the required gate remains unresolved.

## Reruns and Recovery

Do not delete tags automatically. If a remote gate fails, fix the cause, make
a new preparation commit, push `master`, and repeat the remote-gate wait. Do
not reuse a tag for a different commit. A rerun of the same GitHub release may
use the workflow's explicit `--clobber` behavior only for the same version and
only when the complete asset set is regenerated.

For a command failure, inspect `git status --short`, correct the release scope,
and rerun `validate`. Do not use `git reset --hard`, broad `git clean`, or
automatic tag deletion as recovery steps.

## Post-Publication Checks

After `v1.0.0` is published, download the CLI and server archives into clean
directories, verify `SHA256SUMS`, and run `help`, `start local`, `status`, and
`stop`. Confirm that the CLI uses `REPLICADB_HOME`, the server uses
`REPLICADB_SERVER_HOME`, and no Java or PostgreSQL process remains after the
smoke test.
