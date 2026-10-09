# Mini-plan: OSS Scanner enrollment files (Tier 1, isolated from implementation_plan.md)

## Tasks

1. [x] Dockerfile `.oss-scanner/Dockerfile` (Complexity Medium)
   - Evidence: `docker build` exit 0 on the integrated base (origin/master c9c0823d). The image contains the core artifact in `~/.m2`, and `mvn -o package` for `replicadb-server` succeeds with `--network none`.
   - First attempt failed inside the frontend plugin: the corporate TLS proxy was not trusted in the image. Resolved by disconnecting the VPN, with no change to the Dockerfile.
2. [x] Threat model `.oss-scanner/threat_model.md` (Complexity Low)
   - Evidence: five `##` sections, matching the official template. A sensitive-pattern grep returned no matches.
3. [x] Publication (Complexity Low)
   - Evidence: `.oss-scanner/Dockerfile` and `.oss-scanner/threat_model.md` are listed on `master` through the GitHub API.

## Execution Notes

- Plan isolation: `implementation_plan.md` (Dependabot merges) was neither read further nor advanced.
- Docker daemon was not running at first; it was started by the user.
- Push to `master` was rejected: the remote had 23 new commits. Integration was done without touching the dirty working tree: a temporary worktree from `origin/master` with cherry-picks of `125d8413` (docs, unpublished before) and `59f278d8` (OSS Scanner). Remote hashes: `1618e892` and `c9c0823d`.
- Decision confirmed by the user: publish both local commits, including the docs commit that was outside the original mini-plan.
- Local `master` still points to the pre-rebase hashes and is diverged from `origin/master`. It was not reset.
- The worker terminal policy was unreadable in both contract paths; native and direct-terminal rules were used.

## Execution Retrospective

- Plan accuracy: the mini-plan assumed a fast-forward push. Reality: 23 remote commits and an unpublished local commit made a direct push unsafe.
- Intent-to-Plan gap: "push directo a master" did not state how to handle a non-fast-forward remote or a dirty working tree.
- Plan-to-Implementation gap: the frontend plugin downloads Node at build time. Behind a TLS-intercepting proxy, the container build fails with PKIX errors.
- Pattern: when the working tree is dirty and the remote has advanced, publish from a temporary worktree based on `origin/master`, then re-validate the build on that exact base.
- Known limits: the image runs no tests (they need Testcontainers and Docker). `COPY . /src` copies `target/` and `.worktrees/` because there is no root `.dockerignore`. `project.yaml` and `validate.py` belong to a later step in a fork of `anthropics/oss-scanner`.
