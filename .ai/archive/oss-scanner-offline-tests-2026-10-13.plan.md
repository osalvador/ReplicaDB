# Tier 1 Mini-Plan: OSS Scanner offline build and tests

## Scope (confirmed by user)
- Fix the current OSS Scanner build and offline test errors. The VPN was removed; the proxy errors no longer reproduce.
- Docker-dependent Testcontainers tests are tagged and excluded only from the offline scanner run. They still run in the default CI profile.

## Tasks
1. Dockerfile offline precache: `.oss-scanner/Dockerfile` pre-resolves `surefire-junit-platform:3.2.5` and `junit-platform-launcher:6.0.3` during the networked build step.
2. Docker-dependent tests: `@Tag("docker")` on 23 `*Test.java` classes that use Testcontainers. Offline run uses `-Dreplicadb.excluded-test-groups=embedded-postgres,docker`.
3. Final validation: rebuild the image and run the offline suite with `--network none`.

## Execution Notes
- Build: `docker build -f .oss-scanner/Dockerfile` exits 0 after the VPN removal.
- Offline suite: `mvn -o test -Dreplicadb.excluded-test-groups=embedded-postgres,docker` ran 334 tests, 0 failures, 0 errors, BUILD SUCCESS.
- Before the change, the same offline suite ran 458 tests with 124 errors, all caused by missing Docker for Testcontainers.
- Not verified: `tools/check replicadb`. It clones `master`, which does not yet contain these uncommitted changes.

## Execution Retrospective
- Plan accuracy: the scope matched the observed errors. Surefire needed two cached artifacts beyond `dependency:go-offline`.
- Gap: the scanner shell has no Docker, so Testcontainers classes cannot run there. They are excluded by group, not fixed.
- Pending: commit and push `.oss-scanner/Dockerfile` and the 23 test files to `master`, which the scanner clones. Not done without user approval.
