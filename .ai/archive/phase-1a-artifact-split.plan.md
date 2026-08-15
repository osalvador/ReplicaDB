# Implementation Plan: Phase 1a — Artifact Split (replicadb-server skeleton)

## Task Source

No JIRA ticket. Derived from `ARCHITECTURE_DECISIONS.md`, Decision 1 ("Single Codebase, Two Artifacts, CLI Compatibility") and Decision 2 ("Monolithic Control Plane First"), and the "Phase 1: Spring Boot API and Scheduler" implementation phase.

Phase 0 (`0-a`, `0-b1`, `0-b2`) is implemented (commits `c228ddc`, `4dd4cb5`, `21ce791`). The doc's "State layer" bullet list (informally called "Phase 0-c" in its prose) — `JobDefinition`/`JobRun` domain models, PostgreSQL persistence, Flyway, and the row-locking claim mechanism — was never implemented. Rather than finish it as a standalone "Phase 0-c" first, this plan folds it into Phase 1 as its first sub-plan, **Phase 1a**, and narrows Phase 1a's own scope further: this plan delivers **only the two-artifact build split** (a new `replicadb-server` Maven module with a bare Spring Boot skeleton). No `JobDefinition`/`JobRun`, no persistence, no REST endpoints, no scheduler exist after this plan. That is deliberate — combining the build/CI restructuring with domain/persistence design in one plan would exceed the ~20-task guard, mirroring why Phase 0 itself was split into 0-a/0-b1/0-b2.

Acceptance criteria for this plan, derived from Decision 1's "CLI compatibility contract" and Decision 2's "Phase 1 scope":

- A new `replicadb-server` artifact exists, builds independently, and starts a Spring Boot application under the `api` profile.
- The existing `replicadb` CLI artifact's classpath gains **zero** Spring Boot dependencies — verified by an automated test, not just by inspection.
- Existing CLI arguments, options-file keys, exit codes, and the ability to run with no metadata database reachable are all unaffected (no production files under `src/main/java/org/replicadb/**` change in this plan).
- CI (`CT_Push.yml`) builds and tests both artifacts; the release pipeline (`CI_Release.yml`) packages both.

## Overview

ReplicaDB is currently a single Maven module producing one CLI jar. Phase 1 requires a second artifact, `replicadb-server`, that will eventually host a REST API, a Quartz scheduler, and PostgreSQL-backed job state — without ever pulling Spring Boot onto the CLI's classpath. This plan proves that two-artifact structure works mechanically (build, dependency resolution, CI, release packaging) before any Phase 1 business logic is written, so later sub-plans (state layer, execution service, REST API, scheduler, security, frontend) build on a verified foundation instead of discovering build-system problems late.

## Architecture & Design

**Approach**: Sibling Maven module, no reactor (user-selected over a full parent/reactor restructuring, to avoid moving any existing `src/main/java` files).

- `pom.xml` at the repository root is **not modified structurally** and keeps producing the `replicadb` CLI jar exactly as today (packaging `jar`, no `<modules>`, no new `<parent>`). This is what keeps the CLI's classpath Spring-Boot-free by construction rather than by convention.
- A new top-level directory `replicadb-server/` contains a **standalone** `pom.xml` with its own `<parent>` (`org.springframework.boot:spring-boot-starter-parent`). It is not a submodule of the root pom and the root pom does not declare it as a module — the two projects are built with two separate `mvn` invocations.
- `replicadb-server` depends on `org.replicadb:ReplicaDB` (the existing artifact) as a plain `<dependency>`, resolved from the local Maven repository. This means **`mvn install` (not just `package`) must run on the root project before `replicadb-server` can build** — a real friction point for local development and CI, accepted here because it avoids touching any existing file path. This is called out explicitly in task 1.1's docs update and in CI (task 3.1).
- `replicadb-server` is versioned independently, starting at `0.1.0-SNAPSHOT`, since Phase 1 is unreleased and pre-alpha — it must not be conflated with the CLI's stable `0.18.x` line.
- Package namespace: `org.replicadb.server`, keeping the `org.replicadb` umbrella while staying physically separate from `org.replicadb.*` core packages.
- Scope for this plan is deliberately the `api` Spring profile only (per Decision 2, "The implementation target for Phase 1 is ... the `api` profile"). The `worker` profile belongs to Phase 2 and is out of scope here.
- Security: the only exposed surface in this plan is `/actuator/health`, restricted via `management.endpoints.web.exposure.include=health` (never `*`). There is no Spring Security yet (that is a later Phase 1 sub-plan per the doc's Spring Boot module list) — acceptable now because there is no data or secret behind this endpoint, but the tight exposure list is set now so it is not silently widened later.
- Performance/footprint: this directly implements Decision 1's compatibility contract — the CLI's startup time, footprint, and dependency surface must be provably unchanged, which is why task 2.1 is an automated test, not a manual check.

**Out of scope for this plan** (explicitly deferred to later Phase 1 sub-plans):

- `JobDefinition`/`JobRun` domain models, any persistence, Flyway, the claim mechanism (next sub-plan).
- Any REST endpoint beyond `/actuator/health`.
- Quartz scheduler, Spring Security, sessions, users/roles/audit, frontend SPA.
- Docker/Podman image for `replicadb-server` (`Containerfile`/`Dockerfile` changes) — deferred until there is an actual API worth shipping.
- The `worker` Spring profile (Phase 2).

## Implementation Tasks

### 1. Server Module Foundation

- [x] **1.1 Create the `replicadb-server` Maven module skeleton**
  Files: `replicadb-server/pom.xml` (new)
  Changes: New standalone pom with `<parent>org.springframework.boot:spring-boot-starter-parent</parent>` pinned to a concrete version, `3.3.5` (a known stable GA release compatible with Java 17; future bumps go through the same Dependabot flow already used elsewhere in this repo, not through this plan). `<groupId>org.replicadb</groupId>`, `<artifactId>replicadb-server</artifactId>`, `<version>0.1.0-SNAPSHOT</version>`. Dependencies: `spring-boot-starter-web`, `spring-boot-starter-actuator`, `spring-boot-starter-test` (scope `test`), and `org.replicadb:ReplicaDB:0.18.4` (match whatever the root `pom.xml`'s current `<version>` is at implementation time). Properties: `<java.version>17</java.version>` and `<junit-jupiter.version>6.0.3</junit-jupiter.version>` — this overrides Spring Boot 3.3.x's managed JUnit Jupiter 5.x with the same JUnit Jupiter 6.0.3 the root project already uses, per `test-patterns.instructions.md`'s "JUnit Jupiter 6 only" rule. If overriding this property causes a `spring-boot-starter-test` incompatibility (e.g., an AssertJ/Mockito artifact expecting JUnit 5's engine wiring), fall back to leaving `spring-boot-starter-test`'s managed JUnit 5.x for this module only, and document that isolated exception in task 4.1 the same way `ReplicaDBTest.java`'s legacy JUnit 4 exception is already documented for the root project. Add the `spring-boot-maven-plugin` with the `repackage` goal bound to `package` so `mvn package` produces an executable `replicadb-server-<version>.jar`.
  Tests: Build verification (no JUnit yet at this task): after running `mvn install -DskipTests` from the repository root, `mvn -f replicadb-server/pom.xml compile` succeeds with no missing-dependency errors.
  Dependencies: None

- [x] **1.2 Add the Spring Boot application entry point and `api` profile config**
  Files: `replicadb-server/src/main/java/org/replicadb/server/ReplicaDbServerApplication.java` (new), `replicadb-server/src/main/resources/application.yml` (new), `replicadb-server/src/main/resources/application-api.yml` (new)
  Changes: `ReplicaDbServerApplication` is a `@SpringBootApplication` class with a `main` method calling `SpringApplication.run(ReplicaDbServerApplication.class, args)`. `application.yml` sets `server.port: 8080` and `spring.application.name: replicadb-server`, and excludes the inherited MongoDB auto-configuration so the skeleton does not initiate an external database connection before the metadata layer exists. `application-api.yml` is currently empty (reserved for API-profile-specific config added by later sub-plans) but its presence documents that `-Dspring.profiles.active=api` is the supported startup mode, matching Decision 2's documented start command.
  Tests: `replicadb-server/src/test/java/org/replicadb/server/ReplicaDbServerApplicationTest.java` — `@SpringBootTest` with `@ActiveProfiles("api")` asserts the application context loads without error.
  Dependencies: Task 1.1

- [x] **1.3 Expose a restricted `/actuator/health` endpoint**
  Files: `replicadb-server/src/main/resources/application.yml`
  Changes: Add `management.endpoints.web.exposure.include: health` (only `health`, nothing else) so no other actuator endpoint (`env`, `beans`, `configprops`, etc.) is reachable.
  Tests: `replicadb-server/src/test/java/org/replicadb/server/HealthEndpointTest.java` — `@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)` with `@ActiveProfiles("api")`, uses `TestRestTemplate` to `GET /actuator/health` and asserts HTTP 200 with body containing `"status":"UP"`. A second test in the same class asserts `GET /actuator/env` returns 404 (not exposed).
  Dependencies: Task 1.2

- [x] **1.4 Prove the sibling-module dependency on the core artifact resolves and is usable**
  Files: `replicadb-server/src/test/java/org/replicadb/server/CoreDependencyResolutionTest.java` (new)
  Changes: No production code change. This test is the deliverable. `ReplicaDB`'s only public static members are `main(String[])` and `processReplica(ToolOptions)`, both of which trigger a real replication attempt — neither is safe to call from this test, so the test only inspects class metadata, never invokes behavior.
  Tests: The test calls `Class.forName("org.replicadb.ReplicaDB")` and asserts it does not throw, then asserts `clazz.getPackage().getName().equals("org.replicadb")` and `java.lang.reflect.Modifier.isPublic(clazz.getModifiers())`, proving the core artifact is a real, resolvable, usable classpath entry (not just a compile-time-only dependency) without executing any replication logic.
  Dependencies: Task 1.1

### 2. CLI Compatibility Guard

- [x] **2.1 Add an automated test proving the CLI classpath stays Spring-Boot-free**
  Files: `src/test/java/org/replicadb/NoSpringBootOnClasspathTest.java` (new, root project)
  Changes: No production code change. This test is the deliverable and directly encodes Decision 1's compatibility contract ("The `replicadb` artifact's classpath gaining a Spring Boot application context" is a defined way to break compatibility).
  Tests: The test (a) asserts `Class.forName("org.springframework.boot.SpringApplication")` throws `ClassNotFoundException` on the root project's own test classpath, and (b) reads `pom.xml` from disk and asserts its text contains no `<groupId>org.springframework` dependency declaration, so a future accidental dependency addition fails this test immediately instead of silently bloating the CLI.
  Dependencies: None

- [x] **2.2 Verify the existing CLI test suite and release packaging are unaffected**
  Files: none (verification task, no new files)
  Changes: No production or test code change. This task confirms AC3 (existing CLI arguments, options-file keys, exit codes, and no-metadata-database execution stay unaffected) with concrete commands rather than by inference from "root `pom.xml` is unmodified".
  Tests: Run the seven existing CLI option/config test classes explicitly (`AzureAuthenticationOptionsTest`, `ReplicationTableTest`, `ToolOptionsAutoCreateTest`, `ToolOptionsAzureAuthenticationTest`, `ToolOptionsExecutionContextTest`, `ToolOptionsIncrementalWatermarkTest`, and `ToolOptionsMultipleTablesTest`) and confirm all 40 tests pass unchanged. Run `mvn -B package -P release --file pom.xml` and confirm `target/ReplicaDB-0.18.4.jar` is produced; unzip its manifest with `unzip -p target/ReplicaDB-0.18.4.jar META-INF/MANIFEST.MF | grep Main-Class` and confirm it reads `Main-Class: org.replicadb.ReplicaDB`. The broad slash-based Surefire glob from the initial plan was narrowed during execution because it selected unrelated integration classes.
  Dependencies: None

### 3. CI and Release Wiring

- [x] **3.1 Build and test `replicadb-server` in `CT_Push.yml`**
  Files: `.github/workflows/CT_Push.yml`
  Changes: Add a new job `server` (parallel to the existing `integration`/`non_integration` jobs) that: checks out, sets up JDK 17, runs `mvn -B install -DskipTests --file pom.xml` (installs the CLI artifact locally so `replicadb-server` can resolve it), then `mvn -B test --file replicadb-server/pom.xml` — this single command exercises `ReplicaDbServerApplicationTest` (1.2), `HealthEndpointTest` (1.3), and `CoreDependencyResolutionTest` (1.4).
  Tests: Push to a branch / open a PR and confirm the new `server` job appears and passes in GitHub Actions. Locally, reproduce with `mvn -B install -DskipTests --file pom.xml && mvn -B test --file replicadb-server/pom.xml` and confirm exit code 0.
  Dependencies: Task 1.2, Task 1.3, Task 1.4, Task 2.1

- [x] **3.2 Package `replicadb-server` as a CI build artifact in the release workflow**
  Files: `.github/workflows/CI_Release.yml`
  Changes: After the existing `mvn clean install ... -P release` step (which builds the CLI tar.gz/zip), add a step `mvn -B package --file replicadb-server/pom.xml` to produce `replicadb-server-0.1.0-SNAPSHOT.jar`, then upload it with `actions/upload-artifact@v4` (e.g. `name: replicadb-server-jar`, `path: replicadb-server/target/replicadb-server-*.jar`). Do **not** list it in the `marvinpinto/action-automatic-releases` `files:` block — `replicadb-server` is versioned independently as an unreleased `-SNAPSHOT` skeleton and must not appear as a public asset on a numbered `vX.Y.Z` CLI release. Do **not** add a Docker/Podman build step for `replicadb-server` in this task (out of scope, see Architecture & Design).
  Tests: Locally reproduce the added step: `mvn -B package --file replicadb-server/pom.xml` after a root `mvn clean install -P release` and confirm `replicadb-server/target/replicadb-server-0.1.0-SNAPSHOT.jar` exists and `java -jar` on it (with `-Dspring.profiles.active=api`) starts and answers `/actuator/health` with 200, then stop the process.
  Dependencies: Task 3.1

- [x] **3.3 Ignore the new module's build output in `.gitignore`**
  Files: `.gitignore`
  Changes: The existing `target/*` line is anchored to the repository root and does not match `replicadb-server/target/*`. Add an explicit `replicadb-server/target/*` line.
  Tests: After building `replicadb-server` locally (`mvn -f replicadb-server/pom.xml package`), run `git status --short` and confirm no `replicadb-server/target/...` files appear as untracked.
  Dependencies: Task 1.1

- [x] **3.4 Add a version-drift guard between the root artifact and its pinned dependency**
  Files: `replicadb-server/src/test/java/org/replicadb/server/CoreVersionAlignmentTest.java` (new)
  Changes: No production code change. Guards against the root `pom.xml`'s `<version>` changing (e.g. a manual bump outside `release.sh`, or a Dependabot-unrelated edit) without the corresponding update to `replicadb-server/pom.xml`'s `org.replicadb:ReplicaDB` dependency version, which task 4.2 keeps in sync only through `release.sh`.
  Tests: The test reads the root `pom.xml` from a relative path (`../pom.xml` from the module, resolved via a system property or a fixed relative path since both projects are checked out together in CI and locally) and its own `pom.xml`, extracts both `<version>` values with a simple regex, and asserts they are equal — failing loudly in CI (task 3.1's job) if the root version is ever bumped without updating this module's dependency.
  Dependencies: Task 1.1

### 4. Documentation

- [x] **4.1 Document the delivered split in `ARCHITECTURE_DECISIONS.md`**
  Files: `ARCHITECTURE_DECISIONS.md`
  Changes: Under "### Phase 1: Spring Boot API and Scheduler", add a `#### Phase 1a: Artifact split — IMPLEMENTED` subsection (mirroring the existing Phase 0-a/0-b1/0-b2 subsection style), listing what was delivered (sibling `replicadb-server` module, `api`-profile skeleton, restricted `/actuator/health`, CI/release wiring) and explicitly restating what remains pending (state layer, REST API, scheduler, security, frontend). Update the top status line from "Phase 0-c (state layer) pending" to reflect that the artifact split is done and the state layer is the next Phase 1 sub-plan.
  Tests: Verification, not automated — cross-check every claim in the new subsection against the actual files changed in tasks 1.1–3.3 (file paths, profile name, endpoint path) before committing.
  Dependencies: Task 3.2, Task 3.3, Task 3.4

- [x] **4.2 Teach `release.sh` to bump `replicadb-server`'s dependency version too**
  Files: `release.sh`
  Changes: `update_pom_version()` currently only bumps the root `pom.xml`'s own `<version>`. Add a new function `update_server_pom_dependency_version()` that runs a targeted `sed` replacing the `<version>${old_version}</version>` that immediately follows the `<artifactId>ReplicaDB</artifactId>` line inside `replicadb-server/pom.xml`'s `<dependency>` block (not `replicadb-server`'s own `<version>`, which stays independent per the Architecture section). Call it from `main()` right after `update_pom_version`, and add `replicadb-server/pom.xml` to the `git add` list in `create_release_commit()`.
  Tests: Copy `replicadb-server/pom.xml` to a scratch file, run the new function against it with a sample `old_version`/`new_version` pair, and assert with `grep` that only the dependency's version line changed and `replicadb-server`'s own `<version>0.1.0-SNAPSHOT</version>` line is untouched.
  Dependencies: Task 1.1

- [x] **4.3 Document local build/run instructions for the new module**
  Files: `README.md`, `CONTRIBUTING.md`
  Changes: Add a short section explaining the two-artifact structure: build the CLI as today, then `mvn install` it, then `mvn -f replicadb-server/pom.xml spring-boot:run -Dspring-boot.run.profiles=api` to start the server skeleton locally, plus a one-line warning that `replicadb-server` is an unreleased, unauthenticated skeleton not meant for any real deployment yet. Include a troubleshooting note: if `mvn -f replicadb-server/pom.xml compile` (or `test`/`spring-boot:run`) fails with a "could not resolve dependency `org.replicadb:ReplicaDB`" error, the fix is always to run `mvn install -DskipTests` from the repository root first, since `replicadb-server` resolves it from the local Maven repository, not from a reactor build.
  Tests: Manually follow the documented steps from a clean clone and confirm the server starts and `/actuator/health` responds. Separately, from a clean clone, run `mvn -f replicadb-server/pom.xml compile` **without** installing the root project first and confirm the resulting error matches what the new troubleshooting note describes.
  Dependencies: Task 1.2

## Technical Reference

<details>
<summary>Types & Data Structures</summary>

No new domain types in this plan. Only:
- `org.replicadb.server.ReplicaDbServerApplication` — Spring Boot entry point, no fields/methods beyond `main`.

</details>

## Execution Retrospective (auto-generated by /itx-code)

### Plan Accuracy

- Tasks completed as planned: 10/13 (76.9%)
- Tasks that required plan adjustment: 3/13 (23.1%)
- Test loop iterations: 23 total (13 first-pass, 7 second-pass, 3 third-pass, including environment and command-selection retries)

### Gaps Encountered

#### Gap 1: Inherited MongoDB auto-configuration (Plan-to-Implementation)

- **Task**: 1.2 — Add the Spring Boot application entry point and `api` profile config
- **Plan assumed**: A bare server skeleton depending on the existing CLI artifact would start without requiring an external database because no state layer had been added.
- **Reality**: The CLI dependency brought enough MongoDB integration onto the server classpath for Spring Boot to create a Mongo client and attempt `localhost:27017` during context startup.
- **Resolution**: Excluded `MongoAutoConfiguration` in `application.yml` and reran the context and HTTP tests, confirming startup no longer attempts a database connection.
- **Learning**: When a sibling runtime depends on a broad CLI artifact, inspect transitive auto-configuration before adding the first server context test.

#### Gap 2: Surefire wildcard selected unrelated integration tests (Plan-to-Implementation)

- **Task**: 2.2 — Verify the existing CLI test suite and release packaging are unaffected
- **Plan assumed**: `-Dtest="org/replicadb/cli/*Test"` would select only the CLI tests in this Maven/Surefire configuration.
- **Reality**: The slash-based pattern selected unrelated integration classes in the local environment, starting MongoDB and Oracle containers and producing infrastructure failures unrelated to the CLI regression.
- **Resolution**: Replaced the validation command with the seven explicit CLI test class names and recorded the narrower command in the plan. All 40 CLI tests passed.
- **Learning**: Use explicit class-name lists for focused Surefire validation when repository test selection patterns are not proven locally.

### Patterns Discovered

- **Sibling Maven build dependency**: A standalone server module that depends on the CLI artifact requires a root `mvn install` step before server compilation; CI and contributor documentation must show that order.
- **Artifact version guard**: An independent sibling POM benefits from a test that compares its pinned core dependency version with the root project version.
- **Runtime auto-configuration boundary**: The server skeleton must explicitly disable inherited database auto-configuration until the managed metadata state layer is introduced.

<details>
<summary>Dependencies</summary>

`replicadb-server/pom.xml` (new artifact, independent version line):
- `<parent>org.springframework.boot:spring-boot-starter-parent</parent>` pinned to `3.3.5`
- `spring-boot-starter-web`
- `spring-boot-starter-actuator`
- `spring-boot-starter-test` (scope `test`, explicitly declared in task 1.1)
- `org.replicadb:ReplicaDB` (matches root `pom.xml`'s current version, currently `0.18.4`)
- `spring-boot-maven-plugin` (repackage goal)
- Property override `<junit-jupiter.version>6.0.3</junit-jupiter.version>` to align with the root project's JUnit Jupiter 6 baseline (fallback to Spring Boot's managed JUnit 5.x, documented as an isolated exception, if incompatible — see task 1.1)

Root `pom.xml`: **unchanged** — this is the point of the plan.

</details>

<details>
<summary>Testing Strategy</summary>

- `replicadb-server` gets its own `src/test/java` tree, run via `mvn test --file replicadb-server/pom.xml`, independent of the root project's Surefire/Testcontainers matrix.
- No Testcontainers needed for this plan (no database involved yet).
- The CLI compatibility guard (task 2.1) and regression check (task 2.2) run as part of the root project's existing test tooling in `CT_Push.yml` (task 2.1 matches the `org/replicadb/*Test` glob already used there; task 2.2 is a manual/local verification, not new CI surface) — only the new `server` job (task 3.1) is new CI surface.
- Local verification order for the whole plan: `mvn install -DskipTests` (root) → `mvn test --file replicadb-server/pom.xml` (covers tasks 1.2, 1.3, 1.4, 3.4) → `mvn test --file pom.xml -Dtest=org.replicadb.NoSpringBootOnClasspathTest` → task 2.2's manual commands.

</details>
