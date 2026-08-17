# Implementation Plan: Phase 1c-3a+b — Security (Authentication, Global Roles, Per-Job ACLs)

## Task Source

No JIRA ticket. Source is [ARCHITECTURE_DECISIONS.md](ARCHITECTURE_DECISIONS.md), Decision 4 ("Identity and permissions"), the "Frontend, users, and permissions" section, and the Phase 1c-3 section, which is currently marked **PENDING** ("there is no Spring Security, session, user, role, or ACL code anywhere in `replicadb-server`"). Agreed scope with the user (via clarifying questions): this plan covers **authentication + global roles + per-job ACLs** ("1c-3a+b"). Explicitly **out of scope** and deferred to a follow-up plan ("1c-3c"): audit events, retention purge for audit, and CSRF/rate-limit hardening beyond the baseline already mandated by Decision 4. The frontend (Phase 1c-4) remains a separate, later plan.

Acceptance criteria (derived from Decision 4 and the "Frontend, users, and permissions" section):

- Local users are stored in PostgreSQL; passwords are stored only as Argon2id hashes.
- Global roles `ADMIN`, `OPERATOR`, and `VIEWER` exist. `ADMIN` bypasses per-job ACLs.
- Per-job ACLs grant `VIEW`, `EDIT`, `EXECUTE`, and `CANCEL` permissions; `OPERATOR` and `VIEWER` can access only jobs for which their user holds the corresponding permission.
- Every job/run/schedule endpoint added in Phase 1c-1/1c-2 enforces these rules in the backend — hiding a button in a future frontend is not a security control.
- Session cookies are `HttpOnly`, `Secure`, `SameSite=Lax`; every state-changing request requires a CSRF token.
- Login is rate limited to 5 failed attempts per 15 minutes, counted per account and per source address.
- The first administrator is bootstrapped without any default/hardcoded password.
- Existing Phase 1a/1b/1c-1/1c-2 functionality and tests remain intact under the new security layer.

## Overview

`replicadb-server` currently exposes every `/api/v1` endpoint with no authentication or authorization (an intentional, documented gap from Phase 1c-1). This plan adds local-user authentication with `spring-boot-starter-security` and PostgreSQL-backed sessions (`spring-session-jdbc`), a fixed set of global roles, and a per-job permission model, then wires enforcement into the existing `JobDefinitionController`, `JobRunController`, and `JobScheduleController`. It also adds the endpoints needed to manage users and per-job grants, and a safe first-administrator bootstrap driven by environment variables.

## Architecture & Design

### Global roles: enum column, not `app_role`/`app_user_role` join tables

The "Frontend, users, and permissions" section of the architecture doc lists `app_user`, `app_role`, `app_user_role`, `job_permission`, `user_session`, and `audit_event` as the eventual persistence model. This plan deviates from the literal `app_role`/`app_user_role` join-table split, the same kind of documented, deliberate deviation already precedented by Phase 1c-1's `modeWarning` decision: there are exactly 3 fixed global roles with no stated requirement for custom or multiple roles per user, so a `GlobalRole` enum column on `app_user` (mirroring the existing `ReplicationMode`/`JobRunStatus` enum-column convention already used throughout this codebase) avoids a needless join for a value that is not going to vary in shape. `user_session` is also not a bespoke table — `spring-session-jdbc` provides its own `SPRING_SESSION`/`SPRING_SESSION_ATTRIBUTES` schema, added as a Flyway migration for the same forward-only-migration reason every other table in this project uses Flyway. `audit_event` is out of scope for this plan (see Task Source).

### Session and CSRF

`spring-session-jdbc` backs `HttpSession` with PostgreSQL so sessions survive a process restart, consistent with Decision 2's "durable state" posture. `CookieCsrfTokenRepository.withHttpOnlyFalse()` is used so a future frontend (Phase 1c-4) can read the token and echo it in a header; `/api/v1/auth/login` is the one endpoint exempted from CSRF (an attacker cannot forge a login without already knowing the credentials, so CSRF protection adds nothing there — this is Spring Security's own documented recommendation).

> ⚠️ Known limitation, accepted for this plan: CSRF is verified indirectly throughout this plan — every mutating test (Tasks 5.2, 7.1, 9.1-9.3, 10.1, 12.1, 13.1) must add `.with(csrf())` to pass at all, which proves the protection is active, but no test performs the literal browser sequence of reading the `XSRF-TOKEN` cookie and echoing it as a request header. That sequence is inherently a frontend concern and is untestable without one; it becomes verifiable once Phase 1c-4 ships.

### Authorization model

`GlobalRole.ADMIN` bypasses all per-job checks. For `OPERATOR`/`VIEWER`, a new `job_permission(job_definition_id, user_id, permission)` table (one row per granted permission, composite primary key) is checked by a new `JobAccessService`. Creating a job definition itself has no ACL row to check yet; this plan requires `ADMIN` or `OPERATOR` for creation (`VIEWER` is strictly read-only everywhere) and auto-grants the creator all four permissions on their own new job, mirroring the standard "creator owns what they create" ACL pattern. `AccessDeniedException` (thrown by `@PreAuthorize` or by `JobAccessService`) is handled by the existing `GlobalExceptionHandler` (already a `@RestControllerAdvice`) as 403; a custom `AuthenticationEntryPoint` is still needed for requests that never reach a controller at all (rejected earlier in the Spring Security filter chain), which is the one case `@ExceptionHandler`s cannot cover.

### List filtering requires repository signature changes

Phase 1c-1's `JobDefinitionRepository.findPage`/`count` and `JobRunRepository.findPage`/`count` return all rows visible to the caller unconditionally. Restricting `GET /api/v1/jobs` and `GET /api/v1/runs` to only the jobs a non-admin user can `VIEW` must happen in the SQL `WHERE` clause to keep pagination totals correct — filtering the already-paged Java `List` afterward would silently under-fill a page. Both repositories gain an additional `Set<UUID> restrictToJobIds` parameter (`null` meaning "no restriction", used for `ADMIN`); every existing call site (repository tests, controllers) is updated in the same task that changes the signature.

### Test infrastructure

Spring Security secures every `/api/v1/**` endpoint by default once added, which breaks every existing anonymous `MockMvc` call in `JobDefinitionControllerTest`, `JobRunControllerTest`, `JobScheduleControllerTest`, `JobLifecycleIT`, and `ScheduledJobLifecycleIT`. A dedicated task retrofits these with authentication. A custom `@WithMockReplicaDbUser` test annotation (backed by a `WithSecurityContextFactory`) builds the same `ReplicaDbUserDetails` principal type `JobAccessService` expects, so `OPERATOR`/`VIEWER`-path ACL tests do not need a real login round-trip through the database for every test method; the one full, real login-then-act flow is exercised by a dedicated end-to-end IT.

---

## Implementation Tasks

### 1. User domain and persistence

- [x] **1.1 Add `GlobalRole` enum, `AppUser` domain record, and `app_user` migration**
  Files: `replicadb-server/src/main/resources/db/migration/V7__create_app_user.sql` (new), `replicadb-server/src/main/java/org/replicadb/server/security/domain/GlobalRole.java` (new), `replicadb-server/src/main/java/org/replicadb/server/security/domain/AppUser.java` (new), `replicadb-server/src/test/java/org/replicadb/server/security/domain/AppUserTest.java` (new), [replicadb-server/src/test/java/org/replicadb/server/job/persistence/FlywayMigrationTest.java](replicadb-server/src/test/java/org/replicadb/server/job/persistence/FlywayMigrationTest.java)
  Changes: `GlobalRole` enum `{ADMIN, OPERATOR, VIEWER}`. Migration:
  ```sql
  CREATE TABLE app_user (
      id UUID PRIMARY KEY,
      username VARCHAR(100) NOT NULL UNIQUE,
      password_hash VARCHAR(255) NOT NULL,
      role VARCHAR(20) NOT NULL CHECK (role IN ('ADMIN', 'OPERATOR', 'VIEWER')),
      enabled BOOLEAN NOT NULL DEFAULT true,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
  );
  ```
  `AppUser(UUID id, String username, String passwordHash, GlobalRole role, boolean enabled, Instant createdAt, Instant updatedAt)` record with a compact constructor validating: `username` non-blank and matching `^[A-Za-z0-9._-]{3,100}$`; `passwordHash` non-blank; `role` non-null. Never stores or logs a raw password — only the already-hashed value. Bump `FlywayMigrationTest`'s applied-migration-count assertions from 6 to 7.
  Tests: valid construction succeeds; blank/invalid-character username throws `IllegalArgumentException`; blank `passwordHash` throws; `null` `role` throws; `FlywayMigrationTest` passes with count 7.
  Dependencies: None.

- [x] **1.2 Add `AppUserRepository`**
  Files: `replicadb-server/src/main/java/org/replicadb/server/security/persistence/AppUserRepository.java` (new), `replicadb-server/src/test/java/org/replicadb/server/security/persistence/AppUserRepositoryIT.java` (new)
  Changes: `NamedParameterJdbcTemplate`-based repository (matching `JobDefinitionRepository`'s style): `insert(AppUser): AppUser` (generates `id`/timestamps when null), `findById(UUID): Optional<AppUser>`, `findByUsername(String): Optional<AppUser>`, `findPage(int page, int size): List<AppUser>`, `count(): long`, `update(AppUser): AppUser` (replaces `role`, `enabled`, `passwordHash`, bumps `updatedAt`; `username`/`id`/`createdAt` are immutable after creation), `countByRole(GlobalRole): long` (used by the bootstrap runner in Task 6.1).
  Tests: insert then `findById`/`findByUsername` return the same user; `findByUsername` is case-sensitive and returns empty for an unknown username; duplicate `username` insert throws (Postgres unique violation surfaces as `org.springframework.dao.DuplicateKeyException`); `update` changes `role`/`enabled`/`passwordHash` and bumps `updatedAt` while leaving `username`/`createdAt` unchanged; `countByRole` reflects only matching rows; `findPage` paginates in a stable order.
  Dependencies: Task 1.1.

### 2. Job permission domain and persistence

- [x] **2.1 Add `JobPermissionType` enum, `JobPermission` domain record, and `job_permission` migration**
  Files: `replicadb-server/src/main/resources/db/migration/V8__create_job_permission.sql` (new), `replicadb-server/src/main/java/org/replicadb/server/security/domain/JobPermissionType.java` (new), `replicadb-server/src/main/java/org/replicadb/server/security/domain/JobPermission.java` (new), `replicadb-server/src/test/java/org/replicadb/server/security/domain/JobPermissionTest.java` (new), [replicadb-server/src/test/java/org/replicadb/server/job/persistence/FlywayMigrationTest.java](replicadb-server/src/test/java/org/replicadb/server/job/persistence/FlywayMigrationTest.java)
  Changes: `JobPermissionType` enum `{VIEW, EDIT, EXECUTE, CANCEL}`. Migration:
  ```sql
  CREATE TABLE job_permission (
      job_definition_id UUID NOT NULL REFERENCES job_definition(id) ON DELETE CASCADE,
      user_id UUID NOT NULL REFERENCES app_user(id) ON DELETE CASCADE,
      permission VARCHAR(20) NOT NULL CHECK (permission IN ('VIEW', 'EDIT', 'EXECUTE', 'CANCEL')),
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      PRIMARY KEY (job_definition_id, user_id, permission)
  );
  CREATE INDEX idx_job_permission_user ON job_permission (user_id, permission);
  ```
  `JobPermission(UUID jobDefinitionId, UUID userId, JobPermissionType permission, Instant createdAt)` record validating all three identifiers/enum non-null. `ON DELETE CASCADE` on both foreign keys is deliberate: a permission grant has no meaning once either the job or the user is gone. Bump `FlywayMigrationTest`'s count from 7 to 8.
  Tests: valid construction succeeds; `null` `jobDefinitionId`/`userId`/`permission` each throw; `FlywayMigrationTest` passes with count 8.
  Dependencies: Task 1.1 (for the `app_user` FK target to exist first).

- [x] **2.2 Add `JobPermissionRepository`**
  Files: `replicadb-server/src/main/java/org/replicadb/server/security/persistence/JobPermissionRepository.java` (new), `replicadb-server/src/test/java/org/replicadb/server/security/persistence/JobPermissionRepositoryIT.java` (new)
  Changes: `grant(UUID jobDefinitionId, UUID userId, JobPermissionType permission)` (`INSERT ... ON CONFLICT DO NOTHING`, idempotent), `grantAll(UUID jobDefinitionId, UUID userId)` (grants all 4 permission values), `revoke(UUID jobDefinitionId, UUID userId, JobPermissionType permission)`, `revokeAll(UUID jobDefinitionId, UUID userId)`, `hasPermission(UUID jobDefinitionId, UUID userId, JobPermissionType permission): boolean`, `findJobIdsWithPermission(UUID userId, JobPermissionType permission): Set<UUID>`, `findByJobDefinitionId(UUID jobDefinitionId): List<JobPermission>` (for the permission-listing endpoint in Task 10.1).
  Tests: `grant` then `hasPermission` returns `true`; granting twice does not throw or duplicate; `grantAll` results in exactly 4 rows and `hasPermission` true for all 4 types; `revoke` removes only the targeted permission, leaving others intact; `revokeAll` removes every row for that job/user pair; `hasPermission` is `false` for an ungranted combination; `findJobIdsWithPermission` returns only jobs where that user holds that specific permission and returns an empty `Set` (not `null`) for a user with zero grants anywhere; deleting the referenced `job_definition` row cascades and removes its `job_permission` rows (verifies the FK `ON DELETE CASCADE`).
  Dependencies: Task 2.1, Task 1.2.

### 3. Session store (Spring Session JDBC)

- [x] **3.1 Add `spring-session-jdbc`, its schema migration, and secure cookie configuration**
  Files: [replicadb-server/pom.xml](replicadb-server/pom.xml), `replicadb-server/src/main/resources/db/migration/V9__create_spring_session.sql` (new), [replicadb-server/src/main/resources/application-api.yml](replicadb-server/src/main/resources/application-api.yml), [replicadb-server/src/test/java/org/replicadb/server/job/persistence/FlywayMigrationTest.java](replicadb-server/src/test/java/org/replicadb/server/job/persistence/FlywayMigrationTest.java)
  Changes: Add `spring-session-jdbc` dependency (excluding `spring-boot-starter-logging` like the other starters in this pom). `V9__create_spring_session.sql`: copy the exact contents of `schema-postgresql.sql` bundled inside the `spring-session-jdbc` jar (found under `org/springframework/session/jdbc/`) verbatim — do not hand-type the `SPRING_SESSION`/`SPRING_SESSION_ATTRIBUTES` DDL, to avoid a schema mismatch with the store implementation. Add to `application-api.yml`:
  ```yaml
  spring:
    session:
      store-type: jdbc
      jdbc:
        initialize-schema: never
      timeout: 30m
  server:
    servlet:
      session:
        cookie:
          http-only: true
          secure: true
          same-site: lax
  ```
  `initialize-schema: never` is deliberate — Flyway, not Spring Session's own auto-init, owns this schema, consistent with every other table in this project. Bump `FlywayMigrationTest`'s count from 8 to 9.
  Tests: none dedicated in this task — covered by Task 5.2's `AuthController` test, which exercises a real login creating a session row, and by `FlywayMigrationTest`'s updated count.
  Dependencies: Task 1.1, Task 2.1 (migration ordering only).

### 4. Security configuration and password hashing

- [x] **4.1 Add `spring-boot-starter-security` and `SecurityConfig`**
  Files: [replicadb-server/pom.xml](replicadb-server/pom.xml), `replicadb-server/src/main/java/org/replicadb/server/security/config/SecurityConfig.java` (new), `replicadb-server/src/main/java/org/replicadb/server/security/config/ProblemDetailAuthenticationEntryPoint.java` (new), `replicadb-server/src/test/java/org/replicadb/server/security/config/SecurityConfigTest.java` (new)
  Changes: Add `spring-boot-starter-security` dependency (same logging exclusion pattern). `SecurityConfig`: `@Configuration @EnableWebSecurity @EnableMethodSecurity`, a `SecurityFilterChain` bean configuring CSRF via `CookieCsrfTokenRepository.withHttpOnlyFalse()` with `/api/v1/auth/login` exempted (`ignoringRequestMatchers`), `authorizeHttpRequests` permitting `/api/v1/auth/login` and `/actuator/health` and requiring authentication for every other request, `sessionManagement().sessionCreationPolicy(SessionCreationPolicy.IF_REQUIRED)`, and `exceptionHandling().authenticationEntryPoint(...)` wired to `ProblemDetailAuthenticationEntryPoint`. A `PasswordEncoder` bean using `Argon2PasswordEncoder.defaultsForSpringSecurity()`, matching Decision 4's Argon2id requirement. `ProblemDetailAuthenticationEntryPoint implements AuthenticationEntryPoint` writes a 401 RFC 7807 `application/problem+json` body directly (this path never reaches `GlobalExceptionHandler`, since Spring Security's `ExceptionTranslationFilter` intercepts it before the request reaches `DispatcherServlet`).
  Tests: `@SpringBootTest` + `MockMvc` against Testcontainers Postgres (reusing the existing `PostgresTestcontainersConfig` pattern) — an unauthenticated `GET /api/v1/jobs` returns `401` with `application/problem+json` and the entry point's fixed detail (e.g. `"Authentication required"` — deliberately distinct from `AuthController`'s `"Invalid credentials"` in Task 5.2, since this path means "no session at all" rather than "a login attempt failed"); `GET /actuator/health` remains reachable without authentication. No CSRF-protected-endpoint test belongs in this task: no real endpoint exists to exercise CSRF against yet (`JobDefinitionController` still has no `JobAccessService` wiring, and `AuthController` does not exist until Task 5.2). CSRF rejection/acceptance is exercised implicitly by every mutating test added from Task 5.2 onward, since every one of them must add `.with(csrf())` to pass at all — a request without it failing with `403` is what makes that requirement observable, so no separate synthetic-endpoint test is needed here.
  Dependencies: Task 3.1.

- [x] **4.2 Add `ReplicaDbUserDetails`/`ReplicaDbUserDetailsService`**
  Files: `replicadb-server/src/main/java/org/replicadb/server/security/auth/ReplicaDbUserDetails.java` (new), `replicadb-server/src/main/java/org/replicadb/server/security/auth/ReplicaDbUserDetailsService.java` (new), `replicadb-server/src/test/java/org/replicadb/server/security/auth/ReplicaDbUserDetailsServiceTest.java` (new)
  Changes: `ReplicaDbUserDetails(AppUser appUser) implements UserDetails` — `getAuthorities()` returns a single `SimpleGrantedAuthority("ROLE_" + appUser.role().name())`, `getUsername()` delegates to `appUser.username()`, `getPassword()` delegates to `appUser.passwordHash()`, `isEnabled()`/`isAccountNonLocked()`/etc. delegate to `appUser.enabled()`. `ReplicaDbUserDetailsService implements UserDetailsService`: `loadUserByUsername(String)` calls `AppUserRepository.findByUsername(...)`, wraps the result in `ReplicaDbUserDetails`, and throws `UsernameNotFoundException` when absent — this class is registered as the `UserDetailsService` bean Spring Security's `DaoAuthenticationProvider` uses automatically once `SecurityConfig`'s `PasswordEncoder` bean is also present.
  Tests: existing enabled user loads with the correct single `ROLE_<role>` authority; disabled user loads but `isEnabled()` is `false` (so `DaoAuthenticationProvider` rejects it with `DisabledException`); unknown username throws `UsernameNotFoundException`.
  Dependencies: Task 1.2, Task 4.1.

### 5. Authentication endpoints

- [x] **5.1 Add `LoginAttemptService`**
  Files: `replicadb-server/src/main/java/org/replicadb/server/security/auth/LoginAttemptService.java` (new), `replicadb-server/src/main/java/org/replicadb/server/security/auth/TooManyAttemptsException.java` (new), `replicadb-server/src/test/java/org/replicadb/server/security/auth/LoginAttemptServiceTest.java` (new)
  Changes: In-memory, thread-safe limiter keyed separately by `"user:" + username` and `"addr:" + remoteAddress` in a `ConcurrentHashMap<String, Deque<Instant>>`. `checkAllowed(String username, String remoteAddress)` purges entries older than the 15-minute window and throws `TooManyAttemptsException` (new unchecked exception) if either key already has 5 or more remaining entries. `recordFailure(...)` appends a timestamp to both keys. `recordSuccess(...)` clears both keys. This single-instance, in-memory design is consistent with Decision 2's "Monolithic Control Plane First" — a persisted/shared limiter is not needed until Phase 2 introduces multiple instances.
  Tests: 5 failures for `("alice", "10.0.0.1")` then a 6th `checkAllowed` throws; a successful login clears the counter so a subsequent failure sequence starts fresh; an old failure outside the 15-minute window does not count toward the limit (inject a fake clock or pre-populate the internal deque via a package-private test seam); **address-keyed limit independent of username**: `("alice", "10.0.0.1")` fails 5 times, then `("bob", "10.0.0.1")` is also blocked (same address); **username-keyed limit independent of address**: after that same setup, `("alice", "10.0.0.2")` (different address, same username) is still blocked because the username-keyed counter also has 5 failures, while `("carol", "10.0.0.2")` (neither key seen before) is allowed.
  Dependencies: None.

- [x] **5.2 Add `AuthController` (`login`, `logout`, `me`) and `GlobalExceptionHandler` additions**
  Files: `replicadb-server/src/main/java/org/replicadb/server/security/api/AuthController.java` (new), `replicadb-server/src/main/java/org/replicadb/server/security/api/LoginRequest.java` (new), `replicadb-server/src/main/java/org/replicadb/server/security/api/UserIdentityResponse.java` (new), [replicadb-server/src/main/java/org/replicadb/server/job/api/GlobalExceptionHandler.java](replicadb-server/src/main/java/org/replicadb/server/job/api/GlobalExceptionHandler.java), `replicadb-server/src/test/java/org/replicadb/server/security/api/AuthControllerTest.java` (new)
  Changes: `AuthController` under `/api/v1/auth`, constructor-injected with `AuthenticationManager`, `LoginAttemptService`, and `SecurityContextRepository` (`HttpSessionSecurityContextRepository`). `POST /login` — calls `loginAttemptService.checkAllowed(...)`, calls `authenticationManager.authenticate(new UsernamePasswordAuthenticationToken(username, password))` inside a try/catch that calls `recordFailure(...)` and rethrows on `AuthenticationException`, otherwise calls `recordSuccess(...)`, builds a `SecurityContext`, sets it on `SecurityContextHolder`, and explicitly persists it via `securityContextRepository.saveContext(context, request, response)` (required so the session cookie carrying the authenticated context is actually written) before returning `200` with `UserIdentityResponse(id, username, role)`. `POST /logout` — invalidates the current `HttpSession` and clears the security context, returns `204`. `GET /me` — returns the current `Authentication`'s identity as `UserIdentityResponse` (unreachable when unauthenticated, since only `/login` is in the `permitAll` list). `GlobalExceptionHandler` gains three handlers: `AuthenticationException` → `401` with the fixed detail `"Invalid credentials"` (never echoing which part was wrong, to avoid username enumeration), `TooManyAttemptsException` → `429`, `org.springframework.security.access.AccessDeniedException` → `403`. This `401` path (a failed login POST, handled by `@ControllerAdvice` since the exception originates inside a controller method) is intentionally a different code path and a different message than Task 4.1's `ProblemDetailAuthenticationEntryPoint` (a request to a protected endpoint with no session at all, handled by `ExceptionTranslationFilter` before `DispatcherServlet`) — both return `401`, but they answer different questions ("your login attempt failed" vs. "you were never logged in"), so no message needs to match between them.
  Tests: valid credentials return `200` with the correct identity and set a session cookie (assert `Set-Cookie` presence); wrong password returns `401` problem+json with the generic message and increments the attempt counter; 5 consecutive failures then a 6th attempt (even with the correct password) returns `429`; a disabled user's correct credentials return `401` (via `DisabledException`); `GET /me` after a successful login (same `MockHttpSession` reused across requests) returns the logged-in identity; `GET /me` without a prior login returns `401`; `POST /logout` after login returns `204` and a subsequent `GET /me` in the same session returns `401`.
  Dependencies: Task 4.1, Task 4.2, Task 5.1.

### 6. Admin bootstrap

- [x] **6.1 Add `AdminBootstrapRunner`**
  Files: `replicadb-server/src/main/java/org/replicadb/server/security/execution/AdminBootstrapRunner.java` (new), `replicadb-server/src/test/java/org/replicadb/server/security/execution/AdminBootstrapRunnerTest.java` (new)
  Changes: `@Component implements ApplicationRunner`, constructor-injected with `AppUserRepository` and `PasswordEncoder`. `run(ApplicationArguments args)`: if `appUserRepository.countByRole(GlobalRole.ADMIN) > 0`, return immediately (already bootstrapped). Otherwise read `REPLICADB_BOOTSTRAP_ADMIN_USERNAME`/`REPLICADB_BOOTSTRAP_ADMIN_PASSWORD` from the environment (via an injected, overridable `Function<String,String> envLookup` defaulting to `System::getenv`, so the test can supply fixed values without mutating real process environment variables); if either is null/blank, throw `IllegalStateException` with a message naming both variables, which aborts Spring Boot startup with that message on stdout/stderr (the same, already-standard way any Spring Boot startup failure is surfaced to an operator) — satisfying "no default password" by refusing to start rather than creating one. Otherwise hash the password with the injected `PasswordEncoder` and call `appUserRepository.insert(...)`, catching `org.springframework.dao.DuplicateKeyException` around that single insert: if it is thrown, re-check `countByRole(ADMIN) > 0` and treat a now-nonzero count as a benign race (another instance bootstrapped first) rather than a startup failure, rethrowing only if the count is still zero. This handles Decision 2's single-instance phase safely without over-building for Phase 2's multi-instance concerns. Log a `WARN` naming the created username and instructing the operator to rotate the password (never logging the password itself).
  Tests: zero existing `ADMIN` rows plus both env values present creates exactly one enabled `ADMIN` user whose stored `passwordHash` satisfies `passwordEncoder.matches(rawPassword, storedHash)`; an existing `ADMIN` row present means `run(...)` makes no repository writes even when env values are also present; zero `ADMIN` rows plus a missing or blank env value throws `IllegalStateException` and creates no user; a `DuplicateKeyException` thrown by `insert(...)` when `countByRole(ADMIN)` is now nonzero (simulated via Mockito) does not propagate, while the same exception with `countByRole(ADMIN)` still zero does propagate.
  Dependencies: Task 1.2, Task 4.1 (for the `PasswordEncoder` bean).

### 7. User and role administration API

- [x] **7.1 Add `UserController`**
  Files: `replicadb-server/src/main/java/org/replicadb/server/security/api/UserController.java` (new), `replicadb-server/src/main/java/org/replicadb/server/security/api/UserRequest.java` (new), `replicadb-server/src/main/java/org/replicadb/server/security/api/UserResponse.java` (new), `replicadb-server/src/test/java/org/replicadb/server/security/api/UserControllerTest.java` (new)
  Changes: `@RestController @RequestMapping("/api/v1/users") @PreAuthorize("hasRole('ADMIN')")` class-level (every method requires `ADMIN`). `UserRequest(@NotBlank String username, @NotBlank String password, @NotNull GlobalRole role)` for creation; a separate `UserRequest.RoleUpdate(@NotNull GlobalRole role, boolean enabled)` inner-record style DTO (or a second small DTO class) for `PUT /{id}`. `UserResponse(UUID id, String username, GlobalRole role, boolean enabled, Instant createdAt, Instant updatedAt)` — never includes `passwordHash`. `POST` — pre-checks `appUserRepository.findByUsername(...)` and throws `IllegalStateException` (mapped to `409` by the existing handler) if already taken, otherwise hashes the password and inserts. `GET` (paged, reusing `PageRequestParams`/`PageResponse` exactly like `JobDefinitionController`), `GET /{id}`, `PUT /{id}` (updates `role`/`enabled`), `PUT /{id}/password` (body `{newPassword}`, re-hashes and updates only `passwordHash`).
  Tests: `@SpringBootTest` + `MockMvc` against Testcontainers Postgres, authenticated as `ADMIN` via `@WithMockUser(roles = "ADMIN")` plus `.with(csrf())` on mutating calls — creating a user returns `201` with no password field in the body; duplicate username returns `409`; a non-`ADMIN` caller (`@WithMockUser(roles = "OPERATOR")`) receives `403` on every endpoint; `PUT /{id}` changes role/enabled; `PUT /{id}/password` changes the stored hash such that the old raw password no longer authenticates (verified by loading the user afterward and checking `passwordEncoder.matches(oldPassword, newHash)` is `false`); `GET` lists are paginated consistently with the existing `PageResponse` contract.
  Dependencies: Task 1.2, Task 4.1, Task 4.2.

### 8. Job access authorization

- [x] **8.1 Add `JobAccessService`**
  Files: `replicadb-server/src/main/java/org/replicadb/server/security/JobAccessService.java` (new), `replicadb-server/src/test/java/org/replicadb/server/security/JobAccessServiceTest.java` (new)
  Changes: `@Service` constructor-injected with `JobPermissionRepository`. `require(Authentication authentication, UUID jobDefinitionId, JobPermissionType permission)`: if the authentication's authorities contain `ROLE_ADMIN`, return immediately (role check only, no principal cast needed — this matters for `@WithMockUser`-based tests in Task 9, which do not carry a `ReplicaDbUserDetails` principal); otherwise extract the `AppUser` id from the principal (cast to `ReplicaDbUserDetails`, throwing `IllegalStateException` for an unsupported principal type) and throw `AccessDeniedException` unless `jobPermissionRepository.hasPermission(...)` is `true`. `visibleJobIds(Authentication authentication): Optional<Set<UUID>>` — empty `Optional` means "unrestricted" (`ADMIN`); otherwise returns `jobPermissionRepository.findJobIdsWithPermission(userId, JobPermissionType.VIEW)` (possibly an empty set, meaning "nothing visible"). `currentUserId(Authentication authentication): UUID` for the create-and-auto-grant flow in Task 9.1.
  Tests: `ADMIN` authority bypasses `require(...)` for every permission without ever touching `JobPermissionRepository` (Mockito `verifyNoInteractions`); an `OPERATOR`/`VIEWER` principal with the matching `JobPermission` row passes; without it, `AccessDeniedException` is thrown; `visibleJobIds` returns empty `Optional` for `ADMIN` and the repository's set for others; an authentication whose principal is not `ReplicaDbUserDetails` and is not `ROLE_ADMIN` throws `IllegalStateException` from `currentUserId`/`require`.
  Dependencies: Task 2.2, Task 4.2.

- [x] **8.2 Add `@WithMockReplicaDbUser` test support annotation**
  Files: `replicadb-server/src/test/java/org/replicadb/server/security/WithMockReplicaDbUser.java` (new), `replicadb-server/src/test/java/org/replicadb/server/security/WithMockReplicaDbUserSecurityContextFactory.java` (new)
  Changes: `@Retention(RUNTIME) @WithSecurityContext(factory = WithMockReplicaDbUserSecurityContextFactory.class) public @interface WithMockReplicaDbUser { String userId() default "..."; String username() default "test-user"; GlobalRole role() default GlobalRole.OPERATOR; }`. The factory builds a `ReplicaDbUserDetails` wrapping a synthetic, non-persisted `AppUser` (fixed `UUID` parsed from `userId()`) and an `Authentication` whose principal is that `ReplicaDbUserDetails` — this is what lets `JobAccessService`'s principal cast succeed in controller tests without a real login round-trip or a real `app_user` row, while still exercising the real `JobPermissionRepository` against Testcontainers Postgres for the permission rows themselves.
  Tests: a `@WithMockReplicaDbUser(role = VIEWER)`-annotated dummy test method resolves `SecurityContextHolder.getContext().getAuthentication().getPrincipal()` to a `ReplicaDbUserDetails` with the expected `userId`/`role`, proving the factory wiring before it is relied upon by Task 9's controller tests.
  Dependencies: Task 4.2, Task 8.1.

### 9. Wiring ACL enforcement into existing controllers

- [x] **9.1 Wire `JobAccessService` into `JobDefinitionController` and add ACL-aware repository paging**
  Files: [replicadb-server/src/main/java/org/replicadb/server/job/api/JobDefinitionController.java](replicadb-server/src/main/java/org/replicadb/server/job/api/JobDefinitionController.java), [replicadb-server/src/main/java/org/replicadb/server/job/persistence/JobDefinitionRepository.java](replicadb-server/src/main/java/org/replicadb/server/job/persistence/JobDefinitionRepository.java), [replicadb-server/src/test/java/org/replicadb/server/job/persistence/JobDefinitionRepositoryIT.java](replicadb-server/src/test/java/org/replicadb/server/job/persistence/JobDefinitionRepositoryIT.java), [replicadb-server/src/test/java/org/replicadb/server/job/api/JobDefinitionControllerTest.java](replicadb-server/src/test/java/org/replicadb/server/job/api/JobDefinitionControllerTest.java)
  Changes: `JobDefinitionRepository.findPage(int page, int size, Set<UUID> restrictToIds)` and `count(Set<UUID> restrictToIds)` — when `restrictToIds` is `null`, behave exactly as before (unrestricted); when non-null (including empty), add `AND id = ANY(:restrictToIds)` (an empty `Set` must still bind correctly and yield zero rows, not "no restriction" — use a `UUID[]` array parameter, never string-concatenate the ids). `JobDefinitionController`: constructor-injected with `JobAccessService` and `JobPermissionRepository`. `create` — `@PreAuthorize("hasAnyRole('ADMIN','OPERATOR')")` and `@Transactional` (the method must commit `repository.insert(...)` and `jobPermissionRepository.grantAll(...)` atomically — a creator whose grant silently failed after their job was created would get `403` on their own new job), calling `jobPermissionRepository.grantAll(persisted.id(), jobAccessService.currentUserId(authentication))` right after `repository.insert(...)`. `list` — calls `jobAccessService.visibleJobIds(authentication)` and passes the `Optional<Set<UUID>>` (unwrapped to `null`/the set) into the repository calls. `get`/`update` — call `jobAccessService.require(authentication, id, VIEW)`/`require(authentication, id, EDIT)` respectively before proceeding, taking `Authentication` as a controller method parameter (Spring resolves it automatically).
  Tests: `ADMIN`/`OPERATOR` (`@WithMockUser`) can create; `VIEWER` (`@WithMockUser(roles="VIEWER")`) gets `403` on create; after creation with `@WithMockReplicaDbUser`, that same user can `GET`/`PUT` their own job; a different `@WithMockReplicaDbUser` with no granted permission gets `403` on `GET`/`PUT` for that job; `list` for a non-admin user returns only jobs where they hold `VIEW`, with a correct total count across two pages (insert enough fixtures to force pagination); `ADMIN` sees every job in `list` regardless of grants. `JobDefinitionRepositoryIT` updates every existing `findPage`/`count` call site to pass `null` explicitly and adds new cases for a non-null restriction set (including an empty set returning zero rows).
  Dependencies: Task 2.2, Task 8.1, Task 8.2.

- [x] **9.2 Wire `JobAccessService` into `JobRunController` and add ACL-aware run paging**
  Files: [replicadb-server/src/main/java/org/replicadb/server/job/api/JobRunController.java](replicadb-server/src/main/java/org/replicadb/server/job/api/JobRunController.java), [replicadb-server/src/main/java/org/replicadb/server/job/persistence/JobRunRepository.java](replicadb-server/src/main/java/org/replicadb/server/job/persistence/JobRunRepository.java), [replicadb-server/src/test/java/org/replicadb/server/job/persistence/JobRunRepositoryIT.java](replicadb-server/src/test/java/org/replicadb/server/job/persistence/JobRunRepositoryIT.java), [replicadb-server/src/test/java/org/replicadb/server/job/api/JobRunControllerTest.java](replicadb-server/src/test/java/org/replicadb/server/job/api/JobRunControllerTest.java)
  Changes: `JobRunRepository.findPage(UUID jobDefinitionId, JobRunStatus status, int page, int size, Set<UUID> restrictToJobIds)` and matching `count(...)` overload — same `null`-means-unrestricted convention as Task 9.1, combining correctly with the existing single-job and status filters (a call already scoped to one `jobDefinitionId` via the `/jobs/{id}/runs` path does not need the restriction parameter at all; only the global `/runs` listing does). `JobRunController`: constructor-injected with `JobAccessService`. `listForJob` — `jobAccessService.require(authentication, jobDefinitionId, VIEW)` before delegating (no repository restriction parameter needed, since it is a single-job query already gated by the explicit check). `list` (global) — passes `jobAccessService.visibleJobIds(authentication)` into the new repository parameter. `get`/`log` — resolve the run, then `require(authentication, run.jobDefinitionId(), VIEW)`. `trigger`/`retry` — `require(authentication, jobDefinitionId or failedRun.jobDefinitionId(), EXECUTE)`. `cancel` — `require(authentication, run.jobDefinitionId(), CANCEL)`.
  Tests: a user with only `VIEW` on a job can `GET` its runs/log but gets `403` triggering, cancelling, or retrying; a user with `EXECUTE` (but not `CANCEL`) can trigger but gets `403` on cancel; global `/runs` listing for a non-admin returns only runs belonging to jobs where they hold `VIEW`, with correct pagination totals; `ADMIN` sees every run. `JobRunRepositoryIT` updates every existing `findPage`/`count` call site with an explicit `null` restriction and adds restricted-set cases.
  Dependencies: Task 2.2, Task 8.1, Task 8.2, Task 9.1 (shared `Authentication`-parameter pattern).

- [x] **9.3 Wire `JobAccessService` into `JobScheduleController`**
  Files: [replicadb-server/src/main/java/org/replicadb/server/job/api/JobScheduleController.java](replicadb-server/src/main/java/org/replicadb/server/job/api/JobScheduleController.java), [replicadb-server/src/test/java/org/replicadb/server/job/api/JobScheduleControllerTest.java](replicadb-server/src/test/java/org/replicadb/server/job/api/JobScheduleControllerTest.java)
  Changes: Constructor-injected with `JobAccessService`. `GET` requires `VIEW` on the path's `jobDefinitionId`; `PUT`/`DELETE` require `EDIT`.
  Tests: a `VIEW`-only user can read a schedule but gets `403` on `PUT`/`DELETE`; an `EDIT`-holding user can create/replace/remove it; `ADMIN` can do all three regardless of grants.
  Dependencies: Task 8.1, Task 8.2.

### 10. Job permission management API

- [x] **10.1 Add `JobPermissionController`**
  Files: `replicadb-server/src/main/java/org/replicadb/server/job/api/JobPermissionController.java` (new), `replicadb-server/src/main/java/org/replicadb/server/job/api/JobPermissionRequest.java` (new), `replicadb-server/src/main/java/org/replicadb/server/job/api/JobPermissionResponse.java` (new), `replicadb-server/src/test/java/org/replicadb/server/job/api/JobPermissionControllerTest.java` (new)
  Changes: `@RestController @RequestMapping("/api/v1/jobs/{jobDefinitionId}/permissions")`. Every method calls `jobAccessService.require(authentication, jobDefinitionId, EDIT)` first (an `EDIT`-holder, or `ADMIN`, manages who else can access the job). `GET` — `jobPermissionRepository.findByJobDefinitionId(...)` grouped by `userId` into `JobPermissionResponse(UUID userId, String username, Set<JobPermissionType> permissions)` (joining `AppUserRepository.findById(...)` per distinct user id for the username). `PUT /{userId}` — the `{userId}` path variable is the target `AppUser`'s `UUID` primary key, **not** a username (resolve it with `appUserRepository.findById(userId).orElseThrow(...)` first, mapped to `404` by the existing `NoSuchElementException` handler, before touching permissions); body `JobPermissionRequest(@NotEmpty Set<JobPermissionType> permissions)`, replaces the full permission set for that pair (`revokeAll` then grant each requested type, in one method so a partial failure cannot leave a mixed state — wrap in a single `@Transactional` method). `DELETE /{userId}` — `revokeAll`, `204`, idempotent (no existence check needed, matching `JobScheduleController`'s idempotent-delete precedent).
  Tests: an `EDIT`-holder can grant/list/revoke permissions for another user on the job; a `VIEW`-only (non-`EDIT`) user gets `403` on all three; granting `{"permissions": ["VIEW", "EXECUTE"]}` then re-`PUT`ing `{"permissions": ["VIEW"]}` leaves exactly `VIEW` (proves replace-not-merge semantics); `DELETE` for a user with no existing grants still returns `204`; `GET` reflects multiple users with different permission sets correctly grouped.
  Dependencies: Task 2.2, Task 8.1.

### 11. Error handling

- [x] **11.1 Extend `GlobalExceptionHandlerTest` for the new handlers**
  Files: [replicadb-server/src/test/java/org/replicadb/server/job/api/GlobalExceptionHandlerTest.java](replicadb-server/src/test/java/org/replicadb/server/job/api/GlobalExceptionHandlerTest.java)
  Changes: No new production code (the three handlers were already added directly to `GlobalExceptionHandler` in Task 5.2, since they are needed immediately by `AuthController`) — this task adds the missing direct unit-test coverage for those three handlers in isolation (bypassing the full Spring context), matching the existing style of this test class for the pre-existing handlers.
  Tests: `AccessDeniedException` → `403` `ProblemDetail`; `AuthenticationException` (use a concrete subtype such as `BadCredentialsException`) → `401` with the fixed `"Invalid credentials"` detail; `TooManyAttemptsException` → `429`.
  Dependencies: Task 5.2.

### 12. Retrofit existing tests for authentication

- [x] **12.1 Update existing controller/IT tests to authenticate**
  Files: [replicadb-server/src/test/java/org/replicadb/server/job/api/JobDefinitionControllerTest.java](replicadb-server/src/test/java/org/replicadb/server/job/api/JobDefinitionControllerTest.java), [replicadb-server/src/test/java/org/replicadb/server/job/api/JobRunControllerTest.java](replicadb-server/src/test/java/org/replicadb/server/job/api/JobRunControllerTest.java), [replicadb-server/src/test/java/org/replicadb/server/job/api/JobScheduleControllerTest.java](replicadb-server/src/test/java/org/replicadb/server/job/api/JobScheduleControllerTest.java), [replicadb-server/src/test/java/org/replicadb/server/job/api/JobLifecycleIT.java](replicadb-server/src/test/java/org/replicadb/server/job/api/JobLifecycleIT.java), [replicadb-server/src/test/java/org/replicadb/server/job/api/ScheduledJobLifecycleIT.java](replicadb-server/src/test/java/org/replicadb/server/job/api/ScheduledJobLifecycleIT.java)
  Changes: These pre-existing tests are not testing ACL behavior itself (that is covered by Tasks 9.1-9.3, 10.1) — they only need to keep passing now that every endpoint requires authentication. All five files already use the `@SpringBootTest` + `MockMvc` + Testcontainers Postgres pattern (confirmed against the current sources), so `SecurityConfig`'s filter chain is already active in their context without further setup — no test needs converting from a narrower slice test. Add class-level `@WithMockUser(roles = "ADMIN")` (bypasses all per-job ACL checks per Task 8.1's design, so no `JobPermission` fixture setup is needed in these pre-existing tests) and add `.with(SecurityMockMvcRequestPostProcessors.csrf())` to every mutating (`POST`/`PUT`/`DELETE`) `MockMvc` call. `ScheduledJobLifecycleIT`'s Quartz-fired path (`ScheduledRunTriggerJob`) does not go through HTTP/Spring Security at all and needs no change; only its `MockMvc`-driven `PUT`/`POST` setup calls need the annotation/CSRF additions.
  Tests: This task's own success criterion is that the full existing suite for these five files passes unmodified in behavior, only now with authentication satisfied — no new test scenarios are added here.
  Dependencies: Task 9.1, Task 9.2, Task 9.3.

### 13. Integration and regression

- [x] **13.1 End-to-end IT: real login, role, and ACL scenario**
  Files: `replicadb-server/src/test/java/org/replicadb/server/security/SecurityJobLifecycleIT.java` (new)
  Changes: `@SpringBootTest(webEnvironment = RANDOM_PORT)` against Testcontainers Postgres, mirroring `JobLifecycleIT`'s structure but driving real HTTP calls with a `TestRestTemplate`/`WebTestClient` that preserves cookies across requests (no `@WithMockUser` shortcuts here — this is the one test proving the real login flow end-to-end): bootstrap runs (Task 6.1) create the initial `ADMIN` from env vars set on the test's Spring context; `ADMIN` logs in via `POST /api/v1/auth/login`, creates a second `OPERATOR` user via `POST /api/v1/users`, creates a job definition (auto-granting itself all permissions per Task 9.1), grants the `OPERATOR` user `VIEW`+`EXECUTE` (not `CANCEL`) via `PUT /api/v1/jobs/{id}/permissions/{userId}`; the `OPERATOR` user then logs in with their own session, successfully triggers a run, and receives `403` attempting to cancel it; a third session with no login at all receives `401` on every endpoint.
  Tests: This task *is* the test — the file above is the deliverable and must pass.
  Dependencies: Task 6.1, Task 7.1, Task 9.1, Task 9.2, Task 10.1.

- [x] **13.2 Verify existing Phase 1a/1b/1c-1/1c-2 tests remain unaffected**
  Files: none (verification only)
  Changes: No functional change expected — this task runs the full existing `replicadb-server` test suite (`mvn -f replicadb-server/pom.xml test`) to confirm the new security dependencies, Flyway migrations, and beans do not break `HealthEndpointTest`, `ReplicaDbServerApplicationTest`, `CoreDependencyResolutionTest`, `CoreVersionAlignmentTest`, `FlywayMigrationTest`, or any Phase 1c-1/1c-2 test beyond the deliberate updates already made in Task 12.1.
  Tests: Full module test run must report zero failures.
  Dependencies: Task 13.1.

---

## Technical Reference

<details>
<summary>Types & Data Structures</summary>

- `GlobalRole` enum `{ADMIN, OPERATOR, VIEWER}`
- `AppUser(UUID id, String username, String passwordHash, GlobalRole role, boolean enabled, Instant createdAt, Instant updatedAt)` — validated record
- `JobPermissionType` enum `{VIEW, EDIT, EXECUTE, CANCEL}`
- `JobPermission(UUID jobDefinitionId, UUID userId, JobPermissionType permission, Instant createdAt)` — validated record
- `AppUserRepository.insert/findById/findByUsername/findPage/count/update/countByRole`
- `JobPermissionRepository.grant/grantAll/revoke/revokeAll/hasPermission/findJobIdsWithPermission/findByJobDefinitionId`
- `ReplicaDbUserDetails implements UserDetails` — wraps `AppUser`
- `ReplicaDbUserDetailsService implements UserDetailsService`
- `LoginAttemptService` / `TooManyAttemptsException`
- `JobAccessService.require(Authentication, UUID, JobPermissionType)` / `.visibleJobIds(Authentication): Optional<Set<UUID>>` / `.currentUserId(Authentication): UUID`
- `AuthController` (`/api/v1/auth/login|logout|me`), `LoginRequest`, `UserIdentityResponse`
- `UserController` (`/api/v1/users`), `UserRequest`, `UserResponse`
- `JobPermissionController` (`/api/v1/jobs/{id}/permissions`), `JobPermissionRequest`, `JobPermissionResponse`
- `AdminBootstrapRunner implements ApplicationRunner`
- `@WithMockReplicaDbUser` / `WithMockReplicaDbUserSecurityContextFactory` (test support)

</details>

## Execution Retrospective (auto-generated by /itx-code)

### Plan Accuracy
- Tasks completed as planned: 16/21 (76.2%)
- Tasks that required plan adjustment: 5/21 (23.8%)
- Test loop iterations: 38 total (14 first-pass, 15 second-pass, 9 third-or-later-pass)

### Gaps Encountered

#### Gap 1: Spring Security Argon2 factory name (Plan-to-Implementation)
- **Task**: 4.1 — Add `spring-boot-starter-security` and `SecurityConfig`
- **Plan assumed**: `Argon2PasswordEncoder.defaultsForSpringSecurity()` was available.
- **Reality**: The project resolves Spring Security 6.3.4, whose available factories are versioned and expose `defaultsForSpringSecurity_v5_8()` instead.
- **Resolution**: Used the available v5.8-compatible factory, retaining Argon2 hashing with the project dependency baseline.
- **Learning**: Inspect the resolved dependency API before writing framework factory calls; Spring Security minor versions can expose differently named compatibility factories.

#### Gap 2: Missing Spring Security test dependency (Intent-to-Plan)
- **Task**: 5.2 — Add `AuthController` and security error mappings
- **Plan assumed**: The existing test dependencies provided `SecurityMockMvcRequestPostProcessors.csrf()`.
- **Reality**: `spring-security-test` was not present in `replicadb-server/pom.xml`, so the required CSRF test code could not compile.
- **Resolution**: Added `spring-security-test` as a test-scoped Maven dependency.
- **Learning**: Dependency inventories in implementation plans must include framework test modules whenever tests use framework-specific security or MVC helpers.

#### Gap 3: JDBC sessions serialize the authenticated principal (Plan-to-Implementation)
- **Task**: 5.2 — Persist the authenticated security context
- **Plan assumed**: Implementing `UserDetails` around the domain record was sufficient for Spring Session JDBC persistence.
- **Reality**: Spring Session serializes the `SecurityContext`; the wrapped `AppUser` record was not serializable, so login failed while writing session attributes.
- **Resolution**: Made the immutable `AppUser` record serializable with an explicit serial-version identifier.
- **Learning**: Durable session plans must test serialization of the complete principal graph, not only authentication and password matching.

#### Gap 4: Fail-closed bootstrap and existing test contexts (Plan-to-Implementation)
- **Task**: 6.1 — Add `AdminBootstrapRunner`
- **Plan assumed**: Requiring bootstrap variables when no ADMIN exists would not affect existing server integration contexts.
- **Reality**: Existing Testcontainers contexts start with an empty `app_user` table and intentionally do not provide deployment bootstrap credentials.
- **Resolution**: Added a test-only `application-api.yml` disabling bootstrap for legacy contexts; real security lifecycle tests explicitly enable bootstrap through dynamic test properties and generated in-memory values.
- **Learning**: Fail-closed startup behavior needs an explicit test-profile strategy before it is introduced into a module with many context-loading integration tests.

#### Gap 5: Real-port lifecycle tests were not MockMvc tests (Plan-to-Implementation)
- **Task**: 12.1 — Update existing controller/IT tests to authenticate
- **Plan assumed**: The lifecycle integration tests used MockMvc and could be adapted with `@WithMockUser` and `.with(csrf())`.
- **Reality**: `JobLifecycleIT` and `ScheduledJobLifecycleIT` use `TestRestTemplate` against random HTTP ports, so mock security annotations cannot authenticate them.
- **Resolution**: Added real login helpers that replay JDBC session cookies and CSRF cookie/header values; added dynamic bootstrap properties for those tests. The Spring Security 6.3 CSRF request handler was configured for the raw cookie-to-header browser contract.
- **Learning**: Classify test clients during planning; MockMvc security support and real HTTP session behavior are different integration surfaces.

### Patterns Discovered
- PostgreSQL UUID-array restrictions keep ACL filtering before pagination in [JobDefinitionRepository.java](replicadb-server/src/main/java/org/replicadb/server/job/persistence/JobDefinitionRepository.java) and [JobRunRepository.java](replicadb-server/src/main/java/org/replicadb/server/job/persistence/JobRunRepository.java).
- `JobAccessService` centralizes ADMIN bypass, principal-id extraction, and exact per-job permission checks for every controller.
- Real-port security tests use a small explicit cookie jar for `SESSION` and `XSRF-TOKEN`, matching the future browser client without introducing a production HTTP client abstraction.

<details>
<summary>Dependencies</summary>

- New Maven dependencies: `spring-boot-starter-security`, `spring-session-jdbc` (both excluding `spring-boot-starter-logging`, matching the existing starters in `replicadb-server/pom.xml`).
- Argon2 password hashing is provided by `spring-security-crypto` (already transitive via `spring-boot-starter-security`); no separate dependency needed.
- No new dependency for CSRF; `CookieCsrfTokenRepository` is part of `spring-security-web`.

</details>

<details>
<summary>Testing Strategy</summary>

- Unit tests (no Spring context or a minimal one): `AppUserTest`, `JobPermissionTest`, `LoginAttemptServiceTest`, `JobAccessServiceTest` (Mockito), `AdminBootstrapRunnerTest` (Mockito with an injectable env-lookup function).
- Testcontainers PostgreSQL (`@ServiceConnection`, existing `PostgresTestcontainersConfig` pattern): `AppUserRepositoryIT`, `JobPermissionRepositoryIT`, `SecurityConfigTest`, `ReplicaDbUserDetailsServiceTest`, `AuthControllerTest`, `UserControllerTest`, all updated Phase 1c-1/1c-2 controller tests, `JobPermissionControllerTest`, `SecurityJobLifecycleIT`.
- `@WithMockUser` (Spring Security test support) for coarse global-role checks; `@WithMockReplicaDbUser` (this plan's own annotation) for per-job ACL checks without a real login round-trip; exactly one test (`SecurityJobLifecycleIT`) exercises the real login-then-act flow end-to-end.
- Every mutating `MockMvc` call in both new and updated tests uses `SecurityMockMvcRequestPostProcessors.csrf()`, since CSRF is enabled by `SecurityConfig` for everything except `/api/v1/auth/login`.
- Existing Phase 1a/1b/1c-1/1c-2 tests must all continue to pass, with only the deliberate authentication/CSRF additions from Task 12.1 — no other behavior change.
- CI: no changes expected to `CT_Push.yml`'s `server` job — it already runs Testcontainers-backed tests for this module with Docker configured.

</details>
