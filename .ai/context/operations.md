## Build and Runtime
`pom.xml` targets Java 17 and pins JUnit Jupiter modules and Surefire 3.5.3. The `test` profile assembles a jar-with-dependencies; `release` and `release-no-oracle` create an executable jar and copy runtime dependencies. The launcher scripts resolve the distribution layout and invoke `org.replicadb.ReplicaDB`.

`Dockerfile` uses Eclipse Temurin 17 on Ubuntu Noble; `Containerfile` uses the UBI9 OpenJDK 17 runtime. Both unpack a versioned release archive and start the shell launcher with `conf/replicadb.conf`. The Java 17 migration also requires the existing `java.nio` module-opening flag for ORC paths.

## Logging and Telemetry
Log4j2 writes to the console, controls the `org.replicadb` level from the verbose option, and suppresses noisy external libraries. `ReplicaDB` creates a Sentry transaction for a run; `config.Sentry` attaches operation fields and connection parameters/tags. Treat that integration as a security boundary: credentials and credential-bearing URLs must be redacted before adding new telemetry or log fields.

## Repository Tooling
The root product is Maven/Java. `docs/` is a Jekyll documentation site, and `docs/markdown/` is a separate browser-only Markdown IDE built with Vite, Vitest, and Playwright. These frontend assets are repository tooling, not part of the Java replication runtime. No OpenAPI, AsyncAPI, or protobuf specification is present, so no protocol instruction file is generated.

`openspec/` contains active and archived specifications for JDBC null preservation, staging cleanup, and sink auto-creation. `ARCHITECTURE_DECISIONS.md` describes a future Spring Boot/Redis evolution; it is not evidence that REST, WebSocket, Quartz, or Redis code exists in the current source tree.

## Operational Constraints
- Credentials should be supplied through environment-expanded configuration and must not be committed or copied into context files.
- Database-specific runtime drivers may be bundled or provided externally; driver loading can fail at runtime.
- Integration tests require Docker-compatible infrastructure and can be sensitive to image architecture, reuse settings, and memory.
- Preserve release archive layout, launcher behavior, CLI exit codes, and Java 17 support statements together.

## Reference Implementations
- `pom.xml`
- `Dockerfile`
- `Containerfile`
- `bin/replicadb`
- `src/main/resources/log4j2.xml`
- `src/main/java/org/replicadb/config/Sentry.java`
