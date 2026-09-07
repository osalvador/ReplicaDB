# Implementation Plan: Release 1.0.0 controlada de ReplicaDB

## Task Source - user request

Preparar y publicar la primera release estable `v1.0.0` con un flujo controlado:

1. Actualizar version `0.19.0` -> `1.0.0` en CLI, server y dependencia sibling.
2. Actualizar documentacion y nombres de artefactos.
3. Ejecutar gates locales: tests, embedded PostgreSQL, builds, archives,
   checksums, Docker smoke, workflow lint y documentacion.
4. Crear el commit de preparacion y hacer push a `master`.
5. Esperar CT, CodeQL y Pages en verde.
6. Crear y subir manualmente `v1.0.0` solo despues del verde.

El plan anterior de la release `0.19.0` ya esta archivado en
`.ai/archive/primera-release-de-replicadb-con-servidor.plan.md` y no se debe
sobrescribir. Los archivos no trackeados de diseño, learnings o planes antiguos
no forman parte del release commit y deben permanecer fuera del staging.

## Release policy

- No crear ni subir `v1.0.0` antes de completar todos los gates remotos.
- No ejecutar el flujo antiguo monolitico de `release.sh` que cree tag/push sin
  una validacion previa.
- El commit de preparacion usa `feat(release): prepare 1.0.0`.
- El tag estable es anotado: `v1.0.0`.
- La publicacion debe contener CLI archives, server archives, JAR directo y
  `SHA256SUMS`; nunca un subconjunto.
- Si falla un gate remoto, corregir, crear un nuevo commit de preparacion y
  repetir CT antes de crear el tag.

## Implementation Tasks

### 1. Congelar version y nombres de `1.0.0`

- [x] **1.1 Actualizar el contrato de version CLI/server y artefactos**
  Files: `pom.xml`, `replicadb-server/pom.xml`, `README.md`, `DEPLOYMENT.md`,
  `RELEASE_GUIDE.md`, `CONTRIBUTING.md`, `PRODUCT.md`, `docs/index.md`,
  `docs/server.md`, `replicadb-server/README.md`,
  `replicadb-server/frontend/README.develop.md`.
  Changes: Cambiar ambos project versions y la dependencia
  `org.replicadb:ReplicaDB` a `1.0.0`; actualizar nombres de archives, JAR,
  Docker tags, URLs, ejemplos y tablas de version; retirar referencias de
  `0.19.0` que describan la release actual sin modificar historiales tecnicos
  que documenten releases anteriores; mantener separados `REPLICADB_HOME` y
  `REPLICADB_SERVER_HOME`.
  Tests: Parsear ambos POM y comprobar version de proyecto y dependencia;
  buscar stale references en las superficies de release; validar nombres
  `ReplicaDB-1.0.0.*`, `ReplicaDB-server-1.0.0.*` y
  `replicadb-server-1.0.0.jar`; ejecutar `git diff --check`.
  Dependencies: None.

### 2. Endurecer el flujo local de release

- [x] **2.1 Separar prepare, validate, tag y push en `release.sh`**
  Files: `release.sh`, `scripts/release-script.test.sh` (nuevo),
  `RELEASE_GUIDE.md`.
  Changes: Rediseñar el contrato para que `prepare VERSION` valide branch y
  working tree completo, actualice versiones/documentacion, valide coherencia,
  cree el commit `feat(release): prepare VERSION` y haga push de `master`, pero
  nunca cree tag; `validate VERSION` debe ser no destructivo; `tag VERSION`
  debe exigir working tree limpio, version coherente y ausencia del tag antes de
  crear el tag anotado; `push-tag VERSION` debe ser explicito; rechazar
  versiones invalidas, tag existente, branch incorrecta y archivos no
  trackeados inesperados; no incluir archivos ajenos de `.ai`, diseño o
  artefactos generados.
  Tests: `bash -n`; tabla de comandos validos/invalidos; argumentos ausentes;
  version mismatch; working tree sucio con cambios trackeados o untracked;
  prepare no crea tag; tag rechaza CI no validado cuando se invoque sin la
  confirmacion/gate requerida; tag duplicado; pruebas con repositorio Git
  temporal que no toquen el remoto real.
  Dependencies: 1.1.

### 3. Crear skill reutilizable de release

- [x] **3.1 Crear la skill `replicadb-release` para orquestar la publicacion**
  Files: `.github/skills/replicadb-release/SKILL.md` (nuevo),
  `.github/skills/replicadb-release/scripts/` (solo si se necesitan probes no
  destructivos), `RELEASE_GUIDE.md`.
  Changes: Documentar el flujo agentico: preflight limpio, `release.sh
  prepare`, gates locales, push de `master`, consulta y espera de CT/CodeQL/
  Pages, bloqueo ante cualquier failure, confirmacion final, `release.sh tag`
  y `push-tag`, y verificacion de assets/checksums; prohibir secretos en logs,
  no borrar tags automaticamente y excluir untracked ajenos; definir
  recuperacion para reruns y release existente con `--clobber` solo para la
  misma version.
  Tests: Validar frontmatter/Markdown y rutas; prueba de contrato que exige
  los estados prepare/validate/remote-green/tag/publish; comprobar que la skill
  menciona `v1.0.0`, `SHA256SUMS`, los tres workflows y el bloqueo previo al
  tag; revisar que no contiene credenciales ni comandos destructivos de reset.
  Dependencies: 2.1.

### 4. Ejecutar el release candidate local

- [x] **4.1 Completar todos los gates locales para `1.0.0`**
  Files: `pom.xml`, `replicadb-server/pom.xml`, `scripts/package-server-release.sh`,
  `release.sh`, `.github/workflows/CI_Release.yml`,
  `.github/workflows/CT_Push.yml`, documentacion de release y artefactos
  temporales fuera del repositorio.
  Changes: Ejecutar la secuencia root CLI install -> server package -> server
  archives -> direct JAR -> SHA256SUMS; verificar Main-Class/Start-Class,
  tamano aproximado, listado sin bundles PostgreSQL, identidad del JAR directo,
  Docker image smoke, Compose config, embedded profile warm/cold cache,
  launcher POSIX, `actionlint`, YAML parser, docs gate y guard de
  `start-local.sh`; eliminar staging temporal al terminar.
  Tests: `mvn -B -f replicadb-server/pom.xml test` focalizado y suite server;
  `-Pembedded-postgres`; builds CLI/server; TAR.GZ/ZIP reproducibles;
  checksum verification; `scripts/phase3-image-smoke.sh`;
  `scripts/check-phase3-docs.sh`; `actionlint`; `git diff --check`;
  `bash -n`; `git diff --exit-code --
  replicadb-server/frontend/scripts/start-local.sh`.
  Dependencies: 1.1, 2.1, 3.1.

### 5. Preparar y subir el commit de release

- [x] **5.1 Crear commit `feat(release): prepare 1.0.0` y hacer push**
  Files: Todo el conjunto validado de la release; ningun archivo de diseño,
  learning o plan ajeno.
  Changes: Confirmar que solo estan staged los cambios de version, docs,
  tooling, tests y CI de `1.0.0`; ejecutar `./release.sh prepare 1.0.0` o el
  procedimiento equivalente definido en 2.1; comprobar que el commit existe en
  `master` remoto; no crear tag en esta tarea.
  Tests: `git diff --cached --check`; inspeccion de `git show --stat`; confirmar
  `git status` sin cambios release pendientes; `git ls-remote origin master`
  coincide con el commit de preparacion; confirmar que `git ls-remote --tags`
  no contiene `v1.0.0`.
  Dependencies: 4.1.

### 6. Esperar y cerrar los gates remotos

- [x] **6.1 Esperar CT, CodeQL y Pages en verde para el commit preparado**
  Files: `.github/workflows/CT_Push.yml`, `.github/workflows/CI_Release.yml`
  solo si un fallo atribuible exige correccion; ningun cambio de tag.
  Changes: Consultar workflows por el SHA preparado y esperar su finalizacion;
  exigir `Only CI/CT`, `CodeQL` y `pages-build-deployment` en success;
  clasificar fallos de infraestructura separados de regresiones de codigo;
  si hay error de codigo, corregir y repetir desde tarea 5; bloquear cualquier
  tag mientras un job este pending, failure, cancelled o skipped de forma
  inesperada.
  Tests: `gh run list`, `gh run view --json jobs`, logs de jobs fallidos y
  comprobacion del SHA; no aceptar verde de un commit diferente; CT debe
  incluir Windows launcher, Frontend E2E, embedded matrix, server module,
  fairness y multinode.
  Dependencies: 5.1.

### 7. Crear y publicar `v1.0.0` manualmente

- [x] **7.1 Crear tag anotado y disparar la release despues del verde**
  Files: Git tag remoto y workflow de release; ningun cambio adicional salvo
  una correccion aprobada.
  Changes: Verificar de nuevo working tree limpio, SHA remoto y gates verdes;
  ejecutar `git tag -a v1.0.0 -m "Release v1.0.0"` y `git push origin v1.0.0`;
  esperar `CI_Release.yml`; confirmar jobs build/windows_launcher/
  embedded_postgres/publish; no subir assets manualmente fuera del workflow.
  Tests: `git show v1.0.0`; `git ls-remote --tags origin v1.0.0`; validar
  release GitHub completa con CLI archives, server archives, JAR y
  `SHA256SUMS`; recalcular checksums y comprobar que los Docker tags
  `1.0.0` y `latest` apuntan a la publicacion esperada.
  Dependencies: 6.1.

### 8. Verificar y cerrar la release

- [x] **8.1 Ejecutar smoke post-release y documentar el resultado**
  Files: `RELEASE_GUIDE.md`, `README.md`, `docs/server.md` solo si hay una
  URL/nombre incorrecto; staging temporal local fuera del repo.
  Changes: Descargar los archives desde la release `v1.0.0`, extraer en
  directorios limpios, ejecutar `help`, `start local`, `status`, `stop`,
  verificar SHA256SUMS y comprobar que CLI/server coexisten sin mover homes;
  registrar resultado, warnings y cualquier bloqueo de infraestructura.
  Tests: Smoke instalado desde GitHub Release; checksums GNU/Windows;
  verificacion de version de los archives/JAR; ausencia de procesos Java/
  PostgreSQL despues del smoke; enlaces documentales y `git status` limpio.
  Dependencies: 7.1.

## Acceptance Gate

La release `1.0.0` esta lista solo cuando:

1. Ambos POM y todos los nombres de assets usan `1.0.0` de forma coherente.
2. El README enlaza a la documentacion completa y no mezcla desarrollo con
   instalacion de usuario.
3. `release.sh` y la skill separan preparacion de tag/publish.
4. El commit de preparacion esta en `master` y no contiene artefactos ajenos.
5. CT, CodeQL y Pages estan verdes para exactamente ese commit.
6. El tag anotado `v1.0.0` solo se crea despues del verde.
7. La release GitHub contiene CLI, server, JAR directo y `SHA256SUMS`.
8. El smoke post-release funciona desde archives descargados.

## Known Constraints

- Los archivos no trackeados actuales de `.ai/learnings`,
  `.ai/archive/postgresql-embebido-para-el-servidor-local.plan.md`,
  `replicadb-server/frontend/.impeccable/critique/` y `shape-datasources.png`
  quedan fuera del release commit salvo decision posterior explicita.
- La suite root CLI puede mostrar fallos de infraestructura de contenedores;
  deben clasificarse por separado de regresiones del release.
- El build Jekyll local puede requerir gems no instaladas; el gate remoto de
  Pages es la validacion definitiva del sitio.

## Execution Retrospective (auto-generated by /itx-code)

### Plan Accuracy

- Tasks completed as planned: 8/8 (100%).
- Tasks that required plan adjustment: 4/8 (50%).
- Test loop iterations: 12 total (8 first-pass, 3 second-pass, 1 third-pass).

### Gaps Encountered

#### Gap 1: Stale container defaults outside the original release surface (Plan-to-Implementation)

- **Task**: 4.1.
- **Plan assumed**: Existing image/Compose and phase smoke defaults would follow
  the Maven version.
- **Reality**: Several scripts and Docker/Compose defaults still referenced
  `0.19.0`, breaking CT image and multinode/fairness builds after versioning.
- **Resolution**: Derive image smoke versions from the server POM and align
  Dockerfile/Compose defaults; include those surfaces in `prepare`.
- **Learning**: Release version surfaces include every artifact-producing script,
  Docker build argument, and Compose default, not only docs and POMs.

#### Gap 2: Unrelated documentation work was broader than the initial exclusion list (Intent-to-Plan)

- **Task**: 5.1.
- **Plan assumed**: The known `.ai`, design, and plan exclusions covered all
  unrelated active work.
- **Reality**: An Astro portal, tracked legacy-link edits, OpenAPI fixtures,
  screenshot tests, generated outputs, and docs Playwright processes appeared.
- **Resolution**: Preserved the work in reversible stashes and expanded only the
  explicit untracked exclusions; no portal file entered the release commits.
- **Learning**: A full-worktree release guard must preserve active parallel docs
  work by path, including generated descendants that appear during execution.

#### Gap 3: Windows embedded PostgreSQL requires a user-owned home (Plan-to-Implementation)

- **Task**: 7.1.
- **Plan assumed**: The runner temporary directory was suitable for packaged
  local server state on every platform.
- **Reality**: Windows `initdb` failed to change permissions under `D:\a\_temp`.
- **Resolution**: Use the Windows runner user profile for `REPLICADB_SERVER_HOME`,
  increase first-run timeouts, and emit redacted server logs on failure.
- **Learning**: Native PostgreSQL Windows smoke state must live under a path where
  the runner user can apply database directory ACLs.

#### Gap 4: Windows batch process orchestration could hang indefinitely (Plan-to-Implementation)

- **Task**: 7.1.
- **Plan assumed**: The existing Windows launcher would return from `start local`
  once the server was ready.
- **Reality**: The `for /f`/PowerShell process capture and readiness loop hung for
  the full hosted-job timeout despite the server eventually starting.
- **Resolution**: Bound CIM queries, replace the PowerShell HTTP loop with bounded
  `curl.exe` probes, and write the PID directly from PowerShell instead of through
  a batch `for /f` wrapper.
- **Learning**: Hosted Windows launchers need direct process ownership, bounded
  readiness probes, and phase-level diagnostics; a shell wrapper can hide hangs.

#### Gap 5: Publish exact-set validation created its own helper files (Plan-to-Implementation)

- **Task**: 7.1.
- **Plan assumed**: Temporary comparison files inside staging would not affect
  asset enumeration.
- **Reality**: `expected.list` and `actual.list` made the six-asset comparison
  fail after all checksums and platform jobs passed.
- **Resolution**: Use process substitutions for both sorted lists, leaving staging
  immutable and containing only the six published files.
- **Learning**: Release staging validation must not materialize helper files in the
  directory whose exact contents it validates.

#### Gap 6: Fairness and test-container gates are timing-sensitive (Plan-to-Implementation)

- **Task**: 6.1.
- **Plan assumed**: A single green run would be the final remote-gate result.
- **Reality**: Fairness utilization and an isolated PostgreSQL test deadlock
  failed intermittently while all other jobs passed.
- **Resolution**: Classified the failures from logs, retried only the affected
  exact-SHA jobs, and accepted the final all-green matrix without changing code.
- **Learning**: Remote release gates should support exact-SHA targeted retries for
  demonstrably flaky infrastructure/timing failures, without bypassing failures.

### Patterns Discovered

- `release.sh` allowlists release surfaces and explicitly excludes active portal
  work; see `release.sh` and `scripts/release-script.test.sh`.
- Versioned container builds derive defaults from the server POM or explicit release
  build arguments; see `scripts/phase3-image-smoke.sh` and `docker-compose.server.yml`.
- Windows packaged smoke uses a user-owned server home and direct bounded process
  control; see `replicadb-server/bin/replicadb-server.cmd` and `CI_Release.yml`.
- Complete release staging is validated with process substitutions and no helper
  files in the asset directory; see `.github/workflows/CI_Release.yml`.
