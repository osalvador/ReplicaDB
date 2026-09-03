# Implementation Plan: Local pg2pg Integration Fixture

## Task Source

User request: revisar y preparar los datasources y job runs locales de PostgreSQL para que ReplicaDB arranque con una integración real `pg2pg`, usando el datasource `Pglocal`, una tabla de destino nueva y una traza visible en la interfaz.

Acceptance criteria:

- El arranque local crea un datasource PostgreSQL reutilizable llamado `Pglocal`.
- El arranque local crea un job llamado `pg2pg` con origen y destino PostgreSQL.
- El origen y el destino usan tablas de prueba aisladas del metadata del control plane.
- El arranque inserta datos deterministas en el origen y deja preparado el destino.
- Se lanza una ejecución real mediante el flujo normal de runs, no mediante el mecanismo de historial sintético.
- La ejecución termina en `SUCCEEDED` y deja un `JobRun` y log consultables desde la UI.
- La prueba confirma que las filas llegan a la tabla destino.
- Los fixtures existentes de otros conectores siguen disponibles.
- No se guardan credenciales en el repositorio ni se exponen en logs.

## Overview

El entorno local actual siembra definiciones genéricas con tablas que no existen y usa `X-ReplicaDB-Local-Seed` para fabricar historial cancelado. Se añadirá un fixture dedicado para PostgreSQL que use tablas de negocio aisladas, cree `Pglocal` y `pg2pg`, ejecute una réplica real y espere su resultado antes de anunciar que el entorno está listo.

La preparación de tablas permanecerá en `start-local.sh`, donde ya se controla el ciclo de vida del contenedor PostgreSQL. La creación de recursos y la ejecución permanecerán en `seed-local-jobs.mjs`, reutilizando el API autenticado y el endpoint normal `POST /api/v1/jobs/{id}/runs`.

## Architecture & Design

Approach: Pragmatic balance.

- Mantener el seeder Node como propietario de los recursos del control plane: datasource, job y run.
- Mantener el shell como propietario de la infraestructura local y ejecutar SQL con el cliente `psql` dentro del contenedor ya listo.
- Añadir un esquema aislado `pg2pg` y tablas `pg2pg_source_orders` y `pg2pg_destination_orders`, con `id` y `payload`; no reutilizar `audit_event`, `audit_event_sink`, `job_run` ni ninguna tabla de metadata.
- Usar un job `complete` con una única tarea y columnas explícitas `id, payload`, porque es el camino PostgreSQL ya validado para el fixture local; documentar que limpia el destino antes de cargarlo.
- Disparar el run sin `X-ReplicaDB-Local-Seed`; ese header seguirá reservado al historial sintético de pruebas visuales.
- Hacer polling de `GET /api/v1/runs/{id}` hasta un estado terminal, fallando el arranque si el run no termina en `SUCCEEDED`.
- Comprobar el resultado con una consulta PostgreSQL después del run y emitir un resumen sin credenciales.
- No cambiar controladores Java, repositorios, migraciones ni el contrato REST: las capacidades actuales de ejecución local y el endpoint de runs ya cubren el caso.

Security and operations:

- Mantener usuario y contraseña del administrador en variables de entorno existentes.
- Mantener la conexión JDBC sin credenciales embebidas; el contenedor usa `trust` únicamente para desarrollo local.
- No imprimir cuerpos de datasource, contraseñas, claves maestras ni errores JDBC sin redacción.
- Como `start-local.sh` elimina el contenedor en cada arranque, la preparación puede ser determinista; aun así, las sentencias SQL deben ser seguras ante reintentos dentro del mismo proceso. El script debe usar `$CONTAINER_ENGINE exec` en todas las operaciones para conservar soporte Docker/Podman.

## Implementation Tasks

### 1. Definir el fixture PostgreSQL real

- [x] **1.1 Añadir constantes y constructores de payload para `Pglocal` y `pg2pg`**
  Files: `replicadb-server/frontend/scripts/seed-local-jobs.mjs`
  Changes: Definir nombres exactos, conexión basada en `REPLICADB_POSTGRES_PORT`, tablas aisladas, columnas, modo `complete` y número esperado de filas; añadir builders separados o una configuración explícita sin alterar el formato de los diez fixtures genéricos.
  Tests: Verificar que el payload del datasource no contiene credenciales, que el payload del job enlaza el mismo datasource en ambos extremos, que usa las tablas aisladas y que conserva `complete`, `jobs: 1` y `id, payload`.
  Dependencies: None

- [x] **1.2 Asegurar la creación idempotente del datasource y del job dedicados**
  Files: `replicadb-server/frontend/scripts/seed-local-jobs.mjs`
  Changes: Reutilizar recursos existentes por nombre cuando sean compatibles; crear `Pglocal` y `pg2pg` cuando no existan; no usar el header de local seeding para esta integración; conservar el resultado de los fixtures genéricos.
  Tests: Con un `fetch` simulado, comprobar creación en el orden datasource/job, reutilización sin POST duplicados y ausencia de `sourceConnect`, `sinkConnect`, usuarios o contraseñas en el payload del job.
  Dependencies: 1.1

### 2. Preparar datos PostgreSQL aislados

- [x] **2.1 Crear y poblar las tablas de integración tras la disponibilidad de PostgreSQL**
  Files: `replicadb-server/frontend/scripts/start-local.sh`
  Changes: Después de `pg_isready` y antes de levantar o sembrar el API, ejecutar `psql` dentro de `replicadb-dev-postgres` con `CREATE SCHEMA IF NOT EXISTS pg2pg`, crear las tablas `pg2pg.pg2pg_source_orders` y `pg2pg.pg2pg_destination_orders`, truncarlas e insertar tres filas deterministas en el origen; dejar `public` intacto para Flyway y usar nombres cualificados en el SQL. El bloque debe poder ejecutarse dos veces sin duplicar filas.
  Tests: Ejecutar el script en un contenedor limpio y comprobar existencia, columnas, cantidad y valores del origen, además de destino vacío; repetir el bloque SQL y comprobar que sigue habiendo tres filas; comprobar que un fallo de SQL detiene el arranque con diagnóstico.
  Dependencies: None

- [x] **2.2 Añadir una comprobación de destino observable al seeder**
  Files: `replicadb-server/frontend/scripts/seed-local-jobs.mjs`, `replicadb-server/frontend/scripts/seed-local-jobs.test.mjs`
  Changes: Elegir explícitamente la verificación en shell: después de que el seeder termine, `start-local.sh` ejecutará `$CONTAINER_ENGINE exec` con `psql -Atc 'SELECT count(*) FROM pg2pg.pg2pg_destination_orders'` y comparará el resultado con `PG2PG_EXPECTED_ROWS=3`; el seeder solo será responsable de que el run termine en `SUCCEEDED`. Un conteo distinto o un error SQL debe detener el arranque con un diagnóstico sin secretos.
  Tests: Cubrir en la prueba de proceso el destino con tres filas, destino incompleto y error de consulta; mantener las pruebas unitarias del seeder independientes de una base real y añadir una prueba del camino de error del comando de verificación.
  Dependencies: 2.1, 1.2

### 3. Ejecutar un JobRun real durante el arranque

- [x] **3.1 Separar historial sintético de ejecución real**
  Files: `replicadb-server/frontend/scripts/seed-local-jobs.mjs`
  Changes: Mantener `seedJobRunHistory` para los fixtures visuales que requieren runs terminales artificiales, pero añadir un flujo específico para `pg2pg` que invoque `POST /api/v1/jobs/{jobId}/runs` con `Idempotency-Key` y sin `X-ReplicaDB-Local-Seed`.
  Tests: Verificar que el trigger real no envía el header de local seed, conserva la clave de idempotencia y acepta inicialmente `PENDING` o `RUNNING`.
  Dependencies: 1.2

- [x] **3.2 Esperar el estado terminal y fallar si la réplica no es exitosa**
  Files: `replicadb-server/frontend/scripts/seed-local-jobs.mjs`, `replicadb-server/frontend/scripts/seed-local-jobs.test.mjs`
  Changes: Poll de `GET /api/v1/runs/{runId}` cada `REPLICADB_LOCAL_INTEGRATION_POLL_INTERVAL_MS` (por defecto `500`) durante como máximo `REPLICADB_LOCAL_INTEGRATION_TIMEOUT_SECONDS` (por defecto `120`); aceptar solo `SUCCEEDED` como resultado válido de la integración; incluir el identificador del run y el estado en el diagnóstico; no cancelar automáticamente un run real.
  Tests: Cubrir transición `PENDING` → `RUNNING` → `SUCCEEDED`, `FAILED`, `CANCELLED`, timeout con los valores configurados y respuesta con run inexistente; comprobar que los errores no incluyen contraseñas ni material de clave.
  Dependencies: 3.1

- [x] **3.3 Integrar el fixture real sin romper los fixtures existentes**
  Files: `replicadb-server/frontend/scripts/seed-local-jobs.mjs`, `replicadb-server/frontend/scripts/seed-local-jobs.test.mjs`
  Changes: Ejecutar el flujo dedicado después de crear los diez fixtures actuales o en un orden documentado; ampliar el resultado del seeder con el job real y su run; conservar los contadores existentes. En cada arranque limpio, el SQL de la tarea 2.1 reinicializa origen y destino antes de crear el run; si el datasource/job ya existen, se reutilizan por nombre y se crea una nueva ejecución real sin crear recursos duplicados.
  Tests: Actualizar las aserciones de conteo, probar un seeder completo con mocks y verificar que los fixtures de Oracle, MySQL, MariaDB, PostgreSQL, DB2, DB2 for i, SQLite, SQL Server, Denodo y File siguen creándose.
  Dependencies: 2.2, 3.2

### 4. Documentar y validar el flujo local

- [x] **4.1 Actualizar la guía de desarrollo local**
  Files: `replicadb-server/frontend/README.develop.md`
  Changes: Actualizar la sección que actualmente indica que “los fixtures cubren los modos ...; no ejecutan ninguna replicación” para documentar que el arranque prepara `Pglocal`, `pg2pg`, las tablas aisladas y una ejecución real; indicar dónde consultar el run/log y cómo verificar `pg2pg_destination_orders`; conservar la descripción de los otros fixtures.
  Tests: Ejecutar `scripts/check-phase3-docs.sh` y revisar que los comandos documentados usan el puerto PostgreSQL elegido dinámicamente y que el flujo funciona con `CONTAINER_ENGINE=docker` y `CONTAINER_ENGINE=podman` cuando el motor está disponible.
  Dependencies: 3.3

- [x] **4.2 Validar el recorrido completo en un entorno limpio**
  Files: `replicadb-server/frontend/scripts/start-local.sh`, `replicadb-server/frontend/scripts/seed-local-jobs.mjs`, `replicadb-server/frontend/scripts/seed-local-jobs.test.mjs`
  Changes: Ejecutar la suite del seeder y un arranque limpio; comprobar API, frontend, datasource `Pglocal`, job `pg2pg`, run `SUCCEEDED`, log no vacío y filas equivalentes en origen/destino; confirmar limpieza del contenedor al recibir `Ctrl+C` o al fallar el arranque.
  Tests: `npm --prefix replicadb-server/frontend run test:seed`; prueba manual o de proceso del script con Docker/Podman; consulta PostgreSQL de comparación de filas y revisión del detalle del run desde la UI.
  Dependencies: 4.1

## Technical Reference

### Types & Data Structures

- Datasource request: `name`, `connectorType: "postgres"`, `technicalParams: {}`, `security.connect` y `clearSecurityKeys: []`.
- Job request: datasource ID común en source/sink, `sourceTable`, `sinkTable`, `sourceColumns`, `sinkColumns`, `mode`, `jobs`, `fetchSize`, `bandwidthThrottling` y `verbose`.
- Run trigger: `POST /api/v1/jobs/{jobId}/runs` con `Idempotency-Key`; el resultado inicial es asíncrono y se observa con `GET /api/v1/runs/{runId}`.
- Fixture SQL: esquema `pg2pg` con dos tablas PostgreSQL de aplicación, con la misma forma y sin claves foráneas hacia metadata del control plane; el job usa nombres cualificados `pg2pg.pg2pg_*` y el datasource usa `currentSchema=pg2pg`.

### Dependencies

- No añadir dependencias npm ni Java.
- Requiere `psql` dentro de `postgres:16-alpine`, Docker o Podman, Node.js 22, npm 10 y el entorno Java 17 ya requerido por el arranque.
- Depende de la configuración existente `replicadb.server.local-execution.enabled=true` y de la autenticación CSRF/sesión del seeder.

### Testing Strategy

- Unit tests Node para builders, orden de llamadas, idempotencia, polling y clasificación de estados.
- Validación de proceso con PostgreSQL real para comprobar esquema, datos, transferencia y limpieza.
- No usar el mecanismo `X-ReplicaDB-Local-Seed` para demostrar que la integración funciona.
- No tocar ni usar tablas `audit_event`, `audit_event_sink`, `job_run` o `run_log` como tablas de datos del ejemplo.
- Mantener credenciales administradas por variables de entorno y revisar que los mensajes de fallo y logs del run no las incluyan. El seeder no debe imprimir cuerpos de error completos; debe propagar solo `detail`/`title` ya redacted por el API, y las comprobaciones del shell solo deben mostrar estado y conteos.

## Execution Retrospective (auto-generated by /itx-code)

### Plan Accuracy

- Tasks completed as planned: 5/9 (56%).
- Tasks that required plan adjustment: 4/9 (44%).
- Test loop iterations: 8 total (7 first-pass, 1 second-pass).

### Gaps Encountered

#### Gap 1: Fixture tables had to be isolated from Flyway public schema (Intent-to-Plan)

- **Task**: 2.1 — Crear y poblar las tablas de integración tras la disponibilidad de PostgreSQL.
- **Plan assumed**: Las tablas de prueba podían crearse en `public` antes del API.
- **Reality**: Flyway rechaza un esquema `public` no vacío sin `flyway_schema_history`.
- **Resolution**: Se creó el esquema `pg2pg`, se cualificaron las tablas y se configuró `currentSchema=pg2pg`.
- **Learning**: Los fixtures de datos deben vivir en un esquema separado del esquema de metadata cuando Flyway inicializa la base durante el arranque.

#### Gap 2: The local datasource required an explicit PostgreSQL user (Plan-to-Implementation)

- **Task**: 1.1 — Añadir constantes y constructores de payload.
- **Plan assumed**: PostgreSQL `trust` permitiría conectar sin usuario.
- **Reality**: El driver intentó usar el usuario del sistema `oscarsm`, que no existe en el contenedor.
- **Resolution**: Se añadió `user: postgres` al datasource local, sin contraseña.
- **Learning**: Incluso con autenticación `trust`, los fixtures JDBC deben declarar el rol técnico del contenedor.

#### Gap 3: complete-atomic was not viable for the local fixture (Plan-to-Implementation)

- **Task**: 1.1/3.3 — Configurar y ejecutar el job real.
- **Plan assumed**: El camino `complete-atomic` funcionaría con el datasource PostgreSQL local.
- **Reality**: El run falló en `Sink pre-task failed`; el camino `complete` ya estaba validado y es suficiente para una demostración local.
- **Resolution**: Se cambió solo el fixture `pg2pg` a `complete` y se documentó su limpieza destructiva del destino.
- **Learning**: Los fixtures de integración deben comenzar por el modo manager ya probado y reservar modos alternativos para pruebas específicas del core.

#### Gap 4: The full process validation was stopped during npm dependency installation (Plan-to-Implementation)

- **Task**: 4.2 — Validar el recorrido completo.
- **Plan assumed**: `npm ci` finalizaría dentro de la ventana de validación.
- **Reality**: La réplica y la verificación de destino terminaron, pero `npm ci` siguió ejecutándose y fue interrumpido para limpiar el entorno.
- **Resolution**: Se verificó el backend y la integración real; la instalación de frontend quedó sin completar en esta sesión.
- **Learning**: Separar la validación del seeder/backend de la instalación de dependencias frontend y aplicar límites explícitos a instalaciones de red.

### Patterns Discovered

- Local PostgreSQL fixtures: use a dedicated schema and fully qualified table names; see `replicadb-server/frontend/scripts/start-local.sh` and `seed-local-jobs.mjs`.
