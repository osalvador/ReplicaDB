# Implementation Plan: Primera release de ReplicaDB con servidor

## Task Source - user request

No hay ticket JIRA asociado. Requisitos fijados durante `/itx-explore`:

- Publicar la primera release del servidor junto con la CLI en una release unificada `v0.19.0`.
- Mantener dos descargas independientes: CLI standalone y servidor gestionado.
- Publicar el servidor como paquete independiente `ReplicaDB-server-0.19.0.tar.gz/.zip` y como JAR directo `replicadb-server-0.19.0.jar`.
- Publicar `SHA256SUMS` para los assets descargables.
- No empaquetar binarios PostgreSQL; el servidor local los descarga bajo demanda y los verifica.
- Aceptar el tamaño inicial aproximado de 213 MB del JAR server; modularizar drivers queda fuera de esta release.
- Ofrecer en el paquete server launchers POSIX y Windows con `start`, `stop` y `status`.
- Exigir el modo en `start`: `local`, `api` o `worker`; no elegir una topologia silenciosamente.
- `local` usa PostgreSQL embebido y ejecucion local en el mismo proceso `api`.
- `api` y `worker` usan PostgreSQL externo; `worker` sigue siendo el runtime distribuido sin API publica.
- Separar el namespace del server de la CLI: `REPLICADB_SERVER_HOME`, con default `~/.replicadb`.
- Obtener las credenciales del primer administrador mediante prompt interactivo seguro cuando sea posible; aceptar variables/secretos gestionados para automatizacion y fallar claramente sin terminal.
- Mantener `start-local.sh` como harness de desarrollo efimero basado en Docker, sin cambiar su comportamiento.
- Actualizar README, DEPLOYMENT, RELEASE_GUIDE, CONTRIBUTING y el sitio de documentacion para reflejar el producto server real.
- Mantener la CLI sin Spring Boot y preservar el comportamiento de `api`, `worker`, Docker/Compose y el runtime PostgreSQL externo.

La preparacion de la release debe dejar lista la automatizacion y los assets; la creacion y push del tag `v0.19.0` sera una accion manual posterior sobre un working tree limpio.

## Overview

ReplicaDB pasara de publicar principalmente el archive CLI a publicar dos superficies coordinadas: la CLI independiente para ejecuciones directas y el servidor gestionado para jobs, schedules, autenticacion y observabilidad. El server ya contiene el core ReplicaDB, el runtime PostgreSQL embebido y la UI; falta convertirlo en una distribucion instalable sin Maven, Node/npm, Docker ni PostgreSQL preinstalado.

La release usara una version unica `0.19.0` en los dos POM, los nombres de assets, la imagen Docker y la documentacion. El paquete server no contendra PostgreSQL nativo: al ejecutar `start local`, el launcher usara el JAR server, descargara el bundle correspondiente a la plataforma en `REPLICADB_SERVER_HOME`, verificara su integridad y dejara los datos y el keyring persistentes.

## Architecture & Design

**Approach: release pragmatica con un paquete server independiente y launchers multiplataforma.**

```text
GitHub Release v0.19.0
├── ReplicaDB-0.19.0.tar.gz/.zip          CLI standalone
├── ReplicaDB-server-0.19.0.tar.gz/.zip  server instalable
├── replicadb-server-0.19.0.jar           JAR directo para usuarios avanzados/Docker
└── SHA256SUMS

ReplicaDB-server-0.19.0/
├── bin/replicadb-server                   POSIX daemon launcher
├── bin/replicadb-server.cmd               Windows daemon launcher
├── lib/replicadb-server-0.19.0.jar
├── conf/replicadb-server.env.example
├── README.md
└── LICENSE
```

### Runtime modes

```text
bin/replicadb-server start local
  -> REPLICADB_SERVER_HOME (default ~/.replicadb)
  -> PostgreSQL native bundle download/cache + checksum
  -> PostgreSQL data + keyring
  -> server JAR --replicadb.embedded-postgres.enabled=true
  -> api + local execution

bin/replicadb-server start api
  -> DB_URL/DB_USERNAME/DB_PASSWORD externos
  -> server JAR --spring.profiles.active=api

bin/replicadb-server start worker
  -> DB_URL/DB_USERNAME/DB_PASSWORD externos
  -> server JAR --spring.profiles.active=worker
```

`start` siempre recibira el modo. `status` y `stop` no recibiran secretos ni
los mostraran. El PID file, logs del daemon, lock de arranque y metadatos del
server viviran bajo `REPLICADB_SERVER_HOME`, mientras que `REPLICADB_HOME`
seguira reservado para la instalacion de la CLI y sus `conf/`/`lib/`.

### Release boundaries

- No se cambia `ReplicaDB.processReplica`, los managers, los repositorios, las migraciones ni los contratos REST/OpenAPI para resolver el empaquetado.
- No se crea un tercer artefacto Maven de ejecucion.
- No se incluyen los bundles `postgres-*.txz` en el JAR o en los archives server; solo se distribuye la libreria Java y el manifiesto/metadata necesarios para adquirirlos.
- La imagen `replicadb-server` continua usando PostgreSQL externo; no activa embedded por defecto.
- `replicadb-server/frontend/scripts/start-local.sh` continua usando Docker, seed de fixtures y cleanup destructivo para desarrollo local.
- El tamaño del JAR server se mide y se publica como dato de release, pero no bloquea `v0.19.0` mientras no crezca por encima del presupuesto acordado durante la implementacion.

### Security, reliability and platform policy

- Los launchers validaran que el PID pertenece al server esperado antes de detenerlo y trataran PID files stale como estado recuperable.
- `local` solicitara la password inicial sin eco en POSIX y con prompt oculto equivalente en Windows; la pasara al proceso hijo por entorno/secret handling, nunca por argumentos ni logs, y no la persistira en texto plano.
- Sin terminal interactiva, `local` exigira variables o un mecanismo de secretos externo y fallara con instrucciones; no intentara leer una password desde stdin de un daemon desconectado.
- El server local mantendra loopback y cookies HTTP locales; exponerlo fuera de la maquina requiere TLS/reverse proxy.
- El bundle nativo se adquirira solo para la plataforma detectada, con SHA-256, cache atomica, lock entre procesos y timeouts ya implementados en el runtime.
- La atomicidad del bundle usara `REPLICADB_SERVER_HOME/locks/postgres-<version>-<os>-<arch>.lock`; el archivo se descargara como `.postgres-<resource>.part` en el mismo directorio de cache, se verificara completo y se promovera con `ATOMIC_MOVE` (o fallback seguro) manteniendo el lock; nunca se ejecutara un archivo parcial.
- La matriz inicial publicada sera macOS ARM64/x64, Linux x64 y Windows x64. Las plataformas no soportadas fallaran antes de arrancar Spring.
- La primera ejecucion local requiere red salvo que el bundle ya exista en cache; los reinicios no deben requerirla.

## Implementation Tasks

### 1. Congelar el contrato de version y nombres de la release

- [x] **1.1 Sincronizar version `0.19.0` entre CLI, server y assets**
  Files: `pom.xml`, `replicadb-server/pom.xml`, `release.sh`, `README.md`, `DEPLOYMENT.md`, `RELEASE_GUIDE.md`, `CONTRIBUTING.md`, `replicadb-server/Dockerfile`.
  Changes: Cambiar la version del POM root y del POM server a `0.19.0`; actualizar la dependencia sibling `org.replicadb:ReplicaDB` a `0.19.0`; sustituir nombres de ejemplo `0.18.4`/`0.1.0-SNAPSHOT` donde representen la release; hacer que `release.sh` actualice ambos POM y sus referencias sin cambiar versiones de dependencias externas. Definir los nombres exactos `ReplicaDB-0.19.0.*`, `ReplicaDB-server-0.19.0.*` y `replicadb-server-0.19.0.jar`.
  Tests: Parsear ambos POM y comprobar que la version del proyecto root, la version del proyecto server, la dependencia CLI y los nombres documentados coinciden en `0.19.0`; ejecutar el script con argumentos ausentes/invalidos sin mutar archivos; inspeccionar que no queda `0.1.0-SNAPSHOT` en rutas de release; validar `bash -n release.sh`.
  Dependencies: None.

### 2. Separar el home persistente del server de la CLI

- [x] **2.1 Migrar la configuracion local a `REPLICADB_SERVER_HOME`**
  Files: `replicadb-server/src/main/java/org/replicadb/server/local/EmbeddedPostgresProperties.java`, `replicadb-server/src/main/java/org/replicadb/server/local/EmbeddedPostgresHome.java`, `replicadb-server/src/main/java/org/replicadb/server/local/LocalMasterKeyBootstrap.java`, `replicadb-server/src/main/java/org/replicadb/server/local/EmbeddedPostgresLaunchOptions.java`, `replicadb-server/src/test/java/org/replicadb/server/local/EmbeddedPostgresPropertiesTest.java`, `replicadb-server/src/test/java/org/replicadb/server/local/LocalMasterKeyBootstrapTest.java`, `replicadb-server/src/test/java/org/replicadb/server/local/EmbeddedPostgresServerStartupTest.java`.
  Changes: Reemplazar el contrato publico del server `REPLICADB_HOME`/`replicadb.home` por `REPLICADB_SERVER_HOME`/`replicadb.server.home`, manteniendo `~/.replicadb` como default; derivar bajo esa raiz `data/postgresql`, `cache/postgresql`, `security/master-key.json`, `locks`, `run` y `logs`; documentar o rechazar explicitamente el uso accidental de `REPLICADB_HOME` en el modo server. Mantener `REPLICADB_HOME` sin cambios en `bin/configure-replicadb` y en la CLI.
  Tests: Override por variable y propiedad JVM; default bajo `user.home`; rutas con espacios; coexistencia de una instalacion CLI bajo `REPLICADB_HOME`; validacion de permisos/no-escritura; keyring y datos se resuelven en el server home; una variable CLI no redirige el estado server; migracion de un home local anterior queda fuera de compatibilidad publica y produce diagnostico claro.
  Dependencies: 1.1.

### 3. Definir el contrato del daemon server

- [x] **3.1 Implementar el modelo comun de estado, PID y modos**
  Files: `replicadb-server/bin/replicadb-server` (nuevo), `replicadb-server/bin/replicadb-server.cmd` (nuevo), `replicadb-server/scripts/server-launcher.test.sh` (nuevo), `replicadb-server/src/main/java/org/replicadb/server/local/EmbeddedPostgresProperties.java` si se requiere exponer `run`/`logs` como rutas oficiales.
  Changes: Definir el CLI `start local|api|worker`, `stop`, `status` y `help`; resolver la ubicacion del paquete respecto al launcher; usar `REPLICADB_SERVER_HOME` configurable; crear de forma atomica `run/server.pid`, `run/server.mode` y `run/server.lock`; escribir en `logs/server.log` y conservar como maximo un `server.log.1` al superar 10 MiB; almacenar el modo activo sin secretos; verificar proceso, JAR y home antes de `stop`; limpiar PID stale sin matar procesos ajenos; devolver codigos de salida estables para running/stopped/error/invalid mode. `start` sera background/daemon, comprobara la salud cada segundo durante 180 segundos y solo devolvera exito cuando el endpoint de aplicacion correspondiente responda 200; `api`/`local` usaran `/actuator/health` y `worker` usara `http://127.0.0.1:<management-port>/actuator/health`.
  Tests: Tabla de comandos validos/invalidos; start duplicado rechazado; PID inexistente/stale; PID que apunta a otro proceso; status cuando esta parado, arrancando o fallido; stop idempotente; timeout de readiness a 180 segundos y cleanup; rotacion de log al superar 10 MiB; home con espacios y permisos restringidos; ningun output contiene password, `DB_URL` o material de keyring; test shell no destructivo con procesos fake.
  Dependencies: 2.1.

### 4. Crear el launcher POSIX

- [x] **4.1 Añadir start/stop/status para macOS y Linux**
  Files: `replicadb-server/bin/replicadb-server`, `replicadb-server/scripts/server-launcher.test.sh`, `replicadb-server/src/test/java/org/replicadb/server/local/EmbeddedPostgresServerIT.java` si hay que adaptar la forma de arrancar el JAR empaquetado.
  Changes: Implementar `start` con `nohup`/redireccion de logs, deteccion de Java 17, classpath/JAR relativo al paquete, modo explicito y variables de entorno; `local` añadira el flag embedded y `api`/`worker` los perfiles externos; esperar `/actuator/health` o el management endpoint correcto sin considerar solo el puerto abierto; `stop` enviara SIGTERM y esperara el cierre gracioso antes de un fallback acotado; `status` verificara PID y modo. El prompt local usara `read -s`, no guardara la password y permitira variables preconfiguradas para automatizacion.
  Tests: `bash -n`; test shell del contrato de PID, lock, señales, log y timeout usando un `java` stub; seleccion de argumentos para `local`, `api` y `worker`; home con espacios y variables preconfiguradas; `status` informa running/stopped y rechaza PID ajeno; `stop` es idempotente y usa SIGTERM antes del fallback; `start api`/`worker` rechaza la ausencia de metadata externa con error accionable; un fallo de readiness no deja procesos huerfanos. El smoke real contra el archive server, incluyendo primer arranque, cache caliente, keyring, `status`, `stop` y postmaster, se ejecuta en las tareas 7.1 y 10.1 despues de crear el paquete.
  Dependencies: 3.1.

### 5. Crear el launcher Windows

- [x] **5.1 Añadir start/stop/status para Windows x64**
  Files: `replicadb-server/bin/replicadb-server.cmd`, `replicadb-server/scripts/server-launcher-windows.test.ps1` (nuevo), `.github/workflows/CT_Push.yml`, `.github/workflows/CI_Release.yml`.
  Changes: Implementar el mismo contrato de comandos con `java.exe`, `start /b` o PowerShell controlado, PID/log paths bajo `REPLICADB_SERVER_HOME`, comprobacion de identidad del proceso y parada con `taskkill` solo para el PID validado; usar `Read-Host -AsSecureString` o mecanismo equivalente para el prompt local oculto; no interpolar password en el command line ni en archivos de log. Mantener el comportamiento compatible con usuarios que ejecutan el `.cmd` desde rutas con espacios.
  Tests: Parseo/lint de los scripts PowerShell/CMD; contrato con `java.exe` stub para `help`, `status`, `start`, `stop`, PID, lock y timeout; prompt oculto o variables gestionadas; home con espacios; proceso que muere durante startup; PID stale y PID ajeno; comprobacion de que el archive Windows no contiene bundles PostgreSQL. El smoke real `start local` en `windows-latest` se ejecuta en la matriz de la tarea 10.1 una vez construido el archive.
  Dependencies: 3.1, 4.1.

### 6. Endurecer el bootstrap de credenciales del primer administrador

- [x] **6.1 Conectar prompts y secretos gestionados con el bootstrap existente**
  Files: `replicadb-server/src/main/java/org/replicadb/server/ReplicaDbServerApplication.java`, `replicadb-server/src/main/java/org/replicadb/server/local/EmbeddedPostgresLaunchOptions.java`, `replicadb-server/src/main/java/org/replicadb/server/security/execution/AdminBootstrapRunner.java`, `replicadb-server/src/test/java/org/replicadb/server/local/EmbeddedPostgresServerStartupTest.java`, `replicadb-server/src/test/java/org/replicadb/server/local/EmbeddedPostgresServerIT.java`, `replicadb-server/src/test/java/org/replicadb/server/security/execution/AdminBootstrapRunnerTest.java`.
  Changes: Definir la precedencia entre variables heredadas por el launcher, propiedades de Spring y prompt; permitir que el primer `start local` complete el bootstrap sin password en argumentos; mantener el override de bootstrap controlado solo para embedded; no pedir ni sobrescribir credenciales cuando ya existe un administrador; producir un error accionable si un primer arranque no interactivo carece de variables/secret file. Preservar la cookie HTTP local no segura exclusivamente en embedded y la configuracion segura de api/worker externos.
  Tests: Primer arranque interactivo/gestionado crea admin; reinicio no vuelve a pedir la password; variables explicitas ganan al prompt; ausencia de TTY falla sin bloquear; password nunca aparece en `ps`, logs, excepciones ni argumentos Spring; login/CSRF conserva la sesion; el modo embedded HTTP emite cookie local no segura y el perfil `api` externo conserva `secure=true` (el perfil worker no crea sesion de producto); un fallo de Flyway o Spring limpia el runtime PostgreSQL; perfil worker no crea bootstrap API.
  Dependencies: 4.1, 5.1.

### 7. Preparar el paquete server independiente

- [x] **7.1 Crear layout reproducible y script de empaquetado**
  Files: `scripts/package-server-release.sh` (nuevo), `replicadb-server/conf/replicadb-server.env.example` (nuevo), `replicadb-server/README.md` (nuevo), `replicadb-server/bin/replicadb-server`, `replicadb-server/bin/replicadb-server.cmd`, `LICENSE`, `replicadb-server/Dockerfile`.
  Changes: Crear el layout versionado `ReplicaDB-server-${version}/` con launchers, `lib/replicadb-server-${version}.jar`, `conf/replicadb-server.env.example`, README, LICENSE y `VERSION` en la raiz del paquete cuyo unico contenido sea `${version}\n`; aceptar version/JAR/output como inputs validados; generar TAR.GZ y ZIP deterministas; preservar permisos ejecutables POSIX; no incluir `postgres-*.txz`, `target/test-classes`, fuentes, Node, npm ni dependencias sueltas fuera del JAR; incluir un fichero de version/metadata del paquete y documentar el presupuesto de tamaño.
  Tests: Ejecutar el script en un directorio temporal con el JAR empaquetado; extraer ambos archives y comparar el listado y el contenido de `VERSION`; comprobar launchers ejecutables, rutas relativas, version correcta y ausencia de bundles PostgreSQL/secretos; repetir el build y verificar listado/metadata reproducible; ejecutar `help` desde una ruta con espacios; verificar que el archive funciona sin Maven, npm, Docker o PostgreSQL instalado. El arranque real `start local` se cubre en la matriz de plataforma de la tarea 10.1.
  Dependencies: 1.1, 4.1, 5.1, 6.1.

### 8. Alinear imagen y distribución Docker del server

- [x] **8.1 Hacer que Docker consuma el JAR versionado sin activar embedded**
  Files: `replicadb-server/Dockerfile`, `docker-compose.server.yml`, `scripts/phase3-image-smoke.sh`, `DEPLOYMENT.md`, `.github/workflows/CI_Release.yml`.
  Changes: Eliminar el nombre snapshot hardcodeado del `ARG SERVER_JAR`; aceptar `SERVER_JAR`/version desde el build; mantener `SPRING_PROFILES_ACTIVE=api` como default de imagen y PostgreSQL externo como requisito; pasar `-Dreplicadb.embedded-postgres.enabled=false` despues de `JAVA_OPTS` en el entrypoint para bloquear accidentalmente el modo embedded dentro de la imagen; asegurar que `worker` puede usar la misma imagen con `SPRING_PROFILES_ACTIVE=worker`; conservar usuario no-root, healthchecks, puertos y keyring montado. Actualizar Compose y el smoke para validar el artefacto exacto de la release.
  Tests: Build de imagen con `replicadb-server-0.19.0.jar`; smoke API con PostgreSQL externo y health; smoke worker sin endpoint de producto; Compose `config --quiet`; inspeccion de entrypoint para comprobar que embedded no se activa aunque exista `REPLICADB_EMBEDDED_POSTGRES_ENABLED=true`; prueba de que la imagen no depende de `REPLICADB_SERVER_HOME` ni de cache local del host.
  Dependencies: 1.1, 7.1.

### 9. Publicar assets y checksums en CI de release

- [x] **9.1 Extender `CI_Release.yml` para CLI, server y JAR**
  Files: `.github/workflows/CI_Release.yml`, `scripts/package-server-release.sh`, `release.sh`, `replicadb-server/pom.xml`, `pom.xml`.
  Changes: Construir root CLI e instalarlo antes del server; empaquetar server con el script; copiar el JAR directo; generar `SHA256SUMS` en formato GNU compatible `<64-hex>  <basename>` usando `sha256sum` en Ubuntu y convertir/verificar con `Get-FileHash` en Windows; dejar todos los assets CLI/server/JAR/checksums en un directorio staging validado para que la tarea 10.1 los publique; conservar publicación de imágenes Docker con tag versionado y `latest` solo en tag release. La publicación debe ser idempotente: si `v0.19.0` no existe se crea con todos los assets; si existe, se suben todos con reemplazo explícito solo después de recalcular `SHA256SUMS`; nunca se publica un subconjunto. Añadir validacion previa que rechace nombres/versiones incoherentes y no prepare assets si falta un archivo.
  Tests: Ejecutar el workflow de staging con una version fixture; verificar que todos los assets esperados existen y sus hashes recalculan en formato GNU; validar que el JAR directo es byte-identico al incluido en el archive server; comprobar que los archives no incluyen PostgreSQL nativo; probar fallo por JAR ausente, checksum incorrecto o version mismatch; simular rerun de staging y comprobar que el resultado es reproducible; validar YAML con parser y acciones con los paths reales. La publicacion se prueba en la tarea 10.1 despues de todos los runners.
  Dependencies: 1.1, 7.1, 8.1.

### 10. Añadir gates de plataforma y artifact readiness

- [x] **10.1 Validar instalación limpia en Linux, macOS y Windows**
  Files: `.github/workflows/CT_Push.yml`, `.github/workflows/CI_Release.yml`, `replicadb-server/pom.xml`, `replicadb-server/src/test/java/org/replicadb/server/local/EmbeddedPostgresPlatformIT.java` (nuevo), `replicadb-server/src/test/java/org/replicadb/server/local/EmbeddedPostgresServerIT.java`.
  Changes: Crear/ajustar la matriz `embedded-postgres` en ambos workflows, `.github/workflows/CT_Push.yml` para PR/push y `.github/workflows/CI_Release.yml` como job de validación previo a `publish`, en `ubuntu-latest`, `macos-14` y `windows-latest`, separada de Testcontainers y de suites largas; verificar al principio de cada runner `java -version`, la disponibilidad de las herramientas del launcher (`bash`/PowerShell, `taskkill` en Windows) y la ausencia de dependencia en Docker/PostgreSQL; ejecutar el profile Maven tagged contra el JAR/packaging real; validar bundles nativos por plataforma, cache caliente/fria, checksum, loopback, restart y cleanup; mantener Linux/macOS ARM/x64 mappings y Windows x64 explícitamente. Añadir el job `publish` de `CI_Release.yml` después de esta matriz y del staging de Task 9.1, con `needs` sobre todos los jobs de validacion y packaging; el job debe crear/subir todos los assets de una vez, usar `--clobber` solo en un rerun con la misma version y fallar si la release/tag pertenece a otra version. No hacer que la suite root de la CLI dependa de Docker-free embedded tests.
  Tests: En cada runner, extraer el paquete server, arrancar `local`, autenticar, crear/ejecutar un job PostgreSQL->PostgreSQL, reiniciar y verificar persistencia; `api`/`worker` externos permanecen cubiertos por sus suites actuales; comprobar version PostgreSQL, Flyway v21, Quartz y ausencia de procesos al terminar; ejecutar sin Docker y sin PostgreSQL instalado; comprobar que el profile no comparte `target/surefire`, `REPLICADB_SERVER_HOME`, puertos ni secretos con otros jobs; publicar solo plataforma/version/resultado, nunca rutas sensibles o secretos. Verificar que `publish` no se ejecuta si falla cualquier runner o staging, que la primera publicacion contiene el conjunto completo y que un rerun con la misma version reemplaza el conjunto completo sin crear otro tag.
  Dependencies: 7.1, 9.1.

### 11. Actualizar el quickstart de usuarios

- [x] **11.1 Reescribir README para separar CLI, local server y distribuido**
  Files: `README.md`, `replicadb-server/README.md`, `replicadb-server/frontend/README.develop.md`.
  Changes: Añadir una tabla de decision CLI/server; documentar descarga y extraccion de `ReplicaDB-server-0.19.0`; quickstart `bin/replicadb-server start local`, `status`, `stop`; explicar prompt de admin, `REPLICADB_SERVER_HOME`, primer arranque con/sin red, cache, keyring, backup y limite de 213 MB; documentar `start api`/`worker` solo con PostgreSQL externo; mantener `start-local.sh` como desarrollo efimero Docker y no como instalador durable. Sustituir comandos `java -jar` sin launcher donde representen el camino recomendado.
  Tests: Verificar que cada comando documentado coincide con el layout generado; prueba manual/automatizada del quickstart desde archive extraido; revisar links, version `0.19.0`, nombres de assets y ausencia de credenciales resueltas; comprobar que la guia frontend conserva sus instrucciones Docker/Vite y describe correctamente su naturaleza efimera; documentar que usuarios CLI `0.18.x` conservan su `REPLICADB_HOME` y que el server usa `REPLICADB_SERVER_HOME` sin mover automaticamente archivos CLI.
  Dependencies: 7.1, 9.1, 10.1.

### 12. Actualizar deployment, release y contribucion

- [x] **12.1 Alinear documentacion operativa y de mantenedores**
  Files: `DEPLOYMENT.md`, `RELEASE_GUIDE.md`, `CONTRIBUTING.md`, `PRODUCT.md`, `ARCHITECTURE_DECISIONS.md` solo si alguna afirmacion de distribucion/version queda obsoleta.
  Changes: `DEPLOYMENT.md` debe distinguir paquete local embedded de imagen/Compose externo y documentar secrets, TLS, backups, upgrade del bundle y workers; `RELEASE_GUIDE.md` debe describir version unificada, build order, assets, checksums, matriz CI, smoke post-release, politica de rerun sin assets parciales y el paso manual de tag; `CONTRIBUTING.md` debe eliminar la afirmacion de server skeleton/no apto y explicar los dos builds sibling, perfiles y tests; `PRODUCT.md` debe reflejar que el server ya es una superficie distribuible sin inventar estabilidad o benchmarks no demostrados. La documentacion de upgrade debe indicar que no se migra automaticamente un home experimental anterior basado en `REPLICADB_HOME`: se conserva como backup y se configura un `REPLICADB_SERVER_HOME` nuevo o una migracion manual verificada.
  Tests: Ejecutar `scripts/check-phase3-docs.sh`; buscar y corregir referencias al skeleton, `0.1.0-SNAPSHOT`, solo-CLI release o instrucciones que requieran Docker para el modo local; validar que no se documentan passwords, tokens, DSNs ni keyring material; comprobar enlaces y comandos contra el paquete de prueba; test documental que exige un error claro cuando se detecta un antiguo home server en `REPLICADB_HOME` y no se hace migracion automatica.
  Dependencies: 1.1, 8.1, 9.1, 11.1.

### 13. Incorporar la guia server al sitio de documentacion

- [x] **13.1 Publicar instalacion y operaciones del server en docs**
  Files: `docs/index.md`, `docs/server.md` (nuevo), `docs/docs/docs.md`, `docs/_includes/navigation.html` si el tema requiere una entrada explicita.
  Changes: Añadir pagina server con matriz CLI/local/distributed, descarga `v0.19.0`, launchers, modos, requisitos Java 17, plataformas soportadas, home persistente, cache PostgreSQL, keyring, troubleshooting de permisos/puertos/red/PID, backup/restore y limites; enlazarla desde el indice y mantener el contenido CLI existente sin mezclar contratos.
  Tests: Build/validacion Jekyll si esta disponible; comprobar anchors/links y version; revisar que el sitio distingue `REPLICADB_HOME` de `REPLICADB_SERVER_HOME`; verificar que ejemplos usan placeholders y no valores sensibles; comprobar que `start-local.sh` aparece solo como desarrollo efimero.
  Dependencies: 11.1, 12.1.

### 14. Ejecutar el release candidate y preparar la publicacion

- [x] **14.1 Cerrar el gate de `v0.19.0` sin crear aun el tag**
  Files: `pom.xml`, `replicadb-server/pom.xml`, `scripts/package-server-release.sh`, `release.sh`, `.github/workflows/CI_Release.yml`, `.github/workflows/CT_Push.yml`, `README.md`, `DEPLOYMENT.md`, `RELEASE_GUIDE.md`, `CONTRIBUTING.md`, `docs/index.md`, `docs/server.md`.
  Changes: Ejecutar la secuencia root install -> server package -> server archive -> checksums; inspeccionar Main-Class, Start-Class, dependencias, tamaño y listado; verificar los caminos CLI, server local, api externo, worker externo e imagen Docker; confirmar que `start-local.sh` no cambió funcionalmente; dejar un checklist reproducible para que el mantenedor ejecute `./release.sh 0.19.0` solo después de limpiar el working tree. El release job solo publicara desde el staging validado y debe detenerse antes de cualquier upload si falla un checksum, smoke, runner de plataforma o build; para un rerun con tag/release ya existente, la guia debe usar el procedimiento idempotente definido en Task 9.1 y no borrar tags automaticamente.
  Tests: `mvn -B test` root o gates focalizados disponibles; suite server no embebida; profile `-Pembedded-postgres`; build de ambos archives; smoke de instalación limpia; checksum verification; `git diff --check`; parser YAML; documentación checks; inspección del JAR CLI para confirmar ausencia de Spring Boot; comprobación de que todos los procesos PostgreSQL/Java de prueba terminan; `git diff --exit-code -- replicadb-server/frontend/scripts/start-local.sh` y `bash -n replicadb-server/frontend/scripts/start-local.sh` como guard automatizado de que el harness efimero no fue modificado. Registrar como bloqueo cualquier fallo preexistente de infraestructura separado de regresiones del release.
  Dependencies: Tasks 1.1 a 13.1.

## Technical Reference

### Artifact and version matrix

| Surface | Release output | PostgreSQL metadata |
|---|---|---|
| CLI | `ReplicaDB-0.19.0.tar.gz/.zip` | None |
| Server local | `ReplicaDB-server-0.19.0.tar.gz/.zip` | Native bundle acquired on first local start |
| Server advanced | `replicadb-server-0.19.0.jar` | External or embedded according to flag |
| Container | `osalvador/replicadb-server:0.19.0` | External PostgreSQL via env/Compose |

The server JAR may remain approximately 213 MB because the core artifact
contains broad connector/runtime dependencies. The first release does not
split those drivers. Native PostgreSQL bundles are never included in the
server archive or JAR; their Maven Central artifact coordinates, resource
names and SHA-256 values remain pinned by the existing distribution manifest.

### Launcher contract

```text
replicadb-server start local
replicadb-server start api
replicadb-server start worker
replicadb-server status
replicadb-server stop
```

`local` is the recommended single-node installation. `api` requires
`DB_URL`, `DB_USERNAME`, `DB_PASSWORD` and the external keyring/bootstrap
configuration. `worker` requires the same metadata PostgreSQL and a unique
worker identity; it exposes only its internal management endpoint. No mode is
inferred from the presence or absence of environment variables.

### Existing implementation boundaries

- `ReplicaDbServerApplication` remains the executable Spring Boot entrypoint.
- `EmbeddedPostgresRuntimeFactory` and `PostgresDistributionManager` remain the PostgreSQL lifecycle/cache boundary.
- `LocalMasterKeyBootstrap` remains responsible for local keyring creation; the launcher only supplies bootstrap credentials and paths.
- `JobExecutionService` continues to call `ReplicaDB.processReplica(options)` directly.
- `start-local.sh` continues to own Docker-based disposable fixtures and is not repurposed as the release launcher.

### Testing strategy

- Shell contract tests for POSIX launcher and PowerShell/CMD smoke tests for Windows.
- JUnit unit tests for mode/config/PID/secret precedence and process identity seams.
- Real embedded PostgreSQL tests for cache, JDBC, Flyway, Quartz, authentication, job execution and restart.
- Existing Testcontainers tests remain the evidence for distributed PostgreSQL claims, `SKIP LOCKED`, leases, fencing, Quartz clustering and worker dispatch.
- Artifact tests must execute the packaged JAR, not only `target/classes`; package freshness is a release gate.
- Platform CI jobs must be isolated from long-running Testcontainers/Maven forks and must not share `target/surefire`, homes, ports or secrets.
- Release logs and CI output expose only version/platform/status; never resolved credentials, URLs with passwords, tokens or keyring material.

### Acceptance Gate

The first server release is ready only when:

1. `v0.19.0` is consistent across root POM, server POM, CLI/server assets, JAR, Docker tags and examples.
2. A user can download either the standalone CLI or the independent server package.
3. The server package installs without Maven, npm, Docker or system PostgreSQL.
4. `start local` downloads PostgreSQL only on first use, verifies it, persists it under `REPLICADB_SERVER_HOME`, and restarts without network when the cache is warm.
5. `start`, `stop` and `status` work on supported POSIX and Windows platforms with stale PID/process protection.
6. First-admin bootstrap is interactive-safe or secret-manager compatible and never leaks passwords.
7. The server archive and JAR contain no PostgreSQL native bundle.
8. The direct JAR, local package, external `api`, external `worker`, image and Compose paths are independently smoke-tested.
9. The release page contains CLI archives, server archives, direct JAR and `SHA256SUMS`.
10. README, deployment, release, contributor and website documentation describe the same install/runtime contracts.
11. The existing CLI remains Spring-free and `start-local.sh` remains the disposable Docker development harness.


## Execution Retrospective (auto-generated by /itx-code)
### Plan Accuracy
- Tasks completed as planned: 14/14 (100%).
- Tasks that required plan adjustment: 4/14 (28.6%).
- Test loop iterations: 18 total.

### Gaps Encountered
- **7.1, Plan-to-Implementation**: macOS BSD tar differed from GNU tar. Added a normalized deterministic fallback.
- **4.1, Plan-to-Implementation**: launcher arguments before -jar were parsed as JVM options. Fixed argument ordering and added packaged smoke coverage.
- **6.1, Plan-to-Implementation**: managed bootstrap values were not propagated to Spring defaults in the testable launch seam. Added environment-aware defaults.
- **10.1, Plan-to-Implementation**: runtime verifies the complete Maven bundle JAR, not the inner txz resource. Kept whole-bundle pins and tested the actual cache boundary.

### Patterns Discovered
- Allowlisted packaging in scripts/package-server-release.sh.
- Server-home isolation in replicadb-server/src/main/java/org/replicadb/server/local/EmbeddedPostgresProperties.java.
- Platform checksum validation in replicadb-server/src/test/java/org/replicadb/server/local/EmbeddedPostgresPlatformIT.java.

### Validation Notes
- The Jekyll build could not run because required repository gems are not installed locally.
- The root CLI suite retained pre-existing DB2 and SQL Server container infrastructure failures; the server suite passed 413 tests and the embedded profile passed 7 tests.
