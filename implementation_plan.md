# Implementation Plan: PostgreSQL embebido para el servidor local

## Task Source - user request

No hay ticket JIRA asociado. Requisitos acordados durante la exploracion:

- Ofrecer una instalacion local o monoservidor sin Docker ni PostgreSQL instalado por el usuario.
- Mantener PostgreSQL real como backend del control plane para no tocar repositorios, migraciones, Quartz, claims, leases ni la ejecucion del core.
- Reutilizar el perfil Spring `api`, con ejecucion local habilitada; no crear un perfil Spring nuevo ni arrancar workers en este modo.
- Mantener la CLI `replicadb` independiente y sin dependencias de Spring Boot o del control plane.
- Usar una libreria Java de PostgreSQL embebido para reducir codigo propio.
- Descargar o adquirir los binarios nativos bajo demanda y conservarlos en una cache local; el comportamiento exacto queda sujeto al gate de compatibilidad de la libreria.
- Persistir datos, binarios adquiridos y keyring en `~/.replicadb` por defecto, con una ubicacion configurable.
- Arrancar el runtime embebido antes de crear el contexto Spring mediante la `main()` existente y un flag dedicado.
- Mantener el arranque normal de `api` y `worker` con PostgreSQL externo sin cambios funcionales.

## Overview

El servidor gestionado ya contiene el core ReplicaDB y usa PostgreSQL como fuente durable de jobs, ejecuciones, usuarios, sesiones, schedules, Quartz y estado de coordinacion. El cambio anade un modo opt-in para que `ReplicaDbServerApplication` prepare una instancia PostgreSQL local mediante una libreria embebida antes de iniciar Spring; despues el mismo perfil `api` ejecuta Flyway y sirve la UI.

La instalacion local tendra un solo punto de entrada y conservara los datos entre reinicios. El modo distribuido seguira usando PostgreSQL externo y los perfiles `api`/`worker` actuales. No se anade SQLite, H2 ni una segunda implementacion de persistencia.

## Architecture & Design

**Approach: Libreria Java de PostgreSQL embebido con launcher integrado en la Main-Class existente.**

```text
java -jar replicadb-server.jar \
  --replicadb.embedded-postgres.enabled=true
                 |
                 v
ReplicaDbServerApplication.main()
                 |
                 +-- resuelve REPLICADB_HOME o ~/.replicadb
                 +-- obtiene/verifica binario PostgreSQL para la plataforma
                 +-- inicializa o reutiliza data/postgresql
                 +-- arranca PostgreSQL y espera readiness
                 +-- inyecta datasource local en Spring
                 +-- arranca Spring con perfil api
                 +-- registra apagado coordinado
```

El modo normal no cambia:

```text
java -jar replicadb-server.jar
  -> perfil api -> DB_URL externo

java -Dspring.profiles.active=worker -jar replicadb-server.jar
  -> perfil worker -> DB_URL externo
```

### Decisiones fijadas

- El flag de activacion sera `replicadb.embedded-postgres.enabled=true`; no se crea `application-embedded-postgres.yml`.
- El modo embebido fuerza el perfil `api` y `replicadb.server.local-execution.enabled=true` salvo que la implementacion rechace explicitamente una combinacion incompatible.
- `REPLICADB_HOME` sera la raiz configurable, con valor por defecto `~/.replicadb`. Se aceptara tambien la propiedad JVM equivalente `replicadb.home`, con precedencia documentada.
- La raiz tendra al menos `data/postgresql`, `cache/postgresql`, `security` y `locks`.
- Una configuracion externa de `DB_URL`, `DB_USERNAME` o `DB_PASSWORD` no se mezclara silenciosamente con el modo embebido: se rechazara con un error accionable o se documentara una precedencia unica antes de implementar.
- La instancia escuchara solo en loopback y usara un puerto local libre o configurado. La URL, usuario y password nunca se escribiran en logs.
- El keyring local sera persistente y se creara de forma atomica con permisos restrictivos solo cuando no exista una configuracion externa explicita.
- La libreria candidata `io.zonky.test:embedded-postgres` se evaluara como candidato, no como decision irrevocable. La version publicada 2.2.2 declara binarios runtime y una version de PostgreSQL 14.22, por lo que hay que validar compatibilidad con las migraciones actuales, Quartz y la matriz de plataformas antes de fijarla.
- La descarga o adquisicion de binarios debe tener integridad verificable, cache atomica, bloqueo entre procesos y errores claros sin red. Si la libreria solo funciona como dependencia de tests o no permite una politica aceptable de adquisicion, el gate falla antes de integrar produccion.

### Performance and security

- PostgreSQL local tendra limites pequenos de conexiones y recursos coherentes con una unica instancia `api`; no se optimizara para workers distribuidos.
- El proceso no se ejecutara como root ni usara una carpeta del repositorio para datos por defecto.
- La cache de binarios se separara de `data/postgresql`; una actualizacion no debe destruir el cluster existente.
- Las descargas se comprobaran contra un manifiesto versionado y SHA-256 o una verificacion equivalente proporcionada por la libreria; no se ejecutara un archivo descargado sin validacion.
- Los archivos temporales se crearan bajo `REPLICADB_HOME` con permisos restrictivos y se moveran de forma atomica.
- El launcher evitara imprimir URLs con credenciales, passwords, tokens, material del keyring o comandos completos.
- El apagado usara la API de la libreria o `pg_ctl` equivalente y esperara el cierre antes de cerrar Spring; un cierre anormal dejara el cluster reutilizable o fallara con diagnostico claro.

## Implementation Tasks

### 1. Validar la libreria y cerrar la matriz de distribucion

- [ ] **1.1 Ejecutar un spike de la libreria Java candidata**
  Files: `replicadb-server/pom.xml`, `replicadb-server/src/test/java/org/replicadb/server/local/EmbeddedPostgresLibraryProbeTest.java` (nuevo), `replicadb-server/src/test/resources/` si la libreria requiere fixture.
  Changes: Anadir la dependencia en un alcance controlado y probar su API real para extraer/obtener el binario, arrancar, obtener credenciales y detener PostgreSQL. Registrar la version efectiva de PostgreSQL, plataformas soportadas, estrategia de cache, comportamiento sin red, licencias, vulnerabilidades conocidas y si el uso es razonable fuera de tests. No integrar todavia el launcher con Spring.
  Tests: Prueba etiquetada `embedded-postgres` que arranque una instancia temporal, abra JDBC, ejecute `SELECT version()` y aplique todas las migraciones Flyway en un esquema temporal; crear despues las tablas Quartz V15 y verificar que el `PostgreSQLDelegate` puede inicializar el scheduler. Probar detencion y segundo arranque reutilizando la cache, plataforma sin binario y cache sin red. El gate pasa solo si la libreria no requiere Docker, tiene una licencia aceptable, permite verificar/adquirir sus binarios, cubre las plataformas publicadas y es compatible con el esquema/migraciones actuales. Si falla, se bloquean las tareas posteriores y se debe sustituir la libreria candidata o revisar explicitamente el alcance antes de continuar.
  Dependencies: Ninguna.

### 2. Definir el contrato de configuracion local

- [ ] **2.1 Crear propiedades y resolucion de ubicaciones para el runtime local**
  Files: `replicadb-server/src/main/java/org/replicadb/server/local/EmbeddedPostgresProperties.java` (nuevo), `replicadb-server/src/main/java/org/replicadb/server/local/EmbeddedPostgresHome.java` (nuevo), `replicadb-server/src/test/java/org/replicadb/server/local/EmbeddedPostgresPropertiesTest.java` (nuevo).
  Changes: Definir el flag de activacion, `REPLICADB_HOME`/`replicadb.home`, version de PostgreSQL, puerto opcional, timeout de readiness, timeout de descarga y numero maximo de reintentos. Resolver rutas absolutas de `data`, `cache`, `security` y `locks`; rechazar rutas vacias, no escribibles o ambiguas. Mantener el contrato separado de `replicadb.server.local-execution.enabled` y `local-seeding.enabled`. Fijar que una configuracion externa de `DB_URL`, `DB_USERNAME` o `DB_PASSWORD` junto al flag embebido se rechaza antes de Spring con una excepcion de configuracion estable y un mensaje sin valores sensibles.
  Tests: Valores por defecto en `~/.replicadb`; override por propiedad JVM y variable de entorno, incluida la conversion `REPLICADB_EMBEDDED_POSTGRES_ENABLED` -> `replicadb.embedded-postgres.enabled`; precedencia estable; rutas con espacios; puerto, timeout, reintentos invalidos; directorio no escribible; conflicto de cada una de las tres propiedades externas de metadata; ausencia de conflicto cuando el flag no esta activo.
  Dependencies: Task 1.1.

### 3. Integrar la dependencia sin alterar los artefactos existentes

- [ ] **3.1 Fijar la dependencia y preservar el executable JAR actual**
  Files: `replicadb-server/pom.xml`, `replicadb-server/src/main/java/org/replicadb/server/ReplicaDbServerApplication.java`, `replicadb-server/src/test/java/org/replicadb/server/ReplicaDbServerApplicationTest.java` (nuevo si no existe).
  Changes: Fijar la version aprobada por el spike, sus exclusiones y scopes; resolver de forma explicita cualquier duplicado de PostgreSQL JDBC, Flyway, commons o logging manteniendo las versiones gestionadas por el servidor, y fallar el build ante convergencia no resuelta. Configurar de forma explicita la Main-Class del plugin Spring Boot para que el JAR normal siga arrancando `ReplicaDbServerApplication`. No anadir la dependencia ni el contexto Spring al artefacto root `replicadb`.
  Tests: Ejecutar `mvn dependency:tree` y la comprobacion de convergencia esperando una unica version de PostgreSQL JDBC/Flyway/commons relevante; comprobacion de que el root CLI sigue sin Spring Boot; `java -jar` normal continua usando el entrypoint actual; el JAR server contiene las clases necesarias para el modo embebido sin cambiar el comportamiento del perfil `worker`.
  Dependencies: Task 1.1.

### 4. Implementar adquisicion segura y cache del binario

- [ ] **4.1 Encapsular la adquisicion multiplataforma de PostgreSQL**
  Files: `replicadb-server/src/main/java/org/replicadb/server/local/PostgresDistribution.java` (nuevo), `replicadb-server/src/main/java/org/replicadb/server/local/PostgresDistributionManager.java` (nuevo), `replicadb-server/src/main/java/org/replicadb/server/local/PostgresDistributionManifest.java` (nuevo), `replicadb-server/src/test/java/org/replicadb/server/local/PostgresDistributionManagerTest.java` (nuevo).
  Changes: Adaptar la API de la libreria para cachear por version, sistema operativo y arquitectura; usar descarga/adquisicion temporal seguida de checksum y movimiento atomico; mantener un lock de adquisicion para dos arranques simultaneos; distinguir cache ausente, corrupta, incompleta y plataforma no soportada. Las descargas tendran connect/read timeout y un numero maximo de reintentos configurables; un fallo permanente limpia el temporal y devuelve un error accionable. Si la libreria ya implementa alguna garantia, envolverla sin duplicarla y probar la garantia observable.
  Tests: Cache hit sin red; descarga valida; timeout de conexion, timeout de lectura, DNS/fallo HTTP y agotamiento de reintentos; checksum incorrecto; archivo truncado; interrupcion simulada antes y despues de verificar el checksum sin cache parcial; dos procesos solicitando la misma plataforma; ausencia de red con cache presente; ausencia de red sin cache; version o arquitectura no soportada. Verificar que no quedan temporales ni archivos parciales despues de un crash simulado y que ningun error incluye secretos o URLs con credenciales.
  Dependencies: Tasks 1.1 y 2.1.

### 5. Implementar el cluster local persistente y su ciclo de vida

- [ ] **5.1 Crear el adaptador de runtime PostgreSQL**
  Files: `replicadb-server/src/main/java/org/replicadb/server/local/EmbeddedPostgresRuntime.java` (nuevo), `replicadb-server/src/main/java/org/replicadb/server/local/EmbeddedPostgresRuntimeFactory.java` (nuevo), `replicadb-server/src/test/java/org/replicadb/server/local/EmbeddedPostgresRuntimeTest.java` (nuevo).
  Changes: Exponer una abstraccion pequena para `start`, `jdbcUrl`, `username`, `password` y `close`; inicializar `data/postgresql` solo en la primera ejecucion; reutilizar un cluster valido; configurar loopback, puerto, locale y limites locales; esperar readiness con timeout acotado; tratar un cluster bloqueado o de version incompatible como error recuperable con instrucciones. `close()` debe intentar una parada ordenada, esperar el timeout de apagado, ser idempotente y, si la libreria permite identificarlo, destruir solo el proceso hijo que este runtime haya arrancado. Un fallo de apagado se registra como error operativo sin borrar el data directory. No ejecutar migraciones desde este adaptador.
  Tests: Primer arranque crea el cluster y acepta JDBC; segundo arranque reutiliza datos; puerto ocupado produce diagnostico; timeout y proceso muerto producen error; cierre normal libera el lock; cierre repetido es idempotente; un directorio de datos corrupto no se sobrescribe; `SELECT version()` y una tabla persistente sobreviven a stop/start; fallo de Spring despues del arranque ejecuta `close()` y no deja un proceso hijo ni un lock activo; cierre con proceso bloqueado respeta timeout y no borra datos.
  Dependencies: Tasks 2.1 y 4.1.

### 6. Crear o resolver el keyring persistente local

- [ ] **6.1 Integrar el keyring local con la configuracion de seguridad existente**
  Files: `replicadb-server/src/main/java/org/replicadb/server/local/LocalMasterKeyBootstrap.java` (nuevo), `replicadb-server/src/main/java/org/replicadb/server/security/secret/SecretProtectionProperties.java`, `replicadb-server/src/main/java/org/replicadb/server/security/config/SecretProtectionConfiguration.java`, `replicadb-server/src/test/java/org/replicadb/server/local/LocalMasterKeyBootstrapTest.java` (nuevo).
  Changes: Cuando se active el modo embebido y no haya `REPLICADB_SECURITY_MASTER_KEY_FILE` explicito, usar un keyring bajo `REPLICADB_HOME/security`; generar una clave AES-256 con `SecureRandom`, escribir JSON atomico con permisos restrictivos y pasar su ruta a Spring. Respetar un keyring existente, rechazarlo si es invalido y no generar uno nuevo encima. Mantener intacto el proveedor file-backed y el contrato de rotacion.
  Tests: Creacion inicial; verificar que la clave tiene 256 bits, algoritmo AES y no es una constante; reinicio conserva la misma clave y permite descifrar un bundle; archivo existente valido; JSON malformado; clave no AES-256; permisos o directorio no escribible; variable externa explicita tiene precedencia; logs y excepciones no contienen material de clave.
  Dependencies: Task 2.1.

### 7. Integrar el arranque antes de Spring

- [ ] **7.1 Añadir el flag pre-contexto a la Main-Class existente**
  Files: `replicadb-server/src/main/java/org/replicadb/server/ReplicaDbServerApplication.java`, `replicadb-server/src/main/java/org/replicadb/server/local/EmbeddedPostgresLaunchOptions.java` (nuevo), `replicadb-server/src/test/java/org/replicadb/server/local/EmbeddedPostgresServerStartupTest.java` (nuevo).
  Changes: Detectar `--replicadb.embedded-postgres.enabled=true` antes de `SpringApplication.run`; resolver propiedades, keyring y runtime; validar conflictos de `spring.profiles.active`/`DB_URL`/`DB_USERNAME`/`DB_PASSWORD` y lanzar una excepcion de configuracion antes de crear el contexto. Inyectar `spring.datasource.url`, `spring.datasource.username`, `spring.datasource.password`, `spring.profiles.active=api`, `replicadb.server.local-execution.enabled=true` y `replicadb.security.master-key-file` mediante `SpringApplication.setDefaultProperties(...)` o un mapa equivalente pasado a `SpringApplication`, con precedencia y mecanismo documentados; no usar mutacion global de `System` properties como canal principal. Registrar un shutdown hook y un bloque de cleanup que cierre el contexto y PostgreSQL en orden incluso si falla el refresco del contexto. El camino sin flag debe ser byte a byte equivalente en sus decisiones de perfil y datasource.
  Tests: Flag arranca Spring con PostgreSQL embebido y Flyway; el health endpoint queda disponible; un job local puede ejecutarse sin worker; arranque normal con `DB_URL` externo sigue funcionando; perfil `worker` externo sigue funcionando; flag combinado con `--spring.profiles.active=worker`, `DB_URL`, `DB_USERNAME` o `DB_PASSWORD` conflictivo falla en el launcher antes de llamar a `SpringApplication.run`; se verifica el mensaje de error sin valores sensibles; excepcion durante Spring cierra PostgreSQL; Ctrl+C/close libera el proceso.
  Dependencies: Tasks 5.1 y 6.1.

### 8. Validar la integracion real con el servidor gestionado

- [ ] **8.1 Probar migraciones, autenticacion, schedules y ejecucion de replicas con el runtime embebido**
  Files: `replicadb-server/src/test/java/org/replicadb/server/local/EmbeddedPostgresServerIT.java` (nuevo), `replicadb-server/src/test/java/org/replicadb/server/config/PostgresTestcontainersConfig.java` solo si se necesita extraer una utilidad comun, `replicadb-server/src/test/resources/**` para fixtures aislados.
  Changes: Levantar el launcher con un `REPLICADB_HOME` temporal, permitir que Flyway migre el esquema real y ejercitar el mismo flujo que usa el servidor: bootstrap de admin, login/CSRF, alta de datasource, creacion de job, trigger, ejecucion `ReplicaDB.processReplica` y lectura de estado. Verificar que Quartz JDBC, sesiones, leases, watermarks y cifrado usan el PostgreSQL embebido sin adaptaciones de dialecto.
  Tests: Flujo HTTP autenticado completo con una replica PostgreSQL a PostgreSQL en un runner donde no se usa Docker ni una instalacion PostgreSQL del sistema; reinicio del servidor conserva usuarios, jobs, historial y datasources cifrados; schedule persistido sobrevive al reinicio; migracion desde un data directory vacio; fallo de Flyway no deja el runtime abierto; dos arranques simultaneos del mismo `REPLICADB_HOME` dejan uno rechazado limpiamente. El test debe usar PostgreSQL embebido real para este slice y mantener los tests Testcontainers existentes para claims/distribucion; la limitacion de que la prueba se ejecuta en una maquina limpia se documentara en el job CI de plataforma.
  Dependencies: Tasks 5.1, 6.1 y 7.1.

### 9. Añadir pruebas de compatibilidad de plataforma y operaciones

- [ ] **9.1 Incorporar una matriz CI para binarios y ciclo de vida**
  Files: `.github/workflows/CT_Push.yml`, `.github/workflows/CI_Release.yml`, `replicadb-server/pom.xml`, `replicadb-server/src/test/java/org/replicadb/server/local/EmbeddedPostgresPlatformIT.java` (nuevo).
  Changes: Añadir un perfil Maven `embedded-postgres` que active una ejecucion Surefire dedicada para tests JUnit con `@Tag("embedded-postgres")`; el job CI invocara ese perfil con seleccion explicita de clases y no compartira `REPLICADB_HOME`, puertos ni directorios temporales con otros jobs. Seleccionar runners disponibles para Linux x64 y macOS/Windows cuando la infraestructura lo permita; publicar el resultado de version, plataforma y cache sin imprimir rutas sensibles. Mantener los gates actuales de Testcontainers y packaging.
  Tests: Compilacion y test del JAR server; smoke de primer arranque y reinicio en cada runner soportado; cache sin red despues de la primera adquisicion; plataforma no soportada falla con diagnostico; ejecucion paralela de suites confirma aislamiento de home/puerto; comprobacion de que el artefacto CLI root sigue sin Spring Boot; prueba de dependencias enumera plataformas incluidas y falla si se excede el conjunto objetivo.
  Dependencies: Tasks 3.1 y 8.1.

### 10. Definir comandos de uso, backup y recuperacion

- [ ] **10.1 Documentar el contrato operativo de la instalacion local**
  Files: `README.md`, `DEPLOYMENT.md`, `replicadb-server/README.develop.md`, `replicadb-server/Dockerfile` solo si el empaquetado final requiere ajustar el entrypoint de la imagen (no activar embedded por defecto).
  Changes: Documentar el comando de arranque con flag, `REPLICADB_HOME`, primer arranque con y sin red, ubicacion de datos/cache/keyring, parada, backup consistente de PostgreSQL y keyring, restauracion, actualizacion de la libreria/binario y limites del modo local. Explicar que `api` local no crea workers y que Docker/Compose siguen siendo la ruta distribuida. No incluir credenciales reales ni valores secretos.
  Tests: Comprobar los comandos documentados contra un JAR empaquetado; smoke de start/stop/restart y backup/restore en un directorio temporal; revisar que la documentacion no presenta SQLite/H2 ni el modo local como soporte distribuido; validacion de enlaces/rutas y del listado de variables de entorno.
  Dependencies: Tasks 7.1, 8.1 y 9.1.

### 11. Gate final de release

- [ ] **11.1 Verificar que la nueva capacidad no rompe los dos artefactos actuales**
  Files: `pom.xml`, `replicadb-server/pom.xml`, `.github/workflows/CT_Push.yml`, `.github/workflows/CI_Release.yml`, `replicadb-server/target/` como salida de build, sin versionar.
  Changes: Ejecutar la secuencia completa de build CLI -> install local -> package server; comprobar Main-Class, dependencias runtime, listado de binarios soportados y arranque de los tres caminos: CLI, server api externo y server worker externo, mas api embebido. Fijar un presupuesto de crecimiento del JAR en el spike (incluyendo dependencias Java, pero no cache descargada) y hacer que el gate falle si se supera ese valor sin decision aprobada. Confirmar que no se modifican migraciones ni contratos REST/OpenAPI.
  Tests: `mvn -B test` del root; tests server focalizados y suite de persistencia existente; `mvn -B package -DskipTests` server; smoke HTTP de health; smoke de una replica local; `git diff --check`; inspeccion de dependencias para asegurar que Spring Boot no entra en el artefacto CLI; comparacion automatizada del listado de migraciones y del schema OpenAPI generado contra el estado base, permitiendo solo diferencias vacias; comprobacion del presupuesto de tamaño definido en Task 1.1. El gate no pasa si el modo normal necesita la cache embebida o si un error de descarga deja un proceso PostgreSQL huerfano.
  Dependencies: Tasks 1.1 a 10.1.

## Technical Reference

### Types & Data Structures

- `EmbeddedPostgresProperties`: configuracion opt-in, home, version, puerto y timeouts.
- `EmbeddedPostgresHome`: rutas derivadas y validaciones de permisos.
- `PostgresDistributionManifest`: version, SO, arquitectura, URL o identificador de la libreria, checksum y formato.
- `PostgresDistributionManager`: cache, integridad, locks y adquisicion.
- `EmbeddedPostgresRuntime`: ciclo de vida y credenciales efimeras del proceso local.
- `LocalMasterKeyBootstrap`: creacion/resolucion atomica del keyring persistente.
- `EmbeddedPostgresLaunchOptions`: parseo minimo de argumentos antes del contexto Spring.

No se deben añadir campos de lease, estado de run ni credenciales al modelo REST. El runtime local es infraestructura de arranque y no una nueva fuente de verdad.

### Dependency and Version Strategy

- Mantener PostgreSQL JDBC, Flyway, Quartz y las migraciones del servidor existentes.
- Fijar la libreria embebida solo despues del spike de Task 1.1.
- Verificar especialmente la diferencia entre la version PostgreSQL incluida por la libreria candidata y `postgres:16-alpine`, que es la referencia actual de desarrollo/CI.
- Mantener el artefacto root `replicadb` sin Spring Boot ni dependencia del runtime embebido.
- No incluir indiscriminadamente binarios de todas las plataformas si la estrategia elegida es adquisicion bajo demanda.

### Testing Strategy

- Unit tests con JUnit Jupiter 6 y Mockito solo para seams de filesystem, locks, manifest y lifecycle.
- Un test de integracion real del runtime embebido para proceso, JDBC, Flyway y reinicio.
- Mantener Testcontainers como evidencia de dialecto PostgreSQL, concurrencia, `FOR UPDATE SKIP LOCKED`, Quartz cluster y dispatch distribuido; no sustituir esos tests por mocks ni por el runtime local.
- Ejecutar la suite embebida en un perfil/tag explicito porque requiere procesos nativos y puede necesitar red solo en la primera adquisicion.
- Probar siempre cache caliente y cache fria, con red y sin red, y los caminos de corrupcion/recuperacion.
- Mantener todos los logs de pruebas libres de passwords, URLs con credenciales, tokens y material del keyring.

### Acceptance Gate

La implementacion estara lista cuando:

1. Un usuario pueda arrancar el JAR server con un flag, sin Docker ni PostgreSQL instalado.
2. La primera adquisicion de binarios sea verificable y el segundo arranque no requiera red.
3. `~/.replicadb` o su override conserve cluster, cache y keyring entre reinicios.
4. Flyway, Quartz, sesiones, seguridad, jobs y ejecucion local funcionen contra PostgreSQL embebido real.
5. Los arranques normales `api`/`worker` con PostgreSQL externo no cambien.
6. La CLI root siga siendo independiente y ejecutable sin metadata PostgreSQL.
7. Los fallos de plataforma, red, checksum, permisos, lock, corrupcion y apagado tengan diagnosticos accionables y no filtren secretos.
8. La matriz CI cubra las plataformas publicadas y el artefacto documente claramente las no soportadas.
