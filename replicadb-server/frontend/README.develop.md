# ReplicaDB Frontend: desarrollo local

> **Estado:** este frontend está disponible únicamente para desarrollo local. Todavía no está publicado como producto, no forma parte de una release oficial y no debe utilizarse como despliegue de producción.

## Arquitectura local

Durante el desarrollo se ejecutan dos procesos:

```text
Browser :5173
    |
    +-- Vite dev server
          |
          +-- proxy /api/v1 y /v3/api-docs
                    |
                    v
             Spring Boot API :8080
                    |
                    v
             PostgreSQL metadata
```

El servidor Vite sirve la interfaz y reenvía las llamadas al API local. Las migraciones Flyway crean las tablas de metadata al arrancar el perfil `api`.

## Sistema visual

El control plane usa una **teal/rust identity** de ReplicaDB sobre **neutral surfaces**: el teal es la acción primaria y el rust la acción secundaria, con superficies elevadas y secciones enmarcadas para separar el trabajo operativo. Los estados semánticos, el foco visible, la escala tipográfica, la elevación y el ritmo de espaciado pertenecen al tema MUI compartido; los radios son contenidos y las tablas mantienen una densidad adecuada para escanear definiciones y ejecuciones.

La navegación de escritorio conserva el AppBar superior y el enlace de marca a `/`. No hay un **bottom navigation** por defecto: ReplicaDB es un control plane desktop-first y las acciones permanecen junto al contexto de cada pantalla. Esta es la **no-bottom-navigation decision** del producto. El layout sigue siendo usable en móvil mediante wrapping, grids responsivos y overflow local para tablas y logs.

El sistema adaptado M3 usa **restrained radius**, semantic states, foco visible y una escala tipográfica estable; no carga fuentes remotas ni introduce una paleta violeta.

## Requisitos

- Java 17.
- Node.js 22 o superior y npm 10.
- Docker o Podman para ejecutar PostgreSQL localmente.
- El repositorio clonado y situado en la raíz de ReplicaDB.

## Arranque limpio con un comando

Para el uso diario puedes lanzar todo el entorno con el script incluido. En
cada ejecución elimina el contenedor PostgreSQL de desarrollo anterior y sus
datos, instala el artefacto CLI, arranca el API, arranca Vite y espera a que
ambos servicios estén disponibles.

Desde la raíz del repositorio:

```bash
export REPLICADB_BOOTSTRAP_ADMIN_PASSWORD='<local-password>'
./replicadb-server/frontend/scripts/start-local.sh
```

El nombre de usuario opcional es `admin` por defecto. Puedes
personalizarlo sin guardar credenciales en el repositorio:

```bash
export REPLICADB_BOOTSTRAP_ADMIN_USERNAME='my-local-admin'
export REPLICADB_BOOTSTRAP_ADMIN_PASSWORD='<local-password>'
./replicadb-server/frontend/scripts/start-local.sh
```

Cuando el script termine de arrancar:

- Frontend: `http://localhost:5173`
- API: `http://localhost:8080`
- El script crea jobs de prueba para Oracle, MySQL, MariaDB, PostgreSQL, DB2 LUW,
  DB2 for i, SQLite, SQL Server, Denodo y File. Los fixtures cubren los modos
  `complete`, `complete-atomic` e `incremental`; no ejecutan ninguna replicación.

Pulsa `Ctrl+C` para detener API y Vite y eliminar el contenedor PostgreSQL.
El script requiere libres los puertos `5432`, `8080` y `5173`; no detiene
procesos ajenos que estén utilizando esos puertos. Para usar Podman en lugar
de Docker, define `CONTAINER_ENGINE=podman`.

## 1. Arrancar PostgreSQL

Ejemplo para desarrollo local con Docker:

```bash
docker run --name replicadb-dev-postgres \
  -e POSTGRES_DB=replicadb \
  -e POSTGRES_HOST_AUTH_METHOD=trust \
  -p 5432:5432 \
  -d postgres:16-alpine
```

`POSTGRES_HOST_AUTH_METHOD=trust` es solo una comodidad para este entorno local. No lo uses en un entorno compartido o productivo.

Si el contenedor ya existe, arráncalo con:

```bash
docker start replicadb-dev-postgres
```

## 2. Instalar el artefacto CLI

El módulo `replicadb-server` depende del artefacto CLI instalado en el repositorio Maven local. Desde la raíz del proyecto:

```bash
mvn install -DskipTests
```

Si ese comando ya se ejecutó y el artefacto local está actualizado, puedes continuar con el siguiente paso.

## 3. Arrancar el API Spring Boot

Abre un terminal nuevo en la raíz del repositorio y define las variables de desarrollo. No guardes la contraseña en el repositorio.

```bash
export DB_URL='<metadata-jdbc-url>'
export DB_USERNAME='postgres'
export DB_PASSWORD=''
export REPLICADB_BOOTSTRAP_ADMIN_USERNAME='frontend-admin'
export REPLICADB_BOOTSTRAP_ADMIN_PASSWORD='<local-password>'
```

Arranca el perfil `api`:

```bash
mvn -f replicadb-server/pom.xml spring-boot:run \
  -Dspring-boot.run.profiles=api \
  -Dskip.installnodenpm=true \
  -Dskip.npm=true
```

Los dos flags `skip` evitan que Maven intente construir el frontend durante este modo de desarrollo. El frontend se ejecuta en el terminal de Vite del paso siguiente.

Comprueba que el API está disponible:

```bash
curl http://localhost:8080/actuator/health
```

La respuesta debe indicar que el estado es `UP`. En el primer arranque, `AdminBootstrapRunner` crea el usuario administrador definido por `REPLICADB_BOOTSTRAP_ADMIN_USERNAME` y `REPLICADB_BOOTSTRAP_ADMIN_PASSWORD` si todavía no existe un administrador.

## 4. Arrancar Vite

En otro terminal, desde la raíz del repositorio:

```bash
cd replicadb-server/frontend
npm ci
npm run dev
```

`npm run dev` inicia Vite con **Vite HMR**, que actualiza los cambios del frontend en el navegador sin reiniciar el API Spring Boot ni PostgreSQL. Las ediciones limitadas a `replicadb-server/frontend` no requieren reiniciar el stack de API o metadata. Mantén el API en `http://localhost:8080` y el frontend en `http://localhost:5173`; el proxy de Vite conserva las rutas `/api/v1` y `/v3/api-docs`.

Abre:

```text
http://localhost:5173
```

El proxy de `vite.config.ts` reenvía automáticamente:

- `/api/v1` hacia `http://localhost:8080/api/v1`.
- `/v3/api-docs` hacia `http://localhost:8080/v3/api-docs`.

Inicia sesión con las credenciales de bootstrap del paso anterior. El cliente obtiene el token CSRF antes del login y mantiene la sesión mediante las cookies del API.

## Comandos frontend

Desde `replicadb-server/frontend`:

```bash
# Comprobación TypeScript
npm run typecheck

# Tests unitarios y de componentes
npm test

# Build Vite local
npm run build

# Servir el build generado para una comprobación manual
npm run preview
```

El build escribe temporalmente los assets en `replicadb-server/src/main/resources/static`, porque ese es el directorio que Spring Boot sirve en el artefacto `replicadb-server`.

## Regenerar tipos OpenAPI

Con el API arrancado en `http://localhost:8080`, regenera el schema TypeScript con:

```bash
npm run generate:api-types
```

Si el API usa otro puerto durante una comprobación local, define
`OPENAPI_SCHEMA_URL`, por ejemplo:

```bash
OPENAPI_SCHEMA_URL='http://localhost:8082/v3/api-docs' npm run generate:api-types
```

El comando actualiza `src/api/schema.ts`. Para comprobar el schema sin sobrescribir el archivo versionado:

```bash
OPENAPI_SCHEMA_OUTPUT=/tmp/replicadb-schema.ts npm run generate:api-types
diff -u src/api/schema.ts /tmp/replicadb-schema.ts
```

## Contrato de estado distribuido Phase 3.1

El perfil `api` persiste la política de recuperación de cada job:

- `maxAttempts` incluye el intento inicial y por defecto es `3`.
- `retryBackoffSeconds` es el retraso directo antes de que un retry sea elegible y por defecto es `60`.
- `automaticRetryEnabled` es verdadero por defecto para `incremental` y `complete-atomic`, y falso para `complete`. Un job `complete` puede activarlo explícitamente, pero conserva su advertencia destructiva.

Los runs elegibles usan `availableAt`, evaluado con el reloj PostgreSQL. Cada claim genera un lease token opaco; el token se usa solo dentro del estado y nunca aparece en respuestas REST, tipos TypeScript, logs o UI. Las renovaciones y escrituras terminales de un worker obsoleto son rechazadas por fencing. Una expiración conserva el run original y crea una nueva tentativa desde el principio cuando la política lo permite; no existe resume.

La cancelación se persiste antes de intentar la señal local. Esto permite que una futura instancia worker observe `CANCEL_REQUESTED` aunque la petición HTTP haya sido atendida por otra instancia. `LISTEN/NOTIFY`, polling distribuido, el perfil `worker`, el heartbeat durante merge/swap y Quartz JDBC clustering pertenecen a Phase 3.2 y 3.3; no forman parte del runtime actual.

## Ejecutar el smoke test E2E

El test necesita un API arrancado, PostgreSQL disponible y las mismas variables de bootstrap:

```bash
export PLAYWRIGHT_BASE_URL='http://localhost:8080'
export REPLICADB_BOOTSTRAP_ADMIN_USERNAME='frontend-admin'
export REPLICADB_BOOTSTRAP_ADMIN_PASSWORD='<local-password>'
npm run test:e2e
```

La configuración usa Chrome instalado localmente por defecto. En CI puede seleccionarse el navegador gestionado por Playwright con:

```bash
PLAYWRIGHT_CHANNEL=chromium npm run test:e2e
```

## Probar el artefacto empaquetado

Este modo no es necesario para el desarrollo diario, pero comprueba que los assets terminan dentro del jar:

```bash
mvn -f replicadb-server/pom.xml package -DskipTests
jar tf replicadb-server/target/replicadb-server-*.jar | grep 'BOOT-INF/classes/static/index.html'
```

Después arranca el jar con el perfil `api` y las variables `DB_URL`, `DB_USERNAME`, `DB_PASSWORD` y bootstrap definidas:

```bash
java -Dspring.profiles.active=api \
  -jar replicadb-server/target/replicadb-server-0.1.0-SNAPSHOT.jar
```

En este modo la interfaz se sirve desde el mismo proceso en `http://localhost:8080`.

## Detener el entorno

```bash
docker stop replicadb-dev-postgres
```

Para eliminar el contenedor local y sus datos:

```bash
docker rm replicadb-dev-postgres
```

No hay todavía una imagen, release ni URL pública oficial del frontend. Los nombres de endpoints, pantallas y configuración pueden cambiar mientras Phase 2b y Phase 2c sigan pendientes.
