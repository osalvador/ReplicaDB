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
export DB_URL='jdbc:postgresql://localhost:5432/replicadb'
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
