# Plan de edición de la documentación de ReplicaDB

**Objetivo.** Que un lector pueda explicar qué hace ReplicaDB, elegir CLI o Server, verificar su primera transferencia y predecir qué hará cada modo con el destino.

**Enfoque.** La portada, los quickstarts y la guía de modos compartirán un caso de replicación de pedidos. La documentación seguirá en inglés y el plan se irá refinando en español.

**Base técnica.** El portal usa Astro Starlight, contenido Markdown y MDX, y referencia de API generada desde OpenAPI. La primera entrega usará PostgreSQL como conector de origen y destino.

## Alcance editorial

El inventario inicial cubrió 67 páginas editoriales publicadas y una muestra de la API generada. La portada repite parte de la elección entre CLI y Server. Los quickstarts requieren que el lector complete la configuración de datos por su cuenta. La guía de modos explica el flujo con diagramas, pero carece de filas que permitan seguir el resultado de cada modo.

| Entrega | Trabajo | Resultado verificable |
| --- | --- | --- |
| 1. Entender y ejecutar | Portada, elección CLI/Server, dos quickstarts, guía de modos y conceptos básicos. | Ambos caminos copian y verifican los mismos datos. La guía de modos explica las filas finales y los efectos de un fallo. |
| 2. Trabajar con ReplicaDB | Guías de CLI y Server, conectores y referencia de configuración. | Cada tarea tiene una guía de entrada y cada opción o límite enlaza a su referencia canónica. |
| 3. Desplegar y operar | Arquitectura, Operaciones y API. | Los procedimientos indican precondiciones, pasos, comprobaciones y decisiones ante fallo. Los enlaces existentes siguen resolviendo. |

## Entrega 1 · Índice y recorrido

| Orden | Página y secciones | Archivo y decisión |
| --- | --- | --- |
| 1 | **ReplicaDB** · What ReplicaDB does · CLI or Server · First transfer | `docs/src/content/docs/index.mdx`. Abrir con pedidos de una base operativa a reporting. Colocar la imagen conceptual del README tras la explicación inicial y antes de la elección CLI/Server. Ofrecer ambos quickstarts. |
| 2 | **Choose CLI or Server** · Who starts and schedules runs · Who owns state and access · Choose your quickstart | `docs/src/content/docs/getting-started/choose-cli-or-server.mdx`. Comparar ejecución, programación, estado, permisos e historial en una tabla. |
| 3 | **CLI quickstart** · Prepare sample data · Install CLI · Configure and run · Verify the destination · Clean up | `docs/src/content/docs/getting-started/cli-quickstart.md`. Ejecutar un fichero completo de opciones y verificar las filas. |
| 4 | **Server quickstart** · Prepare sample data · Start local Server · Create two datasources · Create and run a job · Verify the run and destination · Clean up | `docs/src/content/docs/getting-started/server-quickstart.md`. Completar la misma transferencia desde la interfaz web. |
| 5 | **Replication modes** · Choose a mode · Follow the rows · Complete · Complete-atomic · Incremental · Verify and retry | `docs/src/content/docs/cli/replication-modes.mdx`. Mostrarla en *Getting started* y enlazarla desde CLI y Server. Mantener la URL actual. |
| 6 | **Core concepts** · Source and sink · Connector and mode · Datasource, job and run · Watermark | `docs/src/content/docs/getting-started/concepts.md`. Mantener el vocabulario que se usa en el recorrido. Enlazar a Arquitectura para intentos, workers y leases. |
| Auxiliar | **Sample data for the quickstarts** · Start a disposable PostgreSQL · Create the tables · Inspect and reset the data | Crear `docs/src/content/docs/getting-started/sample-data.md`. Enlazar desde los dos quickstarts y la guía de modos. Así los tres recorridos usan una sola preparación y una sola consulta de comprobación. |

### Historia de datos compartida

El ejercicio usa una instancia PostgreSQL de laboratorio con dos esquemas, `shop` y `reporting`. Ambos endpoints usan `jdbc:postgresql://127.0.0.1:55432/replicadb_demo`. Los esquemas permiten identificar el origen y el destino sin tener que arrancar dos instancias. PostgreSQL soporta los tres modos. El PostgreSQL embebido de Server guarda sus metadatos y es independiente del PostgreSQL de laboratorio.

1. El origen empieza con el pedido `1` en estado `pending` y con `change_seq = 10`. El destino empieza vacío. Ambos quickstarts copian esa fila con `mode=complete` y un único worker.
2. Para comparar modos, el destino arranca con la copia anterior y el pedido `9`, una fila histórica que solo existe en reporting. En origen se actualiza el pedido `1` y se crea el `2`.
3. Antes de probar cada modo se restaura la misma base de comparación en el destino. Así el resultado de una prueba no condiciona la siguiente.

**Preparación inicial**

```sql
CREATE SCHEMA shop;
CREATE SCHEMA reporting;

CREATE TABLE shop.orders (
  id integer PRIMARY KEY,
  status text NOT NULL,
  change_seq bigint NOT NULL
);

CREATE TABLE reporting.orders (
  id integer PRIMARY KEY,
  status text NOT NULL,
  change_seq bigint NOT NULL
);

INSERT INTO shop.orders VALUES (1, 'pending', 10);
```

Con ambos quickstarts, ejecutar `SELECT id, status, change_seq FROM reporting.orders ORDER BY id;` debe devolver `(1, pending, 10)`.

**Cambios en origen y base de comparación**

```sql
UPDATE shop.orders SET status = 'paid', change_seq = 11 WHERE id = 1;
INSERT INTO shop.orders VALUES (2, 'pending', 12);

-- Repeat these two statements before testing each mode.
TRUNCATE reporting.orders;
INSERT INTO reporting.orders VALUES (1, 'pending', 10), (9, 'archived', 8);
```

| Modo | Selección y escritura en el ejemplo | Resultado de la consulta de verificación |
| --- | --- | --- |
| `complete` | Lee todos los pedidos. Por defecto limpia el destino antes de cargar. Un fallo tras la limpieza puede dejar el destino vacío o parcial. | `(1, paid, 11)`, `(2, pending, 12)`. Desaparece el pedido `9`. |
| `complete-atomic` | Carga los pedidos en staging. En PostgreSQL, el borrado transaccional puede adelantarse y ejecutarse mientras se carga staging. La inserción desde staging y el borrado se confirman juntos. Una consulta nueva de otra sesión debería ver el conjunto antiguo hasta la confirmación y el nuevo después. Describir por separado la limpieza de staging. | `(1, paid, 11)`, `(2, pending, 12)`. Desaparece el pedido `9`. |
| `incremental` | Lee `change_seq > 10`, carga staging y aplica un upsert por `id`. El candidato calculado por el motor es `12` y Server lo persiste al finalizar con éxito. El ejemplo de CLI pasa el límite explícitamente en cada ejecución. | `(1, paid, 11)`, `(2, pending, 12)`, `(9, archived, 8)`. |

El número de secuencia es un dato didáctico controlado. Explicar por qué un valor tardío igual o inferior al watermark puede quedar fuera de la siguiente lectura. Cubrir el primer incremental sin valor inicial, la cancelación, el reintento desde el inicio, la ausencia de propagación de borrados y la diferencia entre un destino escrito y un watermark confirmado.

La guía actual dice que el CLI informa del candidato. En el código revisado, el motor lo expone en `ReplicationExecutionContext` y Server lo consume, mientras que `ReplicaDB.main` devuelve el código de salida. Comprobar también el paquete publicado. Si su salida coincide con el código, corregir esa afirmación en modos, conceptos y la guía de watermarks. Para el laboratorio, introducir `12` en la segunda ejecución de CLI a partir del resultado conocido del ejercicio.

### Tareas de la primera entrega

#### 1. Preparar y comprobar el ejemplo

- [x] Crear `docs/src/content/docs/getting-started/sample-data.md` con el SQL anterior y los comandos del laboratorio. Usar `demo` y `demo-local-only` como usuario y contraseña de ese contenedor desechable. Cargar el primer bloque SQL al preparar el laboratorio. Cargar el segundo solo al empezar la guía de modos y repetir sus dos últimas sentencias antes de cada prueba.

```bash
docker run --rm -d --name replicadb-docs-demo \
  -p 127.0.0.1:55432:5432 \
  -e POSTGRES_DB=replicadb_demo \
  -e POSTGRES_USER=demo \
  -e POSTGRES_PASSWORD=demo-local-only \
  postgres:16-alpine
docker exec replicadb-docs-demo pg_isready -h 127.0.0.1 -U demo -d replicadb_demo
docker exec -i replicadb-docs-demo psql -v ON_ERROR_STOP=1 -U demo -d replicadb_demo
```

Esperar a que `pg_isready` indique `accepting connections` antes de abrir `psql`. Pegar el bloque SQL correspondiente y salir con `\q`. Al terminar, ejecutar `docker stop replicadb-docs-demo` para eliminar el contenedor de la demo.

- [x] Verificar que `shop.orders` contiene la fila inicial y que `reporting.orders` está vacía. Probar el bloque de cambios y reset completo y comprobar que puede restaurarse la base de comparación antes de cada modo.

#### 2. Reescribir portada, elección y navegación

- [x] Incorporar [la imagen conceptual usada por el README](https://raw.githubusercontent.com/osalvador/ReplicaDB/gh-pages/docs/media/ReplicaDB-Conceptual.jpg) como `docs/src/assets/media/ReplicaDB-Conceptual.jpg`. Usarla en `docs/src/content/docs/index.mdx` con texto alternativo que describa lo que representa. Revisar su legibilidad en escritorio y móvil.
- [x] Editar `index.mdx`, `getting-started/choose-cli-or-server.mdx` y `getting-started/concepts.md` bajo `docs/src/content/docs/` con el índice anterior. Dar a la portada un enlace visible a cada quickstart y a la comparación.
- [x] Actualizar `docs/astro.config.mjs` para situar el acceso principal a *Replication modes* en *Getting started*. Revisar adelante y atrás en el recorrido y los enlaces desde CLI y Server.

#### 3. Completar y verificar los quickstarts

- [x] Editar `docs/src/content/docs/getting-started/cli-quickstart.md`. Usar el archivo de una release comprobada, fijar `mode=complete`, `jobs=1`, `source.table=shop.orders` y `sink.table=reporting.orders`. Mostrar cómo definir el JDBC del laboratorio y las credenciales en el entorno. Ejecutar `./bin/replicadb --options-file ./replicadb.conf`, comprobar código `0` y consultar el destino.
- [x] Editar `docs/src/content/docs/getting-started/server-quickstart.md`. Arrancar el paquete Server de la misma release con `./bin/replicadb-server start local`. Desde la URL del launcher, crear los perfiles PostgreSQL `Orders source` y `Reporting sink` con el mismo JDBC de laboratorio. Crear `Copy orders to reporting` con las dos tablas, `complete` y un worker. Lanzar manualmente, comprobar `SUCCEEDED` y consultar el destino. Usar un destino vacío al iniciar cada quickstart.

#### 4. Explicar y contrastar los tres modos

- [x] Editar `docs/src/content/docs/cli/replication-modes.mdx` con la tabla de resultados y un flujo por modo. Aprovechar los diagramas existentes cuando coincidan con el código. Contrastar el orden de preparación, escritura, confirmación y limpieza con `src/main/java/org/replicadb/ReplicaDB.java`, `src/main/java/org/replicadb/manager/SqlManager.java` y `src/main/java/org/replicadb/manager/PostgresqlManager.java`.
- [x] Probar los tres modos en el laboratorio. Para incremental, pasar `incremental.watermark.column=change_seq` e `incremental.watermark.value=10` en CLI. Usar staging en `public` para v1.0.3, que resuelve el nombre por el `search_path` al hacer el merge. En Server configurar `change_seq`, valor inicial `10` y staging en `public` en un job incremental nuevo. Verificar las filas tras cada modo y el valor `12` donde corresponda. Comprobar que repetir la lectura incremental con el valor `12` deja el destino igual.
- [x] Comprobar un fallo del merge incremental con un destino de laboratorio sin clave primaria y verificar que el valor confirmado no avanza. En `complete-atomic`, verificar con una segunda sesión la visibilidad antes y después del commit y documentar las diferencias entre staging automático y staging indicado por el usuario.

#### 5. Validar la entrega

- [x] Ejecutar `npm --prefix docs run test:all`. Revisar los contratos de contenido, rutas, imágenes y navegación si cambian las páginas esperadas. El build, la validación de enlaces y las pruebas deben pasar.
- [x] Abrir el sitio generado. Seguir los dos recorridos desde la portada, comprobar la imagen y los enlaces en escritorio y móvil y confirmar que la URL anterior de modos sigue resolviendo.

## Entrega 2 · Guías y referencia

- [ ] **CLI.** Consolidar `cli/parallelism.md` y `cli/performance.md` en un recorrido de medición y ajuste. Ampliar `cli/filtering-and-queries.md` con tabla frente a consulta, mapeo de columnas y resultado verificable. Conservar en `cli/multi-table.md` la secuencia y el resultado si falla la segunda tabla. Conectar `cli/incremental-watermarks.md` con el ejemplo y mantener allí los límites de configuración.
- [ ] **Server.** Reordenar `server/index.md` y las guías por tareas. Integrar *Dashboard* y los estados de error en los procedimientos de trabajos, ejecuciones y diagnóstico. Mantener datasources, jobs, schedules, runs y permisos como guías con resultado propio.
- [ ] **Conectores.** Mantener `connectors/index.mdx` y su matriz como entrada para comprobar compatibilidad. Revisar las fichas según roles, modos, conexión, tipos, staging y fallos propios. Conservar páginas breves que resuelvan una decisión concreta.
- [ ] **Referencia.** Usar `reference/cli-options.md` y `reference/environment-variables.md` como destinos canónicos para valores, precedencia y alcance. Mantener en `reference/example-options-files.md` los ejemplos que completen una tarea distinta. Contrastar la matriz de capacidades con `docs/src/data/connector-capabilities.json`.

Las rutas anteriores son relativas a `docs/src/content/docs/`, salvo las que empiezan por `docs/`. Antes de fusionar, registrar URL anterior, destino y sección equivalente en el mapa de edición. Comprobar enlaces y redirecciones con los tests del portal.

## Entrega 3 · Arquitectura, Operaciones y API

- [ ] **Arquitectura.** Ampliar el flujo del motor en `architecture/overview.mdx` y `architecture/core-and-server-boundaries.md`. Organizar ciclo de vida, dispatch, leases y fencing alrededor de una ejecución de Server. Mantener topología, escalado y seguridad donde respondan preguntas propias.
- [ ] **Operaciones.** Convertir `operations/index.md` en un mapa por tarea. Dar a `operations/backups-and-restore.md` y `operations/upgrades.md` pasos previos, ejecución y verificación. Agrupar recuperación y troubleshooting por síntomas y decisiones. Mantener las guías de despliegue distribuido, Cloud Run y gestión de claves.
- [ ] **API.** Reorganizar `api/index.md`, publicado en `/api-introduction/`, alrededor de sesión, permisos, lanzamiento y comprobación del resultado. Distinguir en el menú la guía de uso de las páginas generadas bajo `/api/`. Usar `docs/openapi/replicadb-server.json` como contrato y verificar las rutas y ejemplos contra ese esquema.
- [ ] **Cierre.** Revisar el inventario completo con las decisiones de conservar, ampliar o fusionar. Ejecutar `npm --prefix docs run test:all` y comprobar la navegación y las redirecciones del sitio generado.

Las rutas de contenido de esta entrega son relativas a `docs/src/content/docs/`.

## Estado de la primera entrega

La portada, los quickstarts, el laboratorio compartido, los modos y el glosario están editados. Se probaron el CLI y la interfaz de Server de la release v1.0.3 contra PostgreSQL 15 local. Se verificaron las filas de cada modo, la visibilidad del reemplazo atómico antes y después del commit y la recuperación del incremental tras un fallo de merge. La segunda ejecución incremental de Server leyó cero filas después de confirmar el watermark `12`.

El CLI de v1.0.3 no imprime el candidato del watermark. La guía muestra cómo pasar un límite conocido en cada invocación. El ejemplo usa `public` para staging incremental porque esa release resuelve el nombre de la tabla por el `search_path`.

- [ ] Repetir el arranque del laboratorio con Docker y PostgreSQL 16 cuando el motor Docker esté disponible. Las transferencias y el SQL de preparación ya se verificaron con PostgreSQL 15 local.
