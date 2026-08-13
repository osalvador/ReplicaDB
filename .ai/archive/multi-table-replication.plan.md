# Implementation Plan: Replicación de varias tablas por configuración

## Task Source

GitHub issues #168, #143 y #43 de `osalvador/ReplicaDB`:

- #168 solicita configurar varias tablas, especialmente en una ejecución incremental de SQL Server.
- #143 solicita replicar varias tablas o todas las tablas desde una configuración reutilizable.
- #43 propone listas de tablas, un archivo de nombres y selección por patrones.

### Acceptance Criteria

- Permitir declarar varios pares `source.table`/`sink.table` en un único archivo `.conf`.
- Mantener el formato legacy de una sola tabla sin cambios de comportamiento.
- Representar pares origen/destino explícitos, porque no todas las tablas tienen el mismo nombre.
- Ejecutar los pares secuencialmente; `jobs` seguirá controlando únicamente el paralelismo interno de cada tabla.
- Reutilizar el ciclo existente de ReplicaDB por cada par: creación de managers, pre-tasks, tareas, post-tasks y cleanup.
- Detener la ejecución ante el primer error y devolver el código de error existente.
- Evitar que el staging generado, las columnas inferidas, los archivos temporales o los managers de una tabla se reutilicen accidentalmente en otra.
- Preservar el modo `complete`, `incremental` y `complete-atomic` según las capacidades de cada manager.
- Mantener la precedencia actual: valores CLI no vacíos sobrescriben valores del archivo para el modo legacy.
- Validar entradas incompletas, índices duplicados o discontinuos y conflictos entre modo multi-tabla y `source.table`/`sink.table` escalares.
- Mantener credenciales y parámetros sensibles fuera de logs, `toString()`, Sentry y ejemplos documentados.
- Documentar que wildcard/regex y descubrimiento automático de tablas quedan fuera de este MVP y pueden resolverse después generando entradas explícitas.

## Overview

ReplicaDB tiene una arquitectura de transferencia punto a punto: `ToolOptions` contiene una única tabla, `ReplicaDB` prepara un único ciclo de managers y cada `ReplicaTask` particiona esa tabla usando `jobs`. Las tres issues abiertas describen la necesidad de repetir ese ciclo para varias tablas, no de combinar sus filas en una misma consulta.

El enfoque elegido añade un catálogo indexado de pares en el mismo `.conf` y un runner nativo que ejecuta una replicación completa por par, de forma secuencial. Cada par recibe una copia aislada de las opciones, por lo que conserva su tabla destino, metadata inferida, lifecycle y recursos temporales, mientras se reutiliza sin duplicación el motor de replicación actual.

Ejemplo objetivo:

```properties
mode=incremental
jobs=1
source.connect=${SOURCE_CONNECT}
source.user=${SOURCE_USER}
source.password=${SOURCE_PASSWORD}
sink.connect=${SINK_CONNECT}
sink.user=${SINK_USER}
sink.password=${SINK_PASSWORD}
sink.staging.schema=dbo

replication.table.1.source=dbo.customers
replication.table.1.sink=dbo.customers
replication.table.2.source=dbo.orders
replication.table.2.sink=dbo.sales_orders
```

Flujo previsto:

```text
ToolOptions(base .conf)
        |
        v
  validar catálogo
        |
        v
  +-------------------------------+
  | por cada par, en orden        |
  | copia de ToolOptions          |
  | reset de estado temporal      |
  | ciclo ReplicaDB existente     |
  +-------------------------------+
        |
        v
  primer error => código 1 y stop
```

## Architecture & Design

### Selected approach: runner Java nativo y secuencial

El cambio se mantiene en la frontera de configuración y orquestación:

1. `OptionsFile` lee propiedades `replication.table.<n>.source` y `replication.table.<n>.sink`.
2. Un modelo inmutable `ReplicationTable` representa cada par.
3. `ToolOptions` expone una lista inmutable y puede producir una copia específica para un par.
4. `ReplicaDB.processReplica()` detecta el catálogo y repite el ciclo individual existente.
5. Los managers siguen siendo responsables de SQL, particionamiento, staging, tipos y hooks.

No se añadirá una consulta multi-tabla al source manager, no se concatenarán nombres con comas en SQL y no se convertirá `jobs` en un segundo nivel de scheduling. Cada ejecución individual conserva sus conexiones propias y las tareas internas existentes.

### Configuration contract

Las propiedades nuevas son únicamente de archivo de opciones en este MVP:

```text
replication.table.1.source=<source table>
replication.table.1.sink=<sink table>
replication.table.2.source=<source table>
replication.table.2.sink=<sink table>
```

Los índices deben empezar en `1`, ser positivos, contiguos y tener ambos valores no vacíos. `Properties` y la expansión `${ENV_NAME}` existentes se mantienen como mecanismo de lectura y sustitución.

El modo multi-tabla es explícito: cuando existe al menos una entrada indexada, `source.table` y `sink.table` no pueden tener valores escalares y `--source-table`/`--sink-table` no pueden acompañar esa ejecución. Esto evita que una opción legacy sobrescriba silenciosamente todos los pares. Las opciones CLI de tabla siguen funcionando sin cambios cuando no hay catálogo multi-tabla.

Las opciones comunes (`source.columns`, `source.where`, `sink.columns`, `sink.staging.schema`, flags de sink, formato, autenticación y `jobs`) se aplican a todos los pares. `source.query` no es compatible con el catálogo porque no representa una tabla distinta por entrada. En modos que usan staging, `sink.staging.table` fijo no se permite en el MVP: se usará un staging generado por tabla, opcionalmente dentro de `sink.staging.schema`, para evitar mezclar esquemas heterogéneos. El rechazo debe ser explícito y explicar la alternativa.

### Lifecycle and isolation

El método actual de ejecución se separará conceptualmente en una operación individual reutilizable y un bucle multi-tabla. La operación individual seguirá siendo responsable de:

- crear managers source/sink;
- ejecutar `preSourceTasks()` y `preSinkTasks()`;
- esperar el future de pre-sink cuando corresponda;
- ejecutar `jobs` instancias de `ReplicaTask`;
- ejecutar post-tasks;
- cerrar managers y executors en `finally`;
- devolver `0` o `1` según el contrato existente.

Antes de cada operación individual se limpiará el nombre estático de staging generado en `ConnManager`, porque actualmente los managers de las tareas comparten ese nombre dentro de una ejecución y una segunda tabla en la misma JVM podría heredarlo. `FileManager.setTempFilesPath(new HashMap<>())` ya se reinicia por ejecución y se conservará.

Cada par se ejecutará con una copia de `ToolOptions`. La copia debe preservar todas las opciones comunes, clonar `Properties`, arrays y listas mutables, copiar autenticación source/sink y sustituir solo `sourceTable` y `sinkTable`; no debe conservar el catálogo para que la copia no vuelva a entrar recursivamente en el runner. Esto también evita que `getAllSinkColumns()` deje las columnas inferidas de la primera tabla en las siguientes.

El runner se detendrá cuando una ejecución devuelva error. No habrá concurrencia entre tablas en este MVP; la concurrencia existente dentro de una tabla permanece sin cambios.

### Performance, security and compatibility

- El consumo de conexiones queda acotado al de una ejecución actual; no se crea un pool ni se comparten conexiones entre tablas.
- La duración total será aproximadamente la suma de las ejecuciones, a cambio de aislar staging y simplificar rollback.
- Los nombres de tabla siguen llegando al manager como input del usuario y no se relajan las reglas de escape existentes.
- Los logs pueden identificar el índice y el par de tablas, pero nunca deben incluir passwords, connection strings con secretos o parámetros sensibles.
- `Sentry` debe recibir el `ToolOptions` específico del par actual; el runner no debe serializar el catálogo completo ni credenciales.
- La ausencia de `replication.table.*` mantiene exactamente la ruta legacy de una sola tabla.
- La funcionalidad no se generaliza a S3, Kafka, archivos u otros sinks como si fueran tablas; el catálogo solo valida pares que el ciclo normal pueda ejecutar.

## Implementation Tasks

### 1. Modelar y validar el catálogo de pares

- [x] **1.1 Añadir el modelo inmutable de una tabla origen/destino**
  Files: `src/main/java/org/replicadb/cli/ReplicationTable.java`
  Changes: Crear un `record` o clase final con `sourceTable` y `sinkTable`, validación de valores no vacíos y representación segura para logs. Mantener el modelo libre de conexiones, SQL y lógica de scheduling. Exponer una colección inmutable desde los consumidores.
  Tests: Crear `src/test/java/org/replicadb/cli/ReplicationTableTest.java` con pares válidos, nombres cualificados, valores nulos/blancos y comprobación de inmutabilidad/igualdad.
  Dependencies: None

- [x] **1.2 Leer propiedades indexadas y rechazar catálogos inválidos**
  Files: `src/main/java/org/replicadb/cli/OptionsFile.java`, `src/main/java/org/replicadb/cli/ToolOptions.java`
  Changes: Añadir un parser basado en `Properties` para las claves exactas `replication.table.<n>.source` y `.sink`. Recorrer `stringPropertyNames()`, rechazar cualquier clave que empiece por `replication.table.` pero no coincida exactamente con `^replication\\.table\\.(\\d+)\\.(source|sink)$`, agrupar en un `TreeMap<Integer, ...>`, rechazar índices no positivos o fuera de rango, comprobar continuidad desde `1` hasta el máximo y exigir ambos lados no blancos por índice. Mantener el valor de tabla tal como fue configurado después de usar `isBlank()` solo para validarlo. Los errores deben indicar la clave o el índice inválido; un typo como `replication.table.1.sorce` nunca puede ser ignorado ni activar silenciosamente el modo legacy. Resolver valores después de la expansión de entorno existente. En `ToolOptions`, ejecutar la validación después de cargar el archivo y aplicar las opciones CLI: si hay catálogo, rechazar `source.query`, valores escalares no vacíos de `source.table`/`sink.table`, `--source-table`/`--sink-table` presentes en `CommandLine`, y `sink.staging.table`/`sink.staging.table.alias` cuando el modo usa staging. Mantener intacta la lectura de una configuración legacy sin esas claves. Las claves con espacios o segmentos extra se tratan como malformadas y fallan explícitamente.
  Tests: Crear `src/test/java/org/replicadb/cli/ToolOptionsMultipleTablesTest.java` con un catálogo de dos pares, expansión `${ENV_NAME}`, orden numérico aunque `Properties` no lo preserve, índice faltante, lado source/sink faltante, valor blanco, índice no positivo, overflow, typo de propiedad, segmentos extra/espacios en la clave, mezcla con tablas escalares, presencia de opciones CLI de tabla, mezcla con `source.query`, staging fijo en modo incremental, y configuración legacy que sigue aceptándose. Verificar que la incompatibilidad con `source.query` y los conflictos scalar/CLI fallan durante la construcción de `ToolOptions`, antes de crear managers.
  Dependencies: Task 1.1

### 2. Crear copias de configuración por par

- [x] **2.1 Añadir una copia de `ToolOptions` específica para una tabla**
  Files: `src/main/java/org/replicadb/cli/ToolOptions.java`, `src/main/java/org/replicadb/cli/AzureAuthenticationOptions.java`, `src/test/java/org/replicadb/cli/ToolOptionsMultipleTablesTest.java`
  Changes: Implementar un constructor de copia o método público `forReplicationTable(ReplicationTable)` porque `ReplicaDB` vive en otro paquete y necesita crear la copia concreta. Debe copiar todos los valores escalares y booleanos, modo, jobs, formatos, `AzureAuthenticationOptions` source/sink mediante un método/constructor de copia explícito, `Properties` mediante una nueva instancia con `putAll`, `sourcePrimaryKeys` mediante `Arrays.copyOf` y `sourceColumnDescriptors` mediante una nueva lista cuyos elementos también se copien si son mutables. Copiar los valores ya resueltos, no volver a parsear argumentos ni reexpandir el entorno. Sustituir únicamente `sourceTable` y `sinkTable`, dejar vacío el catálogo de la copia y asegurar que `sinkColumns` comienza con el valor configurado de esa copia (normalmente `null`), de modo que `SqlManager.getAllSinkColumns()` pueda inferirlo sin contaminar otro par. No incluir secretos en una nueva representación textual.
  Tests: Verificar que una copia conserva cada opción común relevante, que source/sink apuntan al par solicitado, que dos copias no comparten `Properties`, arrays, listas, descriptores ni autenticación mutable, que el catálogo no se propaga, que la inferencia de `sinkColumns` en la copia del primer par no modifica la base ni la copia del segundo, y que `source.where`, staging schema, flags y formatos permanecen iguales para ambos pares.
  Dependencies: Tasks 1.1 y 1.2

### 3. Repetir el ciclo existente de forma nativa

- [x] **3.1 Separar ejecución individual y runner multi-tabla en `ReplicaDB`**
  Files: `src/main/java/org/replicadb/ReplicaDB.java`, `src/main/java/org/replicadb/ReplicaTask.java`
  Changes: Renombrar/extraer el cuerpo actual de `executeReplication()` a `private static int executeSingleReplication(ToolOptions options)`, que ejecuta exactamente un ciclo y solo retorna después de que `finally` haya cerrado managers y executors. Añadir `private static int executeMultipleReplications(ToolOptions baseOptions)` y hacer que `processReplica()` elija este método solo cuando `baseOptions.hasReplicationTables()` sea verdadero; el runner recorrerá la lista ya ordenada, construirá `forReplicationTable(...)`, registrará un contexto no sensible, llamará a `executeSingleReplication(...)` y se detendrá ante el primer código distinto de cero. Los hooks pre-source, pre-sink, post-source, post-sink y cleanup se ejecutan dentro de `executeSingleReplication` para cada par, y no se crea un executor externo para tablas. Mantener el camino legacy, el manejo de `--help`/`--version`, la restauración del flag de interrupción, los nombres `TaskId-*`, el código de salida y el cleanup de executors.
  Tests: Añadir `src/test/java/org/replicadb/ReplicaDBMultipleTablesTest.java` con una seam/fábrica inyectable que demuestre orden source-to-sink, una ejecución por par, invocación de todos los hooks por par en `complete`, `incremental` y `complete-atomic` donde el manager lo soporte, aislamiento de opciones, parada en el primer error, propagación de código `1`, no ejecución de pares posteriores, y retorno únicamente después del cleanup de la tabla anterior. Cubrir explícitamente la ruta legacy de una sola tabla con una configuración sin catálogo y verificar que conserva el número de tareas y el ciclo actual. Cubrir también catálogo vacío y `--help`/`--version` sin conexiones.
  Dependencies: Task 2.1

- [x] **3.2 Aislar estado generado entre ejecuciones de tabla**
  Files: `src/main/java/org/replicadb/manager/ConnManager.java`, `src/main/java/org/replicadb/ReplicaDB.java`, `src/test/java/org/replicadb/manager/SqlManagerStagingTableTest.java` o una nueva `src/test/java/org/replicadb/manager/ConnManagerStagingIsolationTest.java`
  Changes: Añadir `public static void resetGeneratedSinkStagingTableName()` junto al estado estático existente y llamarlo como primer paso de `executeSingleReplication(...)`, antes de `FileManager.setTempFilesPath(...)` y antes de `createConnectionManagers(options)`. El reset ocurre exactamente una vez por par; no se ejecuta dentro de `ReplicaTask`, porque los managers de una misma tabla deben seguir compartiendo el nombre generado. Conservar `FileManager.setTempFilesPath(new HashMap<>())` al inicio de cada ejecución individual para que archivos temporales de una tabla se descarten antes de crear la siguiente. Conservar la protección de staging definido por el usuario y el cleanup actual. Documentar en el código solo la razón no obvia de este reset.
  Tests: Usar dos sinks con nombres distintos para evitar depender de una colisión aleatoria y demostrar que dos ejecuciones secuenciales generan nombres de staging que no heredan el prefijo de la primera tabla; dentro de una ejecución, verificar que los managers de pre-sink y las tareas siguen usando el mismo staging. Verificar mediante una seam de orden o un fake que el reset del nombre y el reset de archivos temporales ocurren antes de crear managers, que un `sink.staging.table` explícito no se elimina y que una excepción durante la primera tabla no deja su nombre ni archivos temporales para la segunda.
  Dependencies: Task 3.1

### 4. Preservar errores, lifecycle y observabilidad

- [x] **4.1 Revisar cierre de managers y contexto por tabla**
  Files: `src/main/java/org/replicadb/ReplicaTask.java`, `src/main/java/org/replicadb/ReplicaDB.java`, `src/main/java/org/replicadb/config/Sentry.java`, `src/main/java/org/replicadb/config/CredentialRedactor.java` si la cobertura existente detecta un punto sin sanitizar
  Changes: Asegurar que cada ejecución cierra source y sink aun cuando falle la autenticación o la transferencia de uno de ellos, conserva la causa original y agrega cierres secundarios como suppressed. Confirmar que `executeSingleReplication(...)` no devuelve hasta que el `finally` haya cerrado managers, esperado/terminado los futures y llamado al cleanup; ninguna conexión o tarea de la tabla anterior puede quedar activa al crear la siguiente. Los hooks pre-source, pre-sink, post-source, post-sink y cleanup deben recibir la copia de `ToolOptions` del par actual y ejecutarse una vez por par, incluso cuando el par siguiente tenga otra tabla. Actualizar el contexto/log de inicio y fallo para incluir índice/par de forma redacted, sin volcar el catálogo completo ni credenciales. No cambiar el contrato de los modos ni la inicialización de Sentry fuera del ciclo individual. Auditar explícitamente que no existe checkpoint, watermark o cursor incremental global en el código actual; la única información incremental copiada es la configuración común `source.where` y el staging se reinicia por par.
  Tests: Ampliar `src/test/java/org/replicadb/ReplicaTaskAuthenticationFailureTest.java` o crear una prueba de runner con fakes que falle en source, sink, lectura, inserción y cleanup; verificar cierre independiente, causas suprimidas, parada del catálogo, que no se crea el siguiente manager antes del cierre anterior, y ausencia de passwords/connection strings en mensajes capturados. Verificar que cada hook ve la tabla actual y que cada transacción Sentry corresponde al par actual sin probar un endpoint externo. Añadir un caso incremental con dos pares y `source.where` configurado para demostrar que no se comparte ningún estado de ejecución no configurado.
  Dependencies: Tasks 3.1 y 3.2

### 5. Documentar el contrato y los límites del MVP

- [x] **5.1 Añadir ejemplos de configuración y operación multi-tabla**
  Files: `README.md`, `docs/docs/docs.md`, `docs/index.md`, `conf/_replicadb.conf`
  Changes: Añadir el formato indexado con pares origen/destino y variables `${ENV_NAME}`; explicar con un ejemplo que tres entradas producen tres ciclos secuenciales, mientras `jobs=4` solo divide en hasta cuatro tareas la tabla que se está ejecutando. Documentar que no se crea un executor externo, que un error detiene las entradas posteriores, y que el log identifica el índice fallido. Documentar las incompatibilidades con `source.query`, tablas escalares y staging fijo en modos con staging; incluir diagnóstico del primer error y una recomendación para generar las entradas desde `information_schema` externamente. Marcar wildcard/regex, descubrimiento automático y scheduling como fuera de alcance. Actualizar el roadmap para distinguir “catálogo explícito multi-tabla” de esas capacidades futuras sin prometerlas como implementadas.
  Tests: Ejecutar el build de documentación/Jekyll aplicable, `git diff --check`, buscar que cada propiedad documentada exista en `ToolOptions`/`OptionsFile`, y ejecutar un ejemplo de parseo sin conexiones usando valores ficticios y variables de entorno. Verificar que no aparecen passwords, DSN reales ni endpoints credential-bearing y que el ejemplo diferencia claramente número de tablas de `jobs`.
  Dependencies: Tasks 1.2 y 3.1

### 6. Validación final y compatibilidad de distribución

- [x] **6.1 Ejecutar pruebas enfocadas y regresión sin ampliar el alcance**
  Files: `pom.xml` solo si las pruebas muestran que falta configuración; `src/test/java/org/replicadb/cli/ToolOptionsMultipleTablesTest.java`, `src/test/java/org/replicadb/ReplicaDBMultipleTablesTest.java`, `src/test/java/org/replicadb/manager/ConnManagerStagingIsolationTest.java` y documentación tocada
  Changes: No añadir dependencias nuevas salvo una necesidad demostrada por la implementación. Confirmar que el runner compila con Java 17/JUnit Jupiter 6, que los tests normales no requieren Docker, bases externas ni credenciales, y que el empaquetado conserva el launcher y los recursos actuales. Registrar cualquier limitación de integración real sin convertirla en una refactorización ajena al objetivo.
  Tests: Ejecutar primero `mvn -B -Dtest=ToolOptionsMultipleTablesTest,ReplicaDBMultipleTablesTest,ConnManagerStagingIsolationTest test` con los nombres finales de las clases; después `mvn -B test`; ejecutar el build de release o el smoke check de empaquetado disponible y revisar `git diff --check`. Añadir una comprobación manual del modo legacy y otra del `.conf` multi-tabla contra una base de datos local/fixture solo si el entorno ya está disponible; no exigir una matriz completa de Testcontainers para este cambio de orquestación.
  Dependencies: Tasks 1.1 through 5.1

## Technical Reference

### Types & data structures

`ReplicationTable` representa exclusivamente un par de identificadores. `ToolOptions` mantiene una `List<ReplicationTable>` inmutable para el catálogo base y ofrece una copia concreta por par. La copia conserva las opciones comunes ya parseadas, pero no vuelve a exponer el catálogo para evitar recursión.

El índice de configuración es parte del formato externo, no del modelo de manager. El parser debe ordenar por índice y rechazar huecos, en vez de depender del orden no garantizado de `Properties`.

### Dependencies

No se prevén nuevas dependencias Maven. El cambio usa las clases existentes de Commons CLI, `Properties`, executors, managers y Log4j2. La baseline continúa siendo Java 17 y las pruebas nuevas usan JUnit Jupiter 6 con Mockito únicamente si se necesita aislar managers o JDBC.

### Testing strategy

La cobertura se divide en tres niveles:

| Nivel | Qué verifica | Requisito externo |
| --- | --- | --- |
| Unitario de configuración | Parser, índices, precedencia, incompatibilidades, copia profunda | Ninguno |
| Unitario de orquestación | Orden, parada ante error, lifecycle, códigos de salida, aislamiento de staging | Fakes/Mockito |
| Regresión/integración opcional | Ejecución legacy y una secuencia real de tablas homogéneas o heterogéneas | Base local o fixture existente |

No se debe afirmar compatibilidad universal de todos los managers a partir de una sola integración. Los managers no reciben cambios de dialecto: la prueba real debe seleccionar una combinación soportada por la matriz del README y comprobar únicamente que el runner repite correctamente el contrato ya existente.

### Operational constraints

- Las tablas se procesan en el orden numérico declarado.
- Un error detiene las siguientes tablas y retorna `1`; el log debe indicar el índice fallido.
- `jobs` conserva su semántica actual y puede abrir varias conexiones por tabla.
- En `incremental` y `complete-atomic`, el MVP genera staging por tabla; un staging fijo compartido se rechaza.
- La duración total puede ser mayor que una ejecución individual, pero no se introducen conexiones simultáneas entre tablas.
- Las tablas que necesiten columnas, filtros o staging diferentes requieren catálogos separados en este MVP; el formato por entrada podrá ampliarse en una issue posterior.

### Rollback and compatibility

Eliminar las propiedades `replication.table.*` devuelve la ejecución al camino actual sin cambios en el contrato de una sola tabla. Si el cambio falla, el rollback lógico consiste en no activar el runner y conservar `source.table`/`sink.table`. No se requieren migraciones de base de datos ni cambios en imágenes o dependencias.

## Quality Gate

Antes de `/itx-code`, comprobar:

- Cada tarea tiene archivos concretos, cambios implementables, pruebas específicas y dependencias.
- El modo legacy está cubierto explícitamente.
- Los pares incompletos y las combinaciones ambiguas fallan antes de abrir conexiones.
- Cada tabla recibe una copia de opciones y un ciclo de cleanup independiente.
- El nombre de staging generado no cruza los límites de dos tablas.
- `jobs` no se reutiliza como número de tablas ni se introduce paralelismo externo.
- No hay secrets, DSNs, endpoints reales ni PII en fixtures, logs o documentación.
- El plan no intenta resolver wildcard/regex, metadata discovery o scheduling en la misma entrega.

## Execution Retrospective (auto-generated by /itx-code)

### Plan Accuracy

- Tasks completed as planned: 8/8 (100%).
- Tasks that required plan adjustment: 0/8 (0%).
- Test loop iterations: 6 total (4 first-pass, 2 second-pass).

### Gaps Encountered

#### Gap 1: Full integration matrix blocked by emulated Oracle 18c (Plan-to-Implementation)

- **Task**: 6.1 — Ejecutar pruebas enfocadas y regresión sin ampliar el alcance.
- **Plan assumed**: The full Maven suite could complete in the local Docker environment.
- **Reality**: `Oracle2OracleCrossVersionLobTest` started the `gvenzl/oracle-xe:18-slim-faststart` amd64 image under Apple Silicon emulation; the container exited with `ORA-00443: background process "PMON" did not start`, leaving Surefire waiting without producing a product assertion.
- **Resolution**: Stopped only the blocked full-suite terminal after confirming the container exit and zero-CPU Surefire process. The new 40-test focused slice, affected existing tests, Java diagnostics, documentation build, and `mvn -B -DskipTests package` all passed. Earlier integration classes observed in the run also reported zero failures.
- **Learning**: Run cross-version Oracle integration tests on a native supported architecture or CI runner; treat Docker emulation failures as infrastructure gaps and preserve a Docker-free focused validation slice for orchestration changes.

### Patterns Discovered

- **Sequential per-table lifecycle**: `ReplicaDB.executeSingleReplication(...)` isolates one table's managers, hooks, executors, temporary files, and generated staging state before the next catalog entry.
- **Indexed options catalog**: `OptionsFile.getReplicationTables()` uses exact-key validation and numeric ordering while preserving the existing `Properties` and environment-expansion boundary.