---
target: run log viewer
total_score: 20
max_score: 40
na_heuristics: 
p0_count: 0
p1_count: 2
target_identity: "file:/Users/oscarsm/Documents/GitHub/ReplicaDB/replicadb-server/frontend/src/components/RunLogViewer.tsx"
target_fingerprint: "sha256:651da0bed56d2a07118e5e9105d650162b1f8c19100b3825aef666e902918be0"
target_path: /Users/oscarsm/Documents/GitHub/ReplicaDB/replicadb-server/frontend/src/components/RunLogViewer.tsx
timestamp: 2026-09-03T06-03-53Z
slug: db-server-frontend-src-components-runlogviewer-tsx
closed: true
---
⚠️ DEGRADED: single-context (el agente B no tuvo acceso a terminal/navegador; la evidencia se completó mediante fallback del contexto padre)

**Target:** [RunLogViewer.tsx](replicadb-server/frontend/src/components/RunLogViewer.tsx), dentro de [RunDetailPage.tsx](replicadb-server/frontend/src/pages/RunDetailPage.tsx)

**Design Health Score**

| # | Heurística | Score | Problema principal |
|---|---|---:|---|
| 1 | Visibilidad del estado | 2 | Muestra loading, error y truncamiento, pero no fecha, frescura ni límites claros. |
| 2 | Correspondencia con el mundo real | 3 | El log raw y los bytes son reconocibles, pero falta una lectura de “inicio, hueco, final”. |
| 3 | Control y libertad | 1 | No hay copy, búsqueda, descarga, colapsado ni salto a errores. |
| 4 | Consistencia y estándares | 3 | Se integra con MUI, pero el `<pre>` queda visualmente plano frente al resto del control plane. |
| 5 | Prevención de errores | 3 | Redacción y límite del servidor protegen datos; el usuario puede interpretar el log como completo. |
| 6 | Reconocimiento frente a recuerdo | 2 | El marker existe como texto, pero no como límite visual o estado semántico. |
| 7 | Flexibilidad y eficiencia | 1 | El diagnóstico exige copiar a herramientas externas para buscar o filtrar. |
| 8 | Diseño minimalista | 2 | Una pared monoespaciada domina el detalle y dificulta el escaneo. |
| 9 | Recuperación de errores | 2 | El error de carga no ofrece reintento; el truncamiento no ofrece una alternativa clara. |
| 10 | Ayuda y documentación | 1 | No explica `capturedSize`, `formatVersion` ni la política 75/25. |
| **Total** | | **20/40** | **Aceptable, pero insuficiente para diagnóstico operativo rápido.** |

**Veredicto de especificidad**

El componente sí es propio de ReplicaDB por su contexto de runs, redacción de credenciales, timestamps y metadata de ejecución. El problema no es la identidad visual, sino la falta de una gramática de diagnóstico: actualmente cualquier herramienta podría mostrar el mismo `<pre>`.

La propuesta GitHub Actions es adecuada si se adapta a la realidad del servidor: debe facilitar el escaneo, búsqueda y copia del contenido disponible, pero nunca insinuar que el log está completo cuando el servidor eliminó el tramo central.

**Evidencia determinista y visual**

- Detector CLI: exit code `0`, `[]`, cero hallazgos en `RunLogViewer.tsx`.
- Inyección del detector en una pestaña nueva: completada correctamente.
- Run observado: `SUCCEEDED`, `6.194 bytes`, `75 líneas`, no truncado.
- El `<pre>` real usa `16px/24px`, `white-space: pre-wrap`, `overflow-x: auto`, `overflow-wrap: anywhere`, fondo transparente, padding `0` y color de texto oscuro.
- En esta sesión no había un run real truncado para comprobar visualmente los 256 KiB; esa política sí está confirmada por [DEPLOYMENT.md](DEPLOYMENT.md) y por el contrato del servidor.
- No se observaron hallazgos del detector en el DOM instrumentado.

**Impresión general**

El visor conserva la información, pero no ayuda a leerla. En un incidente, el usuario recibe una pared de texto, descubre tarde que puede faltar el 50% central y debe llevar el contenido a otra herramienta para buscar o compartirlo. La mayor oportunidad es convertirlo en un “log viewer” de diagnóstico, no en un simple volcado de terminal.

**Lo que funciona**

- El servidor redacciona credenciales y limita el tamaño antes de entregar el contenido.
- Se preservan saltos de línea y líneas largas sin romper el layout.
- Loading, empty, error y truncation states ya existen y el componente es puro, por lo que puede ampliarse sin mover la lógica de runs.

**Carga cognitiva**

Fallan cuatro puntos:

- **Visibilidad:** no se distingue visualmente entre log completo y parcial.
- **Working memory:** el usuario debe recordar qué significa `256 KiB`, `capturedSize` y el marker.
- **Progressive disclosure:** metadata y contenido están juntos, pero no hay herramientas graduadas de búsqueda/copia/filtrado.
- **Context switch:** para una búsqueda o compartir un fragmento hay que salir del producto.

**Viaje emocional**

El run cargado produce alivio. La llegada al log empieza bien, pero la pared monoespaciada obliga a leer línea por línea. Si aparece `[TRUNCATED: middle omitted]`, la confianza cae porque no queda claro qué se omitió ni si existe una alternativa. El final actual es resignación: copiar el log a `grep`, `less`, un editor o un ticket externo.

**Problemas prioritarios**

### [P1] El truncamiento no tiene una frontera visual ni explica el hueco

**Por qué importa:** El marker se muestra como texto raw y puede confundirse con una línea emitida por ReplicaDB. El usuario no recibe una señal clara de que se conserva el inicio y el final, mientras falta el centro.

**Fix:** Detectar el marker cuando `log.truncated` es `true` y renderizar tres bloques: `Beginning`, un divisor visible “Middle omitted by server limit”, y `End`. Añadir un estado accesible que diga que el servidor conserva aproximadamente el primer 75% y el último 25%. No prometer descarga del log completo si el API no lo ofrece.

**Comando sugerido:** `/impeccable polish`

### [P1] Falta una barra de herramientas de diagnóstico

**Por qué importa:** Para buscar `ERROR`, copiar un fragmento o compartir el resultado, el usuario debe salir de la aplicación. Esto aumenta el tiempo de diagnóstico.

**Fix:** Añadir búsqueda local sobre el contenido disponible, contador de coincidencias, botón Copy y botón Clear search. El botón de descarga solo debe añadirse si existe un endpoint real para el contenido devuelto; no debe inventarse un `raw=true` ni sugerir que recupera el tramo eliminado.

**Comando sugerido:** `/impeccable bolder`

### [P2] El log no tiene tratamiento visual propio

**Por qué importa:** El `<pre>` transparente no tiene la contención ni el ritmo del resto del control plane. En logs largos, el texto se convierte en una superficie difícil de escanear.

**Fix:** Usar una superficie de diagnóstico con fondo `page-green` o un neutral ligeramente más profundo, padding estable, radio contenido, scrollbar local y líneas legibles. Mantener el monospace solo para datos y diagnóstico.

**Comando sugerido:** `/impeccable polish`

### [P2] Metadata incompleta y desconectada del contenido

**Por qué importa:** Solo se muestra `Captured 6194 bytes · format 1`. El usuario no ve cuándo fue capturado/actualizado ni qué significa el formato.

**Fix:** Convertirlo en una barra compacta: `Captured`, tamaño, estado `Complete`/`Partial`, fecha de captura y fecha de actualización cuando existan. El estado truncado debe aparecer arriba del contenido, no enterrado en una alerta genérica.

**Comando sugerido:** `/impeccable clarify`

### [P2] Error y empty states no son accionables

**Por qué importa:** “Unable to load the run log” no permite reintentar; “No detailed log available” no distingue ausencia de captura, retención o contenido vacío.

**Fix:** Añadir reintento para el error de carga y copy contextual para el estado vacío. Mantener el lenguaje honesto: no asumir que la ausencia significa fallo del run.

**Comando sugerido:** `/impeccable harden`

**Persona Red Flags**

- **Alex, power user:** No puede buscar `ERROR`, filtrar por severidad ni copiar una selección sin llevar el log a otra herramienta.
- **Jordan, first-timer:** No sabe si `[TRUNCATED: middle omitted]` es un error de ReplicaDB, una línea real o una limitación del visor.
- **Sam, usuario de accesibilidad:** El landmark “Detailed log” existe, pero el estado completo/parcial y los límites del contenido no están descritos de forma explícita.
- **Riley, on-call:** En un run truncado no puede saber rápidamente si el fallo está en el inicio, el final o en el tramo que falta.

**Observaciones menores**

- El endpoint actual solo devuelve el contenido limitado; un botón “Download full log” sería engañoso sin una extensión del API.
- La política 256 KiB debería mostrarse como límite del servidor, no como límite del navegador.
- `capturedAt` y `updatedAt` existen en el schema y deberían aprovecharse.
- El visor conserva el contenido redacted; cualquier copy/search debe operar sobre ese contenido ya seguro.
- La agrupación por severidad requiere parsing confiable de logs y no debería inventarse a partir de texto arbitrario; una primera iteración por bloques y búsqueda es más segura.

**Preguntas a considerar**

- ¿Quieres priorizar primero truncamiento visible y metadata, o las herramientas de búsqueda/copia?
- ¿Debe el visor agrupar por bloques temporales/severidad solo cuando el formato del log lo permita?
- ¿Quieres una mejora solo frontend o también una futura extensión del API para recuperar logs completos?
