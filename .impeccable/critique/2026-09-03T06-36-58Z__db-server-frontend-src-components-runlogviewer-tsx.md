---
target: run log preferences
total_score: 26
max_score: 40
na_heuristics: 
p0_count: 0
p1_count: 2
target_identity: "file:/Users/oscarsm/Documents/GitHub/ReplicaDB/replicadb-server/frontend/src/components/RunLogViewer.tsx"
target_fingerprint: "sha256:8a2145ddd081031f2f02de532fb2d545f65201fb2a6dc43ed79bedbaaac8d725"
target_path: /Users/oscarsm/Documents/GitHub/ReplicaDB/replicadb-server/frontend/src/components/RunLogViewer.tsx
timestamp: 2026-09-03T06-36-58Z
slug: db-server-frontend-src-components-runlogviewer-tsx
closed: true
---
⚠️ DEGRADED: single-context (el agente B no tuvo acceso a terminal; la evidencia CLI se completó en el contexto padre)

**Target:** [RunLogViewer.tsx](replicadb-server/frontend/src/components/RunLogViewer.tsx)

## Design Health Score

| # | Heurística | Score | Problema principal |
|---|---|---:|---|
| 1 | Visibilidad del estado | 3 | El estado completo/parcial y `Captured at` son visibles, pero hay metadata secundaria que distrae. |
| 2 | Correspondencia con el mundo real | 3 | El log operativo es reconocible, pero envolver líneas rompe la unidad de cada evento. |
| 3 | Control y libertad | 3 | Copy y retry existen; el buscador duplicado añade una interacción innecesaria. |
| 4 | Consistencia y estándares | 2 | El visor contradice la convención de inspección de líneas y mezcla demasiadas señales de truncamiento. |
| 5 | Prevención de errores | 2 | El log puede parecer más legible de lo que es porque una línea partida parece varias líneas. |
| 6 | Reconocimiento frente a recuerdo | 3 | `Captured at` ayuda; bytes, formato y `Updated at` añaden ruido para este flujo. |
| 7 | Flexibilidad y eficiencia | 3 | Copy es útil; el navegador ya cubre búsqueda y el buscador interno duplica capacidades. |
| 8 | Diseño minimalista | 2 | La toolbar y metadata superfluas compiten con el contenido operativo. |
| 9 | Recuperación de errores | 3 | Retry está disponible, pero debe validarse el orden de hooks durante transiciones de estado. |
| 10 | Ayuda y documentación | 2 | La política de truncamiento está explicada, pero la UI presenta más información de la necesaria. |
| **Total** | | **26/40** | **Base funcional, pero requiere alinearse con la lectura de logs operativos.** |

## Diagnóstico específico

Tus dos decisiones son correctas para este producto:

- **No hacer word-wrap:** cada línea representa un evento y debe conservar su continuidad visual. La superficie debe usar `white-space: pre` y scroll horizontal local, eliminando `overflow-wrap: anywhere`.
- **Mostrar solo `Captured at`:** el tamaño, `formatVersion` y `Updated at` son metadata técnica que puede permanecer en el contrato, pero no necesita ocupar espacio en la lectura principal. El estado `Complete/Partial` y la alerta de truncamiento sí deben conservarse porque cambian la confianza en el diagnóstico.
- **Eliminar búsqueda interna:** el navegador ya ofrece Cmd/Ctrl+F con menos estado y menos UI. Mantener Copy porque permite llevar el contenido redacted disponible a una herramienta de análisis o ticket.

Hay además un defecto independiente y prioritario: `RunLogViewer` llama `useState` después de retornar para loading, error o ausencia de contenido. Eso viola las Rules of Hooks y puede desordenar estado cuando una query cambia entre esos estados.

## Evidencia determinista y visual

- Detector CLI: exit code `0`, JSON `[]`, cero hallazgos en `RunLogViewer.tsx`.
- En la pestaña real del run: `6.194 bytes`, `75 líneas`, estado `Complete log`, Copy visible y buscador ausente porque el estado actual fue inspeccionado después de una versión parcial del flujo.
- El bloque medido usa `white-space: pre-wrap`, `overflow-wrap: anywhere`, `overflow-x: auto`, `14px`, `12px` de padding y scroll del documento sin overflow horizontal.
- Metadata actual: `Captured 6194 bytes · format 1`, `Captured at 1 Sept 2026, 16:58`, `Updated at 1 Sept 2026, 16:58`.
- No se inspeccionó un run truncado real en esta sesión; el contrato del servidor confirma el límite de 256 KiB y la retención 75/25.

## Impresión general

La dirección correcta es más austera que la iteración anterior: una barra pequeña con estado y `Captured at`, Copy como única acción, y un bloque monoespaciado que preserve literalmente cada línea. El visor debe comportarse como una consola operativa contenida, no como un editor de texto.

## Lo que funciona

- El contenido se conserva en un `<pre>` y el servidor ya redacciona secretos antes de devolverlo.
- El estado `Complete/Partial` y la frontera del truncamiento hacen visible la limitación de 256 KiB.
- Copy es una acción de alto valor para compartir el log disponible y analizarlo externamente.

## Problemas prioritarios

### [P1] Word-wrap rompe la lectura de eventos

**Por qué importa:** `pre-wrap` y `overflow-wrap: anywhere` parten líneas largas en puntos arbitrarios. En logs con timestamps, stack traces o SQL, el usuario ya no distingue una línea de evento de una continuación visual.

**Fix:** Usar `white-space: pre` y eliminar `overflow-wrap`. Mantener `overflow-x: auto` en la superficie del log. En móvil se acepta scroll horizontal local; es preferible a alterar la semántica visual del log.

**Comando sugerido:** `/impeccable polish`

### [P1] Los hooks están después de retornos condicionales

**Por qué importa:** `useState` se ejecuta solo cuando existe contenido. Si el visor pasa de loading/error/empty a contenido, React puede lanzar un error de orden de hooks o asociar estado incorrectamente.

**Fix:** Mover `useState` al inicio del componente, antes de cualquier retorno condicional. Resetear opcionalmente el feedback cuando cambie `log?.runId`; no añadir estado innecesario.

**Comando sugerido:** `/impeccable harden`

### [P2] Metadata secundaria ocupa espacio sin ayudar al diagnóstico

**Por qué importa:** `capturedSize`, `formatVersion` y `Updated at` son útiles para API/operaciones, pero en esta vista compiten con el log y obligan a leer tres mensajes antes del contenido.

**Fix:** Mantener `Complete log`/`Partial log`, conservar solo `Captured at` y retirar del layout principal bytes, versión y `Updated at`. El detalle de 256 KiB debe permanecer en la alerta parcial, porque sí afecta la interpretación.

**Comando sugerido:** `/impeccable clarify`

### [P2] Búsqueda interna sería redundante

**Por qué importa:** El navegador ya ofrece búsqueda inmediata, accesible y familiar. El buscador React añade estado, rerenderiza hasta 256 KiB por pulsación y puede alterar visualmente el log con `<mark>`.

**Fix:** Eliminar `searchTerm`, contador, resaltado, `TextField`, `ClearIcon` y feedback de búsqueda. Mantener Copy y su feedback accesible.

**Comando sugerido:** `/impeccable distill`

### [P3] La señal de truncamiento puede simplificarse

**Por qué importa:** Chip, alerta y divisor son válidos, pero deben tener una jerarquía clara para no repetir “partial” tres veces.

**Fix:** Conservar el chip como estado persistente, la alerta como explicación breve y el divisor como frontera física. No repetir el mismo texto más veces.

**Comando sugerido:** `/impeccable polish`

## Personas

- **Alex, on-call:** necesita ver timestamps y líneas completas sin reconstruir saltos; Copy es útil, búsqueda del navegador suficiente.
- **Jordan, primerizo:** demasiada metadata puede hacerle pensar que bytes/formato son decisiones operativas; `Captured at` y Partial explican lo esencial.
- **Sam, accesibilidad:** el `<pre>` focusable es útil; debe anunciarse que existe scroll horizontal local y no depender de wrapping para leer líneas.
- **Riley, stress tester:** una transición loading → error → contenido puede activar el bug de hooks; un log parcial sin marker debe seguir mostrando una limitación honesta.

## Observaciones menores

- No añadir Download mientras el API no pueda devolver el contenido omitido.
- Mantener el marker `[TRUNCATED: middle omitted]` en Copy para que el contenido exportado conserve la evidencia del hueco.
- `formatVersion` puede seguir en la respuesta API aunque no se muestre en la vista.
- Los avisos de React Router y Emotion vistos en browser son preexistentes/runtime y no forman parte de este cambio.

## Preguntas para considerar

- ¿Confirmamos `white-space: pre` también en móvil, aceptando scroll horizontal local como comportamiento oficial?
- ¿El chip `Complete/Partial` debe mantenerse junto a `Captured at`, o prefieres que Partial aparezca solo en la alerta?
- ¿Quieres que Copy copie también una cabecera mínima con estado y `Captured at`, o únicamente el contenido raw disponible?
