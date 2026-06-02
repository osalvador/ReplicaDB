## Roadmap validado de Markup Forge

Este bloque refleja el estado real del código en `docs/markdown/`. La visión original del documento sigue abajo como referencia histórica.

### Ya implementado

- [x] Editor Markdown principal con CodeMirror, toolbar, atajos `Cmd/Ctrl+B`, `Cmd/Ctrl+I` y `Cmd/Ctrl+K`, undo/redo nativo, line numbers y syntax highlighting.
- [x] Layout tipo IDE local con modos `Split`, `Editor` y `Preview`.
- [x] Live Preview con pestañas `Teams`, `Jira` y `HTML`.
- [x] Copy adaptado al tab activo.
- [x] Importación de `.md`, `.markdown`, `.txt`, `.html` y `.csv`.
- [x] `Paste as Markdown` para HTML rico, TSV/CSV, texto plano e imágenes.
- [x] Abrir archivo, descargar `.md`, `Load sample` y `Clear` con confirmación.
- [x] Autosave en `localStorage` y restauración al recargar.
- [x] Drag & drop de archivos al workspace.
- [x] Markdown como fuente de verdad.
- [x] HTML navegable seguro generado desde Markdown.
- [x] Salidas Jira y Teams basadas en Markdown.
- [x] Export ZIP `Source + Artifact`.
- [x] PWA offline con `manifest.webmanifest` y `sw.js`.
- [x] Estadísticas básicas del documento.
- [x] Slash commands básicos como `/table` y `/callout`.

### Parcial

- [~] Smart paste: funciona bien para contenido rico y tablas, pero todavía falta una importación dedicada para Word y Jira.
- [~] HTML → Markdown limpio: existe para los formatos soportados por la importación actual.
- [~] Tabla inteligente: hay parsing/render/import de tablas, pero no reparación/export avanzado.

### Pendiente priorizado

#### P0

1. Refinar smart ingestion para Word y Jira.
2. Optimizar el buscador del Markdown para `Cmd/Ctrl+F`.
3. Corregir el color de selección del editor para que el texto seleccionado siga siendo legible.

#### P1

4. Tabla inteligente avanzada.
5. Templates reales por caso de uso.
6. Snippets con variables.
7. Linter Markdown.
8. Formatter Markdown.
9. Diff entre Markdown origen y salida.
10. Preview por destino.
11. Exportación avanzada (`.pdf`, `.docx`, `.txt`, `.json`, ZIP ampliado).

#### P2

12. Importación avanzada.
13. Historial local con búsqueda.
14. Workspace local.
15. Accessibility quick check.
16. Token cost estimator.
17. Format comparator.
18. Template + Data export.
19. Noisy diff detector.
20. Safe HTML policy profiles.
21. Privacy mode visible.

#### P3

22. Document Recipes.
23. AI Output Cleanup.
24. IA opcional explícita.
25. Prompt/Agent Profile Packs.
26. PR Review Report.
27. Stakeholder Report.
28. Architecture / ADR Mode.
29. Mermaid y diagramas.
30. Keyboard-first UX completa.
31. Búsqueda y reemplazo avanzada.
32. Outline / Document Navigator.
33. Folding de secciones.
34. Frontmatter Manager.
35. Assets Manager.
36. Clipboard Output Manager.

### Notas

- `Command Palette` no se prioriza porque fue retirada de la visión del producto.
- El inspector de documento no aporta valor suficiente para la versión actual y queda fuera.

---

Perfecto. Partiendo de tu nueva dirección, el producto debería dejar de parecer “una colección de conversores” y pasar a ser:

Markup Forge: un IDE de Markdown con una única fuente editable y múltiples salidas controladas.

El modelo mental sería:

Entradas externas → Normalización a Markdown → Editor/IDE Markdown → Salidas especializadas

Es decir:

* El Markdown es la fuente de verdad.
* El usuario puede abrir, pegar, escribir, editar o importar desde portapapeles.
* Todo lo que venga de Teams, HTML, web, email, Word, Jira, etc., primero se transforma a Markdown.
* Desde ese Markdown se generan las salidas: Teams, Jira, Confluence, GitHub, HTML, email, PDF, DOCX, agentes, etc.
* No hay “muchos conversores sueltos”; hay un único editor potente con exportadores/perfiles de salida.

⸻

Roadmap consolidado por importancia e impacto

P0 — Core estratégico del producto

1. IDE Markdown como experiencia principal

Esta es la pieza más importante. Markup Forge debe girar alrededor de un editor Markdown potente, no alrededor de botones de conversión.

Funciones clave:

* Editor Markdown avanzado.
* Preview en tiempo real.
* Modo split: editor + preview.
* Modo triple opcional: Markdown origen + preview + salida generada.
* Sintaxis resaltada.
* Autocompletado Markdown.
* Shortcuts de teclado.
* Comandos rápidos tipo /table, /code, /callout, /jira, /email.
* Soporte para tablas, listas, código, citas, checklists, enlaces, imágenes y frontmatter.
* Drag & drop de archivos .md, .txt, .html, .csv.
* Abrir archivo local.
* Guardar/exportar archivo Markdown.
* Restaurar contenido anterior.
* Modo focus writing.
* Modo oscuro/claro.

Impacto: convierte Markup Forge en una herramienta diaria, no en una utilidad puntual.

⸻

2. Markdown como fuente única de verdad

El producto debe asumir que todo termina en Markdown antes de salir a cualquier destino.

Esto implica:

* El editor siempre contiene Markdown normalizado.
* Cualquier input externo se transforma primero a Markdown.
* Todas las salidas se generan desde ese Markdown.
* El usuario puede corregir manualmente el Markdown antes de exportar.
* El Markdown puede guardarse como documento fuente.
* Las salidas son artefactos derivados, no la fuente principal.

Modelo recomendado:

Clipboard / HTML / Teams / Web / File
        ↓
Markdown normalizado
        ↓
Editor IDE Markdown
        ↓
Profiles de salida

Impacto: simplifica el producto, reduce complejidad mental y permite construir una arquitectura más sólida.

⸻

3. Paste inteligente desde portapapeles

El botón Paste debe ser una feature estrella.

No debe limitarse a pegar texto plano. Debe detectar el origen o el tipo de contenido y convertirlo a Markdown limpio.

Casos prioritarios:

* HTML copiado desde una web → Markdown.
* Texto enriquecido copiado desde Teams → Markdown.
* Email/Outlook → Markdown.
* Word/Google Docs → Markdown.
* Jira/Confluence → Markdown.
* Tabla copiada desde Excel → Markdown table.
* Tabla HTML → Markdown table.
* Texto plano → Markdown limpio.
* Código/logs → bloque de código.
* Lista pegada mal formateada → lista Markdown correcta.

Debe haber una opción tipo:

Paste as Markdown

Y otra más avanzada:

Smart Paste & Clean

Impacto: resuelve uno de los mayores dolores reales: copiar contenido desde herramientas que rompen formato.

⸻

4. Perfiles de salida desde el mismo Markdown

En vez de tener conversores separados, Markup Forge debería tener un selector de salida.

Ejemplo:

Fuente: Markdown
Salida: Teams | Jira | Confluence | GitHub | HTML | Email | PDF | DOCX | Agent Markdown

Perfiles prioritarios:

* Teams-friendly.
* Jira.
* Confluence.
* GitHub/GitLab Markdown.
* Email HTML.
* Safe HTML.
* Navigable HTML.
* Plain text.
* PDF.
* DOCX.
* Agent-ready Markdown.
* Executive summary format.
* Compact mobile format.

Cada perfil puede tener reglas propias:

* Mantener/quitar emojis.
* Convertir tablas a listas.
* Compactar espacios.
* Mantener bloques de código.
* Transformar headings.
* Convertir enlaces.
* Adaptar listas anidadas.
* Añadir tabla de contenidos.
* Generar HTML seguro.
* Optimizar para copiar/pegar.

Impacto: el usuario escribe una vez y obtiene múltiples salidas confiables.

⸻

5. Markdown → Safe Navigable HTML

Esta es una de las funcionalidades más diferenciales por el debate Markdown vs HTML.

Desde el Markdown origen, Markup Forge debería generar HTML navegable para humanos:

* Tabla de contenidos.
* Navegación lateral.
* Secciones colapsables.
* Anclas por heading.
* Botones de copiar en bloques de código.
* Tablas responsive.
* Callouts visuales.
* Badges de estado.
* Modo impresión.
* HTML standalone.
* Sin JavaScript por defecto.
* Sin llamadas externas.
* Sin CDNs.
* Fuentes del sistema.
* CSS embebido seguro.

Nombre posible:

Publish as Safe HTML

Impacto: convierte una pared de Markdown largo en un artefacto profesional para humanos.

⸻

6. Source + Artifact Mode

El producto debería permitir trabajar con el patrón:

Markdown source + HTML artifact

Es decir:

* El Markdown se mantiene como fuente editable.
* El HTML se genera como artefacto visual.
* Se pueden exportar ambos juntos.
* El HTML puede regenerarse desde el Markdown.
* El Markdown se puede versionar en Git.
* El HTML se puede compartir con stakeholders.

Casos ideales:

* Weekly status.
* Informes técnicos.
* ADRs.
* RFCs.
* Postmortems.
* Release notes.
* Informes ejecutivos.
* Revisiones de PR.
* Documentos generados por IA.

Impacto: resuelve el dilema central: Markdown para edición/trazabilidad, HTML para lectura humana.

⸻

P1 — Diferenciación fuerte

7. Format Decision Engine

Un asistente interno que recomiende la salida adecuada según el caso.

El usuario podría elegir:

* Lo leerá una persona.
* Lo leerá otro agente.
* Lo leerán personas y agentes.
* Debe vivir en Git.
* Debe pegarse en Teams.
* Debe ir a Jira.
* Debe enviarse por email.
* Debe ser un informe visual.
* Debe alimentar otro prompt.
* Debe ser seguro/offline.

Y Markup Forge recomienda:

Caso	Recomendación
Humano	HTML navegable
Agente	Markdown estructurado
Humano + agente	Markdown source + HTML artifact
Git	Markdown
Teams	Teams-friendly
Jira	Jira profile
Email	Email HTML
CI/CD	Markdown/JSON
Informe largo	Safe navigable HTML

Impacto: Markup Forge no solo convierte; ayuda a decidir.

⸻

8. Inspector de documento

Un panel lateral que analice el Markdown actual.

Debe mostrar:

* Palabras.
* Caracteres.
* Tiempo estimado de lectura.
* Número de headings.
* Número de enlaces.
* Número de tablas.
* Número de imágenes.
* Número de bloques de código.
* Número de checklists.
* Complejidad del documento.
* Longitud por sección.
* Elementos potencialmente problemáticos.
* Compatibilidad por destino.
* Recomendaciones.

Ejemplo:

“Este documento tiene 180 líneas. Para lectura humana se recomienda generar HTML navegable.”

O:

“Esta tabla puede no pegarse bien en Teams. Recomendación: convertir a lista compacta.”

Impacto: aumenta confianza y convierte el editor en una herramienta profesional.

⸻

9. Validador de compatibilidad por destino

Antes de copiar o exportar, Markup Forge debería avisar si algo puede romperse.

Destinos:

* Teams.
* Jira.
* Confluence.
* GitHub.
* Outlook/email.
* HTML.
* PDF.
* Agent Markdown.

Checks:

* Tablas no soportadas.
* HTML embebido.
* Listas demasiado anidadas.
* Bloques de código sin lenguaje.
* Imágenes externas.
* Enlaces rotos o sospechosos.
* Headings mal ordenados.
* Markdown no estándar.
* Elementos incompatibles con email.
* Elementos que se perderán al pegar en Teams.

Impacto: evita frustración antes de pegar o compartir.

⸻

10. Fix for Teams

Un botón muy concreto y de alto valor:

Fix for Teams

Transforma el Markdown para que se pegue mejor en Teams.

Acciones posibles:

* Simplificar tablas.
* Convertir tablas complejas a listas.
* Compactar espacios.
* Ajustar listas anidadas.
* Mantener código legible.
* Convertir links a formato seguro.
* Evitar formatos que Teams rompe.
* Generar versión “copy-ready”.

Impacto: problema frecuente, solución inmediata, valor muy claro.

⸻

11. Tabla inteligente

Las tablas son uno de los mayores dolores en Markdown y copy/paste.

Funciones:

* Excel/CSV → Markdown table.
* HTML table → Markdown table.
* Markdown table → CSV.
* Markdown table → HTML table.
* Markdown table → Jira table.
* Markdown table → Confluence table.
* Tabla → lista.
* Tabla → layout Teams-friendly.
* Alinear columnas.
* Reparar tablas rotas.
* Detectar columnas inconsistentes.
* Ordenar filas.
* Limpiar celdas.
* Convertir tabla a cards para HTML.

Impacto: feature muy práctica y muy reutilizable.

⸻

12. Templates orientados a casos reales

Los templates deberían crearse dentro del editor Markdown.

Packs recomendados:

Product / Agile

* User story.
* Acceptance criteria.
* Bug report.
* Release notes.
* Product brief.
* Roadmap item.

Engineering

* ADR.
* RFC.
* Postmortem.
* Runbook.
* API documentation.
* Technical assessment.
* Architecture decision report.

Management

* Weekly status.
* Executive summary.
* Meeting notes.
* Decision log.
* Stakeholder update.

Support / Ops

* Incident report.
* Troubleshooting guide.
* Customer response.
* Escalation summary.

AI workflows

* Agent instruction file.
* Claude/Cursor/Copilot task spec.
* PR review report.
* Prompt output review.
* Markdown source + HTML artifact template.

Impacto: reduce fricción de inicio y orienta el producto a casos de uso reales.

⸻

13. Snippets con variables

Biblioteca reutilizable dentro del editor.

Funciones:

* Crear snippets.
* Categorías.
* Favoritos.
* Variables tipo {{project}}, {{date}}, {{owner}}.
* Insertar snippet con shortcut.
* Exportar/importar snippets.
* Packs de snippets.
* Snippets para Teams, Jira, email, PRs, releases, incidentes.
* Snippets técnicos: callouts, warnings, checklists, bloques de código.

Ejemplo:

## Decision
**Status:** {{status}}  
**Owner:** {{owner}}  
**Date:** {{date}}
### Context
### Options considered
### Decision
### Consequences

Impacto: aumenta retención: el usuario vuelve porque tiene su biblioteca.

⸻

14. Agent-ready Markdown

Un perfil específico para contenido que van a consumir agentes o pipelines.

Debe generar Markdown:

* Limpio.
* Estructurado.
* Sin decoración visual innecesaria.
* Con headings consistentes.
* Con frontmatter opcional.
* Con IDs de sección.
* Con tablas normalizadas.
* Con bloques de código etiquetados.
* Con separadores claros.
* Compatible con Claude Code, Cursor, Copilot, ChatGPT, pipelines internos.

Ejemplo:

---
type: technical_analysis
audience: agent
version: 1.0
---
# Context
# Constraints
# Requirements
# Risks
# Expected Output

Impacto: posiciona Markup Forge en la nueva ola de outputs para agentes.

⸻

15. HTML Risk Scanner

Si el usuario pega HTML generado por IA o copiado desde web, Markup Forge debería analizarlo.

Checks:

* JavaScript embebido.
* Eventos inline: onclick, onload, etc.
* Iframes.
* Formularios.
* Links externos.
* Imágenes remotas.
* CDNs.
* Imports externos.
* CSS sospechoso.
* Network calls.
* HTML minificado.
* Riesgo de XSS.
* Compatibilidad offline.
* Compatibilidad email.
* Complejidad del DOM.

Acciones:

* Limpiar HTML.
* Convertir a Markdown seguro.
* Generar Safe HTML.
* Eliminar scripts.
* Eliminar dependencias externas.
* Mostrar reporte.

Impacto: diferencia a Markup Forge como herramienta segura frente a HTML generado por IA.

⸻

16. HTML → Markdown limpio

Feature necesaria porque cada vez más agentes producirán HTML.

Debe permitir:

* Convertir HTML a Markdown.
* Eliminar estilos.
* Eliminar scripts.
* Preservar estructura.
* Convertir tablas.
* Convertir listas.
* Convertir headings.
* Preservar bloques de código.
* Extraer enlaces.
* Convertir callouts.
* Generar Markdown apto para Git/agentes.
* Normalizar contenido pegado desde webs.

Impacto: completa el flujo bidireccional y refuerza Markdown como fuente.

⸻

P2 — Producto avanzado y profesional

17. Linter Markdown

Un linter integrado tipo IDE.

Checks:

* Headings saltados.
* Listas mal indentadas.
* Tablas rotas.
* Bloques de código sin cerrar.
* Enlaces vacíos.
* Imágenes sin alt text.
* Duplicidad de headings.
* Exceso de líneas largas.
* HTML embebido no deseado.
* Frontmatter inválido.
* Espacios y saltos inconsistentes.

Acciones rápidas:

* Fix all.
* Fix table.
* Normalize headings.
* Format document.
* Remove trailing spaces.
* Normalize lists.

Impacto: hace que el editor se sienta como un IDE real.

⸻

18. Formateador automático de Markdown

Botón:

Format Markdown

Acciones:

* Normalizar headings.
* Corregir espaciado.
* Alinear tablas.
* Ordenar listas si aplica.
* Normalizar checklists.
* Limpiar saltos dobles.
* Mantener bloques de código intactos.
* Normalizar enlaces.
* Aplicar estilo de documento.

Impacto: muy útil para contenido pegado o generado por IA.

⸻

19. Diff entre Markdown origen y salida

Especialmente útil cuando se aplica un perfil.

Vistas:

* Markdown original.
* Markdown transformado.
* Salida renderizada.
* Diff de cambios.
* Cambios por regla aplicada.

Ejemplo:

“La tabla se convirtió a lista porque el perfil Teams-friendly no garantiza tablas complejas.”

Impacto: aporta transparencia y control.

⸻

20. Preview por destino

Simulación aproximada de cómo quedará el contenido.

Destinos:

* Markdown estándar.
* Teams.
* Jira.
* Confluence.
* GitHub.
* Email.
* HTML.
* PDF.
* Mobile compact.

No tiene que ser perfecto, pero sí ayudar a prever problemas.

Impacto: reduce prueba/error y aumenta confianza.

⸻

21. Exportación avanzada

Desde la fuente Markdown, permitir exportar a:

* .md
* .html
* .txt
* .pdf
* .docx
* .csv desde tablas.
* .json estructurado.
* ZIP con Markdown + HTML + assets.
* Email HTML.
* Clipboard-ready output.
* Agent-ready package.

Impacto: amplía casos de uso sin romper el modelo de fuente única.

⸻

22. Importación avanzada

Todo lo importado debe convertirse a Markdown.

Soportar:

* .md
* .txt
* .html
* .csv
* .json
* .docx, fase posterior.
* Clipboard rich text.
* Web HTML.
* Teams.
* Jira.
* Confluence.
* Email.
* Logs.
* Código.

Impacto: Markup Forge se convierte en normalizador universal hacia Markdown.

⸻

23. Historial local con búsqueda

Como todo ocurre en el navegador, el historial puede ser local.

Funciones:

* Historial de documentos.
* Búsqueda.
* Pin/favoritos.
* Restaurar versión.
* Comparar versiones.
* Borrado automático.
* Modo incógnito.
* Exportar historial.
* Importar historial.
* Cifrado local opcional.

Impacto: retención y confianza, especialmente en entornos corporativos.

⸻

24. Workspace local

Un paso más allá del historial.

Funciones:

* Documentos recientes.
* Plantillas propias.
* Snippets.
* Perfiles.
* Configuración.
* Preferencias de exportación.
* Import/export de workspace.
* Guardado local.
* Modo offline.
* Sin backend obligatorio.

Impacto: convierte Markup Forge en una app personal, no en una página.

⸻

25. PWA offline

Instalable y funcional sin conexión.

Funciones:

* Instalar como app.
* Service worker.
* Funciona offline.
* Abrir archivos.
* Guardar últimos documentos.
* Icono propio.
* Atajos.
* Pantalla “What’s new”.
* Actualizaciones controladas.

Impacto: refuerza privacidad y uso diario.

⸻

P3 — Gobernanza, seguridad y enterprise-lite

26. Accessibility Quick Check

Para HTML generado desde Markdown.

Checks:

* Jerarquía de headings.
* Alt text en imágenes.
* Links descriptivos.
* Contraste básico.
* Landmarks.
* Tablas con encabezados.
* Orden lógico.
* Idioma del documento.
* Compatibilidad teclado si hay interacción.

Impacto: profesionaliza las salidas HTML.

⸻

27. Token Cost Estimator

Estimador aproximado para outputs de agentes.

Comparar:

* Markdown.
* Lean HTML.
* Rich HTML.
* JSON.
* Markdown + HTML artifact.

Mostrar:

* Tokens estimados.
* Diferencia porcentual.
* Coste estimado por modelo configurable.
* Coste mensual según volumen.
* Recomendación.

Impacto: útil para equipos que empiezan a gobernar costes de IA.

⸻

28. Format Comparator

Comparador entre formatos de salida.

Ejemplo:

Métrica	Markdown	HTML
Legibilidad humana	Media	Alta
Git diff	Alta	Baja
Interactividad	Baja	Alta
Seguridad	Alta	Media
Tokens	Bajo	Alto
Ideal para agentes	Sí	No
Ideal para stakeholders	No	Sí

Impacto: educa y ayuda a decidir.

⸻

29. Template + Data Export

Para evitar diffs ruidosos en HTML.

Exportar:

/report
  source.md
  template.html
  data.json
  styles.css
  README.md

Ventajas:

* Markdown como fuente.
* JSON como datos.
* HTML como presentación.
* Diffs más limpios.
* Mejor para Git.
* Regeneración controlada.

Impacto: feature avanzada para equipos técnicos.

⸻

30. Noisy Diff Detector

Analiza si una salida HTML será mala para revisar en Git.

Checks:

* HTML muy verboso.
* CSS inline excesivo.
* Minificado.
* Mucha estructura repetida.
* Datos mezclados con presentación.
* Cambios difíciles de auditar.

Recomendaciones:

* Convertir a Markdown.
* Exportar template + data.
* Simplificar HTML.
* Guardar solo artifact fuera de Git.

Impacto: útil para equipos que versionan artefactos.

⸻

31. Safe HTML Policy Profiles

Perfiles de seguridad configurables:

* Strict: sin JS, sin externos, sin iframes, sin formularios.
* Email-safe.
* Offline-safe.
* Internal report.
* Interactive allowed.
* Custom corporate profile.

Impacto: prepara el producto para uso corporativo.

⸻

32. Privacy Mode visible

La app debería comunicar claramente:

* Todo se ejecuta localmente.
* No se envía contenido a servidores.
* Sin APIs externas por defecto.
* Modo incógnito.
* Borrar sesión.
* Borrar historial.
* Cifrado local opcional.

Impacto: confianza inmediata en documentos sensibles.

⸻

P4 — Productividad y AI opcional

33. Document Recipes

El usuario no elige solo una plantilla, sino un objetivo.

Ejemplos:

* “Crear release notes para Jira y Teams.”
* “Crear informe semanal para stakeholders.”
* “Crear ADR y exportar HTML.”
* “Preparar PR review.”
* “Convertir notas sueltas en postmortem.”
* “Preparar prompt para agente de código.”

La receta define:

* Template Markdown.
* Snippets sugeridos.
* Perfil de salida.
* Validaciones.
* Export recomendado.

Impacto: guía al usuario hacia resultados completos.

⸻

34. AI Output Cleanup

Modo para limpiar outputs generados por IA.

Acciones:

* Normalizar Markdown.
* Quitar repetición.
* Ordenar estructura.
* Corregir headings.
* Convertir tablas.
* Añadir frontmatter.
* Separar decisiones, riesgos y acciones.
* Generar versión para humanos o agentes.

Impacto: muy alineado con el uso moderno de IA.

⸻

35. IA opcional

No la pondría como core, porque la privacidad local es una ventaja. Pero puede ser un modo opcional y explícito.

Funciones posibles:

* Resumir.
* Reescribir tono.
* Traducir conservando Markdown.
* Generar release notes.
* Crear user stories.
* Extraer tareas.
* Generar resumen ejecutivo.
* Convertir notas en documento estructurado.
* Revisar claridad.
* Sugerir formato de salida.

Regla de producto:

Core local. IA solo bajo acción explícita.

Impacto: potente, pero no debe distraer del editor/conversor.

⸻

36. Prompt/Agent Profile Packs

Perfiles para herramientas de IA:

* Claude Code.
* Cursor.
* Copilot.
* ChatGPT.
* Gemini.
* Agents internos.
* CI agent.

Generar Markdown adaptado a cada uno:

* Instrucciones claras.
* Criterios verificables.
* Contexto.
* Restricciones.
* Expected output.
* Acceptance criteria.
* No scope creep.
* Simplicity first.
* Surgical changes.

Impacto: Markup Forge puede ayudar a preparar mejores inputs para agentes.

⸻

37. PR Review Report

Template/salida específica.

Desde Markdown o output de agente:

* Resumen.
* Archivos afectados.
* Severidades.
* Riesgos.
* Cambios sugeridos.
* Checklist.
* Bloques colapsables por archivo.
* Export HTML para humanos.
* Markdown para Git.

Impacto: caso de uso muy potente para desarrolladores.

⸻

38. Stakeholder Report

Template/salida específica.

* Resumen ejecutivo.
* Decisiones.
* Estado.
* Riesgos.
* Próximos pasos.
* Secciones colapsables.
* Export HTML/PDF/email.
* Fuente Markdown.

Impacto: conecta ingeniería/producto con negocio.

⸻

39. Architecture / ADR Mode

Modo especializado para arquitectura.

* ADR template.
* RFC template.
* Diagrama Mermaid.
* Decisiones.
* Trade-offs.
* Consecuencias.
* Riesgos.
* Export Markdown + HTML.
* Tabla de decisiones.
* Historial de cambios.

Impacto: muy alineado con perfiles técnicos y arquitectos.

⸻

40. Mermaid y diagramas

Soporte dentro del Markdown editor:

* Mermaid preview.
* Exportar como SVG/PNG.
* Mantener código Mermaid en Markdown.
* Incluir diagrama en HTML.
* Validar sintaxis básica.
* Templates de sequence, flowchart, C4 básico.

Impacto: eleva el editor de Markdown a herramienta técnica seria.

⸻

P5 — Experiencia avanzada de editor

41. Command Palette

Como en un IDE.

Ejemplos:

* Convert to Teams.
* Export Safe HTML.
* Insert table.
* Insert ADR template.
* Format document.
* Fix for Teams.
* Scan HTML.
* Generate TOC.
* Convert table to list.
* Toggle preview.
* Open recent file.

Impacto: experiencia power-user.

⸻

42. Keyboard-first UX

Atajos para:

* Guardar.
* Abrir.
* Exportar.
* Preview.
* Insertar bloque de código.
* Insertar tabla.
* Buscar.
* Reemplazar.
* Formatear.
* Pegar inteligente.
* Cambiar perfil de salida.

Impacto: uso frecuente más rápido.

⸻

43. Búsqueda y reemplazo avanzada

Funciones:

* Buscar en documento.
* Regex opcional.
* Buscar headings.
* Buscar enlaces.
* Buscar TODOs.
* Buscar tablas.
* Reemplazar.
* Navegar por secciones.

Impacto: básico para un editor tipo IDE.

⸻

44. Outline / Document Navigator

Panel lateral con estructura del documento.

* Headings.
* Secciones.
* Conteo por sección.
* Drag & drop para mover secciones, fase avanzada.
* Detección de secciones largas.
* Estado por sección.

Impacto: esencial para documentos largos.

⸻

45. Folding de secciones en editor

Permitir colapsar:

* Headings.
* Bloques de código.
* Tablas.
* Callouts.
* Frontmatter.

Impacto: mejora mucho la edición de documentos largos.

⸻

46. Frontmatter Manager

Editor visual de metadata.

Campos:

* title.
* type.
* status.
* owner.
* date.
* audience.
* tags.
* version.
* output_profile.
* privacy.
* source.

Impacto: útil para Agent-ready Markdown, docs y artefactos.

⸻

47. Assets Manager

Gestión local de imágenes y recursos.

* Pegar imagen.
* Referenciar imagen.
* Convertir imagen a data URI para HTML standalone, opcional.
* Detectar imágenes remotas.
* Alt text.
* Lista de assets usados.
* Export ZIP con assets.

Impacto: necesario para HTML/PDF/DOCX más completos.

⸻

48. Clipboard Output Manager

Después de generar una salida, ofrecer:

* Copiar como Markdown.
* Copiar como HTML.
* Copiar para Teams.
* Copiar para Jira.
* Copiar texto plano.
* Copiar solo sección seleccionada.
* Copiar tabla como CSV.
* Copiar bloque de código.

Impacto: mejora el flujo real de trabajo.

⸻

Agrupación final por módulos de producto

1. Markdown IDE

El corazón del producto.

Incluye:

* Editor avanzado.
* Preview.
* Outline.
* Linter.
* Formatter.
* Folding.
* Search/replace.
* Command palette.
* Snippets.
* Templates.
* Frontmatter.
* Mermaid.
* Historial.
* Workspace local.

⸻

2. Smart Ingestion

Todo lo que entra se convierte a Markdown.

Incluye:

* Paste inteligente.
* HTML → Markdown.
* Teams → Markdown.
* Web → Markdown.
* Email → Markdown.
* Word/Docs → Markdown.
* Excel/CSV → Markdown table.
* Logs/code → Markdown.
* File import.

⸻

3. Output Profiles

Todo lo que sale parte del Markdown.

Incluye:

* Teams.
* Jira.
* Confluence.
* GitHub/GitLab.
* Email HTML.
* Safe HTML.
* Navigable HTML.
* Plain text.
* PDF.
* DOCX.
* Agent-ready Markdown.
* JSON estructurado.
* Template + data.

⸻

4. Trust & Validation

La capa de seguridad, calidad y compatibilidad.

Incluye:

* Inspector.
* Compatibility checker.
* Safe HTML scanner.
* Accessibility check.
* Noisy diff detector.
* Token estimator.
* Format comparator.
* Privacy mode.
* Output warnings.

⸻

5. Recipes & Workflows

Casos de uso completos.

Incluye:

* PR review.
* ADR/RFC.
* Weekly status.
* Release notes.
* Bug report.
* User story.
* Incident postmortem.
* Stakeholder report.
* Agent task spec.
* AI output cleanup.

⸻

Roadmap recomendado

Fase 1 — Convertir Markup Forge en un IDE Markdown real

Prioridad máxima.

Entregables:

1. Editor Markdown avanzado.
2. Preview split.
3. Abrir/guardar archivo.
4. Paste inteligente básico.
5. HTML → Markdown limpio.
6. Teams/web/email rich text → Markdown básico.
7. Formatter Markdown.
8. Outline del documento.
9. Export Markdown.
10. Export Teams/Jira/GitHub/HTML básico.

Objetivo:

Que el usuario use Markup Forge como editor principal de contenido Markdown.

⸻

Fase 2 — Fuente única + salidas profesionales

Entregables:

1. Selector de perfiles de salida.
2. Teams-friendly output.
3. Jira output.
4. Confluence output.
5. GitHub/GitLab output.
6. Email HTML.
7. Safe Navigable HTML.
8. Source + Artifact Mode.
9. Copy output manager.
10. Preview por destino básico.

Objetivo:

Escribes una vez en Markdown y exportas de forma fiable al destino correcto.

⸻

Fase 3 — Calidad, validación y confianza

Entregables:

1. Inspector de documento.
2. Compatibility checker.
3. Fix for Teams.
4. Tabla inteligente.
5. Linter Markdown.
6. Safe HTML scanner.
7. Accessibility quick check.
8. Diff origen/salida.
9. Privacy mode.
10. Historial local.

Objetivo:

Que el usuario confíe en la salida antes de copiarla o compartirla.

⸻

Fase 4 — Workflows y productividad diaria

Entregables:

1. Templates por caso de uso.
2. Snippets con variables.
3. Document recipes.
4. Workspace local.
5. PWA offline.
6. Command palette.
7. Keyboard-first UX.
8. Mermaid preview.
9. Frontmatter manager.
10. Export PDF/DOCX.

Objetivo:

Que Markup Forge sea una herramienta recurrente, no una utilidad puntual.

⸻

Fase 5 — Era agentes / IA / enterprise-lite

Entregables:

1. Agent-ready Markdown.
2. AI output cleanup.
3. Prompt/agent profile packs.
4. Token cost estimator.
5. Format decision engine.
6. Format comparator.
7. Template + Data export.
8. Noisy diff detector.
9. Safe HTML policy profiles.
10. IA opcional explícita.

Objetivo:

Posicionar Markup Forge como herramienta de referencia para convertir outputs de IA en artefactos útiles, seguros y trazables.

⸻

Top 20 final por impacto

Orden descendente:

1. IDE Markdown como experiencia principal
2. Markdown como fuente única de verdad
3. Paste inteligente hacia Markdown
4. Perfiles de salida desde Markdown
5. Markdown → Safe Navigable HTML
6. Source + Artifact Mode
7. HTML → Markdown limpio
8. Inspector de documento
9. Validador de compatibilidad por destino
10. Fix for Teams
11. Tabla inteligente
12. Templates reales
13. Snippets con variables
14. Linter Markdown
15. Formatter Markdown
16. Preview por destino
17. Safe HTML Scanner
18. Agent-ready Markdown
19. Historial/workspace local
20. PWA offline

⸻

Visión final de producto

Yo lo formularía así:

Markup Forge es un IDE local de Markdown que convierte cualquier entrada en una fuente Markdown limpia y permite publicar esa fuente en el formato correcto para cada destino: Teams, Jira, Confluence, GitHub, email, HTML, PDF, DOCX o agentes de IA.

Y más corto:

One Markdown source. Every output you need.
