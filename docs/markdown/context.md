Aquí tienes el contexto completo y ordenado para pasárselo a un agente de código IA y que pueda continuar la edición del site sin perder el hilo.

⸻

Contexto de producto: Markup Forge

Estamos trabajando en una aplicación web llamada Markup Forge.

URL publicada actual:

https://osalvador.github.io/ReplicaDB/markdown/converter.html

Repositorio:

https://github.com/osalvador/ReplicaDB

Ruta de publicación en GitHub Pages:

docs/markdown/converter.html

Los assets PWA van también en:

docs/markdown/

La última versión generada es:

markup-forge-ide-v35-pwa.zip

Contiene:

converter.html
manifest.webmanifest
sw.js
markdown-copy-lab-icon.svg
markdown-copy-lab-icon-192.png
markdown-copy-lab-icon-512.png
apple-touch-icon.png
favicon-32.png

La app debe seguir siendo estática, local-first, sin backend, y todo debe ejecutarse en el navegador.

⸻

Visión actual del producto

El producto ha evolucionado desde un conversor aislado hacia un IDE local de Markdown.

Visión:

Markup Forge es un IDE local de Markdown que convierte cualquier entrada en una fuente Markdown limpia y permite publicar esa fuente en el formato correcto para cada destino.

Frase de producto:

One Markdown source. Every output you need.

En español:

Una fuente Markdown. Todas las salidas que necesitas.

Modelo mental:

Entradas externas → Normalización a Markdown → Editor/IDE Markdown → Salidas especializadas

El Markdown Source es la fuente de verdad.

⸻

Estado funcional actual

La app actual debe tener como experiencia principal:

Markdown Source → Live Preview

Funcionalidades actuales importantes

La versión actual incluye:

* Editor Markdown principal.
* Preview en tiempo real.
* Layout tipo IDE.
* Modos de vista:
    * Split
    * Editor
    * Preview
* Abrir/importar archivos:
    * .md
    * .markdown
    * .txt
* Descargar Markdown actual como .md.
* Copiar salida según pestaña activa.
* Limpiar editor con confirmación.
* Estadísticas básicas:
    * palabras
    * caracteres
    * líneas
    * headings
* Autosave local en localStorage.
* Restauración automática al recargar.
* PWA offline con manifest.webmanifest y sw.js.

⸻

Último diseño/arquitectura deseada

Layout principal

Debe parecer un IDE Markdown local.

Estructura conceptual:

Header principal
  - Markup Forge
  - One Markdown source. Every output you need.
  - View mode: Split / Editor / Preview
  - Download .md
  - Clear
Workspace
  Panel izquierdo: Markdown Source
    - Header del panel
    - Botón Open
    - Botón Load sample
    - Toolbar Markdown estilo GitHub
    - Textarea/editor Markdown
  Panel derecho: Live Preview
    - Header del panel
    - Tabs de salida
    - Profile/config selector por salida
    - Copy correspondiente
    - Preview custom por salida

⸻

Editor Markdown

El editor debe simular el editor Markdown de GitHub.

Requisitos actuales

* No debe tener pestaña interna Preview.
* Solo debe haber zona de escritura.
* La preview está únicamente en el panel derecho Live Preview.
* Debe tener toolbar estilo GitHub.
* El título Write de la pestaña del editor debe estar centrado.
* Editor con fuente monoespaciada.
* Buen contraste.
* Altura cómoda para documentos largos.
* Cmd/Ctrl+Z y redo deben funcionar con cualquier acción del editor.

Toolbar Markdown actual

Debe contener acciones como:

* Heading
* Bold
* Italic
* Quote
* Inline code
* Link
* Bulleted list
* Numbered list
* Task list
* Code block

Debe NO contener:

* Mention @
* Undo button

Motivo: undo/redo debe ser nativo con teclado.

Undo/redo

Cualquier edición debe poder deshacerse con Cmd/Ctrl+Z.

Esto incluye:

* escribir manualmente,
* acciones de toolbar,
* Load sample,
* Clear.

La implementación debe usar una estrategia compatible con el historial nativo del textarea, idealmente usando operaciones sobre selección / setRangeText / execCommand si aplica, o una estrategia propia robusta si no queda otra.

⸻

Live Preview

El panel de preview es la pieza más importante después del editor.

Estado deseado

El Live Preview no debe tener un desplegable tipo:

Markdown preview
HTML
Jira

Debe tener pestañas de salida.

Pestañas actuales deseadas:

Teams
Jira
HTML

A futuro podrían añadirse:

Confluence
GitHub
Email
PDF
DOCX
Agent

Pero no hacer scope creep salvo que se pida.

Copy button

El botón Copy debe estar dentro del panel Live Preview.

Debe cambiar según pestaña activa:

Teams → Copy for Teams
Jira  → Copy Jira
HTML  → Copy HTML

Cada pestaña debe tener su propia salida, perfil/configuración y copy.

⸻

Profiles / configuración por salida

En cada pestaña de salida debe existir un selector de perfil/configuración.

Esto se pidió explícitamente:

En cada pestaña, cuando eliges el tipo de salida, sí debemos incluir el perfil o la optimización o la configuración de cada una de las salidas, tal y como teníamos en la versión 30.

Profiles actuales esperados

Teams

Teams Rich
Teams Compact

Comportamiento deseado de Teams Compact:

* No debe eliminar todos los saltos de línea.
* Debe compactar líneas sueltas dentro del mismo párrafo.
* Debe conservar separación entre párrafos cuando hay doble salto de línea.
* Debe conservar listas, headings, quotes y code fences.
* En resumen: compacto, pero sin destruir estructura semántica.

Jira

Jira Wiki Markup
Jira Plain Text

HTML

Full HTML Document
HTML Fragment

⸻

Preview de Teams

Esta es la feature más usada. Hay que cuidarla.

Se pidió recuperar el preview de Teams de versiones anteriores, especialmente la experiencia visual tipo Teams composer.

Requisitos visuales del preview Teams

* Debe verse sobre fondo oscuro, no en un recuadro blanco general.
* Debe simular el composer/editor de Teams.
* Debe mantener altura actual del workspace.
* Debe tener:
    * toolbar superior estilo Teams,
    * contenido renderizado,
    * quote visual similar a Teams,
    * code card similar a Teams,
    * footer visual tipo composer si aplica.

Problemas anteriores a evitar

* No debe haber un fondo blanco/gris global alrededor del preview Teams.
* El quote no debe ser solo una línea vertical; en Teams real va en un recuadro/área con borde y barra vertical interna.
* Los code blocks no deben crear demasiado espacio vertical antes/después.
* El código debe mostrarse con colores tipo hljs siempre que sea posible.
* No debe aparecer código ilegible con fondo blanco sobre fondo oscuro.
* El preview no debe saltar arriba/abajo mientras escribes.
* Editor y preview deben mantener una sincronización razonable de scroll, especialmente cuando se escribe al final de textos largos.

⸻

Preview HTML

La pestaña HTML puede mostrar HTML como código oscuro, o en el futuro un preview de email/documento.

Última petición aceptada:

* En general las salidas deben estar sobre fondo oscuro.
* Solo en HTML para email tenía sentido un recuadro blanco.
* En el estado actual de IDE Feature 1, HTML puede ser una salida en code block oscuro.

⸻

Preview Jira

La salida Jira debe mostrarse como texto/markup en bloque oscuro.

Debe copiar Jira markup, no HTML.

⸻

Segunda row contextual

En versiones anteriores había una segunda fila con botones. Se decidió eliminar botones inútiles.

Estado deseado actual:

* La segunda row debe explicar brevemente el conversor o el modo.
* Debe ocupar todo el ancho disponible.
* No debe tener saltos de línea si hay suficiente espacio.
* Debe evitar que textos se monten.
* En el rediseño IDE, probablemente esa row puede ser mínima o integrarse en el header.

Últimas instrucciones relacionadas:

En la segunda row es donde podemos poner el editor, el preview o ambos, con la I de información.

Pero luego se movió hacia:

* Split / Editor / Preview en header principal.
* Botón info eliminado del header.
* Dashboard/usage en sidebar.

Si hay conflicto, priorizar lo último del producto: IDE limpio, sin paleta Command K, sin botón info visible por defecto.

⸻

Command palette

Se eliminó.

No debe existir:

* Botón ⌘K Command.
* Atajo Cmd/Ctrl+K.
* Paleta de comandos.

Motivo: no hay suficientes acciones como para justificarla ahora.

⸻

Sidebar / dashboard / info

En una versión anterior había sidebar lateral con utilities, snippets, history, profiles, etc.

En la visión IDE Feature 1, mucho de eso quedó fuera de alcance.

Pero el usuario pidió:

La I de información debería eliminarse de ahí y pasarla al sidebar menú general, donde podemos poner el icono de un usuario como si fuera su profile y ahí le damos las estadísticas de todo lo que ha hecho en la aplicación.

Esto está más cerca de futuras features. Si existe algo en el código actual:

* El panel de información debe estar cerrado por defecto.
* No debe abrirse automáticamente al entrar.
* Un dashboard de uso puede existir, pero no debe dominar la experiencia principal.
* No implementar historial avanzado si se está manteniendo Feature 1 estricta.

⸻

Nombre de la app

Nombre actual:

Markup Forge

No usar:

Markdown Copy Lab
Graphite
Graphite Studio

Se pidió eliminar relación con la palabra Graphite.

El título principal debe ser:

Markup Forge

Subtítulo:

One Markdown source. Every output you need.

En la descripción inicial se pidió evitar saltos de línea innecesarios en desktop.

⸻

Iconos y PWA

Hay icono moderno ya generado.

Assets actuales esperados:

markdown-copy-lab-icon.svg
markdown-copy-lab-icon-192.png
markdown-copy-lab-icon-512.png
apple-touch-icon.png
favicon-32.png

Aunque el nombre del archivo siga teniendo markdown-copy-lab, no es ideal, pero puede mantenerse para no romper referencias. Si se cambia, actualizar manifest, HTML y sw.

PWA debe seguir funcionando:

manifest.webmanifest
sw.js

Cada versión debe incrementar cache en sw.js, por ejemplo:

markup-forge-ide-v35
markup-forge-ide-v36

⸻

Accesibilidad / Lighthouse

Chrome Lighthouse detectó problemas anteriores. Ya se corrigieron algunos, pero el agente debe tenerlos en cuenta.

Problemas detectados anteriormente

ARIA prohibido

Había aria-label en div.side-group.

Solución aplicada o recomendada:

* Usar nav, section, role="group" o quitar aria-label del div.

Botones sin nombre accesible

Botones con solo iconos deben tener:

aria-label="..."
title="..."

Contraste insuficiente en line numbers

Color anterior:

#535b68 sobre #10131a

Contraste bajo.

Se cambió a algo como:

#a7b0c0

Mantener contraste.

⸻

Estilo visual actual

La app usa un estilo oscuro, elegante, tipo editor/IDE.

Evitar:

* Estética “AI purple”.
* Referencias a Graphite.
* Botones flotantes raros en mobile.
* Sidebar flotando en el centro de móvil.

Mobile:

* Si hay sidebar/botones laterales, deben convertirse en barra inferior o layout responsive correcto.
* No deben quedar centrados flotando sobre el contenido.

⸻

Problemas recientes que se corrigieron o deben vigilarse

Header Teams to Markdown

Hubo un solapamiento entre título y profile.

Solución general:

* Headers de panel deben ser flexibles.
* Si no cabe, acciones bajan a segunda línea.
* No montar título con selector.

Drawer lateral

Hubo problema de blur: el sidebar derecho quedaba detrás del backdrop.

Solución:

* Backdrop sin blur.
* Drawer con z-index superior.
* Click fuera cierra drawer.
* Drawer nítido.

Si se mantiene drawer, respetar esto.

Botones de sidebar en móvil

Se pidió corregir que flotaran en medio.

Solución:

* En móvil, sidebar inferior fixed o layout integrado.
* justify-content: flex-start
* overflow-x: auto
* No centrar botones verticalmente en pantalla.

⸻

Estado de la última versión generada

Última versión generada por ChatGPT:

markup-forge-ide-v35-pwa.zip

Cambios de v35:

* Live Preview con tabs:
    * Teams
    * Jira
    * HTML
* Eliminado dropdown Markdown preview / HTML / Jira.
* Copy sincronizado con pestaña activa.
* Añadido selector profile/config por salida:
    * Teams Rich / Teams Compact
    * Jira Wiki Markup / Jira Plain Text
    * Full HTML Document / HTML Fragment
* Preview vuelve a fondo oscuro.
* Eliminado recuadro blanco/gris global alrededor del preview.
* Teams usa panel oscuro directamente.
* Jira y HTML como bloques de código oscuros.
* Teams Compact conserva estructura.
* Cache PWA actualizada a markup-forge-ide-v35.

⸻

Funciones / arquitectura recomendada

El código debería mantener separación conceptual:

markdownSource
renderPreview(markdown)
renderTeamsPreview(markdown, profile)
renderJiraOutput(markdown, profile)
renderHtmlOutput(markdown, profile)
updateStats(markdown)
loadFile(file)
downloadMarkdown(markdown, filename)
copyCurrentOutput()
copyMarkdown(markdown)
setViewMode(mode)
setOutputTab(tab)
setOutputProfile(profile)

El Markdown sigue siendo la fuente de verdad.

⸻

Requisitos de privacidad

La app debe ser local-first:

* No enviar contenido a servidores.
* No usar APIs remotas para contenido.
* No telemetría.
* No login.
* No backend.
* No sincronización cloud.

Puede usar:

* localStorage para autosave.
* PWA/service worker.
* Clipboard API.
* File input.
* Blob download.

⸻

Fuera de alcance por ahora

No implementar salvo que se pida:

* Paste inteligente avanzado.
* Perfiles complejos.
* Export PDF/DOCX.
* Safe HTML Scanner.
* Accessibility checker avanzado.
* Token estimator.
* Agent-ready Markdown.
* Templates.
* Snippets.
* Linter avanzado.
* Formatter avanzado.
* Historial con versiones.
* Workspace local complejo.
* IA.
* Login/backend/cloud.

⸻

Cómo probar manualmente

Después de modificar:

1. Abrir converter.html.
2. Escribir Markdown.
3. Confirmar preview en Teams se actualiza.
4. Cambiar pestañas:
    * Teams
    * Jira
    * HTML
5. Cambiar profiles de cada salida.
6. Confirmar Copy cambia de label y contenido.
7. Probar toolbar:
    * bold
    * italic
    * heading
    * quote
    * list
    * code
8. Probar Cmd/Ctrl+Z tras:
    * escribir,
    * usar toolbar,
    * Load sample,
    * Clear.
9. Probar Open .md.
10. Probar Download .md.
11. Probar Clear con confirmación.
12. Recargar y confirmar autosave local.
13. Probar responsive móvil.
14. Ejecutar Lighthouse o revisar:

* accesibilidad de botones,
* contraste,
* ARIA.

⸻

Validaciones técnicas recomendadas

Si el agente puede ejecutar tests:

node --check extracted-inline-script.js

O extraer scripts inline del HTML y validarlos.

Verificar ZIP final contiene:

converter.html
manifest.webmanifest
sw.js
markdown-copy-lab-icon.svg
markdown-copy-lab-icon-192.png
markdown-copy-lab-icon-512.png
apple-touch-icon.png
favicon-32.png

Verificar que no quedan referencias no deseadas:

Graphite
Markdown Copy Lab
commandButton
Cmd+K
sourcePreviewTab
composerPreview
outputFormat select

Dependiendo de la versión, outputFormat puede haber sido reemplazado por tabs.

⸻

Peticiones pendientes / próximas mejoras probables

Estas son las cosas que probablemente el usuario seguirá pidiendo:

1. Refinar mucho más el preview Teams.
2. Mejorar sincronización scroll editor-preview.
3. Añadir outputs nuevos como Confluence/GitHub/Email.
4. Añadir paste inteligente hacia Markdown.
5. Reintroducir templates/snippets, pero como feature controlada.
6. Añadir linter/formatter Markdown.
7. Añadir inspector de documento.
8. Añadir historial local con IndexedDB más adelante.
9. Añadir export HTML/PDF/DOCX más adelante.

No implementarlas todavía si el prompt no las pide.

⸻

Instrucción final para el agente

Trabaja de forma incremental. No reescribas toda la app si no es necesario. Mantén el producto como IDE Markdown local-first con múltiples salidas. Prioriza:

1. Editor Markdown robusto.
2. Preview Teams excelente.
3. Output tabs claros.
4. Copy correcto por salida.
5. Accesibilidad y responsive.

Entregar siempre un ZIP único con todos los archivos PWA.
