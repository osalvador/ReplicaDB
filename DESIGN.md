---
name: ReplicaDB Control Plane
description: A calm, auditable control plane for heterogeneous data replication.
colors:
  brand-teal: "#0B6E69"
  terracotta: "#B15C38"
  page-green: "#F3F6F4"
  paper: "#FFFFFF"
  mist-green: "#E8F0ED"
  ink: "#1B2926"
  muted-ink: "#50625D"
  success: "#216E4A"
  info: "#1769AA"
  warning: "#8A4B08"
  error: "#B3261E"
typography:
  display:
    fontFamily: 'Georgia, "Times New Roman", serif'
    fontSize: "2.25rem"
    fontWeight: 600
    lineHeight: 1.2
    letterSpacing: "normal"
  headline:
    fontFamily: 'Georgia, "Times New Roman", serif'
    fontSize: "1.75rem"
    fontWeight: 600
    lineHeight: 1.25
    letterSpacing: "normal"
  title:
    fontFamily: 'Georgia, "Times New Roman", serif'
    fontSize: "1.5rem"
    fontWeight: 600
    lineHeight: 1.3
    letterSpacing: "normal"
  body:
    fontFamily: '"Avenir Next", "Helvetica Neue", sans-serif'
    fontSize: "1rem"
    fontWeight: 400
    lineHeight: 1.5
    letterSpacing: "normal"
  label:
    fontFamily: '"Avenir Next", "Helvetica Neue", sans-serif'
    fontSize: "0.875rem"
    fontWeight: 700
    lineHeight: 1.25
    letterSpacing: "normal"
  mono:
    fontFamily: "monospace"
    fontSize: "0.875rem"
    fontWeight: 400
    lineHeight: 1.55
    letterSpacing: "normal"
rounded:
  section: "8px"
  control: "8px"
  input: "6px"
  chip: "6px"
  pagination: "4px"
spacing:
  unit: "8px"
  control: "16px"
  section: "24px"
  page: "32px"
components:
  button-primary:
    backgroundColor: "{colors.brand-teal}"
    textColor: "{colors.paper}"
    typography: "{typography.label}"
    rounded: "{rounded.control}"
    padding: "8px 16px"
    height: "40px"
  button-secondary:
    backgroundColor: "{colors.paper}"
    textColor: "{colors.brand-teal}"
    typography: "{typography.label}"
    rounded: "{rounded.control}"
    padding: "8px 16px"
    height: "40px"
  button-text:
    backgroundColor: "transparent"
    textColor: "{colors.brand-teal}"
    typography: "{typography.label}"
    rounded: "{rounded.control}"
    padding: "8px 16px"
    height: "40px"
  input-outlined:
    backgroundColor: "{colors.paper}"
    textColor: "{colors.ink}"
    typography: "{typography.body}"
    rounded: "{rounded.input}"
    padding: "8px 14px"
    height: "40px"
  dropdown-option:
    backgroundColor: "{colors.paper}"
    textColor: "{colors.ink}"
    typography: "{typography.body}"
    rounded: "{rounded.chip}"
    padding: "8px 12px"
    height: "44px"
  dropdown-paper:
    backgroundColor: "{colors.paper}"
    textColor: "{colors.ink}"
    rounded: "{rounded.section}"
    padding: "4px"
  surface-section:
    backgroundColor: "{colors.paper}"
    textColor: "{colors.ink}"
    rounded: "{rounded.section}"
    padding: "16px 24px"
  status-chip:
    backgroundColor: "{colors.mist-green}"
    textColor: "{colors.ink}"
    typography: "{typography.label}"
    rounded: "{rounded.chip}"
    padding: "0 10px"
    height: "28px"
  navigation-bar:
    backgroundColor: "{colors.paper}"
    textColor: "{colors.ink}"
    rounded: "0px"
    padding: "8px 32px"
    height: "56px"

# Design System: ReplicaDB Control Plane

## Overview

**Creative North Star: "The Engineering Ledger"**

ReplicaDB's control plane reads as a working ledger for data movement:
composed, dependable, and easy to audit. The system uses an editorial serif
for orientation and a sans-serif workhorse for values, controls, and status.
It gives dense operational material a clear frame without turning
infrastructure into spectacle.

Teal is the primary signal, terracotta is a secondary counterpoint, and
semantic colors are reserved for state. White surfaces sit on a pale
green-gray page; borders do the routine structural work and shadows arrive
only for genuinely lifted layers such as dialogs. The visual language is
assured, pragmatic, and human: technical enough for repeated operations,
warm enough to keep the interface legible under pressure. It avoids sterile
enterprise gloss and unexplained jargon.

**Key Characteristics:**
- Quietly structured, scan-first surfaces
- Editorial headings over operational sans-serif detail
- Teal-led actions with terracotta as a secondary signal
- Restrained corners, visible focus, and semantic state color
- Flat resting surfaces with purposeful elevation

**The Ledger Before Theater Rule.** Every visual decision serves confident
configuration, scanning, or diagnosis; calmness comes from clarity rather than
decoration.

## Colors

The palette is built around a deep working teal and grounded terracotta, held
by cool green neutrals and explicit semantic signals.

### Primary
- **Deep Teal** (`{colors.brand-teal}`): Primary actions, links, selected
  indicators, and the visible focus ring.

### Secondary
- **Terracotta** (`{colors.terracotta}`): Secondary emphasis and supporting
  action paths. It should not compete with the primary teal.

### Neutral
- **Cool Page Green** (`{colors.page-green}`): The application canvas around
  working surfaces.
- **Paper White** (`{colors.paper}`): Sections, cards, dialogs, tables, and
  form controls.
- **Mist Green** (`{colors.mist-green}`): Subtle table headers, disabled
  surfaces, and low-emphasis grouping.
- **Ink** (`{colors.ink}`): Primary text and high-contrast headings.
- **Muted Ink** (`{colors.muted-ink}`): Descriptions, secondary text, borders,
  and dividers.

### Semantic states
- **Success Green** (`{colors.success}`): Completed replication and positive
  state.
- **Information Blue** (`{colors.info}`): Informational guidance and neutral
  system context.
- **Warning Ochre** (`{colors.warning}`): Caution, pending intervention, or
  partial risk.
- **Error Red** (`{colors.error}`): Failed operations and validation errors.

**The Teal/Rust Division Rule.** Teal carries primary action and focus;
terracotta carries secondary emphasis; semantic colors communicate state, not
decoration.

## Typography

**Display Font:** Georgia (with Times New Roman fallback)
**Body Font:** Avenir Next (with Helvetica Neue and sans-serif fallbacks)
**Label/Mono Font:** System monospace for captured run logs only

**Character:** The pairing gives operational screens a measured editorial
entry point and a crisp workhorse underneath. Serif headings orient the user;
the sans-serif carries dense values, controls, status labels, and navigation.

### Hierarchy
- **Display** (600, 2.25rem, 1.2): Primary page-level orientation when the
  largest heading treatment is needed.
- **Headline** (600, 1.75rem, 1.25): Secondary page hierarchy and major
  content groupings.
- **Title** (600, 1.5rem, 1.3): Page headers and prominent operational titles.
- **Body** (400, 1rem, 1.5): Descriptions, values, and primary reading text.
  Secondary body copy uses 0.875rem with a 1.45 line-height.
- **Label** (700, 0.875rem, 1.25): Buttons and compact control labels.
  Table headers tighten to 0.75rem, use 0.04em tracking, and are uppercase
  for fast scanning.
- **Mono** (400, 0.875rem, 1.45): Preserved multiline run diagnostics and
  captured logs, with wrapping and local horizontal overflow as needed.

**The Heading/Body Handshake Rule.** Georgia headings provide editorial
orientation; Avenir carries operational copy and controls.

## Layout

The control plane is desktop-first and remains usable on mobile web through
wrapping, responsive grids, and local overflow for wide tables and logs. A
top AppBar anchors navigation and identity; the main content is centered with
a maximum width of 1600px. Page content uses 16px horizontal padding on small
screens, 24px from the small breakpoint, and 32px from the medium breakpoint,
with 16px vertical padding on small screens and 32px on medium screens.

Sections commonly stack on an 24px rhythm. Page headers place title and action
clusters side by side from the small breakpoint upward, then stack them on
small screens. Form grids collapse to one column on narrow screens and use
two or three columns only when the content supports it. Tables keep a useful
minimum width inside an overflow container rather than shrinking operational
columns into unreadability.

The navigation remains top-aligned at every size. There is no default bottom
navigation: actions stay beside the context they affect.

## Elevation & Depth

The system is flat at rest and uses tonal layering plus restrained borders to
separate work areas. Standard cards, sections, the AppBar, and buttons have no
shadow. Dialogs are the deliberate exception: they receive a compact ambient
shadow so transient content reads as lifted without changing the visual
weight of the underlying work.

### Shadow Vocabulary
- **Resting surface** (`box-shadow: none`): Default for sections, cards,
  buttons, and navigation.
- **Low ambient** (`0 1px 2px 0 rgba(27, 41, 38, 0.08)`): Available for a
  subtle transient lift, not a default container treatment.
- **Dialog lift** (`0 2px 8px 0 rgba(27, 41, 38, 0.12)`): Separation for
  modal surfaces.
- **Field focus wash** (`0 0 0 3px rgba(11, 110, 105, 0.16)`): A state cue,
  not elevation.

**The Flat Resting Surface Rule.** White sections are flat at rest; shadow
appears only where a dialog needs separation.

## Shapes

The form language is contained and repeatable. Sections, buttons, icon
buttons, dialogs, and the global shape default use the 8px radius. Inputs and
chips tighten to 6px; pagination controls use 4px. One-pixel muted borders
frame sections, tables, dialogs, and fields without becoming ornamental.

Focus is always visible: interactive controls use a 3px teal outline with a
2px offset, while focused outlined inputs also strengthen their border and
receive a restrained teal wash. Corners stay contained; there are no pill
shapes in the primary layout and no decorative clipping.

**The Contained Corner Rule.** Keep radii restrained and repeat the 8px
section/control silhouette; inputs and chips tighten to 6px and pagination to
4px.

## Components

Components are restrained and dependable: compact enough for repeated
operations, explicit enough to communicate state, and always kept beside the
work they affect.

### Buttons
- **Shape:** Contained 8px corners, no elevation, and a 40px default height;
  small actions use 32px and large actions use 48px.
- **Primary:** Deep Teal fill with Paper White text and 8px by 16px internal
  padding. Use for the main action in a page or section.
- **Hover / Focus:** Hover deepens the primary fill or adds a light teal wash
  to outlined and text actions. Focus uses the shared 3px outline and 2px
  offset. Disabled buttons retain their layout and reduce opacity.
- **Secondary / Ghost / Tertiary:** Outlined actions keep a teal border and
  white surface; text actions remain quiet and context-bound. Terracotta is
  available for secondary emphasis but does not replace the primary action
  signal.

### Chips
- **Style:** Compact 28px labels with 6px corners, strong weight, and 10px
  horizontal label padding.
- **State:** Outlined chips identify modes and capabilities. Filled semantic
  chips identify statuses such as success or failure. StatusChip exposes a
  `role="status"` and a readable status label.

### Cards / Containers
- **Corner Style:** Surface sections and MUI cards use the 8px section radius.
- **Background:** Paper White on the Cool Page Green canvas; Mist Green is
  reserved for subtle grouping.
- **Shadow Strategy:** Flat at rest; use the Elevation vocabulary only for
  transient dialog separation or state.
- **Border:** One-pixel muted ink alpha border provides routine structure.
- **Internal Padding:** 16px on small screens and 24px at medium widths for
  SurfaceSection.

### Inputs / Fields
- **Style:** Outlined Paper White fields with a 6px radius and 40px minimum
  height. Labels and helper text remain in the Avenir workhorse voice.
- **Focus:** Teal border at 2px plus the shared 3px teal focus wash.
- **Error / Disabled:** Error borders use Error Red. Disabled fields move to
  Mist Green and preserve readable text contrast.

### Dropdowns
- **Paper:** Select and Autocomplete menus share a Paper White surface, a
  muted one-pixel border, an 8px radius, and a restrained low ambient shadow.
- **Options:** Primary labels use a moderate 500 weight; secondary descriptions
  and connector types use 400 weight in Muted Ink. Rows use 8px by 12px
  padding and 6px corners, with natural wrapping for long names.
- **States:** Hover and keyboard focus use a light teal wash; selected options
  use a stronger teal wash. The state treatment is shared across menu types.

### Navigation
- **Style:** A white, flat AppBar with a muted bottom border and no bottom
  navigation. ReplicaDB is a teal wordmark link, primary destinations sit
  beside it, and signed-in identity plus logout stay grouped at the far edge.
- **Responsive:** Toolbar content wraps on narrow screens; identity and
  actions remain visible without introducing a second navigation model.

### Tables and Run Diagnostics
- **Style:** Tables are white, dense, and locally scrollable when needed.
  Headers use Mist Green, uppercase compact labels, and muted ink; rows use a
  light teal hover wash. Run status uses semantic chips.
- **Logs:** Captured diagnostics lead with a complete/partial state, captured
  time when available. Content sits in a contained neutral surface with
  monospace text, stable padding, and horizontal overflow that preserves each
  original log line. A truncated response is split into Beginning and End blocks
  around a visible marker explaining that the server omitted the middle at its
  256 KiB limit. Log loading failures provide an explicit retry action, and
  the content block remains keyboard-focusable for long or horizontally dense
  output. Copy exports the redacted content currently available.

**The Context-Carrying Action Rule.** Actions stay beside the work they
change; desktop navigation remains top-aligned, with no default bottom
navigation.

## Do's and Don'ts

### Do:
- **Do** use the teal role for primary actions, links, selected indicators,
  and focus.
- **Do** use terracotta as a secondary signal rather than a competing primary
  brand color.
- **Do** keep operational tables dense but locally scrollable and readable.
- **Do** preserve visible focus, semantic status colors, and accessible roles.
- **Do** use serif headings with sans-serif operational detail.
- **Do** keep standard surfaces flat and let borders establish structure.

### Don't:
- **Don't** introduce purple, decorative gradients, remote fonts, or ornamental
  effects that compete with job and run state.
- **Don't** use large marketing-style hero compositions for control-plane
  screens.
- **Don't** add bottom navigation as a default mobile pattern.
- **Don't** use shadows as decoration on ordinary sections, cards, or buttons.
- **Don't** expose credentials or sensitive connector values in visual output.
- **Don't** replace clear operational language with unexplained jargon.
