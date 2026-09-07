---
target: "critique revisa la home https://osalvador.github.io/ReplicaDB/"
total_score: 19
max_score: 32
na_heuristics: 5,9
p0_count: 2
p1_count: 2
target_identity: "url:https://osalvador.github.io/ReplicaDB"
timestamp: 2026-09-07T13-20-45Z
slug: osalvador-github-io-replicadb
---
Method: dual-agent (A: Explore · B: Explore; B unavailable for execution, supplemented by parent detector/HTML evidence)

## Design Health Score

| # | Heuristic | Score | Key Issue |
|---|---|---:|---|
| 1 | Visibility of System Status | 2/4 | The architecture diagram explains the system, but the homepage does not signal a clear starting path. |
| 2 | Match System / Real World | 3/4 | Product terminology is accurate but jargon-heavy for first-time visitors. |
| 3 | User Control and Freedom | 3/4 | Users can choose either path, but there is no guided "not sure" route. |
| 4 | Consistency and Standards | 3/4 | The visual system is consistent, but the CTA color inheritance undermines button convention and contrast. |
| 5 | Error Prevention | n/a | No risky form or destructive action on this surface. |
| 6 | Recognition Rather Than Recall | 2/4 | CLI and Server cards are textually differentiated but visually interchangeable. |
| 7 | Flexibility and Efficiency | 2/4 | Power users can navigate quickly, while new users must infer the correct product path. |
| 8 | Aesthetic and Minimalist Design | 1/4 | Minimalism has become emptiness: the home lacks a focal moment and authored product character. |
| 9 | Error Recovery | n/a | No error workflow is present on the homepage. |
| 10 | Help and Documentation | 3/4 | The portal has strong downstream documentation, but the home delays its useful quickstarts. |
| **Total** |  | **19/32** | **Needs a focused hierarchy and first-use pass** |

## Design Specificity Verdict

The page is coherent with ReplicaDB's Engineering Ledger system, but the homepage is only moderately specific to the product. The sidebar and documentation system feel authored; the landing composition could belong to almost any infrastructure documentation site. The detector found no automated pattern violations (`detect.mjs --json docs/src/content/docs/index.mdx` returned `[]`). That is a useful clean signal, but it does not measure narrative hierarchy, visual sameness, or actual contrast interactions.

The deployed URL returned HTTP 200 and contains the expected hero, product choice, diagram, support matrix, and quickstart links. Browser automation was attempted but could not launch because the local Playwright Chromium executable is not installed, so no reliable live overlay or viewport measurement is claimed.

## Overall Impression

The page is technically tidy but emotionally flat. It asks visitors to choose between two operating models before clearly explaining the outcome ReplicaDB provides. The single biggest opportunity is to turn the first viewport into a confident decision aid: explain the job ReplicaDB does, then make the CLI/server distinction visually and verbally obvious.

## What's Working

1. The product split is clear and honest: standalone CLI versus managed server, with no invented claims.
2. The underlying visual system is disciplined: serif orientation, sans-serif operational copy, restrained teal/terracotta palette, semantic focus states, and responsive one-column fallback.
3. The downstream information architecture is strong. The home links into focused quickstarts rather than reviving the old monolithic guide.

## Priority Issues

### [P0] The homepage asks for a decision before providing enough context

**Why it matters:** "Choose CLI or Server" makes a first-time visitor solve ReplicaDB's information architecture before understanding which workflow fits. "High-performance, non-intrusive bulk replication" is accurate but opaque.

**Fix:** Reframe the hero around the outcome: move data between supported systems with either a direct CLI workflow or a durable control plane. Add a short "Choose based on this" row inside or immediately above the cards, including state/ownership and intended operating context. Keep the comparison link as the uncertainty escape hatch.

**Suggested command:** `/impeccable clarify` then `/impeccable layout`

### [P0] The CTA contrast is broken in the shipped composition

**Why it matters:** The screenshot shows teal CTA text on the terracotta button, which is difficult to read and contradicts the button token that calls for paper text on a filled action. The global `.sl-markdown-content a` selector can override `.product-choice__link` because the latter does not match the same contextual specificity.

**Fix:** Scope the link color override explicitly to `.sl-markdown-content .product-choice__link` or use `!important` only on the component token; set default text to paper and hover text to paper as well. Add a focused contrast assertion for the rendered CTA in light and dark themes.

**Suggested command:** `/impeccable audit` then `/impeccable colorize`

### [P1] The two product cards are structurally identical and visually under-differentiated

**Why it matters:** Equal white rectangles with subtle borders force line-by-line reading. Visitors should recognize direct execution versus durable shared operations at a glance.

**Fix:** Keep equal height, but give each card a distinct visual signal: a small CLI terminal motif/icon and a server/control-plane motif/icon, a short metadata line such as `Direct execution` versus `Durable state`, and one concrete fit cue. Do not make one card arbitrarily larger; use controlled variation in accent and information hierarchy instead.

**Suggested command:** `/impeccable bolder`

### [P1] The page hierarchy gives the diagram and matrix too much equal weight

**Why it matters:** The architecture diagram, support matrix, screenshot, and quickstart list arrive as a stack of similarly framed blocks. The most actionable content, the two quickstarts, is last.

**Fix:** Promote the two quickstarts into a clear primary action band directly after the hero. Move the architecture diagram and support matrix into a quieter "How it fits" / "Supported paths" section below, with tighter spacing and a less card-like treatment. Surface the operator path for users who already know they need the managed server.

**Suggested command:** `/impeccable layout`

### [P2] The value proposition is too implementation-led

**Why it matters:** "Bulk replication" and "durable state" describe mechanics, not the user's job: migration, synchronization, or repeatable managed runs. The page feels like an index rather than a confident product entry point.

**Fix:** Replace or supplement the tagline with a plain-language outcome, then retain the technical qualifier as supporting copy. Add two or three factual use cases grounded in the existing docs, without inventing benchmarks or customer claims.

**Suggested command:** `/impeccable clarify`

### [P2] The logo and visual assets arrive too late to establish product character

**Why it matters:** The logo is rendered below the main decision and reads as a closing stamp, while the first viewport is almost entirely text and white panels. This contributes directly to the austere feeling.

**Fix:** Use the existing logo or a restrained product mark near the hero/choice section, and let the current architecture diagram become the visual anchor after the decision. Avoid decorative gradients or generic illustration; the visual should explain ReplicaDB's data movement.

**Suggested command:** `/impeccable delight`

## Persona Red Flags

**First-timer:** Encounters jargon, then a binary product choice, before seeing a concrete workflow. The comparison page is a necessary detour rather than an obvious safety net.

**Power user / platform engineer:** Can reach the right guide, but the home does not offer a fast operator route to deployment, monitoring, troubleshooting, or API reference. The useful entry points are buried below the introductory stack.

**Data engineer / DBA:** The connector matrix signals breadth but only two rows are featured on the home, so the page does not immediately communicate whether their source and sink pair is supported.

## Minor Observations

- Keep the two cards equal height; the fix is stronger content and accent differentiation, not arbitrary unequal sizing.
- The terracotta hover state is a useful secondary signal, but should not change the CTA foreground to teal.
- The support matrix caveat is small and visually subordinate; keep it readable if it remains on the home.
- The architecture diagram should be color-coded or labeled enough to distinguish direct execution from durable state without requiring paragraph reading.
- The detector was clean, so avoid chasing generic pattern rules as a substitute for solving the actual hierarchy and contrast issues.

## Questions to Consider

1. Which direction should come first: fix the CTA contrast, rebuild the hero/decision hierarchy, or add stronger CLI/server visual differentiation?
2. Should the home feel **warmer and more product-led**, **more technical and operator-focused**, or remain **quiet and editorial** with stronger hierarchy?
3. Do you want the next pass to address the **top 3 issues only** or implement the full five-issue homepage pass?
