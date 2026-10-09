# Tier 1 mini-plan: postcss-nested override for Dependabot alert 107

Source: explicit Tier 1 request, isolated operation (no implementation_plan.md present).

## Tasks

1. Override postcss-nested to ^8.0.1 in docs/package.json and resolve docs/package-lock.json strictly.
   Tests: npm ci passes; postcss-nested resolves to 8.0.1 and postcss-selector-parser to 7.1.6.
2. Verify docs build and tests after the override.
   Tests: npm test (68 passing) and npm run build; expressive-code markup compared to the pre-change baseline.
3. Verify mermaid rendering with katex 0.18.9.
   Tests: npm run test:e2e (6 passing) and a headless browser check of the diagram pages.
4. Dismiss Dependabot alert 106 (http-cache-semantics, no upstream fix) as tolerable_risk.

## Execution Notes

- Baseline before change: 25 generated pages with expressive-code markup, 258 ec-line occurrences.
- After change: same counts; npm test 68/68; npm run build passes; test:e2e 6/6.
- Headless check on /, architecture/overview, architecture/run-lifecycle, and architecture/distributed-topology: each page rendered one mermaid SVG, with no page errors.
- Alert 107 (postcss-selector-parser) is resolved by the override.
- Alert 106 dismissed on GitHub after explicit confirmation.
- The docs base path is /ReplicaDB; the mermaid check used it.

## Retrospective

- Intent-to-Plan gap: the e2e suite does not assert mermaid SVG output, so a targeted browser check was needed.
- Plan-to-Implementation gap: npm 10 crashes with an arborist error on vitest peer resolution under --legacy-peer-deps; strict resolution works when run in a separate step. This applied to the earlier vitest work.
- Pattern: compare generated markup counts against a pre-change baseline when changing a transitive markdown or CSS pipeline dependency.
