---
type: Decision
description: The server OpenAPI document and generated TypeScript schema are the contract boundary between the control plane and SPA.
sources:
  - id: openapi
    resource: replicadb-server/pom.xml
  - id: schema
    resource: replicadb-server/frontend/src/api/schema.ts
  - id: generator
    resource: replicadb-server/frontend/scripts/generate-api-types.mjs
  - id: tests
    resource: replicadb-server/src/test/java/org/replicadb/server/OpenApiSpecificationIT.java
generated: { by: itx-init/2.1, at: "2026-08-19T14:26:22Z" }
status: stable
---

Driving forces: avoid drift between immutable Java response records and frontend request/query code, while keeping framework implementation details out of the public contract.

Decision: generate `src/api/schema.ts` from Springdoc OpenAPI, alias its component schemas in endpoint modules, and test live or committed schema stability. DTOs explicitly represent lower-case modes, nullable wire values, CSRF responses, and deterministic property order where required.

Trade-offs: backend changes require schema generation and serialized-JSON validation. A generated type can still be structurally wrong for runtime nulls, so contract tests remain necessary.
