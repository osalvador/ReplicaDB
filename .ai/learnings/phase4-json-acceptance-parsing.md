---
type: Learning
description: Parse structured API responses with JSON tools in shell acceptance gates.
sources:
  - id: plan
    resource: .ai/archive/phase-4-reusable-managed-datasources-with-encrypted-credentials.plan.md
generated: { by: itx-code, at: "2026-09-01T09:49:02Z" }
status: stable
---

A datasource-only Compose smoke used greedy text extraction for a job response and selected a nested datasource ID. The subsequent schedule request then reported that the job was missing.

Use `jq` to select the top-level response field instead of regular expressions when shell gates consume JSON APIs.
