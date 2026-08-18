---
applyTo: '**'
---

# ReplicaDB AI Knowledge Map

## Rules
Load applicable rules from `.github/instructions/*.instructions.md`. The project-specific Java, functional, and testing rules are intentionally slim; do not treat them as a source inventory.

## Project Context
`.ai/context.md` is the project index. `.ai/context/*.md` contains focused details for the core CLI/adapters, managed server domain/infrastructure/API, frontend, testing, operations, and recent changes. These files are prompt-directed references, not a normal coding dependency.

## Workflow
Always keep the `/itx-` prefix on workflow commands:
- `/itx-init` - bootstrap or update `.ai/` and instruction context
- `/itx-explore` - investigate ideas, tickets, or architecture read-only
- `/itx-plan` - generate `implementation_plan.md` in the repository root
- `/itx-code` - execute a plan task by task and archive it in `.ai/archive/`

## History
Completed plans and execution retrospectives live in `.ai/archive/`. Read the relevant archive only during `/itx-init`, `/itx-explore`, `/itx-plan`, or `/itx-code` work.

## Loading Rules
Do NOT read `.ai/` during normal coding. Load only the relevant context layers when running `/itx-explore`, `/itx-plan`, or `/itx-init`.

## Security
Never include credentials, tokens, API keys, DSNs, internal URLs, or PII in generated context, plans, instructions, logs, or examples. Refer to environment-managed configuration without copying values.
