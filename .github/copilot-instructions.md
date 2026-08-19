---
applyTo: '**'
---

# ReplicaDB AI Knowledge Map

## Rules
Load applicable rules from `.github/instructions/*.instructions.md`. These files contain project rules, not an inventory of implementation details.

## Project Context
`.ai/index.md` is the OKF 0.2 bundle index. `.ai/{architecture,interfaces,patterns,decisions,learnings}/*.md` contains focused concepts and evidence pointers. `.ai/archive/` contains completed plans and retrospectives used during context work.

## Workflow
Always keep the `/itx-` prefix on workflow commands:
- `/itx-init` - bootstrap or update `.ai/` and instruction context
- `/itx-explore` - investigate ideas, tickets, or architecture read-only
- `/itx-plan` - generate `implementation_plan.md` in the repository root
- `/itx-code` - execute a plan task by task and archive it in `.ai/archive/`

## History
Read `.ai/archive/` only during `/itx-init`, `/itx-explore`, `/itx-plan`, or `/itx-code` work.

## Loading Rules
Do NOT read `.ai/` during normal coding. Load only the relevant concepts when running `/itx-explore`, `/itx-plan`, or `/itx-init`.

## Security
NEVER include credentials, tokens, API keys, DSNs, internal URLs, or PII in generated context, plans, instructions, logs, or examples. Refer to environment-managed configuration without copying values.
