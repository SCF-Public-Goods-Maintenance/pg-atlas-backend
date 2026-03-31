# CLAUDE.md

Read `.github/copilot-instructions.md` first — it is the single source of truth for
project conventions, tooling, code style, and architecture. This file contains only
Claude-specific extensions.

## Agentic Workflow

Specialist agents are defined in `.claude/agents/`. Verify that this directory exists
on the local file system. If it does not: ignore all instructions in the Agentic
Workflow section.

Invoke them via the `/agents` command or the Agent tool. Each agent has a clear lane;
hand off between agents in the sequence below for non-trivial work.

### Agent Registry

| Agent | Model | When to Invoke |
| ----- | ----- | -------------- |
| `systems-architect` | Opus 4.6 | **First, always.** Repo recon, architecture mapping, call-graph tracing, scoping PRs, identifying risks and invariants before any code is written. |
| `metrics-ecologist` | Opus 4.6 | Any task touching metric definition, graph analytics, ecological framing, or manuscript alignment (CONTRIBUTION_COMPASS, CELL_PATTERNS_MANUSCRIPT). Invoke before `backend-implementer` for metric work. |
| `backend-implementer` | Sonnet 4.6 | Translating a scoped plan into code: FastAPI endpoints, SQLAlchemy models, Procrastinate tasks, ingestion logic, crawlers, migrations. |
| `verification-engineer` | Sonnet 4.6 | Proving a change works: test design, invariant validation, failure debugging, migration safety, PR readiness. |
| `governance-writer` | Sonnet 4.6 | Packaging work for others: PR descriptions, ADRs, manuscript Methods text, funder-facing summaries. |

### Standard Delegation Sequence

```txt
systems-architect → [metrics-ecologist] → backend-implementer → verification-engineer → governance-writer
```

The `metrics-ecologist` step is required when the task involves any metric, graph
algorithm, or manuscript-method alignment. Skip it only for pure infrastructure work
(endpoint plumbing, crawler fixes, migration, config).

### Model Rationale

- **Opus 4.6** for `systems-architect` and `metrics-ecologist`: these roles require
multi-hop reasoning across complex architectures and scientific rigor for
governance-grade claims. The quality ceiling here directly affects what gets built and
what gets published.
- **Sonnet 4.6** for `backend-implementer`, `verification-engineer`, and
`governance-writer`: these roles execute against a well-defined plan. Reliable,
efficient output matters more than exploratory depth.
