# AGENTS.md

Repository guidance for agents working in SmallServer.

## Required context workflow

Use [skills/smallserver-docs-context/SKILL.md](./skills/smallserver-docs-context/SKILL.md) before non-trivial planning, implementation, review, or handoff work.

## Runtime boundary

SmallServer is built on SmallOS's cooperative runtime. Preserve SmallOS as the owner of task scheduling and readiness polling. Do not import or depend on `asyncio` in the framework core unless the active framework plan explicitly changes that boundary.

## Durable context

- Read [docs/INDEX.md](./docs/INDEX.md) first.
- Treat [docs/conversation-context.md](./docs/conversation-context.md) as the current work state and [docs/smallserver-project-context.md](./docs/smallserver-project-context.md) as stable architecture context.
- Update the narrowest relevant document when decisions, constraints, status, blockers, validation, or architecture materially change.
- Keep frontmatter and `_Last updated` dates accurate. Regenerate instead of editing `docs/INDEX.md` directly.
