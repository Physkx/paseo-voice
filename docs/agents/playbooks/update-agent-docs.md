# Playbook: update agent documents

## Use when

Changing `AGENTS.md`, `CLAUDE.md`, `docs/agents/*`, playbooks, or agent-document linting.

## Preconditions

- The change is intentional rule, state, boundary, or playbook maintenance.
- Product architecture remains owned by `DECISIONS.md` and `docs/IMPLEMENTATION.md`.
- The root `AGENTS.md` remains a slim router rather than a duplicated manual.

## Files you will touch

| Change type                                     | File                              |
| ----------------------------------------------- | --------------------------------- |
| STOP list, Git posture, router, response format | `AGENTS.md`                       |
| Current hosting and validation facts            | `docs/agents/state.md`            |
| Browser and public deployment safety            | `docs/agents/publish-boundary.md` |
| Task procedures                                 | `docs/agents/playbooks/*.md`      |
| Claude entry point                              | `CLAUDE.md`                       |
| Consistency checks                              | `scripts/lint-agents.mjs`         |

## Steps

1. Put each fact in one owning document.
2. Add a router row only when a new task type needs different instructions.
3. Link to architecture and roadmap documents instead of copying them.
4. Update `state.md` whenever hosting, deployment, or validation facts change.
5. Keep internal agent documents out of any future public web build.

## Validate

- Referenced paths and package scripts exist.
- `CLAUDE.md` contains exactly `@AGENTS.md`.
- No secret values, private infrastructure details, or forbidden dash characters were added.
- Run `pnpm lint:agents` and `pnpm check`.

## Done means

- Agents can route the task from the root file and find each operational fact in one place.
- Automated agent-document checks pass.

## Do not

- Duplicate volatile deployment facts across documents.
- Add product-specific implementation detail to the root router.
- Run deployment, DNS, secret, or binding commands while maintaining documents.

## If stuck

Prefer a short pointer from the router to a focused document. Record uncertain operational facts
as unknown instead of guessing.
