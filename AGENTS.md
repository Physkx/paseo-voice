# Coding agent instructions

## Scope

These instructions apply to the entire repository.

## Stack

- Node.js 26
- Strict TypeScript with ESM and NodeNext module resolution
- pnpm
- Vitest
- Oxlint and Prettier

## Working rules

- Read `README.md`, `DECISIONS.md`, and the relevant source and tests before changing behavior.
- Preserve dependency injection for process, network, clock, and socket boundaries.
- Never use a shell to invoke Paseo or Bitwarden commands.
- Never log, serialise, or place secrets in command arguments.
- Keep all write operations behind the proposal and confirmation gate.
- Use `~` for example home-directory paths and reserved example domains for example hosts.
- Do not introduce personal names, email addresses, hostnames, IP addresses, or infrastructure
  details into source, fixtures, documentation, or commits.
- Do not add em dash or en dash characters to user-facing copy, documentation, comments, or commit
  messages.
- Preserve unrelated worktree changes.

## Verification

Run focused tests while developing. Before completion, run:

```bash
pnpm check
```
