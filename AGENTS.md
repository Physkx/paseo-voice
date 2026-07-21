# AGENTS.md - paseo-voice router

Slim entry point for coding agents. Read only the documents the task router points to.

## STOP - do not unless explicitly asked

- Run `wrangler deploy`, change Cloudflare settings, or trigger a manual deployment
- Change DNS, custom domains, Worker bindings, environment variables, or secrets
- Expose the broker, Paseo daemon, realtime API credentials, or summariser to the public internet
- Weaken the proposal and confirmation gate or add voice approval for Paseo permissions
- Persist transcripts, summaries, or agent output without an approved retention and deletion policy
- Publish the npm package or remove its `private` setting
- Create branches or pull requests, rebase shared history, merge, or force push

## Git posture

- Default branch is `main`. Agents have standing approval to push task commits directly to
  `origin/main` after applicable validation passes.
- Stage intentionally. Never use `git add -A`; add only the files changed for the task.
- Before commit, confirm `main`, review `git status --short`, stage explicit paths, and inspect
  `git diff --cached --stat`.
- Never commit secrets, `.env` values, local `paseo.json`, logs, transcripts, generated `dist/`, or
  generated `target/`.
- If task changes cannot be isolated from unrelated worktree changes, stop and explain.

## Task router

| If you are...                                        | Read                                                         |
| ---------------------------------------------------- | ------------------------------------------------------------ |
| Changing broker, realtime, tools, security, or tests | `docs/IMPLEMENTATION.md` and `DECISIONS.md`                  |
| Changing the browser or future web GUI               | `docs/agents/publish-boundary.md` and `docs/agents/state.md` |
| Planning or verifying a web deployment               | `docs/agents/playbooks/verify-review-deploy.md`              |
| Changing agent rules or playbooks                    | `docs/agents/playbooks/update-agent-docs.md`                 |
| Unsure what is current or deployed                   | `docs/agents/state.md`                                       |

## Stack and code rules

- Use Rust 1.97 for the backend, safe Rust only, repository commands through pnpm, and plain
  secret-free JavaScript for browser assets.
- Preserve dependency injection for process, network, clock, and socket boundaries.
- Never use a shell to invoke Paseo or Bitwarden commands.
- Never log, serialise, or place secrets in command arguments.
- Keep all write operations behind the proposal and confirmation gate.
- Use `~` for example home paths and reserved example domains for hosts.
- Do not introduce personal names, email addresses, hostnames, IP addresses, or infrastructure
  details into source, fixtures, documentation, or commits.
- Do not use em dash or en dash characters in user-facing copy, documentation, comments, or
  commit messages.
- Preserve unrelated worktree changes.

## Validation

Run focused tests while developing. Before commit and push, run:

```bash
pnpm check
```

When agent documents change, `pnpm check` includes `pnpm lint:agents`.

## Response format

Every task completion should include:

- **Changed** - files and one-line purpose
- **Validation** - commands run and pass or fail
- **Git** - commit and push status, or exact files left to stage
- **Not done** - deferred STOP items, deployment checks, follow-ups, or blockers

## Pointers

| Topic                           | Location                          |
| ------------------------------- | --------------------------------- |
| Current operational facts       | `docs/agents/state.md`            |
| Browser and deployment boundary | `docs/agents/publish-boundary.md` |
| Implementation architecture     | `docs/IMPLEMENTATION.md`          |
| Architecture decisions          | `DECISIONS.md`                    |
| Product roadmap                 | `README.md`                       |
| Contribution guide              | `CONTRIBUTING.md`                 |
