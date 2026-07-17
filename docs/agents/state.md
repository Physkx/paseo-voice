# Agent state

This file holds current operational facts that agents need across tasks. Product intent and future
work remain in the README roadmap. Update this file when runtime, hosting, deployment, or
validation facts change.

## Current phase

| Item           | Current                                                                    |
| -------------- | -------------------------------------------------------------------------- |
| Phase          | Early alpha, reliable voice and agent-control foundation                   |
| Broker         | Node.js service running beside the Paseo CLI                               |
| Browser        | Secret-free static push-to-talk client served by the broker                |
| Voice          | OpenAI Realtime when configured, text-only mock mode otherwise             |
| Summaries      | Optional OpenAI-compatible local endpoint with cleaned-text fallback       |
| Writes         | Two-phase proposal and explicit confirmation gate                          |
| Rust           | Inert Phase 1 workspace; no runtime, secret, network, or Paseo capability  |
| Active roadmap | Rust control plane, provenance-bound replies, summary queue, and dashboard |

## Hosting

| Item               | Current                                                                        |
| ------------------ | ------------------------------------------------------------------------------ |
| Broker deployment  | Local or private-host only; no public deployment is configured                 |
| Web deployment     | Planned, not configured                                                        |
| Future target      | Cloudflare Workers Static Assets                                               |
| Deployment trigger | Git-driven Cloudflare Workers Builds from `main`                               |
| Manual deployment  | Disabled by default; do not run `wrangler deploy`                              |
| Web workspace      | Not selected yet; define its root and output directory when the GUI is created |
| Node version       | 26, pinned by the repository                                                   |
| Production DNS     | Not configured; changes require an explicit request                            |

The future static GUI and the broker are separate deployment units. Cloudflare may host the static
GUI, but the broker must remain near the Paseo CLI and expose only an authenticated, encrypted,
and explicitly approved connection. Do not infer that deploying the GUI also deploys or exposes
the broker.

## Planned Cloudflare contract

When a standalone web workspace is added:

- Store `wrangler.jsonc` in that workspace with static assets pointing to its build output.
- Configure Cloudflare Workers Builds with the workspace as root, its build script as the build
  command, `main` as the production branch, and Node.js 26.
- Keep deployment Git-driven. A push may trigger a build, but agents must not run manual deploy,
  secret, binding, custom-domain, or DNS commands unless explicitly asked.
- Record the final workspace, build command, assets directory, Worker name, and review URL here.
- Add web-specific `AGENTS.md` instructions and validation commands before enabling deployment.

## Environment names

Document names only, never values. Current secret names and configuration examples live in
`.env.example` and `config.example.json`. Any future browser variable must be safe for public
exposure and use an explicit `PUBLIC_` prefix.

## Validation matrix

| Check                 | Command            | When                                    |
| --------------------- | ------------------ | --------------------------------------- |
| Full repository check | `pnpm check`       | Before every task push                  |
| Agent document lint   | `pnpm lint:agents` | Agent rules, state, or playbooks change |
| Focused unit tests    | `pnpm test`        | Runtime behavior or tests change        |
| Production compile    | `pnpm build`       | TypeScript runtime changes              |
| Rust workspace        | `pnpm rust:test`   | Rust control-plane changes              |
| Future web build      | To be defined      | Every future web workspace change       |

No remote deployment verification is currently available because Cloudflare Workers Builds is not
connected for Paseo Voice.
