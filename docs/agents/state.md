# Agent state

This file holds current operational facts. Architecture belongs in `DECISIONS.md` and
`docs/IMPLEMENTATION.md`; product limits are summarised in the README.

## Runtime

| Item          | Current                                                                                |
| ------------- | -------------------------------------------------------------------------------------- |
| Phase         | Early alpha                                                                            |
| Backend       | One privileged Rust service beside the Paseo CLI                                       |
| Browser       | Secret-free protocol-v2 dashboard with typed, live voice, and English dictation modes  |
| Voice         | OpenAI Realtime or a configured custom endpoint; text-only mock mode remains available |
| Summaries     | Manual reply reads with optional model summary and bounded local fallback              |
| Writes        | Provenance-bound proposal plus browser or deterministic console confirmation           |
| Hosts         | Connection-scoped selector over trusted broker profiles                                |
| Persistence   | Runtime output is limited to content-free SQLite metadata and browser preferences      |
| Summary queue | Per connection, with conditional sequential reconnect deduplication in broker memory   |
| Main gap      | Automatic completion detection awaits a stable supported Paseo reply marker            |

The default listener is loopback. Same-origin browser checks exist, but application-level remote
authentication and TLS termination do not. The broker is not approved for direct public or shared
network exposure.

## Hosting

| Item               | Current                                                       |
| ------------------ | ------------------------------------------------------------- |
| Broker deployment  | Local or private-host only                                    |
| Web deployment     | Not configured                                                |
| Future target      | Cloudflare Workers Static Assets                              |
| Deployment trigger | Planned Git-driven Workers Builds from `main`                 |
| Manual deployment  | Disabled by policy; do not run `wrangler deploy`              |
| Web workspace      | No standalone public web workspace or output directory exists |
| Tooling Node       | 26, pinned by the repository                                  |
| Production DNS     | Not configured; changes require an explicit request           |

The browser and broker are separate deployment concerns. Publishing static assets would not deploy
or authorise exposure of the broker.

## Future deployment contract

Before a public static GUI can be connected:

- Add a standalone web workspace, build command, output directory, and `wrangler.jsonc`.
- Configure Git-driven Workers Builds from `main`; do not use manual deployment as a workaround.
- Approve broker authentication, encrypted transport, and allowed origins.
- Verify that the static output contains no secrets, internal agent documents, local endpoints, or
  runtime content.
- Record the final workspace, build command, assets directory, Worker name, and review URL here.

## Validation

| Check                 | Command            | When                                    |
| --------------------- | ------------------ | --------------------------------------- |
| Full repository check | `pnpm check`       | Before every task push                  |
| Agent document lint   | `pnpm lint:agents` | Agent rules, state, or playbooks change |
| Focused tests         | `pnpm test`        | Runtime behavior or tests change        |
| Production build      | `pnpm build`       | Rust runtime changes                    |
| Rust workspace        | `pnpm rust:test`   | Rust backend changes                    |

No remote deployment verification is available because Cloudflare is not connected for Paseo Voice.
