# paseo-voice

Paseo Voice is a push-to-talk voice interface for Paseo coding-agent sessions. A local broker
reads agent replies aloud, accepts spoken steering, and protects writes with an explicit
two-phase confirmation gate.

The project is early alpha. Interfaces and configuration may change between commits.

## Requirements

- Node.js 26 or newer
- pnpm 11.13.1
- Rust 1.97.0 through `rustup` for control-plane development and repository checks
- A working `paseo` CLI for live session operations
- An OpenAI API key for realtime voice mode
- An OpenAI-compatible chat-completions endpoint for optional reply summaries
- One supported secret source:
  - Bitwarden Secrets Manager CLI (`bws`), which remains the default
  - 1Password CLI (`op`)
  - Process environment variables

The broker starts in text-only mock mode when no OpenAI key is available. This is enough to
develop and test the tool loop.

## Quick start

```bash
pnpm install --frozen-lockfile
pnpm check
pnpm build
pnpm console
```

To start the browser client:

```bash
pnpm start
```

Open `http://localhost:8790`. Use `http://localhost` or HTTPS when testing microphone access,
because browsers require a secure context.

## Configuration

Configuration precedence is environment variables, then the JSON configuration file, then
built-in defaults. The default file is `~/.config/paseo-voice/config.json`; override it with
`PASEO_VOICE_CONFIG`.

Start from [config.example.json](config.example.json). Select one secret provider for the whole
process with `secretProvider` or `PASEO_VOICE_SECRET_PROVIDER`. Accepted values are `bitwarden`,
`onepassword`, and `environment`. Bitwarden is the default when the setting is omitted.

The `environment` provider reads `OPENAI_API_KEY` and `PASEO_PASSWORD`. Start from
[.env.example](.env.example) and load those values through your shell or preferred secret manager.
The application does not automatically load `.env` files. Empty values count as missing.

The `bitwarden` provider reads a Bitwarden Secrets Manager access token from
`~/.config/bws.env` and fetches `bwsSecretIdOpenai` and `bwsSecretIdPaseo` at startup. Existing
`PASEO_VOICE_BWS_*` environment overrides remain supported.

```json
{
  "secretProvider": "bitwarden",
  "bwsSecretIdOpenai": "<openai-secret-id>",
  "bwsSecretIdPaseo": "<paseo-secret-id>"
}
```

Equivalent environment overrides are:

```bash
export PASEO_VOICE_SECRET_PROVIDER=bitwarden
export PASEO_VOICE_BWS_SECRET_ID_OPENAI='<openai-secret-id>'
export PASEO_VOICE_BWS_SECRET_ID_PASEO='<paseo-secret-id>'
```

The `onepassword` provider calls the 1Password CLI directly. Configure secret references in the
JSON file:

```json
{
  "secretProvider": "onepassword",
  "onePasswordSecretRefOpenai": "op://example-vault/openai/api-key",
  "onePasswordSecretRefPaseo": "op://example-vault/paseo/password"
}
```

Equivalent environment overrides are:

```bash
export PASEO_VOICE_SECRET_PROVIDER=onepassword
export PASEO_VOICE_ONEPASSWORD_SECRET_REF_OPENAI='op://example-vault/openai/api-key'
export PASEO_VOICE_ONEPASSWORD_SECRET_REF_PASEO='op://example-vault/paseo/password'
```

Authenticate `op` before starting Paseo Voice. Interactive use can rely on 1Password desktop-app
integration. Unattended use can provide `OP_SERVICE_ACCOUNT_TOKEN` to the Paseo Voice process.
`OP_ACCOUNT` selects an account when desktop integration has multiple accounts. The CLI child
inherits the full process environment and resolves each configured reference sequentially with a
20-second timeout. Override the executable with `onePasswordBin` or
`PASEO_VOICE_ONEPASSWORD_BIN` when `op` is not on `PATH`.

All providers resolve secrets once at startup. Failures remain independent and best effort: a
missing OpenAI key selects mock mode, while a missing Paseo password disables Paseo tools. Restart
the process after rotating a secret. Secret values remain in process memory, never enter command
arguments, and are never logged. 1Password references enter the short-lived `op` process argument
list but are redacted from application logs.

`devMode` and `PASEO_VOICE_DEV` have been removed. Use the `environment` provider instead.

See the current official [1Password CLI secret-reference documentation](https://www.1password.dev/cli/secret-references)
and [1Password CLI authentication documentation](https://www.1password.dev/cli/get-started) for
CLI setup and authentication details.

### Manual 1Password smoke test

Use a local configuration containing valid test references, authenticate the CLI through desktop
integration or a service account, then build and start the broker with external OpenAI calls
disabled:

```bash
pnpm build
PASEO_VOICE_MOCK=1 pnpm start
```

Confirm that startup reports `"secretProvider":"onepassword"`, does not report a
`onepassword secret fetch failed` warning, and does not report that the Paseo password is
unresolved. Stop the broker with Ctrl+C.

To verify best-effort degradation, temporarily replace one local test reference with an unused
`op://` reference and start the broker again. It should still start, report only the affected
secret role and a sanitized error category, and never print the reference or secret output. Do
not commit the local references or any secret values.

See [DECISIONS.md](DECISIONS.md) for current architectural decisions and
[docs/IMPLEMENTATION.md](docs/IMPLEMENTATION.md) for implementation details. Current hosting and
deployment facts are maintained in [docs/agents/state.md](docs/agents/state.md).

## Commands

| Command           | Purpose                                           |
| ----------------- | ------------------------------------------------- |
| `pnpm build`      | Compile TypeScript into `dist/`                   |
| `pnpm check`      | Run formatting, lint, typecheck, tests, and build |
| `pnpm console`    | Open the text console after building              |
| `pnpm format`     | Format tracked source and documentation           |
| `pnpm lint`       | Run Oxlint                                        |
| `pnpm rust:build` | Build the Rust workspace in release mode          |
| `pnpm rust:lint`  | Run Clippy across the Rust workspace              |
| `pnpm rust:test`  | Run all Rust tests                                |
| `pnpm start`      | Start the compiled broker                         |
| `pnpm test`       | Run the Vitest suite once                         |
| `pnpm test:watch` | Run Vitest in watch mode                          |
| `pnpm typecheck`  | Typecheck without emitting files                  |

## Project layout

- `src/`: broker, Paseo adapter, confirmation gate, summariser, and realtime bridge
- `public/`: browser push-to-talk client with no build step or secrets
- `test/`: Vitest unit tests and sanitised CLI fixtures
- `docs/`: architecture and implementation documentation
- `docs/agents/`: task-specific agent rules, operational state, and deployment playbooks

## Roadmap

The goal is a real-time voice assistant with a visual GUI and talking avatar for monitoring and
steering multiple coding-agent threads. It should announce short summaries as agents finish,
accept a spoken or typed response, and guarantee that the response can only be submitted to the
thread that produced the summary.

The approved backend direction is a privileged Rust control-plane process. It will own reply
provenance, proposal and confirmation state, delivery identifiers, the Paseo credential, and the
only Paseo write path. The TypeScript broker remains the browser, audio, and OpenAI Realtime
adapter during the phased migration. See
[docs/RUST_CONTROL_PLANE_PLAN.md](docs/RUST_CONTROL_PLANE_PLAN.md) for the implementation plan.

### 1. Rust control-plane foundation

- Add a Cargo workspace with a pure safety-core crate and a separate control-plane executable.
- Keep the Rust process out of the Node.js process so credentials, write authority, crashes, and
  recovery state are isolated from the voice adapter.
- Define a narrow local interface where response proposals identify an immutable summary context,
  never a caller-selected destination thread.
- Model summary, proposal, confirmation, dispatch, and delivery states explicitly and reject
  invalid transitions.
- Run the Rust decision engine in shadow mode before moving credentials or write authority.
- Cut over only after property, concurrency, crash-recovery, and cross-thread routing tests pass.
- Keep the existing TypeScript write path available only as a rollback during migration, never as
  an independently selectable production path after cutover.

### 2. Reliable real-time foundation

- Harden the existing browser, broker, Paseo CLI, realtime voice, and local summariser loop.
- Detect newly completed agent replies without rereading or announcing the same reply twice.
- Keep summaries short and outcome-first, with concrete results and any question or blocker.
- Preserve the existing two-phase proposal and confirmation gate for every write.
- Add integration coverage for disconnects, reconnects, retries, duplicate events, and degraded
  summarisation.

### 3. Provenance-bound summaries and responses

- Give every summary an immutable context containing its summary ID, source thread ID, source
  reply ID, and creation time.
- Bind the visible response field and active voice turn to that context. Changing the displayed
  thread invalidates drafts and pending confirmations rather than silently retargeting them.
- Build send proposals from the stored summary context, never from a thread name or destination
  supplied by the voice model, browser, or response text.
- Reject confirmation if the summary context is stale, missing, already consumed, or does not
  match the proposal target.
- Show the bound destination beside the response field and repeat it during confirmation.
- Test the invariant end to end: a response originating from summary A can never be pasted,
  proposed, confirmed, or delivered to the thread for summary B.

### 4. Concurrent summary queue

- Funnel agent completion events through one ordered summary queue so simultaneous replies do not
  overlap, overwrite context, or race for the response field.
- Track explicit queued, summarising, ready, speaking, awaiting-response, completed, and failed
  states.
- Allow only one active spoken summary and one provenance-bound response context at a time.
- Display the remaining queue count while keeping spoken summaries as concise as possible.
- Define deterministic ordering and deduplication, with safe retry and recovery after a broker or
  browser restart.
- Defer voice commands for skipping, replaying, deferring, or reprioritising summaries unless
  real-world use shows that short summaries alone are insufficient.

### 5. Agent dashboard and talking avatar

- Replace the audio-terminal layout with agent cards showing thread name, provider, live state,
  latest short summary, and queued-response count.
- Add a talking avatar with lip sync and clear listening, thinking, speaking, awaiting-approval,
  and error states.
- Keep the active thread name visible beside the avatar and response field as a continuous
  provenance cue.
- Make status and routing understandable without relying on colour, animation, or audio alone.
- Preserve a lightweight, secret-free browser client and responsive keyboard, pointer, and touch
  controls.

### 6. Natural interruption and recovery

- Support barge-in by stopping speech and playback as soon as the user starts talking.
- Preserve the interrupted summary and its source context so it can be resumed or replayed without
  changing the response target.
- Separate conversational interruptions from confirmations so incidental speech can never approve
  a pending write.
- Recover cleanly from microphone, audio, network, realtime API, summariser, and Paseo failures.

### 7. Audited response timeline

- Maintain a searchable timeline of summaries and responses with source thread, destination
  thread, confirmation state, timestamps, and delivery result.
- Record durable identifiers and routing metadata without storing credentials or unnecessary
  sensitive content.
- Make failed and cancelled responses clearly distinguishable from delivered responses.
- Provide retention and deletion controls before enabling durable history by default.

### Release criteria

The first complete release should demonstrate two or more agents replying concurrently, ordered
and non-overlapping spoken summaries, avatar and dashboard state updates, barge-in recovery, and a
response sent only to the immutable source thread after explicit confirmation. Automated tests
must attempt cross-thread retargeting and prove that the broker rejects it.

## Contributing

Read [CONTRIBUTING.md](CONTRIBUTING.md) before opening a pull request. AI coding agents should also
follow [AGENTS.md](AGENTS.md).

## License

[MIT](LICENSE)
