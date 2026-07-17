# paseo-voice

Paseo Voice is a push-to-talk voice interface for Paseo coding-agent sessions. A local broker
reads agent replies aloud, accepts spoken steering, and protects writes with an explicit
two-phase confirmation gate.

The project is early alpha. Interfaces and configuration may change between commits.

## Requirements

- Node.js 26 or newer
- pnpm 11.13.1
- A working `paseo` CLI for live session operations
- An OpenAI API key for realtime voice mode
- An OpenAI-compatible chat-completions endpoint for optional reply summaries
- Bitwarden Secrets Manager CLI (`bws`) for the default secret-loading flow

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

Start from [config.example.json](config.example.json). For environment-based local development,
copy `.env.example` values into your shell or preferred secret manager. The application does not
automatically load `.env` files.

In normal mode, the broker reads a Bitwarden Secrets Manager access token from
`~/.config/bws.env` and fetches configured secret IDs at startup. Secret values remain in process
memory and are never placed in command arguments.

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
