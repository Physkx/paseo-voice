# Implementation guide

## Architecture

```text
Secret-free browser
  | local HTTP and WebSocket
  v
Rust paseo-control-plane
  | OpenAI Realtime WebSocket
  | direct paseo, bws, or op child processes
  | OpenAI-compatible summariser HTTP
  | content-free SQLite metadata journal
  v
Coding-agent sessions
```

The production backend is one Rust process. Browser assets under `public/` remain plain
JavaScript and contain no credentials. Node.js and pnpm are repository tooling only. There is no
production TypeScript backend, credential owner, confirmation gate, or alternate Paseo write path.

## Runtime commands

`paseo-control-plane serve` serves the browser, health endpoint, and browser WebSocket.
`paseo-control-plane console` opens the deterministic text interface over the same Rust tool
engine. `--serve-stdio` remains as the strict migration-protocol harness and is not used by the
production runtime.

Production composition constructs and injects the listening socket, monotonic clock, HTTP client,
Realtime WebSocket connector, and process executor. Tests replace or isolate these boundaries with
fakes, loopback listeners, and temporary state.

Each browser connection owns its own selection, trusted interaction sequence, summary context, and
pending proposal state. OpenAI Realtime function-call IDs are single-use. Binary browser frames are
PCM16 audio at 24 kHz. Starting a turn cancels active playback, and releasing push-to-talk commits
the input buffer. Browser WebSocket upgrades with an `Origin` header must match the HTTP `Host`;
cross-origin browser connections fail with HTTP 403.

## Configuration and secrets

Rust validates the optional JSON configuration and then applies `PASEO_VOICE_*` environment
overrides. The default file is `~/.config/paseo-voice/config.json`.

The selected startup provider is Bitwarden, 1Password, or environment. Secret values are resolved
once, retained only in memory, omitted from logs and arguments, and supplied to Paseo only through
the child environment. Secret-manager programs and Paseo are invoked directly without a shell,
with bounded output and deadlines.

A missing OpenAI key selects mock mode. A missing Paseo password keeps tools unavailable without
preventing the browser server from starting.

## Provenance and confirmation

`paseo-safety-core` is pure and has no I/O dependencies. It owns validated identifiers, exact
response bytes, immutable source provenance, deterministic queue order, proposal expiry,
later-interaction confirmation, cancellation, dispatch, and delivery states.

Reading a reply creates the only actionable summary context. `send_message` accepts response text
only and cannot accept a session or destination. Confirmation accepts only the proposal handle.
The destination supplied to the Paseo adapter is derived from the source thread stored in the
summary context. Selecting or reading another context invalidates the previous draft and proposal.

`start_run` uses the same later-interaction, expiry, single-use, exact-argument, and journal-before-
dispatch rules. Paseo permission requests can be narrated but never approved by voice.

## Delivery and recovery

The Paseo adapter reports `delivered` only when successful JSON contains a validated receiver
message ID. A structured rejection before acceptance is `rejected`. Timeout, malformed output,
missing receipt, or uncertain completion is `outcome_unknown` and is never retried automatically.

Before a child write starts, Rust appends a `dispatching` metadata transition. The journal stores
only opaque operation, summary, source or destination identifiers, SHA-256 digests, timestamps,
states, and optional receiver IDs. It has no transcript, summary text, response body, prompt,
credential, or agent-output column. The journal file is mode 0600 and its directory mode 0700 on
Unix. Restart recovery maps `dispatching` to `outcome_unknown` and invalidates `pending`; it
never constructs a fresh send. The read-only `get_operation_status` tool queries the journal by
opaque operation ID. Retention is bounded to the latest 10,000 metadata transitions.

Paseo 0.1.107 does not expose caller-supplied write idempotency IDs, so the application does not
claim exactly-once delivery.

## Local protocol

The retained version 1 stdio protocol uses a four-byte big-endian length followed by at most
131,072 bytes of strict JSON. Unknown versions, fields, variants, duplicate fields, malformed,
truncated, oversized, or trailing input fail closed. Identical request bytes replay the exact
response; conflicting reuse of a request ID is rejected. Shared fixtures live in
`docs/RUST_PROTOCOL_FIXTURES.json`.

## Verification

```bash
pnpm check
```

This runs Prettier, rustfmt, agent-document lint, browser JavaScript lint, Clippy, all Cargo tests,
and the release build. Tests cover the safety state machine, property-generated confirmation
replay, concurrency, strict protocol framing, secret providers, process failure classification,
journal recovery, mock browser runtime, and an end-to-end fake Realtime WebSocket.
