# Implementation guide

## Architecture

```text
Browser PTT client
  | PCM16 audio and JSON control frames over WebSocket
  v
Local Paseo Voice broker
  | OpenAI Realtime WebSocket
  | paseo CLI child processes
  | OpenAI-compatible summariser endpoint
  v
Coding-agent sessions
```

The Rust workspace now contains the pure safety state machine and a strict version 1 local
protocol. `paseo-control-plane --serve-stdio` serves bounded length-delimited JSON on inherited
stdin and stdout until clean EOF. It opens no socket, owns no credential, performs no Paseo I/O,
and is not started by the production Node.js broker. The current runtime remains Node.js until the
later cutover gates pass.

Every request has a validated request ID. Identical request bytes replay the exact original
response. Reusing an ID for different bytes returns `request_id_conflict`. Unknown versions,
operations, fields, enum variants, duplicate fields, truncated frames, oversized frames, and
trailing bytes fail closed. The shared fixtures are in `docs/RUST_PROTOCOL_FIXTURES.json`.

Realtime function-call IDs are also single-use. A repeated
`response.function_call_arguments.done` event is ignored before tool dispatch, so reconnect or
provider replay cannot duplicate a proposal or write transition.

The browser is a secret-free audio terminal. The broker owns configuration, secret resolution,
session selection, tool dispatch, and confirmation state. Each browser connection gets its own
realtime session, dispatcher state, and proposal store.

## Configuration and secrets

`src/config.ts` validates configuration with Zod. Precedence is:

1. `PASEO_VOICE_*` environment variables
2. `$PASEO_VOICE_CONFIG` or `~/.config/paseo-voice/config.json`
3. portable defaults

`src/secrets.ts` supports three providers:

- The `bitwarden` provider parses `BWS_ACCESS_TOKEN` from the configured bws environment file and
  invokes `bws secret get` for configured secret IDs.
- The `onepassword` provider invokes `op read --format json` sequentially for configured `op://`
  references. The child inherits the process environment so the CLI can use desktop-app
  integration or `OP_SERVICE_ACCOUNT_TOKEN`. Each read has a 20-second timeout.
- The `environment` provider reads `OPENAI_API_KEY` and `PASEO_PASSWORD` from the process
  environment. Empty values count as missing.

The provider is selected once per process by `secretProvider` or
`PASEO_VOICE_SECRET_PROVIDER`. Bitwarden is the default. `devMode` and `PASEO_VOICE_DEV` were
removed and produce migration errors directing users to the environment provider.

Missing OpenAI credentials select mock mode. Missing Paseo credentials replace the live client
with an unavailable stub that returns a clear tool error. Resolution is independent and best
effort for each secret. 1Password failures log only the provider, secret role, sanitized error
category, and numeric exit code when available.

## Paseo adapter

`src/paseo.ts` invokes the Paseo CLI with `execFile`, never a shell. `PASEO_PASSWORD` and an
optional `PASEO_HOST` are passed through the child environment. Output parsing is intentionally
tolerant because CLI response shapes may evolve.

Read operations include listing sessions, reading logs, inspection, and listing pending
permissions. Write operations include sending messages and starting detached runs.

## Confirmation gate

`src/gate.ts` is the hard safety boundary for writes. Tool calls first create a proposal containing
an opaque random token, payload, spoken echo, and expiration time. Confirmation consumes the exact
stored payload. Unknown, expired, replaced, cancelled, and reused tokens cannot execute writes.

Model instructions reinforce this policy but are not trusted to enforce it.

## Realtime and mock modes

`src/realtime.ts` connects one OpenAI Realtime session per browser connection. Push-to-talk release
commits the input buffer and requests a response. Starting another turn cancels the active response
and flushes client playback.

`src/mock-realtime.ts` provides a text command loop over the same dispatcher for development
without OpenAI credentials or audio hardware.

## Browser client

The static client under `public/` captures mono microphone audio, downsamples to PCM16 at 24 kHz,
and sends binary WebSocket frames while push-to-talk is held. Separate AudioWorklets handle capture
and playback. JSON frames carry status, transcript, tool, proposal, and playback-control events.

## Verification

The required local and CI check is:

```bash
pnpm check
```

This verifies Prettier, rustfmt, agent-document lint, Oxlint, Clippy, strict TypeScript compilation,
Vitest, Cargo tests, and TypeScript and Rust production builds. Tests use injected process, network,
time, and socket dependencies so they do not require live services.
