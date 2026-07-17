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

The browser is a secret-free audio terminal. The broker owns configuration, secret resolution,
session selection, tool dispatch, and confirmation state. Each browser connection gets its own
realtime session, dispatcher state, and proposal store.

## Configuration and secrets

`src/config.ts` validates configuration with Zod. Precedence is:

1. `PASEO_VOICE_*` environment variables
2. `$PASEO_VOICE_CONFIG` or `~/.config/paseo-voice/config.json`
3. portable defaults

`src/secrets.ts` supports two flows:

- Normal mode parses `BWS_ACCESS_TOKEN` from the configured bws environment file and invokes
  `bws secret get` for configured secret IDs.
- Development mode reads `OPENAI_API_KEY` and `PASEO_PASSWORD` from the process environment.

Missing OpenAI credentials select mock mode. Missing Paseo credentials replace the live client
with an unavailable stub that returns a clear tool error.

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

This verifies formatting, lint, strict TypeScript compilation, unit tests, and the production
build. Tests use injected process, network, time, and socket dependencies so they do not require
live services.
