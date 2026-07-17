# Architecture decisions

This file records decisions that materially constrain implementation. Revisit them through a
focused pull request with matching tests and documentation updates.

## Broker location

The broker runs beside the Paseo CLI and exposes a browser push-to-talk client. It shells out to
the supported CLI surface instead of relying on an undocumented daemon API.

## Secret handling

Normal mode resolves secrets from Bitwarden Secrets Manager once at startup. Development mode can
read `OPENAI_API_KEY` and `PASEO_PASSWORD` from the process environment. Secret values stay in
memory, never enter command arguments, and must not be logged.

## Audio interaction

The client uses manual push-to-talk with PCM16 audio at 24 kHz. Starting a new push-to-talk turn
cancels active playback so the user can interrupt a response.

## Write confirmation

`send_message` and `start_run` only create proposals. A distinct `confirm_action` call may execute
the current proposal after the user hears its readback and explicitly confirms it. Proposals are
single-use, expire after 120 seconds by default, and replace any earlier proposal.

Paseo permission approvals remain outside the voice tool surface.

## Summarisation

Long replies may be summarised through a configurable OpenAI-compatible endpoint. The default is
`http://127.0.0.1:1234/v1`, which suits a locally running LM Studio instance. If summarisation is
unavailable, the broker reads a cleaned tail of the original reply.

## Mock mode

When no OpenAI key resolves, the broker runs a text-only mock realtime loop. Mock mode exercises
the same dispatcher and confirmation gate as realtime mode.

## Package publishing

The repository is open source under MIT, but the package remains marked `private` to prevent
accidental npm publication while the project is early alpha.

## Deferred work

- Selecting among multiple remote Paseo daemons
- Packaging the tool surface as an MCP server
- Phone and PWA polish
- Voice activity detection and wake-word modes
