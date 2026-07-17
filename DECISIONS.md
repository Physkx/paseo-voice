# Architecture decisions

This file records decisions that materially constrain implementation. Revisit them through a
focused pull request with matching tests and documentation updates.

## Broker location

The broker runs beside the Paseo CLI and exposes a browser push-to-talk client. It shells out to
the supported CLI surface instead of relying on an undocumented daemon API.

## Secret handling

One explicit provider resolves secrets once at startup. Supported providers are Bitwarden Secrets
Manager, 1Password CLI, and the process environment. Bitwarden remains the default for backward
compatibility. The environment provider reads `OPENAI_API_KEY` and `PASEO_PASSWORD`. The
1Password provider invokes `op read` sequentially for configured `op://` references and delegates
authentication to the CLI's desktop integration or service-account environment. Secret values
stay in memory, never enter command arguments, and must not be logged. Configured references may
enter command arguments but must not be logged.

Resolution remains best effort per secret. A missing or failed OpenAI secret selects mock mode,
while a missing or failed Paseo password disables Paseo tools. Rotation takes effect after a
process restart.

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

## Host profiles and session creation

This section records the planned host-profile design. It is not implemented by the current alpha.

Paseo daemon targets are explicit broker-side host profiles, not free-form browser or voice input.
Each profile has a stable ID, display label, daemon target, default working directory, and default
provider/model. Exactly one profile may be the default. The browser receives only safe display
metadata and availability, while daemon targets and the shared alpha-stage Paseo credential remain
in the broker. Per-profile credentials are deferred.

Host selection belongs to one browser connection and resets to the configured default on
reconnect. The selection applies to every Paseo operation. Changing it invalidates the current
session, drafts, pending proposals, and confirmation tokens. An unavailable host or provider/model
fails explicitly without automatic fallback or substitution.

The spoken phrase "new session" begins intent collection and asks for the task. The model-facing
`create_session` tool accepts only that task. Trusted broker state supplies the selected profile's
host, working directory, and provider/model. Paths such as `~/` pass unchanged for expansion by the
selected daemon. The proposal readback includes all resolved settings and requires a later explicit
confirmation. Alpha omits title collection.

Successful creation requires a validated Paseo `agentId`, which becomes the current session. An
ambiguous result is reported as outcome unknown and reconciled through a session refresh without
guessing the created session.

## Package publishing

The repository is open source under MIT, but the package remains marked `private` to prevent
accidental npm publication while the project is early alpha.

## Privileged Rust backend

The phased migration is complete. One Rust process owns the browser server, audio and OpenAI
Realtime bridge, summarisation, reply provenance, ordered summary queue, proposal and confirmation
state, recovery metadata, secret resolution, the Paseo credential, and the only Paseo write path.
Browser assets remain plain secret-free JavaScript. Node.js is tooling only.

Response proposal and confirmation calls cannot accept a destination thread. Rust derives the
destination from the immutable summary context created when the reply was read. The removed
TypeScript backend remains available only through Git history and cannot be selected at runtime.

Exactly-once delivery requires a receiver-recognised idempotency identifier and an authoritative
delivery receipt. Rust alone does not provide that guarantee. Until the supported Paseo CLI can
accept a stable caller-supplied message identifier, ambiguous delivery outcomes must be reported as
unknown and must not be retried automatically.

See `docs/RUST_CONTROL_PLANE_PLAN.md` for sequencing, acceptance gates, and rollback rules.

## Deferred work

- Per-profile Paseo credentials
- Editable provider/model and working-directory selectors for new sessions
- Packaging the tool surface as an MCP server
- Phone and PWA polish
- Voice activity detection and wake-word modes
