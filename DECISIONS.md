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

## Package publishing

The repository is open source under MIT, but the package remains marked `private` to prevent
accidental npm publication while the project is early alpha.

## Privileged Rust control plane

The backend will migrate in phases to a privileged Rust control-plane process. Rust will own reply
provenance, the ordered summary queue, proposal and confirmation state, delivery identifiers,
recovery metadata, the Paseo credential, and the only Paseo write path.

The TypeScript broker remains the browser, audio, and OpenAI Realtime adapter during the migration.
The Rust control plane runs out of process and exposes a narrow local interface. It is not a native
Node.js addon. Proposal and confirmation calls do not accept a destination thread; the control
plane derives the destination from an immutable summary context that it already owns.

Migration is incremental. The Rust decision engine must first run in shadow mode against the
existing TypeScript behavior. Credential and write authority move only after the safety contract,
cross-thread routing tests, concurrency tests, and crash-recovery tests pass. After cutover, the
TypeScript process must have no Paseo write credential and no alternate write path.

Exactly-once delivery requires a receiver-recognised idempotency identifier and an authoritative
delivery receipt. Rust alone does not provide that guarantee. Until the supported Paseo CLI can
accept a stable caller-supplied message identifier, ambiguous delivery outcomes must be reported as
unknown and must not be retried automatically.

See `docs/RUST_CONTROL_PLANE_PLAN.md` for sequencing, acceptance gates, and rollback rules.

## Deferred work

- Selecting among multiple remote Paseo daemons
- Packaging the tool surface as an MCP server
- Phone and PWA polish
- Voice activity detection and wake-word modes
