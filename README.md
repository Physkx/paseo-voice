<p align="center">
  <img src="docs/assets/logo.svg" alt="Paseo Voice logo" width="96" height="96" />
</p>

# Paseo Voice Agent

Paseo Voice is a push-to-talk voice interface for Paseo coding-agent sessions. A local broker
reads agent replies aloud, accepts spoken steering, and protects writes with an explicit
two-phase confirmation gate.

![The Paseo Voice dashboard in mock mode](docs/assets/dashboard.png)

The project is early alpha. Interfaces and configuration may change between commits.

## Requirements

- Node.js 26 or newer and pnpm 11.13.1 for repository tooling
- Rust 1.97.0 through `rustup` for the application and repository checks
- A working `paseo` CLI for live session operations
- An OpenAI API key for realtime voice through the official OpenAI endpoint
- An OpenAI-compatible chat-completions endpoint for optional reply summaries
- One supported secret source:
  - Bitwarden Secrets Manager CLI (`bws`), which remains the default
  - 1Password CLI (`op`)
  - Process environment variables

With the default official endpoint, the broker starts in text-only mock mode when no OpenAI key is
available. Credential-free custom endpoints can still use realtime mode without that key. Set
`forceMock` or `PASEO_VOICE_MOCK=true` to disable outbound Realtime connections unconditionally.

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

Configure `paseoHosts` to populate the browser host selector. Every profile requires a stable ID,
display label, optional daemon target, default working directory, provider/model, and a default
flag. IDs must be unique and exactly one profile must be the default. The browser receives labels,
availability, and creation defaults only. Targets and the shared alpha-stage Paseo credential stay
in the Rust backend. Selection resets to the configured default on every connection.

```json
{
  "paseoHosts": [
    {
      "id": "wsl",
      "label": "Paseo WSL Host",
      "target": "paseo-wsl.example:6767",
      "default": true,
      "defaultCwd": "~/",
      "defaultProvider": "opencode/gpt-5.6-sol-max"
    }
  ]
}
```

Paths such as `~/` are passed unchanged for expansion on the selected Paseo daemon. The legacy
`PASEO_VOICE_PASEO_HOST` override targets the configured default profile only.

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
missing OpenAI key selects mock mode for the official endpoint, while a credential-free custom
endpoint can still run live and a missing Paseo password disables Paseo tools. Restart the process
after rotating a secret. Secret values remain in process memory, never enter command arguments, and
are never logged. 1Password references enter the short-lived `op` process argument list but are
redacted from application logs. Use `forceMock` or `PASEO_VOICE_MOCK=true` when outbound Realtime
must be disabled regardless of endpoint.

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

Confirm that `/healthz` reports the expected mode and that Paseo tools work without exposing a
secret value or reference. Stop the broker with Ctrl+C.

To verify best-effort degradation, temporarily replace one local test reference with an unused
`op://` reference and start the broker again. It should still start, report only the affected
capability as unavailable, and never print the reference or secret output. Do not commit the local
references or any secret values.

See [DECISIONS.md](DECISIONS.md) for current architectural decisions and
[docs/IMPLEMENTATION.md](docs/IMPLEMENTATION.md) for implementation details. Current hosting and
deployment facts are maintained in [docs/agents/state.md](docs/agents/state.md).

## Commands

| Command           | Purpose                                                 |
| ----------------- | ------------------------------------------------------- |
| `pnpm build`      | Build the Rust workspace in release mode                |
| `pnpm check`      | Run formatting, lint, browser and Rust tests, and build |
| `pnpm console`    | Open the Rust text console                              |
| `pnpm format`     | Format tracked source and documentation                 |
| `pnpm lint`       | Lint browser JavaScript and tooling                     |
| `pnpm rust:build` | Build the Rust workspace in release mode                |
| `pnpm rust:lint`  | Run Clippy across the Rust workspace                    |
| `pnpm rust:test`  | Run all Rust tests                                      |
| `pnpm start`      | Start the Rust broker and browser                       |
| `pnpm test`       | Run browser JavaScript and Rust tests                   |

## Project layout

- `crates/paseo-safety-core/`: pure provenance and confirmation state machine
- `crates/paseo-control-plane/`: Rust runtime, adapters, protocol, and tests
- `public/`: browser push-to-talk client with no build step or secrets
- `docs/`: architecture and implementation documentation
- `docs/agents/`: task-specific agent rules, operational state, and deployment playbooks

## Roadmap

The goal is a real-time voice assistant with a visual GUI and talking avatar for monitoring and
steering multiple coding-agent threads. It should announce short summaries as agents finish,
accept a spoken or typed response, and guarantee that the response can only be submitted to the
thread that produced the summary.

The backend migration is complete. One privileged Rust process owns browser WebSocket and audio,
OpenAI Realtime, summarisation, reply provenance, confirmation, recovery metadata, secrets, and
the only Paseo write path. Browser assets remain JavaScript and Node.js remains repository tooling.
See [docs/RUST_CONTROL_PLANE_PLAN.md](docs/RUST_CONTROL_PLANE_PLAN.md) for the phased record and
[docs/RUST_DECISIONS_PENDING.md](docs/RUST_DECISIONS_PENDING.md) for choices retained for final
review. The implementation sequence for dictation work is maintained in
[docs/DICTATION_IMPLEMENTATION_PROMPTS.md](docs/DICTATION_IMPLEMENTATION_PROMPTS.md).

### 1. Rust control-plane foundation - complete

- Maintain a Cargo workspace with a pure safety-core crate and a control-plane executable.
- Keep response proposals bound to an immutable summary context,
  never a caller-selected destination thread.
- Model summary, proposal, confirmation, dispatch, and delivery states explicitly and reject
  invalid transitions.
- Preserve property, concurrency, crash-recovery, cross-thread routing, mock-runtime, and Realtime
  integration tests.
- Keep the removed TypeScript backend available only through Git history as rollback evidence.

### 2. Reliable real-time foundation

- Harden the existing browser, broker, Paseo CLI, realtime voice, and local summariser loop.
- Detect newly completed agent replies without rereading or announcing the same reply twice.
- Keep summaries short and outcome-first, with concrete results and any question or blocker.
- Preserve the existing two-phase proposal and confirmation gate for every write.
- Add integration coverage for disconnects, reconnects, retries, duplicate events, and degraded
  summarisation.

Manual reads now suppress duplicate output on the same trusted host without clearing the active
response context or pending proposal. Successful and degraded speakable summaries have a local
2,400-character ceiling. Realtime output is correlated to bounded single-use response, item, and
call IDs so barge-in quarantines late audio, transcripts, tools, and completion events. Long-reply
summarisation runs asynchronously without blocking interruption, host, or provider events, and
replacement reads use bounded local fallback rather than overlap requests. The summary queue is now
process-wide and content-free, surviving browser reconnects so an already-handled reply is not
announced again; automatic completion observation that would populate it without a manual read is
still open work, gated on a stable Paseo completion marker. In the live Realtime
path, initial routing presentation, model tools, changed-host selection, and browser confirmation
now run as ownership-moving jobs without blocking the browser and provider transport loop. The mock
runtime remains synchronous. Provider loss, ambiguous acknowledgements, exhausted event registries,
and connection timeouts close the local session so the browser can establish a fresh
protocol-versioned connection instead of guessing state ownership.

### 3. Provenance-bound summaries and responses - alpha complete

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

The browser and broker now use an exact protocol version 2 handshake. Typed turns and microphone
recordings carry strictly increasing connection IDs plus the immutable summary ID displayed at turn
start. The browser waits for a correlated typed-turn acceptance before clearing the draft. Provider
responses retain the captured context through every follow-up, and `send_message` fails if another
summary has since become active. Realtime operations with acknowledgements that cannot be safely
correlated are serialised and reconnect on ambiguity instead of being assigned to newer work.

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

The summary queue is now broker-owned and process-wide, deduplicated by source thread and reply,
with deterministic observation ordering and a single active context. It survives browser reconnects
without carrying summary content, an active context, or a pending proposal, so an already-handled
reply is not announced again. The full concurrent flow, with explicit queued, summarising,
speaking, and awaiting-response states and a visible remaining-queue count, is populated only once
automatic completion detection lands, which remains gated on a stable Paseo completion marker.

### 5. Voice-created sessions and host profiles - alpha complete

- Treat "new session" as the start of a broker-enforced creation flow. The first model-originated
  `create_session` call records only the trusted interaction, discards its prompt, and asks what the
  session should work on without validating a provider, invoking Paseo, or creating a proposal.
- Allow one `create_session` proposal only after a later trusted user interaction supplies the task.
  Repeated same-interaction calls remain inert, and host changes or cancellation clear collection.
- Configure an explicit broker-side list of Paseo host profiles. Each profile has a stable ID,
  display label, daemon target, default working directory, default provider/model, and at most one
  profile is the default.
- Keep daemon targets and credentials in the broker. Send only profile IDs, labels, and availability
  to the secret-free browser. Alpha uses one shared Paseo credential across profiles, with
  per-profile credentials deferred.
- Show a persistent host dropdown near the current-session indicator. Host selection is scoped to
  one browser connection, resets to the configured default after reconnect, and applies to listing,
  reading, sending, and creating sessions.
- Clear the current session, drafts, pending proposals, and confirmation tokens when the selected
  host changes. Never preserve or retarget host-bound state across profiles.
- Show the selected profile's working directory and provider/model as read-only values during the
  alpha creation flow. Provider/model and directory selectors remain deferred.
- Expose model-facing `create_session` instead of `start_run`, accepting only the later task prompt.
  Resolve the selected host, working directory, and provider/model from trusted broker state rather
  than model claims. Keep deterministic console commands on their existing trusted path.
- Pass paths such as `~/` unchanged so the selected daemon expands them against its own home
  directory. If the directory is missing, fail without falling back to another directory.
- Validate the configured provider/model against the selected daemon before proposing creation.
  Block unavailable profiles or providers explicitly, and never silently fail over or substitute a
  host, provider, or model.
- Read back the host label, working directory, provider/model, and task. Reject confirmation on the
  task interaction and require a third trusted browser interaction through the explicit Confirm
  control. Omit explicit title collection for the alpha flow.
- After confirmation, accept success only with a validated Paseo `agentId`, then make that session
  current. Treat timeouts and malformed success output as outcome unknown, reconcile by refreshing
  the session list, and never guess which session was created.

### 6. Agent dashboard and talking avatar - alpha foundation complete

- Replace the audio-terminal layout with agent cards showing thread name, provider, live state,
  latest short summary, and queued-response count.
- Add a talking avatar with lip sync and clear listening, thinking, speaking, awaiting-approval,
  and error states.
- Keep the active thread name visible beside the avatar and response field as a continuous
  provenance cue.
- Make status and routing understandable without relying on colour, animation, or audio alone.
- Preserve a lightweight, secret-free browser client and responsive keyboard, pointer, and touch
  controls.

The alpha dashboard now renders safe selected-host agent snapshots, the active bounded summary,
broker-owned routing text, queue counts, and an accessible CSS avatar. It exposes state through text
and ARIA as well as colour and motion, respects reduced-motion preferences, and keeps typed and
push-to-talk steering available. Automatic process-wide completion observation and non-zero summary
queue population remain phase 2 and phase 4 work.

### 7. Natural interruption and recovery

- Support barge-in by stopping speech and playback as soon as the user starts talking.
- Preserve the interrupted summary and its source context so it can be resumed or replayed without
  changing the response target.
- Separate conversational interruptions from confirmations so incidental speech can never approve
  a pending write.
- Recover cleanly from microphone, audio, network, realtime API, summariser, and Paseo failures.
- Review the interruption, turn-taking, and barge-in pipeline patterns documented by the Pipecat
  project as design prior art for the native implementation. Pipecat remains a reference only,
  never a runtime dependency inside the broker trust boundary.

Browser confirmation is now explicit-control only. The Realtime model has no confirmation tool,
and Confirm or Cancel is bound to the exact displayed proposal through a broker-generated
presentation handle. Ordinary speech, silence, text turns, stale readbacks, and replaced proposal
gestures cannot approve a write.

Live turns now carry connection-scoped recording IDs, and microphone loss, page blur, hidden-page
interruption, or stale controls abort without committing audio. Accepted turn starts establish an
ordered playback cutoff, while browser and broker frame limits keep queued PCM bounded.

An interrupted current summary can be replayed from the beginning without rereading Paseo or
changing its destination. Replay accepts no caller-selected context and runs in a broker-enforced
tool-disabled turn, so summary content cannot invoke another capability.

### 8. Audited response timeline

- Maintain a searchable timeline of summaries and responses with source thread, destination
  thread, confirmation state, timestamps, and delivery result.
- Record durable identifiers and routing metadata without storing credentials or unnecessary
  sensitive content.
- Make failed and cancelled responses clearly distinguishable from delivered responses.
- Provide retention and deletion controls before enabling durable history by default.

The current read-only `list_operation_timeline` tool searches recent dispatched-operation metadata
using exact state, summary ID, and destination ID filters with bounded snapshot pagination. It
contains opaque operation, summary, destination, timestamp, delivery state, and optional receiver
IDs only. It does not persist or return summaries, responses, transcripts, or agent output. Durable
cancelled and expired proposal history remains deferred with the broader retention and deletion
decision.

### 9. Browser dictation mode - alpha complete

- Add a prominent Live Response toggle. New browsers default to live response, and the selected
  mode persists locally across reloads without persisting dictated content.
- When Live Response is on, preserve the existing conversational Realtime and tool behavior. When
  it is off, transcribe English speech, clean it, and insert the final text into the bound response
  draft without creating an assistant response or submitting a write.
- Insert at the saved caret or replace only the saved selection. Preserve the rest of the draft,
  add exactly one space between adjacent word characters, add no space before punctuation, and
  preserve intentional paragraphs and list formatting within the dictated text. Never reformat
  text outside the inserted or replaced range.
- Keep partial transcription in an ephemeral preview and insert the final cleaned result
  atomically. If cleanup fails and the target is still valid, insert the raw transcript with a
  visible degraded warning. If transcription fails or detects no speech, leave the draft unchanged.
- Capture the original field, selection, host, and immutable response context when recording
  starts. If only the field selection becomes stale while host and provenance remain unchanged,
  require an explicit Insert or Discard choice. If the host or immutable response context changes,
  discard the result and clear the draft under the existing invalidation rules.
- Allow only one recording or cleanup operation per browser connection. Provide a visible Cancel
  control and Escape shortcut that discard buffered audio and partial text, restore the original
  draft and selection, and retain no recovery copy.
- Support hold-to-record and tap-to-toggle interactions, with hold-to-record as the default. Add
  optional auto-stop after a configurable silence period for toggle recording and configurable
  page-scoped shortcuts with text-entry and conflict safeguards.
- Add optional start, stop, success, and error sounds, enabled by default at a restrained volume,
  while keeping equivalent visual and text states. Request echo cancellation, noise suppression,
  automatic gain control, and short pre-roll buffering by default, with advanced overrides for
  incompatible microphone setups.
- Add a microphone picker that stores only the chosen device identifier locally and visibly falls
  back to the system default when the device is unavailable.
- Insert directly through the page without reading or changing the system clipboard. Dictated text
  remains an editable draft, and sending continues through the existing proposal and explicit
  confirmation gate.

Host and immutable-context changes now terminate browser capture and broker recording,
transcription, or cleanup work with an operation-correlated cancellation. Late provider events and
stale pointer or key releases cannot revive the discarded draft operation.

All live and dictation captures share one monotonic connection-scoped recording sequence. Dictation
starts require a broker-validated active summary, and terminal controls must match the opaque
operation returned for that exact recording. Conversation turns remain gated until capture,
transcription, cleanup, or cancellation has reached a correlated terminal state.

Microphone setup and recovery are request-generation scoped. Permission loss requires explicit
Retry, stale setup results cannot replace current resources, and selected or system-default device
changes recover without persisting labels or device fingerprints.

### 10. Dictation customisation and private processing

- Expose separate broker-approved provider and model choices for speech-to-text and cleanup. Show
  availability, processing location, and degraded state without exposing endpoints, credentials,
  or unrestricted caller-selected models to the browser.
- Keep API credentials in the broker's existing secret-provider boundary. The GUI may show setup
  guidance and capability status but must never accept, store, or transmit secret values.
- Provide one original strict cleanup prompt that treats speech as text to edit, never as an
  instruction to answer or execute. Add GUI controls to view, edit, reset, and test the prompt while
  preserving intent and returning only cleaned text.
- Support an explicit locally stored English vocabulary list for names, acronyms, and technical
  terms. Use it only to guide transcription and cleanup, and provide clear review and deletion
  controls.
- Add user-managed spoken snippets after the core dictation path is reliable. Expand snippets only
  into the editable draft, never submit them automatically, and keep secret values out of synced or
  exported settings.
- Add broker-hosted local speech-to-text and cleanup as a later privacy and offline phase. Keep
  model downloading, process management, and GPU use outside the browser.
- Evaluate Speaches (`speaches-ai/speaches`) with Kokoro or Piper voices as a broker-approved
  local OpenAI-compatible speech-to-text and text-to-speech server. The existing OpenAI-compatible
  client boundary should make a local endpoint a configuration change rather than a new
  integration, and local text-to-speech would allow spoken summaries without the realtime API.
- Never cross from local processing to a cloud provider automatically. Preserve the draft, report
  the failure, and require an explicit user retry with cloud processing.
- Keep dictation and cleanup English-only. Do not add automatic language detection or translation.

### 11. System-wide desktop companion

- Add a separate future desktop companion for dictation into editors, terminals, email, and other
  applications. Keep the browser milestone independent so desktop automation cannot weaken the
  broker or response-provenance boundary.
- Capture the original focused application and text target before recording. Paste only back to
  that target, detect stale or unavailable targets, and require an explicit recovery choice instead
  of pasting into whichever application later has focus.
- Preserve all available clipboard formats, paste the cleaned text using platform-appropriate
  normal or terminal behavior, and restore the clipboard only if it still contains the companion's
  temporary value. Offer an explicit option to keep dictated text in the clipboard.
- Support configurable global hold, toggle, and cancel hotkeys, including multiple bindings,
  platform permission checks, and conflict errors. Never provide a voice or global-hotkey path for
  confirming Paseo writes or permissions.
- Add opt-in system media pausing. Resume only media that the companion itself paused.
- Keep credentials, model endpoints, transcripts, drafts, and agent output out of desktop command
  arguments, logs, crash reports, and durable storage.
- Study Handy (`cjpais/Handy`, MIT licensed and written in Rust) as a reference implementation for
  global hotkey capture, platform permission checks, and paste-back behavior, and reuse its
  approaches where they fit the companion's safety rules.

### 12. Retention-gated learning and history

- Defer automatic correction learning until an approved design defines what edits are observed,
  how corrections are distinguished from rewrites, and how learned entries can be reviewed,
  undone, exported, and deleted.
- Defer transcript, discarded-recording, and dictation-content history until retention duration,
  encryption, deletion, export, access-control, and crash-recovery rules are approved.
- Keep the initial dictation pipeline ephemeral. Persist only explicit non-content preferences.
  Persist user-managed cleanup prompts, vocabulary, or snippets only after a focused local-settings
  decision approves their storage, redaction, reset, and deletion controls.

### 13. Alternative realtime voice providers

- Treat OpenAI Realtime as the first supported realtime voice provider rather than a permanent
  dependency. Keep session negotiation, audio transport, and tool events behind one broker-owned
  provider boundary so adding a provider is an adapter change, not a rewrite.
- Research alternative realtime speech-to-speech APIs before selecting a second provider. Current
  candidates include Google Gemini Live, Amazon Nova Sonic, the xAI Grok Voice Agent API, Hume
  EVI, ElevenLabs Conversational AI, Deepgram Voice Agent, and the AssemblyAI Voice Agent API.
  Compare latency, barge-in behavior, tool calling, transcription quality, protocol shape, and
  pricing against the existing tool loop and confirmation gate.
- Prefer providers that follow the OpenAI Realtime event schema where quality is comparable. The
  schema is emerging as a de facto standard across vendors, which keeps adapters small and
  testable.
- Add a broker-configured OpenAI-compatible realtime endpoint override so the realtime client can
  target a self-hosted server such as the LocalAI realtime API. This reuses the existing
  OpenAI-compatible client boundary already planned for summaries and dictation.
- Reserve local realtime endpoints for powerful local setups only. Document the expected hardware,
  keep the option off by default, and show clearly whether voice processing is local or cloud.
- Never fail over automatically between providers or between local and cloud endpoints. Preserve
  state, report the failure, and require an explicit user choice, matching the dictation privacy
  rules.
- Keep provider selection, endpoints, and credentials in the broker and its secret providers. The
  browser receives provider labels, availability, and processing location only.
- Hold every provider to the same safety bar. The two-phase proposal and confirmation gate,
  provenance-bound responses, and cross-thread retargeting rejection must pass the existing
  integration tests unchanged for each provider before it is offered.

### Dictation milestone criteria

The browser dictation milestone should demonstrate that switching Live Response off prevents an
assistant response, produces one English transcript, cleans it, and inserts it exactly once at the
captured caret or selection without changing the clipboard. Tests must cover cancellation, silent
audio, cleanup fallback, stale field and provenance context, reconnects, host changes, concurrent
start attempts, smart spacing, and confirmation-gate preservation. No transcript, cleanup output,
draft, or recording may be written to durable storage.

Implementation decisions that still require review are tracked in
[`docs/DICTATION_DECISIONS_PENDING.md`](docs/DICTATION_DECISIONS_PENDING.md).

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
