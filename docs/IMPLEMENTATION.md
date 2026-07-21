# Implementation guide

This document describes the current runtime. Security invariants are normative in
`RUST_SAFETY_CONTRACT.md`; historical rationale is in `../DECISIONS.md`.

## Architecture

```text
Secret-free browser
  | local HTTP and protocol-v3 WebSocket
  v
Rust paseo-control-plane
  | selected OpenAI, xAI, or compatible Realtime WebSocket
  | fixed summary HTTP plus selected cleanup-profile HTTP
  | direct paseo, bws, or op child processes
  | content-free SQLite operation journal
  v
Paseo coding-agent sessions
```

`paseo-control-plane` is the only production backend. It serves `public/`, owns all credentials and
mutable routing state, and executes the only Paseo write path. `paseo-safety-core` contains the pure
reply queue and proposal state machine. Node.js and pnpm run repository tooling only.

Production composition injects the listener, monotonic clock, HTTP client, Realtime connector,
dictation cleaner, and process executor. Tests replace these boundaries with fakes, loopback
services, and temporary journals.

## Runtime commands

- `paseo-control-plane serve` serves the browser, `/healthz`, and `/ws`.
- `paseo-control-plane console` opens a deterministic text interface over the same tool engine.
- `paseo-control-plane --serve-stdio` runs the retained strict protocol test harness. Production
  does not spawn it.

The default listener is `127.0.0.1:8790`. Browser WebSocket upgrades with an `Origin` header must
match the HTTP `Host`. Native clients without an `Origin` are accepted, so this check is not remote
authentication.

## Browser session

Each browser connection owns its selected host, voice profile, cleanup profile, provider generation,
trusted interaction sequence, active summary context, voice mode, recording sequence,
session-creation task collection, and pending proposal presentation. Reconnect starts from the
configured defaults and inherits none of that selection or content state.

The browser must send the exact protocol version 3 hello within five seconds. Typed turns carry a
strictly increasing turn ID and the displayed summary ID or explicit null context. Push-to-talk
recordings use their own strictly increasing IDs. Stale or mismatched context is rejected before
content reaches Realtime or the tool engine.

Binary frames contain PCM16 audio at 24 kHz. Browser and broker buffers are bounded. Starting an
accepted recording clears provider input, retires active playback, and sends an ordered playback
flush to support barge-in.

The dashboard receives bounded presentation data: safe host and session labels, safe voice and
cleanup profile metadata, derived processing locations, provider and state labels, one active short
summary, routing text, and queue counts. It never receives endpoint URLs, credential IDs, secret
references, secrets, daemon targets, raw Paseo rows, logs, or the internal confirmation token.

## Tool surface

The Realtime model can:

- List and select sessions.
- Read the latest reply and bind it as the active response context.
- Replay the current short summary without rereading Paseo.
- List pending permissions without approving them.
- Propose a response with `send_message`; the tool accepts text and no destination.
- Begin or continue broker-gated session creation.
- Cancel a pending proposal.
- Query content-free operation status and timeline metadata.

Model tool calls run in provider output order. IDs, output indices, argument strings, aggregate
queued arguments, and event registries are bounded. Duplicate exact events are inert; conflicting
reuse or ambiguous ordering closes the provider connection.

Process-capable live Realtime work moves sole ownership of the tool engine to one blocking job at a
time. The browser and provider transport loop remains responsive while session probes, tools, host
changes, or confirmation work runs. The text-only mock runtime remains synchronous.

## Reply context and confirmation

A successful manual reply read creates an immutable summary context from the trusted host, source
thread, and reply identity. Paseo currently supplies no stable reply ID, so the broker uses the
source thread plus exact output digest as its interim identity. Reading the same unchanged reply is
non-destructive and does not announce it again.

Only one summary context is active for response. `send_message` stores exact response bytes in a
single-use proposal derived from that context. Changing host or reading a different reply clears the
old draft and proposal.

The browser receives a fresh proposal presentation handle. The Realtime model cannot confirm, and
typed or voice turns are blocked while confirmation is pending. The exact current Confirm control
consumes the presentation, journals `dispatching`, and makes one Paseo attempt. Duplicate or stale
controls do nothing. Disconnect cannot abort an already-authorised classification job or cause a
retry.

Model-originated session creation uses three separate user interactions: one starts task collection,
a later interaction supplies the task and may create a proposal, and a later explicit browser
control confirms it. Host, working directory, and provider/model come from the selected trusted
profile.

## Realtime lifecycle

The provider adapter owns handshake construction, exact credential attachment, session shape,
model, voice, transcription model, provider-specific event normalization, and capability
declarations. Official OpenAI and xAI endpoints are exact-pinned. Compatible loopback endpoints may
be keyless and remote compatible endpoints require WSS. xAI cumulative
`conversation.item.input_audio_transcription.updated` events become replacement previews.

The connection supervisor keeps the browser socket alive while an idle voice-profile switch closes
and retires the old provider, advances a generation, clears provider and content-bound transient
state, invalidates any unconfirmed proposal and summary, preserves the selected Paseo host, and
connects the selected profile. Work from a retired generation cannot publish. A confirmed write
dispatch is never interrupted or retried.

The broker correlates provider responses, items, function calls, audio commits, and transcription by
broker-owned state and bounded single-use IDs. OpenAI Realtime does not correlate every server event
to a client event, so the broker permits only one unresolved response creation and one unresolved
audio commit. Timeout or ambiguous acknowledgement closes the connection rather than guessing.

The first terminal event freezes a response. Completed responses drain accepted calls in order;
non-completed responses suppress queued and late output. Before the first model tool, the broker
captures an ephemeral checkpoint of mutable routing and proposal state. A failed or cancelled
provider response restores it unless an explicit browser cancellation, confirmation, or host change
has taken authority.

Long-reply summarisation runs asynchronously and is correlated to its response generation and
summary context. Only one request is retained per browser connection. A replacement long read uses a
bounded cleaned-tail fallback rather than overlapping requests. Speakable output is capped at 2,400
Unicode characters.

The opt-in automatic reply poller runs per live browser connection on a separate async task and uses
the selected host's read-only Paseo adapter through `spawn_blocking`. The first successful session
list is a baseline. A later non-idle to idle transition reads the bounded latest reply and sends it
through an eight-entry in-memory channel. While Live Response is quiescent and no proposal awaits
confirmation, Rust activates the reply provenance without advancing user interaction, publishes the
bound dashboard context, and creates one out-of-band Realtime audio response. Its input is bounded to
24,000 Unicode characters, its tools are disabled, and its instructions require a short spoken
summary. Host changes reset the poll baseline. Dictation, active responses, and pending proposals
delay consumption without weakening the write confirmation gate.

Each connection's `SummaryQueue` stores reply identities and deterministic ordering only. An eligible
graceful disconnect can return committed deduplication state to a shared in-memory snapshot for a
later connection. Concurrent connection snapshots are independent and are not merged, so concurrent
automatic pollers may announce the same reply. Queue state is lost when the broker exits.

## Dictation

Connections start in `live_response` mode. Dictation is available only after a manual reply read has
created an active immutable summary context. Switching to `dictation` keeps the same microphone path
but commits audio only for English transcription. It does not call `response.create`, expose tools,
or create a proposal.

One recording, transcription, or cleanup operation may exist per connection. The browser captures
the draft, selection, host, field, and immutable summary ID when recording begins. Rust binds the
operation to its recording ID, operation ID, host, summary, provider generation, and selected
cleanup profile. Cleanup uses that profile's isolated client, a strict editing-only prompt, and a
12-second request timeout. A successful bounded raw transcript is used with a degraded warning if
cleanup fails. No other profile is tried, and no speech or transcription failure leaves the draft
changed.

Insertion is atomic at the captured selection. A changed field or selection requires explicit
Insert or Discard review. A changed host or summary discards the result. Cancel restores the original
draft only while that target remains valid. Dictation never uses the system clipboard.

Each committed dictation item is deleted from the Realtime conversation before the operation reaches
a terminal browser state or the connection accepts context-reusing work. Missing or ambiguous item
deletion closes the provider connection.

The browser stores only non-content preferences for voice mode, microphone device ID, recording
mode, silence timing, audio processing, sound cues, and conflict-checked page shortcuts. Audio,
transcripts, previews, drafts, cleanup output, and device labels remain ephemeral.

## Configuration and secrets

Rust loads `~/.config/paseo-voice/config.json`, applies unrelated runtime `PASEO_VOICE_*`
environment overrides, and validates endpoint, profile, credential, private-host allowlist, and
host-profile configuration before opening the listener. Legacy OpenAI and Spark routing and secret
fields are rejected instead of migrated.

One secret provider resolves the named API credential catalogue plus the separate Paseo password:

- Bitwarden Secrets Manager is the default and resolves configured IDs through `bws`.
- 1Password resolves configured references through `op read` and inherits the environment needed
  for desktop or service-account authentication.
- Environment mode reads only each configured `environmentVariable` and `PASEO_PASSWORD`.

Named credentials are resolved once into a bounded broker-only map keyed by credential ID. A
profile receives only the credential it explicitly references. The Grok CLI OAuth store remains a
separate optional fallback only for exact xAI dictation cleanup and is never used for voice or fixed
summarisation.

Paseo and Bitwarden receive narrow child environments. Paseo receives its password through the child
environment, never an argument. Secret values and process output content are excluded from logs.

URL credentials, configured queries, fragments, and unsupported schemes are rejected. Processing
location is derived from the exact validated host as `local`, `private_remote`, or `cloud`.
Plaintext model HTTP is limited to loopback or an explicitly opted-in Tailscale IPv4 or exact
broker-allowlisted host. Separate no-proxy, no-redirect clients keep credentials bound to their one
validated model endpoint.

## Process and recovery boundary

Paseo and secret-manager programs are invoked directly without a shell. Output capture is bounded to
8 MiB per stream and covered by a monotonic overall deadline. On Unix, children run in owned process
groups so an unreaped timed-out process can be killed with its remaining group. Platform-specific
cleanup details are tested in `system_process_executor.rs`.

A read-only Paseo result is accepted only after certain spawn, capture, and exit-zero completion.
For writes, spawn failure is `rejected`; every uncertain result after spawn is `outcome_unknown`.
Delivery requires a validated `messageId`, and session creation requires a validated `agentId`.

The SQLite journal stores opaque operation, summary, destination, digest, timestamp, state, and
receiver metadata only. It is mode 0600 with a mode 0700 parent directory on Unix. Startup recovery
maps unfinished dispatches to `outcome_unknown`, invalidates pending records, and never sends.

## Verification

```bash
pnpm check
```

This runs Prettier, rustfmt, agent-document lint, browser JavaScript lint and tests, Clippy, all Rust
tests, and the release build. Coverage includes provenance substitution, concurrent confirmation,
protocol framing, browser lifecycle, Realtime ordering and interruption, dictation cancellation,
host changes, secret providers, endpoint credential isolation, process uncertainty, and journal
recovery.
