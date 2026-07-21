# Architecture history and decisions

This file keeps only decisions that explain or constrain the current system. Detailed implementation
belongs in `docs/IMPLEMENTATION.md`; operational deployment facts belong in
`docs/agents/state.md`. Superseded plans remain available in Git history.

## Short history

- **Initial local broker, 2026-07-18:** Paseo Voice started as a TypeScript broker connecting a
  secret-free push-to-talk browser to Paseo through its supported CLI. It already used proposal and
  confirmation as separate write steps.
- **Rust safety boundary, 2026-07-18:** Wrong-thread delivery and ambiguous recovery risks led to a
  pure Rust safety core and then a complete Rust backend migration. Rust became the sole credential,
  provenance, confirmation, journal, and Paseo write owner. The TypeScript backend was removed.
- **Trusted routing, 2026-07-18:** Host profiles and session creation moved behind broker-owned
  configuration. Browser and model inputs can select a profile ID but cannot supply a daemon target,
  credential, or response destination.
- **Dashboard and dictation, 2026-07-18:** The browser grew into an agent dashboard and gained an
  English dictation mode. Dictation edits an ephemeral draft and cannot create assistant responses,
  call tools, or submit writes.
- **Protocol-enforced context, 2026-07-19:** Browser protocol version 2 added immutable summary
  context, correlated turns and recordings, and connection-bound proposal presentations. Safety no
  longer depends on display text or model compliance.
- **Realtime isolation and reconnect deduplication, 2026-07-20 to 2026-07-21:** Process-capable work
  moved off the Realtime transport loop. A broker-owned snapshot began carrying committed reply
  deduplication between sequential browser connections, while active summaries, drafts, proposals,
  and confirmation authority stayed connection-scoped.

## Current decisions

### One privileged Rust backend

The production application is one Rust process. It serves the browser, connects to Realtime and the
model endpoint, resolves secrets, runs the supported Paseo CLI directly without a shell, owns the
SQLite metadata journal, and provides the only write path. Browser assets remain plain JavaScript;
Node.js is repository tooling only.

`paseo-safety-core` stays deterministic and free of I/O. Process, network, clock, socket, and storage
boundaries remain injectable for tests.

### Immutable provenance and explicit confirmation

Reading a reply creates an immutable context containing a summary ID, source thread ID, source reply
identity, and observation time. `send_message` accepts response text only. Rust derives the write
destination from the source thread and stores the exact response bytes before confirmation.

The Realtime model has no confirmation tool. The browser receives an opaque presentation handle,
not the safety token, and only the exact current Confirm control may execute the proposal after a
later trusted interaction. The local console retains a separate deterministic text confirmation
path. Proposals expire, are single-use, and cannot survive a host or context change. Paseo permission
requests may be listed but never approved through voice.

### Conservative delivery and recovery

Each accepted confirmation starts at most one child-process write attempt. Delivery requires exit
zero and a validated Paseo receiver ID. Once a child exists, timeout, malformed output, missing
receipt, output truncation, signal, or nonzero exit is `outcome_unknown`, not a safe retry condition.

The SQLite journal records content-free operation metadata before dispatch. Restart recovery marks
unfinished dispatches unknown and invalidates pending operations; it never reconstructs a response
body or starts another write. Exactly-once delivery is not claimed because Paseo does not accept a
caller-supplied idempotency identifier.

### Ephemeral content

Runtime-generated audio, transcripts, summaries, drafts, responses, session prompts, agent output,
and device labels are not persisted. The browser may store only non-content interaction preferences.
The journal retains at most 10,000 metadata transitions containing opaque IDs, hashes, timestamps,
states, and optional receiver IDs. Operator-managed JSON configuration may contain host targets,
secret references, directories, and provider settings, but never secret values.

The same-origin browser may receive safe, bounded presentation fields: host and session IDs and
labels, availability, provider/model and state labels, profile working-directory defaults, one short
active summary, queue counts, routing text, capability IDs and location labels, and opaque summary,
operation, and presentation handles. A bounded dictation transcript and cleanup result may return
only to the browser connection that started the operation. Browser code must not persist these
runtime fields or include them in static assets.

Durable content, correction learning, custom prompts, vocabulary, snippets, and cancelled-proposal
history remain blocked until retention, encryption, access, deletion, export, and recovery rules are
approved.

### Endpoint and credential isolation

OpenAI, Paseo, and optional model (summariser / dictation cleanup) secrets are resolved once at
startup from one selected provider: Bitwarden Secrets Manager, 1Password CLI, or the process
environment. When a manager does not supply the model key, `PASEO_VOICE_SPARK_API_KEY` or
`XAI_API_KEY` remains a narrow process environment fallback that is not forwarded to secret-manager
children. Values remain in memory and are excluded from logs.

The OpenAI bearer is sent only to the exact official Realtime endpoint. The model bearer is sent
only by the shared summarisation and dictation-cleanup HTTP client, including official xAI
(`https://api.x.ai/v1`) and other OpenAI-compatible HTTPS endpoints. Plain model HTTP remains
loopback-only unless the operator explicitly opts in to a Tailscale IPv4 endpoint. Other
non-loopback endpoints require TLS. The model HTTP client disables redirects and ambient proxies so
content is not forwarded to an unapproved destination.

### Trusted host profiles

Paseo targets are configured as broker-owned profiles with a stable ID, display label, optional
daemon target, default working directory, and default provider/model. Exactly one profile is the
default. Browser selection is connection-scoped and resets on reconnect. Host changes invalidate all
host-bound state, and there is no automatic fallback.

Model-originated session creation requires separate task collection, proposal, and browser
confirmation interactions. Rust supplies host, working directory, and provider/model from the
selected profile. A validated `agentId` is required before the new session becomes current.

### Grok subscription OAuth for model cleanup

The summariser and dictation-cleanup model bearer may be taken from the provider-owned Grok CLI
OAuth store at `~/.grok/auth.json` after `PASEO_VOICE_SPARK_API_KEY` and `XAI_API_KEY`. That session
is the same SuperGrok / grok.com login used by the Grok CLI. Tokens stay in memory only and are never
logged.

### Multi-provider Realtime (OpenAI and xAI)

Official Realtime hosts are first-class: OpenAI (`wss://api.openai.com/v1/realtime`) and xAI Grok
Voice (`wss://api.x.ai/v1/realtime`). Both require a cloud bearer. OpenAI uses the OpenAI secret
path; xAI prefers the model/xAI credential (including the Grok OAuth store) and falls back to the
OpenAI-named field when that is the only configured key. Custom local and remote Realtime hosts
remain supported, including credential-free private endpoints. Config field names such as
`openaiBaseUrl` stay stable for public compatibility; they describe the Realtime transport, not a
single vendor lock-in.

### Realtime and dictation isolation

Browser and provider work is correlated with bounded, single-use IDs. Ambiguous ordering, exhausted
registries, failed provider item deletion, or unrecoverable correlation closes the connection rather
than assigning stale work to a newer turn. Barge-in retires the active response and quarantines its
late media, transcript, tool, and completion events.

Dictation uses English Realtime transcription and the configured text cleanup endpoint. It is bound
to the host, summary, draft value, and selection captured at recording start. A stale field requires
explicit review; a stale host or summary discards the result. Dictation provider items are deleted
before the connection returns to reusable conversation state.

### Alpha automatic observation and sequential reconnect deduplication

When explicitly enabled with a bounded polling interval, each live browser connection may use an
alpha read-only completion heuristic until Paseo exposes a supported stable marker. A separate
poller baselines the selected host's current session states, observes a later non-idle to idle
transition, reads that session's latest reply, and submits one bounded out-of-band response to OpenAI
Realtime. That response has tools disabled, does not enter the default provider conversation, and
instructs Realtime to summarise and speak only the supplied agent output.

The heuristic may miss transitions completed between polls, may announce the same completion in
concurrent browsers, and cannot distinguish identical consecutive reply text because the interim
reply identity remains a host, session, and content digest. The poller runs outside the transport and
tool-engine ownership loops, retains only content-free session status between polls, and uses a
bounded in-memory reply channel. Automatic observation never advances trusted user interaction and
cannot confirm or execute a write. The proposal and explicit browser confirmation gate is unchanged.

Each browser connection has its own content-free summary queue and at most one active context. On an
eligible graceful close, the connection returns its committed snapshot to a broker-owned in-memory
slot that can seed a later connection. Concurrent connections do not merge their snapshots, so this
mechanism is sequential reconnect deduplication rather than one authoritative multi-client queue. It
never carries summary text, active context, drafts, proposals, or confirmation authority and does
not survive broker restart.

### Local-only deployment boundary

The broker defaults to loopback and has same-origin browser checks, but no application-level remote
authentication or TLS termination. It must not be exposed directly to a public or shared network.
Any future static GUI deployment is separate from broker deployment and cannot include private
configuration or runtime content.

## Open boundaries

- Automatic completion detection requires a stable Paseo completion or reply marker.
- Exactly-once delivery requires receiver-recognised idempotency and an authoritative receipt.
- Remote use requires authentication, encrypted transport, and an approved origin policy.
- Provider catalogues and local model lifecycle management need a focused design before
  implementation.
- Per-profile credentials and editable new-session defaults remain deferred.
- Any durable user content or correction learning requires an explicit retention and deletion
  policy.
- Replace alpha status polling with a stable Paseo completion and reply marker.
- A system-wide desktop companion requires a separate process, authentication, focus, clipboard,
  hotkey, packaging, and update design.
