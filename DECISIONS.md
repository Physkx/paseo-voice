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

Resolution remains best effort per secret. A missing or failed OpenAI secret selects mock mode for
the official endpoint, while a credential-free custom endpoint can still run live. A missing or
failed Paseo password disables Paseo tools. `forceMock` is the unconditional outbound Realtime
override. Rotation takes effect after a process restart.

## Endpoint transport and Realtime credentials

The final provider credential policy remains pending under D029. The current conservative policy
validates Realtime and summariser base URLs before secret resolution. URLs with credentials,
queries, fragments, missing hosts, malformed syntax, or unsupported schemes are rejected. Plain
`ws://` Realtime and `http://` summariser endpoints are accepted only for exact loopback hosts:
`localhost`, IPv4 `127.0.0.0/8`, or IPv6 loopback. Other Realtime endpoints require `wss://`, and
other summariser endpoints require `https://`.

Only `wss://api.openai.com/v1/realtime`, with an optional trailing slash, receives
`Authorization: Bearer OPENAI_API_KEY`. The WebSocket connector does not follow redirects. Custom
loopback and secure remote endpoints receive no OpenAI credential, secret reference, or substitute
authentication header and can operate without an OpenAI key. The official endpoint requires that
key; otherwise the broker uses mock mode. Custom endpoint configuration does not claim provider
compatibility or successful health. The Realtime model is appended as one parsed URL query
parameter after endpoint classification; configured base queries are not accepted.

One shared production HTTP client serves both dictation cleanup and reply summarisation with all
redirect following and ambient proxy discovery disabled. It does not inherit `HTTP_PROXY`,
`HTTPS_PROXY`, `ALL_PROXY`, or their lowercase forms. An HTTP redirect is handled as one non-success
response, so the adapter degrades without forwarding transcript or agent-output bodies to the
redirect destination. Client construction failure returns one bounded startup error and starts no
listener. Custom model endpoints currently support neither authentication nor ambient proxies.

## Audio interaction

The client uses manual push-to-talk with PCM16 audio at 24 kHz. Starting a new push-to-talk turn
cancels active playback so the user can interrupt a response.

Every live-response and dictation recording shares one client-generated, strictly increasing,
connection-scoped recording ID sequence. Live start, end, and abort controls repeat that ID;
dictation terminal controls use the broker operation ID correlated to the same recording. Missing,
stale, cross-mode, or replayed controls are ignored. Microphone loss, page blur, and hidden-page
interruption abort without committing audio or creating a response. Every accepted start also
creates an ordered broker playback-flush boundary. Browser playback accepts at most 96,000 bytes per
PCM frame and buffers at most 48,000 samples, or two seconds at 24 kHz; excess incoming audio is
dropped rather than extending the backlog.

Each browser connection has a validated voice mode. `live_response` remains the default and keeps
the conversational Realtime and tool loop. `dictation` commits the same microphone buffer for
English input transcription but never creates an assistant response, observes a confirmation turn,
or invokes a tool. The completed transcript is returned only to that browser connection as an
ephemeral draft result. The browser may persist the non-content mode preference locally.

The broker sends each successful English transcript to a dependency-injected, text-only cleanup
boundary backed by the configured local or remote OpenAI-compatible endpoint. Cleanup has a 12 second
request timeout and bounded input and output. Its strict instruction can edit but cannot answer,
route, submit, or invoke tools. Failure returns the bounded raw transcript with an explicit
degraded status. Empty transcription returns no insertable result.

The browser captures the draft value, selection, selected host, and immutable summary identifier
at recording start. It inserts once at that range only while the snapshot remains current. Draft or
selection edits require explicit Insert or Discard. Host or summary changes discard the result and
clear the stale draft. Insertion uses the DOM selection API and never reads or changes the system
clipboard.

Dictation offers hold-to-record and tap-to-toggle, with hold as the default. Toggle recording may
auto-stop after a locally selected 1.0, 1.6, 2.5, or 4.0 seconds of silence. A recording shorter than 250 milliseconds
is treated as no change. Each connection permits one recording, transcription, or cleanup at a
time. Cancel and the page-scoped Escape key clear provider audio, abort cleanup where possible,
ignore late results, and restore the pre-recording draft and selection. Host changes and
disconnects clear state without restoring stale content.

Realtime `item_id` values correlate committed buffers, partial transcription, completion,
failure, cancellation, and a 30 second transcription timeout. The browser also requires the
connection-scoped operation ID before accepting partial or final dictation text. Dictation is
unavailable until the browser has an immutable bound summary context.

Each committed dictation item is removed from the shared Realtime conversation before any
dictation terminal, ready state, or later context-reusing turn. Cleanup and one item deletion begin
together after successful transcription and may complete in either order. Failure, timeout,
cancellation, and context changes start deletion as soon as the committed item ID is known. The
broker correlates the provider acknowledgement by exact item ID because the server event ID does
not echo the client delete event ID. Missing, rejected, conflicting, unsent, or disconnected
deletion state closes the provider connection without changing the draft. Non-empty live-response
items remain in the conversation. An owned live-response item with an empty completed transcription
is deleted before ready, and recording or context-reusing controls remain blocked until that
deletion is acknowledged.

## Browser wire protocol

The browser WebSocket uses an exact protocol version 2 hello and explicit ready response. A client
must send that exact hello within five seconds. The broker does not allocate an outbound Realtime
connection before negotiation succeeds. A client that omits or mismatches the version must reconnect
or reload rather than falling back to the earlier permissive wire shape. Realtime connection
establishment has its own five second deadline and exposes no provider or credential failure detail.
Typed turns carry a strictly increasing turn ID plus the displayed summary ID or null; the browser
clears the draft only after a matching content-free broker acceptance. Push-to-talk starts carry the
same immutable context snapshot. The broker rejects stale snapshots before sending content to the
provider or creating a proposal.

Realtime response creation and audio commit acknowledgements do not echo the client event ID. The
broker serialises each class to one unresolved request and closes the provider connection on timeout,
an invalid or reused acknowledgement while a commit is pending, or correlated ambiguity. It never
guesses that a late event belongs to newer work.

Response and function-call ID registries retain at most 64 unique IDs per browser connection. An
attempt to exceed either bound sends a reconnect-required error and closes both sockets without
eviction. Typed work is not accepted unless response capacity is available. Exact duplicate provider
events remain inert below the bound, and a fresh connection starts with empty registries.

Every browser binary audio frame is validated before provider or mock use. Accepted PCM16 frames are
nonempty, even length, and at most 96,000 bytes. An invalid frame emits a bounded rejection correlated
to the exact active recording and clears that recording without commit, interaction evidence,
assistant response, or dictation result. Real and mock accepted starts both flush browser playback.

Browser transcript and activity presentation is bounded in Unicode code points and entry count,
oldest-first, and remains ephemeral. Malformed broker frames are reported categorically without raw
frame excerpts. Page exit or back-forward-cache suspension aborts capture, closes transport, clears
ephemeral text and device labels, and disposes microphone and audio resources. A restored cached page
opens a fresh protocol-versioned connection and requires explicit microphone re-enablement.

After microphone permission, the browser may show device labels and persist only the selected
device identifier. A stale or ended device falls back visibly to the system default. Echo
cancellation, noise suppression, automatic gain control, a roughly 160 millisecond pre-roll, and
restrained sound cues default on. These settings, recording mode, silence behavior, and distinct
page shortcut key codes are non-content local preferences. Shortcuts are allowlisted, conflict
checked, and ignored while focus is in an input, textarea, select, or editable element. Audio,
transcripts, cleanup output, drafts, previews, and device labels are never stored.

Microphone setup is generation-scoped so stale browser API results cannot replace a newer stream.
Permission revocation requires an explicit user retry. Physical selected-device loss falls back to
the system default only while permission remains granted, and system-default changes are detected
through an ephemeral device fingerprint that is never persisted.

The broker may expose dictation capability metadata using only these fields for each fixed
capability: `id`, `label`, `model_id`, `processing_location`, and `status`. The current speech
capability identifies the fixed English Realtime transcription model. It uses the `OpenAI cloud`
location only for the exact official endpoint, otherwise it is a broker-configured local or remote
endpoint. Cleanup uses the equivalent local or remote classification. Endpoint URLs, credentials,
secret references, request headers, and arbitrary browser-supplied provider or model values are
never included. `configured` does not imply compatibility or that a health check has passed.

## Write confirmation

`send_message` and `create_session` only create proposals. A distinct `confirm_action` call may execute
the current proposal after the user hears its readback and explicitly confirms it. Proposals are
single-use, expire after 120 seconds by default, and replace any earlier proposal.

Browser sessions require the explicit connection-bound Confirm control. The Realtime model does not
receive `confirm_action` and provider-originated attempts are rejected before tool dispatch. The
browser receives a broker-generated presentation handle, not the safety-core proposal token, and
Confirm or Cancel applies only when that handle still matches the displayed readback. The local
console retains its deterministic exact-text confirmation path. Spoken browser confirmation remains
deferred pending review of a strict broker-owned phrase contract.

Paseo permission approvals remain outside the voice tool surface.

## Summarisation

Long replies may be summarised through a configurable OpenAI-compatible endpoint. The default is
`http://127.0.0.1:1234/v1`, which suits a locally running LM Studio instance. If summarisation is
unavailable, the broker reads a cleaned tail of the original reply.

Summarisation runs as one correlated asynchronous request per connection. Browser, provider, host,
and interruption events remain responsive while it runs. A replacement long read uses the bounded
local fallback while the prior request drains, so the broker does not overlap summariser requests
or let a stale result overwrite a newer context.

`replay_summary` repeats the complete current short summary from the beginning. It accepts no
summary, thread, destination, or text argument and performs no Paseo or summariser call. Replay must
be the first tool in its originating response; that response is then locked against later tools, and
the speech follow-up is tool-disabled in both provider configuration and broker enforcement.

## Mock mode

When no OpenAI key resolves for the official endpoint, or `forceMock` is enabled, the broker runs a
text-only mock realtime loop. Credential-free custom endpoints can run live without that key. Mock
mode exercises the same protocol negotiation, audio validation, accepted-start flush, dispatcher,
and confirmation gate as realtime mode.

## Host profiles and session creation

This section records the implemented alpha host-profile design.

Paseo daemon targets are explicit broker-side host profiles, not free-form browser or voice input.
Each profile has a stable ID, display label, daemon target, default working directory, and default
provider/model. Exactly one profile must be the default. The browser receives only safe display
metadata and availability, while daemon targets and the shared alpha-stage Paseo credential remain
in the broker. Per-profile credentials are deferred.

Host selection belongs to one browser connection and resets to the configured default on
reconnect. The selection applies to every Paseo operation. Changing it invalidates the current
session, drafts, pending proposals, and confirmation tokens. An unavailable host or provider/model
fails explicitly without automatic fallback or substitution.

The spoken phrase "new session" begins intent collection and asks for the task. Model instructions
are not evidence that collection occurred. The first model-originated `create_session` call in one
trusted interaction records only that interaction sequence, discards its prompt without parsing or
provider validation, and returns `session_task_required` with the fixed task question. Repeated calls
in that interaction cannot propose. One call after a later trusted user interaction may propose from
only that later prompt. The task turn cannot confirm the proposal, so browser execution requires a
third trusted interaction through the explicit Confirm control.

Host changes, cancellation, connection replacement, and completed proposal attempts clear the
collection state. Trusted deterministic console commands keep their existing immediate behavior.
Trusted broker state supplies the selected profile's host, working directory, and provider/model.
Paths such as `~/` pass unchanged for expansion by the selected daemon. The proposal readback
includes all resolved settings. Alpha omits title collection.

Successful creation requires a validated Paseo `agentId`, which becomes the current session. An
ambiguous result is reported as outcome unknown and reconciled through a session refresh without
guessing the created session.

## Package publishing

The repository is open source under MIT, but the package remains marked `private` to prevent
accidental npm publication while the project is early alpha.

## Privileged Rust backend

The phased migration is complete. One Rust process owns the browser server, audio and OpenAI
Realtime bridge, summarisation, reply provenance, a process-wide content-free summary queue,
proposal and confirmation state, recovery metadata, secret resolution, the Paseo credential, and
the only Paseo write path.
Browser assets remain plain secret-free JavaScript. Node.js is tooling only.

Response proposal and confirmation calls cannot accept a destination thread. Rust derives the
destination from the immutable summary context created when the reply was read. The removed
TypeScript backend remains available only through Git history and cannot be selected at runtime.

Exactly-once delivery requires a receiver-recognised idempotency identifier and an authoritative
delivery receipt. Rust alone does not provide that guarantee. Until the supported Paseo CLI can
accept a stable caller-supplied message identifier, ambiguous delivery outcomes must be reported as
unknown and must not be retried automatically.

See `docs/RUST_CONTROL_PLANE_PLAN.md` for sequencing, acceptance gates, and rollback rules.

## Browser dashboard metadata

Approved on 2026-07-18.

The local same-origin browser may receive ephemeral display metadata needed by the roadmap:
selected-host thread IDs and names, provider/model labels, live state labels, one bounded short
summary for the active reply, queue counts, and the opaque active summary ID. This data is an
untrusted presentation cue. Rust-owned provenance and confirmation remain the routing boundary.

The browser must not persist this metadata, receive raw session objects or logs, receive daemon
targets or credentials, or expose it in a static build. Disconnect and host changes clear the
ephemeral dashboard, typed draft, bound context, and pending confirmation display.

## Automatic completion trigger

Approved on 2026-07-18.

Production automatic completion detection remains deferred until Paseo exposes a supported stable
completion or reply marker. Polling undocumented active-to-idle status transitions and supervising
`paseo logs --follow` are not approved production triggers. Manual reads remain the source of
actionable summary contexts until the stable marker exists.

## Process-wide summary queue

Approved on 2026-07-21.

The broker owns one content-free summary queue for the whole process, chosen over the earlier
connection-scoped queue (D019). It records observed source replies, their deduplication keys,
deterministic ordering, and single-active selection, and holds no summary text, proposal,
confirmation, or interaction state.

A browser connection seeds its safety-core queue from the shared owner at connection start and
stores its snapshot back at graceful disconnect. Seeding settles the queue first, so the
deduplication history survives a reconnect while no active context, draft, proposal, or
confirmation is inherited, preserving the existing reconnect-invalidation boundary. A reply already
handled in a prior connection stays deduplicated and is treated as already read; responding to it
again requires a fresh reply. Because summary text is never persisted, content is not restored
across a reconnect.

The per-connection response checkpoint and rollback are unchanged: each connection keeps its own
local queue during its lifetime, and the live path writes the shared owner back only when the
engine is owned and no response rollback is pending, so a speculative in-flight observation is never
carried across a reconnect. Automatic, process-wide completion detection that would populate this
queue without a manual read remains deferred until Paseo exposes a stable completion or reply
marker.

## Audited timeline scope

Approved on 2026-07-18.

The searchable timeline remains limited to content-free metadata for dispatched operations. It may
show delivered, rejected, outcome-unknown, dispatching, and recovered-invalidated states. Pending,
cancelled, and expired proposals are not durable timeline entries until retention and deletion
controls are approved. No summary, response, transcript, or agent-output content is persisted.

## Block avatar rendering

Approved on 2026-07-19.

The browser avatar is a WebGL block face rendered with the vendored OGL bundle at
`public/vendor/ogl.js`, pinned to version 1.0.11 under the Unlicense with regeneration
instructions in its header. Vendoring keeps the browser free of a build step and of CDN
dependencies; the file is excluded from Prettier and oxlint. Three.js remains the approved
fallback library if OGL proves insufficient.

The face geometry is a 2.5D depth-map grid, not a voxelised mesh. `public/avatar-depth.png` is
the single source of truth with depth in R, a mouth mask in G, an eye mask in B, and the
silhouette in A. The committed texture is produced by the deterministic procedural generator
`scripts/generate-avatar-depth.mjs`. An orthographic depth render of a CC0 MakeHuman head with
the same channel layout may replace it; assets derived from scans of real people are not
acceptable.

The CSS face remains the fallback for missing WebGL, lost contexts, and as the semantic base.
Avatar state stays a helpful cue only; text and ARIA labels remain the accessible surface, and
broker validation remains the security boundary. Reduced motion renders a static formed face.
The `?avatar=demo` and `avatarState` URL parameters drive only the local visual state machine
and must never gain access to broker data.

## Realtime tool execution ownership

Approved on 2026-07-20.

No process-capable operation in the live Realtime path may block the browser and provider transport
loop. Each browser connection retains one tool engine owner and moves it through one
ownership-moving blocking job at a time for initial presentation, model tools, changed-host
selection, and browser confirmation. The engine must not be shared behind a mutex. Initial probing
runs while transport is serviced, and routing-dependent controls fail closed until the presentation
and engine return. Repeated exact hello controls and same-host selections replay cached content-free
host and dashboard frames without moving the engine, mutating checkpoints or recordings, or probing.
Immediate and deferred host changes use the same host job and publish host, dashboard, proposal clear,
then one ready state when quiescent. Completed local job and summary branches precede browser and
provider reads in the biased select, after timeout branches. A live commit acknowledgement received
before a changed-host worker returns may only arm the quiescence-ready latch; host, dashboard, and
proposal publication precede its single ready frame. Join failure closes without retry.

Exact browser confirmation consumes its presentation, commits any response checkpoint, and stores the
non-abortable confirmation job before any fallible socket send or await. The job advances trusted
interaction state, completes journal-before-dispatch authorization, makes one write attempt, records
the terminal classification, and refreshes host and dashboard state. Duplicate clicks cannot schedule
another attempt. The browser retains its disabled in-flight proposal until the job returns, then
receives final proposal state and transcript. Immediately after storing the job, and before any
socket await, the broker retires the originating provider response, queued follow-ups, pending
response creates, function-call publication authority, and its summary response. Provider
cancellation and browser playback flush follow those in-memory retirements. Late media, transcript,
terminal, and tool events from that response are inert. Disconnect may detach the blocking job, which
still finishes journal classification but publishes no result and never retries. Proposal
cancellation requires no process probe and republishes dashboard state when engine ownership is
available. This decision currently isolates only the live Realtime path; the mock runtime remains
synchronous.

Accepted calls execute by unique, nondecreasing provider `output_index`, never by argument arrival.
Arguments are bounded to 65,536 bytes per call and 262,144 retained bytes per connection. The first
matching terminal freezes later response events. A completed terminal drains calls accepted in
contiguous order. A non-completed terminal immediately suppresses running publication and clears
queued work. Before the first accepted model tool, the response captures one ephemeral checkpoint of
all mutable tool-engine and safety-core state, plus the matching browser proposal presentation,
without cloning process adapters, credentials, or journal handles. A completed terminal drops the
checkpoint and commits those mutations. A non-completed terminal restores it before deferred
cancellation or host intent. The exact saved presentation is restored only when its pending token
still matches; otherwise the broker clears the presentation and hidden pending action. Completed arguments behind an earlier
incomplete item are terminal ordering ambiguity, not silently discarded work. Capacity exhaustion
releases registry entries trapped behind an incomplete call and drains dispatchable model and summary
jobs, but suppresses all follow-up responses before closing. Model and summary result publication
stops on the first browser or provider send failure. Blocked and replay-policy function outputs use
the same fail-closed boundary, so a failed rejection send dispatches no remaining tool or follow-up. A
finished summary takes priority over another queued model tool. Session lifecycle events cannot emit
ready until all conversation work and engine ownership have resolved.

Explicit barge-in, proposal cancellation, known host replacement, or response retirement remains
authoritative while the engine is away. A valid recording start performs cancellation, provider
input clear, and browser flush, then receives a correlated rejection without acquiring recording
ownership. Deferred cancellation or host selection is applied before stale output is considered,
and a host intent coalesced back to its original profile still invalidates hidden host-bound state.
Ready dictation cleanup is drained when ownership returns, and neither normal cleanup nor live-empty
deletion can emit ready ahead of a deferred host transition. The host identifier remains pending
until engine ownership can move directly into the host-selection job. Its completion publishes host
state, dashboard state, and any proposal clear before one ready state is released after full
conversation quiescence. Live-empty deletion and response completion share that readiness latch, so
their reverse ordering cannot publish a second ready state. Retired work that mutates pending action state clears any stale browser
proposal before a new turn is accepted. Provider attempts to call `confirm_action` are rejected
inside the isolated job and cannot cross the browser-only confirmation boundary. Exact browser
cancellation clears both live and checkpointed proposal authority, while exact browser confirmation
drops the response checkpoint before dispatch so an executed or outcome-unknown write can never be
re-authorised by provider rollback.

## Deferred work

- Per-profile Paseo credentials
- Editable provider/model and working-directory selectors for new sessions
- Packaging the tool surface as an MCP server
- Phone and PWA polish
- Voice activity detection and wake-word modes
- Automatic process-wide completion detection until Paseo exposes a supported stable completion or
  reply marker
- Durable cancelled and expired proposal history, including retention and deletion controls
