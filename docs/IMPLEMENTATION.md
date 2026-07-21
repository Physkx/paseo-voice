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
pending proposal state. The browser must complete the exact protocol version 2 hello within five
seconds before ordinary controls are accepted or an outbound Realtime connection is allocated.
Provider connection establishment has a separate five second deadline and reports only a bounded
generic failure. Typed turns carry a strictly increasing turn ID and the displayed summary ID or
explicit null context. The browser clears a submitted draft only after the broker returns the
matching acceptance; stale turns receive a content-free correlated rejection. OpenAI Realtime
function-call IDs are single-use. Binary browser frames are PCM16 audio at 24 kHz.
Starting a turn cancels active playback, and releasing push-to-talk commits the input buffer. The
connection starts in `live_response` voice mode and accepts only strict `live_response` or
`dictation` updates while no write awaits confirmation. Dictation configures English input
transcription, commits audio without `response.create`, and returns one bounded ephemeral draft
result. A text-only cleanup adapter calls the configured local or remote model endpoint with bounded
content, a strict editing-only prompt, and a 12 second timeout. It reports a raw-transcript fallback
as degraded and produces no result for empty speech. The browser atomically inserts at a captured
field selection only when the host, immutable summary identifier, field value, and selection still
match.
Stale field state requires explicit review, while stale routing context is discarded. Browser
WebSocket upgrades with an `Origin` header must match the HTTP `Host`; cross-origin browser
connections fail with HTTP 403.

Realtime response, output-item, and function-call events are accepted only when their bounded IDs
match broker-owned response state. Barge-in cancels the exact active response and quarantines
pending or late-created responses. Late audio, transcripts, tool calls, and terminal events from
cancelled or retired responses are ignored. Response and call IDs are single-use for one connection
up to a fail-closed limit of 64 each. A new unique ID beyond either limit sends a bounded
reconnect-required error and terminates both sockets. Response capacity is reserved before a typed
turn is accepted, and reconnecting starts fresh registries. Only a correlated response with terminal
status `completed` may release a queued follow-up response.

Model-originated tool calls run one at a time in provider output order. Each function item must carry
a unique, nondecreasing `output_index`; exact duplicate item events are inert, while reused or
descending indices close the connection as ambiguous. Argument completion cannot overtake an earlier
function item. One argument string may retain at most 65,536 bytes, and incomplete, queued, and
running model calls may retain at most 262,144 argument bytes in aggregate. Either byte limit closes
the connection before the rejected string is copied into a call queue.

The live Realtime connection moves its sole tool engine into ownership-moving blocking jobs for the
initial routing presentation, model tools, changed-host selection, and browser confirmation. Each job
returns the engine, result, and process-refreshed presentation together; the engine is not shared
behind a lock and the transport select loop performs no process-capable refresh. Initial host and
dashboard probing begins after base capability frames while the select loop already services browser
and provider traffic. Routing-dependent controls fail closed until that job returns. Host then
dashboard are published in deterministic order. Repeated exact hello controls and same-host
selections replay cached content-free host and dashboard frames without moving the engine, changing
recording or checkpoint state, or starting another probe.

A known host replacement that arrives during another engine job is applied by a subsequent
ownership-moving host job before stale output can publish. Immediate and deferred host changes use
the same path. A host intent that changes and then coalesces back to the original host still
invalidates hidden provenance, proposal, selected-session, and session-task state. Finished local job
and summary completion branches precede browser and provider reads in the biased transport select,
while transport timeouts retain higher priority. A finished summary also takes priority over queued
model-tool dispatch. A live audio commit acknowledgement that arrives while a changed-host worker
owns the engine only arms the quiescence-ready latch. The completed host transition publishes host,
dashboard, and proposal state before exactly one ready frame. Join failure closes both sockets
without retry.

Exact browser confirmation consumes the presentation handle, commits any response checkpoint, and
stores one non-abortable blocking job before any fallible socket send or await. That job advances the
trusted interaction, completes the journal-before-dispatch confirmation, makes at most one write
attempt, records its terminal classification, and refreshes host and dashboard presentation.
Duplicate clicks cannot schedule a second attempt. The browser retains its disabled in-flight
proposal presentation until terminal completion, when the broker publishes final host, dashboard,
proposal, and transcript state. Immediately after storing the job and before awaiting socket I/O, the
broker retires the originating provider response, pending response creates, queued follow-ups,
function-call publication authority, and its summary response. It then requests provider
cancellation and flushes browser playback. Late audio, transcript, terminal, and tool events from
that response are inert. Disconnect drops the join handle but does not abort that classification
work; no result, retry, or follow-up can reach the closed connection. Proposal cancellation requires
no process probe and republishes dashboard state when engine ownership is available. This process
isolation applies to the live Realtime path only; the mock runtime remains synchronous in this slice.

The first matching `response.done` immediately freezes that response against later audio,
transcripts, items, arguments, and calls. A completed response drains calls accepted in contiguous
output order before finalization. A non-completed response immediately makes running work
non-publishable, clears queued calls and follow-ups, and waits only to recover sole engine ownership.
Before its first accepted model tool, a response captures one ephemeral in-memory checkpoint of all
mutable tool-engine state, including safety-core reply deduplication, routing, proposals, selected
session, dashboard cache, and session-task collection, together with the matching browser proposal
presentation. It does not copy process adapters, credentials, or journal handles and is never logged
or persisted. A completed terminal commits mutations by dropping the checkpoint. A non-completed
terminal restores it whether work is queued, running, or already published, then applies deferred
cancellation or host intent. The broker refreshes its connection view and republishes the exact saved
proposal presentation only while its restored pending token still matches; otherwise it clears both
the presentation and any hidden pending action. Browser cancellation clears the saved presentation
and is reapplied after rollback. An exact browser confirmation commits the checkpoint before dispatch
because an executed or outcome-unknown write cannot be rolled back by a later provider terminal.
Completed arguments behind an earlier incomplete item make terminal ordering ambiguous; queued
argument bytes are released and the connection closes with a bounded error after unavoidable
blocking work returns. Barge-in, proposal cancellation, known host replacement, or another explicit
retirement suppresses stale provider output and cancels any returned proposal before restoring the
engine. If stale work changed or cancelled the pending action, the broker reconciles the browser
proposal handle and sends a clear frame before accepting another turn. Model, summary, and blocked
policy function outputs publish in order and stop on the first browser or provider send failure,
before another tool or follow-up is dispatched. Function-call capacity exhaustion releases registry
entries trapped behind an incomplete call, drains dispatchable model and summary jobs, suppresses
every follow-up response, then sends the bounded reconnect-required error and closes.

The Realtime service does not correlate `response.created` or `input_audio_buffer.committed` with
the client event ID. The broker therefore permits only one unresolved response creation and one
unresolved audio commit at a time. It retains the immutable summary ID captured at the originating
browser turn through every response and tool follow-up. `send_message` fails if that ID no longer
matches the active summary. Duplicate provider fields, event IDs, item IDs, response IDs, and call
IDs fail closed. Missing, invalid, or reused commit acknowledgements while a commit is pending, a
commit timeout, or a correlated error that leaves ordering ambiguous closes the provider connection
without emitting a recording terminal or ready state instead of assigning the event to newer work.
Provider `session.created` and `session.updated` events emit ready only when the engine is present and
all model calls, responses, terminals, summaries, host intents, recordings, and conversation work
have resolved.

All live-response and dictation push-to-talk starts share one strictly increasing,
connection-scoped recording ID sequence and carry the displayed immutable summary ID. Live end and
abort controls repeat that recording ID. Dictation terminal controls use the opaque operation ID
returned for the same recording. Missing, stale, replayed, cross-mode, or mismatched controls cannot
terminate newer capture. Abort clears provider input without committing, advancing interaction
evidence, or creating a response. Every accepted start sends an ordered browser playback flush after
clearing provider input. Realtime audio deltas and browser worklet frames are limited to 96,000
PCM16 bytes; the worklet retains at most 48,000 samples and drops incoming excess. The broker accepts
an inbound audio frame only when it is nonempty, even-length PCM16 data no larger than 96,000 bytes.
An invalid frame clears and rejects the exact active recording without commit, interaction evidence,
assistant response, or dictation result. Mock mode applies the same validation and accepted-start
flush boundary.

While an ownership-moving engine task is active, a structurally valid live-response or dictation
start still retires the active response, clears provider input, and flushes playback, but receives an
exact correlated `recording_rejected` frame. It acquires no recording or dictation operation, so
later audio and terminal controls cannot append or commit anything. Proposal cancellation and known
host replacement also clear browser proposal state immediately and retain only one bounded deferred
intent. Dictation cleanup or deletion that becomes ready while the engine is away remains latched
until the engine returns. A pending host identifier is not consumed until engine ownership is
available and the host-selection job can start. A live-empty deletion acknowledgement that arrives
first therefore retains and drains the deferred host transition after the model job returns, without
emitting ready or restoring stale host state. The completed transition publishes host state, then
dashboard state, then proposal clear when applicable, and finally one ready state only after all
dictation and conversation work has resolved.

Dictation cleanup runs in an abortable task so the WebSocket loop remains responsive. The
connection rejects concurrent recording, transcription, or cleanup work. It assigns the opaque
operation ID when recording starts. Cancellation clears the provider input buffer, aborts cleanup,
ignores expected late transcription events, and emits an idempotent browser cancellation frame.
Host changes terminate recording, pending commit correlation, transcription, and cleanup with the
same operation ID before returning to ready. The browser supports default hold recording and
optional tap-to-toggle with local silence auto-stop, while Escape and the visible Cancel control
restore the captured draft snapshot.

Every provider conversation item committed in dictation mode is deleted before the broker emits a
dictation terminal or returns to ready. Transcription completion starts cleanup and one correlated
item deletion concurrently; either may finish first, and the broker retains only one bounded
ephemeral outcome until both complete and the immutable summary still matches. Cancellation,
transcription failure or timeout, cleanup failure, and context changes delete the item as soon as
its ID is known without waiting for transcription. Controls that could reuse provider conversation
context remain blocked while deletion is unresolved, including after cleanup completes but before
deletion is acknowledged. Non-empty live-response items remain conversational context. A
live-response item whose owned transcription completes empty is deleted, emits no user transcript,
and blocks ready and context-reusing controls until deletion is acknowledged. Deletion and response
finalization share one readiness latch, so either completion order emits exactly one ready state.

Realtime deletion acknowledgement is correlated by the exact broker-owned `item_id`, not by
echoing the client delete event ID. Bounded item and provider-event tombstones make exact late
duplicates inert and conflicting reuse fail closed. A delete error, acknowledgement timeout,
conflicting acknowledgement, outbound delete failure, or provider disconnect closes the connection
without a terminal or ready state.

The broker correlates each committed input buffer and transcription event by provider `item_id`,
and the browser accepts final or partial text only for its active broker operation ID. Cancelled
item IDs cannot be applied to a later recording. Transcription has a 30 second timeout, cleanup has
a 12 second request timeout, and both leave the draft unchanged on timeout. Partial transcription
deltas are bounded and displayed only in the ephemeral preview.

Successful model summaries and degraded cleaned-tail fallbacks are both bounded to 2,400 Unicode
characters before speech. Summarisation makes one attempt, then falls back locally without changing
the active provenance context.

Long-reply summarisation runs in a response-generation and immutable-summary-correlated task rather
than the WebSocket event branch. A stale, cancelled, or replaced response cannot emit its tool
output or overwrite a newer dashboard context. Only one summariser request is retained at a time;
replacement long reads use the bounded degraded fallback until that request drains. Summary-only
dashboard snapshots do not invoke Paseo process probes.

The read-only `replay_summary` tool accepts only an empty object and returns the current bounded
summary plus its opaque ID. It does not read Paseo, run the summariser, mutate interaction evidence,
or change proposal state. Replay is accepted only as the first tool in its response, locks the rest
of that response against tool dispatch, and creates a tool-disabled speech response. Host changes
and successful confirmation remove the active context, after which replay fails safely.

The browser activates the capture worklet after permission and keeps a bounded in-memory pre-roll
of about 160 milliseconds. After permission, it can enumerate microphones, reconnect using the
selected device and browser processing constraints, and visibly fall back to the default device.
It stores only non-content preferences: device identifier, audio-processing switches, cue switch,
recording mode, silence behavior, and conflict-checked page shortcut codes. Device labels, audio,
and all derived text remain ephemeral.

Choosing the system-default microphone removes the saved physical device identifier so reloads
continue to follow the operating-system default. Explicit device choices persist only their
identifier. Missing and ended devices visibly return to the default path.

Microphone acquisition is generation-scoped before `getUserMedia` starts. Late setup completion or
failure can clean up only its own resources. Permission and device enumeration enrich an already
working stream but are not hard setup gates. Permission loss requires explicit Retry; selected or
default device changes recover only when the current permission and ephemeral device fingerprint
support that action. Lost hold-key, page blur, hidden-page, and ended-track paths abort capture once.

The browser bounds provider transcript deltas to 4,000 Unicode code points, each transcript entry
to 32,000 code points, transcript history to 64 entries, activity messages to 240 code points, and
activity history to 128 entries. Oldest entries are removed first and none are persisted. Malformed
broker frames produce a categorical activity entry without including raw frame content. A normal
`pagehide` performs final idempotent teardown. A back-forward-cache `pagehide` aborts capture,
closes the socket, disposes media and audio resources, clears device labels and ephemeral content,
and permits one fresh protocol-versioned connection on the matching `pageshow`; microphone access
then requires a new explicit enable action.

The broker emits one read-only capability frame for the fixed English transcription and configured
cleanup adapters. It contains only `id`, `label`, `model_id`, `processing_location`, and `status`.
Location comes from the validated official, broker-configured local, or broker-configured remote
classification and does not imply compatibility or health. The frame does not expose endpoint
URLs, credentials, secret references, or arbitrary browser-selected models. Additional providers
and prompt persistence remain gated by
`docs/DICTATION_DECISIONS_PENDING.md`.

The browser receives a content-bounded `dashboard_state` presentation frame with safe agent labels,
provider and state labels, one active short summary, broker-owned routing context, and queue counts.
It never receives raw Paseo session rows, logs, daemon targets, credentials, or confirmation tokens
through that frame. Disconnect and host selection changes clear browser drafts and presentation
state. The avatar maps ready, listening, thinking, speaking, awaiting-approval, disconnected,
and error states to text and ARIA labels in addition to visual changes.

The primary avatar is a WebGL block face rendered by `public/avatar-blocks.js` through the
vendored OGL bundle in `public/vendor/ogl.js`. It draws one instanced cube per opaque texel of
`public/avatar-depth.png`, whose channels carry depth (R), a mouth articulation mask (G), an eye
emphasis mask (B), and the head silhouette (A). `scripts/generate-avatar-depth.mjs` regenerates
the texture deterministically; run it again after editing the procedural face. The pure state
machine in `public/avatar-params.js` eases per-state animation weights (assembly, drift, orbit,
rigidity, glitch, shimmer, ripple, mouth) and is covered by `scripts/avatar-params.test.mjs`.
State accents resolve at runtime from the `--avatar-accent-*` CSS custom properties. The original
CSS face stays in the DOM and returns automatically when WebGL is unavailable or the context is
lost, and `prefers-reduced-motion` renders a static formed face whose states change only by
colour and brightness. AnalyserNode taps on the
playback path and microphone source drive the speaking mouth displacement and the listening
ripple; level data stays inside the render loop and never leaves the browser. Appending
`?avatar=demo` cycles every state locally for visual tuning with synthetic audio levels, and
`?avatar=demo&avatarState=<state>` pins one; both operate purely on the local visual state
machine and expose no broker data.

## Configuration and secrets

Rust validates the optional JSON configuration and then applies `PASEO_VOICE_*` environment
overrides. The default file is `~/.config/paseo-voice/config.json`.

After overrides, Rust parses and validates both configured service base URLs before resolving any
secret or starting a listener. Embedded credentials, configured queries, fragments, missing hosts,
malformed URLs, and unsupported schemes fail configuration. Plain `ws://` Realtime and `http://`
summariser or cleanup endpoints are allowed only for exact `localhost`, IPv4 `127.0.0.0/8`, or IPv6
loopback hosts. Non-loopback Realtime requires `wss://`; non-loopback summariser and cleanup require
`https://`.

The exact official Realtime endpoint is `wss://api.openai.com/v1/realtime`, with an optional
trailing slash. Only that endpoint receives `Authorization: Bearer OPENAI_API_KEY`. The production
WebSocket connector performs one direct handshake and does not follow redirects. Custom loopback
and secure remote endpoints receive no OpenAI credential, configured secret reference, or
substitute authentication header and can operate without an OpenAI key. The official endpoint uses
mock mode when its key is unavailable. Custom endpoints are not claimed compatible or healthy. Rust
appends the configured model through parsed URL query APIs only after classification; configured
base queries are rejected.

Production builds one shared `reqwest` client before opening the listener and passes that same
client to dictation cleanup and runtime summarisation. Redirect following and ambient proxy
discovery are disabled, so `HTTP_PROXY`, `HTTPS_PROXY`, `ALL_PROXY`, and their lowercase forms are
not inherited. Cleanup and summarisation therefore treat every 3xx response as one failed model
attempt and degrade locally; they never forward transcript or agent-output request bodies to a
redirect destination. Client construction failure returns a bounded startup error without exposing
dependency details. Custom model endpoints currently support neither authentication nor ambient
proxies.

Capability metadata uses only content-free location classes. The official Realtime endpoint is
`OpenAI cloud`; custom loopback endpoints are `Broker-configured local endpoint`; secure
non-loopback endpoints are `Broker-configured remote endpoint`. Cleanup metadata uses the same local
or remote classification. Endpoint URLs and authentication data are never included.

The selected startup provider is Bitwarden, 1Password, or environment. Secret values are resolved
once, retained only in memory, omitted from logs and arguments, and supplied to Paseo only through
the child environment. Secret-manager programs and Paseo are invoked directly without a shell,
after clearing the inherited environment and supplying only their exact arguments and selected
environment. Standard output and standard error are each capped at 8 MiB, and process diagnostics
contain only status and byte counts rather than command or output content. Each reader probes beyond
the cap without retaining the probe bytes. Any additional byte marks capture uncertain, so an
exit-zero valid receipt in the retained prefix still cannot produce a successful write outcome.

Each successfully spawned command has one monotonic overall deadline for the direct-child wait and
both pipe readers. On Unix, the direct child starts as the leader of an owned process group and both
pipe read ends are made nonblocking. Named readers poll and read bounded chunks against the same
deadline, then close their own descriptors and are joined before return. Unix reader threads are
never detached.

Unix sends `SIGKILL` to the owned group only when no direct-child status has been obtained and a
final `try_wait` reports the unreaped leader still running. Once a status is obtained, the numeric
group ID is never signalled because it may later be reused. Descendants that keep inherited pipes
open after the leader exits are not killed; the readers close at their deadline and the outcome is uncertain. A
descendant that leaves the group is also outside the cleanup claim. After killing an unreaped child,
the executor waits at most 100 milliseconds. A still-unreaped, pipe-free `Child` handle moves to the
named process-wide reaper, which retains no output or credentials. One reaper thread fairly polls
every owned child with `try_wait`, admits at most one queued child per cycle, and uses a bounded idle
wait. A long-running child cannot block a later child from being reaped. If its channel disconnects,
the reaper continues until every handle it already owns is reaped.

On non-Unix platforms, deadline cleanup kills only the direct child and makes no descendant-cleanup
claim. The bounded reader-channel fallback remains explicit there; a reader may detach only after
the deadline and grace, and its retained output remains capped.

A missing OpenAI key selects mock mode for the official endpoint. Credential-free custom endpoints
can still run live; `forceMock` is the unconditional outbound Realtime override. A missing Paseo
password keeps tools unavailable without preventing the browser server from starting.

Paseo daemon choices are strict broker-side `paseoHosts` profiles. Each browser connection starts
on the one configured default profile and receives only its safe ID, label, availability, working
directory, and provider/model defaults. The daemon target and shared alpha credential remain in
Rust. Changing hosts clears the current session and pending confirmation state. There is no
automatic failover.

## Provenance and confirmation

`paseo-safety-core` is pure and has no I/O dependencies. It owns validated identifiers, exact
response bytes, immutable source provenance, deterministic queue order, proposal expiry,
later-interaction confirmation, cancellation, dispatch, and delivery states.

Reading a reply creates the only actionable summary context. `send_message` accepts response text
only and cannot accept a session or destination. Confirmation accepts only the proposal handle.
The destination supplied to the Paseo adapter is derived from the source thread stored in the
summary context. Selecting or reading another context invalidates the previous draft and proposal.
Rereading the same host, thread, and exact output is content-free and non-destructive: it does not
announce the reply again or invalidate the active context or proposal. The host-scoped digest is an
interim identity until Paseo exposes a supported stable reply marker.

The Realtime model cannot call `confirm_action`. Browser proposal frames carry a connection-scoped
presentation handle distinct from the hidden safety token. Confirm and Cancel controls echo that
handle, and stale, missing, replaced, or replayed handles execute nothing. Typed and push-to-talk
turns are blocked while a proposal is pending. The deterministic local console remains a separate
trusted adapter with exact-text confirmation.

`create_session` accepts only a task prompt. For model-originated calls, the first call records the
current trusted interaction and returns `session_task_required` with a fixed spoken question. Rust
does not parse or retain that call's prompt, validate a provider, invoke Paseo, create a proposal,
write the journal, or mutate routing context. Repeated calls in that interaction remain inert. One
call after a later trusted user interaction may propose using only its prompt, and clears collection
state before validation. A host change, cancellation, connection replacement, or completed proposal
attempt also prevents collection state from crossing ownership boundaries. Trusted deterministic
console calls retain their immediate proposal behavior.

Rust resolves host, working directory, and provider/model from the selected profile, validates the
provider/model through Paseo, and applies the same later-interaction, expiry, single-use,
exact-argument, and journal-before-dispatch rules. The task interaction cannot confirm its own
proposal. Browser execution therefore requires a third trusted interaction through the explicit
Confirm control. Successful creation requires a validated `agentId`, which becomes current.
Ambiguous output is `outcome_unknown`, is never retried, and selects nothing. Paseo permission
requests can be narrated but never approved by voice.

## Delivery and recovery

The Paseo adapter reports `delivered` only when a spawned send exits zero and its JSON contains a
validated `messageId` receiver receipt. It reports `rejected` only when the process fails to spawn
before a child exists. Once a send process is spawned, timeout, signal or missing exit status,
nonzero exit, structured or plain CLI errors, malformed output, and missing or invalid receipts are
`outcome_unknown` and are never retried automatically. A pipe-drain deadline, reader failure, or
output truncation follows the same conservative outcome. Detached runs follow the same boundary and
report creation only after exit zero with a validated `agentId`.

Every read-only Paseo parser uses the same certain-success gate: no spawn failure, no deadline or
capture uncertainty, and exit code zero. Uncertain session JSON cannot select a session, uncertain
logs cannot create reply provenance, uncertain provider output cannot enable a run proposal, and
uncertain permission output cannot be narrated.

Before a child write starts, Rust appends a `dispatching` metadata transition. The journal stores
only opaque operation, summary, source or destination identifiers, SHA-256 digests, timestamps,
states, and optional receiver IDs. It has no transcript, summary text, response body, prompt,
credential, or agent-output column. The journal file is mode 0600 and its directory mode 0700 on
Unix. Restart recovery maps `dispatching` to `outcome_unknown` and invalidates `pending`; it
never constructs a fresh send. The read-only `get_operation_status` tool queries the journal by
opaque operation ID. Retention is bounded to the latest 10,000 metadata transitions.

The read-only `list_operation_timeline` tool returns the latest durable state for recent dispatched
operations. It supports conjunctive exact matching by state, summary ID, and destination thread ID,
plus at most 100 newest-first entries per page. Its opaque cursor binds the filters, snapshot
sequence, page boundary, and retention floor. Later transitions do not mutate an existing traversal,
and a cursor fails explicitly if retention has removed part of its snapshot. Results contain only
content-free identifiers, monotonic timestamps, states, and optional receiver IDs. Cancelled and
expired proposals are not yet journalled and are not claimed as timeline entries.

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

This runs Prettier, rustfmt, agent-document lint, browser JavaScript lint, the browser JavaScript
test suite, Clippy, all Cargo tests, and the release build. Browser-module tests cover the
push-to-talk client wiring, interaction gate, turn context, playback framing, shortcut and
microphone configuration, and microphone lifecycle. Rust tests cover the safety state machine,
property-generated confirmation replay, concurrency, strict protocol framing, secret providers,
process failure classification, journal recovery, mock browser runtime, and an end-to-end fake
Realtime WebSocket. Realtime
integration coverage includes nonblocking initial presentation, deterministic host-transition
publication, same-host recording preservation, confirmation exactly-once behavior, and terminal
journal classification after browser disconnect.
