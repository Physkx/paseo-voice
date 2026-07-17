# Rust safety contract

## Status

Version 1. This contract freezes the behavior required for the Rust backend replacement. It is the
test surface for `paseo-safety-core` and the final Rust broker. Changes require matching tests and an
entry in `RUST_DECISIONS_PENDING.md`.

## Trust model

The browser, voice model, transcription, tool arguments, session titles, spoken echoes, network
frames, CLI output, and wall clock are untrusted inputs. Durable source thread and source reply IDs
observed by the broker are authoritative. Only the Rust backend may hold the Paseo write credential
or execute a Paseo write.

The initial threat model protects against implementation bugs, stale and duplicated events,
malformed input, concurrent clients, process crashes, and a model that ignores its instructions. It
does not claim to protect against a compromised local operating-system account or a malicious Paseo
binary.

## Canonical values

### Identifiers

- IDs are opaque UTF-8 strings from 1 through 128 bytes.
- IDs cannot contain NUL, ASCII control characters, leading whitespace, or trailing whitespace.
- IDs are compared byte-for-byte and are never matched by title, case folding, or prefix inside the
  safety core.
- Display titles are presentation data and never participate in routing decisions.

### Response body

- A response body is from 1 through 65,536 UTF-8 bytes.
- A body must contain at least one non-whitespace Unicode scalar value.
- NUL is rejected.
- All other bytes, including leading whitespace, trailing whitespace, and line endings, are
  preserved exactly from proposal through dispatch.
- A SHA-256 digest of the exact bytes identifies the body in metadata. The digest is not a delivery
  receipt.

### Time

- Proposal lifetime defaults to 120 seconds.
- Safety transitions use an injected monotonic millisecond value, not wall-clock time.
- A proposal is expired when `now >= expires_at`.
- Moving the wall clock cannot revive or prematurely expire a proposal.

## Provenance invariant

Every actionable summary context contains an immutable summary ID, source thread ID, source reply
ID, and observation time. The destination thread is the source thread stored in that context.

A proposal accepts a summary-context handle and response body. It does not accept a destination,
thread title, current-session value, or replacement source identifier. Confirmation accepts a
proposal handle and trusted interaction evidence only. It does not accept response text or routing
data.

Changing browser selection, model current-session state, visible labels, or the active queue item
cannot retarget an existing proposal. A response originating from summary A can never be proposed,
confirmed, dispatched, or reported delivered to the source thread of summary B.

## Interaction and confirmation invariant

Each broker-observed user interaction receives a strictly increasing interaction sequence. A
proposal records the sequence that created it. Confirmation is valid only when its trusted sequence
is greater than the proposal sequence.

Voice confirmation requires a later broker-observed user turn. Same-turn tool calls, replayed model
calls, silence, model-generated confirmation claims, and caller-supplied sequence values are not
trusted confirmation evidence. An authenticated browser confirmation event may be supported as the
strongest confirmation mode.

Every proposal is single-use. A new proposal for the active context replaces the previous pending
proposal. Cancellation, expiry, context consumption, or successful confirmation makes the proposal
terminal.

## Summary queue invariant

- Completion events are deduplicated by source thread ID plus source reply ID.
- At most one summary context is active for response at a time.
- Queue order is deterministic by broker observation sequence, then summary ID.
- Replayed, delayed, or out-of-order completion events do not change an already assigned order.
- Context state changes are explicit and terminal states cannot re-enter the queue.

## Dispatch and delivery invariant

Confirmation moves a proposal to `dispatching` before external I/O. The exact stored destination ID
and response bytes are the only values supplied to the Paseo write adapter.

Delivery states are:

- `delivered`: an authoritative receiver acknowledgement was validated.
- `rejected`: the receiver authoritatively rejected the request before accepting it.
- `outcome_unknown`: dispatch may have reached the receiver but authoritative acknowledgement was
  not obtained.

`outcome_unknown` is never reported as delivered or rejected and is never retried automatically.
Exactly-once delivery may be claimed only when Paseo accepts a stable caller-supplied idempotency ID
and returns or exposes an authoritative receipt for that same ID.

## Replay invariant

Every external command carries a request ID. Repeating a completed request ID with identical bytes
returns the original result. Reusing a request ID with different bytes is rejected. Concurrent
requests for the same proposal serialize through one state owner and cannot create multiple
dispatches.

## Persistence and recovery invariant

Until a separate retention policy is approved, durable state may contain identifiers, response
digests, timestamps, state transitions, adapter result categories, and receiver message IDs only.
It cannot contain transcripts, summaries, spoken echoes, response bodies, agent output, credentials,
or secret references.

Because response bodies are not durable, a restart cannot resume an unacknowledged body dispatch.
Recovered dispatching operations become `outcome_unknown`. Pending proposals without their body are
invalidated. Recovery never constructs a fresh send.

## Logging invariant

Logs cannot contain credentials, secret references, response bodies, transcript content, raw agent
output, full CLI stdout or stderr, confirmation tokens, or local infrastructure details. Errors use
bounded categories and opaque operation IDs.

## Compatibility invariant

The final browser wire behavior preserves push-to-talk audio, text turns, transcripts, proposal
display, state updates, playback flushing, and mock development mode. During migration, differential
tests compare Rust outcomes with characterized TypeScript behavior. After cutover, no production
TypeScript backend write path or credential owner remains.
