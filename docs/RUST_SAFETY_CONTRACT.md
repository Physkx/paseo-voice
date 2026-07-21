# Rust safety contract

This is the normative contract for `paseo-safety-core` and the privileged Rust broker. Changes to a
rule require matching tests and an update to `DECISIONS.md` when the architecture changes.

## Trust model

The browser, model, transcription, tool arguments, session titles, display text, network frames,
CLI output, and wall clock are untrusted. Broker-observed source provenance is authoritative. Rust
owns and resolves the Paseo credential, discloses it only to the trusted Paseo child environment,
and provides the only write path.

The contract covers bugs, stale and duplicated events, malformed input, concurrent clients,
provider ambiguity, process failure, and a model that ignores instructions. It does not protect
against a compromised local account or malicious Paseo binary.

## Canonical values

- Opaque identifiers are 1 through 128 UTF-8 bytes, with no NUL, ASCII control characters, or
  leading or trailing whitespace.
- A response body is 1 through 65,536 UTF-8 bytes, contains non-whitespace text, rejects NUL, and
  otherwise preserves its exact bytes.
- Proposal lifetime defaults to 120 seconds and uses an injected monotonic clock. A proposal is
  expired at `now >= expires_at`.
- Display titles never participate in identity or routing.

## Provenance

Every actionable reply context contains an immutable summary ID, source thread ID, source reply ID,
and observation time. Its source thread is the only possible response destination.

A proposal accepts a summary-context handle and response body. It cannot accept a destination,
thread title, current-session value, or replacement source ID. Confirmation accepts a proposal
handle and trusted interaction evidence only. It cannot replace text or routing data.

Every accepted browser turn carries the summary ID displayed when it started, or explicit null
context. Typed turn IDs and recording IDs increase strictly within one protocol-versioned
connection. Rust retains the captured context through provider responses and tool follow-ups.
Missing, stale, replayed, cross-mode, or mismatched controls cannot produce provider content, a
proposal, confirmation, or write.

Changing host or active summary cannot retarget a proposal. A response created for summary A can
never be dispatched to the source thread of summary B.

## Confirmation

Each trusted user interaction receives an increasing sequence. A proposal records the sequence that
created it and can be confirmed only by a later trusted sequence.

The Realtime model has no confirmation tool. Silence, speech, model claims, same-turn calls, and
caller-supplied sequence values are not confirmation. Browser confirmation requires the exact
current broker-generated presentation handle. Paseo permissions cannot be approved by voice.

Proposals are single-use. Replacement, cancellation, expiry, context consumption, or successful
confirmation makes the prior proposal terminal.

## Queue

- Replies are deduplicated by source thread and source reply identity.
- At most one summary context is active per browser connection.
- Queue order is deterministic by observation sequence and then summary ID.
- Replayed or delayed observations cannot change assigned order.
- Terminal contexts cannot re-enter the queue.
- A connection may return committed content-free deduplication state to the broker for a later
  sequential connection. Concurrent connection snapshots are not one authoritative queue and are
  not merged.
- Shared queue state contains no summary text or write authority and is not durable.

## Provider correlation

Responses, items, function calls, typed turns, recordings, and dictation operations use bounded,
single-use identifiers. Exact duplicates may be ignored; conflicting reuse or ambiguous ownership
fails closed. The broker never assigns a late provider event to newer work by timing alone.

Barge-in or explicit retirement stops publication authority before transport cancellation. Late
audio, transcripts, tools, and completion events from retired responses are inert.

Dictation results are valid only for the host, summary context, field, value, and selection captured
at recording start. A stale host or summary discards the result. Provider dictation items must be
deleted before conversation context can be reused.

## Dispatch and delivery

Confirmation moves a proposal to `dispatching` before external I/O. The exact stored destination
and response bytes are the only values supplied to the write adapter. One confirmation starts at
most one process attempt.

Delivery states are:

- `delivered`: the child exited zero and a valid receiver acknowledgement was parsed.
- `rejected`: no child process existed.
- `outcome_unknown`: a child existed but authoritative acknowledgement was not obtained.

Timeout, missing status, signal, nonzero exit, malformed output, missing receipt, reader failure, or
output truncation after spawn is `outcome_unknown`. It is never automatically retried or relabelled
as rejection. Exactly-once delivery may be claimed only after Paseo accepts a stable caller-supplied
idempotency ID and provides an authoritative receipt for it.

Paseo and secret-manager commands are invoked directly without a shell. Secrets cannot enter logs.
Paseo credentials are supplied through a controlled child environment, not command arguments.
Process output and execution time are bounded; uncertainty cannot be parsed into a successful read
or write.

## Persistence and recovery

Runtime-generated durable state may contain opaque identifiers, response digests, timestamps, state
transitions, adapter result categories, and receiver IDs only. It cannot contain audio, transcripts,
summaries, response bodies, session prompts, agent output, or credentials. Operator-managed
configuration may contain host targets and secret references, but never secret values.

Before dispatch, Rust records a content-free `dispatching` transition. On restart, unfinished
dispatches become `outcome_unknown` and pending proposals are invalidated. Recovery cannot construct
a response body or start a write.

## Logging

Logs cannot contain credentials, secret references, response bodies, transcript content, raw agent
output, full process output, confirmation tokens, or private infrastructure details. Errors use
bounded categories, state, byte counts, and opaque operation IDs.
