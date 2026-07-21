# Rust control-plane implementation plan

## Status

Phases 0 through 8 are implemented. Production commands start one Rust backend. The TypeScript
backend and its dependencies are removed. Browser JavaScript remains secret-free, and no
deployment, DNS, binding, or external secret change was made.

## Problem statement

Paseo Voice must guarantee that a response created for one agent reply cannot be proposed,
confirmed, or delivered to another agent thread. Before this migration, the TypeScript broker
froze a resolved session ID inside a proposal and prevented a proposal token from invoking the CLI
twice in one process lifetime. It did not bind the response to the reply that produced its summary,
enforce that confirmation happened in a later user interaction, recover delivery state after a
crash, or distinguish a confirmed failure from an ambiguous delivery outcome.

The goal is not a performance rewrite. The goal is a deep safety module whose small interface makes
wrong-thread delivery, destination substitution, invalid state transitions, and unsafe retries
difficult or impossible for callers to express.

## Target architecture

```text
Secret-free browser
  |
  v
Privileged Rust backend
  - browser WebSocket and audio
  - OpenAI Realtime and local summarisation
  - reply observation and immutable provenance
  - ordered summary queue
  - proposal and confirmation state machine
  - delivery journal and recovery policy
  - Paseo credential and write adapter
  |
  v
Supported Paseo CLI
```

The Rust process is the only production backend and the only process allowed to execute Paseo
writes. Browser labels, model tool arguments, session titles, response text, and spoken echoes are
untrusted inputs and can never select or replace a destination.

## Safety contract

The implementation must preserve these invariants through its public interface:

1. Every actionable summary has an immutable summary ID, source thread ID, source reply ID, and
   creation time.
2. A response proposal refers to a summary-context handle and response body. It does not accept a
   thread ID, thread title, or destination field.
3. The control plane derives and stores the destination from the immutable summary context.
4. Confirmation refers only to an existing proposal and a later trusted interaction. It cannot
   supply replacement text, source context, or destination data.
5. The exact response bytes are captured once. The spoken echo is presentation only and is never
   parsed to reconstruct the response.
6. One summary context can produce at most one delivered response unless an explicit future product
   decision introduces a controlled reopen operation.
7. Duplicate, stale, expired, cancelled, consumed, cross-thread, and concurrently replayed commands
   fail closed.
8. An ambiguous result after dispatch is `outcome_unknown`. It is not labelled failed or retried
   automatically.
9. After credential cutover, TypeScript cannot bypass the control plane to invoke a Paseo write.
10. Durable storage contains no transcript, summary, response body, or agent output until retention,
    deletion, access-control, and redaction policies are approved.

## Planned Rust workspace

The initial scaffolding phase should introduce this shape:

```text
Cargo.toml
rust-toolchain.toml
rustfmt.toml
clippy.toml
crates/
  paseo-safety-core/
    Cargo.toml
    src/lib.rs
    tests/
  paseo-control-plane/
    Cargo.toml
    src/main.rs
    src/lib.rs
    tests/
```

`paseo-safety-core` is a pure domain module. It has no sockets, filesystem access, process spawning,
clock reads, random-number generation, or secret resolution. Those values enter through explicit
inputs so state transitions are deterministic and property-testable.

`paseo-control-plane` owns adapters for local IPC, clock, randomness, metadata persistence, secret
resolution, and Paseo process execution. Adapter interfaces remain internal to this executable
unless two production implementations genuinely require an external seam.

The repository remains a pnpm workspace for consistent developer commands. Cargo now performs all
backend build, lint, and test work; Node.js remains only for browser and documentation tooling.

## Proposed control-plane interface

The exact encoding is decided during the contract phase. The semantic operations are:

- Register or observe a completed reply and return its immutable summary context.
- Claim the next ready summary context.
- Propose a response using only a summary-context handle and response body.
- Confirm a proposal using only its handle and trusted interaction evidence.
- Cancel a proposal.
- Query proposal and delivery status by opaque operation ID.
- Report health, protocol version, and supported capabilities.

All requests and responses carry a protocol version, request ID, and bounded payload size. Duplicate
request IDs return the original result or a deterministic duplicate response. Protocol decoding
rejects unknown operation kinds, unknown enum variants, duplicate object fields, oversized input,
invalid identifiers, and trailing data.

## State model

Summary contexts progress through explicit states:

```text
observed -> queued -> summarising -> ready -> active -> consumed
                           |           |        |
                           v           v        v
                         failed      deferred  expired
```

Response proposals progress through explicit states:

```text
pending -> dispatching -> delivered
   |           |             |
   |           +-----------> outcome_unknown
   v
cancelled or expired
```

There is no direct transition from `pending` to `delivered`, from `outcome_unknown` to `pending`, or
from any terminal state back to `dispatching`. Retry, if later supported, creates a separately
auditable attempt using the same receiver-recognised idempotency identifier.

## Phased implementation

### Phase 0: Freeze the safety contract

Goal: turn the roadmap statements into executable behavioral expectations before adding Rust.

Commits:

1. Add TypeScript characterization tests for current proposal replacement, expiration, single use,
   and immutable destination after proposal creation.
2. Add executable characterization tests where the behavior already exists, plus a versioned
   contract-case catalog for unimplemented guarantees: summary A never targeting thread B,
   same-interaction confirmation rejection, duplicate Realtime call IDs, and ambiguous CLI
   completion. Promote each case into the executable suite with the phase that implements it.
3. Define canonical identifier formats, maximum response size, exact whitespace behavior, Unicode
   handling, NUL rejection, expiration semantics, and clock-skew assumptions.
4. Define the trusted interaction evidence used by confirmation. Prefer a browser confirmation event
   for the strongest mode and require a later broker-observed user turn for voice mode.
5. Record the metadata retention policy needed for operation IDs, hashes, timestamps, and delivery
   states. Keep content persistence disabled.

Exit gate:

- The safety contract has named tests and no unresolved destination or confirmation semantics.
- Current passing behavior remains green.
- New unimplemented guarantees are explicit contract cases, not failing or silently skipped tests.

Rollback: documentation and specification-only commits can be reverted independently without
changing runtime behavior.

### Phase 1: Add inert Rust scaffolding

Status: implemented. The exit gate must remain green in every later phase.

Goal: establish a reproducible Rust workspace without changing application behavior.

Commits:

1. Pin the stable Rust toolchain and required formatter and lint components.
2. Add the Cargo workspace and the two empty crates described above.
3. Configure workspace-wide dependency, lint, formatting, and release-profile policy. Forbid unsafe
   code in both crates.
4. Add a minimal control-plane executable that supports only `--version` and exits without opening a
   socket, reading secrets, or invoking Paseo.
5. Add `pnpm` scripts for Rust formatting, linting, tests, and build, then include them in `pnpm check`.
6. Update contributor documentation with the pinned Rust prerequisite and focused verification
   commands.
7. Add CI coverage for the pinned Node.js and Rust toolchains without publishing binaries.

Exit gate:

- A clean checkout can run the complete Node.js and Rust checks deterministically.
- The production Node.js entry point behaves exactly as before.
- The Rust binary has no credential, network, filesystem, or Paseo capability.

Rollback: remove the Cargo workspace and Rust check scripts. No runtime path references Rust yet.

### Phase 2: Implement the pure safety core

Goal: model identifiers, summary contexts, proposals, interaction evidence, and state transitions in
safe Rust with no I/O.

Commits:

1. Add validated opaque identifier newtypes and bounded response-body construction.
2. Add summary-context values and their legal state transitions.
3. Add proposal values that derive destination only from a summary context.
4. Add confirmation rules for expiration, later-interaction evidence, single use, cancellation, and
   consumed contexts.
5. Add dispatch and delivery outcomes, including the distinct `outcome_unknown` terminal state.
6. Add deterministic state-machine tests through the public interface.
7. Add property tests generating arbitrary command sequences and asserting all safety invariants.
8. Add concurrency model tests for duplicate confirmation and competing proposals.

Exit gate:

- Cross-thread destination substitution cannot be represented by the public interface.
- Property tests cover generated command sequences and shrink failures to reproducible cases.
- The crate contains no unsafe code or I/O dependencies.

Rollback: the crate remains unused by production and can be removed without changing Node.js.

### Phase 3: Define and implement local IPC

Goal: expose the safety-core behavior to TypeScript while Rust remains unprivileged and has no write
authority.

Commits:

1. Choose framed stdin/stdout for initial portability or a Unix domain socket for multi-client
   operation, and record the decision with authentication and permission assumptions.
2. Define versioned request and response envelopes with request IDs and size limits.
3. Implement strict Rust decoding and structured error responses.
4. Implement a TypeScript client adapter with injected transport and timeout dependencies.
5. Add contract fixtures consumed by both Vitest and Rust tests.
6. Add malformed-frame, truncation, duplication, reordering, timeout, child-exit, and version-mismatch
   tests.
7. Add graceful startup and shutdown supervision without automatic write retries.

Exit gate:

- Both languages pass the same protocol contract suite.
- A malformed or crashed sidecar cannot cause a Paseo write.
- No secrets or reply content are logged by either side.

Rollback: disable sidecar startup and continue using the existing TypeScript implementation.

### Phase 4: Run the Rust safety core in shadow mode

Status: completed as an automated shadow gate under D014. Shared protocol fixtures, characterized
TypeScript gate behavior, replay tests, malformed transport tests, duplicate Realtime call-ID
tests, concurrency tests, and two-thread provenance tests form the reproducible comparison suite.
The production write path was not changed.

Goal: compare Rust decisions with the existing TypeScript gate under real application event ordering
without letting Rust execute writes.

Commits:

1. Mirror proposal, cancel, expiration, and confirmation events to Rust after TypeScript handles them.
2. Compare normalized decisions and emit redacted mismatch telemetry locally.
3. Add replayable sanitized traces for reconnect, duplicate tool-call, barge-in, and concurrent-summary
   scenarios.
4. Run fault injection for Rust startup failure, IPC loss, delayed response, and process crash.
5. Define the mismatch-free observation period and evidence required before authority moves.

Exit gate:

- Rust and TypeScript agree for all contract tests and approved sanitized traces.
- Shadow-mode failure has no effect on the current write path.
- Every mismatch is understood and resolved rather than ignored.

Rollback: remove event mirroring. Existing runtime behavior remains unchanged.

### Phase 5: Move provenance and queue authority to Rust

Status: Rust authority is implemented in the safety core and strict protocol. Source thread and
reply provenance are immutable, completion pairs are deduplicated, ordering is assigned at broker
observation, one context is active, and proposal and confirmation messages cannot contain a
destination. Final browser presentation wiring occurs in the single-process Rust cutover, avoiding
an intermediate production authority split that would immediately be removed. This replacement
keeps the queue scoped to one browser connection, matching the previous one-session-per-client
runtime. A process-wide automatic completion observer remains separate roadmap work under D019.

Goal: make Rust authoritative for reply observation, immutable summary contexts, the ordered queue,
and response proposal construction while TypeScript remains authoritative for actual Paseo writes.

Commits:

1. Add the reply-observation adapter using stable source thread and source reply identifiers.
2. Implement deterministic deduplication and queue ordering.
3. Bind each visible response context to an opaque Rust-owned summary handle.
4. Reject stale, missing, consumed, and cross-thread context operations.
5. Drive browser destination labels and proposal echoes from Rust-owned presentation data.
6. Add end-to-end two-thread tests that attempt retargeting through browser, model, reconnect, and
   concurrent-event paths.

Exit gate:

- Automated tests prove summary A cannot produce a proposal for thread B.
- The model and browser no longer supply a destination for summary-bound responses.
- Queue and context recovery behavior is specified for process restarts.

Rollback: restore TypeScript provenance ownership while leaving the unused Rust implementation in
place for diagnosis.

### Phase 6: Move Paseo write authority and credentials to Rust

Status: the Rust-only direct process adapter, credential environment isolation, bounded output,
timeout classification, receipt validation, and failure-injection tests are implemented. Successful
exit without a validated receiver message ID is `outcome_unknown`. Production authority moves only
with the Phase 8 entry-point cutover, so no dual write mode is introduced.

The live read-only smoke passed on 2026-07-18 against the existing private local Paseo 0.1.107
daemon. The release Rust console resolved its credential through Bitwarden, listed sessions through
the injected direct-process adapter, and disclosed neither session data nor secret values in the
test output. No live write was attempted. Automated failure and write-path coverage uses fakes.

Goal: make the Rust process the only production write path.

Commits:

1. Implement the Paseo process adapter using direct argument passing without a shell.
2. Parse and validate the successful CLI response instead of relying only on exit status.
3. Move Paseo secret resolution and in-memory credential ownership into Rust.
4. Execute confirmed proposals from the exact immutable response body stored by the safety core.
5. Remove the Paseo password and write-capable client methods from the TypeScript production wiring.
6. Add a startup assertion that refuses production mode if both TypeScript and Rust write paths are
   enabled.
7. Add failure-injection tests before spawn, after spawn, before acknowledgement, after
   acknowledgement, and during shutdown.
8. Perform the controlled cutover with an explicit rollback build that restores the previous single
   TypeScript path. Never operate both paths concurrently.

Exit gate:

- TypeScript cannot invoke `send_message` or `create_session` directly and does not possess the Paseo
  credential.
- Exactly one production write adapter exists.
- CLI output is schema-validated and ambiguous completion is reported as `outcome_unknown`.
- Full repository checks and live private-host smoke tests pass.

Rollback: deploy the last validated TypeScript-only build. Do not add a runtime switch that permits
both write adapters in one build.

### Phase 7: Add receiver-recognised idempotency and recovery metadata

Status: a content-free `SQLite` transition journal and conservative recovery are implemented.
Recovered `dispatching` operations become `outcome_unknown`; recovered `pending` operations are
invalidated, and neither produces a send. Paseo 0.1.107 has no caller-supplied message-ID option, so
automatic retry remains disabled and exactly-once delivery is not claimed.

Goal: make retry and crash recovery safe without overstating exactly-once guarantees.

Commits:

1. Obtain a supported Paseo CLI option for a caller-supplied message ID and an authoritative receipt,
   or keep automatic retry disabled.
2. Add an append-only metadata journal for operation ID, summary ID, source and destination IDs,
   response hash, timestamps, state transitions, attempt ID, and receiver message ID.
3. Add startup recovery that reconstructs non-content state and preserves `outcome_unknown`.
4. Add deterministic retry only when the same receiver-recognised idempotency ID is reused.
5. Add crash tests at every journal and process boundary.
6. Add retention and deletion enforcement for metadata. Persist content only after a separate policy
   approval and security review.

Exit gate:

- Replaying the same operation cannot create a second accepted message when Paseo supports the
  stable identifier.
- Recovery never turns an unknown outcome into an automatic fresh send.
- Journal inspection demonstrates that prohibited content is absent.

Rollback: disable recovery-driven retries and retain fail-closed status reporting.

### Phase 8: Decide whether to complete a Rust backend migration

Status: completed. Reliability and maintenance locality improved because the browser lifecycle,
Realtime call deduplication, provenance, confirmation, credential, journal, and write adapter now
share one Rust owner. Protocol churn is limited to the external browser and Realtime wires; the
stdio protocol remains a tested diagnostic harness rather than a production process boundary.
Release builds produce one binary, structured browser errors remain bounded, and rollback is the
last validated TypeScript commit in Git history rather than a dual-authority runtime switch.

Goal: evaluate moving browser WebSocket, OpenAI Realtime, summarisation, and read-only Paseo adapters
after the safety control plane has proven stable.

This is a new decision, not an automatic continuation. A full migration is justified only if it
reduces interface complexity, duplicated lifecycle handling, or operational failure modes. Browser
assets remain JavaScript regardless.

Implemented comparison:

| Factor               | Rust backend selected                                                                | Permanent TypeScript adapter rejected                                           |
| -------------------- | ------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------- |
| Reliability          | One state owner covers Realtime calls, provenance, confirmation, journal, and writes | Cross-process failure and duplicated authority remain                           |
| Maintenance locality | Safety transitions and all privileged adapters share one workspace and type system   | Safety changes require coordinated TypeScript and Rust protocol updates         |
| Protocol churn       | Only browser, Realtime, summariser, and Paseo external wires remain                  | A permanent private IPC protocol becomes another compatibility surface          |
| Binary packaging     | One release binary plus static browser assets                                        | Node runtime, installed packages, Rust child, and supervision remain            |
| Observability        | Bounded browser errors and one content-free SQLite transition journal                | Events and failures are split across two processes                              |
| Rollback             | Rebuild the last validated TypeScript-only Git revision                              | A runtime authority switch risks two write paths and is therefore not permitted |

The Rust option wins on the primary safety objective because the only public Paseo write methods
require opaque authorizations produced by the confirmation gate. The tradeoff is a larger Rust
dependency graph and direct ownership of evolving Realtime events. D010 and D016 record those
choices for final review.

Exit gate:

- A written comparison covers reliability, maintenance locality, protocol churn, binary packaging,
  observability, and rollback.
- Any migration retains dependency injection and interface-level tests.

## Testing decisions

Tests target observable behavior through the safety-core and control-plane interfaces. They do not
assert private enum layout, internal collections, serialization implementation, or adapter call
ordering unless ordering is itself part of the safety contract.

Required suites:

- Example-based transition tests for every legal and illegal state change.
- Property tests for arbitrary sequences, expiration boundaries, and identifier reuse.
- Concurrency tests for competing proposals, confirmations, cancellation, and replay.
- Retained IPC contract fixtures and Rust replay tests from the migration boundary.
- End-to-end cross-thread tests with at least two simultaneous agent replies.
- Fault-injection tests for process timeout, exit, malformed output, disconnect, and restart.
- Unicode, multiline, whitespace, maximum-size, and NUL input tests.
- Metadata-retention tests proving response and transcript content are not persisted.

Rust tests use injected process boundaries, fake Realtime and browser WebSockets, temporary
journals, property generation, and public safety-core commands for observable assertions.

## Toolchain and dependency policy

- Pin a stable Rust toolchain and commit the toolchain file.
- Deny warnings in CI after the initial scaffold is warning-free.
- Require rustfmt, Clippy, unit tests, property tests, and release build in `pnpm check`.
- Forbid unsafe code in workspace crates. Any future exception requires a separate architecture and
  security decision.
- Keep the dependency set small and justify crates that handle IPC, persistence, cryptography, or
  process execution.
- Commit `Cargo.lock` because the control plane is an application.
- Do not publish crates or binaries as part of scaffolding.
- Do not add network listeners. Local IPC must default to the narrowest practical permissions.

## Out of scope for initial scaffolding

- Moving browser code, audio worklets, or the OpenAI Realtime connection to Rust.
- Public broker exposure, deployment, DNS, Cloudflare, bindings, or secrets changes.
- Voice approval for Paseo permission requests.
- Persisting transcripts, summaries, response bodies, or agent output.
- Claiming exactly-once delivery before Paseo accepts stable caller-supplied message IDs.
- Publishing npm packages, Cargo crates, or release binaries.
- Removing the current TypeScript gate before shadow-mode and cutover exit gates pass.

## First implementation slice

The first coding task should implement Phase 1 only. It should add inert workspace scaffolding,
integrate Rust validation into `pnpm check`, update contributor prerequisites, and prove that the
existing Node.js application behavior is unchanged. Phase 0 specification work may precede it or be
included as separate commits, but no production path should call Rust during the first slice.
