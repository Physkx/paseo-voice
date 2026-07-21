# Dictation implementation prompts

Use these prompts in order. Each phase should start from a clean `main`, preserve unrelated work,
and end with an isolated commit only after applicable validation passes. Do not deploy, publish,
change infrastructure, persist dictated content, expose secrets, or weaken provenance and explicit
confirmation. Run focused tests while developing and `pnpm check` before every commit.

## Implementation status

- Phases 1 through 5 are implemented and covered by focused Rust and browser-module tests.
- Phase 6 is partially implemented: the broker exposes read-only, redacted capability metadata.
  Multiple provider selection and the prompt studio await decisions DCT001 and DCT003.
- Phases 7 and 8 await decisions DCT002 through DCT004.
- Phases 9 and 10 remain the separate future desktop-companion work and await DCT005.
- Phase 11 remains blocked by DCT006 and DCT007. Default operation persists no dictated content.

The pending register is `docs/DICTATION_DECISIONS_PENDING.md`.

## Phase 1: mode contract and state machine

Prompt:

> Add an explicit connection-scoped voice mode contract with `live_response` and `dictation`
> states. Audit `crates/paseo-control-plane/src/realtime.rs`, the mock runtime, browser wire
> handling, and existing tests before editing. Define validated browser control and broker state
> frames for selecting and reporting the mode. New browser profiles default to live response, but
> the browser may restore a previously selected non-content preference after connection. Host
> changes must invalidate the automatic insertion target and clear drafts, bound context, and
> proposals under the existing contract. Reconnects must cancel active dictation work without
> preserving audio or transcript content. Add failing protocol and runtime tests first, including
> invalid modes, duplicate mode frames, reconnect reset, and an attempted mode change during a
> pending proposal. Preserve the existing later-turn confirmation and immutable response-context
> rules. Do not implement transcription or draft insertion in this phase.

Done when:

- Mode transitions are explicit, validated, and observable by the browser.
- Live response remains the default when no local preference exists.
- Mode changes cannot confirm, cancel, retarget, or otherwise mutate a pending proposal.
- Disconnect and host-change behavior is deterministic and content-free.
- Focused tests and `pnpm check` pass.

## Phase 2: minimal dictation vertical slice

Prompt:

> Implement the smallest end-to-end browser dictation slice. When mode is `live_response`, retain
> the current microphone commit and Realtime assistant-response behavior. When mode is
> `dictation`, commit one microphone turn for English transcription but do not create an assistant
> response, expose tools, or dispatch any write. Return a bounded dictation result frame containing
> an operation ID, status, and transcript only for the active browser connection. In mock mode,
> provide deterministic secret-free transcript fixtures. In `public/index.html` and
> `public/app.js`, add a prominent Live Response toggle and an ephemeral dictation preview. Insert
> the completed text into the response draft only, without submitting the form or touching the
> clipboard. Add public-path tests proving that one dictation produces one result, zero assistant
> responses, zero tool calls, and zero proposals. Do not add cleanup, smart spacing, or advanced
> recording controls yet.

Done when:

- The same microphone path selects exactly one of live response or dictation behavior.
- Dictation never produces assistant audio or an automatic Paseo write.
- Dictated text appears as an editable draft and is not persisted.
- Live response behavior and existing safety tests remain unchanged.
- Focused tests and `pnpm check` pass.

## Phase 3: cleanup and atomic draft insertion

Prompt:

> Add a dependency-injected English dictation-cleanup boundary in the Rust broker. Keep
> speech-to-text and cleanup as separate stages with bounded inputs, timeouts, cancellation, and
> explicit degraded results. Write an original strict default prompt that edits the transcript,
> preserves meaning and voice, removes filler and false starts, applies punctuation and obvious
> self-corrections, and never answers or executes dictated instructions. On cleanup failure, insert
> the raw transcript automatically when the captured target remains valid and show a visible
> degraded status. On transcription failure or no speech, return no insertable text. In the
> browser, capture the response field, selection range, original value, selected host, and
> immutable bound response context at recording start. Insert final text atomically at the captured
> caret or replace only the captured selection. Add exactly one space
> where inserted text meets adjacent word characters, add no space before punctuation, preserve
> formatting inside the inserted text, and never reformat text outside the inserted or replaced
> range. If only the field selection becomes stale while the host and immutable response context
> remain unchanged, show Insert and Discard controls instead of retargeting. If the host or response
> context changes, discard the result and clear the draft under the existing invalidation rules. Add
> focused unit and browser-path tests for all boundaries, Unicode, multiline content, stale
> contexts, cleanup fallback, and exact single insertion.

Done when:

- Cleanup cannot answer, execute, submit, or select a destination.
- Existing draft content outside the selected range remains byte-for-byte unchanged.
- Stale results never paste automatically into a different target.
- Cleanup failure is visible and does not lose a successful transcript.
- The system clipboard remains untouched.
- Focused tests and `pnpm check` pass.

## Phase 4: cancellation, concurrency, and recording controls

Prompt:

> Model recording, transcribing, cleaning, preview-ready, inserting, cancelled, and failed states
> explicitly for one browser connection. Permit only one active recording or cleanup operation.
> Add a visible Cancel control and page-scoped Escape shortcut that cancel broker work where
> possible, discard buffered audio and partial text, restore the exact pre-dictation draft and
> selection after a user-initiated cancel, and retain no recovery copy. Host changes and disconnects
> must clear the draft and selection without restoring stale state. Update the Audio interaction
> decision in `DECISIONS.md` with the approved recording modes and matching tests before adding
> both hold-to-record and tap-to-toggle, keeping hold-to-record as the default. Add optional
> auto-stop after a configurable silence period for toggle mode only, and treat silent or very
> short audio as no change. Reject or clearly report concurrent start attempts rather than queueing
> them. Test cancellation at every state boundary, late provider results, rapid repeated controls,
> disconnects, host changes, and attempts to interact while a proposal is pending.

Done when:

- Cancel is idempotent and late results cannot mutate the draft.
- A connection never has two active dictation operations.
- Hold, toggle, silence, and no-speech paths have deterministic state transitions.
- Draft, provenance, and proposal state remain isolated.
- Focused tests and `pnpm check` pass.

## Phase 5: microphone, accessibility, and local preferences

Prompt:

> Add a browser microphone picker after permission is granted. Persist only the selected device
> identifier and non-content preferences, and visibly fall back to the system default when the
> device is missing or permission changes. Request echo cancellation, noise suppression, automatic
> gain control, and a short pre-roll buffer by default. Provide advanced overrides for browser
> processing without exposing raw device labels before permission. Add optional start, stop,
> success, and error sounds, enabled by default at a restrained volume, with accessible visual and
> text equivalents. Add configurable page-scoped shortcuts for hold, toggle, and cancel, with
> input-field guards and conflict detection. Persist the Live Response preference locally, but
> never store audio, transcripts, cleanup output, drafts, or previews. Test unavailable devices,
> stale device IDs, permission revocation, keyboard conflicts, reduced motion, sound disabled, and
> reload behavior.

Done when:

- Device and preference recovery is predictable without retaining content.
- Quiet starts are not clipped by the normal capture path.
- Every sound cue has a non-audio equivalent.
- Shortcuts cannot type into, submit, or confirm from the wrong context.
- Focused tests and `pnpm check` pass.

## Phase 6: provider settings and prompt studio

Prompt:

> Separate speech-to-text and cleanup provider capabilities in trusted broker configuration. The
> broker must own endpoints, credentials, allowlisted models, defaults, and availability checks.
> Before sending new capability metadata or persisting a user-edited prompt, update `DECISIONS.md`
> with the exact safe browser fields and the local configuration, reset, deletion, and redaction
> boundary. Send the browser only safe provider IDs, labels, model IDs, processing-location
> descriptions, and current status. Add GUI selectors for speech-to-text and cleanup independently.
> Add a prompt studio that can view, edit, reset, and test the original built-in cleanup prompt
> using bounded sample text. Store only safe selected IDs and the explicit user-edited prompt
> according to the approved configuration boundary. The browser must never accept an API key,
> arbitrary endpoint, or unrestricted model string. Provider failure must preserve the draft and
> show a specific retry path. Never move from local to cloud automatically. Add configuration,
> redaction, capability, timeout, and failover-rejection tests.

Done when:

- Transcription and cleanup providers can be selected independently from broker-approved options.
- Provider location and degraded state are clear before and after a turn.
- Prompt testing cannot invoke tools, submit a response, or enter conversation history.
- Credentials and private endpoints never reach browser frames, logs, or arguments.
- Focused tests and `pnpm check` pass.

## Phase 7: vocabulary and snippets

Prompt:

> Add an explicit locally stored English vocabulary list for names, acronyms, and technical terms.
> Before persisting user-authored vocabulary or snippets, record a focused decision covering local
> storage, size limits, deletion, redaction, and secret handling. Provide add, review, and delete
> controls, bounded entry and collection sizes, Unicode-safe validation, and deterministic guidance
> to both transcription and cleanup providers where they support it. Do not learn entries
> automatically. Then add user-managed spoken snippets with
> validated trigger phrases and bounded replacement text. Expand snippets only within the final
> editable draft after cleanup, preserve insertion and provenance rules, and never submit an
> expansion automatically. Do not place secret-like snippet values in sync, export, logs, model
> prompts beyond the active operation, or telemetry. Add tests for trigger boundaries, overlapping
> triggers, punctuation, case, vocabulary echo, deletion, size limits, and malicious content.

Done when:

- Vocabulary and snippets are always explicit and reversible user actions.
- Expansion is deterministic, visible, and confined to the draft.
- Neither feature creates a new write or bypasses confirmation.
- No automatic correction monitoring exists in this phase.
- Focused tests and `pnpm check` pass.

## Phase 8: broker-hosted local processing

Prompt:

> Add one dependency-injected broker-hosted local speech-to-text adapter and one local cleanup
> adapter behind the provider contracts from phase 6. Keep model acquisition, process lifecycle,
> health checks, GPU or CPU selection, and resource limits outside the browser. Never invoke model
> processes through a shell, put secrets or dictated content in process arguments, or log content.
> Expose explicit loading, ready, unavailable, and failed states. If local processing fails, keep
> the existing draft unchanged and require an explicit user action before retrying with any cloud
> provider. Add fake-boundary tests first, followed by opt-in local integration tests that can skip
> cleanly when models are unavailable. Document installation and rollback without embedding
> machine-specific infrastructure details.

Done when:

- Local speech-to-text and cleanup work without browser-held endpoints or credentials.
- Resource and failure states are visible and bounded.
- No automatic local-to-cloud transition exists.
- Cloud behavior remains unchanged when local providers are not configured.
- Focused tests and `pnpm check` pass.

## Phase 9: desktop companion foundation

Prompt:

> Before implementation, write and review a desktop-companion architecture decision covering the
> Windows-first process boundary, authentication to the existing broker, signed packaging,
> updates, permission prompts, focus capture, clipboard ownership, global hotkeys, terminal paste
> behavior, and crash containment. Keep the browser milestone fully functional without the
> companion. Then create the smallest safe companion skeleton with dependency-injected platform
> interfaces and no transcript persistence. Capture the original focused application and editable
> target before recording. Preserve all available clipboard formats, place temporary dictated text
> on the clipboard, paste using the correct normal or terminal shortcut, and restore the clipboard
> only if it still contains the companion-owned value. If the original target is stale, require an
> explicit recovery choice. Add an option to keep dictated text in the clipboard. Use fake platform
> adapters for deterministic focus, clipboard-race, terminal, permission, and crash tests before
> any native integration.

Done when:

- The desktop trust boundary is approved and documented before native automation ships.
- Pasting targets the captured application, never merely the application focused at completion.
- Rich clipboard content survives successful, failed, cancelled, and racing paste attempts.
- The companion cannot access Paseo write credentials or bypass browser and broker safety rules.
- Focused tests and `pnpm check` pass.

## Phase 10: desktop global controls and media behavior

Prompt:

> Implement platform adapters for configurable global hold, toggle, and cancel hotkeys with
> multiple bindings per action, permission checks, registration diagnostics, and conflict errors.
> Do not register any global confirmation action and do not interpret voice as approval for Paseo
> writes or permissions. Add opt-in system media pausing that records exactly which sessions the
> companion paused and resumes only those sessions when recording stops or cancels. Preserve the
> desktop focus and clipboard contracts from phase 9 across hotkey changes, sleep, screen lock,
> application exit, and companion restart. Add platform-focused tests and a manual verification
> checklist. Do not publish installers or trigger deployment from this phase.

Done when:

- Global controls fail safely and report actionable permission or conflict states.
- Confirmation remains a separate trusted interaction in the Paseo interface.
- Media not paused by the companion is never resumed by it.
- Exit and crash paths release hotkeys and restore only companion-owned transient state.
- Focused tests and `pnpm check` pass.

## Phase 11: retention-gated correction learning and history

Prompt:

> Do not implement content persistence until an approved product and security decision defines
> retention duration, encryption, access control, deletion, export, redaction, backup, sync, and
> crash recovery. First write that decision and a threat model for observing post-insertion edits.
> Define how a likely spelling correction is distinguished from an intentional rewrite, how users
> review and undo learned entries, and how monitoring is disclosed and disabled. Only after
> approval, implement the smallest opt-in correction-learning slice with bounded observation and
> no background capture outside the inserted range. Treat transcript, discarded-recording, and
> dictation history as a separate opt-in feature with independent deletion controls. Add retention,
> encryption, migration, deletion, export, and adversarial edit tests before enabling either
> feature.

Done when:

- The phase remains blocked at design review until every retention requirement is approved.
- Correction learning is opt-in, reviewable, undoable, and limited to the disclosed field range.
- History and learning can be disabled and deleted independently.
- Default operation still persists no dictated content.
- Focused tests and `pnpm check` pass after any approved implementation.

## Final review prompt

> Review all dictation changes against `README.md`, `DECISIONS.md`, `docs/IMPLEMENTATION.md`,
> `docs/RUST_SAFETY_CONTRACT.md`, `docs/agents/publish-boundary.md`, and the repository
> `AGENTS.md`. Trace live response, dictation, cancellation, cleanup fallback, stale context,
> provider failure, and confirmed delivery end to end. Look specifically for duplicate assistant
> responses, cross-context insertion, clipboard mutation in the browser, transcript persistence,
> secret exposure, implicit cloud fallback, and any path that bypasses proposal confirmation. Fix
> validated findings, run focused tests and `pnpm check`, and update roadmap completion labels only
> for behavior demonstrated by tests. Do not deploy or publish.
