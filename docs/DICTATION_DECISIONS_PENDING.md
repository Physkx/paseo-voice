# Dictation decisions pending review

## Purpose

The browser dictation path through microphone and accessibility preferences is implemented without
persisting dictated content. The remaining phases cross durable-content, provider, native desktop,
or retention boundaries. This register records the choices needed before those phases continue.

## Decision register

| ID     | Area                    | Recommended choice                                                                                                                                                                                                                             | Decision needed                                                                                                                                                             |
| ------ | ----------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| DCT001 | Custom cleanup prompt   | Store one explicit user-authored prompt in a dedicated broker-owned local settings file with owner-only permissions, an 8,000 character limit, reset and delete controls, and no logging or sync                                               | Approve this storage boundary, choose browser-only storage instead, or keep the built-in prompt only                                                                        |
| DCT002 | Vocabulary and snippets | Use the same broker-owned settings boundary, with separate delete-all controls, bounded entries and collections, no automatic learning, no export or sync, and a warning not to store secrets                                                  | Approve persistence and limits before Phase 7                                                                                                                               |
| DCT003 | Provider catalogue      | Keep cloud English transcription and broker-configured cleanup as the fixed baseline. Add providers only as broker-configured allowlisted IDs. Never permit arbitrary browser endpoints or automatic local-to-cloud fallback                   | Identify the additional speech-to-text and cleanup providers and models that should be allowlisted                                                                          |
| DCT004 | Local processing        | Add broker adapters only after selecting the local speech model, cleanup runtime, model acquisition process, health checks, resource limits, and DGX or local-CPU ownership                                                                    | Select the supported local models and which host owns their lifecycle                                                                                                       |
| DCT005 | Desktop companion       | Build a separate Windows-first Rust companion using injected Win32 focus, UI Automation, clipboard, hotkey, and media interfaces. Authenticate it to the loopback broker with a companion-specific capability that cannot confirm Paseo writes | Approve the process and authentication boundary, packaging and update mechanism, and terminal detection rules before native work                                            |
| DCT006 | Correction learning     | Keep disabled. If later approved, make it opt-in and limited to edits within the inserted range, with review, undo, and delete-all controls                                                                                                    | Define how spelling corrections are distinguished from rewrites and approve the disclosure model                                                                            |
| DCT007 | Dictation history       | Keep disabled and ephemeral by default                                                                                                                                                                                                         | Define retention duration, encryption at rest, access control, deletion, export, backups, sync, redaction, and crash recovery before any transcript or draft history exists |

## Current conservative behavior

- English speech-to-text uses the fixed broker-configured Realtime transcription model.
- Cleanup uses the configured local or remote OpenAI-compatible endpoint and model.
- Cleanup failure inserts the successful bounded raw transcript with a visible degraded warning.
- No automatic cloud fallback exists for cleanup.
- Capability frames expose only approved IDs, labels, model IDs, location descriptions, and status.
- Audio, transcripts, cleanup output, drafts, previews, device labels, and cancellation recovery are
  never written to durable storage.
- System-wide application insertion remains a separate desktop-companion phase.
