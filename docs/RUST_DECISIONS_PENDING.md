# Rust migration decisions pending review

## Purpose

The user requested autonomous implementation and will review decisions at the end. Entries here
record provisional choices that materially affect product behavior, security, compatibility, or
operations. Implementation proceeds with the conservative choice shown unless evidence requires a
new entry.

## Decision register

| ID   | Area                 | Provisional choice                                                                                             | Main consequence                                                                                       | Alternatives for final review                                       |
| ---- | -------------------- | -------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------- |
| D001 | Response size        | Limit exact UTF-8 response bodies to 65,536 bytes                                                              | Rejects unusually large replies before proposal                                                        | 16 KiB, 32 KiB, configurable maximum                                |
| D002 | Expiry               | Expire at `now >= expires_at` using monotonic milliseconds                                                     | Exact deadline is already expired                                                                      | Preserve the TypeScript `now > expires_at` edge                     |
| D003 | Voice confirmation   | Require a later broker-observed user interaction                                                               | Model cannot propose and confirm in one turn                                                           | Require browser click for every write                               |
| D004 | Browser confirmation | Support an explicit connection-bound browser confirmation on the loopback broker                               | Deterministic approval is available, but it is not authenticated for non-loopback exposure             | Add authenticated remote access, or require voice-only confirmation |
| D005 | Content persistence  | Persist metadata and hashes only                                                                               | Pending bodies cannot survive restart                                                                  | Encrypted short-lived body journal with retention policy            |
| D006 | Ambiguous delivery   | Mark unknown and never retry automatically                                                                     | May require manual reconciliation                                                                      | Retry with risk of duplicate until Paseo has idempotency            |
| D007 | Paseo transport      | Use the supported CLI until a supported stable-ID interface exists                                             | Cannot claim exactly-once delivery yet                                                                 | Supported daemon client protocol or upstream CLI extension          |
| D008 | Secret providers     | Preserve Bitwarden, environment, and OnePassword; default Bitwarden                                            | Rust replacement must implement three adapters                                                         | Drop a provider or change the default                               |
| D009 | Local interface      | Keep length-delimited JSON over stdin/stdout as a diagnostic and compatibility harness                         | Production uses direct in-process Rust calls; the harness opens no listener                            | Remove the harness after downstream users confirm it is unused      |
| D010 | Final architecture   | Replace the Node.js backend with one Rust process                                                              | Browser assets remain JavaScript; Node remains tooling only                                            | Permanent TypeScript voice adapter plus Rust sidecar                |
| D011 | Runtime persistence  | Use SQLite metadata journal with restrictive local permissions                                                 | Adds one embedded database dependency                                                                  | Append-only JSON records or no crash recovery                       |
| D012 | Rust async and HTTP  | Tokio, Axum, rustls, and structured Serde messages                                                             | Conventional maintained stack, larger dependency graph                                                 | Lower-level hyper/tungstenite implementation                        |
| D013 | Reply identity       | Assign a broker observation ID from thread ID and the exact output digest                                      | Conservatively deduplicates identical output because Paseo 0.1.107 logs expose no reply ID             | Require an upstream reply ID, or add a trusted completion sequence  |
| D014 | Shadow evidence      | Use contract, sanitized trace, duplicate-event, and cross-thread automation as the migration gate              | No calendar-based live shadow observation period is required before the direct Rust cutover            | Require a live mismatch-free soak before cutover                    |
| D015 | New-run gate         | Keep `start_run` in a separate Rust pending-action type with the same later-turn, expiry, and journal controls | Avoids weakening response provenance while preserving existing run creation behavior                   | Generalise the safety core to a typed action state machine          |
| D016 | Realtime API drift   | Pin the tested Rust Realtime event handling and fail closed on unknown function calls                          | Future API changes may require an explicit compatibility update                                        | Add a versioned compatibility layer or vendor-supported SDK         |
| D017 | Browser origin       | Reject browser WebSocket upgrades whose `Origin` does not match `Host`                                         | Blocks cross-site control of a loopback broker; native clients without `Origin` remain supported       | Require an authenticated session for every client                   |
| D018 | Metadata retention   | Retain at most the latest 10,000 content-free journal transitions                                              | Bounds local metadata growth and deletes the oldest history automatically                              | Time-based retention or a different row limit                       |
| D019 | Queue scope          | Keep observation and active-summary queues connection-scoped for the replacement                               | Reconnect safely invalidates context; automatic process-wide completion detection remains roadmap work | Add a process-wide observer and presentation queue now              |

The D009 harness uses a four-byte big-endian length followed by at most 131,072 bytes of strict
versioned JSON. It opens no listener, processes requests sequentially, and shuts down on EOF. The
production broker does not spawn it. There is no automatic request or write retry.

D004 is safe under the current default `127.0.0.1` listen address. It must not be interpreted as
authorization for public or shared-network exposure. Authentication and origin controls are a
required design decision before any such exposure.

## Decisions not deferred

These requirements are fixed by existing project policy and are not offered as final-review
choices:

- No public broker exposure or deployment changes.
- No voice approval for Paseo permission requests.
- No shell invocation for Paseo or secret-manager commands.
- No secrets in arguments, logs, fixtures, documentation, or durable metadata.
- No transcript, summary, response body, or agent-output persistence without an approved policy.
- One production write path after cutover.
- No npm, Cargo crate, or binary publication.
