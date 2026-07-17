# Rust migration decisions pending review

## Purpose

The user requested autonomous implementation and will review decisions at the end. Entries here
record provisional choices that materially affect product behavior, security, compatibility, or
operations. Implementation proceeds with the conservative choice shown unless evidence requires a
new entry.

## Decision register

| ID   | Area                 | Provisional choice                                                    | Main consequence                                                    | Alternatives for final review                              |
| ---- | -------------------- | --------------------------------------------------------------------- | ------------------------------------------------------------------- | ---------------------------------------------------------- |
| D001 | Response size        | Limit exact UTF-8 response bodies to 65,536 bytes                     | Rejects unusually large replies before proposal                     | 16 KiB, 32 KiB, configurable maximum                       |
| D002 | Expiry               | Expire at `now >= expires_at` using monotonic milliseconds            | Exact deadline is already expired                                   | Preserve the TypeScript `now > expires_at` edge            |
| D003 | Voice confirmation   | Require a later broker-observed user interaction                      | Model cannot propose and confirm in one turn                        | Require browser click for every write                      |
| D004 | Browser confirmation | Support explicit authenticated browser confirmation as strongest mode | Enables deterministic approval independent of language-model intent | Voice-only confirmation                                    |
| D005 | Content persistence  | Persist metadata and hashes only                                      | Pending bodies cannot survive restart                               | Encrypted short-lived body journal with retention policy   |
| D006 | Ambiguous delivery   | Mark unknown and never retry automatically                            | May require manual reconciliation                                   | Retry with risk of duplicate until Paseo has idempotency   |
| D007 | Paseo transport      | Use the supported CLI until a supported stable-ID interface exists    | Cannot claim exactly-once delivery yet                              | Supported daemon client protocol or upstream CLI extension |
| D008 | Secret providers     | Preserve Bitwarden, environment, and OnePassword; default Bitwarden   | Rust replacement must implement three adapters                      | Drop a provider or change the default                      |
| D009 | Local interface      | Use length-delimited JSON over child stdin/stdout during migration    | One supervised child per broker, no listening socket                | Unix domain socket or direct in-process Rust broker        |
| D010 | Final architecture   | Replace the Node.js backend with one Rust process                     | Browser assets remain JavaScript; Node remains tooling only         | Permanent TypeScript voice adapter plus Rust sidecar       |
| D011 | Runtime persistence  | Use SQLite metadata journal with restrictive local permissions        | Adds one embedded database dependency                               | Append-only JSON records or no crash recovery              |
| D012 | Rust async and HTTP  | Tokio, Axum, rustls, and structured Serde messages                    | Conventional maintained stack, larger dependency graph              | Lower-level hyper/tungstenite implementation               |

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
