<p align="center">
  <img src="docs/assets/logo.svg" alt="Paseo Voice logo" width="96" height="96" />
</p>

# Paseo Voice Agent

Paseo Voice is a local push-to-talk and dictation interface for Paseo coding-agent sessions. A
Rust broker connects a secret-free browser to Paseo and selectable Realtime providers while keeping every write
behind an explicit proposal and confirmation gate.

![The Paseo Voice dashboard in mock mode](docs/assets/dashboard.png)

The project is early alpha. It is intended for local or private-host use and is not ready for
direct public-network exposure.

## Current state

The current application provides:

- A browser dashboard with trusted Paseo host profiles, session selection, typed steering, and
  push-to-talk voice interaction.
- Connection-scoped voice API and dictation cleanup selectors populated only from broker-safe
  profile metadata. Reconnect always restores the configured defaults.
- Manual reading and opt-in automatic announcement of the latest agent reply. The reply becomes an
  immutable response context tied to its source session.
- Provenance-bound response proposals and broker-gated session creation. In the browser, only the
  explicit Confirm control can execute a proposal; the local console has a separate deterministic
  text confirmation path.
- English dictation that transcribes, cleans, and inserts text into the bound draft without
  producing an assistant response or submitting a write.
- Barge-in, summary replay, microphone selection, hold and toggle recording, local shortcuts, a
  WebGL avatar with CSS fallback, and accessible text states.
- A content-free SQLite operation journal and conservative recovery for attempted writes.
- A text-only mock browser runtime and deterministic local console.

When `autoReplyPollMs` is nonzero, each live browser connection polls its selected Paseo host and
treats a visible non-idle to idle transition as a completed reply. It reads and binds that reply,
then asks OpenAI Realtime to summarise and speak it with tools disabled. This is an alpha fallback:
fast transitions may be missed, concurrent browsers may announce the same reply, and identical reply
text is deduplicated by its synthetic digest. Manual reads remain available. After a connection
closes gracefully, the broker can give its committed content-free deduplication snapshot to a later
connection. No queue state survives a broker restart.

## Requirements

Node.js and Rust are required to build and start the repository. The remaining dependencies enable
full live functionality; mock mode can start without them.

- Node.js 26 or newer and pnpm 11.13.1 for repository tooling
- Rust 1.97.0 through `rustup`
- A working `paseo` CLI for session operations
- An explicit credential for each configured official OpenAI Realtime profile
- A named API credential or the provider-owned Grok OAuth session for official xAI routes
- An OpenAI-compatible chat-completions endpoint for optional reply summaries and dictation cleanup
- Bitwarden Secrets Manager CLI, 1Password CLI, or process environment variables for secrets

If the default official Realtime profile has no resolved credential, the broker selects text-only
mock mode. A credential-free local compatible endpoint can run live. Set `forceMock` or
`PASEO_VOICE_MOCK=true` to disable outbound Realtime connections in every case.

## Quick start

```bash
pnpm install --frozen-lockfile
pnpm check
pnpm build
pnpm start
```

Open `http://localhost:8790`. Microphone access normally requires `http://localhost` or HTTPS.

Use the local text interface with:

```bash
pnpm console
```

## Configuration

Unrelated runtime environment overrides take precedence over the JSON file and built-in defaults.
Voice, cleanup, summarisation, and API credential definitions are JSON-only so routing remains
explicit and reviewable.
The default file is `~/.config/paseo-voice/config.json`; override it with `PASEO_VOICE_CONFIG`.
Start from [config.example.json](config.example.json).

`paseoHosts` defines the broker-owned host selector. Exactly one profile must be the default. The
browser receives safe labels and creation defaults, while daemon targets and credentials remain in
Rust.

```json
{
  "paseoHosts": [
    {
      "id": "local",
      "label": "Local Paseo",
      "target": null,
      "default": true,
      "defaultCwd": "~/",
      "defaultProvider": "opencode/gpt-5.6-sol-max"
    }
  ]
}
```

Paths are passed unchanged for expansion by the selected Paseo daemon. The legacy
`PASEO_VOICE_PASEO_HOST` override changes the target of the configured default profile only.

Set `autoReplyPollMs` to `1000` for the alpha automatic announcement fallback, or leave it at `0`
to disable polling. The equivalent environment override is `PASEO_VOICE_AUTO_REPLY_POLL_MS`.

`voiceProfiles` contains named `openai`, `xai`, or `openai-compatible` routes. Exactly one must be
the default. Official profiles are pinned to `wss://api.openai.com/v1/realtime` or
`wss://api.x.ai/v1/realtime`; the broker adds the validated model query. xAI transcription uses the
configured model such as `grok-transcribe`, and cumulative xAI transcription updates replace the
preview instead of being appended as deltas.

`cleanupProfiles` contains named OpenAI-compatible `POST /chat/completions` routes. Exactly one must
be the default. Cleanup selection is independent from the fixed `summarisation` route. A cleanup
failure returns the bounded raw transcript with a degraded warning and never tries another profile.
See [config.example.json](config.example.json) for official OpenAI and xAI voice, keyless local
voice, local and private cleanup, xAI OAuth cleanup, fixed summarisation, and reusable credentials.

### Secrets

Select one provider for the process with `secretProvider` or
`PASEO_VOICE_SECRET_PROVIDER`: `bitwarden`, `onepassword`, or `environment`. Bitwarden is the
default.

- `environment` reads each exact `apiCredentials.environmentVariable` plus `PASEO_PASSWORD`.
  Ambient OpenAI or xAI aliases are not inferred.
- `bitwarden` reads a Secrets Manager token from `~/.config/bws.env` and resolves the configured
  `apiCredentials[].bwsSecretId` values and the separate Paseo secret ID.
- `onepassword` resolves each `apiCredentials[].onePasswordSecretRef` and the separate Paseo
  password reference through `op read`.

The provider-owned Grok OAuth store at `~/.grok/auth.json`, optionally overridden with
`GROK_AUTH_FILE`, is eligible only for exact official xAI voice, cleanup, and summarisation routes.
The broker refreshes a near-expiry token through the exact xAI OAuth token endpoint and writes the
refreshed provider session back to the same file. OAuth is preferred over a named environment
credential whose configured variable is exactly `XAI_API_KEY`. Explicit Bitwarden, 1Password, and
other named environment credentials remain operator overrides. Named credential values and OAuth
tokens are never forwarded to secret-manager child processes.

Secrets are resolved once at startup. Missing OpenAI credentials affect Realtime only; missing
Paseo credentials disable Paseo tools without preventing the server from starting. Missing model
credentials make summarisation and dictation cleanup degrade to safe local fallbacks. Restart after
secret rotation.

### Breaking configuration change

The old `openai*`, `spark*`, and associated OpenAI or Spark secret reference fields and environment
overrides are rejected. They are not translated. Replace them with `voiceProfiles`,
`cleanupProfiles`, `summarisation`, and `apiCredentials` before starting this version.

### Endpoint policy

Each bearer is attached only to the exact validated endpoint of the profile that references it.
Plain `ws://` is accepted only for an OpenAI-compatible loopback voice endpoint. Plain model
`http://` is accepted on loopback. A Tailscale IPv4 or exact broker allowlisted private host also
requires that route's `allowInsecurePrivateHttp` opt-in. Other remote endpoints require `wss://`
and `https://`. Model redirects and ambient HTTP proxy discovery are disabled.

## Safety model

The browser, model, transcription, tool arguments, display labels, and CLI output are untrusted.
Rust owns the credentials, immutable reply provenance, proposal state, and only Paseo write path.

- A response proposal accepts text but no destination. Rust derives the destination from the reply
  context created by the last successful read.
- Changing host or reply context invalidates the draft and proposal instead of retargeting them.
- Voice switching is idle-only, retires the old provider, ignores its late events, preserves the
  selected Paseo host, and clears the response context so the latest reply must be read again.
- Cleanup switching is blocked during recording, transcription, or cleanup. It does not invalidate
  the active Paseo summary or proposal.
- The Realtime model cannot confirm a write. Browser confirmation requires the exact current
  presentation and a later trusted interaction; the local console uses a separate trusted text
  path.
- Each confirmation makes at most one Paseo process attempt. Any uncertain post-spawn result is
  `outcome_unknown` and is never retried automatically.
- Runtime-generated durable state excludes transcripts, summaries, response bodies, prompts, agent
  output, and credentials. Operator-managed secret-provider files remain outside that journal.

See [docs/RUST_SAFETY_CONTRACT.md](docs/RUST_SAFETY_CONTRACT.md) for the normative invariants.

## Commands

| Command           | Purpose                                                 |
| ----------------- | ------------------------------------------------------- |
| `pnpm build`      | Build the Rust workspace in release mode                |
| `pnpm check`      | Run formatting, lint, browser and Rust tests, and build |
| `pnpm console`    | Open the Rust text console                              |
| `pnpm format`     | Format tracked source and documentation                 |
| `pnpm lint`       | Lint browser JavaScript and tooling                     |
| `pnpm rust:build` | Build the Rust workspace in release mode                |
| `pnpm rust:lint`  | Run Clippy across the Rust workspace                    |
| `pnpm rust:test`  | Run all Rust tests                                      |
| `pnpm start`      | Start the Rust broker and browser                       |
| `pnpm test`       | Run browser JavaScript and Rust tests                   |

## Project layout

- `crates/paseo-safety-core/`: pure provenance, queue, proposal, and confirmation state
- `crates/paseo-control-plane/`: Rust runtime, adapters, protocol, and tests
- `public/`: secret-free browser client with no build step
- `docs/IMPLEMENTATION.md`: current runtime architecture
- `DECISIONS.md`: short architectural history and unresolved boundaries
- `docs/agents/`: operational state, publish boundary, and agent playbooks

## Current limits

- Automatic completion uses an opt-in status-polling heuristic until Paseo exposes stable markers
- No exactly-once delivery claim until Paseo supports receiver-recognised idempotency
- No authenticated remote broker transport or configured public web deployment
- No durable transcript, summary, draft, response, or cancelled-proposal history
- No automatic provider failover or browser persistence of voice and cleanup profile selection
- No custom cleanup prompts, vocabulary, snippets, correction learning, or desktop companion

## Contributing

Read [CONTRIBUTING.md](CONTRIBUTING.md) before opening a pull request. Coding agents must also follow
[AGENTS.md](AGENTS.md).

## License

[MIT](LICENSE)
