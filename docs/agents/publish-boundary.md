# Publish boundary

Read this before changing the browser, future web GUI, or deployment configuration. It defines
what must never enter a public static build or cross the browser boundary.

## Never ship

- API keys, passwords, Bitwarden tokens or secret IDs, private endpoints, or local configuration
  that is not explicitly classified as safe display metadata in `DECISIONS.md`
- `paseo.json`, `.env` files, logs, persisted transcripts, raw agent output, or local session
  history
- Broker-side tools, confirmation tokens, process access, or credentials in browser code
- `docs/agents/`, internal notes, fixtures, or repository-only operational metadata in web output
- A response target chosen from editable text, a thread title, or model-generated arguments
- Durable conversation history before retention, deletion, access control, and redaction are defined

## Required boundaries

- The browser remains secret-free and communicates with the broker over authenticated and
  encrypted transport before any non-local deployment.
- Every summary carries immutable source-thread and source-reply identifiers assigned by the
  broker.
- The response field and confirmation proposal remain bound to that source context. The broker
  rejects stale, missing, consumed, or cross-thread contexts.
- Browser labels and avatar state are helpful cues, but broker validation is the security boundary.
- Live dashboard metadata is ephemeral, content-bounded, same-origin data classified in
  `DECISIONS.md`; browser code must not persist it or include it in static assets.
- A bounded dictation transcript may cross only its same-origin browser connection as the
  ephemeral draft result classified in `DECISIONS.md`. It must not enter static assets, logs,
  durable storage, or another browser connection.
- Public configuration uses only values intentionally safe for anyone to inspect.

## Safe to publish

- Compiled static GUI assets containing no secrets or private operational data
- Accessibility, avatar, dashboard, and local interaction code that treats broker data as untrusted
- Reserved example domains and documented public variable names without values

## If unsure

Stop and read `docs/agents/state.md`. Do not deploy or expose the broker until authentication,
transport security, origin policy, and response-routing tests are implemented and approved.
