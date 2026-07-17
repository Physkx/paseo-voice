# Contributing

Paseo Voice is early alpha. Small, focused pull requests with tests are the easiest to review.

## Development setup

Use Node.js 26, the pnpm version declared in `package.json`, and Rust through `rustup`. The
repository pins the exact Rust release and required rustfmt and Clippy components in
`rust-toolchain.toml`.

```bash
corepack enable
pnpm install --frozen-lockfile
pnpm check
```

Running a Cargo or pnpm Rust command from the repository installs the pinned toolchain when needed.
Use the focused commands below while developing Rust code:

```bash
pnpm rust:format
pnpm rust:lint
pnpm rust:test
pnpm rust:build
```

The test suite does not require live Paseo, OpenAI, Bitwarden, 1Password, microphone, or
summariser services.

## Making changes

- Keep TypeScript strict and preserve dependency injection at process and network boundaries.
- Keep Rust free of unsafe code and preserve the pure safety-core module boundary.
- Declare shared third-party Rust dependencies in the root `[workspace.dependencies]` table and
  inherit them from member crates.
- Add or update tests for behavior changes.
- Keep write operations behind the two-phase confirmation gate.
- Never put credentials in source, fixtures, logs, command arguments, or documentation.
- Use portable paths such as `~/dev/project` and reserved example domains for hostnames.
- Keep machine-specific endpoints in local configuration, not source defaults or fixtures.
- Do not add em dash or en dash characters to user-facing copy, documentation, comments, or commits.
- Run `pnpm format` before the final check.

## Required verification

```bash
pnpm check
```

Pull requests should explain the user-visible change, notable design choices, and any manual testing
that could not be automated.

## Commit guidance

Use short imperative subjects. Keep refactors separate from behavior changes when practical. Do not
include generated `dist/` output, local configuration, or secrets.

## Reporting problems

Open a GitHub issue with reproduction steps, expected behavior, actual behavior, Node and pnpm
versions, and relevant redacted logs. Do not include API keys, passwords, secret IDs, or private
infrastructure details.
