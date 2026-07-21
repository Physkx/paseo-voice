# Playbook: verify review deployment

## Use when

Confirming a future static GUI deployment after Cloudflare Workers Builds has been explicitly
configured and a web change has been pushed to `main`.

This playbook is dormant while `docs/agents/state.md` says web deployment is not configured.

## Preconditions

- A standalone web workspace, build command, output directory, and `wrangler.jsonc` exist.
- `docs/agents/state.md` records the Worker and review URL without secret values.
- Cloudflare Workers Builds is connected to this repository and `main`.
- Applicable local validation has passed before push.
- Broker authentication, encrypted transport, and allowed-origin policy are approved for remote use.

## Files you will touch

Verification normally changes no files. Update `docs/agents/state.md` only when confirmed hosting
or validation facts have changed.

## Steps

1. Run `pnpm check` and the future web workspace build.
2. Push the intentional task commit to `origin/main`.
3. Observe the Git-driven Workers Build without triggering a manual deployment.
4. Open the recorded review URL and confirm the expected commit is served.
5. Test page load, static assets, microphone permission guidance, reconnect behavior, and a safe
   broker connection failure.
6. Confirm no secret, internal agent document, source map containing private data, or local endpoint
   is present in the static output.

## Validate

- Local repository and web checks pass.
- The review deployment serves the expected commit over HTTPS.
- The GUI fails closed when broker authentication or routing context is absent.
- Browser responses remain bound to the thread that originated each summary.

## Done means

- The Git-driven review deployment is healthy and its commit and URL are reported.
- No manual deploy, DNS, secret, or Worker binding action was performed.

## Do not

- Run `wrangler deploy`, modify secrets or bindings, or change custom domains or DNS.
- Treat a static GUI deployment as authorization to expose the broker publicly.
- Verify a live deployment while `state.md` still marks deployment as unconfigured.

## If stuck

Report the local validation result and the missing Cloudflare or broker prerequisite. Do not work
around a failed build with a manual deployment.
