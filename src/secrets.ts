import type { Config } from "./config.js";
import { ExecError, type ExecFn } from "./exec.js";
import type { Logger } from "./log.js";

/**
 * Secrets live in memory for the lifetime of the process. They are fetched
 * once at startup (secret-manager round-trips are too slow to pay per tool call), are
 * never written to disk, never logged, and never placed in argv.
 *
 * Sources:
 * - environment provider: OPENAI_API_KEY / PASEO_PASSWORD from the process environment.
 * - bitwarden provider: BWS_ACCESS_TOKEN parsed from the bws env file (read as text,
 *   never shell-sourced), then `bws secret get <id>` per configured id with
 *   the token in the child env only.
 * - onepassword provider: `op read --format json <reference>` for each configured
 *   reference, using the CLI's inherited desktop or service-account authentication.
 *
 * Missing pieces resolve to null so the broker can degrade to MOCK mode or
 * disable Paseo tools instead of crashing.
 */
export interface Secrets {
  openaiApiKey: string | null;
  paseoPassword: string | null;
}

export interface SecretsDeps {
  execFile: ExecFn;
  readFile: (path: string) => Promise<string>;
  env: NodeJS.ProcessEnv;
  log: Logger;
}

const BWS_TOKEN_PATTERN =
  /^(?:\s*export\s+)?BWS_ACCESS_TOKEN\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s#]+))\s*(?:#.*)?$/m;

export function parseBwsEnvFile(text: string): string | null {
  const match = BWS_TOKEN_PATTERN.exec(text);
  if (!match) return null;
  return match[1] ?? match[2] ?? match[3] ?? null;
}

async function fetchBwsSecret(
  secretId: string,
  token: string,
  config: Config,
  deps: SecretsDeps,
): Promise<string | null> {
  try {
    const { stdout } = await deps.execFile(config.bwsBin, ["secret", "get", secretId], {
      env: { BWS_ACCESS_TOKEN: token, PATH: deps.env["PATH"] ?? "" },
      timeoutMs: 20_000,
    });
    const parsed: unknown = JSON.parse(stdout);
    if (
      parsed !== null &&
      typeof parsed === "object" &&
      "value" in parsed &&
      typeof (parsed as { value: unknown }).value === "string"
    ) {
      return (parsed as { value: string }).value;
    }
    deps.log.warn("bws secret response had no string value", { secretId });
    return null;
  } catch (err) {
    deps.log.warn("bws secret fetch failed", {
      secretId,
      error: err instanceof Error ? err.message : String(err),
    });
    return null;
  }
}

type SecretRole = "openaiApiKey" | "paseoPassword";

function copyDefinedEnv(env: NodeJS.ProcessEnv): Record<string, string> {
  return Object.fromEntries(
    Object.entries(env).filter((entry): entry is [string, string] => entry[1] !== undefined),
  );
}

function onePasswordErrorCategory(
  error: unknown,
): "not_found" | "timeout" | "nonzero_exit" | "unknown" {
  if (!(error instanceof Error)) return "unknown";
  if (/ENOENT/i.test(error.message)) return "not_found";
  if (error instanceof ExecError && error.timedOut) return "timeout";
  if (/timed?\s*out|ETIMEDOUT|killed/i.test(error.message)) return "timeout";
  if (error instanceof ExecError && error.exitCode !== null) return "nonzero_exit";
  return "unknown";
}

async function fetchOnePasswordSecret(
  role: SecretRole,
  reference: string,
  config: Config,
  deps: SecretsDeps,
): Promise<string | null> {
  let stdout: string;
  try {
    ({ stdout } = await deps.execFile(
      config.onePasswordBin,
      ["read", "--format", "json", reference],
      {
        env: copyDefinedEnv(deps.env),
        timeoutMs: 20_000,
      },
    ));
  } catch (error) {
    deps.log.warn("onepassword secret fetch failed", {
      provider: "onepassword",
      role,
      category: onePasswordErrorCategory(error),
      exitCode: error instanceof ExecError ? error.exitCode : null,
    });
    return null;
  }

  try {
    const value: unknown = JSON.parse(stdout);
    if (typeof value === "string") return value;
  } catch {
    // Report invalid output below without including stdout.
  }
  deps.log.warn("onepassword secret fetch failed", {
    provider: "onepassword",
    role,
    category: "invalid_output",
  });
  return null;
}

export async function loadSecrets(config: Config, deps: SecretsDeps): Promise<Secrets> {
  if (config.secretProvider === "environment") {
    const openaiApiKey = deps.env["OPENAI_API_KEY"] || null;
    const paseoPassword = deps.env["PASEO_PASSWORD"] || null;
    deps.log.info("environment provider: secrets from process env", {
      openaiApiKey: openaiApiKey ? "present" : "absent",
      paseoPassword: paseoPassword ? "present" : "absent",
    });
    return { openaiApiKey, paseoPassword };
  }

  if (config.secretProvider === "onepassword") {
    const openaiApiKey = config.onePasswordSecretRefOpenai
      ? await fetchOnePasswordSecret(
          "openaiApiKey",
          config.onePasswordSecretRefOpenai,
          config,
          deps,
        )
      : null;
    const paseoPassword = config.onePasswordSecretRefPaseo
      ? await fetchOnePasswordSecret(
          "paseoPassword",
          config.onePasswordSecretRefPaseo,
          config,
          deps,
        )
      : null;
    return { openaiApiKey, paseoPassword };
  }

  let token: string | null = null;
  try {
    token = parseBwsEnvFile(await deps.readFile(config.bwsEnvFile));
  } catch {
    token = null;
  }
  if (!token) {
    deps.log.warn("no BWS_ACCESS_TOKEN available; all bws-backed secrets unresolved", {
      bwsEnvFile: config.bwsEnvFile,
    });
    return { openaiApiKey: null, paseoPassword: null };
  }

  const openaiApiKey = config.bwsSecretIdOpenai
    ? await fetchBwsSecret(config.bwsSecretIdOpenai, token, config, deps)
    : null;
  if (!config.bwsSecretIdOpenai) {
    deps.log.info("bwsSecretIdOpenai unset; OpenAI disabled, MOCK mode");
  }

  const paseoPassword = config.bwsSecretIdPaseo
    ? await fetchBwsSecret(config.bwsSecretIdPaseo, token, config, deps)
    : null;
  if (!config.bwsSecretIdPaseo) {
    deps.log.info("bwsSecretIdPaseo unset; paseo tools need it");
  }

  return { openaiApiKey, paseoPassword };
}
