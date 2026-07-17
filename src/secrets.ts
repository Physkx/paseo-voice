import type { Config } from "./config.js";
import type { ExecFn } from "./exec.js";
import type { Logger } from "./log.js";

/**
 * Secrets live in memory for the lifetime of the process. They are fetched
 * once at startup (bws round-trips are too slow to pay per tool call), are
 * never written to disk, never logged, and never placed in argv.
 *
 * Sources:
 * - dev mode: OPENAI_API_KEY / PASEO_PASSWORD from the process environment.
 * - normal: BWS_ACCESS_TOKEN parsed from the bws env file (read as text,
 *   never shell-sourced), then `bws secret get <id>` per configured id with
 *   the token in the child env only.
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

export async function loadSecrets(config: Config, deps: SecretsDeps): Promise<Secrets> {
  if (config.devMode) {
    const openaiApiKey = deps.env["OPENAI_API_KEY"] ?? null;
    const paseoPassword = deps.env["PASEO_PASSWORD"] ?? null;
    deps.log.info("dev mode: secrets from process env", {
      openaiApiKey: openaiApiKey ? "present" : "absent",
      paseoPassword: paseoPassword ? "present" : "absent",
    });
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
