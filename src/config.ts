import { homedir } from "node:os";
import { join } from "node:path";
import { z } from "zod";

/**
 * Broker configuration. Precedence: environment variables, then the optional
 * JSON config file, then defaults. Secrets are NOT config: they are resolved
 * separately in secrets.ts and only ever held in memory.
 */
const ConfigSchema = z.object({
  listenHost: z.string().default("127.0.0.1"),
  /** 0 means an ephemeral port (used by tests). */
  listenPort: z.number().int().min(0).max(65535).default(8790),
  openaiModel: z.string().default("gpt-realtime-2.1"),
  openaiVoice: z.string().default("marin"),
  openaiBaseUrl: z.string().url().default("wss://api.openai.com/v1/realtime"),
  sparkBaseUrl: z.string().url().default("http://127.0.0.1:1234/v1"),
  sparkModel: z.string().default("qwen3.5-9b-instruct-nvfp4"),
  summariseThresholdChars: z.number().int().positive().default(700),
  logTailEntries: z.number().int().positive().default(40),
  proposalTtlMs: z.number().int().positive().default(120_000),
  paseoBin: z.string().default("paseo"),
  bwsBin: z.string().default("bws"),
  bwsEnvFile: z.string().default(join(homedir(), ".config", "bws.env")),
  bwsSecretIdOpenai: z.string().optional(),
  bwsSecretIdPaseo: z.string().optional(),
  /** Dev mode: read secrets from process env instead of bws. */
  devMode: z.boolean().default(false),
  /** Force MOCK realtime even if an OpenAI key resolves. */
  forceMock: z.boolean().default(false),
  logLevel: z.enum(["debug", "info", "warn", "error"]).default("info"),
});

export type Config = z.infer<typeof ConfigSchema>;

const ENV_MAP: Record<string, keyof Config> = {
  PASEO_VOICE_LISTEN_HOST: "listenHost",
  PASEO_VOICE_LISTEN_PORT: "listenPort",
  PASEO_VOICE_OPENAI_MODEL: "openaiModel",
  PASEO_VOICE_OPENAI_VOICE: "openaiVoice",
  PASEO_VOICE_OPENAI_BASE_URL: "openaiBaseUrl",
  PASEO_VOICE_SPARK_BASE_URL: "sparkBaseUrl",
  PASEO_VOICE_SPARK_MODEL: "sparkModel",
  PASEO_VOICE_SUMMARISE_THRESHOLD: "summariseThresholdChars",
  PASEO_VOICE_LOG_TAIL: "logTailEntries",
  PASEO_VOICE_PROPOSAL_TTL_MS: "proposalTtlMs",
  PASEO_VOICE_PASEO_BIN: "paseoBin",
  PASEO_VOICE_BWS_BIN: "bwsBin",
  PASEO_VOICE_BWS_ENV_FILE: "bwsEnvFile",
  PASEO_VOICE_BWS_SECRET_ID_OPENAI: "bwsSecretIdOpenai",
  PASEO_VOICE_BWS_SECRET_ID_PASEO: "bwsSecretIdPaseo",
  PASEO_VOICE_DEV: "devMode",
  PASEO_VOICE_MOCK: "forceMock",
  PASEO_VOICE_LOG_LEVEL: "logLevel",
};

const NUMBER_KEYS: ReadonlySet<keyof Config> = new Set([
  "listenPort",
  "summariseThresholdChars",
  "logTailEntries",
  "proposalTtlMs",
]);
const BOOLEAN_KEYS: ReadonlySet<keyof Config> = new Set(["devMode", "forceMock"]);

export class ConfigError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ConfigError";
  }
}

function coerceEnvValue(key: keyof Config, raw: string, envName: string): unknown {
  if (NUMBER_KEYS.has(key)) {
    const n = Number(raw);
    if (!Number.isFinite(n)) throw new ConfigError(`${envName} must be a number, got "${raw}"`);
    return n;
  }
  if (BOOLEAN_KEYS.has(key)) {
    if (raw === "1" || raw.toLowerCase() === "true") return true;
    if (raw === "0" || raw.toLowerCase() === "false") return false;
    throw new ConfigError(`${envName} must be 1/0/true/false, got "${raw}"`);
  }
  return raw;
}

export interface LoadConfigDeps {
  env: NodeJS.ProcessEnv;
  readFile: (path: string) => Promise<string>;
}

export function defaultConfigPath(env: NodeJS.ProcessEnv): string {
  return env["PASEO_VOICE_CONFIG"] ?? join(homedir(), ".config", "paseo-voice", "config.json");
}

export async function loadConfig(deps: LoadConfigDeps): Promise<Config> {
  const filePath = defaultConfigPath(deps.env);
  let fromFile: Record<string, unknown> = {};
  try {
    const rawText = await deps.readFile(filePath);
    const parsed: unknown = JSON.parse(rawText);
    if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
      throw new ConfigError(`${filePath} must contain a JSON object`);
    }
    fromFile = parsed as Record<string, unknown>;
  } catch (err) {
    if (err instanceof ConfigError) throw err;
    if (err instanceof SyntaxError) {
      throw new ConfigError(`${filePath} is not valid JSON: ${err.message}`);
    }
    // Missing file is fine; defaults + env apply.
  }

  const fromEnv: Record<string, unknown> = {};
  for (const [envName, key] of Object.entries(ENV_MAP)) {
    const raw = deps.env[envName];
    if (raw !== undefined && raw !== "") {
      fromEnv[key] = coerceEnvValue(key, raw, envName);
    }
  }

  const merged = { ...fromFile, ...fromEnv };
  const result = ConfigSchema.safeParse(merged);
  if (!result.success) {
    const issues = result.error.issues
      .map((i) => `${i.path.join(".") || "(root)"}: ${i.message}`)
      .join("; ");
    throw new ConfigError(`invalid configuration: ${issues}`);
  }
  return result.data;
}

/** Config as safe-to-log fields. Secret ids are ids, not values, but still trimmed. */
export function describeConfig(config: Config): Record<string, unknown> {
  return {
    listen: `${config.listenHost}:${config.listenPort}`,
    openaiModel: config.openaiModel,
    openaiVoice: config.openaiVoice,
    sparkBaseUrl: config.sparkBaseUrl,
    sparkModel: config.sparkModel,
    summariseThresholdChars: config.summariseThresholdChars,
    logTailEntries: config.logTailEntries,
    proposalTtlMs: config.proposalTtlMs,
    devMode: config.devMode,
    forceMock: config.forceMock,
    bwsSecretIdOpenai: config.bwsSecretIdOpenai ? "set" : "unset",
    bwsSecretIdPaseo: config.bwsSecretIdPaseo ? "set" : "unset",
  };
}
