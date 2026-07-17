import { resolve } from "node:path";
import { z } from "zod";
import type { ExecFn } from "./exec.js";
import { ExecError } from "./exec.js";

/**
 * Typed adapter over the paseo CLI. Verified live against paseo 0.1.107 on
 * 2026-07-15 (see test/fixtures/ for sanitised captures):
 *
 * - `paseo ls --json` returns an array of camelCase rows
 *   {id, shortId, name, provider, thinking, status, cwd, created}.
 * - `paseo inspect <id> --json` returns PascalCase keys (Id, Name, Status,
 *   PendingPermissions, ...). Kept as a loose record.
 * - `paseo logs <id> --tail N` renders TEXT regardless of --json/-o json.
 *   `--filter text` limits it to assistant message text, which is what the
 *   voice loop wants. Treated as plain text, never parsed as JSON.
 * - `paseo permit ls --json` returns [] when nothing is pending. The
 *   non-empty shape is UNVERIFIED (no pending permission existed during the
 *   capture); rows are handled tolerantly.
 * - CLI errors print {error: {code, message, details}} JSON. DAEMON_NOT_RUNNING
 *   observed live; other codes mapped generically.
 * - `send` waits for the agent by default: the adapter always passes --no-wait.
 *   `run` is always passed --detach. send/run are excluded from read-only
 *   integration checks; their success shapes are handled
 *   tolerantly.
 *
 * Secrets: PASEO_PASSWORD goes in the child env only. Remote daemon targeting
 * uses the PASEO_HOST env var (supported per CLI error text), never --host
 * argv with an embedded password.
 */

export interface PaseoSession {
  id: string;
  shortId: string;
  name: string;
  provider: string;
  thinking: string | null;
  status: string;
  cwd: string;
  created: string;
}

export interface PendingPermission {
  description: string;
  raw: unknown;
}

export class PaseoCliError extends Error {
  constructor(
    message: string,
    readonly code: string,
    readonly detail?: string,
  ) {
    super(message);
    this.name = "PaseoCliError";
  }
}

const SessionRowSchema = z
  .object({
    id: z.string(),
    shortId: z.string().default(""),
    name: z.string().default("(untitled)"),
    provider: z.string().default("unknown"),
    thinking: z.string().nullish(),
    status: z.string().default("unknown"),
    cwd: z.string().default(""),
    created: z.string().default(""),
  })
  .passthrough();

const CliErrorSchema = z.object({
  error: z.object({
    code: z.string().default("UNKNOWN"),
    message: z.string().default("paseo CLI error"),
    details: z.string().optional(),
  }),
});

function extractCliError(text: string): { code: string; message: string; details?: string } | null {
  try {
    const parsed = CliErrorSchema.safeParse(JSON.parse(text));
    if (parsed.success) return parsed.data.error;
  } catch {
    // not JSON
  }
  return null;
}

export interface PaseoClientOptions {
  exec: ExecFn;
  password: string;
  bin?: string;
  /** Remote daemon, e.g. "paseo-host.example:443". Uses PASEO_HOST env. */
  host?: string;
  env?: NodeJS.ProcessEnv;
  timeoutMs?: number;
}

export interface PaseoClient {
  listSessions(includeArchived?: boolean): Promise<PaseoSession[]>;
  readLogText(id: string, tail: number, filterText?: boolean): Promise<string>;
  inspect(id: string): Promise<Record<string, unknown>>;
  listPendingPermissions(): Promise<PendingPermission[]>;
  sendMessage(id: string, text: string): Promise<string>;
  startRun(
    prompt: string,
    opts?: { provider?: string; cwd?: string; title?: string },
  ): Promise<string>;
}

function describePermission(row: unknown): string {
  if (row === null || typeof row !== "object") return String(row);
  const rec = row as Record<string, unknown>;
  const parts: string[] = [];
  for (const key of [
    "agent",
    "agentName",
    "name",
    "tool",
    "toolName",
    "kind",
    "description",
    "summary",
  ]) {
    const value = rec[key];
    if (typeof value === "string" && value.length > 0) parts.push(value);
  }
  return parts.length > 0 ? parts.join(", ") : JSON.stringify(rec).slice(0, 200);
}

export function createPaseoClient(options: PaseoClientOptions): PaseoClient {
  const bin = options.bin ?? "paseo";
  const timeoutMs = options.timeoutMs ?? 20_000;
  const baseEnv = options.env ?? process.env;
  const expandHome = (path: string): string => {
    const home = baseEnv["HOME"];
    if (!home) return path;
    if (path === "~") return home;
    return path.startsWith("~/") ? resolve(home, path.slice(2)) : path;
  };

  const childEnv = (): Record<string, string> => {
    const env: Record<string, string> = {
      PATH: baseEnv["PATH"] ?? "",
      HOME: baseEnv["HOME"] ?? "",
      PASEO_PASSWORD: options.password,
    };
    if (options.host) env["PASEO_HOST"] = options.host;
    return env;
  };

  const invoke = async (args: string[], opts: { timeoutMs?: number } = {}): Promise<string> => {
    try {
      const { stdout } = await options.exec(bin, args, {
        env: childEnv(),
        timeoutMs: opts.timeoutMs ?? timeoutMs,
      });
      return stdout;
    } catch (err) {
      if (err instanceof ExecError) {
        const cliError = extractCliError(err.stdout) ?? extractCliError(err.stderr);
        if (cliError) {
          throw new PaseoCliError(cliError.message, cliError.code, cliError.details);
        }
        const stderrSnippet = err.stderr.trim().slice(0, 300);
        throw new PaseoCliError(
          `paseo ${args[0] ?? ""} failed (exit ${err.exitCode ?? "?"})${stderrSnippet ? `: ${stderrSnippet}` : ""}`,
          "CLI_FAILED",
        );
      }
      throw err;
    }
  };

  const invokeJson = async (args: string[]): Promise<unknown> => {
    const stdout = await invoke([...args, "--json"]);
    try {
      return JSON.parse(stdout);
    } catch {
      throw new PaseoCliError(
        `paseo ${args[0] ?? ""} returned non-JSON output: ${stdout.trim().slice(0, 200)}`,
        "CLI_BAD_JSON",
      );
    }
  };

  return {
    async listSessions(includeArchived = false) {
      const args = includeArchived ? ["ls", "-a", "-g"] : ["ls", "-g"];
      const data = await invokeJson(args);
      if (!Array.isArray(data)) {
        throw new PaseoCliError("paseo ls did not return an array", "CLI_BAD_JSON");
      }
      const sessions: PaseoSession[] = [];
      for (const row of data) {
        const parsed = SessionRowSchema.safeParse(row);
        if (parsed.success) {
          sessions.push({
            id: parsed.data.id,
            shortId: parsed.data.shortId,
            name: parsed.data.name,
            provider: parsed.data.provider,
            thinking: parsed.data.thinking ?? null,
            status: parsed.data.status,
            cwd: parsed.data.cwd,
            created: parsed.data.created,
          });
        }
      }
      return sessions;
    },

    async readLogText(id, tail, filterText = true) {
      const args = ["logs", id, "--tail", String(tail)];
      if (filterText) args.push("--filter", "text");
      return (await invoke(args)).trim();
    },

    async inspect(id) {
      const data = await invokeJson(["inspect", id]);
      if (data === null || typeof data !== "object" || Array.isArray(data)) {
        throw new PaseoCliError("paseo inspect did not return an object", "CLI_BAD_JSON");
      }
      return data as Record<string, unknown>;
    },

    async listPendingPermissions() {
      const data = await invokeJson(["permit", "ls"]);
      if (!Array.isArray(data)) return [];
      return data.map((row) => ({ description: describePermission(row), raw: row }));
    },

    async sendMessage(id, text) {
      // --no-wait: a voice turn must not block on agent completion.
      const stdout = await invoke(["send", id, "--prompt", text, "--no-wait", "--json"]);
      return stdout.trim();
    },

    async startRun(prompt, opts = {}) {
      const args = ["run", prompt, "--detach"];
      if (opts.provider) args.push("--provider", opts.provider);
      if (opts.cwd) args.push("--cwd", expandHome(opts.cwd));
      if (opts.title) args.push("--title", opts.title);
      args.push("--json");
      // Longer timeout: run creates a workspace before detaching.
      const stdout = await invoke(args, { timeoutMs: 60_000 });
      return stdout.trim();
    },
  };
}
