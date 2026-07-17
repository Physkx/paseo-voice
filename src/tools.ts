import { z } from "zod";
import type { Config } from "./config.js";
import type { ProposalStore } from "./gate.js";
import type { Logger } from "./log.js";
import type { PaseoClient, PaseoSession } from "./paseo.js";
import { PaseoCliError } from "./paseo.js";
import { cleanForSpeech, type SummariseInput, type SummariseResult } from "./summarise.js";

/**
 * The function tools exposed to the realtime voice model, and their
 * execution. Safety invariants:
 *
 * - send_message and start_run NEVER execute; they only register a proposal
 *   in the gate and return a spoken echo for readback.
 * - confirm_action is the only path that executes a write, and only with a
 *   valid, unexpired, single-use token.
 * - list_pending_permissions is narration only; there is no approval tool.
 *
 * Every dispatch returns a JSON-serialisable result and never throws across
 * the realtime boundary.
 */

export interface ToolDeps {
  paseo: PaseoClient;
  gate: ProposalStore;
  summarise: (input: SummariseInput) => Promise<SummariseResult>;
  config: Config;
  log: Logger;
}

export type ToolResult = Record<string, unknown> & { ok: boolean };

export interface ToolDispatcher {
  dispatch(name: string, argsJson: string): Promise<ToolResult>;
  currentSession(): { id: string; name: string } | null;
}

/** Realtime API function tool definitions (session.tools entries). */
export const TOOL_DEFINITIONS = [
  {
    type: "function" as const,
    name: "list_sessions",
    description:
      "List Paseo agent sessions with id, title, provider, and state. Use before reading or sending when the target session is unclear.",
    parameters: {
      type: "object",
      properties: {
        include_archived: { type: "boolean", description: "Also list archived sessions" },
      },
      required: [],
    },
  },
  {
    type: "function" as const,
    name: "set_current_session",
    description:
      "Set the default session for subsequent commands. Accepts a session id, short id, or a fragment of the session title.",
    parameters: {
      type: "object",
      properties: {
        session: { type: "string", description: "Session id, short id, or title fragment" },
      },
      required: ["session"],
    },
  },
  {
    type: "function" as const,
    name: "read_latest_reply",
    description:
      "Read the latest reply from a session. mode=summary (default) gives a short spoken brief of recent output; mode=full reads the latest reply with light cleanup.",
    parameters: {
      type: "object",
      properties: {
        session: {
          type: "string",
          description: "Session id, short id, or title fragment. Defaults to the current session.",
        },
        mode: { type: "string", enum: ["summary", "full"] },
      },
      required: [],
    },
  },
  {
    type: "function" as const,
    name: "list_pending_permissions",
    description:
      "List permission requests waiting for approval across sessions. Read-only: approvals happen in the Paseo UI, never by voice.",
    parameters: { type: "object", properties: {}, required: [] },
  },
  {
    type: "function" as const,
    name: "send_message",
    description:
      "PROPOSE sending a message to a session. Does not send. Read the returned spoken_echo to the user and only call confirm_action after an explicit yes.",
    parameters: {
      type: "object",
      properties: {
        session: {
          type: "string",
          description: "Session id, short id, or title fragment. Defaults to the current session.",
        },
        text: { type: "string", description: "The message to send" },
      },
      required: ["text"],
    },
  },
  {
    type: "function" as const,
    name: "start_run",
    description:
      "PROPOSE starting a new agent run. Does not start anything. Read the returned spoken_echo to the user and only call confirm_action after an explicit yes.",
    parameters: {
      type: "object",
      properties: {
        prompt: { type: "string", description: "The task for the new agent" },
        provider: { type: "string", description: "Optional provider, e.g. claude or codex" },
        cwd: { type: "string", description: "Optional working directory" },
        title: { type: "string", description: "Optional short title for the run" },
      },
      required: ["prompt"],
    },
  },
  {
    type: "function" as const,
    name: "confirm_action",
    description:
      "Execute the pending proposal. Only call this after the user explicitly confirmed the spoken echo with a clear yes.",
    parameters: {
      type: "object",
      properties: {
        token: { type: "string", description: "The proposal_token from the proposing call" },
      },
      required: ["token"],
    },
  },
  {
    type: "function" as const,
    name: "cancel_action",
    description: "Cancel the pending proposal. Use when the user declines or changes their mind.",
    parameters: { type: "object", properties: {}, required: [] },
  },
];

const SetCurrentArgs = z.object({ session: z.string().min(1) });
const ReadArgs = z.object({
  session: z.string().min(1).optional(),
  mode: z.enum(["summary", "full"]).optional(),
});
const ListArgs = z.object({ include_archived: z.boolean().optional() });
const SendArgs = z.object({ session: z.string().min(1).optional(), text: z.string().min(1) });
const RunArgs = z.object({
  prompt: z.string().min(1),
  provider: z.string().optional(),
  cwd: z.string().optional(),
  title: z.string().optional(),
});
const ConfirmArgs = z.object({ token: z.string().min(1) });

type Resolution =
  { ok: true; session: PaseoSession } | { ok: false; error: string; candidates?: string[] };

function resolveFrom(sessions: PaseoSession[], ref: string): Resolution {
  const needle = ref.trim().toLowerCase();
  const exact = sessions.filter(
    (s) => s.id.toLowerCase() === needle || s.shortId.toLowerCase() === needle,
  );
  const byPrefix = sessions.filter((s) => s.id.toLowerCase().startsWith(needle));
  const byName = sessions.filter((s) => s.name.toLowerCase().includes(needle));
  const matches = exact.length > 0 ? exact : byPrefix.length > 0 ? byPrefix : byName;
  if (matches.length === 1) return { ok: true, session: matches[0]! };
  if (matches.length === 0) {
    return { ok: false, error: `No session matches "${ref}"` };
  }
  return {
    ok: false,
    error: `"${ref}" is ambiguous`,
    candidates: matches.slice(0, 5).map((s) => `${s.name} (${s.shortId}, ${s.status})`),
  };
}

function sessionDigest(sessions: PaseoSession[]): string {
  if (sessions.length === 0) return "No sessions.";
  const byStatus = new Map<string, number>();
  for (const s of sessions) byStatus.set(s.status, (byStatus.get(s.status) ?? 0) + 1);
  const counts = [...byStatus.entries()].map(([status, n]) => `${n} ${status}`).join(", ");
  const names = sessions
    .slice(0, 8)
    .map((s) => `${s.name} (${s.status})`)
    .join("; ");
  return `${sessions.length} sessions: ${counts}. ${names}`;
}

export function createToolDispatcher(deps: ToolDeps): ToolDispatcher {
  const { paseo, gate, config, log } = deps;
  let current: { id: string; name: string } | null = null;

  const resolveTarget = async (ref: string | undefined): Promise<Resolution> => {
    if (ref === undefined) {
      if (current === null) {
        return {
          ok: false,
          error: "No current session set. Ask for list_sessions or name a session.",
        };
      }
      const sessions = await paseo.listSessions();
      const found = sessions.find((s) => s.id === current!.id);
      if (!found) {
        current = null;
        return { ok: false, error: "The current session no longer exists. Pick another." };
      }
      return { ok: true, session: found };
    }
    return resolveFrom(await paseo.listSessions(), ref);
  };

  const handlers: Record<string, (argsJson: string) => Promise<ToolResult>> = {
    async list_sessions(argsJson) {
      const args = ListArgs.parse(parseArgs(argsJson));
      const sessions = await paseo.listSessions(args.include_archived ?? false);
      return {
        ok: true,
        current_session: current,
        sessions: sessions.map((s) => ({
          id: s.id,
          short_id: s.shortId,
          title: s.name,
          provider: s.provider,
          status: s.status,
          cwd: s.cwd,
          created: s.created,
        })),
        spoken_hint: sessionDigest(sessions),
      };
    },

    async set_current_session(argsJson) {
      const args = SetCurrentArgs.parse(parseArgs(argsJson));
      const resolution = await resolveTarget(args.session);
      if (!resolution.ok) {
        return { ok: false, error: resolution.error, candidates: resolution.candidates ?? [] };
      }
      current = { id: resolution.session.id, name: resolution.session.name };
      return { ok: true, session_id: current.id, title: current.name };
    },

    async read_latest_reply(argsJson) {
      const args = ReadArgs.parse(parseArgs(argsJson));
      const resolution = await resolveTarget(args.session);
      if (!resolution.ok) {
        return { ok: false, error: resolution.error, candidates: resolution.candidates ?? [] };
      }
      const session = resolution.session;
      const mode = args.mode ?? "summary";
      // --filter text yields recent assistant messages as plain text. Message
      // boundaries are not machine-parsable, so full mode uses a small tail
      // as an approximation of "the latest reply".
      const tail = mode === "full" ? 2 : 6;
      let text = await paseo.readLogText(session.id, tail, true);
      if (text.length === 0) {
        text = await paseo.readLogText(session.id, config.logTailEntries, false);
      }
      if (text.length === 0) {
        return {
          ok: true,
          session_id: session.id,
          spoken_text: `Session ${session.name} has no readable output yet. It is ${session.status}.`,
          degraded: false,
        };
      }
      if (mode === "full" || text.length <= config.summariseThresholdChars) {
        return {
          ok: true,
          session_id: session.id,
          spoken_text: cleanForSpeech(text).slice(-2400),
          degraded: false,
        };
      }
      const summary = await deps.summarise({ text, sessionTitle: session.name });
      return {
        ok: true,
        session_id: session.id,
        spoken_text: summary.spokenText,
        degraded: summary.degraded,
        note: summary.degraded ? "Summariser offline; this is the raw tail." : undefined,
      };
    },

    async list_pending_permissions() {
      const permissions = await paseo.listPendingPermissions();
      return {
        ok: true,
        count: permissions.length,
        items: permissions.map((p) => p.description),
        spoken_hint:
          permissions.length === 0
            ? "Nothing is waiting on a permission."
            : `${permissions.length} permission request${permissions.length === 1 ? "" : "s"} waiting. Approvals happen in the Paseo UI, not by voice.`,
      };
    },

    async send_message(argsJson) {
      const args = SendArgs.parse(parseArgs(argsJson));
      const resolution = await resolveTarget(args.session);
      if (!resolution.ok) {
        return { ok: false, error: resolution.error, candidates: resolution.candidates ?? [] };
      }
      const session = resolution.session;
      const spokenEcho = `Send to ${session.name}: "${args.text}". Confirm?`;
      const proposal = gate.propose(
        {
          kind: "send_message",
          sessionId: session.id,
          sessionName: session.name,
          text: args.text,
        },
        spokenEcho,
      );
      return {
        ok: true,
        proposed: true,
        executed: false,
        proposal_token: proposal.token,
        spoken_echo: spokenEcho,
        expires_in_seconds: Math.round(config.proposalTtlMs / 1000),
      };
    },

    async start_run(argsJson) {
      const args = RunArgs.parse(parseArgs(argsJson));
      const where = args.cwd ? ` in ${args.cwd}` : "";
      const who = args.provider ? ` with ${args.provider}` : "";
      const spokenEcho = `Start a new agent${who}${where}: "${args.prompt}". Confirm?`;
      const proposal = gate.propose(
        {
          kind: "start_run",
          prompt: args.prompt,
          ...(args.provider !== undefined ? { provider: args.provider } : {}),
          ...(args.cwd !== undefined ? { cwd: args.cwd } : {}),
          ...(args.title !== undefined ? { title: args.title } : {}),
        },
        spokenEcho,
      );
      return {
        ok: true,
        proposed: true,
        executed: false,
        proposal_token: proposal.token,
        spoken_echo: spokenEcho,
        expires_in_seconds: Math.round(config.proposalTtlMs / 1000),
      };
    },

    async confirm_action(argsJson) {
      const args = ConfirmArgs.parse(parseArgs(argsJson));
      const result = gate.confirm(args.token);
      if (!result.ok) {
        const reasons: Record<string, string> = {
          no_pending: "There is no pending proposal.",
          expired: "The proposal expired. Propose it again if still wanted.",
          wrong_token: "That token does not match the pending proposal.",
          already_used: "That proposal was already executed.",
        };
        return { ok: false, executed: false, error: reasons[result.reason] ?? result.reason };
      }
      const action = result.action;
      try {
        if (action.kind === "send_message") {
          await paseo.sendMessage(action.sessionId, action.text);
          return {
            ok: true,
            executed: true,
            spoken_hint: `Sent to ${action.sessionName}.`,
          };
        }
        const runOpts: { provider?: string; cwd?: string; title?: string } = {};
        if (action.provider !== undefined) runOpts.provider = action.provider;
        if (action.cwd !== undefined) runOpts.cwd = action.cwd;
        if (action.title !== undefined) runOpts.title = action.title;
        await paseo.startRun(action.prompt, runOpts);
        return { ok: true, executed: true, spoken_hint: "Run started." };
      } catch (err) {
        const message =
          err instanceof PaseoCliError
            ? `${err.message}`
            : err instanceof Error
              ? err.message
              : String(err);
        log.error("confirmed action failed", { kind: action.kind, error: message });
        return { ok: false, executed: false, error: `Execution failed: ${message}` };
      }
    },

    async cancel_action() {
      const had = gate.cancel();
      return { ok: true, cancelled: had };
    },
  };

  return {
    async dispatch(name, argsJson) {
      const handler = handlers[name];
      if (!handler) {
        return { ok: false, error: `Unknown tool: ${name}` };
      }
      try {
        return await handler(argsJson);
      } catch (err) {
        if (err instanceof z.ZodError) {
          return {
            ok: false,
            error: `Invalid arguments for ${name}: ${err.issues.map((i) => i.message).join("; ")}`,
          };
        }
        const message =
          err instanceof PaseoCliError
            ? `${err.message} (${err.code})`
            : err instanceof Error
              ? err.message
              : String(err);
        log.error("tool dispatch failed", { tool: name, error: message });
        return { ok: false, error: message };
      }
    },
    currentSession: () => current,
  };
}

function parseArgs(argsJson: string): unknown {
  if (argsJson.trim().length === 0) return {};
  try {
    return JSON.parse(argsJson);
  } catch {
    throw new z.ZodError([{ code: "custom", message: "arguments were not valid JSON", path: [] }]);
  }
}
