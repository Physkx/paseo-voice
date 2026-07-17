import type { ToolDispatcher, ToolResult } from "./tools.js";

/**
 * Text command parser shared by the terminal console and the MOCK voice
 * session. In real voice mode the realtime model does this mapping; here it
 * is deterministic so the tool loop can be exercised end to end without
 * OpenAI. Returns a speakable reply string.
 */
export interface CommandContext {
  dispatch: ToolDispatcher["dispatch"];
  lastProposalToken: string | null;
}

const HELP = [
  "Commands: ls | use <session> | read [session] | full [session] | perms |",
  "send [session =] <text> | run <prompt> | yes | no | help",
].join(" ");

function formatResult(result: ToolResult, fallback: string): string {
  if (!result.ok) {
    const candidates = Array.isArray(result["candidates"]) ? result["candidates"] : [];
    const suffix = candidates.length > 0 ? ` Candidates: ${candidates.join("; ")}` : "";
    return `Error: ${String(result["error"] ?? "unknown")}.${suffix}`;
  }
  return fallback;
}

export async function runCommand(input: string, ctx: CommandContext): Promise<string> {
  const line = input.trim();
  if (line.length === 0) return HELP;
  const lower = line.toLowerCase();

  if (lower === "help" || lower === "?") return HELP;

  if (lower === "ls" || lower === "list" || lower === "sessions") {
    const result = await ctx.dispatch("list_sessions", "{}");
    return formatResult(result, String(result["spoken_hint"] ?? "Done."));
  }

  if (lower.startsWith("use ")) {
    const result = await ctx.dispatch(
      "set_current_session",
      JSON.stringify({ session: line.slice(4).trim() }),
    );
    return formatResult(result, `Current session: ${String(result["title"] ?? "set")}.`);
  }

  if (
    lower === "read" ||
    lower.startsWith("read ") ||
    lower === "full" ||
    lower.startsWith("full ")
  ) {
    const isFull = lower === "full" || lower.startsWith("full ");
    const rest = line.slice(4).trim();
    const args: Record<string, unknown> = { mode: isFull ? "full" : "summary" };
    if (rest.length > 0) args["session"] = rest;
    const result = await ctx.dispatch("read_latest_reply", JSON.stringify(args));
    const note = result["degraded"] === true ? " (summariser offline, raw tail)" : "";
    return formatResult(result, `${String(result["spoken_text"] ?? "")}${note}`);
  }

  if (lower === "perms" || lower === "permissions") {
    const result = await ctx.dispatch("list_pending_permissions", "{}");
    const items = Array.isArray(result["items"]) ? (result["items"] as string[]) : [];
    const list = items.length > 0 ? ` ${items.join(". ")}` : "";
    return formatResult(result, `${String(result["spoken_hint"] ?? "")}${list}`);
  }

  if (lower.startsWith("send ")) {
    const rest = line.slice(5).trim();
    const eq = rest.indexOf("=");
    const args =
      eq > 0
        ? { session: rest.slice(0, eq).trim(), text: rest.slice(eq + 1).trim() }
        : { text: rest };
    if (args.text.length === 0) return "Error: nothing to send.";
    const result = await ctx.dispatch("send_message", JSON.stringify(args));
    if (result.ok) ctx.lastProposalToken = String(result["proposal_token"]);
    return formatResult(result, `${String(result["spoken_echo"] ?? "Proposed.")} (yes/no)`);
  }

  if (lower.startsWith("run ")) {
    const result = await ctx.dispatch(
      "start_run",
      JSON.stringify({ prompt: line.slice(4).trim() }),
    );
    if (result.ok) ctx.lastProposalToken = String(result["proposal_token"]);
    return formatResult(result, `${String(result["spoken_echo"] ?? "Proposed.")} (yes/no)`);
  }

  if (lower === "yes" || lower === "y" || lower === "confirm") {
    if (ctx.lastProposalToken === null) return "Nothing to confirm.";
    const token = ctx.lastProposalToken;
    ctx.lastProposalToken = null;
    const result = await ctx.dispatch("confirm_action", JSON.stringify({ token }));
    return formatResult(result, String(result["spoken_hint"] ?? "Done."));
  }

  if (lower === "no" || lower === "n" || lower === "cancel") {
    ctx.lastProposalToken = null;
    const result = await ctx.dispatch("cancel_action", "{}");
    return formatResult(result, result["cancelled"] === true ? "Cancelled." : "Nothing pending.");
  }

  return `Unrecognised: "${line}". ${HELP}`;
}
