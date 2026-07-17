import { describe, expect, it } from "vitest";
import { loadConfig } from "../src/config.js";
import { createProposalStore } from "../src/gate.js";
import { nullLogger } from "../src/log.js";
import type { PaseoClient, PaseoSession } from "../src/paseo.js";
import { TOOL_DEFINITIONS, createToolDispatcher } from "../src/tools.js";

const config = await loadConfig({
  env: {},
  readFile: async () => {
    throw new Error("no file");
  },
});

const sessions: PaseoSession[] = [
  {
    id: "aaaa1111-0000-0000-0000-000000000000",
    shortId: "aaaa111",
    name: "Fix the auth tests",
    provider: "claude/claude-fable-5",
    thinking: "max",
    status: "running",
    cwd: "~/dev/auth",
    created: "1 hour ago",
  },
  {
    id: "bbbb2222-0000-0000-0000-000000000000",
    shortId: "bbbb222",
    name: "Refactor the API layer",
    provider: "codex/gpt-5.4",
    thinking: null,
    status: "idle",
    cwd: "~/dev/api",
    created: "2 hours ago",
  },
];

interface FakeState {
  sent: Array<{ id: string; text: string }>;
  runs: Array<{ prompt: string; opts: unknown }>;
  logText: string;
  summariseCalls: number;
}

function makeFixture(logText = "short reply") {
  const state: FakeState = { sent: [], runs: [], logText, summariseCalls: 0 };
  const paseo: PaseoClient = {
    listSessions: async () => sessions,
    readLogText: async () => state.logText,
    inspect: async () => ({}),
    listPendingPermissions: async () => [],
    sendMessage: async (id, text) => {
      state.sent.push({ id, text });
      return "{}";
    },
    startRun: async (prompt, opts) => {
      state.runs.push({ prompt, opts });
      return "{}";
    },
  };
  const gate = createProposalStore(config.proposalTtlMs);
  const dispatcher = createToolDispatcher({
    paseo,
    gate,
    config,
    log: nullLogger,
    summarise: async () => {
      state.summariseCalls += 1;
      return { spokenText: "A short brief.", degraded: false };
    },
  });
  return { dispatcher, state };
}

describe("tool definitions", () => {
  it("exposes exactly the expected tool names", () => {
    expect(TOOL_DEFINITIONS.map((t) => t.name).sort()).toEqual([
      "cancel_action",
      "confirm_action",
      "list_pending_permissions",
      "list_sessions",
      "read_latest_reply",
      "send_message",
      "set_current_session",
      "start_run",
    ]);
  });

  it("has no permission approval tool", () => {
    const joined = JSON.stringify(TOOL_DEFINITIONS);
    expect(joined).not.toMatch(/permit_allow|approve_permission|allow_permission/);
  });
});

describe("createToolDispatcher", () => {
  it("list_sessions returns rows and a spoken digest", async () => {
    const { dispatcher } = makeFixture();
    const result = await dispatcher.dispatch("list_sessions", "{}");
    expect(result.ok).toBe(true);
    expect(result["sessions"]).toHaveLength(2);
    expect(String(result["spoken_hint"])).toContain("2 sessions");
  });

  it("set_current_session resolves a title fragment", async () => {
    const { dispatcher } = makeFixture();
    const result = await dispatcher.dispatch("set_current_session", '{"session": "auth"}');
    expect(result).toMatchObject({ ok: true, title: "Fix the auth tests" });
    expect(dispatcher.currentSession()?.name).toBe("Fix the auth tests");
  });

  it("set_current_session reports ambiguity with candidates", async () => {
    const { dispatcher } = makeFixture();
    const result = await dispatcher.dispatch("set_current_session", '{"session": "the"}');
    expect(result.ok).toBe(false);
    expect(result["candidates"]).toHaveLength(2);
  });

  it("read_latest_reply speaks short text directly without summarising", async () => {
    const { dispatcher, state } = makeFixture("Tests pass. Nothing pending.");
    await dispatcher.dispatch("set_current_session", '{"session": "auth"}');
    const result = await dispatcher.dispatch("read_latest_reply", "{}");
    expect(result.ok).toBe(true);
    expect(String(result["spoken_text"])).toContain("Tests pass");
    expect(state.summariseCalls).toBe(0);
  });

  it("read_latest_reply summarises long text", async () => {
    const { dispatcher, state } = makeFixture("x".repeat(2000));
    const result = await dispatcher.dispatch("read_latest_reply", '{"session": "api"}');
    expect(result.ok).toBe(true);
    expect(result["spoken_text"]).toBe("A short brief.");
    expect(state.summariseCalls).toBe(1);
  });

  it("read_latest_reply mode=full never summarises", async () => {
    const { dispatcher, state } = makeFixture("y".repeat(2000));
    const result = await dispatcher.dispatch(
      "read_latest_reply",
      '{"session": "api", "mode": "full"}',
    );
    expect(result.ok).toBe(true);
    expect(state.summariseCalls).toBe(0);
    expect(String(result["spoken_text"]).length).toBeLessThanOrEqual(2400);
  });

  it("read_latest_reply without a current session asks for one", async () => {
    const { dispatcher } = makeFixture();
    const result = await dispatcher.dispatch("read_latest_reply", "{}");
    expect(result.ok).toBe(false);
    expect(String(result["error"])).toContain("No current session");
  });

  it("send_message proposes and does not execute", async () => {
    const { dispatcher, state } = makeFixture();
    const result = await dispatcher.dispatch(
      "send_message",
      '{"session": "auth", "text": "also run the linter"}',
    );
    expect(result).toMatchObject({ ok: true, proposed: true, executed: false });
    expect(String(result["spoken_echo"])).toContain("also run the linter");
    expect(state.sent).toHaveLength(0);
  });

  it("confirm_action executes the proposed send exactly once", async () => {
    const { dispatcher, state } = makeFixture();
    const proposal = await dispatcher.dispatch(
      "send_message",
      '{"session": "auth", "text": "also run the linter"}',
    );
    const token = String(proposal["proposal_token"]);
    const confirmed = await dispatcher.dispatch("confirm_action", JSON.stringify({ token }));
    expect(confirmed).toMatchObject({ ok: true, executed: true });
    expect(state.sent).toEqual([
      { id: "aaaa1111-0000-0000-0000-000000000000", text: "also run the linter" },
    ]);
    const again = await dispatcher.dispatch("confirm_action", JSON.stringify({ token }));
    expect(again).toMatchObject({ ok: false, executed: false });
    expect(state.sent).toHaveLength(1);
  });

  it("confirm_action with wrong token executes nothing", async () => {
    const { dispatcher, state } = makeFixture();
    await dispatcher.dispatch("send_message", '{"session": "auth", "text": "risky"}');
    const result = await dispatcher.dispatch("confirm_action", '{"token": "nope"}');
    expect(result.ok).toBe(false);
    expect(state.sent).toHaveLength(0);
  });

  it("start_run proposes then confirm executes with options", async () => {
    const { dispatcher, state } = makeFixture();
    const proposal = await dispatcher.dispatch(
      "start_run",
      '{"prompt": "fix tests", "provider": "codex", "cwd": "~/dev/repo"}',
    );
    expect(state.runs).toHaveLength(0);
    const token = String(proposal["proposal_token"]);
    await dispatcher.dispatch("confirm_action", JSON.stringify({ token }));
    expect(state.runs).toEqual([
      { prompt: "fix tests", opts: { provider: "codex", cwd: "~/dev/repo" } },
    ]);
  });

  it("cancel_action clears the pending proposal", async () => {
    const { dispatcher, state } = makeFixture();
    const proposal = await dispatcher.dispatch("send_message", '{"session": "auth", "text": "x"}');
    await dispatcher.dispatch("cancel_action", "{}");
    const confirmed = await dispatcher.dispatch(
      "confirm_action",
      JSON.stringify({ token: String(proposal["proposal_token"]) }),
    );
    expect(confirmed.ok).toBe(false);
    expect(state.sent).toHaveLength(0);
  });

  it("unknown tool and malformed args return structured errors", async () => {
    const { dispatcher } = makeFixture();
    expect((await dispatcher.dispatch("nuke_everything", "{}")).ok).toBe(false);
    const bad = await dispatcher.dispatch("send_message", "not json");
    expect(bad.ok).toBe(false);
    expect(String(bad["error"])).toContain("send_message");
  });

  it("list_pending_permissions narrates without any approval path", async () => {
    const { dispatcher } = makeFixture();
    const result = await dispatcher.dispatch("list_pending_permissions", "{}");
    expect(result).toMatchObject({ ok: true, count: 0 });
    expect(String(result["spoken_hint"])).toContain("Nothing is waiting");
  });
});
