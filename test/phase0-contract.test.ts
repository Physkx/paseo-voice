import { describe, expect, it } from "vitest";
import { loadConfig } from "../src/config.js";
import { createProposalStore } from "../src/gate.js";
import { nullLogger } from "../src/log.js";
import type { PaseoClient, PaseoSession } from "../src/paseo.js";
import { createToolDispatcher } from "../src/tools.js";

const sessions: PaseoSession[] = [
  {
    id: "aaaaaaaa-0000-0000-0000-000000000000",
    shortId: "aaaaaaa",
    name: "Alpha task",
    provider: "codex/example",
    thinking: null,
    status: "idle",
    cwd: "~/dev/alpha",
    created: "now",
  },
  {
    id: "bbbbbbbb-0000-0000-0000-000000000000",
    shortId: "bbbbbbb",
    name: "Beta task",
    provider: "claude/example",
    thinking: null,
    status: "idle",
    cwd: "~/dev/beta",
    created: "now",
  },
];

async function fixture(sendMessage?: PaseoClient["sendMessage"]) {
  const config = await loadConfig({
    env: {},
    readFile: async () => {
      throw new Error("no config file");
    },
  });
  const sent: Array<{ id: string; text: string }> = [];
  const paseo: PaseoClient = {
    listSessions: async () => sessions,
    readLogText: async () => "",
    inspect: async () => ({}),
    listPendingPermissions: async () => [],
    sendMessage:
      sendMessage ??
      (async (id, text) => {
        sent.push({ id, text });
        return '{"status":"sent"}';
      }),
    startRun: async () => "{}",
  };
  const dispatcher = createToolDispatcher({
    paseo,
    gate: createProposalStore(config.proposalTtlMs),
    summarise: async () => ({ spokenText: "summary", degraded: false }),
    config,
    log: nullLogger,
  });
  return { dispatcher, sent };
}

describe("Phase 0 TypeScript characterization", () => {
  it("freezes the resolved destination and exact response text inside a proposal", async () => {
    const { dispatcher, sent } = await fixture();
    const text = "  ngā mihi\nrun the tests  ";
    const proposal = await dispatcher.dispatch(
      "send_message",
      JSON.stringify({ session: "Alpha", text }),
    );

    await dispatcher.dispatch("set_current_session", '{"session":"Beta"}');
    const result = await dispatcher.dispatch(
      "confirm_action",
      JSON.stringify({ token: proposal["proposal_token"] }),
    );

    expect(result).toMatchObject({ ok: true, executed: true });
    expect(sent).toEqual([{ id: sessions[0]!.id, text }]);
  });

  it("invalidates a replaced proposal without executing it", async () => {
    const { dispatcher, sent } = await fixture();
    const first = await dispatcher.dispatch("send_message", '{"session":"Alpha","text":"first"}');
    const second = await dispatcher.dispatch("send_message", '{"session":"Beta","text":"second"}');

    const replaced = await dispatcher.dispatch(
      "confirm_action",
      JSON.stringify({ token: first["proposal_token"] }),
    );
    expect(replaced).toMatchObject({ ok: false, executed: false });
    expect(sent).toEqual([]);

    await dispatcher.dispatch(
      "confirm_action",
      JSON.stringify({ token: second["proposal_token"] }),
    );
    expect(sent).toEqual([{ id: sessions[1]!.id, text: "second" }]);
  });

  it("does not automatically retry after an adapter error with an ambiguous outcome", async () => {
    const calls: Array<{ id: string; text: string }> = [];
    const { dispatcher } = await fixture(async (id, text) => {
      calls.push({ id, text });
      throw new Error("connection ended before acknowledgement");
    });
    const proposal = await dispatcher.dispatch(
      "send_message",
      '{"session":"Alpha","text":"run checks"}',
    );
    const confirmation = JSON.stringify({ token: proposal["proposal_token"] });

    const first = await dispatcher.dispatch("confirm_action", confirmation);
    const replay = await dispatcher.dispatch("confirm_action", confirmation);

    expect(first).toMatchObject({ ok: false, executed: false });
    expect(replay).toMatchObject({ ok: false, executed: false });
    expect(calls).toEqual([{ id: sessions[0]!.id, text: "run checks" }]);
  });
});
