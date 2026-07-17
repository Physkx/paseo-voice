import { readFile } from "node:fs/promises";
import { createInterface } from "node:readline/promises";
import { runCommand, type CommandContext } from "./commands.js";
import { loadConfig } from "./config.js";
import { realExec } from "./exec.js";
import { createProposalStore } from "./gate.js";
import { createLogger } from "./log.js";
import { loadSecrets } from "./secrets.js";
import { summarise } from "./summarise.js";
import { createToolDispatcher } from "./tools.js";
import { buildPaseoClient } from "./wiring.js";

/**
 * Terminal REPL over the exact tool loop the voice model uses, minus audio
 * and OpenAI. Useful for smoke testing against the live daemon and for
 * driving the gate flow by hand.
 */
async function main(): Promise<void> {
  const config = await loadConfig({ env: process.env, readFile: (p) => readFile(p, "utf8") });
  const log = createLogger("warn");
  const secrets = await loadSecrets(config, {
    execFile: realExec,
    readFile: (p) => readFile(p, "utf8"),
    env: process.env,
    log,
  });
  const paseo = buildPaseoClient(config, secrets, log);
  const gate = createProposalStore(config.proposalTtlMs);
  const dispatcher = createToolDispatcher({
    paseo,
    gate,
    summarise: (input) => summarise(input, { fetchFn: fetch, config, log }),
    config,
    log,
  });
  const ctx: CommandContext = { dispatch: dispatcher.dispatch, lastProposalToken: null };

  process.stdout.write("paseo-voice console. help for commands, exit to quit.\n");
  const rl = createInterface({ input: process.stdin, output: process.stdout });
  for (;;) {
    let line: string;
    try {
      line = await rl.question("> ");
    } catch {
      break;
    }
    const trimmed = line.trim().toLowerCase();
    if (trimmed === "exit" || trimmed === "quit") break;
    const reply = await runCommand(line, ctx);
    process.stdout.write(reply + "\n");
  }
  rl.close();
}

main().catch((err) => {
  process.stderr.write(`console failed: ${err instanceof Error ? err.message : String(err)}\n`);
  process.exitCode = 1;
});
