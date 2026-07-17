import { readFile } from "node:fs/promises";
import { describeConfig, loadConfig } from "./config.js";
import { realExec } from "./exec.js";
import { createLogger } from "./log.js";
import { loadSecrets } from "./secrets.js";
import { createBrokerServer } from "./server.js";
import { buildVoiceWiring } from "./wiring.js";

async function main(): Promise<void> {
  const config = await loadConfig({ env: process.env, readFile: (p) => readFile(p, "utf8") });
  const log = createLogger(config.logLevel);
  log.info("paseo-voice starting", describeConfig(config));

  const secrets = await loadSecrets(config, {
    execFile: realExec,
    readFile: (p) => readFile(p, "utf8"),
    env: process.env,
    log,
  });

  const wiring = buildVoiceWiring(config, secrets, log);
  if (wiring.mode === "mock") {
    log.warn("running in MOCK mode: no OpenAI key resolved; text turns only");
  }

  const server = createBrokerServer({ config, log, wiring });

  const shutdown = (signal: string) => {
    log.info("shutting down", { signal });
    server.close(() => process.exit(0));
    setTimeout(() => process.exit(0), 1500).unref();
  };
  process.on("SIGINT", () => shutdown("SIGINT"));
  process.on("SIGTERM", () => shutdown("SIGTERM"));
}

main().catch((err) => {
  process.stderr.write(
    `paseo-voice failed to start: ${err instanceof Error ? err.message : String(err)}\n`,
  );
  process.exitCode = 1;
});
