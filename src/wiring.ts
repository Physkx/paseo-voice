import type { Config } from "./config.js";
import { createProposalStore } from "./gate.js";
import { buildInstructions } from "./instructions.js";
import type { Logger } from "./log.js";
import { createMockVoiceSession } from "./mock-realtime.js";
import type { PaseoClient } from "./paseo.js";
import { PaseoCliError, createPaseoClient } from "./paseo.js";
import { createRealtimeSession } from "./realtime.js";
import type { Secrets } from "./secrets.js";
import type { VoiceWiring } from "./server.js";
import { summarise } from "./summarise.js";
import { createToolDispatcher } from "./tools.js";
import { realExec } from "./exec.js";
import { wsSocketFactory } from "./ws-socket.js";

/** Builds the Paseo client, or a stub that explains what is missing. */
export function buildPaseoClient(config: Config, secrets: Secrets, log: Logger): PaseoClient {
  if (secrets.paseoPassword !== null && secrets.paseoPassword.length > 0) {
    return createPaseoClient({
      exec: realExec,
      password: secrets.paseoPassword,
      bin: config.paseoBin,
      env: process.env,
    });
  }
  log.warn("paseo password unresolved; paseo tools will report themselves unavailable");
  const unavailable = async (): Promise<never> => {
    throw new PaseoCliError(
      "Paseo access is not configured yet. Configure PASEO_PASSWORD with the selected secret provider.",
      "NO_PASSWORD",
    );
  };
  return {
    listSessions: unavailable,
    readLogText: unavailable,
    inspect: unavailable,
    listPendingPermissions: unavailable,
    sendMessage: unavailable,
    startRun: unavailable,
  };
}

export function buildVoiceWiring(config: Config, secrets: Secrets, log: Logger): VoiceWiring {
  const paseo = buildPaseoClient(config, secrets, log);
  const summariseFn = (input: { text: string; sessionTitle?: string }) =>
    summarise(input, { fetchFn: fetch, config, log });
  const mode: "real" | "mock" =
    secrets.openaiApiKey !== null && !config.forceMock ? "real" : "mock";
  const apiKey = secrets.openaiApiKey ?? "";

  return {
    mode,
    createSession(callbacks) {
      const gate = createProposalStore(config.proposalTtlMs);
      const dispatcher = createToolDispatcher({
        paseo,
        gate,
        summarise: summariseFn,
        config,
        log,
      });
      if (mode === "real") {
        const session = createRealtimeSession({
          apiKey,
          config,
          dispatcher,
          callbacks,
          socketFactory: wsSocketFactory,
          log,
          instructions: buildInstructions(),
        });
        return { session, gate };
      }
      return { session: createMockVoiceSession(dispatcher, callbacks), gate };
    },
  };
}
