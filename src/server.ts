import { createReadStream, existsSync } from "node:fs";
import type { IncomingMessage, Server, ServerResponse } from "node:http";
import { createServer as createHttpServer } from "node:http";
import { extname, join, normalize, resolve } from "node:path";
import { WebSocketServer, type WebSocket } from "ws";
import type { Config } from "./config.js";
import type { Logger } from "./log.js";
import type { RealtimeCallbacks } from "./realtime.js";
import type { ProposalStore } from "./gate.js";
import type { VoiceSessionLike } from "./mock-realtime.js";

/**
 * The broker's client-facing server: static files for the PTT page plus the
 * /ws bridge between browser frames and a voice session (real OpenAI or
 * mock). One voice session per client connection; the gate and dispatcher
 * are per connection too (single-user tool, but no shared-proposal surprises
 * if two tabs are open).
 */

export interface VoiceWiring {
  /** Builds a voice session plus the gate backing its dispatcher. */
  createSession(callbacks: RealtimeCallbacks): { session: VoiceSessionLike; gate: ProposalStore };
  mode: "real" | "mock";
}

export interface BrokerServerOptions {
  config: Config;
  log: Logger;
  wiring: VoiceWiring;
  publicDir?: string;
}

const CONTENT_TYPES: Record<string, string> = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".svg": "image/svg+xml",
  ".png": "image/png",
  ".ico": "image/x-icon",
};

function serveStatic(publicDir: string, req: IncomingMessage, res: ServerResponse): void {
  const url = new URL(req.url ?? "/", "http://localhost");
  const requested = url.pathname === "/" ? "/index.html" : url.pathname;
  const safePath = normalize(requested).replace(/^(\.\.[/\\])+/, "");
  const filePath = resolve(join(publicDir, safePath));
  if (!filePath.startsWith(resolve(publicDir)) || !existsSync(filePath)) {
    res.writeHead(404, { "Content-Type": "text/plain" });
    res.end("not found");
    return;
  }
  res.writeHead(200, {
    "Content-Type": CONTENT_TYPES[extname(filePath)] ?? "application/octet-stream",
    "Cache-Control": "no-store",
  });
  createReadStream(filePath).pipe(res);
}

interface ClientMessage {
  type?: string;
  text?: string;
}

function wireClient(client: WebSocket, wiring: VoiceWiring, log: Logger): void {
  const sendJson = (payload: Record<string, unknown>) => {
    if (client.readyState === client.OPEN) client.send(JSON.stringify(payload));
  };
  const sendBinary = (chunk: Buffer) => {
    if (client.readyState === client.OPEN) client.send(chunk, { binary: true });
  };

  let gateRef: ProposalStore | null = null;
  const pushProposal = () => {
    sendJson({ type: "proposal", echo: gateRef?.pending()?.spokenEcho ?? null });
  };

  const callbacks: RealtimeCallbacks = {
    onAudio: (chunk) => sendBinary(chunk),
    onTranscriptDelta: (text) => sendJson({ type: "transcript_delta", text }),
    onTranscriptDone: (text) => {
      sendJson({ type: "transcript_done", text });
      pushProposal();
    },
    onUserTranscript: (text) => sendJson({ type: "user_transcript", text }),
    onToolCall: (name) => sendJson({ type: "tool", name, phase: "call" }),
    onToolResult: (name, ok) => {
      sendJson({ type: "tool", name, phase: ok ? "ok" : "error" });
      pushProposal();
    },
    onStateChange: (state, detail) => sendJson({ type: "state", state, detail: detail ?? null }),
    onFlushPlayback: () => sendJson({ type: "flush_audio" }),
    onError: (message) => sendJson({ type: "error", message }),
  };

  const { session, gate } = wiring.createSession(callbacks);
  gateRef = gate;
  sendJson({ type: "mode", mode: wiring.mode });

  client.on("message", (data, isBinary) => {
    if (isBinary) {
      session.appendAudio(Buffer.isBuffer(data) ? data : Buffer.from(data as ArrayBuffer));
      return;
    }
    let message: ClientMessage;
    try {
      message = JSON.parse(data.toString()) as ClientMessage;
    } catch {
      log.warn("client sent invalid JSON frame");
      return;
    }
    switch (message.type) {
      case "hello":
        sendJson({ type: "mode", mode: wiring.mode });
        return;
      case "ptt_start":
        session.pttStart();
        return;
      case "ptt_end":
        session.pttEnd();
        return;
      case "text_turn":
        if (typeof message.text === "string" && message.text.length > 0) {
          session.sendTextTurn(message.text);
        }
        return;
      default:
        log.warn("client sent unknown control frame", { frameType: message.type ?? "none" });
    }
  });

  client.on("close", () => {
    session.close();
  });
}

export function createBrokerServer(options: BrokerServerOptions): Server {
  const { config, log, wiring } = options;
  const publicDir = options.publicDir ?? join(import.meta.dirname, "..", "public");

  const server = createHttpServer((req, res) => {
    if (req.url === "/healthz") {
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ ok: true, mode: wiring.mode }));
      return;
    }
    serveStatic(publicDir, req, res);
  });

  const wss = new WebSocketServer({ server, path: "/ws" });
  wss.on("connection", (client) => {
    log.info("client connected");
    wireClient(client, wiring, log);
  });

  server.listen(config.listenPort, config.listenHost, () => {
    log.info("broker listening", {
      host: config.listenHost,
      port: config.listenPort,
      mode: wiring.mode,
    });
  });

  return server;
}
