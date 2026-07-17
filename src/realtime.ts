import type { Config } from "./config.js";
import type { Logger } from "./log.js";
import type { ToolDispatcher } from "./tools.js";
import { TOOL_DEFINITIONS } from "./tools.js";

/**
 * Bridge to the OpenAI Realtime API over WebSocket (GA wire format).
 *
 * Verified against the current API reference on 2026-07-15 via the indexed
 * docs at developers.openai.com/api/reference/resources/realtime:
 * - connect: wss://api.openai.com/v1/realtime?model=<id>, Authorization: Bearer <key>.
 *   Current model ids include gpt-realtime, gpt-realtime-1.5, gpt-realtime-2,
 *   gpt-realtime-2.1 (the guide's WebSocket examples use gpt-realtime-2.1).
 * - session.update: { type: "session.update", session: { type: "realtime",
 *   instructions, tools, tool_choice, output_modalities, audio: { input: {
 *   format: { type: "audio/pcm", rate: 24000 }, turn_detection }, output: {
 *   format, voice, speed } } } }. turn_detection: null disables server VAD
 *   (manual PTT commits).
 * - client audio: input_audio_buffer.append { audio: <base64 pcm16 24k mono> },
 *   then input_audio_buffer.commit and response.create on PTT release.
 * - server events handled: session.created/updated, input_audio_buffer.committed,
 *   response.created, response.output_audio.delta (beta name response.audio.delta
 *   accepted defensively), response.output_audio_transcript.delta/.done,
 *   response.output_item.added/.done, response.function_call_arguments.done,
 *   response.done, error.
 * - function call results: conversation.item.create with an item
 *   { type: "function_call_output", call_id, output } followed by
 *   response.create (deferred until the active response finishes).
 * - barge-in: response.cancel while a response is active.
 */

export interface RealtimeSocket {
  send(data: string): void;
  close(): void;
  onOpen(cb: () => void): void;
  onMessage(cb: (data: string) => void): void;
  onClose(cb: (code: number, reason: string) => void): void;
  onError(cb: (err: Error) => void): void;
}

export type SocketFactory = (url: string, headers: Record<string, string>) => RealtimeSocket;

export type RealtimeState = "connecting" | "ready" | "responding" | "closed" | "error";

export interface RealtimeCallbacks {
  onAudio(chunk: Buffer): void;
  onTranscriptDelta(text: string): void;
  onTranscriptDone(text: string): void;
  onUserTranscript?(text: string): void;
  onToolCall(name: string): void;
  onToolResult(name: string, ok: boolean): void;
  onStateChange(state: RealtimeState, detail?: string): void;
  /** Client playback must flush immediately (barge-in). */
  onFlushPlayback(): void;
  onError(message: string): void;
}

export interface RealtimeSessionOptions {
  apiKey: string;
  config: Config;
  dispatcher: ToolDispatcher;
  callbacks: RealtimeCallbacks;
  socketFactory: SocketFactory;
  log: Logger;
  instructions: string;
}

export interface RealtimeSession {
  appendAudio(pcm: Buffer): void;
  pttStart(): void;
  pttEnd(): void;
  sendTextTurn(text: string): void;
  close(): void;
  state(): RealtimeState;
}

export function createRealtimeSession(options: RealtimeSessionOptions): RealtimeSession {
  const { apiKey, config, dispatcher, callbacks, socketFactory, log, instructions } = options;

  const url = `${config.openaiBaseUrl}?model=${encodeURIComponent(config.openaiModel)}`;
  const socket = socketFactory(url, { Authorization: `Bearer ${apiKey}` });

  let state: RealtimeState = "connecting";
  let activeResponseId: string | null = null;
  let followupQueued = false;
  let closedByUs = false;
  /** function_call item name lookup: call_id -> name, from output_item events. */
  const callNames = new Map<string, string>();
  /** A Realtime call ID is dispatched once even if completion is replayed. */
  const dispatchedCallIds = new Set<string>();

  const setState = (next: RealtimeState, detail?: string) => {
    if (state === next) return;
    state = next;
    callbacks.onStateChange(next, detail);
  };

  const send = (event: Record<string, unknown>) => {
    try {
      socket.send(JSON.stringify(event));
    } catch (err) {
      log.error("realtime send failed", {
        type: String(event["type"]),
        error: err instanceof Error ? err.message : String(err),
      });
    }
  };

  const sendSessionUpdate = () => {
    send({
      type: "session.update",
      session: {
        type: "realtime",
        instructions,
        tools: TOOL_DEFINITIONS,
        tool_choice: "auto",
        output_modalities: ["audio"],
        audio: {
          input: {
            format: { type: "audio/pcm", rate: 24000 },
            turn_detection: null,
          },
          output: {
            format: { type: "audio/pcm", rate: 24000 },
            voice: config.openaiVoice,
          },
        },
      },
    });
  };

  const createResponse = () => {
    followupQueued = false;
    send({ type: "response.create" });
  };

  const handleFunctionCall = async (callId: string, name: string, argsJson: string) => {
    if (dispatchedCallIds.has(callId)) {
      log.warn("realtime: duplicate function call ignored", { callId });
      return;
    }
    dispatchedCallIds.add(callId);
    callbacks.onToolCall(name);
    const result = await dispatcher.dispatch(name, argsJson);
    callbacks.onToolResult(name, result.ok);
    send({
      type: "conversation.item.create",
      item: { type: "function_call_output", call_id: callId, output: JSON.stringify(result) },
    });
    if (activeResponseId === null) {
      createResponse();
    } else {
      followupQueued = true;
    }
  };

  const asRecord = (value: unknown): Record<string, unknown> | null =>
    value !== null && typeof value === "object" && !Array.isArray(value)
      ? (value as Record<string, unknown>)
      : null;

  const handleServerEvent = (raw: string) => {
    let event: Record<string, unknown> | null = null;
    try {
      event = asRecord(JSON.parse(raw));
    } catch {
      log.warn("realtime: non-JSON server frame", { snippet: raw.slice(0, 120) });
      return;
    }
    if (!event) return;
    const type = String(event["type"] ?? "");

    switch (type) {
      case "session.created":
      case "session.updated": {
        setState("ready");
        return;
      }
      case "response.created": {
        const response = asRecord(event["response"]);
        activeResponseId = String(response?.["id"] ?? "unknown");
        setState("responding");
        return;
      }
      case "response.output_audio.delta":
      case "response.audio.delta": {
        const delta = event["delta"];
        if (typeof delta === "string" && delta.length > 0) {
          callbacks.onAudio(Buffer.from(delta, "base64"));
        }
        return;
      }
      case "response.output_audio_transcript.delta": {
        const delta = event["delta"];
        if (typeof delta === "string") callbacks.onTranscriptDelta(delta);
        return;
      }
      case "response.output_audio_transcript.done": {
        const transcript = event["transcript"];
        if (typeof transcript === "string") callbacks.onTranscriptDone(transcript);
        return;
      }
      case "conversation.item.input_audio_transcription.completed": {
        const transcript = event["transcript"];
        if (typeof transcript === "string") callbacks.onUserTranscript?.(transcript);
        return;
      }
      case "response.output_item.added":
      case "response.output_item.done": {
        const item = asRecord(event["item"]);
        if (item && item["type"] === "function_call") {
          const callId = typeof item["call_id"] === "string" ? item["call_id"] : null;
          const name = typeof item["name"] === "string" ? item["name"] : null;
          if (callId && name) callNames.set(callId, name);
        }
        return;
      }
      case "response.function_call_arguments.done": {
        const callId = typeof event["call_id"] === "string" ? event["call_id"] : null;
        if (!callId) return;
        const name =
          typeof event["name"] === "string" && event["name"].length > 0
            ? event["name"]
            : (callNames.get(callId) ?? "");
        const argsJson = typeof event["arguments"] === "string" ? event["arguments"] : "{}";
        if (!name) {
          log.warn("realtime: function call with unknown name", { callId });
          return;
        }
        void handleFunctionCall(callId, name, argsJson);
        return;
      }
      case "response.done": {
        activeResponseId = null;
        if (followupQueued) {
          createResponse();
        } else {
          setState("ready");
        }
        return;
      }
      case "error": {
        const error = asRecord(event["error"]);
        const message = String(error?.["message"] ?? "unknown realtime error");
        // Cancellation races are benign: cancelling an already-finished
        // response produces an error event we can ignore.
        if (message.toLowerCase().includes("cancel")) {
          log.debug("realtime: benign cancel error", { message });
          return;
        }
        log.error("realtime error event", { message });
        callbacks.onError(message);
        return;
      }
      default:
        return;
    }
  };

  socket.onOpen(() => {
    log.info("realtime socket open", { model: config.openaiModel });
    sendSessionUpdate();
  });
  socket.onMessage(handleServerEvent);
  socket.onClose((code, reason) => {
    if (!closedByUs) {
      log.warn("realtime socket closed", { code, reason: reason.slice(0, 120) });
      setState("closed", `socket closed (${code})`);
    } else {
      setState("closed");
    }
  });
  socket.onError((err) => {
    log.error("realtime socket error", { error: err.message });
    setState("error", err.message);
    callbacks.onError(err.message);
  });

  return {
    appendAudio(pcm) {
      if (pcm.length === 0) return;
      send({ type: "input_audio_buffer.append", audio: pcm.toString("base64") });
    },

    pttStart() {
      if (activeResponseId !== null) {
        send({ type: "response.cancel" });
        activeResponseId = null;
        callbacks.onFlushPlayback();
        setState("ready");
      }
      send({ type: "input_audio_buffer.clear" });
    },

    pttEnd() {
      send({ type: "input_audio_buffer.commit" });
      createResponse();
    },

    sendTextTurn(text) {
      send({
        type: "conversation.item.create",
        item: { type: "message", role: "user", content: [{ type: "input_text", text }] },
      });
      createResponse();
    },

    close() {
      closedByUs = true;
      try {
        socket.close();
      } catch {
        // already closed
      }
    },

    state: () => state,
  };
}
