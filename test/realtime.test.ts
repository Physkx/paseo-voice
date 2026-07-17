import { describe, expect, it } from "vitest";
import { loadConfig } from "../src/config.js";
import { nullLogger } from "../src/log.js";
import {
  createRealtimeSession,
  type RealtimeCallbacks,
  type RealtimeSocket,
} from "../src/realtime.js";
import type { ToolDispatcher, ToolResult } from "../src/tools.js";

const config = await loadConfig({
  env: {},
  readFile: async () => {
    throw new Error("no file");
  },
});

class FakeSocket implements RealtimeSocket {
  sent: Array<Record<string, unknown>> = [];
  private openCb: (() => void) | null = null;
  private messageCb: ((data: string) => void) | null = null;
  private closeCb: ((code: number, reason: string) => void) | null = null;

  send(data: string): void {
    this.sent.push(JSON.parse(data) as Record<string, unknown>);
  }
  close(): void {
    this.closeCb?.(1000, "closed");
  }
  onOpen(cb: () => void): void {
    this.openCb = cb;
  }
  onMessage(cb: (data: string) => void): void {
    this.messageCb = cb;
  }
  onClose(cb: (code: number, reason: string) => void): void {
    this.closeCb = cb;
  }
  onError(): void {}

  open(): void {
    this.openCb?.();
  }
  emit(event: Record<string, unknown>): void {
    this.messageCb?.(JSON.stringify(event));
  }
  sentOfType(type: string): Array<Record<string, unknown>> {
    return this.sent.filter((e) => e["type"] === type);
  }
}

interface Recorded {
  audio: Buffer[];
  transcript: string[];
  tools: string[];
  states: string[];
  flushes: number;
  errors: string[];
}

function makeSession(dispatchResult: ToolResult = { ok: true }) {
  const socket = new FakeSocket();
  const recorded: Recorded = {
    audio: [],
    transcript: [],
    tools: [],
    states: [],
    flushes: 0,
    errors: [],
  };
  const dispatched: Array<{ name: string; args: string }> = [];
  const dispatcher: ToolDispatcher = {
    dispatch: async (name, args) => {
      dispatched.push({ name, args });
      return dispatchResult;
    },
    currentSession: () => null,
  };
  const callbacks: RealtimeCallbacks = {
    onAudio: (chunk) => recorded.audio.push(chunk),
    onTranscriptDelta: (text) => recorded.transcript.push(text),
    onTranscriptDone: () => {},
    onToolCall: (name) => recorded.tools.push(name),
    onToolResult: () => {},
    onStateChange: (state) => recorded.states.push(state),
    onFlushPlayback: () => (recorded.flushes += 1),
    onError: (message) => recorded.errors.push(message),
  };
  const session = createRealtimeSession({
    apiKey: "sk-test",
    config,
    dispatcher,
    callbacks,
    socketFactory: () => socket,
    log: nullLogger,
    instructions: "test instructions",
  });
  return { session, socket, recorded, dispatched };
}

const flushMicrotasks = () => new Promise((resolve) => setTimeout(resolve, 0));

describe("createRealtimeSession", () => {
  it("sends a GA session.update on open with PTT config and tools", () => {
    const { socket } = makeSession();
    socket.open();
    const updates = socket.sentOfType("session.update");
    expect(updates).toHaveLength(1);
    const session = updates[0]!["session"] as Record<string, unknown>;
    expect(session["type"]).toBe("realtime");
    expect(session["instructions"]).toBe("test instructions");
    const audio = session["audio"] as Record<string, Record<string, unknown>>;
    expect(audio["input"]!["turn_detection"]).toBeNull();
    expect(audio["input"]!["format"]).toEqual({ type: "audio/pcm", rate: 24000 });
    expect(audio["output"]!["voice"]).toBe("marin");
    expect(Array.isArray(session["tools"])).toBe(true);
    expect((session["tools"] as unknown[]).length).toBeGreaterThan(0);
  });

  it("PTT flow: append then commit and response.create on release", () => {
    const { session, socket } = makeSession();
    socket.open();
    session.pttStart();
    session.appendAudio(Buffer.from([1, 2, 3, 4]));
    session.pttEnd();
    expect(socket.sentOfType("input_audio_buffer.clear")).toHaveLength(1);
    const appends = socket.sentOfType("input_audio_buffer.append");
    expect(appends).toHaveLength(1);
    expect(Buffer.from(String(appends[0]!["audio"]), "base64")).toEqual(Buffer.from([1, 2, 3, 4]));
    expect(socket.sentOfType("input_audio_buffer.commit")).toHaveLength(1);
    expect(socket.sentOfType("response.create")).toHaveLength(1);
  });

  it("audio and transcript deltas reach callbacks (GA and beta names)", () => {
    const { socket, recorded } = makeSession();
    socket.open();
    socket.emit({
      type: "response.output_audio.delta",
      delta: Buffer.from("ab").toString("base64"),
    });
    socket.emit({ type: "response.audio.delta", delta: Buffer.from("cd").toString("base64") });
    socket.emit({ type: "response.output_audio_transcript.delta", delta: "Hello" });
    expect(recorded.audio.map((b) => b.toString())).toEqual(["ab", "cd"]);
    expect(recorded.transcript).toEqual(["Hello"]);
  });

  it("function call round trip: dispatch, function_call_output, deferred response.create", async () => {
    const { socket, recorded, dispatched } = makeSession({ ok: true, sessions: [] });
    socket.open();
    socket.emit({ type: "response.created", response: { id: "resp_1" } });
    socket.emit({
      type: "response.output_item.added",
      item: { type: "function_call", call_id: "call_1", name: "list_sessions" },
    });
    socket.emit({
      type: "response.function_call_arguments.done",
      call_id: "call_1",
      arguments: "{}",
    });
    await flushMicrotasks();
    expect(dispatched).toEqual([{ name: "list_sessions", args: "{}" }]);
    expect(recorded.tools).toEqual(["list_sessions"]);
    const outputs = socket.sentOfType("conversation.item.create");
    expect(outputs).toHaveLength(1);
    const item = outputs[0]!["item"] as Record<string, unknown>;
    expect(item["type"]).toBe("function_call_output");
    expect(item["call_id"]).toBe("call_1");
    expect(JSON.parse(String(item["output"]))).toEqual({ ok: true, sessions: [] });
    // response still active: follow-up response.create deferred
    expect(socket.sentOfType("response.create")).toHaveLength(0);
    socket.emit({ type: "response.done", response: { id: "resp_1" } });
    expect(socket.sentOfType("response.create")).toHaveLength(1);
  });

  it("function call after response.done triggers immediate response.create", async () => {
    const { socket } = makeSession();
    socket.open();
    socket.emit({ type: "response.created", response: { id: "resp_1" } });
    socket.emit({ type: "response.done", response: { id: "resp_1" } });
    socket.emit({
      type: "response.function_call_arguments.done",
      call_id: "call_2",
      name: "list_sessions",
      arguments: "{}",
    });
    await flushMicrotasks();
    expect(socket.sentOfType("response.create")).toHaveLength(1);
  });

  it("barge-in: pttStart during active response cancels and flushes playback", () => {
    const { session, socket, recorded } = makeSession();
    socket.open();
    socket.emit({ type: "response.created", response: { id: "resp_1" } });
    session.pttStart();
    expect(socket.sentOfType("response.cancel")).toHaveLength(1);
    expect(recorded.flushes).toBe(1);
  });

  it("pttStart when idle does not cancel", () => {
    const { session, socket, recorded } = makeSession();
    socket.open();
    session.pttStart();
    expect(socket.sentOfType("response.cancel")).toHaveLength(0);
    expect(recorded.flushes).toBe(0);
  });

  it("sendTextTurn creates a user message item and a response", () => {
    const { session, socket } = makeSession();
    socket.open();
    session.sendTextTurn("list my sessions");
    const items = socket.sentOfType("conversation.item.create");
    expect(items).toHaveLength(1);
    const item = items[0]!["item"] as Record<string, unknown>;
    expect(item["type"]).toBe("message");
    expect(socket.sentOfType("response.create")).toHaveLength(1);
  });

  it("error events reach onError, cancel races are swallowed", () => {
    const { socket, recorded } = makeSession();
    socket.open();
    socket.emit({ type: "error", error: { message: "Cancellation failed: no active response" } });
    expect(recorded.errors).toHaveLength(0);
    socket.emit({ type: "error", error: { message: "rate limit exceeded" } });
    expect(recorded.errors).toEqual(["rate limit exceeded"]);
  });

  it("session.created marks ready and response lifecycle transitions state", () => {
    const { socket, recorded } = makeSession();
    socket.open();
    socket.emit({ type: "session.created", session: {} });
    socket.emit({ type: "response.created", response: { id: "r" } });
    socket.emit({ type: "response.done", response: { id: "r" } });
    expect(recorded.states).toEqual(["ready", "responding", "ready"]);
  });
});
