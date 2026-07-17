import type { RealtimeCallbacks } from "./realtime.js";
import { runCommand, type CommandContext } from "./commands.js";
import type { ToolDispatcher } from "./tools.js";

/**
 * Stand-in for the OpenAI realtime session when no API key is configured
 * (or PASEO_VOICE_MOCK=1). Accepts text turns only and routes them through
 * the deterministic command parser to the real tool dispatcher, emitting
 * transcript events through the same callbacks the real session uses. This
 * exercises server, tools, gate, summariser, and paseo CLI end to end
 * without OpenAI.
 */
export interface VoiceSessionLike {
  appendAudio(pcm: Buffer): void;
  pttStart(): void;
  pttEnd(): void;
  sendTextTurn(text: string): void;
  close(): void;
}

export function createMockVoiceSession(
  dispatcher: ToolDispatcher,
  callbacks: RealtimeCallbacks,
): VoiceSessionLike {
  const ctx: CommandContext = { dispatch: dispatcher.dispatch, lastProposalToken: null };
  let heardAudio = false;

  queueMicrotask(() => {
    callbacks.onStateChange("ready", "mock mode, no OpenAI key");
  });

  const speak = (text: string) => {
    callbacks.onTranscriptDelta(text);
    callbacks.onTranscriptDone(text);
  };

  return {
    appendAudio() {
      heardAudio = true;
    },
    pttStart() {
      heardAudio = false;
    },
    pttEnd() {
      if (heardAudio) {
        speak("Mock mode has no speech recognition. Type a text turn instead.");
      }
    },
    sendTextTurn(text) {
      callbacks.onStateChange("responding");
      void runCommand(text, ctx)
        .then((reply) => speak(reply))
        .catch((err) => callbacks.onError(err instanceof Error ? err.message : String(err)))
        .finally(() => callbacks.onStateChange("ready"));
    },
    close() {
      // nothing to release
    },
  };
}
