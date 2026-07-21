function opaqueSummaryId(value) {
  return typeof value === "string" && value.length > 0 ? value : null;
}

export const BROWSER_PROTOCOL_VERSION = 3;
export const MAX_TURN_ID = 2_147_483_647;

export function browserHelloControl() {
  return Object.freeze({ type: "hello", protocol_version: BROWSER_PROTOCOL_VERSION });
}

export function isProtocolReadyFrame(frame) {
  return (
    frame !== null &&
    typeof frame === "object" &&
    !Array.isArray(frame) &&
    Object.keys(frame).length === 2 &&
    frame.type === "protocol_ready" &&
    frame.version === BROWSER_PROTOCOL_VERSION
  );
}

export function isProtocolMismatchFrame(frame) {
  return (
    frame !== null &&
    typeof frame === "object" &&
    !Array.isArray(frame) &&
    Object.keys(frame).length === 2 &&
    frame.type === "protocol_mismatch" &&
    frame.required_version === BROWSER_PROTOCOL_VERSION
  );
}

function validVoiceMode(mode) {
  return mode === "live_response" || mode === "dictation";
}

function setVoiceModeControl(mode) {
  return Object.freeze({ type: "set_voice_mode", mode });
}

/** Gate browser conversation controls on protocol and initial voice-mode ordering. */
export function createBrowserConnectionController(initialPreferredMode) {
  let preferredVoiceMode = validVoiceMode(initialPreferredMode)
    ? initialPreferredMode
    : "live_response";
  let protocolReady = false;
  let initialVoiceModeReceived = false;
  let brokerVoiceMode = null;
  let pendingVoiceMode = null;

  return Object.freeze({
    get protocolReady() {
      return protocolReady;
    },
    get initialVoiceModeReceived() {
      return initialVoiceModeReceived;
    },
    get voiceModeChangePending() {
      return pendingVoiceMode !== null;
    },
    get preferredVoiceMode() {
      return preferredVoiceMode;
    },
    conversationReady({ loopback = false, hostAvailable = false, socketOpen = false } = {}) {
      return (
        loopback ||
        (protocolReady &&
          initialVoiceModeReceived &&
          pendingVoiceMode === null &&
          hostAvailable &&
          socketOpen)
      );
    },
    acceptProtocol(frame) {
      if (!isProtocolReadyFrame(frame)) return false;
      protocolReady = true;
      return true;
    },
    acceptVoiceMode(mode) {
      if (!protocolReady || !validVoiceMode(mode)) return null;
      brokerVoiceMode = mode;
      if (!initialVoiceModeReceived) {
        initialVoiceModeReceived = true;
        if (mode !== preferredVoiceMode) {
          pendingVoiceMode = preferredVoiceMode;
          return Object.freeze({
            mode,
            control: setVoiceModeControl(preferredVoiceMode),
            settled: false,
          });
        }
      }
      if (pendingVoiceMode !== null && mode !== pendingVoiceMode) {
        return Object.freeze({ mode, control: null, settled: false });
      }
      pendingVoiceMode = null;
      preferredVoiceMode = brokerVoiceMode;
      return Object.freeze({ mode, control: null, settled: true });
    },
    requestVoiceMode(mode) {
      if (
        !protocolReady ||
        !initialVoiceModeReceived ||
        pendingVoiceMode !== null ||
        !validVoiceMode(mode) ||
        mode === brokerVoiceMode
      ) {
        return null;
      }
      preferredVoiceMode = mode;
      pendingVoiceMode = mode;
      return setVoiceModeControl(mode);
    },
    rejectVoiceModeChange() {
      if (pendingVoiceMode === null || !validVoiceMode(brokerVoiceMode)) return null;
      pendingVoiceMode = null;
      preferredVoiceMode = brokerVoiceMode;
      return brokerVoiceMode;
    },
    disconnect() {
      protocolReady = false;
      initialVoiceModeReceived = false;
      brokerVoiceMode = null;
      pendingVoiceMode = null;
    },
  });
}

export function displayedSummaryId(boundContext) {
  return opaqueSummaryId(boundContext?.summary_id);
}

export function createBoundDraftState({
  summaryId = null,
  value = "",
  selectionStart = 0,
  selectionEnd = selectionStart,
} = {}) {
  return Object.freeze({
    summaryId: opaqueSummaryId(summaryId),
    value,
    selectionStart,
    selectionEnd,
  });
}

/** Bind a draft to the immutable summary currently displayed by the browser. */
export function reduceBoundDraft(state, event) {
  if (event?.type === "clear-draft") {
    return createBoundDraftState({ summaryId: event.summaryId });
  }
  if (event?.type !== "display-summary") return state;
  const summaryId = opaqueSummaryId(event.summaryId);
  if (state.summaryId === summaryId) return state;
  return createBoundDraftState({ summaryId });
}

/** Build a typed turn bound to the summary visible when it is submitted. */
export function textTurnControl(text, summaryId, turnId) {
  return Object.freeze({
    type: "text_turn",
    text,
    summary_id: opaqueSummaryId(summaryId),
    turn_id: turnId,
  });
}

function sameDraft(left, right) {
  return (
    left.summaryId === right.summaryId &&
    left.value === right.value &&
    left.selectionStart === right.selectionStart &&
    left.selectionEnd === right.selectionEnd
  );
}

/** Own one bounded typed turn until its exact broker acknowledgement arrives. */
export function createTextTurnController(initialSequence = 0) {
  let sequence =
    Number.isSafeInteger(initialSequence) && initialSequence >= 0
      ? Math.min(initialSequence, MAX_TURN_ID)
      : MAX_TURN_ID;
  let pending = null;

  return Object.freeze({
    get hasPending() {
      return pending !== null;
    },
    get pendingTurnId() {
      return pending?.turnId ?? null;
    },
    begin(text, draft) {
      if (pending || sequence >= MAX_TURN_ID || typeof text !== "string" || text.length === 0) {
        return null;
      }
      const turnId = sequence + 1;
      const capturedDraft = createBoundDraftState(draft);
      pending = Object.freeze({ turnId, text, draft: capturedDraft });
      sequence = turnId;
      return textTurnControl(text, capturedDraft.summaryId, turnId);
    },
    accept(turnId, currentDraft) {
      if (!pending || turnId !== pending.turnId) return null;
      const accepted = pending;
      pending = null;
      const current = createBoundDraftState(currentDraft);
      return Object.freeze({
        text: accepted.text,
        clearedDraft: sameDraft(accepted.draft, current)
          ? createBoundDraftState({ summaryId: current.summaryId })
          : null,
      });
    },
    reject(turnId) {
      if (!pending || turnId !== pending.turnId) return null;
      const rejected = pending;
      pending = null;
      return Object.freeze({ text: rejected.text });
    },
    disconnect() {
      pending = null;
    },
  });
}
