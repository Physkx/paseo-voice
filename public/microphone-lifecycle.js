export const MAX_RECORDING_ID = 2_147_483_647;

/** Allocate one bounded cross-mode recording and its correlated start frame. */
export function allocateRecording(previousRecordingId, mode, summaryId = null) {
  if (
    !Number.isSafeInteger(previousRecordingId) ||
    previousRecordingId < 0 ||
    previousRecordingId >= MAX_RECORDING_ID ||
    !["live_response", "dictation"].includes(mode)
  ) {
    return null;
  }
  const recordingId = previousRecordingId + 1;
  const capturedSummaryId =
    typeof summaryId === "string" && summaryId.length > 0 ? summaryId : null;
  return Object.freeze({
    sequence: recordingId,
    recording: Object.freeze({
      mode,
      recordingId,
      summaryId: capturedSummaryId,
    }),
    startControl: Object.freeze({
      type: "ptt_start",
      recording_id: recordingId,
      summary_id: capturedSummaryId,
    }),
  });
}

/** Match one broker start rejection to the exact current recording snapshot. */
export function recordingMatchesRejection(recording, frame) {
  return (
    recording !== null &&
    frame?.mode === recording.mode &&
    Number.isSafeInteger(frame.recording_id) &&
    frame.recording_id === recording.recordingId
  );
}

/** Retain one released live recording until the broker accepts or rejects it. */
export function createLiveRecordingController() {
  let pending = null;
  let stateCorrelationAvailable = false;
  const accept = () => {
    const accepted = pending;
    pending = null;
    return accepted;
  };
  return Object.freeze({
    get pending() {
      return pending;
    },
    release(recording) {
      if (pending || recording?.mode !== "live_response") return false;
      pending = recording;
      return true;
    },
    accept,
    acceptState(frame) {
      const hasRecordingId =
        frame !== null && typeof frame === "object" && Object.hasOwn(frame, "recording_id");
      if (hasRecordingId) stateCorrelationAvailable = true;
      if (!pending || !["responding", "ready"].includes(frame?.state)) return null;
      if (
        hasRecordingId &&
        (!Number.isSafeInteger(frame.recording_id) || frame.recording_id !== pending.recordingId)
      ) {
        return null;
      }
      if (stateCorrelationAvailable && !hasRecordingId) return null;
      return accept();
    },
    reject(frame) {
      if (!recordingMatchesRejection(pending, frame)) return null;
      const rejected = pending;
      pending = null;
      return rejected;
    },
    reset() {
      pending = null;
      stateCorrelationAvailable = false;
    },
  });
}

/** Immutably correlate a broker operation with the exact dictation recording that started it. */
export function bindDictationOperation(recording, operationId, recordingId) {
  if (
    recording?.mode !== "dictation" ||
    !Number.isSafeInteger(recording.recordingId) ||
    recordingId !== recording.recordingId ||
    typeof operationId !== "string" ||
    operationId.length === 0
  ) {
    return null;
  }
  return Object.freeze({ recording, operationId });
}

function boundDictationOperationId(recording, dictationOperation) {
  const operationId = dictationOperation?.operationId;
  return recording?.mode === "dictation" &&
    dictationOperation?.recording === recording &&
    typeof operationId === "string" &&
    operationId.length > 0
    ? operationId
    : null;
}

/** Build the terminal commit frame from the recording snapshot captured at start. */
export function recordingEndControl(recording, dictationOperation = null) {
  if (recording?.mode === "live_response" && Number.isSafeInteger(recording.recordingId)) {
    return Object.freeze({ type: "ptt_end", recording_id: recording.recordingId });
  }
  const operationId = boundDictationOperationId(recording, dictationOperation);
  return operationId === null
    ? null
    : Object.freeze({ type: "ptt_end", operation_id: operationId });
}

/** Build the terminal abort frame from the exact live recording snapshot. */
export function recordingAbortControl(recording) {
  return recording?.mode === "live_response" && Number.isSafeInteger(recording.recordingId)
    ? Object.freeze({ type: "ptt_abort", recording_id: recording.recordingId })
    : null;
}

/** Build a cancellation frame only for the exact correlated dictation recording. */
export function dictationCancelControl(recording, dictationOperation = null) {
  const operationId = boundDictationOperationId(recording, dictationOperation);
  if (operationId === null) return null;
  return Object.freeze({
    type: "cancel_dictation",
    operation_id: operationId,
  });
}

/** Clear terminal intent only after the socket accepts its exact control payload. */
export function attemptPendingControlSend(pendingIntent, control, send) {
  const delivery = attemptSocketSend(JSON.stringify(control), send);
  return Object.freeze({
    sent: delivery.sent,
    pendingIntent: delivery.sent ? null : pendingIntent,
    error: delivery.error,
  });
}

/** Attempt one browser socket payload without allowing a synchronous send throw to escape. */
export function attemptSocketSend(payload, send) {
  try {
    send(payload);
    return Object.freeze({ sent: true, error: null });
  } catch (error) {
    return Object.freeze({ sent: false, error });
  }
}

function pageCaptureState(holdCode = null, holdRecording = null, terminatingRecording = null) {
  return Object.freeze({ holdCode, holdRecording, terminatingRecording });
}

function pageCaptureDecision(state, effect = "none", recording = null) {
  const brokerControl =
    effect === "abort-live"
      ? Object.freeze({
          type: "ptt_abort",
          ...(Number.isSafeInteger(recording?.recordingId)
            ? { recording_id: recording.recordingId }
            : {}),
        })
      : null;
  return Object.freeze({ state, effect, recording, brokerControl });
}

/** Track page hold ownership and suppress duplicate page-lifecycle termination. */
export function createPageCaptureState() {
  return pageCaptureState();
}

/** Decide whether page keyboard or lifecycle events terminate the current capture. */
export function transitionPageCapture(state, event) {
  if (event.type === "hold-started") {
    if (!event.recording || typeof event.code !== "string") return pageCaptureDecision(state);
    return pageCaptureDecision(pageCaptureState(event.code, event.recording));
  }

  if (event.type === "capture-ended") {
    if (!event.recording) return pageCaptureDecision(state);
    const held = state.holdRecording === event.recording;
    const terminating = state.terminatingRecording === event.recording;
    if (!held && !terminating) return pageCaptureDecision(state);
    return pageCaptureDecision(
      pageCaptureState(
        held ? null : state.holdCode,
        held ? null : state.holdRecording,
        terminating ? null : state.terminatingRecording,
      ),
    );
  }

  if (event.type === "page-keyup") {
    const recording = event.activeRecording;
    if (
      event.code !== state.holdCode ||
      recording === null ||
      recording !== state.holdRecording ||
      recording === state.terminatingRecording
    ) {
      return pageCaptureDecision(state);
    }
    return pageCaptureDecision(pageCaptureState(null, null, recording), "stop", recording);
  }

  if (event.type !== "interrupt") return pageCaptureDecision(state);

  const recording = event.activeRecording;
  if (!recording) {
    if (state.holdRecording === null) return pageCaptureDecision(state);
    return pageCaptureDecision(pageCaptureState(null, null, state.terminatingRecording));
  }
  if (recording === state.terminatingRecording) return pageCaptureDecision(state);

  const effect =
    recording.mode === "live_response"
      ? "abort-live"
      : recording.mode === "dictation"
        ? "cancel-dictation"
        : "none";
  if (effect === "none") return pageCaptureDecision(state);
  return pageCaptureDecision(pageCaptureState(null, null, recording), effect, recording);
}

/** Decide whether an asynchronous request may still affect microphone ownership. */
export function microphoneTransactionDecision(state, requestToken, hasStream) {
  const current = state.requestToken !== null && state.requestToken === requestToken;
  return Object.freeze({
    current,
    instruction: current
      ? hasStream
        ? "activate-stream"
        : "apply-failure"
      : hasStream
        ? "stop-stream"
        : "ignore",
  });
}

/** Retain microphone presentation while higher-priority proposal or dictation state is visible. */
export function microphonePresentationTransition(
  currentPresentation,
  foregroundState,
  requestedPresentation,
) {
  const stored = requestedPresentation ?? currentPresentation ?? null;
  const hidden = ["awaiting-approval", "transcribing", "cleaning", "cancelling"].includes(
    foregroundState,
  );
  return Object.freeze({ stored, visible: hidden ? null : stored });
}

const VOICE_MODE_SELECTION_ERRORS = new Set([
  "Invalid voice mode request.",
  "Finish or cancel the pending action before changing voice mode.",
  "Finish or abort active recording before changing mode.",
  "Cancel active dictation before changing mode.",
]);

/** Identify broker errors that explicitly reject a voice-mode request. */
export function isVoiceModeSelectionError(message) {
  return VOICE_MODE_SELECTION_ERRORS.has(message);
}

/** Lock mutable microphone controls while capture or acquisition owns their values. */
export function microphoneControlsLocked(state, recording, voiceModeChangePending = false) {
  return (
    voiceModeChangePending ||
    recording !== null ||
    state.requestToken !== null ||
    ["setting-up", "reconnecting-default"].includes(state.phase)
  );
}

/** Gate direct capture separately from controls that remain locked during active recording. */
export function microphoneInteractionGate(state, recording, voiceModeChangePending) {
  const setupPending =
    state.requestToken !== null || ["setting-up", "reconnecting-default"].includes(state.phase);
  return Object.freeze({
    startBlocked: voiceModeChangePending || setupPending,
    controlsLocked: microphoneControlsLocked(state, recording, voiceModeChangePending),
  });
}

function lifecycleState(phase, streamToken, selectedDeviceId, requestToken = null) {
  return Object.freeze({ phase, streamToken, selectedDeviceId, requestToken });
}

function outcome(state, capture = "none", recovery = "none", recording = null) {
  const recordingId = recording?.recordingId;
  const brokerControl =
    capture === "abort-live"
      ? Object.freeze({
          type: "ptt_abort",
          ...(Number.isSafeInteger(recordingId) ? { recording_id: recordingId } : {}),
        })
      : null;
  return Object.freeze({ state, capture, recovery, brokerControl });
}

function captureEffect(event) {
  if (!event.recording) return "none";
  return event.recording.mode === "dictation" ? "cancel-dictation" : "abort-live";
}

function captureOutcome(state, event, recovery) {
  return outcome(state, captureEffect(event), recovery, event.recording);
}

/** Create deterministic microphone ownership state without retaining browser resources. */
export function createMicrophoneLifecycleState() {
  return lifecycleState("idle", null, null);
}

/** Decide capture and recovery effects while the browser adapter owns all browser APIs. */
export function transitionMicrophoneLifecycle(state, event) {
  if (event.type === "setup-started") {
    return captureOutcome(
      lifecycleState("setting-up", null, null, event.requestToken ?? null),
      event,
      "none",
    );
  }

  if (event.type === "setup-failed") {
    if (state.requestToken === null || event.requestToken !== state.requestToken) {
      return outcome(state);
    }
    return captureOutcome(
      lifecycleState("retry-required", null, state.selectedDeviceId),
      event,
      "require-retry",
    );
  }

  if (event.type === "connected") {
    if (
      (state.requestToken === null && event.requestToken !== undefined) ||
      (state.requestToken !== null && event.requestToken !== state.requestToken)
    ) {
      return outcome(state);
    }
    const phase = ["setting-up", "reconnecting-default"].includes(state.phase)
      ? state.phase
      : "ready";
    return outcome(
      lifecycleState(
        phase,
        event.streamToken,
        typeof event.selectedDeviceId === "string" && event.selectedDeviceId
          ? event.selectedDeviceId
          : null,
        state.requestToken,
      ),
    );
  }

  if (
    event.type === "configuration-complete" &&
    ["setting-up", "reconnecting-default"].includes(state.phase) &&
    event.streamToken === state.streamToken
  ) {
    if (state.requestToken === null || event.requestToken !== state.requestToken) {
      return outcome(state);
    }
    return outcome(lifecycleState("ready", state.streamToken, state.selectedDeviceId));
  }

  if (
    event.type === "track-ended" &&
    ["setting-up", "reconnecting-default"].includes(state.phase) &&
    event.streamToken === state.streamToken
  ) {
    return captureOutcome(
      lifecycleState("retry-required", null, state.selectedDeviceId),
      event,
      "require-retry",
    );
  }

  if (
    event.type === "permission-changed" &&
    state.phase === "ready" &&
    event.streamToken === state.streamToken &&
    event.permissionState === "denied"
  ) {
    return captureOutcome(
      lifecycleState("retry-required", null, state.selectedDeviceId),
      event,
      "require-retry",
    );
  }

  if (
    event.type === "devices-changed" &&
    state.phase === "ready" &&
    event.streamToken === state.streamToken &&
    event.permissionState === "granted" &&
    (state.selectedDeviceId === null || !event.deviceIds.includes(state.selectedDeviceId))
  ) {
    return captureOutcome(
      lifecycleState("reconnecting-default", state.streamToken, null),
      event,
      "reconnect-default",
    );
  }

  if (
    event.type === "loss-inspected" &&
    state.phase === "checking-loss" &&
    event.streamToken === state.streamToken &&
    event.permissionState === "granted" &&
    (state.selectedDeviceId === null || !event.deviceIds.includes(state.selectedDeviceId))
  ) {
    return outcome(
      lifecycleState("reconnecting-default", state.streamToken, null),
      "none",
      "reconnect-default",
    );
  }

  if (
    event.type === "loss-inspected" &&
    state.phase === "checking-loss" &&
    event.streamToken === state.streamToken
  ) {
    return outcome(
      lifecycleState("retry-required", null, state.selectedDeviceId),
      "none",
      "require-retry",
    );
  }

  if (
    event.type !== "track-ended" ||
    state.phase !== "ready" ||
    event.streamToken !== state.streamToken
  ) {
    return outcome(state);
  }

  return captureOutcome(
    lifecycleState("checking-loss", state.streamToken, state.selectedDeviceId),
    event,
    "inspect-loss",
  );
}
