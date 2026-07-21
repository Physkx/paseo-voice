import test from "node:test";
import assert from "node:assert/strict";

import {
  MAX_RECORDING_ID,
  allocateRecording,
  attemptPendingControlSend,
  attemptSocketSend,
  bindDictationOperation,
  createLiveRecordingController,
  createPageCaptureState,
  createMicrophoneLifecycleState,
  dictationCancelControl,
  isVoiceModeSelectionError,
  microphoneControlsLocked,
  microphoneInteractionGate,
  microphonePresentationTransition,
  microphoneTransactionDecision,
  recordingMatchesRejection,
  recordingAbortControl,
  recordingEndControl,
  transitionPageCapture,
  transitionMicrophoneLifecycle,
} from "../public/microphone-lifecycle.js";

test("one global sequence increases strictly across live and dictation starts", () => {
  const live = allocateRecording(0, "live_response", "summary-a");
  const dictation = allocateRecording(live.sequence, "dictation", "summary-a");
  const nextLive = allocateRecording(dictation.sequence, "live_response", null);

  assert.equal(live.recording.recordingId, 1);
  assert.deepEqual(dictation, {
    sequence: 2,
    recording: {
      mode: "dictation",
      recordingId: 2,
      summaryId: "summary-a",
    },
    startControl: {
      type: "ptt_start",
      recording_id: 2,
      summary_id: "summary-a",
    },
  });
  assert.equal(nextLive.recording.recordingId, 3);
});

test("recording rejection matches exact mode and ID and ignores late A after B", () => {
  const recordingA = allocateRecording(0, "dictation", "summary-a").recording;
  const recordingB = allocateRecording(1, "live_response", "summary-b").recording;

  assert.equal(
    recordingMatchesRejection(recordingA, {
      mode: "dictation",
      recording_id: 1,
      message: "rejected",
    }),
    true,
  );
  assert.equal(
    recordingMatchesRejection(recordingB, {
      mode: "dictation",
      recording_id: 2,
      message: "wrong mode",
    }),
    false,
  );
  assert.equal(
    recordingMatchesRejection(recordingB, {
      mode: "dictation",
      recording_id: 1,
      message: "late A",
    }),
    false,
  );
});

test("released live recording accepts mock ready and rejects stale state once correlated", () => {
  const mockController = createLiveRecordingController();
  const mockRecording = allocateRecording(0, "live_response", null).recording;
  assert.equal(mockController.release(mockRecording), true);
  assert.equal(mockController.acceptState({ state: "ready" }), mockRecording);

  const controller = createLiveRecordingController();
  const recordingA = allocateRecording(0, "live_response", "summary-a").recording;
  const recordingB = allocateRecording(1, "live_response", "summary-b").recording;

  assert.equal(controller.release(recordingA), true);
  assert.equal(controller.pending, recordingA);
  assert.equal(controller.acceptState({ state: "thinking", recording_id: 1 }), null);
  assert.equal(controller.pending, recordingA);
  assert.equal(controller.acceptState({ state: "responding", recording_id: 1 }), recordingA);
  assert.equal(controller.release(recordingB), true);
  assert.equal(controller.acceptState({ state: "ready" }), null);
  assert.equal(controller.pending, recordingB);
  assert.equal(controller.acceptState({ state: "ready", recording_id: 1 }), null);
  assert.equal(controller.pending, recordingB);
  assert.equal(controller.acceptState({ state: "ready", recording_id: 2 }), recordingB);
  assert.equal(controller.pending, null);
});

test("a throwing socket send preserves pending terminal intent until disconnect reset", () => {
  const pendingIntent = Object.freeze({ kind: "end", recordingId: 1 });
  const failure = new Error("socket send failed");
  const failed = attemptPendingControlSend(
    pendingIntent,
    { type: "ptt_end", operation_id: "operation-a" },
    () => {
      throw failure;
    },
  );

  assert.equal(failed.sent, false);
  assert.equal(failed.pendingIntent, pendingIntent);
  assert.equal(failed.error, failure);

  let payload = null;
  const sent = attemptPendingControlSend(
    pendingIntent,
    { type: "ptt_end", operation_id: "operation-a" },
    (nextPayload) => {
      payload = nextPayload;
    },
  );
  assert.deepEqual(sent, { sent: true, pendingIntent: null, error: null });
  assert.equal(payload, '{"type":"ptt_end","operation_id":"operation-a"}');
});

test("safe socket send reports synchronous text or binary failure without throwing", () => {
  const failure = new Error("closed");
  const payload = new ArrayBuffer(2);
  assert.deepEqual(
    attemptSocketSend(payload, () => {
      throw failure;
    }),
    { sent: false, error: failure },
  );

  let delivered = null;
  assert.deepEqual(
    attemptSocketSend('{"type":"hello"}', (value) => {
      delivered = value;
    }),
    { sent: true, error: null },
  );
  assert.equal(delivered, '{"type":"hello"}');
});

test("only explicit mode-selection errors terminate the pending mode gate", () => {
  for (const message of [
    "Invalid voice mode request.",
    "Finish or cancel the pending action before changing voice mode.",
    "Finish or abort active recording before changing mode.",
    "Cancel active dictation before changing mode.",
  ]) {
    assert.equal(isVoiceModeSelectionError(message), true);
  }

  for (const message of [
    "Realtime provider disconnected.",
    "Finish or cancel the pending action before starting another turn.",
    "Invalid voice mode request",
    undefined,
  ]) {
    assert.equal(isVoiceModeSelectionError(message), false);
  }
});

test("a matching hold keyup stops its recording after focus moves to an interactive target", () => {
  const recording = Object.freeze({ mode: "live_response", recordingId: 41 });
  const owned = transitionPageCapture(createPageCaptureState(), {
    type: "hold-started",
    code: "Space",
    recording,
  }).state;

  const released = transitionPageCapture(owned, {
    type: "page-keyup",
    code: "Space",
    interactiveTarget: true,
    activeRecording: recording,
  });

  assert.equal(released.effect, "stop");
  assert.equal(released.recording, recording);
  assert.equal(released.state.holdRecording, null);
});

test("unrelated keyups and hold keyups that started no capture remain ignored", () => {
  const recording = Object.freeze({ mode: "live_response", recordingId: 42 });
  const owned = transitionPageCapture(createPageCaptureState(), {
    type: "hold-started",
    code: "Space",
    recording,
  }).state;

  const unrelated = transitionPageCapture(owned, {
    type: "page-keyup",
    code: "KeyD",
    activeRecording: recording,
  });
  assert.equal(unrelated.effect, "none");
  assert.equal(unrelated.state, owned);

  const neverStarted = transitionPageCapture(createPageCaptureState(), {
    type: "page-keyup",
    code: "Space",
    activeRecording: recording,
  });
  assert.equal(neverStarted.effect, "none");
  assert.equal(neverStarted.state.holdRecording, null);
});

test("page blur aborts live capture with its captured recording ID", () => {
  const recording = Object.freeze({ mode: "live_response", recordingId: 43 });
  const interrupted = transitionPageCapture(createPageCaptureState(), {
    type: "interrupt",
    reason: "blur",
    activeRecording: recording,
  });

  assert.equal(interrupted.effect, "abort-live");
  assert.equal(interrupted.recording, recording);
  assert.deepEqual(interrupted.brokerControl, { type: "ptt_abort", recording_id: 43 });
});

test("repeated blur and hidden visibility interrupt one dictation capture exactly once", () => {
  const recording = Object.freeze({ mode: "dictation", recordingId: null });
  const blurred = transitionPageCapture(createPageCaptureState(), {
    type: "interrupt",
    reason: "blur",
    activeRecording: recording,
  });
  assert.equal(blurred.effect, "cancel-dictation");
  assert.equal(blurred.recording, recording);

  const hidden = transitionPageCapture(blurred.state, {
    type: "interrupt",
    reason: "visibility-hidden",
    activeRecording: recording,
  });
  assert.equal(hidden.effect, "none");
  assert.equal(hidden.state, blurred.state);
});

test("normal hold release stops instead of aborting and clears ownership on completion", () => {
  const recording = Object.freeze({ mode: "dictation", recordingId: null });
  const owned = transitionPageCapture(createPageCaptureState(), {
    type: "hold-started",
    code: "Space",
    recording,
  }).state;
  const released = transitionPageCapture(owned, {
    type: "page-keyup",
    code: "Space",
    activeRecording: recording,
  });
  assert.equal(released.effect, "stop");
  assert.equal(released.brokerControl, null);

  const completed = transitionPageCapture(released.state, {
    type: "capture-ended",
    recording,
  });
  assert.deepEqual(completed.state, createPageCaptureState());
  assert.equal(completed.effect, "none");
});

test("live recording IDs are monotonic, bounded, and reused by start and end", () => {
  const first = allocateRecording(0, "live_response");
  const second = allocateRecording(first.sequence, "live_response");

  assert.deepEqual(first.recording, {
    mode: "live_response",
    recordingId: 1,
    summaryId: null,
  });
  assert.deepEqual(first.startControl, {
    type: "ptt_start",
    recording_id: 1,
    summary_id: null,
  });
  assert.equal(second.recording.recordingId, 2);
  assert.deepEqual(recordingEndControl(first.recording), {
    type: "ptt_end",
    recording_id: 1,
  });
  assert.deepEqual(recordingAbortControl(first.recording), {
    type: "ptt_abort",
    recording_id: 1,
  });
  assert.equal(recordingAbortControl({ mode: "dictation", recordingId: 2 }), null);
  assert.equal(recordingEndControl({ mode: "dictation", recordingId: null }), null);
  assert.equal(allocateRecording(MAX_RECORDING_ID, "live_response"), null);
});

test("a live recording snapshots summary A even after the dashboard changes to B", () => {
  let displayedSummaryId = "summary-a";
  const allocation = allocateRecording(0, "live_response", displayedSummaryId);
  displayedSummaryId = "summary-b";

  assert.deepEqual(allocation.recording, {
    mode: "live_response",
    recordingId: 1,
    summaryId: "summary-a",
  });
  assert.deepEqual(allocation.startControl, {
    type: "ptt_start",
    recording_id: 1,
    summary_id: "summary-a",
  });
  assert.equal(displayedSummaryId, "summary-b");
  assert.deepEqual(recordingEndControl(allocation.recording), {
    type: "ptt_end",
    recording_id: 1,
  });
  assert.deepEqual(
    transitionPageCapture(createPageCaptureState(), {
      type: "interrupt",
      reason: "blur",
      activeRecording: allocation.recording,
    }).brokerControl,
    { type: "ptt_abort", recording_id: 1 },
  );
});

test("dictation start carries its required displayed summary ID", () => {
  assert.deepEqual(allocateRecording(0, "dictation", "summary-a"), {
    sequence: 1,
    recording: {
      mode: "dictation",
      recordingId: 1,
      summaryId: "summary-a",
    },
    startControl: { type: "ptt_start", recording_id: 1, summary_id: "summary-a" },
  });
});

test("dictation terminal controls wait for the broker operation bound to that recording", () => {
  const recording = allocateRecording(0, "dictation", "summary-a").recording;

  assert.equal(recordingEndControl(recording), null);
  const operation = bindDictationOperation(recording, "operation-a", recording.recordingId);
  assert.deepEqual(recordingEndControl(recording, operation), {
    type: "ptt_end",
    operation_id: "operation-a",
  });
});

test("dictation cancellation carries the exact correlated broker operation ID", () => {
  const recording = allocateRecording(0, "dictation", "summary-a").recording;

  assert.equal(dictationCancelControl(recording), null);
  const operation = bindDictationOperation(recording, "operation-a", recording.recordingId);
  assert.deepEqual(dictationCancelControl(recording, operation), {
    type: "cancel_dictation",
    operation_id: "operation-a",
  });
});

test("a delayed operation from recording A cannot terminate recording B", () => {
  const recordingA = allocateRecording(0, "dictation", "summary-a").recording;
  assert.equal(bindDictationOperation(recordingA, "operation-a", 2), null);
  const operationA = bindDictationOperation(recordingA, "operation-a", recordingA.recordingId);
  const recordingB = allocateRecording(1, "dictation", "summary-b").recording;

  assert.equal(Object.isFrozen(operationA), true);
  assert.equal(operationA.recording, recordingA);
  assert.equal(recordingEndControl(recordingB, operationA), null);
  assert.equal(dictationCancelControl(recordingB, operationA), null);
  assert.deepEqual(recordingEndControl(recordingA, operationA), {
    type: "ptt_end",
    operation_id: "operation-a",
  });
});

test("request generations isolate stale setup resolution and failure", () => {
  const initial = createMicrophoneLifecycleState();
  const first = transitionMicrophoneLifecycle(initial, {
    type: "setup-started",
    requestToken: 1,
    recording: null,
  }).state;
  const second = transitionMicrophoneLifecycle(first, {
    type: "setup-started",
    requestToken: 2,
    recording: null,
  }).state;
  assert.equal(second.requestToken, 2);
  assert.deepEqual(microphoneTransactionDecision(second, 1, true), {
    current: false,
    instruction: "stop-stream",
  });
  assert.deepEqual(microphoneTransactionDecision(second, 1, false), {
    current: false,
    instruction: "ignore",
  });
  assert.deepEqual(microphoneTransactionDecision(second, 2, true), {
    current: true,
    instruction: "activate-stream",
  });

  const staleFailure = transitionMicrophoneLifecycle(second, {
    type: "setup-failed",
    requestToken: 1,
    recording: { mode: "live_response", recordingId: 7 },
  });
  assert.equal(staleFailure.state, second);
  assert.equal(staleFailure.brokerControl, null);

  const currentFailure = transitionMicrophoneLifecycle(second, {
    type: "setup-failed",
    requestToken: 2,
    recording: { mode: "live_response", recordingId: 7 },
  });
  assert.equal(currentFailure.state.phase, "retry-required");
  assert.equal(currentFailure.state.requestToken, null);
  assert.deepEqual(currentFailure.brokerControl, { type: "ptt_abort", recording_id: 7 });

  const dictationFailure = transitionMicrophoneLifecycle(second, {
    type: "setup-failed",
    requestToken: 2,
    recording: { mode: "dictation", recordingId: null },
  });
  assert.equal(dictationFailure.capture, "cancel-dictation");
  assert.equal(dictationFailure.brokerControl, null);
});

test("only the current request can activate or complete a stream", () => {
  const currentRequest = transitionMicrophoneLifecycle(createMicrophoneLifecycleState(), {
    type: "setup-started",
    requestToken: 2,
    recording: null,
  }).state;
  const staleConnected = transitionMicrophoneLifecycle(currentRequest, {
    type: "connected",
    requestToken: 1,
    streamToken: Symbol("stale stream"),
    selectedDeviceId: "stale-device",
  });
  assert.equal(staleConnected.state, currentRequest);

  const currentStream = Symbol("current stream");
  const connected = transitionMicrophoneLifecycle(currentRequest, {
    type: "connected",
    requestToken: 2,
    streamToken: currentStream,
    selectedDeviceId: "current-device",
  }).state;
  const staleComplete = transitionMicrophoneLifecycle(connected, {
    type: "configuration-complete",
    requestToken: 1,
    streamToken: currentStream,
  });
  assert.equal(staleComplete.state, connected);

  const complete = transitionMicrophoneLifecycle(connected, {
    type: "configuration-complete",
    requestToken: 2,
    streamToken: currentStream,
  });
  assert.equal(complete.state.phase, "ready");
  assert.equal(complete.state.requestToken, null);

  const lateFailure = transitionMicrophoneLifecycle(complete.state, {
    type: "setup-failed",
    requestToken: 2,
    recording: { mode: "live_response", recordingId: 9 },
  });
  assert.equal(lateFailure.state, complete.state);
  assert.equal(lateFailure.brokerControl, null);
});

test("only current track loss aborts capture without committing it", () => {
  const currentStream = Symbol("current stream");
  const supersededStream = Symbol("superseded stream");
  const ready = transitionMicrophoneLifecycle(createMicrophoneLifecycleState(), {
    type: "connected",
    streamToken: currentStream,
    selectedDeviceId: "desk-mic",
  }).state;

  const stale = transitionMicrophoneLifecycle(ready, {
    type: "track-ended",
    streamToken: supersededStream,
    recording: { mode: "dictation", recordingId: null },
  });
  assert.equal(stale.state, ready);
  assert.equal(stale.capture, "none");
  assert.equal(stale.brokerControl, null);

  const dictationLoss = transitionMicrophoneLifecycle(ready, {
    type: "track-ended",
    streamToken: currentStream,
    recording: { mode: "dictation", recordingId: null },
  });
  assert.equal(dictationLoss.capture, "cancel-dictation");
  assert.equal(dictationLoss.brokerControl, null);
  assert.equal(dictationLoss.recovery, "inspect-loss");

  const liveLoss = transitionMicrophoneLifecycle(ready, {
    type: "track-ended",
    streamToken: currentStream,
    recording: { mode: "live_response", recordingId: 1 },
  });
  assert.equal(liveLoss.capture, "abort-live");
  assert.notEqual(liveLoss.capture, "ptt_end");
});

test("current live loss emits exactly one ptt_abort across repeated callbacks", () => {
  const streamToken = Symbol("live stream");
  const ready = transitionMicrophoneLifecycle(createMicrophoneLifecycleState(), {
    type: "connected",
    streamToken,
    selectedDeviceId: null,
  }).state;

  const ended = transitionMicrophoneLifecycle(ready, {
    type: "track-ended",
    streamToken,
    recording: { mode: "live_response", recordingId: 1 },
  });
  assert.deepEqual(ended.brokerControl, { type: "ptt_abort", recording_id: 1 });

  const repeatedEnded = transitionMicrophoneLifecycle(ended.state, {
    type: "track-ended",
    streamToken,
    recording: { mode: "live_response", recordingId: 1 },
  });
  assert.equal(repeatedEnded.brokerControl, null);

  const repeatedPermission = transitionMicrophoneLifecycle(ended.state, {
    type: "permission-changed",
    streamToken,
    permissionState: "denied",
    recording: { mode: "live_response", recordingId: 1 },
  });
  assert.equal(repeatedPermission.brokerControl, null);

  const permissionFirst = transitionMicrophoneLifecycle(ready, {
    type: "permission-changed",
    streamToken,
    permissionState: "denied",
    recording: { mode: "live_response", recordingId: 1 },
  });
  assert.deepEqual(permissionFirst.brokerControl, { type: "ptt_abort", recording_id: 1 });
  const endedAfterPermission = transitionMicrophoneLifecycle(permissionFirst.state, {
    type: "track-ended",
    streamToken,
    recording: { mode: "live_response", recordingId: 1 },
  });
  assert.equal(endedAfterPermission.brokerControl, null);
});

test("loss uses the captured recording mode and ID instead of the current toggle", () => {
  const streamToken = Symbol("captured-mode stream");
  const ready = transitionMicrophoneLifecycle(createMicrophoneLifecycleState(), {
    type: "connected",
    streamToken,
    selectedDeviceId: null,
  }).state;

  const liveLoss = transitionMicrophoneLifecycle(ready, {
    type: "track-ended",
    streamToken,
    recording: { mode: "live_response", recordingId: 41 },
    voiceMode: "dictation",
  });
  assert.equal(liveLoss.capture, "abort-live");
  assert.deepEqual(liveLoss.brokerControl, { type: "ptt_abort", recording_id: 41 });

  const dictationLoss = transitionMicrophoneLifecycle(ready, {
    type: "permission-changed",
    streamToken,
    permissionState: "denied",
    recording: { mode: "dictation", recordingId: null },
    voiceMode: "live_response",
  });
  assert.equal(dictationLoss.capture, "cancel-dictation");
  assert.equal(dictationLoss.brokerControl, null);
});

test("a missing selected device with permission reconnects to system default", () => {
  const streamToken = Symbol("selected stream");
  const ready = transitionMicrophoneLifecycle(createMicrophoneLifecycleState(), {
    type: "connected",
    streamToken,
    selectedDeviceId: "removed-mic",
  }).state;

  const missing = transitionMicrophoneLifecycle(ready, {
    type: "devices-changed",
    streamToken,
    deviceIds: ["remaining-mic"],
    permissionState: "granted",
    recording: null,
  });

  assert.equal(missing.state.phase, "reconnecting-default");
  assert.equal(missing.state.selectedDeviceId, null);
  assert.equal(missing.recovery, "reconnect-default");
});

test("track loss inspection recovers a removed selected device exactly once", () => {
  const streamToken = Symbol("ended selected stream");
  const ready = transitionMicrophoneLifecycle(createMicrophoneLifecycleState(), {
    type: "connected",
    streamToken,
    selectedDeviceId: "removed-mic",
  }).state;
  const checking = transitionMicrophoneLifecycle(ready, {
    type: "track-ended",
    streamToken,
    recording: null,
  }).state;

  const inspected = transitionMicrophoneLifecycle(checking, {
    type: "loss-inspected",
    streamToken,
    deviceIds: ["remaining-mic"],
    permissionState: "granted",
  });
  assert.equal(inspected.state.phase, "reconnecting-default");
  assert.equal(inspected.recovery, "reconnect-default");

  const duplicate = transitionMicrophoneLifecycle(inspected.state, {
    type: "loss-inspected",
    streamToken,
    deviceIds: ["remaining-mic"],
    permissionState: "granted",
  });
  assert.equal(duplicate.recovery, "none");
});

test("default track loss reconnects to default when permission remains granted", () => {
  const streamToken = Symbol("default stream");
  const ready = transitionMicrophoneLifecycle(createMicrophoneLifecycleState(), {
    type: "connected",
    streamToken,
    selectedDeviceId: null,
  }).state;
  const checking = transitionMicrophoneLifecycle(ready, {
    type: "track-ended",
    streamToken,
    recording: null,
  }).state;

  const inspected = transitionMicrophoneLifecycle(checking, {
    type: "loss-inspected",
    streamToken,
    deviceIds: ["replacement-default"],
    permissionState: "granted",
  });
  assert.equal(inspected.state.phase, "reconnecting-default");
  assert.equal(inspected.recovery, "reconnect-default");
});

test("devicechange reconnects a followed default once", () => {
  const streamToken = Symbol("followed default");
  const ready = transitionMicrophoneLifecycle(createMicrophoneLifecycleState(), {
    type: "connected",
    streamToken,
    selectedDeviceId: null,
  }).state;
  const changed = transitionMicrophoneLifecycle(ready, {
    type: "devices-changed",
    streamToken,
    deviceIds: ["new-default"],
    permissionState: "granted",
    recording: null,
  });
  assert.equal(changed.state.phase, "reconnecting-default");
  assert.equal(changed.recovery, "reconnect-default");

  const duplicate = transitionMicrophoneLifecycle(changed.state, {
    type: "devices-changed",
    streamToken,
    deviceIds: ["new-default"],
    permissionState: "granted",
    recording: null,
  });
  assert.equal(duplicate.recovery, "none");
});

test("microphone presentation preserves proposal and dictation precedence", () => {
  const retry = Object.freeze({ state: "error", detail: "retry required" });
  const hiddenRetry = microphonePresentationTransition(null, "awaiting-approval", retry);
  assert.deepEqual(hiddenRetry, { stored: retry, visible: null });

  const restoredRetry = microphonePresentationTransition(hiddenRetry.stored, "ready");
  assert.deepEqual(restoredRetry, { stored: retry, visible: retry });
  assert.deepEqual(microphonePresentationTransition(hiddenRetry.stored, "awaiting_review"), {
    stored: retry,
    visible: retry,
  });

  const ready = Object.freeze({ state: "ready", detail: "" });
  for (const foreground of ["transcribing", "cleaning", "cancelling"]) {
    const hiddenReady = microphonePresentationTransition(retry, foreground, ready);
    assert.deepEqual(hiddenReady, { stored: ready, visible: null });
    assert.deepEqual(microphonePresentationTransition(hiddenReady.stored, "ready"), {
      stored: ready,
      visible: ready,
    });
  }
});

test("mode, device, and processing controls lock for recording and setup", () => {
  const idle = createMicrophoneLifecycleState();
  assert.equal(microphoneControlsLocked(idle, null), false);
  assert.equal(microphoneControlsLocked(idle, { mode: "live_response", recordingId: 3 }), true);

  const settingUp = transitionMicrophoneLifecycle(idle, {
    type: "setup-started",
    requestToken: 4,
    recording: null,
  }).state;
  assert.equal(microphoneControlsLocked(settingUp, null), true);
});

test("pending voice mode blocks direct capture and locks microphone controls", () => {
  const ready = createMicrophoneLifecycleState();
  assert.deepEqual(microphoneInteractionGate(ready, null, false), {
    startBlocked: false,
    controlsLocked: false,
  });
  assert.deepEqual(microphoneInteractionGate(ready, null, true), {
    startBlocked: true,
    controlsLocked: true,
  });

  const settingUp = transitionMicrophoneLifecycle(ready, {
    type: "setup-started",
    requestToken: 5,
    recording: null,
  }).state;
  assert.deepEqual(microphoneInteractionGate(settingUp, null, false), {
    startBlocked: true,
    controlsLocked: true,
  });
});

test("permission revocation requires one explicit retry and never reconnects", () => {
  const streamToken = Symbol("revoked stream");
  const ready = transitionMicrophoneLifecycle(createMicrophoneLifecycleState(), {
    type: "connected",
    streamToken,
    selectedDeviceId: "desk-mic",
  }).state;

  const revoked = transitionMicrophoneLifecycle(ready, {
    type: "permission-changed",
    streamToken,
    permissionState: "denied",
    recording: { mode: "dictation", recordingId: null },
  });
  assert.equal(revoked.state.phase, "retry-required");
  assert.equal(revoked.state.streamToken, null);
  assert.equal(revoked.capture, "cancel-dictation");
  assert.equal(revoked.recovery, "require-retry");

  const repeated = transitionMicrophoneLifecycle(revoked.state, {
    type: "permission-changed",
    streamToken,
    permissionState: "denied",
    recording: null,
  });
  assert.equal(repeated.recovery, "none");
});

test("prompt or unsupported permission state does not tear down a working stream", () => {
  const streamToken = Symbol("working stream");
  const ready = transitionMicrophoneLifecycle(createMicrophoneLifecycleState(), {
    type: "connected",
    streamToken,
    selectedDeviceId: null,
  }).state;

  for (const permissionState of ["prompt", "unknown"]) {
    const unchanged = transitionMicrophoneLifecycle(ready, {
      type: "permission-changed",
      streamToken,
      permissionState,
      recording: { mode: "live_response", recordingId: 12 },
    });
    assert.equal(unchanged.state, ready);
    assert.equal(unchanged.brokerControl, null);
  }
});

test("ended-track inspection treats non-granted permission as retry-only", () => {
  const streamToken = Symbol("permission-ended stream");
  const ready = transitionMicrophoneLifecycle(createMicrophoneLifecycleState(), {
    type: "connected",
    streamToken,
    selectedDeviceId: null,
  }).state;
  const checking = transitionMicrophoneLifecycle(ready, {
    type: "track-ended",
    streamToken,
    recording: null,
  }).state;

  const revoked = transitionMicrophoneLifecycle(checking, {
    type: "loss-inspected",
    streamToken,
    deviceIds: [],
    permissionState: "denied",
  });
  assert.equal(revoked.state.phase, "retry-required");
  assert.equal(revoked.recovery, "require-retry");
});

test("repeated setup retires the old stream before resource cleanup", () => {
  const oldStream = Symbol("old setup stream");
  const ready = transitionMicrophoneLifecycle(createMicrophoneLifecycleState(), {
    type: "connected",
    streamToken: oldStream,
    selectedDeviceId: null,
  }).state;

  const settingUp = transitionMicrophoneLifecycle(ready, {
    type: "setup-started",
    requestToken: 1,
    recording: { mode: "live_response", recordingId: 21 },
  });
  assert.equal(settingUp.state.phase, "setting-up");
  assert.equal(settingUp.state.streamToken, null);
  assert.equal(settingUp.capture, "abort-live");

  const stoppedOldTrack = transitionMicrophoneLifecycle(settingUp.state, {
    type: "track-ended",
    streamToken: oldStream,
    recording: null,
  });
  assert.equal(stoppedOldTrack.recovery, "none");

  const failed = transitionMicrophoneLifecycle(settingUp.state, {
    type: "setup-failed",
    requestToken: 1,
    recording: null,
  });
  assert.equal(failed.state.phase, "retry-required");
  assert.equal(failed.recovery, "require-retry");
});

test("device enumeration is advisory without granted permission", () => {
  const streamToken = Symbol("permission-hidden stream");
  const ready = transitionMicrophoneLifecycle(createMicrophoneLifecycleState(), {
    type: "connected",
    streamToken,
    selectedDeviceId: "hidden-mic",
  }).state;

  const permissionHidden = transitionMicrophoneLifecycle(ready, {
    type: "devices-changed",
    streamToken,
    deviceIds: [],
    permissionState: "prompt",
    recording: { mode: "live_response", recordingId: 15 },
  });
  assert.equal(permissionHidden.state, ready);
  assert.equal(permissionHidden.capture, "none");
  assert.equal(permissionHidden.recovery, "none");
});

test("setup failure after stream acquisition still retires that stream", () => {
  const acquiredStream = Symbol("partially configured stream");
  const settingUp = transitionMicrophoneLifecycle(createMicrophoneLifecycleState(), {
    type: "setup-started",
    requestToken: 1,
    recording: null,
  }).state;
  const acquired = transitionMicrophoneLifecycle(settingUp, {
    type: "connected",
    requestToken: 1,
    streamToken: acquiredStream,
    selectedDeviceId: null,
  }).state;

  const failed = transitionMicrophoneLifecycle(acquired, {
    type: "setup-failed",
    requestToken: 1,
    recording: null,
  });
  assert.equal(failed.state.phase, "retry-required");
  assert.equal(failed.state.streamToken, null);
  assert.equal(failed.recovery, "require-retry");
});

test("an acquired setup stream is not ready until configuration completes", () => {
  const acquiredStream = Symbol("configuring stream");
  const settingUp = transitionMicrophoneLifecycle(createMicrophoneLifecycleState(), {
    type: "setup-started",
    requestToken: 1,
    recording: null,
  }).state;
  const acquired = transitionMicrophoneLifecycle(settingUp, {
    type: "connected",
    requestToken: 1,
    streamToken: acquiredStream,
    selectedDeviceId: null,
  }).state;
  assert.equal(acquired.phase, "setting-up");

  const ended = transitionMicrophoneLifecycle(acquired, {
    type: "track-ended",
    streamToken: acquiredStream,
    recording: null,
  });
  assert.equal(ended.state.phase, "retry-required");
  assert.equal(ended.recovery, "require-retry");

  const ready = transitionMicrophoneLifecycle(acquired, {
    type: "configuration-complete",
    requestToken: 1,
    streamToken: acquiredStream,
  });
  assert.equal(ready.state.phase, "ready");
});
