/**
 * Paseo Voice browser client. Deliberately dumb: microphone in, speaker out,
 * push-to-talk control frames. No secrets, no OpenAI contact, no build step.
 *
 * Wire protocol to the broker (/ws):
 * - client -> broker: binary = pcm16 24 kHz mono mic audio (while PTT held);
 *   JSON text = {type: "hello" | "set_voice_mode" | "ptt_start" | "ptt_end" |
 *   "ptt_abort" | "cancel_dictation" | "text_turn" | "select_host" |
 *   "confirm_proposal" | "cancel_proposal"}
 *   Every text turn carries the displayed summary_id or null. Every PTT start
 *   also carries one globally increasing recording_id across both voice modes.
 *   Live-response end and abort controls retain that recording_id.
 *   Dictation end and cancellation controls carry the broker-issued operation_id.
 * - broker -> client: binary = pcm16 24 kHz assistant audio; JSON text =
 *   protocol_ready / protocol_mismatch / state / transcript_delta /
 *   transcript_done / user_transcript / tool / host_state / dashboard_state /
 *   proposal / recording_rejected / flush_audio / error / mode / voice_mode
 *
 * ?loopback=1 skips the server and echoes mic audio to the speaker, to test
 * the audio path alone.
 */

import {
  applyCapturedDraft,
  canStartDictation,
  createDictationController,
  dictationOwnsContext,
  hasSameRoutingContext,
  insertText,
} from "./dictation-target.js";
import {
  canStartConversationalTurn,
  isNativeProposalActivationKeydown,
  proposalActivationTransition,
  proposalAwareInterfaceState,
  proposalFrameTransition,
  proposalStateFromFrame,
  shouldFocusProposalConfirm,
} from "./interaction-gate.js";
import { createPlaybackController } from "./playback-frame.js";
import { isInteractiveTarget, validateShortcuts } from "./shortcut-config.js";
import { createBlockAvatar } from "./avatar-blocks.js";
import {
  buildMicrophoneConstraints,
  canFallbackFromSavedDevice,
  cueFrequency,
  defaultMicrophoneTransition,
  effectiveMicrophonePermission,
  persistedDeviceId,
} from "./microphone-config.js";
import {
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
  recordingAbortControl,
  recordingMatchesRejection,
  recordingEndControl,
  transitionMicrophoneLifecycle,
  transitionPageCapture,
} from "./microphone-lifecycle.js";
import {
  browserHelloControl,
  createBrowserConnectionController,
  createBoundDraftState,
  createTextTurnController,
  displayedSummaryId,
  isProtocolMismatchFrame,
  reduceBoundDraft,
} from "./turn-context.js";

const $ = (id) => document.getElementById(id);
const connPill = $("conn-pill");
const modePill = $("mode-pill");
const statePill = $("state-pill");
const setupPanel = $("setup");
const enableMicrophoneButton = $("enable-mic");
const pttButton = $("ptt");
const pttLabel = $("ptt-label");
const transcriptBox = $("transcript");
const activityBox = $("activity");
const proposalBanner = $("proposal-banner");
const proposalText = $("proposal-text");
const confirmProposalButton = $("confirm-proposal");
const cancelProposalButton = $("cancel-proposal");
const textForm = $("text-form");
const textInput = $("text-input");
const textSubmitButton = textForm.querySelector("button");
const hostSelect = $("host-select");
const hostCwd = $("host-cwd");
const hostProvider = $("host-provider");
const avatar = $("avatar");
const avatarState = $("avatar-state");
const boundThread = $("bound-thread");
const responseDestination = $("response-destination");
const queueCount = $("queue-count");
const agentCount = $("agent-count");
const agentGrid = $("agent-grid");
const dashboardEmpty = $("dashboard-empty");
const voiceModeToggle = $("voice-mode-toggle");
const voiceModeDescription = $("voice-mode-description");
const dictationPreview = $("dictation-preview");
const dictationPreviewText = $("dictation-preview-text");
const dictationReviewActions = $("dictation-review-actions");
const recordingModeSelect = $("recording-mode");
const silenceStop = $("silence-stop");
const silencePeriod = $("silence-period");
const cancelDictationButton = $("cancel-dictation");
const dictationSettings = $("dictation-settings");
const microphoneSelect = $("microphone-select");
const microphoneStatus = $("microphone-status");
const echoCancellation = $("echo-cancellation");
const noiseSuppression = $("noise-suppression");
const autoGain = $("auto-gain");
const preRoll = $("pre-roll");
const soundCues = $("sound-cues");
const holdShortcut = $("hold-shortcut");
const toggleShortcut = $("toggle-shortcut");
const cancelShortcut = $("cancel-shortcut");
const shortcutStatus = $("shortcut-status");

const loopback = new URLSearchParams(location.search).get("loopback") === "1";
const avatarDemo = new URLSearchParams(location.search).get("avatar") === "demo";
let pageLifecycle = "active";
let pageGeneration = 0;
const ownedEventListeners = [];

function pageIsActive() {
  return pageLifecycle === "active";
}

function listen(target, type, listener, options) {
  if (pageLifecycle === "final") return () => {};
  const owned = { target, type, listener, options };
  target.addEventListener(type, listener, options);
  ownedEventListeners.push(owned);
  return () => {
    const index = ownedEventListeners.indexOf(owned);
    if (index === -1) return;
    ownedEventListeners.splice(index, 1);
    target.removeEventListener(type, listener, options);
  };
}

function unlisten(target, type, listener) {
  target?.removeEventListener(type, listener);
  const index = ownedEventListeners.findIndex(
    (owned) => owned.target === target && owned.type === type && owned.listener === listener,
  );
  if (index !== -1) ownedEventListeners.splice(index, 1);
}

function removeOwnedEventListeners(ownedTarget = null) {
  for (let index = ownedEventListeners.length - 1; index >= 0; index -= 1) {
    const owned = ownedEventListeners[index];
    if (ownedTarget && owned.target !== ownedTarget) continue;
    ownedEventListeners.splice(index, 1);
    owned.target.removeEventListener(owned.type, owned.listener, owned.options);
  }
}

let blockAvatar = null;
let avatarMicAnalyser = null;
let avatarVoiceAnalyser = null;
void createBlockAvatar({
  host: avatar,
  demo: avatarDemo,
  demoPin: avatarDemo ? new URLSearchParams(location.search).get("avatarState") : null,
  reducedMotion: matchMedia("(prefers-reduced-motion: reduce)").matches,
})
  .then((instance) => {
    if (pageLifecycle === "final") {
      instance?.destroy();
      return;
    }
    blockAvatar = instance;
    blockAvatar?.setState(interfaceState);
    blockAvatar?.setAnalysers({ mic: avatarMicAnalyser, voice: avatarVoiceAnalyser });
  })
  .catch(() => {
    blockAvatar = null;
  });
const voiceModeStorageKey = "paseoVoice.voiceMode";
const recordingModeStorageKey = "paseoVoice.recordingMode";
const silenceStopStorageKey = "paseoVoice.silenceStop";
const silencePeriodStorageKey = "paseoVoice.silencePeriod";
const deviceStorageKey = "paseoVoice.microphoneDevice";
const processingStorageKey = "paseoVoice.audioProcessing";
const soundStorageKey = "paseoVoice.soundCues";
const shortcutStorageKey = "paseoVoice.shortcuts";
const MAX_PROVIDER_TRANSCRIPT_DELTA_CODE_POINTS = 4_000;
const MAX_TRANSCRIPT_ENTRY_CODE_POINTS = 32_000;
const MAX_TRANSCRIPT_ENTRIES = 64;
const MAX_BROKER_MESSAGE_CODE_POINTS = 240;
const MAX_DICTATION_TEXT_CODE_POINTS = 32_000;
const MAX_ACTIVITY_MESSAGE_CODE_POINTS = 240;
const MAX_ACTIVITY_ENTRIES = 128;

function storedVoiceMode() {
  try {
    return localStorage.getItem(voiceModeStorageKey) === "dictation"
      ? "dictation"
      : "live_response";
  } catch {
    return "live_response";
  }
}

function storedPreference(key) {
  try {
    return localStorage.getItem(key);
  } catch {
    return null;
  }
}

function storedJson(key, fallback) {
  try {
    return JSON.parse(localStorage.getItem(key)) ?? fallback;
  } catch {
    return fallback;
  }
}

function storePreference(key, value) {
  try {
    localStorage.setItem(key, typeof value === "string" ? value : JSON.stringify(value));
  } catch {
    // The preference remains page-scoped when browser storage is unavailable.
  }
}

function removePreference(key) {
  try {
    localStorage.removeItem(key);
  } catch {
    // The preference remains page-scoped when browser storage is unavailable.
  }
}

let socket = null;
let handledSocketClose = null;
let reconnectTimer = null;
let pageCleanupPromise = Promise.resolve();
let finalPageTeardownPromise = null;
let reloadRequired = false;
let draftAwaitingReconnect = false;
let audioContext = null;
let captureNode = null;
let playbackNode = null;
let playbackController = null;
let microphoneStream = null;
let microphoneSource = null;
let microphoneTrackListenerCleanup = null;
let microphonePermissionStatus = null;
let microphonePermissionListener = null;
let microphoneLifecycle = createMicrophoneLifecycleState();
let nextMicrophoneStreamToken = 0;
let nextMicrophoneRequestToken = 0;
let audioSetupPromise = null;
let audioCleanupPromise = Promise.resolve();
let usingSystemDefault = true;
let defaultDeviceFingerprint = null;
let micReady = false;
let talking = false;
let activeRecording = null;
const liveRecordings = createLiveRecordingController();
let pageCapture = createPageCaptureState();
let recordingSequence = 0;
let assistantEntry = null;
let hostAvailable = false;
let hostCount = 0;
let writeInFlight = false;
let proposalState = proposalStateFromFrame({ echo: null });
let proposalActivationLatch = null;
let interfaceState = "connecting";
let microphonePresentation = null;
let boundContext = null;
let voiceMode = storedVoiceMode();
const browserConnection = createBrowserConnectionController(voiceMode);
const dictation = createDictationController();
const textTurns = createTextTurnController();
let dictationRecording = null;
let dictationOperation = null;
let pendingDictationTerminal = null;
let reviewDictationText = null;
let partialDictationText = "";
let recordingMode = storedPreference(recordingModeStorageKey) === "toggle" ? "toggle" : "hold";
let recordingStartedAt = 0;
let lastVoiceAt = 0;
let preRollFrames = [];
let shortcuts = { hold: "Space", toggle: "KeyD", cancel: "Escape" };

recordingModeSelect.value = recordingMode;
silenceStop.checked = storedPreference(silenceStopStorageKey) !== "off";
const storedSilencePeriod = storedPreference(silencePeriodStorageKey);
silencePeriod.value = ["1000", "1600", "2500", "4000"].includes(storedSilencePeriod)
  ? storedSilencePeriod
  : "1600";
soundCues.checked = storedPreference(soundStorageKey) !== "off";
const processing = storedJson(processingStorageKey, {});
echoCancellation.checked = processing.echoCancellation !== false;
noiseSuppression.checked = processing.noiseSuppression !== false;
autoGain.checked = processing.autoGain !== false;
preRoll.checked = processing.preRoll !== false;
const storedShortcuts = validateShortcuts(storedJson(shortcutStorageKey, shortcuts));
if (storedShortcuts.valid) shortcuts = storedShortcuts.value;

for (const select of [holdShortcut, toggleShortcut, cancelShortcut]) {
  for (const [code, label] of [
    ["Space", "Space"],
    ["KeyD", "D"],
    ["KeyR", "R"],
    ["KeyC", "C"],
    ["Escape", "Escape"],
  ]) {
    const option = document.createElement("option");
    option.value = code;
    option.textContent = label;
    select.append(option);
  }
}
holdShortcut.value = shortcuts.hold;
toggleShortcut.value = shortcuts.toggle;
cancelShortcut.value = shortcuts.cancel;

const stateLabels = {
  connecting: "Connecting",
  ready: "Ready",
  listening: "Listening",
  thinking: "Thinking",
  transcribing: "Transcribing",
  cleaning: "Cleaning",
  cancelling: "Cancelling",
  speaking: "Speaking",
  "awaiting-approval": "Awaiting approval",
  disconnected: "Disconnected",
  error: "Needs attention",
};

function updatePushToTalkCopy() {
  if (talking) {
    pttLabel.textContent = "Listening...";
  } else {
    pttLabel.textContent =
      voiceMode === "dictation" && recordingMode === "toggle"
        ? "Tap to dictate"
        : voiceMode === "dictation"
          ? "Hold to dictate"
          : "Hold to talk";
  }
  $("ptt-hint").textContent =
    voiceMode === "dictation" ? "Speech is inserted into the draft" : "Or hold the space bar";
}

function presentVoiceMode(mode, persistPreference) {
  voiceMode = mode;
  voiceModeToggle.checked = mode === "live_response";
  voiceModeDescription.textContent =
    mode === "live_response"
      ? "Paseo Voice responds aloud and may use agent tools."
      : "Cleaned speech is inserted into the response draft without sending.";
  updatePushToTalkCopy();
  if (persistPreference) {
    try {
      localStorage.setItem(voiceModeStorageKey, mode);
    } catch {
      // The preference remains connection-scoped when browser storage is unavailable.
    }
  }
  syncInputState();
}

function handleVoiceModeFrame(mode) {
  const transition = browserConnection.acceptVoiceMode(mode);
  if (!transition) return false;
  presentVoiceMode(transition.mode, transition.settled);
  if (transition.control) sendSocketControl(transition.control);
  return true;
}

function setInterfaceState(state, detail = "") {
  interfaceState = stateLabels[state] ? state : "ready";
  const label = stateLabels[interfaceState];
  const readable = detail ? `${label}: ${detail}` : label;
  avatar.dataset.state = interfaceState;
  blockAvatar?.setState(interfaceState);
  avatar.setAttribute("aria-label", `Paseo Voice is ${readable.toLowerCase()}`);
  avatarState.textContent = readable;
  setPill(
    statePill,
    readable.toLowerCase(),
    interfaceState === "error" || interfaceState === "disconnected" ? "error" : "",
  );
  cancelDictationButton.classList.toggle(
    "hidden",
    voiceMode !== "dictation" ||
      !["listening", "transcribing", "cleaning"].includes(interfaceState),
  );
}

function clearActiveRecording(recording = activeRecording) {
  if (activeRecording !== recording) return;
  activeRecording = null;
  pageCapture = transitionPageCapture(pageCapture, {
    type: "capture-ended",
    recording,
  }).state;
  syncInputState();
}

function handleSocketSendFailure({ preserveDraftOnFailure = false } = {}) {
  const failedSocket = socket;
  logActivity("socket send failed; disconnecting", "error");
  try {
    failedSocket?.close();
  } catch {
    // The shared close path below still retires all local ownership.
  }
  handleSocketClose(failedSocket, { preserveDraft: preserveDraftOnFailure });
}

function sendSocketPayload(payload, options) {
  if (!socket || socket.readyState !== WebSocket.OPEN) {
    handleSocketSendFailure(options);
    return false;
  }
  const delivery = attemptSocketSend(payload, (nextPayload) => socket.send(nextPayload));
  if (!delivery.sent) handleSocketSendFailure(options);
  return delivery.sent;
}

function sendSocketControl(control, options) {
  return sendSocketPayload(JSON.stringify(control), options);
}

function sendSocketBinary(payload) {
  return sendSocketPayload(payload);
}

function resetDictationRecordingState() {
  dictationRecording = null;
  dictationOperation = null;
  pendingDictationTerminal = null;
}

function dictationTerminalControl(kind, recording) {
  return kind === "end"
    ? recordingEndControl(recording, dictationOperation)
    : dictationCancelControl(recording, dictationOperation);
}

function requestDictationTerminal(kind, recording) {
  if (recording !== dictationRecording) return false;
  const intent = Object.freeze({ kind, recording });
  pendingDictationTerminal = intent;
  const control = dictationTerminalControl(kind, recording);
  if (!control) return false;
  if (loopback) return false;
  if (!socket || socket.readyState !== WebSocket.OPEN) {
    handleSocketSendFailure();
    return false;
  }
  const delivery = attemptPendingControlSend(intent, control, (payload) => socket.send(payload));
  pendingDictationTerminal = delivery.pendingIntent;
  if (!delivery.sent) handleSocketSendFailure();
  return delivery.sent;
}

function acceptDictationOperation(operationId, recordingId) {
  const recording = dictationRecording;
  const operation = bindDictationOperation(recording, operationId, recordingId);
  if (!operation || !dictation.bindOperation(operationId, recordingId)) return false;
  dictationOperation = operation;
  const pending = pendingDictationTerminal;
  if (!pending) return true;
  if (pending.recording !== recording) {
    pendingDictationTerminal = null;
    return true;
  }
  requestDictationTerminal(pending.kind, recording);
  return true;
}

function acceptLiveRecordingState(frame) {
  if (!liveRecordings.acceptState(frame)) return false;
  syncInputState();
  return true;
}

function handleRecordingRejected(frame) {
  const activeLiveRecording = activeRecording?.mode === "live_response" ? activeRecording : null;
  const recording =
    frame?.mode === "dictation"
      ? dictationRecording
      : (activeLiveRecording ?? liveRecordings.pending);
  if (!recordingMatchesRejection(recording, frame)) return false;
  const message =
    typeof frame.message === "string" && frame.message.length > 0
      ? boundText(frame.message, MAX_BROKER_MESSAGE_CODE_POINTS)
      : "Recording was rejected by the broker.";

  if (recording.mode === "dictation") {
    if (!dictation.rejectRecording(frame.recording_id)) return false;
    stopLocalDictationCapture();
    resetDictationRecordingState();
    dictationPreviewText.textContent = message;
    dictationPreview.classList.remove("hidden");
  } else if (recording === activeLiveRecording) {
    if (!stopLocalLiveCapture()) return false;
  } else if (liveRecordings.reject(frame) !== recording) {
    return false;
  }

  clearActiveRecording(recording);
  logActivity(`recording rejected: ${message}`, "error");
  setInterfaceState("error", message);
  syncInputState();
  return true;
}

function completeBrokerDictationCancellation(operationId) {
  const recording = dictationRecording;
  if (!dictation.completeCancellation(operationId)) return false;
  stopLocalDictationCapture();
  resetDictationRecordingState();
  clearActiveRecording(recording);
  restoreMicrophoneInterfaceState();
  syncInputState();
  return true;
}

function handleDictationFailure(frame) {
  const message =
    typeof frame.message === "string" && frame.message.length > 0
      ? boundText(frame.message, MAX_BROKER_MESSAGE_CODE_POINTS)
      : "Dictation failed. The draft was not changed.";
  if (frame.pending_cancellation === true) {
    if (!dictation.markPendingCancellation(frame.operation_id)) return false;
    const recording = dictationRecording;
    stopLocalDictationCapture();
    clearActiveRecording(recording);
    dictationPreviewText.textContent = message;
    dictationPreview.classList.remove("hidden");
    logActivity("dictation failed while cancellation remains pending", "error");
    playCue("error");
    setInterfaceState("error", message);
    syncInputState();
    return true;
  }
  const recording = dictationRecording;
  if (!dictation.completeFailure(frame.operation_id)) return false;
  stopLocalDictationCapture();
  resetDictationRecordingState();
  clearActiveRecording(recording);
  partialDictationText = "";
  dictationPreviewText.textContent = message;
  dictationReviewActions.classList.add("hidden");
  dictationPreview.classList.remove("hidden");
  logActivity("dictation failed", "error");
  playCue("error");
  restoreMicrophoneInterfaceState();
  syncInputState();
  return true;
}

function stopLocalDictationCapture() {
  talking = false;
  recordingStartedAt = 0;
  lastVoiceAt = 0;
  preRollFrames = [];
  pttButton.classList.remove("active");
  updatePushToTalkCopy();
  reviewDictationText = null;
  partialDictationText = "";
  dictationReviewActions.classList.add("hidden");
}

function restoreAfterLocalDictationTermination() {
  if (!loopback && socket?.readyState !== WebSocket.OPEN) {
    setInterfaceState("disconnected");
    return;
  }
  restoreMicrophoneInterfaceState();
}

function abortLiveRecordingForRoutingChange({ sendControl = true } = {}) {
  const activeLiveRecording = activeRecording?.mode === "live_response" ? activeRecording : null;
  const recording = activeLiveRecording ?? liveRecordings.pending;
  if (!recording) return false;
  if (activeLiveRecording) stopLocalLiveCapture();
  const control = recordingAbortControl(recording);
  if (sendControl && !loopback && !sendSocketControl(control)) return true;
  if (liveRecordings.pending === recording) liveRecordings.accept();
  clearActiveRecording(recording);
  restoreMicrophoneInterfaceState();
  syncInputState();
  return true;
}

function terminateDictationForRoutingChange(current, { sendCancel = true } = {}) {
  const disposition = dictation.invalidate(current);
  if (!disposition) return null;
  const recording = activeRecording;
  stopLocalDictationCapture();
  try {
    if (!disposition.brokerCancellation) {
      resetDictationRecordingState();
      restoreAfterLocalDictationTermination();
    } else if (loopback || socket?.readyState !== WebSocket.OPEN) {
      dictation.disconnect();
      resetDictationRecordingState();
      restoreAfterLocalDictationTermination();
    } else {
      if (sendCancel) {
        requestDictationTerminal("cancel", dictationRecording);
      }
      setInterfaceState("cancelling");
    }
  } finally {
    clearActiveRecording(recording);
    syncInputState();
  }
  return disposition;
}

function currentSummaryId() {
  return displayedSummaryId(boundContext);
}

function captureBoundDraftState() {
  return createBoundDraftState({
    summaryId: currentSummaryId(),
    value: textInput.value,
    selectionStart: textInput.selectionStart ?? textInput.value.length,
    selectionEnd: textInput.selectionEnd ?? textInput.value.length,
  });
}

function applyBoundDraftState(state) {
  textInput.value = state.value;
  textInput.setSelectionRange(state.selectionStart, state.selectionEnd);
}

function bindDraftToDisplayedSummary(summaryId) {
  const current = captureBoundDraftState();
  const next = reduceBoundDraft(current, { type: "display-summary", summaryId });
  if (next !== current) applyBoundDraftState(next);
}

function clearTypedDraft(summaryId = currentSummaryId()) {
  applyBoundDraftState(
    reduceBoundDraft(captureBoundDraftState(), { type: "clear-draft", summaryId }),
  );
}

function clearBrowserContext({
  nextHostId = null,
  dictationMessage = "",
  sendCancel = true,
  disconnected = false,
  preserveDraft = false,
} = {}) {
  let dictationInvalidated = false;
  if (disconnected) {
    abortLiveRecordingForRoutingChange({ sendControl: false });
    const recording = activeRecording;
    stopLocalDictationCapture();
    dictation.disconnect();
    resetDictationRecordingState();
    clearActiveRecording(recording);
  } else {
    abortLiveRecordingForRoutingChange();
    dictationInvalidated = terminateDictationForRoutingChange(
      {
        hostId: nextHostId,
        contextId: null,
      },
      { sendCancel },
    );
  }
  boundContext = null;
  if (preserveDraft) draftAwaitingReconnect = true;
  else clearTypedDraft(null);
  boundThread.textContent = "No reply is bound";
  responseDestination.textContent = "Destination: no bound reply";
  queueCount.textContent = "0";
  agentCount.textContent = "0 agents";
  agentGrid.replaceChildren();
  dashboardEmpty.classList.remove("hidden");
  proposalState = proposalStateFromFrame({ echo: null });
  proposalActivationLatch = proposalActivationTransition(proposalActivationLatch, {
    type: "clear",
  }).latch;
  proposalText.textContent = "";
  proposalBanner.classList.add("hidden");
  writeInFlight = false;
  dictationPreviewText.textContent = "";
  dictationPreview.classList.add("hidden");
  dictationReviewActions.classList.add("hidden");
  reviewDictationText = null;
  partialDictationText = "";
  if (dictationInvalidated && dictationMessage) {
    dictationPreviewText.textContent = dictationMessage;
    dictationPreview.classList.remove("hidden");
  }
}

function currentDraftTarget() {
  const draft = captureBoundDraftState();
  return {
    value: draft.value,
    selectionStart: draft.selectionStart,
    selectionEnd: draft.selectionEnd,
    hostId: hostSelect.value || null,
    contextId: draft.summaryId,
  };
}

function updateDraft(result) {
  applyBoundDraftState(
    createBoundDraftState({
      summaryId: currentSummaryId(),
      value: result.value,
      selectionStart: result.selectionStart,
      selectionEnd: result.selectionEnd,
    }),
  );
  textInput.dispatchEvent(new Event("input", { bubbles: true }));
  textInput.focus();
}

function renderDashboard(msg) {
  const agents = Array.isArray(msg.agents) ? msg.agents : [];
  if (draftAwaitingReconnect) {
    clearTypedDraft(null);
    draftAwaitingReconnect = false;
  }
  const nextBoundContext =
    msg.bound_context && typeof msg.bound_context === "object" ? msg.bound_context : null;
  const nextSummaryId = displayedSummaryId(nextBoundContext);
  const nextHostId =
    typeof msg.selected_host_id === "string" ? msg.selected_host_id : hostSelect.value || null;
  const routingChanged =
    currentSummaryId() !== nextSummaryId || (hostSelect.value || null) !== nextHostId;
  if (routingChanged) {
    abortLiveRecordingForRoutingChange();
    if (!loopback && (!browserConnection.protocolReady || socket?.readyState !== WebSocket.OPEN)) {
      return;
    }
  }
  const dictationInvalidated = terminateDictationForRoutingChange({
    hostId: nextHostId,
    contextId: nextSummaryId,
  });
  bindDraftToDisplayedSummary(nextSummaryId);
  if (dictationInvalidated) {
    dictationPreviewText.textContent = "Dictation discarded because the bound response changed.";
    dictationPreview.classList.remove("hidden");
  }
  boundContext = nextBoundContext;
  const destination = boundContext?.thread_name || boundContext?.thread_id;
  boundThread.textContent = destination ? `Bound to ${destination}` : "No reply is bound";
  responseDestination.textContent = destination
    ? `Destination locked by broker: ${destination}`
    : "Destination: no bound reply";
  queueCount.textContent = String(Number.isSafeInteger(msg.queue_count) ? msg.queue_count : 0);
  agentCount.textContent = `${agents.length} ${agents.length === 1 ? "agent" : "agents"}`;
  agentGrid.replaceChildren();

  for (const agent of agents) {
    const card = document.createElement("article");
    card.className = "agent-card";
    const isActive = boundContext?.thread_id === agent.thread_id;
    if (isActive) card.classList.add("active");

    const header = document.createElement("div");
    header.className = "agent-card-header";
    const name = document.createElement("h3");
    name.textContent = agent.thread_name || "Untitled agent";
    const state = document.createElement("span");
    state.className = "agent-state";
    state.textContent = agent.state || "unknown";
    header.append(name, state);

    const provider = document.createElement("p");
    provider.className = "agent-provider";
    provider.textContent = agent.provider || "unknown provider";
    const summary = document.createElement("p");
    summary.className = "agent-summary";
    if (isActive) {
      const fallback = boundContext.summary_degraded === true ? "Fallback summary: " : "";
      summary.textContent = `${fallback}${boundContext.latest_summary || "Reply is ready for a response."}`;
    } else {
      summary.textContent = "No queued summary for this agent.";
    }
    const queued = document.createElement("p");
    queued.className = "agent-queue";
    const count = Number.isSafeInteger(agent.queued_response_count)
      ? agent.queued_response_count
      : 0;
    queued.textContent = `${count} ${count === 1 ? "response" : "responses"} queued`;
    card.append(header, provider, summary, queued);
    agentGrid.append(card);
  }
  dashboardEmpty.classList.toggle("hidden", agents.length > 0);
  syncInputState();
}

function brokerConversationInteractionAllowed() {
  return canStartConversationalTurn({
    proposalState,
    confirmationDispatchInFlight: writeInFlight,
  });
}

function socketReadyForConversation() {
  return (
    !reloadRequired &&
    browserConnection.conversationReady({
      loopback,
      hostAvailable,
      socketOpen: socket?.readyState === WebSocket.OPEN,
    })
  );
}

function localInteractionOwnsContext() {
  return (
    activeRecording !== null ||
    liveRecordings.pending !== null ||
    dictationOwnsContext(dictation.phase) ||
    textTurns.hasPending ||
    draftAwaitingReconnect
  );
}

function conversationInteractionAllowed() {
  return (
    brokerConversationInteractionAllowed() &&
    socketReadyForConversation() &&
    !localInteractionOwnsContext()
  );
}

function syncInputState() {
  const brokerConversationAllowed = brokerConversationInteractionAllowed();
  const connectionReady = socketReadyForConversation();
  const conversationAllowed = conversationInteractionAllowed();
  const microphoneGate = microphoneInteractionGate(
    microphoneLifecycle,
    activeRecording,
    browserConnection.voiceModeChangePending,
  );
  pttButton.disabled =
    !micReady ||
    !canStartDictation(voiceMode, currentSummaryId()) ||
    (voiceMode === "dictation" && dictation.phase === "cancelling") ||
    microphoneGate.startBlocked ||
    !conversationAllowed;
  textInput.disabled =
    !hostAvailable || !brokerConversationAllowed || !connectionReady || reloadRequired;
  textSubmitButton.disabled = !hostAvailable || !conversationAllowed;
  voiceModeToggle.disabled =
    loopback ||
    !micReady ||
    !socket ||
    socket.readyState !== WebSocket.OPEN ||
    browserConnection.voiceModeChangePending ||
    microphoneGate.controlsLocked ||
    !conversationAllowed;
  recordingModeSelect.disabled = microphoneGate.controlsLocked;
  setMicrophoneControlsEnabled(micReady && !microphoneGate.controlsLocked);
  hostSelect.disabled = hostCount < 2 || !conversationAllowed;
  confirmProposalButton.disabled = proposalState.status !== "pending" || writeInFlight;
  cancelProposalButton.disabled = proposalState.status !== "pending";
}

function logActivity(text, cls = "") {
  const line = document.createElement("div");
  line.textContent = `${new Date().toLocaleTimeString()} ${boundText(text, MAX_ACTIVITY_MESSAGE_CODE_POINTS)}`;
  if (cls) line.className = cls;
  activityBox.append(line);
  pruneOldestEntries(activityBox, MAX_ACTIVITY_ENTRIES);
  activityBox.scrollTop = activityBox.scrollHeight;
}

function boundText(value, codePointLimit) {
  if (typeof value !== "string") return "";
  const codePoints = [];
  for (const codePoint of value) {
    if (codePoints.length === codePointLimit) break;
    const codeUnit = codePoint.charCodeAt(0);
    const unpairedSurrogate = codePoint.length === 1 && codeUnit >= 0xd800 && codeUnit <= 0xdfff;
    codePoints.push(unpairedSurrogate ? "\ufffd" : codePoint);
  }
  return codePoints.join("");
}

function pruneOldestEntries(container, entryLimit, beforeRemove = null) {
  while (container.children.length > entryLimit) {
    const oldest = container.children[0];
    beforeRemove?.(oldest);
    oldest.remove();
  }
}

function addTranscript(who, text) {
  const entry = document.createElement("div");
  entry.className = `entry ${who}`;
  const label = document.createElement("div");
  label.className = "who";
  label.textContent = who === "user" ? "you" : "paseo voice";
  const body = document.createElement("div");
  body.textContent = boundText(text, MAX_TRANSCRIPT_ENTRY_CODE_POINTS);
  entry.append(label, body);
  transcriptBox.append(entry);
  pruneOldestEntries(transcriptBox, MAX_TRANSCRIPT_ENTRIES, (oldest) => {
    if (assistantEntry?.parentNode === oldest) assistantEntry = null;
  });
  transcriptBox.scrollTop = transcriptBox.scrollHeight;
  return body;
}

function setPill(el, text, cls) {
  el.textContent = text;
  el.className = `pill ${cls ?? ""}`;
}

function transitionMicrophone(event) {
  const decision = transitionMicrophoneLifecycle(microphoneLifecycle, event);
  microphoneLifecycle = decision.state;
  return decision;
}

function beginMicrophoneRequest(requestPageGeneration = pageGeneration) {
  if (!pageGenerationIsActive(requestPageGeneration)) return null;
  if (nextMicrophoneRequestToken >= Number.MAX_SAFE_INTEGER) {
    throw new Error("Microphone request limit reached; reload required.");
  }
  const requestToken = ++nextMicrophoneRequestToken;
  const decision = transitionMicrophone({
    type: "setup-started",
    requestToken,
    recording: activeRecording,
  });
  applyMicrophoneCaptureEffect(decision.capture, decision.brokerControl);
  syncInputState();
  return Object.freeze({ requestToken, pageGeneration: requestPageGeneration });
}

function microphoneRequestIsCurrent(request, streamAcquired = false) {
  return (
    request !== null &&
    pageGenerationIsActive(request.pageGeneration) &&
    microphoneTransactionDecision(microphoneLifecycle, request.requestToken, streamAcquired).current
  );
}

function setMicrophoneControlsEnabled(enabled) {
  for (const control of [microphoneSelect, echoCancellation, noiseSuppression, autoGain, preRoll]) {
    control.disabled = !enabled;
  }
}

function markMicrophoneUnavailable(message) {
  micReady = false;
  setMicrophoneControlsEnabled(false);
  microphoneStatus.textContent = message;
  syncInputState();
}

function markMicrophoneReady(message) {
  micReady = true;
  setMicrophoneControlsEnabled(true);
  microphoneStatus.textContent = message;
  setupPanel.classList.add("hidden");
  dictationSettings.classList.remove("hidden");
  syncInputState();
}

function currentPriorityInterfaceState() {
  if (proposalState.status === "pending") return "awaiting-approval";
  return ["transcribing", "cleaning", "cancelling"].includes(dictation.phase)
    ? dictation.phase
    : null;
}

function setMicrophoneInterfaceState(state, detail = "") {
  const requested = Object.freeze({ state, detail });
  const transition = microphonePresentationTransition(
    microphonePresentation,
    currentPriorityInterfaceState() ?? interfaceState,
    requested,
  );
  microphonePresentation = transition.stored;
  if (transition.visible) {
    setInterfaceState(transition.visible.state, transition.visible.detail);
  }
}

function restoreMicrophoneInterfaceState(fallbackState = "ready", fallbackDetail = "") {
  let requested = microphonePresentation;
  if (microphoneLifecycle.phase === "ready" && micReady) {
    if (requested?.state !== "ready") requested = Object.freeze({ state: "ready", detail: "" });
  } else if (
    ["setting-up", "checking-loss", "reconnecting-default", "retry-required"].includes(
      microphoneLifecycle.phase,
    )
  ) {
    if (requested?.state !== "error") {
      requested = Object.freeze({
        state: "error",
        detail: microphoneStatus.textContent || "microphone unavailable",
      });
    }
  }
  requested ??= Object.freeze({ state: fallbackState, detail: fallbackDetail });
  const priorityState = currentPriorityInterfaceState();
  const transition = microphonePresentationTransition(
    microphonePresentation,
    priorityState ?? fallbackState,
    requested,
  );
  microphonePresentation = transition.stored;
  if (priorityState) {
    setInterfaceState(priorityState);
    return;
  }
  if (transition.visible) {
    setInterfaceState(transition.visible.state, transition.visible.detail);
  }
}

function stopLocalLiveCapture() {
  if (!talking) return false;
  talking = false;
  recordingStartedAt = 0;
  lastVoiceAt = 0;
  preRollFrames = [];
  pttButton.classList.remove("active");
  updatePushToTalkCopy();
  return true;
}

function abortLiveCapture(recording, brokerControl) {
  if (activeRecording !== recording || !stopLocalLiveCapture()) return false;
  try {
    if (!loopback && brokerControl) sendSocketControl(brokerControl);
  } finally {
    clearActiveRecording(recording);
  }
  return true;
}

function applyMicrophoneCaptureEffect(effect, brokerControl) {
  if (effect === "cancel-dictation") {
    cancelDictation({ message: "Dictation cancelled because microphone access ended." });
  } else if (effect === "abort-live") {
    const recording = activeRecording;
    abortLiveCapture(recording, brokerControl);
  }
}

function detachMicrophonePermissionWatcher() {
  if (microphonePermissionStatus && microphonePermissionListener) {
    unlisten(microphonePermissionStatus, "change", microphonePermissionListener);
  }
  microphonePermissionStatus = null;
  microphonePermissionListener = null;
}

async function microphonePermissionState() {
  if (!navigator.permissions?.query) return "unknown";
  try {
    const status = await navigator.permissions.query({ name: "microphone" });
    return ["granted", "prompt", "denied"].includes(status.state) ? status.state : "unknown";
  } catch {
    return "unknown";
  }
}

async function watchMicrophonePermission(streamToken, permissionPageGeneration) {
  if (!pageGenerationIsActive(permissionPageGeneration) || !navigator.permissions?.query) return;
  let status;
  try {
    status = await navigator.permissions.query({ name: "microphone" });
  } catch {
    // Track and device events remain the fallback where Permissions API support is incomplete.
    return;
  }
  if (
    !pageGenerationIsActive(permissionPageGeneration) ||
    microphoneLifecycle.phase !== "ready" ||
    microphoneLifecycle.streamToken !== streamToken
  ) {
    return;
  }
  if (status.state === "denied") {
    dispatchMicrophoneLifecycleEvent({
      type: "permission-changed",
      streamToken,
      permissionState: status.state,
      recording: activeRecording,
    });
    return;
  }
  if (status.state !== "granted") return;
  detachMicrophonePermissionWatcher();
  const listener = () => {
    dispatchMicrophoneLifecycleEvent({
      type: "permission-changed",
      streamToken,
      permissionState: status.state,
      recording: activeRecording,
    });
  };
  listen(status, "change", listener);
  microphonePermissionStatus = status;
  microphonePermissionListener = listener;
}

async function enumerateMicrophones() {
  const devices = await navigator.mediaDevices.enumerateDevices();
  return devices.filter((device) => device.kind === "audioinput");
}

function disconnectAudioNode(node) {
  try {
    node?.disconnect();
  } catch {
    // A partially constructed graph may not have reached a connected state.
  }
}

function stopMediaStream(stream) {
  try {
    stream?.getTracks().forEach((track) => track.stop());
  } catch {
    // Resource ownership is still released even if a browser track is already unusable.
  }
}

async function disposeAudioResources() {
  detachMicrophonePermissionWatcher();
  const trackListenerCleanup = microphoneTrackListenerCleanup;
  const context = audioContext;
  const stream = microphoneStream;
  const source = microphoneSource;
  const capture = captureNode;
  const playback = playbackNode;
  const playbackFlow = playbackController;
  const micAnalyser = avatarMicAnalyser;
  const voiceAnalyser = avatarVoiceAnalyser;

  audioContext = null;
  microphoneStream = null;
  microphoneSource = null;
  microphoneTrackListenerCleanup = null;
  captureNode = null;
  playbackNode = null;
  playbackController = null;
  avatarMicAnalyser = null;
  avatarVoiceAnalyser = null;
  usingSystemDefault = true;
  defaultDeviceFingerprint = null;
  preRollFrames = [];
  if (capture) {
    capture.port.onmessage = null;
    try {
      capture.port.postMessage({ type: "set-active", active: false });
    } catch {
      // Closing the context below remains the terminal capture boundary.
    }
  }
  if (playbackFlow) {
    try {
      playbackFlow.dispose();
    } catch {
      // Disconnecting and closing the graph below still drops playback.
    }
  }
  disconnectAudioNode(source);
  disconnectAudioNode(capture);
  disconnectAudioNode(playback);
  disconnectAudioNode(micAnalyser);
  disconnectAudioNode(voiceAnalyser);
  trackListenerCleanup?.();
  stopMediaStream(stream);
  blockAvatar?.setAnalysers({ mic: null, voice: null });
  const previousCleanup = audioCleanupPromise;
  audioCleanupPromise = (async () => {
    await previousCleanup;
    if (context && context.state !== "closed") {
      try {
        await context.close();
      } catch {
        // Cleanup is best effort after all references and tracks have already been released.
      }
    }
  })();
  await audioCleanupPromise;
}

async function enterMicrophoneRetry(message, retryPageGeneration) {
  markMicrophoneUnavailable(message);
  setupPanel.classList.remove("hidden");
  enableMicrophoneButton.textContent = "Retry microphone";
  enableMicrophoneButton.disabled = true;
  setMicrophoneInterfaceState("error", "microphone unavailable; retry required");
  await disposeAudioResources();
  if (
    pageGenerationIsActive(retryPageGeneration) &&
    microphoneLifecycle.phase === "retry-required"
  ) {
    enableMicrophoneButton.disabled = false;
  }
}

async function inspectMicrophoneLoss(streamToken, inspectionPageGeneration) {
  let permissionState = await microphonePermissionState();
  if (
    !pageGenerationIsActive(inspectionPageGeneration) ||
    microphoneLifecycle.streamToken !== streamToken
  ) {
    return;
  }
  let devices = [];
  const followsDefault =
    microphoneLifecycle.streamToken === streamToken &&
    microphoneLifecycle.selectedDeviceId === null;
  if (permissionState === "unknown" || (permissionState === "granted" && !followsDefault)) {
    try {
      devices = await enumerateMicrophones();
      if (
        !pageGenerationIsActive(inspectionPageGeneration) ||
        microphoneLifecycle.streamToken !== streamToken
      ) {
        return;
      }
      permissionState = effectiveMicrophonePermission(permissionState, devices);
    } catch {
      permissionState = "unknown";
    }
  }
  if (
    !pageGenerationIsActive(inspectionPageGeneration) ||
    microphoneLifecycle.streamToken !== streamToken
  ) {
    return;
  }
  dispatchMicrophoneLifecycleEvent({
    type: "loss-inspected",
    streamToken,
    permissionState,
    deviceIds: devices.map((device) => device.deviceId),
  });
}

async function reconnectSystemDefault() {
  const requestPageGeneration = pageGeneration;
  let request = null;
  try {
    request = beginMicrophoneRequest(requestPageGeneration);
    if (!request) return;
    const connection = await connectMicrophone(null, currentProcessingPreferences(), request);
    if (connection.status !== "active") {
      if (connection.status === "ended") {
        await audioCleanupPromise;
      }
      return;
    }
    if (
      !(await completeMicrophoneConnection(connection, {
        message: "Selected microphone disappeared. Using the system default.",
      }))
    ) {
      return;
    }
    if (!microphoneConnectionIsCurrent(connection)) return;
    logActivity("selected microphone disappeared; using system default");
  } catch (error) {
    if (!request) {
      if (!pageGenerationIsActive(requestPageGeneration)) return;
      logActivity(`microphone fallback failed: ${error.message}`, "error");
      return;
    }
    if (!microphoneRequestIsCurrent(request)) return;
    await failMicrophoneRequest(
      request,
      error,
      `Microphone fallback failed: ${error.message}. Select Retry microphone to continue.`,
    );
  }
}

function dispatchMicrophoneLifecycleEvent(event) {
  const decision = transitionMicrophone(event);
  applyMicrophoneCaptureEffect(decision.capture, decision.brokerControl);
  if (decision.recovery !== "none") playCue("error");
  if (decision.recovery === "inspect-loss") {
    markMicrophoneUnavailable("Microphone access ended. Checking available devices...");
    setMicrophoneInterfaceState("error", "checking microphone access");
    void inspectMicrophoneLoss(event.streamToken, pageGeneration);
  } else if (decision.recovery === "reconnect-default") {
    markMicrophoneUnavailable("Selected microphone disappeared. Reconnecting to system default...");
    setMicrophoneInterfaceState("error", "reconnecting microphone");
    void reconnectSystemDefault();
  } else if (decision.recovery === "require-retry") {
    void enterMicrophoneRetry(
      "Microphone access ended. Select Retry microphone to continue.",
      pageGeneration,
    );
  }
}

function handleCapturedAudio(event) {
  if (!talking) {
    if (preRoll.checked && event.data instanceof ArrayBuffer) {
      preRollFrames.push(event.data);
      if (preRollFrames.length > 30) preRollFrames.shift();
    }
    return;
  }
  if (loopback) {
    playbackController?.enqueue(event.data);
    return;
  }
  sendSocketBinary(event.data);
  if (
    activeRecording?.mode === "dictation" &&
    recordingMode === "toggle" &&
    silenceStop.checked &&
    event.data instanceof ArrayBuffer
  ) {
    const samples = new Int16Array(event.data);
    if (samples.some((sample) => Math.abs(sample) > 500)) lastVoiceAt = performance.now();
    const now = performance.now();
    if (now - recordingStartedAt > 600 && now - lastVoiceAt > Number(silencePeriod.value)) {
      stopTalking();
    }
  }
}

function pageGenerationIsActive(generation) {
  return pageIsActive() && generation === pageGeneration;
}

async function initAudio(setupPageGeneration) {
  let request = null;
  try {
    request = beginMicrophoneRequest(setupPageGeneration);
    if (!request) return;
    markMicrophoneUnavailable("Requesting microphone access...");
    await disposeAudioResources();
    if (!microphoneRequestIsCurrent(request)) return;
    audioContext = new AudioContext({ sampleRate: 24000 });
    await audioContext.audioWorklet.addModule("pcm-capture-worklet.js");
    if (!microphoneRequestIsCurrent(request)) return;
    await audioContext.audioWorklet.addModule("pcm-playback-worklet.js");
    if (!microphoneRequestIsCurrent(request)) return;

    playbackNode = new AudioWorkletNode(audioContext, "pcm-playback", {
      numberOfInputs: 0,
      outputChannelCount: [1],
    });
    playbackController = createPlaybackController(playbackNode.port, {
      onOverflow() {
        logActivity(
          "Audio playback stopped because the response exceeded the local buffer.",
          "error",
        );
        setInterfaceState("error", "audio playback buffer exceeded");
        playCue("error");
      },
    });
    playbackNode.connect(audioContext.destination);
    captureNode = new AudioWorkletNode(audioContext, "pcm-capture", {
      numberOfOutputs: 0,
    });
    captureNode.port.onmessage = handleCapturedAudio;

    avatarVoiceAnalyser = audioContext.createAnalyser();
    avatarVoiceAnalyser.fftSize = 512;
    playbackNode.connect(avatarVoiceAnalyser);
    avatarMicAnalyser = audioContext.createAnalyser();
    avatarMicAnalyser.fftSize = 512;
    blockAvatar?.setAnalysers({ mic: avatarMicAnalyser, voice: avatarVoiceAnalyser });

    const preferredDevice = storedPreference(deviceStorageKey);
    const processingPreference = currentProcessingPreferences();
    let usedFallback = false;
    let connection;
    try {
      connection = await connectMicrophone(preferredDevice, processingPreference, request);
    } catch (error) {
      if (!preferredDevice) throw error;
      if (!microphoneRequestIsCurrent(request)) return;
      const permissionState = await microphonePermissionState();
      if (!microphoneRequestIsCurrent(request)) return;
      if (!canFallbackFromSavedDevice(error.name, permissionState)) throw error;
      request = beginMicrophoneRequest(setupPageGeneration);
      if (!request) return;
      connection = await connectMicrophone(null, processingPreference, request);
      usedFallback = true;
    }
    if (connection.status !== "active") {
      if (connection.status === "ended") await audioCleanupPromise;
      return;
    }
    if (!microphoneRequestIsCurrent(request, true)) return;
    captureNode.port.postMessage({ type: "set-active", active: true });
    const message = usedFallback
      ? "Saved microphone unavailable. Using the system default."
      : microphoneLifecycle.selectedDeviceId
        ? "Selected microphone active."
        : "System default active.";
    if (!(await completeMicrophoneConnection(connection, { message, processingPreference }))) {
      return;
    }
    if (!microphoneConnectionIsCurrent(connection)) return;
    logActivity(loopback ? "microphone ready (loopback mode, no server)" : "microphone ready");
  } catch (error) {
    if (!request) {
      if (pageGenerationIsActive(setupPageGeneration)) throw error;
      return;
    }
    if (!microphoneRequestIsCurrent(request)) return;
    await failMicrophoneRequest(
      request,
      error,
      `Microphone unavailable: ${error.message}. Select Retry microphone to continue.`,
    );
  }
}

function requestMicrophoneSetup() {
  if (!pageIsActive() || audioSetupPromise) return;
  const setupPageGeneration = pageGeneration;
  enableMicrophoneButton.disabled = true;
  audioSetupPromise = initAudio(setupPageGeneration)
    .catch((error) => {
      if (!pageGenerationIsActive(setupPageGeneration)) return;
      logActivity(`microphone failed: ${error.message}`, "error");
      setupPanel.classList.remove("hidden");
      enableMicrophoneButton.textContent = "Retry microphone";
      microphoneStatus.textContent = `Microphone unavailable: ${error.message}`;
      setMicrophoneInterfaceState("error", "microphone unavailable; retry required");
      syncInputState();
    })
    .finally(() => {
      audioSetupPromise = null;
      if (pageIsActive()) enableMicrophoneButton.disabled = false;
    });
}

function currentProcessingPreferences() {
  return Object.freeze({
    echoCancellation: echoCancellation.checked,
    noiseSuppression: noiseSuppression.checked,
    autoGain: autoGain.checked,
    preRoll: preRoll.checked,
  });
}

function restoreStoredProcessingPreferences() {
  const stored = storedJson(processingStorageKey, {});
  echoCancellation.checked = stored.echoCancellation !== false;
  noiseSuppression.checked = stored.noiseSuppression !== false;
  autoGain.checked = stored.autoGain !== false;
  preRoll.checked = stored.preRoll !== false;
}

function microphoneConstraints(deviceId, processingPreference) {
  return buildMicrophoneConstraints(deviceId, {
    echoCancellation: processingPreference.echoCancellation,
    noiseSuppression: processingPreference.noiseSuppression,
    autoGainControl: processingPreference.autoGain,
  });
}

async function connectMicrophone(deviceId, processingPreference, request) {
  if (!microphoneRequestIsCurrent(request)) return Object.freeze({ status: "stale" });
  let stream;
  try {
    stream = await navigator.mediaDevices.getUserMedia({
      audio: microphoneConstraints(deviceId, processingPreference),
    });
  } catch (error) {
    if (!microphoneRequestIsCurrent(request)) return Object.freeze({ status: "stale" });
    throw error;
  }

  if (!microphoneRequestIsCurrent(request, true)) {
    stopMediaStream(stream);
    return Object.freeze({ status: "stale" });
  }

  let source = null;
  try {
    if (!audioContext || !captureNode) throw new Error("Audio capture is not initialised.");
    const track = stream.getAudioTracks()[0];
    if (!track) throw new Error("Microphone stream did not provide an audio track.");
    const selectedId = track.getSettings().deviceId || "";
    const preference = persistedDeviceId(deviceId, selectedId);
    const streamToken = ++nextMicrophoneStreamToken;

    microphoneLifecycle = transitionMicrophoneLifecycle(microphoneLifecycle, {
      type: "connected",
      requestToken: request.requestToken,
      streamToken,
      selectedDeviceId: deviceId,
    }).state;
    if (track.readyState === "ended") {
      dispatchMicrophoneLifecycleEvent({
        type: "track-ended",
        streamToken,
        recording: activeRecording,
      });
      stopMediaStream(stream);
      return Object.freeze({ status: "ended" });
    }

    source = audioContext.createMediaStreamSource(stream);
    source.connect(captureNode);
    if (avatarMicAnalyser) source.connect(avatarMicAnalyser);
    if (!microphoneRequestIsCurrent(request, true) || track.readyState === "ended") {
      if (track.readyState === "ended") {
        dispatchMicrophoneLifecycleEvent({
          type: "track-ended",
          streamToken,
          recording: activeRecording,
        });
      }
      disconnectAudioNode(source);
      stopMediaStream(stream);
      return Object.freeze({ status: track.readyState === "ended" ? "ended" : "stale" });
    }

    const previousStream = microphoneStream;
    const previousSource = microphoneSource;
    const previousTrackListenerCleanup = microphoneTrackListenerCleanup;
    microphoneTrackListenerCleanup = null;
    previousTrackListenerCleanup?.();
    microphoneTrackListenerCleanup = listen(track, "ended", () => {
      dispatchMicrophoneLifecycleEvent({
        type: "track-ended",
        streamToken,
        recording: activeRecording,
      });
    });
    microphoneStream = stream;
    microphoneSource = source;
    usingSystemDefault = !deviceId;
    disconnectAudioNode(previousSource);
    stopMediaStream(previousStream);
    return Object.freeze({
      status: "active",
      requestToken: request.requestToken,
      pageGeneration: request.pageGeneration,
      streamToken,
      stream,
      track,
      preference,
    });
  } catch (error) {
    disconnectAudioNode(source);
    stopMediaStream(stream);
    throw error;
  }
}

function pendingMicrophoneConnectionIsCurrent(connection) {
  return (
    connection.status === "active" &&
    pageGenerationIsActive(connection.pageGeneration) &&
    microphoneTransactionDecision(microphoneLifecycle, connection.requestToken, true).current &&
    microphoneLifecycle.streamToken === connection.streamToken &&
    microphoneStream === connection.stream
  );
}

function microphoneConnectionIsCurrent(connection) {
  return (
    connection.status === "active" &&
    pageGenerationIsActive(connection.pageGeneration) &&
    microphoneLifecycle.phase === "ready" &&
    microphoneLifecycle.streamToken === connection.streamToken &&
    microphoneStream === connection.stream
  );
}

async function enhanceMicrophoneConnection(connection) {
  if (!pageIsActive()) return;
  try {
    const devices = await enumerateMicrophones();
    if (microphoneConnectionIsCurrent(connection)) {
      refreshMicrophones(devices);
      if (usingSystemDefault) {
        defaultDeviceFingerprint = defaultMicrophoneTransition(
          defaultDeviceFingerprint,
          devices,
        ).fingerprint;
      }
    }
  } catch (error) {
    if (microphoneConnectionIsCurrent(connection)) {
      microphoneStatus.textContent = `Microphone active; device list unavailable: ${error.message}`;
    }
  }
  if (microphoneConnectionIsCurrent(connection)) {
    try {
      await watchMicrophonePermission(connection.streamToken, connection.pageGeneration);
    } catch {
      // Permission observation is optional after a working stream is active.
    }
  }
}

async function completeMicrophoneConnection(
  connection,
  { message, processingPreference = null } = {},
) {
  if (!pendingMicrophoneConnectionIsCurrent(connection)) return false;
  if (connection.track.readyState === "ended") {
    dispatchMicrophoneLifecycleEvent({
      type: "track-ended",
      streamToken: connection.streamToken,
      recording: activeRecording,
    });
    await audioCleanupPromise;
    return false;
  }

  microphoneLifecycle = transitionMicrophoneLifecycle(microphoneLifecycle, {
    type: "configuration-complete",
    requestToken: connection.requestToken,
    streamToken: connection.streamToken,
  }).state;
  if (!microphoneConnectionIsCurrent(connection)) return false;

  if (connection.preference) storePreference(deviceStorageKey, connection.preference);
  else removePreference(deviceStorageKey);
  if (!usingSystemDefault) defaultDeviceFingerprint = null;
  if (processingPreference) storePreference(processingStorageKey, processingPreference);
  markMicrophoneReady(message);
  setMicrophoneInterfaceState("ready");
  void enhanceMicrophoneConnection(connection);
  return true;
}

async function failMicrophoneRequest(request, error, message) {
  if (!microphoneRequestIsCurrent(request)) return false;
  const decision = transitionMicrophone({
    type: "setup-failed",
    requestToken: request.requestToken,
    recording: activeRecording,
  });
  applyMicrophoneCaptureEffect(decision.capture, decision.brokerControl);
  logActivity(`microphone setup failed: ${error.message}`, "error");
  playCue("error");
  await enterMicrophoneRetry(message, request.pageGeneration);
  return true;
}

function refreshMicrophones(devices) {
  const activeId = microphoneStream?.getAudioTracks()[0]?.getSettings().deviceId || "";
  microphoneSelect.replaceChildren();
  const defaultOption = document.createElement("option");
  defaultOption.value = "";
  defaultOption.textContent = "System default";
  defaultOption.selected = usingSystemDefault;
  microphoneSelect.append(defaultOption);
  devices.forEach((device, index) => {
    if (!device.deviceId || device.deviceId === "default") return;
    const option = document.createElement("option");
    option.value = device.deviceId;
    option.textContent = device.label || `Microphone ${index + 1}`;
    option.selected = !usingSystemDefault && device.deviceId === activeId;
    microphoneSelect.append(option);
  });
}

function playCue(kind) {
  if (!audioContext) return;
  const frequency = cueFrequency(kind, soundCues.checked);
  if (frequency === null) return;
  const oscillator = audioContext.createOscillator();
  const gain = audioContext.createGain();
  oscillator.frequency.value = frequency;
  gain.gain.setValueAtTime(0.025, audioContext.currentTime);
  gain.gain.exponentialRampToValueAtTime(0.001, audioContext.currentTime + 0.08);
  oscillator.connect(gain).connect(audioContext.destination);
  oscillator.start();
  oscillator.stop(audioContext.currentTime + 0.08);
}

function flushPlayback() {
  playbackController?.flush();
}

function startTalking() {
  const summaryId = currentSummaryId();
  if (
    microphoneInteractionGate(
      microphoneLifecycle,
      activeRecording,
      browserConnection.voiceModeChangePending,
    ).startBlocked ||
    !conversationInteractionAllowed() ||
    (!loopback &&
      (!hostAvailable ||
        !browserConnection.protocolReady ||
        !browserConnection.initialVoiceModeReceived ||
        browserConnection.voiceModeChangePending ||
        socket?.readyState !== WebSocket.OPEN)) ||
    !micReady ||
    talking ||
    activeRecording !== null ||
    !canStartDictation(voiceMode, summaryId) ||
    (voiceMode === "dictation" && !["ready", "error"].includes(interfaceState))
  )
    return null;
  const mode = voiceMode;
  const allocation = allocateRecording(recordingSequence, mode, summaryId);
  if (!allocation) {
    logActivity("recording ID limit reached; reload required", "error");
    setInterfaceState("error", "recording limit reached; reload required");
    return null;
  }
  const recording = allocation.recording;
  const startControl = allocation.startControl;
  if (mode === "dictation") {
    if (!dictation.begin(currentDraftTarget(), recording.recordingId)) return null;
    dictationRecording = recording;
    dictationOperation = null;
    pendingDictationTerminal = null;
  }
  recordingSequence = allocation.sequence;
  activeRecording = recording;
  talking = true;
  recordingStartedAt = performance.now();
  lastVoiceAt = recordingStartedAt;
  if (mode === "dictation") {
    reviewDictationText = null;
    partialDictationText = "";
    dictationReviewActions.classList.add("hidden");
    dictationPreviewText.textContent = "Listening...";
    dictationPreview.classList.remove("hidden");
  }
  pttButton.classList.add("active");
  updatePushToTalkCopy();
  syncInputState();
  setInterfaceState("listening");
  playCue("start");
  flushPlayback();
  if (!loopback) {
    if (!sendSocketControl(startControl)) {
      preRollFrames = [];
      return null;
    }
    if (preRoll.checked) {
      for (const frame of preRollFrames) {
        if (!sendSocketBinary(frame)) {
          preRollFrames = [];
          return null;
        }
      }
    }
  }
  preRollFrames = [];
  return recording;
}

function cancelDictation({ restore = true, message = "Dictation cancelled." } = {}) {
  const disposition = dictation.cancel();
  if (!disposition) return;
  const recording = activeRecording;
  stopLocalDictationCapture();
  if (restore) {
    updateDraft({
      value: disposition.target.value,
      selectionStart: disposition.target.selectionStart,
      selectionEnd: disposition.target.selectionEnd,
    });
  }
  dictationPreviewText.textContent = message;
  dictationPreview.classList.remove("hidden");
  try {
    if (!disposition.brokerCancellation) {
      resetDictationRecordingState();
      restoreAfterLocalDictationTermination();
    } else if (loopback || socket?.readyState !== WebSocket.OPEN) {
      dictation.disconnect();
      resetDictationRecordingState();
      restoreAfterLocalDictationTermination();
    } else {
      requestDictationTerminal("cancel", dictationRecording);
      setInterfaceState("cancelling");
    }
  } finally {
    clearActiveRecording(recording);
    syncInputState();
  }
}

function stopTalking() {
  const recording = activeRecording;
  if (!talking || !recording) return;
  const track = microphoneStream?.getAudioTracks()[0];
  if (!track || track.readyState === "ended") {
    dispatchMicrophoneLifecycleEvent({
      type: "track-ended",
      streamToken: microphoneLifecycle.streamToken,
      recording,
    });
    return;
  }
  if (recording.mode === "dictation" && dictation.phase !== "recording") {
    talking = false;
    pttButton.classList.remove("active");
    updatePushToTalkCopy();
    clearActiveRecording(recording);
    return;
  }
  if (recording.mode === "dictation" && performance.now() - recordingStartedAt < 250) {
    cancelDictation({ message: "No change. Hold a little longer to dictate." });
    return;
  }
  if (recording.mode === "dictation" && !dictation.release()) return;
  talking = false;
  pttButton.classList.remove("active");
  updatePushToTalkCopy();
  setInterfaceState(recording.mode === "dictation" ? "transcribing" : "thinking");
  playCue("stop");
  try {
    if (recording.mode === "dictation") {
      if (!loopback) requestDictationTerminal("end", recording);
    } else if (!loopback) {
      if (!liveRecordings.release(recording)) return;
      sendSocketControl(recordingEndControl(recording));
    } else {
      restoreMicrophoneInterfaceState();
    }
  } finally {
    clearActiveRecording(recording);
  }
}

function handleTextTurnAccepted(frame) {
  const result = textTurns.accept(frame.turn_id, captureBoundDraftState());
  if (!result) return false;
  addTranscript("user", result.text);
  if (result.clearedDraft) applyBoundDraftState(result.clearedDraft);
  syncInputState();
  return true;
}

function handleTextTurnRejected(frame) {
  if (!textTurns.reject(frame.turn_id)) return false;
  const message =
    typeof frame.message === "string" && frame.message.length > 0
      ? boundText(frame.message, MAX_BROKER_MESSAGE_CODE_POINTS)
      : "Typed turn was rejected. The draft was not changed.";
  logActivity(`typed turn rejected: ${message}`, "error");
  setInterfaceState("error", message);
  syncInputState();
  return true;
}

function handleProposalFrame(frame) {
  const previousStatus = proposalState.status;
  const focusedElement = document.activeElement;
  const proposalWasPending = previousStatus === "pending";
  const proposalActionFocused =
    focusedElement === confirmProposalButton || focusedElement === cancelProposalButton;
  const focusedControlWillBeDisabled = [
    pttButton,
    textInput,
    textSubmitButton,
    voiceModeToggle,
    hostSelect,
  ].includes(focusedElement);
  const transition = proposalFrameTransition(proposalState, frame);

  if (transition.instruction === "preserve-and-log") {
    logActivity(
      "invalid proposal frame; retained current approval and blocked conversation",
      "error",
    );
    return;
  }

  const nextProposalState = transition.proposalState;
  proposalState = nextProposalState;
  proposalText.textContent = nextProposalState.echo ?? "";
  proposalBanner.classList.toggle("hidden", nextProposalState.status !== "pending");
  if (transition.instruction === "reconnect") {
    writeInFlight = false;
    logActivity("invalid proposal frame without current approval; reconnecting", "error");
    syncInputState();
    socket?.close();
    return;
  }
  if (nextProposalState.status === "pending") {
    writeInFlight = false;
    setInterfaceState("awaiting-approval");
  } else if (nextProposalState.status === "clear") {
    writeInFlight = false;
    if (interfaceState === "awaiting-approval") restoreMicrophoneInterfaceState();
  }
  syncInputState();

  if (
    nextProposalState.status === "pending" &&
    shouldFocusProposalConfirm({
      proposalWasPending,
      focusedControlWillBeDisabled,
      proposalActionFocused,
    })
  ) {
    confirmProposalButton.focus();
  }
}

function handleProtocolReady(frame) {
  if (reloadRequired || !browserConnection.acceptProtocol(frame)) return false;
  syncInputState();
  return true;
}

function handleProtocolMismatch(frame) {
  if (reloadRequired || !isProtocolMismatchFrame(frame)) return false;
  reloadRequired = true;
  const message =
    typeof frame.message === "string" && frame.message.length > 0
      ? boundText(frame.message, MAX_BROKER_MESSAGE_CODE_POINTS)
      : "Browser protocol changed. Reload required.";
  logActivity(`reload required: ${message}`, "error");
  const failedSocket = socket;
  try {
    failedSocket?.close();
  } catch {
    // The common close path below still clears all local ownership.
  }
  handleSocketClose(failedSocket);
  return true;
}

function handleGenericError(frame) {
  const message =
    typeof frame.message === "string" && frame.message.length > 0
      ? boundText(frame.message, MAX_BROKER_MESSAGE_CODE_POINTS)
      : "Broker error.";
  if (browserConnection.voiceModeChangePending && isVoiceModeSelectionError(message)) {
    browserConnection.rejectVoiceModeChange();
    voiceModeToggle.checked = voiceMode === "live_response";
    syncInputState();
  }
  logActivity(`error: ${message}`, "error");
  playCue("error");
  setInterfaceState("error", message);
}

function handleServerJson(msg) {
  switch (msg.type) {
    case "protocol_ready":
      handleProtocolReady(msg);
      return;
    case "protocol_mismatch":
      handleProtocolMismatch(msg);
      return;
    case "mode":
      setPill(modePill, `mode: ${msg.mode}`, msg.mode === "real" ? "ok" : "warn");
      return;
    case "voice_mode":
      handleVoiceModeFrame(msg.mode);
      return;
    case "dictation_capabilities": {
      const stt = msg.speech_to_text || {};
      const cleanup = msg.cleanup || {};
      $("stt-provider").textContent =
        `${stt.label || "Unavailable"} (${stt.model_id || "no model"})`;
      $("stt-provider-status").textContent =
        `${stt.processing_location || "unknown location"}, ${stt.status || "unknown"}`;
      $("cleanup-provider").textContent =
        `${cleanup.label || "Unavailable"} (${cleanup.model_id || "no model"})`;
      $("cleanup-provider-status").textContent =
        `${cleanup.processing_location || "unknown location"}, ${cleanup.status || "unknown"}`;
      return;
    }
    case "state": {
      acceptLiveRecordingState(msg);
      const brokerState = msg.state === "responding" ? "thinking" : msg.state;
      const nextState = proposalAwareInterfaceState(proposalState, brokerState);
      if (nextState === "awaiting-approval") {
        setInterfaceState(nextState);
        return;
      }
      if (voiceMode === "dictation" && !dictation.acceptsState(nextState)) return;
      if (
        voiceMode === "dictation" &&
        ["transcribing", "cleaning"].includes(nextState) &&
        !dictation.advance(nextState)
      ) {
        return;
      }
      if (nextState === "ready") {
        playbackController?.recover();
        restoreMicrophoneInterfaceState();
        assistantEntry = null;
        return;
      }
      setInterfaceState(nextState, msg.detail || "");
      if (msg.state !== "responding") assistantEntry = null;
      return;
    }
    case "host_state": {
      const hosts = Array.isArray(msg.hosts) ? msg.hosts : [];
      const previousHostId = hostSelect.value || null;
      const nextHostId = typeof msg.selected_host_id === "string" ? msg.selected_host_id : null;
      if (
        nextHostId &&
        ((previousHostId && previousHostId !== nextHostId) ||
          (dictation.target && dictation.target.hostId !== nextHostId))
      ) {
        clearBrowserContext({
          nextHostId,
          dictationMessage: "Dictation discarded because the selected host changed.",
          sendCancel: false,
        });
      }
      hostCount = hosts.length;
      hostSelect.replaceChildren();
      for (const host of hosts) {
        const option = document.createElement("option");
        option.value = host.id;
        option.textContent = host.available ? host.label : `${host.label} (unavailable)`;
        option.disabled = !host.available && host.id !== msg.selected_host_id;
        option.selected = host.id === msg.selected_host_id;
        hostSelect.append(option);
      }
      const selected = hosts.find((host) => host.id === msg.selected_host_id);
      hostAvailable = selected?.available === true;
      hostCwd.textContent = selected?.default_cwd ?? "?";
      hostProvider.textContent = selected?.default_provider ?? "?";
      hostSelect.disabled = hostCount < 2 || writeInFlight;
      syncInputState();
      return;
    }
    case "dashboard_state":
      renderDashboard(msg);
      return;
    case "transcript_delta":
      setInterfaceState("speaking");
      if (!assistantEntry) assistantEntry = addTranscript("assistant", "");
      assistantEntry.textContent = boundText(
        `${assistantEntry.textContent}${boundText(msg.text, MAX_PROVIDER_TRANSCRIPT_DELTA_CODE_POINTS)}`,
        MAX_TRANSCRIPT_ENTRY_CODE_POINTS,
      );
      transcriptBox.scrollTop = transcriptBox.scrollHeight;
      return;
    case "transcript_done":
      if (assistantEntry) {
        assistantEntry.textContent = boundText(msg.text, MAX_TRANSCRIPT_ENTRY_CODE_POINTS);
      }
      assistantEntry = null;
      return;
    case "user_transcript":
      addTranscript("user", msg.text);
      return;
    case "text_turn_accepted":
      handleTextTurnAccepted(msg);
      return;
    case "text_turn_rejected":
      handleTextTurnRejected(msg);
      return;
    case "recording_rejected":
      handleRecordingRejected(msg);
      return;
    case "dictation_operation":
      acceptDictationOperation(msg.operation_id, msg.recording_id);
      return;
    case "dictation_result": {
      if (typeof msg.text !== "string" || msg.text.length === 0) return;
      if (!dictation.acceptsOperation(msg.operation_id)) return;
      const text = boundText(msg.text, MAX_DICTATION_TEXT_CODE_POINTS);
      partialDictationText = "";
      const capturedTarget = dictation.target;
      if (!capturedTarget) return;
      const result = applyCapturedDraft(capturedTarget, currentDraftTarget(), text);
      if (result.status === "discarded") {
        dictation.reset();
        resetDictationRecordingState();
        clearTypedDraft();
        dictationPreviewText.textContent = "Dictation discarded because its destination changed.";
        dictationReviewActions.classList.add("hidden");
        logActivity("dictation discarded after destination changed", "error");
      } else if (result.status === "review") {
        dictation.advance("awaiting_review");
        reviewDictationText = text;
        dictationPreviewText.textContent = "The draft or caret changed. Review before inserting.";
        dictationReviewActions.classList.remove("hidden");
        logActivity("dictation requires explicit insertion");
      } else {
        dictation.reset();
        resetDictationRecordingState();
        updateDraft(result);
        const prefix =
          msg.status === "degraded"
            ? "Cleanup unavailable. Raw transcript inserted: "
            : "Inserted: ";
        dictationPreviewText.textContent = `${prefix}${text}`;
        dictationReviewActions.classList.add("hidden");
        logActivity(
          msg.status === "degraded"
            ? "raw transcript inserted after cleanup failed"
            : "cleaned dictation inserted into draft",
        );
        playCue("success");
      }
      dictationPreview.classList.remove("hidden");
      restoreMicrophoneInterfaceState();
      syncInputState();
      return;
    }
    case "dictation_preview":
      if (typeof msg.text !== "string") return;
      if (!dictation.acceptsOperation(msg.operation_id)) return;
      partialDictationText = boundText(
        `${partialDictationText}${msg.text}`,
        MAX_DICTATION_TEXT_CODE_POINTS,
      );
      dictationPreviewText.textContent = partialDictationText || "Transcribing...";
      dictationPreview.classList.remove("hidden");
      return;
    case "dictation_empty":
      if (!dictation.acceptsOperation(msg.operation_id)) return;
      dictation.reset();
      resetDictationRecordingState();
      partialDictationText = "";
      dictationPreviewText.textContent = "No speech detected. The draft was not changed.";
      dictationReviewActions.classList.add("hidden");
      dictationPreview.classList.remove("hidden");
      restoreMicrophoneInterfaceState();
      syncInputState();
      return;
    case "dictation_failed":
      handleDictationFailure(msg);
      return;
    case "dictation_cancelled":
      completeBrokerDictationCancellation(msg.operation_id);
      return;
    case "tool":
      logActivity(`tool ${msg.phase}: ${msg.name}`);
      return;
    case "proposal":
      handleProposalFrame(msg);
      return;
    case "flush_audio":
      flushPlayback();
      return;
    case "error":
      handleGenericError(msg);
      return;
    default:
      return;
  }
}

function cancelReconnect() {
  if (reconnectTimer === null) return;
  clearTimeout(reconnectTimer);
  reconnectTimer = null;
}

function scheduleReconnect() {
  if (!pageIsActive() || reloadRequired) return;
  cancelReconnect();
  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    connect();
  }, 3000);
}

function clearEphemeralPageState() {
  browserConnection.disconnect();
  textTurns.disconnect();
  liveRecordings.reset();
  hostAvailable = false;
  hostCount = 0;
  draftAwaitingReconnect = false;
  clearBrowserContext({ disconnected: true });
  hostSelect.replaceChildren();
  hostCwd.textContent = "?";
  hostProvider.textContent = "?";
  microphoneSelect.replaceChildren();
  microphoneStatus.textContent = "";
  setupPanel.classList.remove("hidden");
  dictationSettings.classList.add("hidden");
  enableMicrophoneButton.textContent = "Enable microphone";
  enableMicrophoneButton.disabled = audioSetupPromise !== null;
  transcriptBox.replaceChildren();
  activityBox.replaceChildren();
  assistantEntry = null;
  for (const id of [
    "stt-provider",
    "stt-provider-status",
    "cleanup-provider",
    "cleanup-provider-status",
  ]) {
    $(id).textContent = "";
  }
}

function teardownPage({ persisted = false } = {}) {
  const final = !persisted;
  if (pageLifecycle === "final") return finalPageTeardownPromise ?? pageCleanupPromise;
  if (pageLifecycle === "suspended" && !final) return pageCleanupPromise;
  pageLifecycle = final ? "final" : "suspended";
  pageGeneration += 1;
  const pendingAudioSetup = audioSetupPromise;
  cancelReconnect();

  if (activeRecording?.mode === "live_response" || liveRecordings.pending) {
    abortLiveRecordingForRoutingChange();
  } else if (dictationOwnsContext(dictation.phase)) {
    cancelDictation({ restore: false, message: "" });
  }

  const closingSocket = socket;
  handledSocketClose = closingSocket;
  socket = null;
  if (closingSocket) removeOwnedEventListeners(closingSocket);
  clearEphemeralPageState();
  micReady = false;
  microphoneLifecycle = createMicrophoneLifecycleState();
  microphonePresentation = null;
  syncInputState();
  try {
    closingSocket?.close();
  } catch {
    // Local resource disposal does not depend on a successful close handshake.
  }
  if (final) {
    removeOwnedEventListeners();
    blockAvatar?.destroy();
    blockAvatar = null;
  }

  pageCleanupPromise = (async () => {
    await disposeAudioResources();
    if (pendingAudioSetup) {
      try {
        await pendingAudioSetup;
      } catch {
        // Setup failure is irrelevant after page ownership has ended.
      }
      await disposeAudioResources();
    }
  })();
  if (final) finalPageTeardownPromise = pageCleanupPromise;
  return pageCleanupPromise;
}

function resumePage() {
  if (pageLifecycle !== "suspended") return false;
  pageLifecycle = "active";
  handledSocketClose = null;
  setPill(connPill, "connecting", "");
  setInterfaceState("connecting");
  microphoneStatus.textContent = "Microphone access requires re-enabling.";
  enableMicrophoneButton.disabled = audioSetupPromise !== null;
  syncInputState();
  connect();
  return true;
}

function handleSocketClose(closedSocket = socket, { preserveDraft = false } = {}) {
  if (!closedSocket) return;
  if (!pageIsActive() || closedSocket !== socket || handledSocketClose === closedSocket) {
    removeOwnedEventListeners(closedSocket);
    return;
  }
  handledSocketClose = closedSocket;
  removeOwnedEventListeners(closedSocket);
  browserConnection.disconnect();
  voiceModeToggle.checked = voiceMode === "live_response";
  textTurns.disconnect();
  liveRecordings.reset();
  const detail = reloadRequired ? "reload required" : "retrying in 3 seconds";
  setPill(connPill, reloadRequired ? "reload required" : "disconnected", "error");
  logActivity(
    reloadRequired ? "protocol update requires a page reload" : "disconnected; retrying in 3 s",
    "error",
  );
  flushPlayback();
  talking = false;
  pttButton.classList.remove("active");
  updatePushToTalkCopy();
  hostAvailable = false;
  hostCount = 0;
  hostSelect.disabled = true;
  clearBrowserContext({
    disconnected: true,
    preserveDraft: preserveDraft || draftAwaitingReconnect,
  });
  setInterfaceState("disconnected", detail);
  syncInputState();
  scheduleReconnect();
}

function connect() {
  if (!pageIsActive()) return;
  if (socket && [WebSocket.CONNECTING, WebSocket.OPEN].includes(socket.readyState)) return;
  if (loopback) {
    setPill(connPill, "loopback", "warn");
    setPill(modePill, "mode: local", "warn");
    restoreMicrophoneInterfaceState();
    return;
  }
  const proto = location.protocol === "https:" ? "wss" : "ws";
  const connection = new WebSocket(`${proto}://${location.host}/ws`);
  socket = connection;
  handledSocketClose = null;
  browserConnection.disconnect();
  connection.binaryType = "arraybuffer";

  listen(connection, "open", () => {
    if (socket !== connection) return;
    setPill(connPill, "connected", "ok");
    if (!sendSocketControl(browserHelloControl())) return;
    logActivity("connected to broker; negotiating protocol v2");
    restoreMicrophoneInterfaceState();
  });
  listen(connection, "close", () => handleSocketClose(connection));
  listen(connection, "error", () => {
    if (socket !== connection) return;
    setPill(connPill, "socket error", "error");
    setInterfaceState("error", "socket error");
  });
  listen(connection, "message", (event) => {
    if (socket !== connection || handledSocketClose === connection) return;
    if (typeof event.data === "string") {
      try {
        handleServerJson(JSON.parse(event.data));
      } catch {
        logActivity("malformed broker frame discarded", "error");
      }
      return;
    }
    if (playbackController?.enqueue(event.data)) {
      setInterfaceState("speaking");
    }
  });
}

listen(enableMicrophoneButton, "click", requestMicrophoneSetup);

listen(pttButton, "pointerdown", (event) => {
  event.preventDefault();
  pttButton.setPointerCapture(event.pointerId);
  if (recordingMode === "hold" || voiceMode === "live_response") startTalking();
});
listen(pttButton, "pointerup", () => {
  if (recordingMode === "hold" || activeRecording?.mode === "live_response") stopTalking();
});
listen(pttButton, "pointercancel", () => {
  if (recordingMode === "hold" || activeRecording?.mode === "live_response") stopTalking();
});
listen(pttButton, "click", () => {
  const mode = activeRecording?.mode ?? voiceMode;
  if (mode !== "dictation" || recordingMode !== "toggle") return;
  if (talking) stopTalking();
  else startTalking();
});

listen(voiceModeToggle, "change", () => {
  if (
    !micReady ||
    microphoneControlsLocked(
      microphoneLifecycle,
      activeRecording,
      browserConnection.voiceModeChangePending,
    ) ||
    !conversationInteractionAllowed() ||
    !socket ||
    socket.readyState !== WebSocket.OPEN
  ) {
    voiceModeToggle.checked = voiceMode === "live_response";
    return;
  }
  const requested = voiceModeToggle.checked ? "live_response" : "dictation";
  const control = browserConnection.requestVoiceMode(requested);
  if (!control) {
    voiceModeToggle.checked = voiceMode === "live_response";
    return;
  }
  syncInputState();
  sendSocketControl(control);
});

function updatePageCapture(event) {
  const decision = transitionPageCapture(pageCapture, event);
  pageCapture = decision.state;
  return decision;
}

function interruptPageCapture(reason) {
  const decision = updatePageCapture({ type: "interrupt", reason, activeRecording });
  if (decision.effect === "cancel-dictation") {
    cancelDictation({ message: "Dictation cancelled because the page became inactive." });
  } else if (
    decision.effect === "abort-live" &&
    abortLiveCapture(decision.recording, decision.brokerControl)
  ) {
    restoreMicrophoneInterfaceState();
  }
}

listen(document, "keydown", (event) => {
  if (isInteractiveTarget(event.target)) return;
  const mode = activeRecording?.mode ?? voiceMode;
  if (event.code === shortcuts.cancel && mode === "dictation") {
    event.preventDefault();
    cancelDictation();
    return;
  }
  if (event.code === shortcuts.hold && !event.repeat) {
    event.preventDefault();
    const recording = startTalking();
    if (recording) {
      updatePageCapture({ type: "hold-started", code: event.code, recording });
    }
    return;
  }
  if (event.code === shortcuts.toggle && !event.repeat && mode === "dictation") {
    event.preventDefault();
    if (talking) stopTalking();
    else startTalking();
  }
});
listen(document, "keyup", (event) => {
  const decision = updatePageCapture({
    type: "page-keyup",
    code: event.code,
    interactiveTarget: isInteractiveTarget(event.target),
    activeRecording,
  });
  if (decision.effect !== "stop") return;
  event.preventDefault();
  stopTalking();
});
listen(window, "blur", () => interruptPageCapture("blur"));
listen(document, "visibilitychange", () => {
  if (document.visibilityState === "hidden") interruptPageCapture("visibility-hidden");
});

listen(textForm, "submit", (event) => {
  event.preventDefault();
  const text = textInput.value.trim();
  if (!conversationInteractionAllowed() || !text) return;
  const control = textTurns.begin(text, captureBoundDraftState());
  if (!control) {
    logActivity("typed turn ID limit reached; reload required", "error");
    setInterfaceState("error", "typed turn limit reached; reload required");
    return;
  }
  syncInputState();
  sendSocketControl(control, { preserveDraftOnFailure: true });
});

listen(hostSelect, "change", () => {
  if (conversationInteractionAllowed() && socket?.readyState === WebSocket.OPEN) {
    const nextHostId = hostSelect.value;
    clearBrowserContext({
      nextHostId,
      dictationMessage: "Dictation discarded because the selected host changed.",
      sendCancel: false,
    });
    sendSocketControl({ type: "select_host", host_id: nextHostId });
  }
});

function updateProposalActivation(event) {
  const transition = proposalActivationTransition(proposalActivationLatch, {
    ...event,
    proposalState,
  });
  proposalActivationLatch = transition.latch;
  return transition.control;
}

function dispatchProposalControl(control) {
  if (!control || socket?.readyState !== WebSocket.OPEN) return;
  if (control.type === "confirm_proposal") {
    if (writeInFlight) return;
    writeInFlight = true;
    syncInputState();
  }
  sendSocketControl(control);
}

for (const [button, action] of [
  [confirmProposalButton, "confirm_proposal"],
  [cancelProposalButton, "cancel_proposal"],
]) {
  listen(button, "pointerdown", () => {
    updateProposalActivation({ type: "capture", action });
  });
  listen(button, "keydown", (event) => {
    if (isNativeProposalActivationKeydown(event)) {
      updateProposalActivation({ type: "capture", action });
    }
  });
  listen(button, "pointercancel", () => {
    updateProposalActivation({ type: "clear", action });
  });
  listen(button, "blur", () => {
    updateProposalActivation({ type: "clear", action });
  });
  listen(button, "click", () => {
    dispatchProposalControl(updateProposalActivation({ type: "click", action }));
  });
}

listen($("insert-dictation"), "click", () => {
  if (!reviewDictationText) return;
  const current = currentDraftTarget();
  const capturedTarget = dictation.target;
  if (!capturedTarget || !hasSameRoutingContext(capturedTarget, current)) {
    terminateDictationForRoutingChange(current);
    clearTypedDraft();
    reviewDictationText = null;
    dictationReviewActions.classList.add("hidden");
    dictationPreviewText.textContent = "Dictation discarded because its destination changed.";
    return;
  }
  updateDraft(
    insertText(current.value, current.selectionStart, current.selectionEnd, reviewDictationText),
  );
  dictation.reset();
  resetDictationRecordingState();
  restoreAfterLocalDictationTermination();
  reviewDictationText = null;
  dictationReviewActions.classList.add("hidden");
  dictationPreviewText.textContent = "Dictation inserted at the current caret.";
  logActivity("dictation inserted after explicit review");
  syncInputState();
});

listen($("discard-dictation"), "click", () => {
  dictation.reset();
  resetDictationRecordingState();
  restoreAfterLocalDictationTermination();
  reviewDictationText = null;
  dictationReviewActions.classList.add("hidden");
  dictationPreviewText.textContent = "Dictation discarded.";
  syncInputState();
});

listen(cancelDictationButton, "click", () => cancelDictation());

listen(recordingModeSelect, "change", () => {
  if (
    microphoneControlsLocked(
      microphoneLifecycle,
      activeRecording,
      browserConnection.voiceModeChangePending,
    )
  ) {
    recordingModeSelect.value = recordingMode;
    return;
  }
  recordingMode = recordingModeSelect.value === "toggle" ? "toggle" : "hold";
  try {
    localStorage.setItem(recordingModeStorageKey, recordingMode);
  } catch {
    // The preference remains page-scoped when browser storage is unavailable.
  }
  updatePushToTalkCopy();
});

listen(silenceStop, "change", () => {
  storePreference(silenceStopStorageKey, silenceStop.checked ? "on" : "off");
});

listen(silencePeriod, "change", () => {
  storePreference(silencePeriodStorageKey, silencePeriod.value);
});

listen(soundCues, "change", () => {
  storePreference(soundStorageKey, soundCues.checked ? "on" : "off");
});

listen(microphoneSelect, "change", async () => {
  if (
    microphoneControlsLocked(
      microphoneLifecycle,
      activeRecording,
      browserConnection.voiceModeChangePending,
    )
  ) {
    return;
  }
  const requestedDeviceId = microphoneSelect.value || null;
  const requestPageGeneration = pageGeneration;
  let request = null;
  try {
    request = beginMicrophoneRequest(requestPageGeneration);
    if (!request) return;
    markMicrophoneUnavailable("Switching microphone...");
    const connection = await connectMicrophone(
      requestedDeviceId,
      currentProcessingPreferences(),
      request,
    );
    if (connection.status !== "active") {
      if (connection.status === "ended") await audioCleanupPromise;
      return;
    }
    await completeMicrophoneConnection(connection, {
      message: requestedDeviceId ? "Selected microphone active." : "System default active.",
    });
  } catch (error) {
    if (!microphoneRequestIsCurrent(request)) return;
    await failMicrophoneRequest(
      request,
      error,
      `Microphone setup failed: ${error.message}. Select Retry microphone to continue.`,
    );
  }
});

async function saveProcessingPreferences() {
  if (
    !micReady ||
    microphoneControlsLocked(
      microphoneLifecycle,
      activeRecording,
      browserConnection.voiceModeChangePending,
    )
  ) {
    return;
  }
  const processingPreference = currentProcessingPreferences();
  const requestPageGeneration = pageGeneration;
  let request = null;
  try {
    request = beginMicrophoneRequest(requestPageGeneration);
    if (!request) return;
    markMicrophoneUnavailable("Applying audio processing settings...");
    const selectedDeviceId = microphoneSelect.value || null;
    const connection = await connectMicrophone(selectedDeviceId, processingPreference, request);
    if (connection.status !== "active") {
      if (connection.status === "ended") {
        restoreStoredProcessingPreferences();
        await audioCleanupPromise;
      }
      return;
    }
    await completeMicrophoneConnection(connection, {
      message: selectedDeviceId ? "Selected microphone active." : "System default active.",
      processingPreference,
    });
  } catch (error) {
    if (!microphoneRequestIsCurrent(request)) return;
    restoreStoredProcessingPreferences();
    await failMicrophoneRequest(
      request,
      error,
      `Audio processing change failed: ${error.message}. Select Retry microphone to continue.`,
    );
  }
}

[echoCancellation, noiseSuppression, autoGain, preRoll].forEach((control) => {
  listen(control, "change", () => void saveProcessingPreferences());
});

function updateShortcuts() {
  const result = validateShortcuts({
    hold: holdShortcut.value,
    toggle: toggleShortcut.value,
    cancel: cancelShortcut.value,
  });
  if (!result.valid) {
    shortcutStatus.textContent = result.message;
    holdShortcut.value = shortcuts.hold;
    toggleShortcut.value = shortcuts.toggle;
    cancelShortcut.value = shortcuts.cancel;
    return;
  }
  shortcuts = result.value;
  shortcutStatus.textContent = "Shortcuts saved for this browser.";
  storePreference(shortcutStorageKey, shortcuts);
}

[holdShortcut, toggleShortcut, cancelShortcut].forEach((control) => {
  listen(control, "change", updateShortcuts);
});

async function handleMicrophoneDeviceChange() {
  if (microphoneLifecycle.phase !== "ready") return;
  const streamToken = microphoneLifecycle.streamToken;
  try {
    const permissionState = await microphonePermissionState();
    if (streamToken !== microphoneLifecycle.streamToken) return;
    if (permissionState === "prompt" || permissionState === "denied") {
      dispatchMicrophoneLifecycleEvent({
        type: "permission-changed",
        streamToken,
        permissionState,
        recording: activeRecording,
      });
      return;
    }
    const devices = await enumerateMicrophones();
    if (streamToken !== microphoneLifecycle.streamToken) return;
    refreshMicrophones(devices);
    const effectivePermission = effectiveMicrophonePermission(permissionState, devices);
    if (microphoneLifecycle.selectedDeviceId === null) {
      if (effectivePermission !== "granted") return;
      const defaultTransition = defaultMicrophoneTransition(defaultDeviceFingerprint, devices);
      defaultDeviceFingerprint = defaultTransition.fingerprint;
      if (!defaultTransition.reconnect) return;
    }
    dispatchMicrophoneLifecycleEvent({
      type: "devices-changed",
      streamToken,
      deviceIds: devices.map((device) => device.deviceId),
      permissionState: effectivePermission,
      recording: activeRecording,
    });
  } catch (error) {
    if (streamToken === microphoneLifecycle.streamToken) {
      microphoneStatus.textContent = `Microphone list unavailable: ${error.message}`;
    }
  }
}

if (navigator.mediaDevices) {
  listen(navigator.mediaDevices, "devicechange", () => {
    void handleMicrophoneDeviceChange();
  });
}

listen(window, "pagehide", (event) => {
  void teardownPage({ persisted: event.persisted === true });
});

listen(window, "pageshow", (event) => {
  if (event.persisted === true) resumePage();
});

connect();
