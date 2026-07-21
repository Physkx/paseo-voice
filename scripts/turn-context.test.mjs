import test from "node:test";
import assert from "node:assert/strict";

import {
  MAX_TURN_ID,
  browserHelloControl,
  createBrowserConnectionController,
  createBoundDraftState,
  createTextTurnController,
  displayedSummaryId,
  isProtocolMismatchFrame,
  isProtocolReadyFrame,
  reduceBoundDraft,
  textTurnControl,
} from "../public/turn-context.js";

test("only dedicated exact protocol v2 frames control negotiation", () => {
  assert.deepEqual(browserHelloControl(), { type: "hello", protocol_version: 2 });
  assert.equal(isProtocolReadyFrame({ type: "protocol_ready", version: 2 }), true);
  assert.equal(isProtocolReadyFrame({ type: "protocol_ready", version: 1 }), false);
  assert.equal(
    isProtocolReadyFrame({ type: "protocol_ready", version: 2, unexpected: true }),
    false,
  );
  assert.equal(isProtocolReadyFrame({ type: "mode", mode: "real" }), false);
  assert.equal(isProtocolMismatchFrame({ type: "protocol_mismatch", required_version: 2 }), true);
  assert.equal(isProtocolMismatchFrame({ type: "protocol_mismatch", required_version: 1 }), false);
  assert.equal(
    isProtocolMismatchFrame({
      type: "protocol_mismatch",
      required_version: 2,
      message: "extra",
    }),
    false,
  );
  assert.equal(
    isProtocolMismatchFrame({ type: "error", message: "Provider protocol version failed." }),
    false,
  );
  assert.equal(
    isProtocolMismatchFrame({ type: "error", message: "Provider says reload required." }),
    false,
  );
});

test("protocol readiness waits for the initial and matching voice mode acknowledgement", () => {
  const connection = createBrowserConnectionController("dictation");
  const transport = { hostAvailable: true, socketOpen: true };

  assert.equal(connection.conversationReady(transport), false);
  assert.equal(connection.acceptProtocol({ type: "protocol_ready", version: 2 }), true);
  assert.equal(connection.conversationReady(transport), false);

  assert.deepEqual(connection.acceptVoiceMode("live_response"), {
    mode: "live_response",
    control: { type: "set_voice_mode", mode: "dictation" },
    settled: false,
  });
  assert.equal(connection.initialVoiceModeReceived, true);
  assert.equal(connection.voiceModeChangePending, true);
  assert.equal(connection.conversationReady(transport), false);

  assert.deepEqual(connection.acceptVoiceMode("live_response"), {
    mode: "live_response",
    control: null,
    settled: false,
  });
  assert.equal(connection.conversationReady(transport), false);

  assert.deepEqual(connection.acceptVoiceMode("dictation"), {
    mode: "dictation",
    control: null,
    settled: true,
  });
  assert.equal(connection.voiceModeChangePending, false);
  assert.equal(connection.conversationReady(transport), true);
});

test("a requested voice mode stays gated until its exact acknowledgement", () => {
  const connection = createBrowserConnectionController("live_response");
  const readyFrame = { type: "protocol_ready", version: 2 };
  connection.acceptProtocol(readyFrame);
  connection.acceptVoiceMode("live_response");

  assert.deepEqual(connection.requestVoiceMode("dictation"), {
    type: "set_voice_mode",
    mode: "dictation",
  });
  assert.equal(connection.preferredVoiceMode, "dictation");
  assert.equal(connection.voiceModeChangePending, true);
  assert.equal(connection.acceptVoiceMode("live_response").settled, false);
  assert.equal(connection.voiceModeChangePending, true);

  assert.equal(connection.rejectVoiceModeChange(), "live_response");
  assert.equal(connection.preferredVoiceMode, "live_response");
  assert.equal(connection.voiceModeChangePending, false);

  connection.requestVoiceMode("dictation");
  connection.disconnect();
  assert.equal(connection.protocolReady, false);
  assert.equal(connection.initialVoiceModeReceived, false);
  assert.equal(connection.voiceModeChangePending, false);
  assert.equal(connection.preferredVoiceMode, "dictation");
  connection.acceptProtocol(readyFrame);
  assert.deepEqual(connection.acceptVoiceMode("live_response").control, {
    type: "set_voice_mode",
    mode: "dictation",
  });
});

test("matching typed acceptance clears only the unchanged captured draft", () => {
  const turns = createTextTurnController();
  const draft = createBoundDraftState({
    summaryId: "summary-a",
    value: "reply to A",
    selectionStart: 10,
    selectionEnd: 10,
  });

  assert.deepEqual(turns.begin("reply to A", draft), {
    type: "text_turn",
    text: "reply to A",
    summary_id: "summary-a",
    turn_id: 1,
  });
  assert.equal(turns.hasPending, true);
  assert.equal(turns.begin("second", draft), null);
  assert.deepEqual(turns.accept(1, draft), {
    text: "reply to A",
    clearedDraft: {
      summaryId: "summary-a",
      value: "",
      selectionStart: 0,
      selectionEnd: 0,
    },
  });
  assert.equal(turns.hasPending, false);
});

test("typed acceptance preserves edits made after submit and ignores a late A acknowledgement", () => {
  const turns = createTextTurnController();
  const draftA = createBoundDraftState({ summaryId: "summary-a", value: "reply A" });
  turns.begin("reply A", draftA);
  const editedA = createBoundDraftState({
    summaryId: "summary-a",
    value: "reply A plus edits",
    selectionStart: 18,
    selectionEnd: 18,
  });

  assert.deepEqual(turns.accept(1, editedA), { text: "reply A", clearedDraft: null });
  const draftB = createBoundDraftState({ summaryId: "summary-b", value: "reply B" });
  assert.equal(turns.begin("reply B", draftB).turn_id, 2);
  assert.equal(turns.accept(1, draftB), null);
  assert.equal(turns.pendingTurnId, 2);
});

test("matching typed rejection retires only that turn and preserves the edited draft", () => {
  const turns = createTextTurnController();
  const draftA = createBoundDraftState({ summaryId: "summary-a", value: "reply A" });
  turns.begin("reply A", draftA);
  const editedA = createBoundDraftState({
    summaryId: "summary-a",
    value: "keep my edits",
    selectionStart: 4,
    selectionEnd: 9,
  });

  assert.equal(turns.reject(9), null);
  assert.equal(turns.hasPending, true);
  assert.deepEqual(turns.reject(1), { text: "reply A" });
  assert.deepEqual(editedA, {
    summaryId: "summary-a",
    value: "keep my edits",
    selectionStart: 4,
    selectionEnd: 9,
  });
  assert.equal(turns.begin("reply B", editedA).turn_id, 2);
  assert.equal(turns.reject(1), null);
  assert.equal(turns.pendingTurnId, 2);
});

test("typed turn IDs are bounded and never wrap", () => {
  const turns = createTextTurnController(MAX_TURN_ID);
  const draft = createBoundDraftState({ summaryId: null, value: "status" });
  assert.equal(turns.begin("status", draft), null);
  assert.equal(turns.hasPending, false);
});

test("typed disconnect retires pending ownership without mutating or reusing its draft ID", () => {
  const turns = createTextTurnController();
  const draft = createBoundDraftState({
    summaryId: "summary-a",
    value: "retain on send failure",
    selectionStart: 3,
    selectionEnd: 8,
  });
  turns.begin("retain on send failure", draft);
  turns.disconnect();

  assert.deepEqual(draft, {
    summaryId: "summary-a",
    value: "retain on send failure",
    selectionStart: 3,
    selectionEnd: 8,
  });
  assert.equal(turns.hasPending, false);
  assert.equal(turns.begin("retry", draft).turn_id, 2);
});

test("a draft bound to summary A clears when summary B is displayed", () => {
  const boundToA = createBoundDraftState({
    summaryId: "summary-a",
    value: "reply to A",
    selectionStart: 3,
    selectionEnd: 8,
  });

  assert.deepEqual(
    reduceBoundDraft(boundToA, { type: "display-summary", summaryId: "summary-b" }),
    {
      summaryId: "summary-b",
      value: "",
      selectionStart: 0,
      selectionEnd: 0,
    },
  );
});

test("a draft bound to summary A clears when no summary is displayed", () => {
  const boundToA = createBoundDraftState({
    summaryId: "summary-a",
    value: "reply to A",
    selectionStart: 10,
    selectionEnd: 10,
  });

  assert.deepEqual(reduceBoundDraft(boundToA, { type: "display-summary", summaryId: null }), {
    summaryId: null,
    value: "",
    selectionStart: 0,
    selectionEnd: 0,
  });
});

test("same-summary presentation updates preserve the draft and selection", () => {
  const boundToA = createBoundDraftState({
    summaryId: "summary-a",
    value: "keep this response",
    selectionStart: 5,
    selectionEnd: 9,
  });

  assert.equal(
    reduceBoundDraft(boundToA, { type: "display-summary", summaryId: "summary-a" }),
    boundToA,
  );
});

test("a text turn carries its displayed summary ID", () => {
  assert.deepEqual(textTurnControl("reply to A", "summary-a", 1), {
    type: "text_turn",
    text: "reply to A",
    summary_id: "summary-a",
    turn_id: 1,
  });
});

test("a command turn remains possible with explicit null context", () => {
  assert.deepEqual(textTurnControl("show status", null, 2), {
    type: "text_turn",
    text: "show status",
    summary_id: null,
    turn_id: 2,
  });
});

test("an explicit connection or host reset clears an unbound command draft", () => {
  const commandDraft = createBoundDraftState({
    summaryId: null,
    value: "show status",
    selectionStart: 4,
    selectionEnd: 10,
  });

  assert.deepEqual(reduceBoundDraft(commandDraft, { type: "clear-draft", summaryId: null }), {
    summaryId: null,
    value: "",
    selectionStart: 0,
    selectionEnd: 0,
  });
});

test("only a non-empty opaque summary ID is treated as displayed context", () => {
  assert.equal(displayedSummaryId({ summary_id: "summary-a" }), "summary-a");
  assert.equal(displayedSummaryId({ summary_id: "" }), null);
  assert.equal(displayedSummaryId({ summary_id: 7 }), null);
  assert.equal(displayedSummaryId(null), null);
});
