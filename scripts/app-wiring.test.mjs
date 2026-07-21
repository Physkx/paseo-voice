import test from "node:test";
import assert from "node:assert/strict";

import { createAppHarness } from "./helpers/fake-browser.mjs";

const hello = { type: "hello", protocol_version: 2 };

function hostState(selectedHostId = "host-a") {
  return {
    type: "host_state",
    selected_host_id: selectedHostId,
    hosts: [
      {
        id: "host-a",
        label: "Host A",
        available: true,
        default_cwd: "~/work-a",
        default_provider: "provider-a/model-a",
      },
      {
        id: "host-b",
        label: "Host B",
        available: true,
        default_cwd: "~/work-b",
        default_provider: "provider-b/model-b",
      },
    ],
  };
}

function dashboardState(summaryId, summary = `Summary ${summaryId}`, selectedHostId = "host-a") {
  return {
    type: "dashboard_state",
    selected_host_id: selectedHostId,
    queue_count: 0,
    agents: [],
    bound_context: summaryId
      ? {
          summary_id: summaryId,
          thread_id: `thread-${summaryId}`,
          thread_name: `Agent ${summaryId}`,
          latest_summary: summary,
        }
      : null,
  };
}

function openSocket(socket) {
  assert.deepEqual(socket.sent, []);
  socket.open();
  assert.deepEqual(socket.sentJson(), [hello]);
  return socket;
}

function readyConnection(browser, voiceMode = "live_response") {
  const socket = openSocket(browser.socket);
  socket.receive({ type: "protocol_ready", version: 2 });
  socket.receive({ type: "voice_mode", mode: voiceMode });
  socket.receive(hostState());
  return socket;
}

function openOnlyReplacement(browser, previousSocket) {
  const previousCount = browser.sockets.length;
  browser.clock.tick(2_999);
  assert.equal(browser.sockets.length, previousCount);
  browser.clock.tick(1);
  assert.equal(browser.sockets.length, previousCount + 1);
  const replacement = browser.socket;
  assert.notEqual(replacement, previousSocket);
  openSocket(replacement);
  browser.clock.tick(3_000);
  assert.equal(browser.sockets.length, previousCount + 1);
  return replacement;
}

function hasUnpairedSurrogate(text) {
  for (let index = 0; index < text.length; index += 1) {
    const codeUnit = text.charCodeAt(index);
    if (codeUnit >= 0xd800 && codeUnit <= 0xdbff) {
      const next = text.charCodeAt(index + 1);
      if (next < 0xdc00 || next > 0xdfff) return true;
      index += 1;
    } else if (codeUnit >= 0xdc00 && codeUnit <= 0xdfff) {
      return true;
    }
  }
  return false;
}

test("the browser entry loads and sends the exact protocol-v2 hello", async (t) => {
  const browser = await createAppHarness();
  t.after(() => browser.restore());

  assert.equal(browser.sockets.length, 1);
  assert.equal(browser.entryUrl.href, new URL("../public/app.js", import.meta.url).href);
  assert.equal(browser.element("text-input").tagName, "TEXTAREA");
  assert.equal(browser.socket.url, "ws://voice.test/ws");
  assert.deepEqual(browser.socket.sent, []);

  browser.socket.open();

  assert.deepEqual(browser.socket.sentJson(), [hello]);
});

test("protocol and preferred voice-mode ordering keep conversation controls gated", async (t) => {
  const browser = await createAppHarness({
    storage: { "paseoVoice.voiceMode": "dictation" },
  });
  t.after(() => browser.restore());
  const socket = browser.socket;
  const textInput = browser.element("text-input");
  const hostSelect = browser.element("host-select");

  openSocket(socket);
  socket.receive(hostState());
  assert.equal(textInput.disabled, true);
  assert.equal(browser.submitButton.disabled, true);
  assert.equal(hostSelect.disabled, true);

  socket.receive({ type: "protocol_ready", version: 2 });
  assert.equal(textInput.disabled, true);
  assert.equal(browser.submitButton.disabled, true);

  socket.receive({ type: "voice_mode", mode: "live_response" });
  assert.deepEqual(socket.sentJson().at(-1), {
    type: "set_voice_mode",
    mode: "dictation",
  });
  assert.equal(textInput.disabled, true);
  assert.equal(browser.submitButton.disabled, true);
  assert.equal(hostSelect.disabled, true);

  socket.receive({ type: "voice_mode", mode: "live_response" });
  assert.equal(textInput.disabled, true);
  assert.equal(browser.submitButton.disabled, true);

  socket.receive({ type: "voice_mode", mode: "dictation" });
  assert.equal(textInput.disabled, false);
  assert.equal(browser.submitButton.disabled, false);
  assert.equal(hostSelect.disabled, false);
});

test("generic errors stay connected while protocol mismatch requires reload", async (t) => {
  const browser = await createAppHarness();
  t.after(() => browser.restore());
  const socket = readyConnection(browser);

  socket.receive({ type: "error", message: "Provider protocol version failed." });
  assert.equal(socket.readyState, WebSocket.OPEN);
  assert.equal(browser.sockets.length, 1);
  assert.equal(browser.element("conn-pill").textContent, "connected");
  assert.equal(browser.element("avatar").dataset.state, "error");

  socket.receive({ type: "protocol_mismatch", required_version: 2 });
  assert.equal(socket.readyState, WebSocket.CLOSING);
  assert.equal(browser.element("conn-pill").textContent, "reload required");
  assert.equal(browser.element("text-input").disabled, true);
  browser.clock.tick(0);
  assert.equal(socket.readyState, WebSocket.CLOSED);
  browser.clock.tick(6_000);
  assert.equal(browser.sockets.length, 1);
  assert.equal(browser.clock.pendingTimerCount, 0);
});

test("dashboard context changes clear the real bound draft while same-summary updates preserve it", async (t) => {
  const browser = await createAppHarness();
  t.after(() => browser.restore());
  const socket = readyConnection(browser);
  const textInput = browser.element("text-input");

  socket.receive(dashboardState("summary-a"));
  textInput.value = "reply selected words to A";
  textInput.setSelectionRange(6, 20);

  socket.receive(dashboardState("summary-a", "Updated presentation for A"));
  assert.equal(textInput.value, "reply selected words to A");
  assert.equal(textInput.selectionStart, 6);
  assert.equal(textInput.selectionEnd, 20);

  socket.receive(dashboardState("summary-b"));
  assert.equal(textInput.value, "");
  assert.equal(textInput.selectionStart, 0);
  assert.equal(textInput.selectionEnd, 0);
  assert.equal(browser.element("bound-thread").textContent, "Bound to Agent summary-b");
});

test("typed turns retain their exact summary and wait for their correlated acknowledgement", async (t) => {
  const browser = await createAppHarness();
  t.after(() => browser.restore());
  const socket = readyConnection(browser);
  const textInput = browser.element("text-input");
  const transcript = browser.element("transcript");

  socket.receive(dashboardState("summary-a"));
  textInput.value = "  reply to A  ";
  textInput.setSelectionRange(2, 12);
  browser.dispatch("text-form", "submit");

  assert.deepEqual(socket.sentJson().at(-1), {
    type: "text_turn",
    text: "reply to A",
    summary_id: "summary-a",
    turn_id: 1,
  });
  assert.equal(textInput.value, "  reply to A  ");
  assert.equal(textInput.selectionStart, 2);
  assert.equal(textInput.selectionEnd, 12);
  assert.equal(browser.submitButton.disabled, true);

  socket.receive({ type: "text_turn_accepted", turn_id: 99 });
  assert.equal(textInput.value, "  reply to A  ");
  assert.equal(transcript.children.length, 0);
  assert.equal(browser.submitButton.disabled, true);

  socket.receive({ type: "text_turn_rejected", turn_id: 1, message: "try again" });
  assert.equal(textInput.value, "  reply to A  ");
  assert.equal(textInput.selectionStart, 2);
  assert.equal(textInput.selectionEnd, 12);
  assert.equal(browser.submitButton.disabled, false);

  browser.dispatch("text-form", "submit");
  assert.deepEqual(socket.sentJson().at(-1), {
    type: "text_turn",
    text: "reply to A",
    summary_id: "summary-a",
    turn_id: 2,
  });

  socket.receive({ type: "text_turn_accepted", turn_id: 1 });
  assert.equal(textInput.value, "  reply to A  ");
  assert.equal(transcript.children.length, 0);

  socket.receive({ type: "text_turn_accepted", turn_id: 2 });
  assert.equal(textInput.value, "");
  assert.equal(textInput.selectionStart, 0);
  assert.equal(textInput.selectionEnd, 0);
  assert.equal(transcript.children.length, 1);
});

test("real PTT events retain the recording and summary captured at start", async (t) => {
  const browser = await createAppHarness();
  t.after(() => browser.restore());
  const socket = readyConnection(browser);

  socket.receive(dashboardState("summary-a"));
  await browser.enableMicrophone();
  assert.deepEqual(browser.audioWorkletModules, [
    "pcm-capture-worklet.js",
    "pcm-playback-worklet.js",
  ]);
  assert.deepEqual(
    browser.audioWorkletNodes.map((node) => node.processorName),
    ["pcm-playback", "pcm-capture"],
  );
  assert.equal(browser.element("ptt").disabled, false);

  browser.dispatch("ptt", "pointerdown", { pointerId: 7 });
  assert.deepEqual(socket.sentJson().at(-1), {
    type: "ptt_start",
    recording_id: 1,
    summary_id: "summary-a",
  });

  browser.dispatch("ptt", "pointerup");
  assert.deepEqual(socket.sentJson().at(-1), { type: "ptt_end", recording_id: 1 });

  socket.receive(dashboardState("summary-b"));
  assert.deepEqual(socket.sentJson().at(-1), { type: "ptt_abort", recording_id: 1 });

  browser.dispatch("ptt", "pointerdown", { pointerId: 8 });
  assert.deepEqual(socket.sentJson().at(-1), {
    type: "ptt_start",
    recording_id: 2,
    summary_id: "summary-b",
  });
  socket.receive(dashboardState("summary-c"));
  assert.deepEqual(socket.sentJson().at(-1), { type: "ptt_abort", recording_id: 2 });
  const sentAfterAbort = socket.sent.length;
  browser.dispatch("ptt", "pointerup");
  assert.equal(socket.sent.length, sentAfterAbort);
});

test("live recording state and rejection frames require exact recording correlation", async (t) => {
  const browser = await createAppHarness();
  t.after(() => browser.restore());
  const socket = readyConnection(browser);
  socket.receive(dashboardState("summary-a"));
  await browser.enableMicrophone();

  browser.dispatch("ptt", "pointerdown", { pointerId: 21 });
  browser.dispatch("ptt", "pointerup");
  assert.equal(browser.submitButton.disabled, true);

  socket.receive({ type: "state", state: "responding", recording_id: 99 });
  assert.equal(browser.submitButton.disabled, true);
  socket.receive({ type: "state", state: "responding", recording_id: 1 });
  assert.equal(browser.submitButton.disabled, false);
  assert.equal(browser.element("avatar").dataset.state, "thinking");

  browser.dispatch("ptt", "pointerdown", { pointerId: 22 });
  assert.equal(browser.element("ptt").classList.contains("active"), true);
  socket.receive({
    type: "recording_rejected",
    mode: "live_response",
    recording_id: 1,
    message: "stale rejection",
  });
  assert.equal(browser.element("ptt").classList.contains("active"), true);

  socket.receive({
    type: "recording_rejected",
    mode: "live_response",
    recording_id: 2,
    message: "matching rejection",
  });
  assert.equal(browser.element("ptt").classList.contains("active"), false);
  assert.equal(browser.element("avatar").dataset.state, "error");
  const sentAfterRejection = socket.sent.length;
  browser.dispatch("ptt", "pointerup");
  assert.equal(socket.sent.length, sentAfterRejection);
});

test("dictation binds terminal and result handlers to the exact recording operation", async (t) => {
  const browser = await createAppHarness({
    storage: { "paseoVoice.voiceMode": "dictation" },
  });
  t.after(() => browser.restore());
  const socket = readyConnection(browser, "dictation");
  const textInput = browser.element("text-input");
  const preview = browser.element("dictation-preview-text");

  socket.receive(dashboardState("summary-a"));
  textInput.value = "hello";
  textInput.setSelectionRange(5, 5);
  await browser.enableMicrophone();

  browser.dispatch("ptt", "pointerdown", { pointerId: 9 });
  assert.deepEqual(socket.sentJson().at(-1), {
    type: "ptt_start",
    recording_id: 1,
    summary_id: "summary-a",
  });
  browser.clock.tick(300);
  browser.dispatch("ptt", "pointerup");
  const sentBeforeOperation = socket.sent.length;

  socket.receive({ type: "dictation_operation", operation_id: "operation-stale", recording_id: 2 });
  assert.equal(socket.sent.length, sentBeforeOperation);

  socket.receive({ type: "dictation_operation", operation_id: "operation-a", recording_id: 1 });
  assert.deepEqual(socket.sentJson().at(-1), {
    type: "ptt_end",
    operation_id: "operation-a",
  });

  socket.receive({ type: "dictation_preview", operation_id: "operation-stale", text: "wrong" });
  assert.notEqual(preview.textContent, "wrong");
  socket.receive({ type: "dictation_preview", operation_id: "operation-a", text: "world" });
  assert.equal(preview.textContent, "world");

  socket.receive({
    type: "dictation_result",
    operation_id: "operation-stale",
    text: "wrong",
    status: "clean",
  });
  assert.equal(textInput.value, "hello");
  socket.receive({
    type: "dictation_result",
    operation_id: "operation-a",
    text: "world",
    status: "clean",
  });
  assert.equal(textInput.value, "hello world");
  assert.equal(textInput.selectionStart, 11);
  assert.equal(textInput.selectionEnd, 11);
});

test("dictation preview and result bounds preserve an astral final code point", async (t) => {
  const browser = await createAppHarness({
    storage: { "paseoVoice.voiceMode": "dictation" },
  });
  t.after(() => browser.restore());
  const socket = readyConnection(browser, "dictation");
  const textInput = browser.element("text-input");
  const preview = browser.element("dictation-preview-text");
  const emoji = "😀";

  socket.receive(dashboardState("summary-a"));
  await browser.enableMicrophone();
  browser.dispatch("ptt", "pointerdown", { pointerId: 10 });
  browser.clock.tick(300);
  browser.dispatch("ptt", "pointerup");
  socket.receive({
    type: "dictation_operation",
    operation_id: "operation-unicode",
    recording_id: 1,
  });

  const expectedPreview = `${"p".repeat(31_999)}${emoji}`;
  socket.receive({
    type: "dictation_preview",
    operation_id: "operation-unicode",
    text: `${expectedPreview}discarded`,
  });
  assert.equal(preview.textContent, expectedPreview);
  assert.equal(Array.from(preview.textContent).length, 32_000);
  assert.equal(hasUnpairedSurrogate(preview.textContent), false);

  const expectedResult = `${"r".repeat(31_999)}${emoji}`;
  socket.receive({
    type: "dictation_result",
    operation_id: "operation-unicode",
    text: `${expectedResult}discarded`,
    status: "clean",
  });
  assert.equal(textInput.value, expectedResult);
  assert.equal(Array.from(textInput.value).length, 32_000);
  assert.equal(hasUnpairedSurrogate(textInput.value), false);
  assert.equal(preview.textContent, `Inserted: ${expectedResult}`);
  assert.equal(hasUnpairedSurrogate(preview.textContent), false);
});

test("dictation failure retires only its correlated operation", async (t) => {
  const browser = await createAppHarness({
    storage: { "paseoVoice.voiceMode": "dictation" },
  });
  t.after(() => browser.restore());
  const socket = readyConnection(browser, "dictation");
  const preview = browser.element("dictation-preview-text");

  socket.receive(dashboardState("summary-a"));
  await browser.enableMicrophone();
  browser.dispatch("ptt", "pointerdown", { pointerId: 31 });
  browser.clock.tick(300);
  browser.dispatch("ptt", "pointerup");
  socket.receive({ type: "dictation_operation", operation_id: "operation-a", recording_id: 1 });
  const previewBeforeFailure = preview.textContent;

  socket.receive({
    type: "dictation_failed",
    operation_id: "operation-stale",
    message: "stale failure",
  });
  assert.equal(preview.textContent, previewBeforeFailure);
  assert.equal(browser.element("ptt").disabled, true);

  socket.receive({
    type: "dictation_failed",
    operation_id: "operation-a",
    message: "Cleanup failed safely.",
  });
  assert.equal(preview.textContent, "Cleanup failed safely.");
  assert.equal(browser.element("dictation-preview").classList.contains("hidden"), false);
  assert.equal(browser.element("ptt").disabled, false);
});

test("a correlated cancellation wins a late dictation result race", async (t) => {
  const browser = await createAppHarness({
    storage: { "paseoVoice.voiceMode": "dictation" },
  });
  t.after(() => browser.restore());
  const socket = readyConnection(browser, "dictation");
  const textInput = browser.element("text-input");

  socket.receive(dashboardState("summary-a"));
  textInput.value = "keep this draft";
  textInput.setSelectionRange(4, 4);
  await browser.enableMicrophone();
  browser.dispatch("ptt", "pointerdown", { pointerId: 32 });
  browser.clock.tick(300);
  browser.dispatch("ptt", "pointerup");
  socket.receive({ type: "dictation_operation", operation_id: "operation-a", recording_id: 1 });

  browser.dispatch("cancel-dictation", "click");
  assert.deepEqual(socket.sentJson().at(-1), {
    type: "cancel_dictation",
    operation_id: "operation-a",
  });
  assert.equal(browser.element("ptt").disabled, true);

  socket.receive({
    type: "dictation_result",
    operation_id: "operation-a",
    text: "must not be inserted",
    status: "clean",
  });
  assert.equal(textInput.value, "keep this draft");
  socket.receive({ type: "dictation_cancelled", operation_id: "operation-stale" });
  assert.equal(browser.element("ptt").disabled, true);
  socket.receive({ type: "dictation_cancelled", operation_id: "operation-a" });
  assert.equal(textInput.value, "keep this draft");
  assert.equal(browser.element("ptt").disabled, false);
});

test("a synchronous typed send failure retires ownership without losing the draft", async (t) => {
  const browser = await createAppHarness();
  t.after(() => browser.restore());
  const failedSocket = readyConnection(browser);
  const textInput = browser.element("text-input");
  const transcript = browser.element("transcript");

  failedSocket.receive(dashboardState("summary-a"));
  textInput.value = "keep this failed send";
  textInput.setSelectionRange(5, 16);
  failedSocket.failNextSend();
  browser.dispatch("text-form", "submit");
  browser.clock.tick(0);

  assert.equal(failedSocket.readyState, WebSocket.CLOSED);
  assert.equal(textInput.value, "keep this failed send");
  assert.equal(textInput.selectionStart, 5);
  assert.equal(textInput.selectionEnd, 16);
  assert.equal(browser.element("bound-thread").textContent, "No reply is bound");
  assert.equal(transcript.children.length, 0);

  const reconnectedSocket = openOnlyReplacement(browser, failedSocket);
  reconnectedSocket.receive({ type: "protocol_ready", version: 2 });
  reconnectedSocket.receive({ type: "voice_mode", mode: "live_response" });
  reconnectedSocket.receive(hostState());
  reconnectedSocket.receive({ type: "text_turn_accepted", turn_id: 1 });
  assert.equal(textInput.value, "keep this failed send");
  assert.equal(transcript.children.length, 0);

  reconnectedSocket.receive(dashboardState("summary-a"));
  textInput.value = "retry after fresh context";
  textInput.setSelectionRange(25, 25);
  browser.dispatch("text-form", "submit");
  assert.deepEqual(reconnectedSocket.sentJson().at(-1), {
    type: "text_turn",
    text: "retry after fresh context",
    summary_id: "summary-a",
    turn_id: 2,
  });
});

test("an asynchronous close retires an accepted typed send before reconnect", async (t) => {
  const browser = await createAppHarness();
  t.after(() => browser.restore());
  const firstSocket = readyConnection(browser);
  const textInput = browser.element("text-input");
  const transcript = browser.element("transcript");

  firstSocket.receive(dashboardState("summary-a"));
  textInput.value = "sent before transport loss";
  textInput.setSelectionRange(6, 15);
  browser.dispatch("text-form", "submit");
  assert.deepEqual(firstSocket.sentJson().at(-1), {
    type: "text_turn",
    text: "sent before transport loss",
    summary_id: "summary-a",
    turn_id: 1,
  });

  firstSocket.disconnect();
  assert.equal(firstSocket.readyState, WebSocket.CLOSING);
  assert.equal(textInput.value, "sent before transport loss");
  browser.clock.tick(0);
  assert.equal(firstSocket.readyState, WebSocket.CLOSED);
  assert.equal(textInput.value, "");

  const replacement = openOnlyReplacement(browser, firstSocket);
  replacement.receive({ type: "protocol_ready", version: 2 });
  replacement.receive({ type: "voice_mode", mode: "live_response" });
  replacement.receive(hostState());
  replacement.receive({ type: "text_turn_accepted", turn_id: 1 });
  assert.equal(transcript.children.length, 0);
  replacement.receive(dashboardState("summary-a"));

  textInput.value = "fresh connection turn";
  textInput.setSelectionRange(21, 21);
  browser.dispatch("text-form", "submit");
  assert.deepEqual(replacement.sentJson().at(-1), {
    type: "text_turn",
    text: "fresh connection turn",
    summary_id: "summary-a",
    turn_id: 2,
  });
});

test("reconnect while dictation is active clears old ownership before a fresh start", async (t) => {
  const browser = await createAppHarness({
    storage: { "paseoVoice.voiceMode": "dictation" },
  });
  t.after(() => browser.restore());
  const firstSocket = readyConnection(browser, "dictation");
  const textInput = browser.element("text-input");

  firstSocket.receive(dashboardState("summary-a"));
  textInput.value = "active dictation draft";
  textInput.setSelectionRange(7, 7);
  await browser.enableMicrophone();
  browser.dispatch("ptt", "pointerdown", { pointerId: 41 });
  assert.equal(browser.element("ptt").classList.contains("active"), true);

  firstSocket.disconnect();
  assert.equal(textInput.value, "active dictation draft");
  browser.clock.tick(0);
  assert.equal(textInput.value, "");
  assert.equal(browser.element("ptt").classList.contains("active"), false);
  assert.equal(browser.element("avatar").dataset.state, "disconnected");

  const replacement = openOnlyReplacement(browser, firstSocket);
  replacement.receive({ type: "protocol_ready", version: 2 });
  replacement.receive({ type: "voice_mode", mode: "dictation" });
  replacement.receive(hostState());
  replacement.receive(dashboardState("summary-b"));
  replacement.receive({
    type: "dictation_operation",
    operation_id: "operation-from-old-connection",
    recording_id: 1,
  });
  replacement.receive({
    type: "dictation_result",
    operation_id: "operation-from-old-connection",
    text: "must not appear",
    status: "clean",
  });
  replacement.receive({
    type: "dictation_cancelled",
    operation_id: "operation-from-old-connection",
  });
  assert.deepEqual(replacement.sentJson(), [hello]);
  assert.equal(textInput.value, "");
  assert.equal(browser.element("ptt").disabled, false);

  browser.dispatch("ptt", "pointerdown", { pointerId: 42 });
  const freshStart = replacement.sentJson().at(-1);
  assert.equal(freshStart.type, "ptt_start");
  assert.equal(freshStart.summary_id, "summary-b");
  assert.equal(Number.isSafeInteger(freshStart.recording_id), true);
});

test("disconnect and host selection changes clear ephemeral routing state", async (t) => {
  const browser = await createAppHarness();
  t.after(() => browser.restore());
  const firstSocket = readyConnection(browser);
  const textInput = browser.element("text-input");
  const populatedDashboard = {
    ...dashboardState("summary-a"),
    queue_count: 3,
    agents: [
      {
        thread_id: "thread-summary-a",
        thread_name: "Agent summary-a",
        state: "idle",
        provider: "provider-a/model-a",
        queued_response_count: 3,
      },
    ],
  };

  firstSocket.receive(populatedDashboard);
  firstSocket.receive({ type: "proposal", echo: "Send this?", handle: "proposal-a" });
  textInput.value = "ephemeral reconnect draft";
  textInput.setSelectionRange(4, 13);
  assert.equal(browser.element("agent-grid").children.length, 1);

  firstSocket.close();
  browser.clock.tick(0);
  assert.equal(textInput.value, "");
  assert.equal(browser.element("bound-thread").textContent, "No reply is bound");
  assert.equal(browser.element("response-destination").textContent, "Destination: no bound reply");
  assert.equal(browser.element("queue-count").textContent, "0");
  assert.equal(browser.element("agent-count").textContent, "0 agents");
  assert.equal(browser.element("agent-grid").children.length, 0);
  assert.equal(browser.element("proposal-text").textContent, "");
  assert.equal(browser.element("proposal-banner").classList.contains("hidden"), true);

  const secondSocket = openOnlyReplacement(browser, firstSocket);
  secondSocket.receive({ type: "protocol_ready", version: 2 });
  secondSocket.receive({ type: "voice_mode", mode: "live_response" });
  secondSocket.receive(hostState("host-b"));
  const hostSelect = browser.element("host-select");
  const hostBOption = hostSelect.options.find((option) => option.value === "host-b");
  assert.equal(hostSelect.options.length, 2);
  assert.equal(hostSelect.value, "host-b");
  assert.equal(hostSelect.selectedIndex, 1);
  assert.equal(hostSelect.disabled, false);
  assert.equal(textInput.disabled, false);
  assert.equal(hostBOption.selected, true);
  assert.equal(hostBOption.textContent, "Host B");
  assert.equal(hostBOption.disabled, false);
  assert.equal(browser.element("host-cwd").textContent, "~/work-b");
  assert.equal(browser.element("host-provider").textContent, "provider-b/model-b");
  secondSocket.receive(dashboardState("summary-a", "Summary summary-a", "host-b"));
  textInput.value = "ephemeral host draft";
  textInput.setSelectionRange(2, 8);

  hostSelect.value = "missing-host";
  assert.equal(hostSelect.value, "");
  assert.equal(hostSelect.selectedIndex, -1);
  hostSelect.value = "host-a";
  browser.dispatch("host-select", "change");
  assert.deepEqual(secondSocket.sentJson().at(-1), {
    type: "select_host",
    host_id: "host-a",
  });
  assert.equal(textInput.value, "");
  assert.equal(browser.element("bound-thread").textContent, "No reply is bound");
  assert.equal(browser.element("response-destination").textContent, "Destination: no bound reply");
  assert.equal(JSON.stringify(browser.storage.entries()).includes("ephemeral"), false);
});

test("broker text presentation is bounded and malformed frames never reach activity", async (t) => {
  const browser = await createAppHarness();
  t.after(() => browser.restore());
  const socket = readyConnection(browser);
  const transcript = browser.element("transcript");
  const activity = browser.element("activity");

  socket.receive({ type: "transcript_delta", text: "a".repeat(5_000) });
  assert.equal(transcript.children[0].children[1].textContent.length, 4_000);
  for (let index = 0; index < 8; index += 1) {
    socket.receive({ type: "transcript_delta", text: "b".repeat(5_000) });
  }
  assert.equal(transcript.children[0].children[1].textContent.length, 32_000);
  socket.receive({ type: "transcript_done", text: "c".repeat(33_000) });
  assert.equal(transcript.children[0].children[1].textContent.length, 32_000);

  socket.receive({ type: "user_transcript", text: "u".repeat(33_000) });
  assert.equal(transcript.children[1].children[1].textContent.length, 32_000);
  for (let index = 0; index < 70; index += 1) {
    socket.receive({ type: "user_transcript", text: `user-${index}` });
  }
  assert.equal(transcript.children.length, 64);
  assert.equal(transcript.children[0].children[1].textContent, "user-6");

  for (let index = 0; index < 130; index += 1) {
    socket.receive(`private-frame-${index}{`);
  }
  assert.equal(activity.children.length, 128);
  assert.ok(
    activity.children.every((entry) =>
      entry.textContent.includes("malformed broker frame discarded"),
    ),
  );
  assert.equal(
    activity.children.some((entry) => entry.textContent.includes("private-frame")),
    false,
  );
});

test("broker text bounds preserve astral code points at every exact boundary", async (t) => {
  const browser = await createAppHarness();
  t.after(() => browser.restore());
  const socket = readyConnection(browser);
  const transcript = browser.element("transcript");
  const emoji = "😀";

  const expectedDelta = `${"d".repeat(3_999)}${emoji}`;
  socket.receive({ type: "transcript_delta", text: `${expectedDelta}discarded` });
  const assistantBody = transcript.children[0].children[1];
  assert.equal(assistantBody.textContent, expectedDelta);
  assert.equal(Array.from(assistantBody.textContent).length, 4_000);
  assert.equal(hasUnpairedSurrogate(assistantBody.textContent), false);

  const expectedDone = `${"f".repeat(31_999)}${emoji}`;
  socket.receive({ type: "transcript_done", text: `${expectedDone}discarded` });
  assert.equal(assistantBody.textContent, expectedDone);
  assert.equal(Array.from(assistantBody.textContent).length, 32_000);
  assert.equal(hasUnpairedSurrogate(assistantBody.textContent), false);

  const expectedUser = `${"u".repeat(31_999)}${emoji}`;
  socket.receive({ type: "user_transcript", text: `${expectedUser}discarded` });
  const userText = transcript.children[1].children[1].textContent;
  assert.equal(userText, expectedUser);
  assert.equal(Array.from(userText).length, 32_000);
  assert.equal(hasUnpairedSurrogate(userText), false);

  const expectedActivity = `tool p: ${"a".repeat(231)}${emoji}`;
  socket.receive({
    type: "tool",
    phase: "p",
    name: `${"a".repeat(231)}${emoji}discarded`,
  });
  const activityText = browser.element("activity").children.at(-1).textContent;
  assert.equal(activityText.endsWith(expectedActivity), true);
  assert.equal(hasUnpairedSurrogate(activityText), false);

  const expectedError = `${"e".repeat(239)}${emoji}`;
  socket.receive({ type: "error", message: `${expectedError}discarded` });
  const errorPresentation = browser.element("avatar-state").textContent;
  assert.equal(errorPresentation.endsWith(expectedError), true);
  assert.equal(hasUnpairedSurrogate(errorPresentation), false);
});

test("persisted pagehide fully suspends resources and pageshow reconnects once", async (t) => {
  const browser = await createAppHarness();
  t.after(() => browser.restore());
  const firstSocket = readyConnection(browser);
  const textInput = browser.element("text-input");
  const microphoneSelect = browser.element("microphone-select");

  firstSocket.receive(dashboardState("summary-a"));
  await browser.enableMicrophone();
  assert.deepEqual(
    microphoneSelect.options.map((option) => option.textContent),
    ["System default", "Fake microphone"],
  );
  firstSocket.receive({ type: "transcript_delta", text: "ephemeral assistant" });
  firstSocket.receive({ type: "user_transcript", text: "ephemeral user" });
  textInput.value = "ephemeral draft";
  textInput.setSelectionRange(3, 8);
  browser.dispatch("ptt", "pointerdown", { pointerId: 61 });

  await browser.pagehide({ persisted: true });

  assert.deepEqual(
    firstSocket
      .sentJson()
      .filter(({ type }) => ["ptt_start", "ptt_end", "ptt_abort"].includes(type)),
    [
      { type: "ptt_start", recording_id: 1, summary_id: "summary-a" },
      { type: "ptt_abort", recording_id: 1 },
    ],
  );
  assert.equal(firstSocket.readyState, WebSocket.CLOSING);
  assert.equal(firstSocket.listenerCount, 0);
  assert.equal(microphoneSelect.options.length, 0);
  assert.equal(browser.element("transcript").children.length, 0);
  assert.equal(browser.element("activity").children.length, 0);
  assert.equal(textInput.value, "");
  assert.equal(JSON.stringify(browser.storage.entries()).includes("ephemeral"), false);
  assert.ok(browser.mediaTracks.every((track) => track.readyState === "ended"));
  assert.ok(browser.mediaTracks.every((track) => track.listenerCount === 0));
  assert.ok(browser.audioContexts.every((context) => context.state === "closed"));
  assert.ok(browser.audioWorkletNodes.every((node) => node.port.onmessage === null));
  assert.ok(browser.window.listenerCount > 0);
  assert.ok(browser.document.listenerCount > 0);
  assert.ok(browser.element("enable-mic").listenerCount > 0);

  browser.clock.tick(0);
  assert.equal(firstSocket.readyState, WebSocket.CLOSED);
  assert.equal(browser.clock.pendingTimerCount, 0);
  assert.equal(browser.sockets.length, 1);

  await browser.pageshow({ persisted: true });

  assert.equal(browser.sockets.length, 2);
  const resumedSocket = browser.socket;
  assert.notEqual(resumedSocket, firstSocket);
  assert.equal(resumedSocket.readyState, WebSocket.CONNECTING);
  assert.deepEqual(resumedSocket.sent, []);
  assert.equal(textInput.disabled, true);
  assert.equal(browser.element("ptt").disabled, true);
  assert.equal(browser.element("enable-mic").disabled, false);

  await browser.pageshow({ persisted: true });
  assert.equal(browser.sockets.length, 2);

  openSocket(resumedSocket);
  assert.equal(textInput.disabled, true);
  resumedSocket.receive({ type: "protocol_ready", version: 2 });
  assert.equal(textInput.disabled, true);
  resumedSocket.receive({ type: "voice_mode", mode: "live_response" });
  assert.equal(textInput.disabled, true);
  resumedSocket.receive(hostState());
  assert.equal(textInput.disabled, false);
  assert.equal(browser.element("ptt").disabled, true);

  await browser.enableMicrophone();
  assert.equal(browser.element("ptt").disabled, false);
});

test("saved-device fallback cannot outlive its BFCache page generation", async (t) => {
  const browser = await createAppHarness({
    storage: { "paseoVoice.microphoneDevice": "stale-microphone" },
    getUserMediaErrors: ["NotFoundError", "NotFoundError"],
    deferredPermissionQueries: 1,
  });
  t.after(() => browser.restore());
  readyConnection(browser).receive(dashboardState("summary-a"));

  await browser.enableMicrophone();
  assert.equal(browser.getUserMediaCalls.length, 1);
  assert.deepEqual(browser.getUserMediaCalls[0].audio.deviceId, {
    exact: "stale-microphone",
  });
  assert.equal(browser.pendingPermissionQueryCount, 1);

  await browser.pagehide({ persisted: true });
  browser.clock.tick(0);
  await browser.pageshow({ persisted: true });
  const resumedSocket = browser.socket;
  openSocket(resumedSocket);
  resumedSocket.receive({ type: "protocol_ready", version: 2 });
  resumedSocket.receive({ type: "voice_mode", mode: "live_response" });
  resumedSocket.receive(hostState());

  browser.resolvePermissionQuery("granted");
  await browser.settle();
  await browser.settle();

  assert.equal(browser.getUserMediaCalls.length, 1);
  assert.equal(browser.mediaTracks.length, 0);
  assert.equal(browser.audioContexts.length, 1);
  assert.ok(browser.audioContexts.every((context) => context.state === "closed"));
  assert.equal(browser.element("ptt").disabled, true);
  assert.equal(browser.element("enable-mic").disabled, false);
  assert.equal(
    browser.element("microphone-status").textContent,
    "Microphone access requires re-enabling.",
  );

  await browser.enableMicrophone();
  assert.equal(browser.getUserMediaCalls.length, 3);
  assert.deepEqual(browser.getUserMediaCalls[1].audio.deviceId, {
    exact: "stale-microphone",
  });
  assert.equal(Object.hasOwn(browser.getUserMediaCalls[2].audio, "deviceId"), false);
  assert.equal(browser.audioContexts.length, 2);
  assert.equal(browser.audioContexts.at(-1).state, "running");
  assert.equal(browser.mediaTracks.filter((track) => track.readyState === "live").length, 1);
  assert.equal(browser.element("ptt").disabled, false);
});

test("default-device permission observation cannot outlive its BFCache generation", async (t) => {
  const browser = await createAppHarness({ deferredPermissionQueries: 1 });
  t.after(() => browser.restore());
  readyConnection(browser).receive(dashboardState("summary-a"));

  await browser.enableMicrophone();
  assert.equal(browser.getUserMediaCalls.length, 1);
  assert.equal(browser.pendingPermissionQueryCount, 1);
  assert.equal(browser.mediaTracks.filter((track) => track.readyState === "live").length, 1);

  await browser.pagehide({ persisted: true });
  browser.clock.tick(0);
  await browser.pageshow({ persisted: true });
  browser.resolvePermissionQuery("granted");
  await browser.settle();
  await browser.settle();

  assert.equal(browser.getUserMediaCalls.length, 1);
  assert.equal(browser.mediaTracks.filter((track) => track.readyState === "live").length, 0);
  assert.equal(browser.element("ptt").disabled, true);
  assert.equal(browser.element("enable-mic").disabled, false);

  await browser.enableMicrophone();
  assert.equal(browser.getUserMediaCalls.length, 2);
  assert.equal(browser.mediaTracks.filter((track) => track.readyState === "live").length, 1);
});

test("microphone replacements retain listeners only for the current stream", async (t) => {
  const browser = await createAppHarness();
  t.after(() => browser.restore());
  const socket = readyConnection(browser);
  const echoCancellation = browser.element("echo-cancellation");
  const microphoneSelect = browser.element("microphone-select");

  socket.receive(dashboardState("summary-a"));
  await browser.enableMicrophone();

  for (let index = 0; index < 3; index += 1) {
    echoCancellation.checked = !echoCancellation.checked;
    browser.dispatch(echoCancellation, "change");
    await browser.settle();
    await browser.settle();
  }
  for (const deviceId of ["fake-microphone", ""]) {
    microphoneSelect.value = deviceId;
    browser.dispatch(microphoneSelect, "change");
    await browser.settle();
    await browser.settle();
  }

  assert.equal(browser.mediaTracks.length, 6);
  assert.equal(browser.mediaTracks.filter((track) => track.readyState === "live").length, 1);
  assert.equal(browser.mediaTracks.filter((track) => track.listenerCount > 0).length, 1);
  assert.ok(
    browser.mediaTracks
      .filter((track) => track.readyState === "ended")
      .every((track) => track.listenerCount === 0),
  );

  await browser.pagehide();
  assert.ok(browser.mediaTracks.every((track) => track.listenerCount === 0));
});

test("pagehide aborts live capture and production code disposes page resources once", async (t) => {
  const browser = await createAppHarness();
  t.after(() => browser.restore());
  const socket = readyConnection(browser);
  const textInput = browser.element("text-input");

  socket.receive(dashboardState("summary-a"));
  socket.receive({ type: "transcript_delta", text: "ephemeral assistant text" });
  socket.receive({ type: "user_transcript", text: "ephemeral user text" });
  textInput.value = "ephemeral draft";
  textInput.setSelectionRange(4, 9);
  await browser.enableMicrophone();
  browser.dispatch("ptt", "pointerdown", { pointerId: 51 });
  socket.receive({ type: "proposal", echo: "Never confirm this", handle: "proposal-hide" });

  await browser.pagehide();

  const captureControls = socket
    .sentJson()
    .filter(({ type }) => ["ptt_start", "ptt_end", "ptt_abort"].includes(type));
  assert.deepEqual(captureControls, [
    { type: "ptt_start", recording_id: 1, summary_id: "summary-a" },
    { type: "ptt_abort", recording_id: 1 },
  ]);
  assert.equal(
    socket.sentJson().some(({ type }) => type === "confirm_proposal"),
    false,
  );
  assert.equal(socket.readyState, WebSocket.CLOSING);
  assert.equal(socket.closeCount, 1);

  assert.ok(browser.mediaTracks.every((track) => track.readyState === "ended"));
  assert.ok(browser.mediaTracks.every((track) => track.stopCount === 1));
  assert.ok(browser.audioContexts.every((context) => context.state === "closed"));
  assert.ok(browser.audioContexts.every((context) => context.closeCount === 1));
  const graphNodes = browser.audioNodes.filter((node) =>
    ["worklet", "media-source", "analyser"].includes(node.kind),
  );
  assert.ok(graphNodes.length > 0);
  assert.ok(graphNodes.every((node) => node.disconnected));
  assert.ok(browser.audioWorkletNodes.every((node) => node.port.onmessage === null));
  assert.deepEqual(browser.audioWorkletNodes[0].port.messages.at(-1).message, { type: "flush" });
  assert.deepEqual(browser.audioWorkletNodes[1].port.messages.at(-1).message, {
    type: "set-active",
    active: false,
  });

  assert.equal(browser.window.listenerCount, 0);
  assert.equal(browser.document.listenerCount, 0);
  assert.equal(browser.mediaDevices.listenerCount, 0);
  assert.ok(browser.permissionStatuses.every((status) => status.listenerCount === 0));
  assert.ok(browser.mediaTracks.every((track) => track.listenerCount === 0));
  assert.equal(socket.listenerCount, 0);
  assert.ok(
    [...browser.document.elements.values()].every((element) => element.listenerCount === 0),
  );
  assert.equal(browser.element("transcript").children.length, 0);
  assert.equal(browser.element("activity").children.length, 0);
  assert.equal(textInput.value, "");
  assert.equal(browser.element("bound-thread").textContent, "No reply is bound");
  assert.equal(browser.element("proposal-text").textContent, "");
  assert.equal(browser.element("dictation-preview-text").textContent, "");
  assert.equal(JSON.stringify(browser.storage.entries()).includes("ephemeral"), false);

  browser.clock.tick(0);
  assert.equal(socket.readyState, WebSocket.CLOSED);
  assert.equal(browser.clock.pendingTimerCount, 0);
  const disposalCounts = {
    sent: socket.sent.length,
    socketCloses: socket.closeCount,
    trackStops: browser.mediaTracks.map((track) => track.stopCount),
    contextCloses: browser.audioContexts.map((context) => context.closeCount),
    nodeDisconnects: graphNodes.map((node) => node.disconnectCount),
  };
  await browser.pagehide();
  browser.clock.tick(6_000);
  assert.deepEqual(
    {
      sent: socket.sent.length,
      socketCloses: socket.closeCount,
      trackStops: browser.mediaTracks.map((track) => track.stopCount),
      contextCloses: browser.audioContexts.map((context) => context.closeCount),
      nodeDisconnects: graphNodes.map((node) => node.disconnectCount),
    },
    disposalCounts,
  );
  assert.equal(browser.sockets.length, 1);
  assert.equal(browser.clock.pendingTimerCount, 0);
});

test("persisted pagehide sends only a correlated cancellation for active dictation", async (t) => {
  const browser = await createAppHarness({
    storage: { "paseoVoice.voiceMode": "dictation" },
  });
  t.after(() => browser.restore());
  const socket = readyConnection(browser, "dictation");
  const textInput = browser.element("text-input");

  socket.receive(dashboardState("summary-a"));
  textInput.value = "discard this ephemeral draft";
  textInput.setSelectionRange(8, 17);
  await browser.enableMicrophone();
  browser.dispatch("ptt", "pointerdown", { pointerId: 52 });
  socket.receive({ type: "dictation_operation", operation_id: "operation-hide", recording_id: 1 });

  await browser.pagehide({ persisted: true });

  assert.deepEqual(
    socket
      .sentJson()
      .filter(({ type }) => ["ptt_start", "ptt_end", "cancel_dictation"].includes(type)),
    [
      { type: "ptt_start", recording_id: 1, summary_id: "summary-a" },
      { type: "cancel_dictation", operation_id: "operation-hide" },
    ],
  );
  assert.equal(textInput.value, "");
  assert.equal(browser.element("dictation-preview-text").textContent, "");
  assert.equal(socket.readyState, WebSocket.CLOSING);
  browser.clock.tick(0);
  assert.equal(socket.readyState, WebSocket.CLOSED);
  assert.equal(browser.clock.pendingTimerCount, 0);
});

test("pagehide cancels a reconnect timer that already owns the page", async (t) => {
  const browser = await createAppHarness();
  t.after(() => browser.restore());
  const socket = readyConnection(browser);

  socket.disconnect();
  browser.clock.tick(0);
  assert.equal(socket.readyState, WebSocket.CLOSED);
  assert.equal(socket.listenerCount, 0);
  assert.equal(browser.clock.pendingTimerCount, 1);

  await browser.pagehide();

  assert.equal(browser.clock.pendingTimerCount, 0);
  browser.clock.tick(3_000);
  assert.equal(browser.sockets.length, 1);
});

test("serial harness restore disposes every tracked browser resource", async (t) => {
  const browser = await createAppHarness();
  t.after(() => browser.restore());
  const socket = readyConnection(browser);
  await browser.enableMicrophone();
  socket.disconnect();

  assert.ok(browser.clock.pendingTimerCount > 0);
  assert.ok(socket.listenerCount > 0);
  assert.ok(browser.document.listenerCount > 0);
  assert.ok(browser.mediaTracks.some((track) => track.readyState === "live"));
  assert.ok(browser.audioContexts.some((context) => context.state === "running"));

  await browser.restore();
  assert.equal(browser.clock.pendingTimerCount, 0);
  assert.ok(browser.sockets.every((candidate) => candidate.readyState === 3));
  assert.ok(browser.sockets.every((candidate) => candidate.listenerCount === 0));
  assert.equal(browser.document.listenerCount, 0);
  assert.ok(browser.mediaTracks.every((track) => track.readyState === "ended"));
  assert.ok(browser.mediaTracks.every((track) => track.listenerCount === 0));
  assert.ok(browser.audioContexts.every((context) => context.state === "closed"));
  assert.ok(browser.audioWorkletNodes.every((node) => node.port.onmessage === null));
});
