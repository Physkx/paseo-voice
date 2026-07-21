import test from "node:test";
import assert from "node:assert/strict";

import {
  applyCapturedDraft,
  canStartDictation,
  createDictationController,
  dictationOwnsContext,
  hasSameRoutingContext,
  insertText,
  routingContextChanged,
  snapshotDraft,
} from "../public/dictation-target.js";

test("every active dictation phase owns conversation context", () => {
  assert.equal(dictationOwnsContext("idle"), false);
  for (const phase of ["recording", "transcribing", "cleaning", "cancelling", "awaiting_review"]) {
    assert.equal(dictationOwnsContext(phase), true);
  }
});

test("inserts at the captured caret with smart word spacing", () => {
  assert.deepEqual(insertText("hello world", 5, 5, "brave"), {
    value: "hello brave world",
    selectionStart: 11,
    selectionEnd: 11,
  });
});

test("controller gates recording and invalidates review state on context changes", () => {
  assert.equal(canStartDictation("dictation", null), false);
  assert.equal(canStartDictation("dictation", "summary-1"), true);
  assert.equal(canStartDictation("live_response", null), true);
  assert.equal(routingContextChanged(true, "summary-1", "summary-2"), true);
  assert.equal(routingContextChanged(false, "summary-1", "summary-2"), false);
});

test("routing invalidation tombstones recording until terminal cancellation", () => {
  const captured = {
    value: "draft",
    selectionStart: 5,
    selectionEnd: 5,
    hostId: "local",
    contextId: "summary-1",
  };

  for (const current of [
    { ...captured, hostId: "remote" },
    { ...captured, contextId: "summary-2" },
  ]) {
    const controller = createDictationController();
    assert.equal(controller.begin(captured, 1), true);
    assert.deepEqual(controller.invalidate(current), {
      target: snapshotDraft(captured),
      brokerCancellation: true,
    });
    assert.equal(controller.release(), false);
    assert.equal(controller.phase, "cancelling");
    assert.equal(controller.target, null);
    assert.equal(controller.begin(current, 2), false);
    assert.equal(controller.bindOperation("dictation-1", 1), true);
    assert.equal(controller.completeCancellation("dictation-1"), true);
    assert.equal(controller.phase, "idle");
  }

  const controller = createDictationController();
  assert.equal(controller.begin({ ...captured, contextId: "summary-2" }, 2), true);
  assert.equal(controller.release(), true);
  assert.equal(controller.phase, "transcribing");
});

test("routing invalidation tombstones transcribing and cleaning state", () => {
  const captured = {
    value: "draft",
    selectionStart: 5,
    selectionEnd: 5,
    hostId: "local",
    contextId: "summary-1",
  };

  for (const phase of ["transcribing", "cleaning"]) {
    const controller = createDictationController();
    controller.begin(captured, 3);
    controller.bindOperation("dictation-3", 3);
    controller.release();
    if (phase !== "transcribing") controller.advance(phase);
    assert.equal(controller.phase, phase);
    assert.deepEqual(controller.invalidate({ ...captured, contextId: "summary-2" }), {
      target: snapshotDraft(captured),
      brokerCancellation: true,
    });
    assert.equal(controller.phase, "cancelling");
    assert.equal(controller.target, null);
    assert.equal(controller.begin({ ...captured, contextId: "summary-2" }, 4), false);
    assert.equal(controller.completeCancellation("dictation-3"), true);
    assert.equal(controller.phase, "idle");
  }
});

test("routing invalidation clears review locally without broker cancellation", () => {
  const captured = {
    value: "draft",
    selectionStart: 5,
    selectionEnd: 5,
    hostId: "local",
    contextId: "summary-1",
  };
  const controller = createDictationController();

  controller.begin(captured, 5);
  controller.bindOperation("dictation-review", 5);
  controller.release();
  controller.advance("awaiting_review");
  assert.deepEqual(controller.invalidate({ ...captured, contextId: "summary-2" }), {
    target: snapshotDraft(captured),
    brokerCancellation: false,
  });
  assert.equal(controller.phase, "idle");
  assert.equal(controller.target, null);
  assert.equal(controller.completeCancellation("dictation-review"), false);
  assert.equal(controller.begin({ ...captured, contextId: "summary-2" }, 6), true);
});

test("controller binds one broker operation while recording and clears it on invalidation", () => {
  const captured = {
    value: "draft",
    selectionStart: 5,
    selectionEnd: 5,
    hostId: "local",
    contextId: "summary-1",
  };
  const controller = createDictationController();

  assert.equal(controller.begin(captured, 7), true);
  assert.equal(controller.bindOperation("dictation-1", 7), true);
  assert.equal(controller.bindOperation("dictation-1", 7), false);
  assert.equal(controller.bindOperation("dictation-2", 7), false);
  assert.equal(controller.release(), true);
  assert.equal(controller.acceptsOperation("dictation-1"), true);
  assert.equal(controller.acceptsOperation("dictation-2"), false);

  assert.equal(controller.invalidate({ ...captured, hostId: "remote" }).brokerCancellation, true);
  assert.equal(controller.acceptsOperation("dictation-1"), false);
  assert.equal(controller.bindOperation("dictation-3", 7), false);
});

test("controller binds an operation only to its exact dictation recording ID", () => {
  const captured = {
    value: "draft",
    selectionStart: 5,
    selectionEnd: 5,
    hostId: "local",
    contextId: "summary-1",
  };
  const controller = createDictationController();

  assert.equal(controller.begin(captured, 41), true);
  assert.equal(controller.bindOperation("dictation-41", 40), false);
  assert.equal(controller.bindOperation("dictation-41", 41), true);
  assert.equal(controller.bindOperation("dictation-other", 41), false);
});

test("broker-first correlated cancellation retires every active dictation phase", () => {
  const captured = {
    value: "draft",
    selectionStart: 5,
    selectionEnd: 5,
    hostId: "local",
    contextId: "summary-1",
  };

  for (const phase of ["recording", "transcribing", "cleaning", "cancelling"]) {
    const controller = createDictationController();
    controller.begin(captured, 51);
    controller.bindOperation("dictation-51", 51);
    if (phase === "cancelling") controller.cancel();
    else if (phase !== "recording") controller.release();
    if (phase === "cleaning") controller.advance("cleaning");

    assert.equal(controller.phase, phase);
    assert.equal(controller.completeCancellation("dictation-other"), false);
    assert.equal(controller.completeCancellation("dictation-51"), true);
    assert.equal(controller.phase, "idle");
  }
});

test("pending cancellation failure keeps its tombstone until correlated cancellation", () => {
  const captured = {
    value: "draft",
    selectionStart: 5,
    selectionEnd: 5,
    hostId: "local",
    contextId: "summary-1",
  };
  const controller = createDictationController();

  controller.begin(captured, 61);
  controller.bindOperation("dictation-61", 61);
  controller.release();
  controller.advance("cleaning");
  assert.equal(controller.markPendingCancellation("dictation-other"), false);
  assert.equal(controller.markPendingCancellation("dictation-61"), true);
  assert.equal(controller.phase, "cancelling");
  assert.equal(controller.target, null);
  assert.equal(controller.acceptsState("cancelling"), true);
  assert.equal(controller.acceptsState("ready"), false);
  assert.equal(controller.begin({ ...captured, contextId: "summary-2" }, 62), false);
  assert.equal(controller.completeCancellation("dictation-61"), true);
  assert.equal(controller.begin({ ...captured, contextId: "summary-2" }, 62), true);
});

test("correlated cancellation wins a result race and retires its tombstone", () => {
  const captured = {
    value: "draft",
    selectionStart: 5,
    selectionEnd: 5,
    hostId: "local",
    contextId: "summary-1",
  };
  const controller = createDictationController();

  controller.begin(captured, 62);
  controller.bindOperation("dictation-62", 62);
  controller.release();
  controller.cancel();

  assert.equal(controller.phase, "cancelling");
  assert.equal(controller.acceptsOperation("dictation-62"), false);
  assert.equal(controller.completeCancellation("dictation-stale"), false);
  assert.equal(controller.phase, "cancelling");
  assert.equal(controller.completeCancellation("dictation-62"), true);
  assert.equal(controller.phase, "idle");
  assert.equal(controller.acceptsOperation("dictation-62"), false);
});

test("ordinary correlated failure terminates every active broker operation phase", () => {
  const captured = {
    value: "draft",
    selectionStart: 5,
    selectionEnd: 5,
    hostId: "local",
    contextId: "summary-1",
  };

  for (const phase of ["recording", "transcribing", "cleaning", "cancelling"]) {
    const controller = createDictationController();
    controller.begin(captured, 63);
    controller.bindOperation("dictation-63", 63);
    if (phase === "cancelling") controller.cancel();
    else if (phase !== "recording") controller.release();
    if (phase === "cleaning") controller.advance("cleaning");

    assert.equal(controller.completeFailure("dictation-other"), false);
    assert.equal(controller.completeFailure("dictation-63"), true);
    assert.equal(controller.phase, "idle");
  }
});

test("recording rejection resets only its exact active or tombstoned dictation", () => {
  const captured = {
    value: "original draft",
    selectionStart: 2,
    selectionEnd: 7,
    hostId: "local",
    contextId: "summary-1",
  };
  const controller = createDictationController();

  controller.begin(captured, 71);
  assert.equal(controller.rejectRecording(70), null);
  const edited = { value: "user edit", selectionStart: 2, selectionEnd: 4 };
  assert.equal(controller.rejectRecording(71), true);
  assert.deepEqual(edited, { value: "user edit", selectionStart: 2, selectionEnd: 4 });
  assert.equal(controller.phase, "idle");

  controller.begin({ ...captured, contextId: "summary-2" }, 72);
  assert.equal(controller.bindOperation("late-dictation-71", 71), false);
  controller.cancel();
  assert.equal(controller.rejectRecording(71), null);
  assert.equal(controller.phase, "cancelling");
  assert.equal(controller.rejectRecording(72), true);
  assert.equal(controller.phase, "idle");
});

test("user cancellation blocks fresh recording until one terminal cancellation", () => {
  const captured = {
    value: "draft",
    selectionStart: 5,
    selectionEnd: 5,
    hostId: "local",
    contextId: "summary-1",
  };
  const controller = createDictationController();

  assert.equal(controller.cancel(), null);
  assert.equal(controller.begin(captured, 81), true);
  assert.equal(controller.bindOperation("dictation-user", 81), true);
  assert.deepEqual(controller.cancel(), {
    target: snapshotDraft(captured),
    brokerCancellation: true,
  });
  assert.equal(controller.phase, "cancelling");
  assert.equal(controller.target, null);
  assert.equal(controller.begin({ ...captured, contextId: "summary-2" }, 82), false);
  assert.equal(controller.cancel(), null);
  assert.equal(controller.reset(), null);
  assert.equal(controller.phase, "cancelling");

  assert.equal(controller.completeCancellation("dictation-user"), true);
  assert.equal(controller.completeCancellation("dictation-user"), false);
  assert.equal(controller.begin({ ...captured, contextId: "summary-2" }, 82), true);
});

test("user cancellation clears review locally without broker cancellation", () => {
  const captured = {
    value: "draft",
    selectionStart: 5,
    selectionEnd: 5,
    hostId: "local",
    contextId: "summary-1",
  };
  const controller = createDictationController();

  controller.begin(captured, 91);
  controller.bindOperation("dictation-review", 91);
  controller.release();
  controller.advance("awaiting_review");
  assert.deepEqual(controller.cancel(), {
    target: snapshotDraft(captured),
    brokerCancellation: false,
  });
  assert.equal(controller.phase, "idle");
  assert.equal(controller.completeCancellation("dictation-review"), false);
  assert.equal(controller.begin({ ...captured, contextId: "summary-2" }, 92), true);
});

test("delayed operation binds only the cancelling tombstone and requires its terminal ID", () => {
  const captured = {
    value: "draft",
    selectionStart: 5,
    selectionEnd: 5,
    hostId: "local",
    contextId: "summary-1",
  };
  const controller = createDictationController();

  controller.begin(captured, 101);
  controller.invalidate({ ...captured, contextId: "summary-2" });
  assert.equal(controller.bindOperation("dictation-old", 100), false);
  assert.equal(controller.bindOperation("dictation-old", 101), true);
  assert.equal(controller.bindOperation("dictation-other", 101), false);
  assert.equal(controller.completeCancellation("dictation-other"), false);
  assert.equal(controller.phase, "cancelling");
  assert.equal(controller.completeCancellation("dictation-old"), true);
  assert.equal(controller.completeCancellation("dictation-old"), false);

  assert.equal(controller.begin({ ...captured, contextId: "summary-2" }, 102), true);
  assert.equal(controller.bindOperation("dictation-old", 102), false);
  assert.equal(controller.bindOperation("dictation-new", 102), true);
});

test("ready and cancelling state frames cannot revive or clear active lifecycle state", () => {
  const captured = {
    value: "draft",
    selectionStart: 5,
    selectionEnd: 5,
    hostId: "local",
    contextId: "summary-1",
  };
  const controller = createDictationController();

  assert.equal(controller.acceptsState("ready"), true);
  assert.equal(controller.acceptsState("cancelling"), false);
  controller.begin(captured, 111);
  controller.bindOperation("dictation-111", 111);
  assert.equal(controller.acceptsState("ready"), false);
  assert.equal(controller.acceptsState("cancelling"), false);
  assert.equal(controller.phase, "recording");

  controller.cancel();
  assert.equal(controller.acceptsState("ready"), false);
  assert.equal(controller.acceptsState("cancelling"), true);
  assert.equal(controller.phase, "cancelling");
  controller.completeCancellation("dictation-111");
  assert.equal(controller.acceptsState("ready"), true);
  assert.equal(controller.acceptsState("cancelling"), false);
});

test("disconnect clears cancellation state and permits a restarted operation sequence", () => {
  const captured = {
    value: "draft",
    selectionStart: 5,
    selectionEnd: 5,
    hostId: "local",
    contextId: "summary-1",
  };
  const controller = createDictationController();

  controller.begin(captured, 121);
  controller.bindOperation("dictation-1", 121);
  controller.cancel();
  controller.disconnect();
  assert.equal(controller.phase, "idle");
  assert.equal(controller.target, null);
  assert.equal(controller.completeCancellation("dictation-1"), false);
  assert.equal(controller.begin({ ...captured, contextId: "summary-2" }, 122), true);
  assert.equal(controller.bindOperation("dictation-1", 122), true);
});

test("replaces only the selected multiline range", () => {
  assert.deepEqual(insertText("keep OLD\ntail", 5, 8, "new\nlines"), {
    value: "keep new\nlines\ntail",
    selectionStart: 14,
    selectionEnd: 14,
  });
});

test("does not add spaces before punctuation and handles Unicode words", () => {
  assert.equal(insertText("Hello world", 5, 5, ",").value, "Hello, world");
  assert.equal(insertText("界界", 1, 1, "語").value, "界 語 界");
});

test("requires review if the field or selection changed", () => {
  const captured = snapshotDraft({
    value: "draft",
    selectionStart: 5,
    selectionEnd: 5,
    hostId: "local",
    contextId: "summary-1",
  });
  assert.equal(
    applyCapturedDraft(captured, { ...captured, selectionStart: 0, selectionEnd: 0 }, "text")
      .status,
    "review",
  );
  assert.equal(
    applyCapturedDraft(captured, { ...captured, value: "changed" }, "text").status,
    "review",
  );
});

test("discards results bound to a different host or response context", () => {
  const captured = snapshotDraft({
    value: "draft",
    selectionStart: 5,
    selectionEnd: 5,
    hostId: "local",
    contextId: "summary-1",
  });
  assert.equal(
    applyCapturedDraft(captured, { ...captured, hostId: "remote" }, "text").status,
    "discarded",
  );
  assert.equal(
    applyCapturedDraft(captured, { ...captured, contextId: "summary-2" }, "text").status,
    "discarded",
  );
  assert.equal(hasSameRoutingContext(captured, { ...captured, contextId: "summary-2" }), false);
});
