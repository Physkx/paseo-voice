import test from "node:test";
import assert from "node:assert/strict";

import {
  canStartConversationalTurn,
  isNativeProposalActivationKeydown,
  proposalActivationTransition,
  proposalAwareInterfaceState,
  proposalControlFrame,
  proposalFrameTransition,
  proposalStateFromFrame,
  shouldFocusProposalConfirm,
} from "../public/interaction-gate.js";

test("native proposal activation keydown accepts only non-repeat Enter and Space", () => {
  assert.equal(isNativeProposalActivationKeydown({ key: "Enter", repeat: false }), true);
  assert.equal(isNativeProposalActivationKeydown({ key: " ", repeat: false }), true);
  assert.equal(isNativeProposalActivationKeydown({ key: "Enter", repeat: true }), false);
  assert.equal(isNativeProposalActivationKeydown({ key: " ", repeat: true }), false);
  assert.equal(isNativeProposalActivationKeydown({ key: "Escape", repeat: false }), false);
});

test("proposal activation consumes the handle captured before a replacement", () => {
  const first = proposalStateFromFrame({ echo: "Send A?", handle: "view-a" });
  const replacement = proposalStateFromFrame({ echo: "Send B?", handle: "view-b" });
  const captured = proposalActivationTransition(null, {
    type: "capture",
    action: "confirm_proposal",
    proposalState: first,
  });
  const clicked = proposalActivationTransition(captured.latch, {
    type: "click",
    action: "confirm_proposal",
    proposalState: replacement,
  });

  assert.deepEqual(captured, {
    latch: { action: "confirm_proposal", handle: "view-a" },
    control: null,
  });
  assert.deepEqual(clicked, {
    latch: null,
    control: { type: "confirm_proposal", handle: "view-a" },
  });
});

test("direct proposal clicks fall back to the currently displayed handle", () => {
  const current = proposalStateFromFrame({ echo: "Cancel B?", handle: "view-b" });

  assert.deepEqual(
    proposalActivationTransition(null, {
      type: "click",
      action: "cancel_proposal",
      proposalState: current,
    }),
    {
      latch: null,
      control: { type: "cancel_proposal", handle: "view-b" },
    },
  );
});

test("new activation overwrites an abandoned latch and matching or global clear removes it", () => {
  const first = proposalStateFromFrame({ echo: "Confirm A?", handle: "view-a" });
  const second = proposalStateFromFrame({ echo: "Cancel B?", handle: "view-b" });
  const firstCapture = proposalActivationTransition(null, {
    type: "capture",
    action: "confirm_proposal",
    proposalState: first,
  });
  const secondCapture = proposalActivationTransition(firstCapture.latch, {
    type: "capture",
    action: "cancel_proposal",
    proposalState: second,
  });
  const unrelatedBlur = proposalActivationTransition(secondCapture.latch, {
    type: "clear",
    action: "confirm_proposal",
  });
  const matchingCancel = proposalActivationTransition(unrelatedBlur.latch, {
    type: "clear",
    action: "cancel_proposal",
  });

  assert.deepEqual(secondCapture.latch, { action: "cancel_proposal", handle: "view-b" });
  assert.deepEqual(unrelatedBlur.latch, secondCapture.latch);
  assert.equal(matchingCancel.latch, null);
  assert.equal(proposalActivationTransition(secondCapture.latch, { type: "clear" }).latch, null);
});

test("malformed proposal frames preserve an existing valid presentation", () => {
  const current = proposalStateFromFrame({ echo: "Send A?", handle: "view-a" });

  assert.deepEqual(proposalFrameTransition(current, { echo: "Missing handle" }), {
    proposalState: current,
    instruction: "preserve-and-log",
  });
});

test("malformed proposal frames without a valid presentation require reconnect", () => {
  const current = proposalStateFromFrame({ echo: null });
  const transition = proposalFrameTransition(current, { echo: "Missing handle" });

  assert.deepEqual(transition, {
    proposalState: { status: "recovering", echo: null, handle: null },
    instruction: "reconnect",
  });
  assert.equal(
    canStartConversationalTurn({
      proposalState: transition.proposalState,
      confirmationDispatchInFlight: false,
    }),
    false,
  );
});

test("pending approval takes precedence over ordinary broker interface states", () => {
  const pending = proposalStateFromFrame({ echo: "Send?", handle: "view-a" });
  const cleared = proposalStateFromFrame({ echo: null });

  for (const state of ["ready", "thinking", "listening"]) {
    assert.equal(proposalAwareInterfaceState(pending, state), "awaiting-approval");
  }
  assert.equal(proposalAwareInterfaceState(pending, "error"), "error");
  assert.equal(proposalAwareInterfaceState(cleared, "thinking"), "thinking");
});

test("valid proposal frames replace the echo and presentation handle atomically", () => {
  const first = proposalStateFromFrame({ echo: "Send the response?", handle: "view-1" });
  const replacement = proposalStateFromFrame({ echo: "Create the session?", handle: "view-2" });

  assert.deepEqual(first, {
    status: "pending",
    echo: "Send the response?",
    handle: "view-1",
  });
  assert.deepEqual(replacement, {
    status: "pending",
    echo: "Create the session?",
    handle: "view-2",
  });
  assert.equal(
    canStartConversationalTurn({
      proposalState: replacement,
      confirmationDispatchInFlight: false,
    }),
    false,
  );
});

test("null proposal frames clear the handle while confirmation dispatch remains gated", () => {
  const cleared = proposalStateFromFrame({ echo: null });

  assert.deepEqual(cleared, { status: "clear", echo: null, handle: null });
  assert.equal(
    canStartConversationalTurn({
      proposalState: cleared,
      confirmationDispatchInFlight: false,
    }),
    true,
  );
  assert.equal(
    canStartConversationalTurn({
      proposalState: cleared,
      confirmationDispatchInFlight: true,
    }),
    false,
  );
});

test("malformed proposal frames discard presentation fields and fail closed", () => {
  for (const frame of [
    { echo: "Missing handle" },
    { echo: null, handle: "stale-view" },
    { echo: "", handle: "view-1" },
    { echo: "Send?", handle: "" },
    {},
  ]) {
    const invalid = proposalStateFromFrame(frame);
    assert.deepEqual(invalid, { status: "invalid", echo: null, handle: null });
    assert.equal(
      canStartConversationalTurn({
        proposalState: invalid,
        confirmationDispatchInFlight: false,
      }),
      false,
    );
  }
});

test("focus moves only when the first proposal disables the focused conversational control", () => {
  assert.equal(
    shouldFocusProposalConfirm({
      proposalWasPending: false,
      focusedControlWillBeDisabled: true,
      proposalActionFocused: false,
    }),
    true,
  );
  assert.equal(
    shouldFocusProposalConfirm({
      proposalWasPending: true,
      focusedControlWillBeDisabled: true,
      proposalActionFocused: false,
    }),
    false,
  );
  assert.equal(
    shouldFocusProposalConfirm({
      proposalWasPending: false,
      focusedControlWillBeDisabled: true,
      proposalActionFocused: true,
    }),
    false,
  );
});

test("proposal controls carry only the current presentation handle", () => {
  const pending = proposalStateFromFrame({ echo: "Send?", handle: "view-current" });

  assert.deepEqual(proposalControlFrame("confirm_proposal", pending), {
    type: "confirm_proposal",
    handle: "view-current",
  });
  assert.deepEqual(proposalControlFrame("cancel_proposal", pending), {
    type: "cancel_proposal",
    handle: "view-current",
  });
  assert.equal(proposalControlFrame("confirm_proposal", proposalStateFromFrame({})), null);
});
