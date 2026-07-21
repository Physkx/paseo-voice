/** Parse one complete broker proposal presentation without retaining partial fields. */
export function proposalStateFromFrame(frame) {
  if (frame?.echo === null && (frame.handle === undefined || frame.handle === null)) {
    return Object.freeze({ status: "clear", echo: null, handle: null });
  }
  if (
    typeof frame?.echo === "string" &&
    frame.echo.length > 0 &&
    typeof frame.handle === "string" &&
    frame.handle.length > 0
  ) {
    return Object.freeze({ status: "pending", echo: frame.echo, handle: frame.handle });
  }
  return Object.freeze({ status: "invalid", echo: null, handle: null });
}

/** Resolve one proposal frame into presentation state and a deterministic browser instruction. */
export function proposalFrameTransition(currentState, frame) {
  const proposalState = proposalStateFromFrame(frame);
  if (proposalState.status === "invalid") {
    if (currentState?.status === "pending") {
      return Object.freeze({ proposalState: currentState, instruction: "preserve-and-log" });
    }
    return Object.freeze({
      proposalState: Object.freeze({ status: "recovering", echo: null, handle: null }),
      instruction: "reconnect",
    });
  }
  return Object.freeze({ proposalState, instruction: "apply" });
}

/** Proposal review and dispatch own the conversational input surface. */
export function canStartConversationalTurn({ proposalState, confirmationDispatchInFlight }) {
  return proposalState?.status === "clear" && !confirmationDispatchInFlight;
}

/** Keep ordinary broker states from obscuring a pending explicit approval. */
export function proposalAwareInterfaceState(proposalState, nextState) {
  if (
    proposalState?.status === "pending" &&
    ["ready", "thinking", "listening"].includes(nextState)
  ) {
    return "awaiting-approval";
  }
  return nextState;
}

/** Bind an explicit proposal action to the current broker presentation handle. */
export function proposalControlFrame(type, proposalState) {
  if (
    proposalState?.status !== "pending" ||
    !["confirm_proposal", "cancel_proposal"].includes(type)
  ) {
    return null;
  }
  return Object.freeze({ type, handle: proposalState.handle });
}

/** Accept only the first native keyboard activation for a proposal button. */
export function isNativeProposalActivationKeydown(event) {
  return event?.repeat === false && (event.key === "Enter" || event.key === " ");
}

/** Capture and consume the proposal handle bound to one native activation gesture. */
export function proposalActivationTransition(latch, event) {
  if (event?.type === "capture") {
    const control = proposalControlFrame(event.action, event.proposalState);
    return Object.freeze({
      latch: control ? Object.freeze({ action: control.type, handle: control.handle }) : null,
      control: null,
    });
  }
  if (event?.type === "click") {
    const control =
      latch?.action === event.action
        ? Object.freeze({ type: event.action, handle: latch.handle })
        : proposalControlFrame(event.action, event.proposalState);
    return Object.freeze({ latch: null, control });
  }
  if (event?.type === "clear") {
    const shouldClear = event.action === undefined || latch?.action === event.action;
    return Object.freeze({ latch: shouldClear ? null : latch, control: null });
  }
  return Object.freeze({ latch, control: null });
}

/** Preserve focus unless a newly visible proposal disables the focused conversation control. */
export function shouldFocusProposalConfirm({
  proposalWasPending,
  focusedControlWillBeDisabled,
  proposalActionFocused,
}) {
  return !proposalWasPending && focusedControlWillBeDisabled && !proposalActionFocused;
}
