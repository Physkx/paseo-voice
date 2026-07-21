const wordCharacter = /[\p{L}\p{N}_]/u;

function firstCharacter(text) {
  return Array.from(text)[0] ?? "";
}

function lastCharacter(text) {
  return Array.from(text).at(-1) ?? "";
}

/** Capture all mutable and routing state that makes an insertion target valid. */
export function snapshotDraft({ value, selectionStart, selectionEnd, hostId, contextId }) {
  const start = Number.isInteger(selectionStart) ? selectionStart : value.length;
  const end = Number.isInteger(selectionEnd) ? selectionEnd : start;
  return Object.freeze({ value, selectionStart: start, selectionEnd: end, hostId, contextId });
}

/** Insert or replace one range without changing content outside that range. */
export function insertText(value, selectionStart, selectionEnd, text) {
  const before = value.slice(0, selectionStart);
  const after = value.slice(selectionEnd);
  const prefix =
    wordCharacter.test(lastCharacter(before)) && wordCharacter.test(firstCharacter(text))
      ? " "
      : "";
  const suffix =
    wordCharacter.test(lastCharacter(text)) && wordCharacter.test(firstCharacter(after)) ? " " : "";
  const inserted = `${prefix}${text}${suffix}`;
  return {
    value: `${before}${inserted}${after}`,
    selectionStart: before.length + inserted.length,
    selectionEnd: before.length + inserted.length,
  };
}

/** Validate provenance before automatically applying a completed dictation. */
export function applyCapturedDraft(snapshot, current, text) {
  if (!hasSameRoutingContext(snapshot, current)) {
    return { status: "discarded" };
  }
  if (
    snapshot.value !== current.value ||
    snapshot.selectionStart !== current.selectionStart ||
    snapshot.selectionEnd !== current.selectionEnd
  ) {
    return { status: "review" };
  }
  return {
    status: "inserted",
    ...insertText(snapshot.value, snapshot.selectionStart, snapshot.selectionEnd, text),
  };
}

/** Compare only the immutable host and response provenance of two target states. */
export function hasSameRoutingContext(snapshot, current) {
  return snapshot.hostId === current.hostId && snapshot.contextId === current.contextId;
}

/** Own target provenance, lifecycle, and broker correlation for one dictation operation. */
export function createDictationController() {
  let phase = "idle";
  let target = null;
  let recordingId = null;
  let operationId = null;
  let retiredOperationId = null;

  function clear() {
    phase = "idle";
    target = null;
    recordingId = null;
    operationId = null;
  }

  function startCancellation() {
    if (!target || !["recording", "transcribing", "cleaning", "awaiting_review"].includes(phase)) {
      return null;
    }
    const captured = target;
    phase = "cancelling";
    target = null;
    return captured;
  }

  function retireAndClear() {
    const completedOperationId = operationId;
    clear();
    if (completedOperationId !== null) retiredOperationId = completedOperationId;
  }

  function cancellationDisposition(captured, brokerCancellation) {
    return Object.freeze({ target: captured, brokerCancellation });
  }

  return Object.freeze({
    get phase() {
      return phase;
    },
    get target() {
      return target;
    },
    get recordingId() {
      return recordingId;
    },
    begin(current, nextRecordingId) {
      if (
        phase !== "idle" ||
        !Number.isSafeInteger(nextRecordingId) ||
        nextRecordingId < 1 ||
        nextRecordingId > 2_147_483_647
      ) {
        return false;
      }
      target = snapshotDraft(current);
      recordingId = nextRecordingId;
      phase = "recording";
      return true;
    },
    release() {
      if (phase !== "recording") return false;
      phase = "transcribing";
      return true;
    },
    bindOperation(nextOperationId, nextRecordingId) {
      if (
        !["recording", "transcribing", "cancelling"].includes(phase) ||
        nextRecordingId !== recordingId ||
        operationId !== null ||
        typeof nextOperationId !== "string" ||
        nextOperationId.length === 0 ||
        nextOperationId === retiredOperationId
      ) {
        return false;
      }
      operationId = nextOperationId;
      if (phase !== "cancelling") retiredOperationId = null;
      return true;
    },
    acceptsOperation(nextOperationId) {
      return operationId === nextOperationId && ["transcribing", "cleaning"].includes(phase);
    },
    acceptsState(nextState) {
      if (nextState === "ready") return phase === "idle";
      if (nextState === "cancelling") return phase === "cancelling";
      return true;
    },
    markPendingCancellation(nextOperationId) {
      if (
        !["recording", "transcribing", "cleaning", "cancelling"].includes(phase) ||
        nextOperationId !== operationId
      ) {
        return false;
      }
      phase = "cancelling";
      target = null;
      return true;
    },
    cancel() {
      if (!target) return null;
      const captured = target;
      if (phase === "awaiting_review") {
        retireAndClear();
        return cancellationDisposition(captured, false);
      }
      if (!startCancellation()) return null;
      return cancellationDisposition(captured, true);
    },
    completeCancellation(nextOperationId) {
      if (
        !["recording", "transcribing", "cleaning", "cancelling"].includes(phase) ||
        typeof nextOperationId !== "string" ||
        nextOperationId !== operationId
      ) {
        return false;
      }
      retireAndClear();
      return true;
    },
    completeFailure(nextOperationId) {
      if (
        !["recording", "transcribing", "cleaning", "cancelling"].includes(phase) ||
        typeof nextOperationId !== "string" ||
        nextOperationId !== operationId
      ) {
        return false;
      }
      retireAndClear();
      return true;
    },
    rejectRecording(nextRecordingId) {
      if (phase === "idle" || nextRecordingId !== recordingId) return null;
      retireAndClear();
      return true;
    },
    disconnect() {
      clear();
      retiredOperationId = null;
    },
    advance(nextPhase) {
      const allowed = {
        transcribing: ["transcribing", "cleaning", "awaiting_review"],
        cleaning: ["cleaning", "awaiting_review"],
        awaiting_review: ["awaiting_review"],
      };
      if (!target || !allowed[phase]?.includes(nextPhase)) return false;
      phase = nextPhase;
      return true;
    },
    invalidate(current) {
      if (!target || hasSameRoutingContext(target, current)) return null;
      const captured = target;
      if (phase === "awaiting_review") {
        retireAndClear();
        return cancellationDisposition(captured, false);
      }
      startCancellation();
      return cancellationDisposition(captured, true);
    },
    reset() {
      if (phase === "cancelling") return null;
      const captured = target;
      retireAndClear();
      return captured;
    },
  });
}

/** Active dictation state is invalidated when immutable response provenance changes. */
export function routingContextChanged(hasActiveTarget, previousContextId, nextContextId) {
  return hasActiveTarget && previousContextId !== nextContextId;
}

/** Active dictation phases exclusively own the current conversational turn context. */
export function dictationOwnsContext(phase) {
  return ["recording", "transcribing", "cleaning", "cancelling", "awaiting_review"].includes(phase);
}

/** Dictation requires immutable response provenance before recording can start. */
export function canStartDictation(voiceMode, contextId) {
  return voiceMode !== "dictation" || (typeof contextId === "string" && contextId.length > 0);
}
