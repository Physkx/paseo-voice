import { describe, expect, it } from "vitest";
import { createProposalStore, type ProposedAction } from "../src/gate.js";

const sendAction: ProposedAction = {
  kind: "send_message",
  sessionId: "abc",
  sessionName: "auth",
  text: "also run the linter",
};

function fixedClock(start = 1_000_000) {
  let t = start;
  return { now: () => t, advance: (ms: number) => (t += ms) };
}

describe("createProposalStore", () => {
  it("confirm with the right token executes exactly the proposed action", () => {
    const clock = fixedClock();
    const store = createProposalStore(120_000, clock.now);
    const proposal = store.propose(sendAction, "Send to auth: also run the linter. Confirm?");
    const result = store.confirm(proposal.token);
    expect(result).toEqual({ ok: true, action: sendAction });
  });

  it("tokens are single use", () => {
    const clock = fixedClock();
    const store = createProposalStore(120_000, clock.now);
    const proposal = store.propose(sendAction, "echo");
    expect(store.confirm(proposal.token).ok).toBe(true);
    expect(store.confirm(proposal.token)).toEqual({ ok: false, reason: "already_used" });
  });

  it("a new proposal invalidates the previous token", () => {
    const clock = fixedClock();
    const store = createProposalStore(120_000, clock.now);
    const first = store.propose(sendAction, "echo one");
    const second = store.propose({ kind: "start_run", prompt: "fix tests" }, "echo two");
    expect(store.confirm(first.token)).toEqual({ ok: false, reason: "wrong_token" });
    expect(store.confirm(second.token).ok).toBe(true);
  });

  it("proposals expire after the TTL", () => {
    const clock = fixedClock();
    const store = createProposalStore(120_000, clock.now);
    const proposal = store.propose(sendAction, "echo");
    clock.advance(120_001);
    expect(store.confirm(proposal.token)).toEqual({ ok: false, reason: "expired" });
    expect(store.pending()).toBeNull();
  });

  it("confirm without any proposal reports no_pending", () => {
    const store = createProposalStore(120_000, fixedClock().now);
    expect(store.confirm("whatever")).toEqual({ ok: false, reason: "no_pending" });
  });

  it("cancel clears the pending proposal", () => {
    const clock = fixedClock();
    const store = createProposalStore(120_000, clock.now);
    const proposal = store.propose(sendAction, "echo");
    expect(store.cancel()).toBe(true);
    expect(store.cancel()).toBe(false);
    expect(store.confirm(proposal.token)).toEqual({ ok: false, reason: "no_pending" });
  });

  it("pending reflects TTL and stores the echo", () => {
    const clock = fixedClock();
    const store = createProposalStore(50, clock.now);
    store.propose(sendAction, "the echo");
    expect(store.pending()?.spokenEcho).toBe("the echo");
    clock.advance(51);
    expect(store.pending()).toBeNull();
  });

  it("tokens differ between proposals", () => {
    const store = createProposalStore(120_000, fixedClock().now);
    const a = store.propose(sendAction, "echo");
    const b = store.propose(sendAction, "echo");
    expect(a.token).not.toBe(b.token);
  });
});
