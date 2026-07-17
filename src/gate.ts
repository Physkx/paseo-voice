import { randomBytes } from "node:crypto";

/**
 * Two-phase write gate. Voice tools that mutate anything (send_message,
 * start_run) may only PROPOSE an action here; execution happens exclusively
 * through confirm() with the token from the proposal, after the user has
 * heard the echo and explicitly said yes.
 *
 * Confirmation policy:
 * - exactly one pending proposal; a new proposal replaces and invalidates
 *   the previous one
 * - proposals expire after ttlMs (default 120 s)
 * - tokens are random, single-use, and never derived from content
 */

export type ProposedAction =
  | { kind: "send_message"; sessionId: string; sessionName: string; text: string }
  | {
      kind: "start_run";
      prompt: string;
      provider?: string;
      cwd?: string;
      title?: string;
    };

export interface Proposal {
  token: string;
  action: ProposedAction;
  spokenEcho: string;
  expiresAt: number;
}

export type ConfirmResult =
  | { ok: true; action: ProposedAction }
  | { ok: false; reason: "no_pending" | "expired" | "wrong_token" | "already_used" };

export interface ProposalStore {
  propose(action: ProposedAction, spokenEcho: string): Proposal;
  confirm(token: string): ConfirmResult;
  cancel(): boolean;
  pending(): Proposal | null;
}

export function createProposalStore(ttlMs: number, now: () => number = Date.now): ProposalStore {
  let current: Proposal | null = null;
  let lastUsedToken: string | null = null;

  const expired = (p: Proposal) => now() > p.expiresAt;

  return {
    propose(action, spokenEcho) {
      const proposal: Proposal = {
        token: randomBytes(8).toString("hex"),
        action,
        spokenEcho,
        expiresAt: now() + ttlMs,
      };
      current = proposal;
      return proposal;
    },

    confirm(token) {
      if (current === null) {
        if (lastUsedToken !== null && token === lastUsedToken) {
          return { ok: false, reason: "already_used" };
        }
        return { ok: false, reason: "no_pending" };
      }
      if (expired(current)) {
        current = null;
        return { ok: false, reason: "expired" };
      }
      if (token !== current.token) {
        return { ok: false, reason: "wrong_token" };
      }
      const action = current.action;
      lastUsedToken = current.token;
      current = null;
      return { ok: true, action };
    },

    cancel() {
      const had = current !== null;
      current = null;
      return had;
    },

    pending() {
      if (current !== null && expired(current)) {
        current = null;
      }
      return current;
    },
  };
}
