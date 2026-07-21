import test from "node:test";
import assert from "node:assert/strict";
import { AVATAR_ACCENTS, AVATAR_STATES, createAvatarParams } from "../public/avatar-params.js";

const NUMERIC_FIELDS = [
  "assembly",
  "drift",
  "breath",
  "sway",
  "orbit",
  "rigid",
  "scan",
  "shimmer",
  "ripple",
  "mouth",
  "pulse",
  "bright",
  "desat",
  "accentGlow",
  "glitch",
];

const MOTION_FIELDS = [
  "drift",
  "breath",
  "sway",
  "orbit",
  "scan",
  "shimmer",
  "ripple",
  "mouth",
  "pulse",
];

/** Advances the machine in 16 ms frames and returns the final params. */
function settle(machine, fromMs, untilMs, levels = {}) {
  let params = null;
  for (let now = fromMs; now <= untilMs; now += 16) params = machine.update(now, levels);
  return params;
}

test("every state settles to finite params within range", () => {
  for (const state of AVATAR_STATES) {
    const machine = createAvatarParams({ random: () => 0.5 });
    machine.setState(state);
    const params = settle(machine, 0, 5000);
    for (const field of NUMERIC_FIELDS) {
      assert.ok(Number.isFinite(params[field]), `${state}.${field} is finite`);
      assert.ok(params[field] >= 0 && params[field] <= 2, `${state}.${field} in range`);
    }
    assert.ok(AVATAR_ACCENTS.includes(params.accent), `${state} accent is known`);
    assert.equal(params.state, state);
  }
});

test("unknown states fall back to ready", () => {
  const machine = createAvatarParams({ random: () => 0.5 });
  machine.setState("nonsense");
  assert.equal(machine.state, "ready");
});

test("awaiting approval freezes the face", () => {
  const machine = createAvatarParams({ random: () => 0.5 });
  machine.setState("awaiting-approval");
  const params = settle(machine, 0, 4000);
  assert.ok(params.rigid > 0.9, "rigid engages");
  assert.ok(params.drift < 0.05, "drift stops");
  assert.ok(params.pulse > 0.9, "pulse engages");
  assert.equal(params.accent, "approval");
});

test("disconnected disassembles and desaturates", () => {
  const machine = createAvatarParams({ random: () => 0.5 });
  machine.setState("ready");
  settle(machine, 0, 3000);
  machine.setState("disconnected");
  const params = settle(machine, 3000, 9000);
  assert.ok(params.assembly < 0.05, "face disassembles");
  assert.ok(params.desat > 0.9, "colour drains");
});

test("transitions ease instead of jumping", () => {
  const machine = createAvatarParams({ random: () => 0.5 });
  machine.setState("ready");
  settle(machine, 0, 3000);
  machine.setState("listening");
  const early = machine.update(3016, { mic: 1 });
  assert.ok(early.ripple > 0 && early.ripple < 0.5, "ripple ramps up gradually");
  const late = settle(machine, 3032, 6000, { mic: 1 });
  assert.ok(late.ripple > 0.9, "ripple reaches its target");
});

test("microphone level scales the listening ripple", () => {
  const machine = createAvatarParams({ random: () => 0.5 });
  machine.setState("listening");
  const quiet = settle(machine, 0, 4000, { mic: 0 });
  assert.ok(quiet.ripple > 0.2 && quiet.ripple < 0.4, "silent mic keeps a faint baseline");
  const loud = settle(machine, 4016, 8000, { mic: 1 });
  assert.ok(loud.ripple > 0.9, "loud mic drives the ripple");
});

test("voice level drives the speaking mouth", () => {
  const machine = createAvatarParams({ random: () => 0.5 });
  machine.setState("speaking");
  const silentMouth = settle(machine, 0, 4000, { voice: 0 }).mouth;
  assert.ok(silentMouth < 0.2, "silence keeps the mouth nearly closed");
  const talkingMouth = settle(machine, 4016, 8000, { voice: 1 }).mouth;
  assert.ok(talkingMouth > 0.9, "speech opens the mouth");
  const releaseMouth = settle(machine, 8016, 8200, { voice: 0 }).mouth;
  assert.ok(releaseMouth < talkingMouth, "mouth starts closing when audio stops");
  assert.ok(releaseMouth > 0.2, "release decays instead of snapping shut");
});

test("ready schedules micro glitches from the injected random source", () => {
  const machine = createAvatarParams({ random: () => 0.5 });
  machine.setState("ready");
  const atBurst = settle(machine, 0, 8000);
  assert.ok(atBurst.glitch > 0.2, "glitch fires eight seconds in");
  const afterDecay = settle(machine, 8016, 9500);
  assert.ok(afterDecay.glitch < 0.05, "glitch decays");
});

test("reduced motion pins the face static in every state", () => {
  for (const state of AVATAR_STATES) {
    const machine = createAvatarParams({ reducedMotion: true, random: () => 0.5 });
    machine.setState(state);
    const params = settle(machine, 0, 1000);
    assert.equal(params.assembly, 1, `${state} stays formed`);
    assert.equal(params.glitch, 0, `${state} never glitches`);
    for (const field of MOTION_FIELDS) {
      assert.equal(params[field], 0, `${state}.${field} is motionless`);
    }
  }
});
