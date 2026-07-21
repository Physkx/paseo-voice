/**
 * Pure animation state machine for the block avatar. No DOM, WebGL, or audio
 * dependencies, so scripts/avatar-params.test.mjs can exercise it under the
 * Node test runner. The renderer in avatar-blocks.js calls update() once per
 * frame and copies the returned values into shader uniforms.
 *
 * All motion values are dimensionless 0..1 weights. The renderer decides what
 * a weight of 1 means visually. Accent is a named key resolved to a colour by
 * the renderer from CSS custom properties.
 */

export const AVATAR_ACCENTS = ["neutral", "listening", "speaking", "approval", "error", "off"];

const BASE = {
  assembly: 1,
  drift: 0,
  breath: 0,
  sway: 0,
  orbit: 0,
  rigid: 0,
  scan: 0,
  shimmer: 0,
  ripple: 0,
  mouth: 0,
  pulse: 0,
  bright: 1,
  desat: 0,
  accentGlow: 0.35,
  accent: "neutral",
  glitchMinMs: 0,
  glitchMaxMs: 0,
  glitchAmp: 0,
};

/** Target parameter sets per interface state. Missing keys fall back to BASE. */
const STATE_TARGETS = {
  connecting: { assembly: 1, drift: 0.3, bright: 0.8, accentGlow: 0.25 },
  ready: {
    drift: 1,
    breath: 1,
    sway: 1,
    glitchMinMs: 6000,
    glitchMaxMs: 10000,
    glitchAmp: 0.35,
  },
  listening: {
    drift: 0.4,
    breath: 0.6,
    sway: 0.3,
    ripple: 1,
    bright: 1.1,
    accent: "listening",
    accentGlow: 0.9,
  },
  thinking: { drift: 0.7, breath: 0.5, sway: 0.4, orbit: 1, scan: 1, accentGlow: 0.55 },
  speaking: {
    drift: 0.5,
    breath: 0.6,
    sway: 0.5,
    mouth: 1,
    accent: "speaking",
    accentGlow: 0.8,
  },
  "awaiting-approval": { rigid: 1, pulse: 1, accent: "approval", accentGlow: 0.9 },
  transcribing: { drift: 0.4, breath: 0.4, shimmer: 1, accent: "listening", accentGlow: 0.6 },
  cleaning: { drift: 0.4, breath: 0.4, shimmer: 1, accent: "listening", accentGlow: 0.6 },
  cancelling: { drift: 0.4, shimmer: 0.7, accentGlow: 0.4 },
  disconnected: { assembly: 0, bright: 0.45, desat: 1, accent: "off", accentGlow: 0 },
  error: {
    drift: 0.6,
    bright: 0.9,
    accent: "error",
    accentGlow: 1,
    glitchMinMs: 400,
    glitchMaxMs: 1200,
    glitchAmp: 1,
  },
};

export const AVATAR_STATES = Object.keys(STATE_TARGETS);

/** Numeric fields eased toward their targets every update. */
const EASED_FIELDS = [
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
];

/** Fields that must be zero when the user prefers reduced motion. */
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

const EASE_TAU_MS = { default: 260, assembly: 600, desat: 500 };

function targetsFor(state) {
  return { ...BASE, ...(STATE_TARGETS[state] ?? STATE_TARGETS.ready) };
}

/**
 * Creates the avatar parameter machine.
 *
 * reducedMotion freezes every motion weight at zero and pins assembly to 1 so
 * the renderer can draw a static formed face per state.
 * random is injectable for deterministic glitch tests.
 */
export function createAvatarParams({ reducedMotion = false, random = Math.random } = {}) {
  let state = "connecting";
  let targets = targetsFor(state);
  let nextGlitchAt = null;
  let glitchLevel = 0;
  let glitchDecayTau = 150;
  let lastNow = null;

  const current = { ...targets, assembly: 0, glitch: 0, state };
  const gates = { mouth: 0, ripple: 0 };
  const smoothed = { mic: 0, voice: 0 };

  function smoothLevel(key, raw, dt) {
    const clamped = Math.min(1, Math.max(0, raw));
    const tau = clamped > smoothed[key] ? 60 : 260;
    smoothed[key] += (clamped - smoothed[key]) * (1 - Math.exp(-dt / tau));
    return smoothed[key];
  }

  function scheduleGlitch(now) {
    if (targets.glitchMaxMs <= 0) {
      nextGlitchAt = null;
      return;
    }
    nextGlitchAt =
      now + targets.glitchMinMs + random() * (targets.glitchMaxMs - targets.glitchMinMs);
  }

  function setState(next) {
    const validated = STATE_TARGETS[next] ? next : "ready";
    if (validated === state) return;
    state = validated;
    targets = targetsFor(state);
    glitchDecayTau = state === "error" ? 300 : 150;
    nextGlitchAt = null;
    current.state = state;
  }

  function update(now, levels = {}) {
    const dt = lastNow === null ? 16 : Math.min(200, Math.max(0, now - lastNow));
    lastNow = now;

    if (reducedMotion) {
      Object.assign(current, targets);
      current.assembly = 1;
      for (const field of MOTION_FIELDS) current[field] = 0;
      current.glitch = 0;
      current.state = state;
      return current;
    }

    for (const field of EASED_FIELDS) {
      const tau = EASE_TAU_MS[field] ?? EASE_TAU_MS.default;
      if (field === "mouth" || field === "ripple") {
        gates[field] += (targets[field] - gates[field]) * (1 - Math.exp(-dt / tau));
      } else {
        current[field] += (targets[field] - current[field]) * (1 - Math.exp(-dt / tau));
      }
    }
    current.accent = targets.accent;

    const voice = smoothLevel("voice", levels.voice ?? 0, dt);
    const mic = smoothLevel("mic", levels.mic ?? 0, dt);
    current.mouth = gates.mouth * (0.12 + 0.88 * voice);
    current.ripple = gates.ripple * (0.3 + 0.7 * mic);

    if (targets.glitchMaxMs > 0 && nextGlitchAt === null) scheduleGlitch(now);
    if (nextGlitchAt !== null && now >= nextGlitchAt) {
      glitchLevel = targets.glitchAmp;
      scheduleGlitch(now);
    }
    glitchLevel *= Math.exp(-dt / glitchDecayTau);
    if (glitchLevel < 0.001) glitchLevel = 0;
    current.glitch = glitchLevel;

    current.state = state;
    return current;
  }

  return {
    setState,
    update,
    get state() {
      return state;
    },
  };
}
