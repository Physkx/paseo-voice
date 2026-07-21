/**
 * WebGL block-face avatar. Renders the depth texture from avatar-depth.png as
 * a grid of instanced cubes with cyberpunk state animation driven by
 * avatar-params.js. The existing CSS face stays in the DOM as the fallback
 * whenever WebGL is unavailable or the context is lost.
 *
 * The whole face is a single instanced draw call. Per frame the JS side only
 * eases parameters and writes uniforms; all per-cube motion happens in the
 * vertex shader.
 */

import { Renderer, Camera, Transform, Program, Geometry, Mesh } from "./vendor/ogl.js";
import { createAvatarParams } from "./avatar-params.js";

const ALPHA_THRESHOLD = 96;
const DEPTH_SCALE = 0.62;
const FIT_SCALE = 0.78;
const DPR_CAP = 2;
const IDLE_FPS = 30;
const DEMO_STEP_MS = 3500;
const DEMO_SEQUENCE = [
  "connecting",
  "ready",
  "listening",
  "thinking",
  "speaking",
  "awaiting-approval",
  "transcribing",
  "cancelling",
  "error",
  "disconnected",
];

const VERTEX = /* glsl */ `
  attribute vec3 position;
  attribute vec3 normal;
  attribute vec3 offset;
  attribute vec4 data;
  attribute vec3 snormal;

  uniform mat4 modelViewMatrix;
  uniform mat4 projectionMatrix;
  uniform mat3 normalMatrix;
  uniform float uTime;
  uniform float uCube;
  uniform float uAssembly;
  uniform float uDrift;
  uniform float uOrbit;
  uniform float uRigid;
  uniform float uGlitch;
  uniform float uShimmer;
  uniform float uScan;
  uniform float uRipple;
  uniform float uMouth;

  varying vec3 vNormal;
  varying vec4 vFx;

  float hash(float n) {
    return fract(sin(n * 127.1) * 43758.5453);
  }

  void main() {
    float seed = data.x;
    float mouthW = data.y;
    float eyeW = data.z;
    float edgeW = data.w;
    vec3 home = offset;

    vec3 scatterDir = normalize(vec3(hash(seed) - 0.5, hash(seed + 1.3) - 0.5, hash(seed + 2.7) - 0.5) + 0.001);
    vec3 scattered = home * 0.4 + scatterDir * (1.3 + 1.5 * hash(seed + 3.1));
    float formed = clamp(uAssembly * 1.45 - hash(seed + 4.9) * 0.45, 0.0, 1.0);
    formed = formed * formed * (3.0 - 2.0 * formed);
    vec3 pos = mix(scattered, home, formed);

    float still = 1.0 - uRigid;
    float driftAmp = uDrift * (0.006 + 0.045 * edgeW) * still;
    pos += vec3(
      sin(uTime * 1.1 + seed * 37.0),
      cos(uTime * 0.9 + seed * 53.0),
      sin(uTime * 1.4 + seed * 23.0)
    ) * driftAmp;

    float orbitW = uOrbit * smoothstep(0.35, 1.0, edgeW) * (0.4 + 0.6 * hash(seed + 6.2));
    float orbitAngle = uTime * 0.8 + seed * 6.2831;
    vec3 orbitPos = home * 1.18 + vec3(cos(orbitAngle), sin(orbitAngle * 0.6), sin(orbitAngle)) * 0.16;
    pos = mix(pos, orbitPos, orbitW);

    pos.z += sin(length(home.xy) * 9.0 - uTime * 5.5) * 0.035 * uRipple * still;

    float jaw = step(home.y, -0.28);
    pos.z += uMouth * mouthW * 0.12;
    pos.y -= uMouth * mouthW * jaw * 0.07;

    float row = floor((home.y + 1.0) * 11.0);
    float tick = floor(uTime * 24.0);
    float torn = step(1.0 - uGlitch * 0.4, hash(row + tick * 0.61));
    pos.x += (hash(row + tick) - 0.5) * 0.3 * torn * uGlitch;

    float shimmer = uShimmer * (0.5 + 0.5 * sin(home.x * 3.5 - uTime * 6.0));
    float scanY = 1.0 - 2.0 * fract(uTime * 0.22);
    float scanBand = uScan * smoothstep(0.14, 0.0, abs(home.y - scanY));

    float scale = uCube * (0.82 + 0.36 * hash(seed + 5.4));
    scale *= mix(0.45, 1.0, formed);
    scale *= 1.0 - 0.25 * orbitW;
    scale = mix(scale, uCube, uRigid * 0.8);

    vec3 world = pos + position * scale;
    vNormal = normalize(normalMatrix * normalize(mix(normal, snormal, 0.68)));
    vFx = vec4(eyeW, shimmer, scanBand, torn * uGlitch);
    gl_Position = projectionMatrix * modelViewMatrix * vec4(world, 1.0);
  }
`;

const FRAGMENT = /* glsl */ `
  precision highp float;

  uniform float uTime;
  uniform vec3 uAccent;
  uniform float uAccentGlow;
  uniform float uBright;
  uniform float uDesat;
  uniform float uPulse;

  varying vec3 vNormal;
  varying vec4 vFx;

  void main() {
    vec3 normal = normalize(vNormal);
    float key = max(dot(normal, normalize(vec3(0.45, 0.55, 0.8))), 0.0);
    float fill = max(dot(normal, normalize(vec3(-0.6, -0.2, 0.4))), 0.0);
    vec3 base = vec3(0.6, 0.63, 0.68);
    vec3 col = base * (0.22 + 0.72 * key + 0.18 * fill);

    float fresnel = pow(1.0 - max(dot(normal, vec3(0.0, 0.0, 1.0)), 0.0), 2.0);
    col += uAccent * fresnel * uAccentGlow * 0.7;
    col += uAccent * vFx.x * uAccentGlow * 0.85;
    col *= 1.0 + vFx.y * 0.4;
    col += uAccent * vFx.z * 0.65;
    col *= 1.0 + 0.16 * uPulse * sin(uTime * 3.6);
    col = mix(col, vec3(col.r * 1.5, col.g * 0.35, col.b * 1.55), clamp(vFx.w, 0.0, 1.0));

    float lum = dot(col, vec3(0.299, 0.587, 0.114));
    col = mix(col, vec3(lum * 0.85), uDesat);
    col *= uBright;
    col *= 0.955 + 0.045 * sin(gl_FragCoord.y * 3.1);

    gl_FragColor = vec4(col, 1.0);
  }
`;

const CUBE_POSITIONS = new Float32Array([
  -1, -1, 1, 1, -1, 1, 1, 1, 1, -1, 1, 1, 1, -1, -1, -1, -1, -1, -1, 1, -1, 1, 1, -1, -1, 1, 1, 1,
  1, 1, 1, 1, -1, -1, 1, -1, -1, -1, -1, 1, -1, -1, 1, -1, 1, -1, -1, 1, 1, -1, 1, 1, -1, -1, 1, 1,
  -1, 1, 1, 1, -1, -1, -1, -1, -1, 1, -1, 1, 1, -1, 1, -1,
]);
const CUBE_NORMALS = new Float32Array([
  0, 0, 1, 0, 0, 1, 0, 0, 1, 0, 0, 1, 0, 0, -1, 0, 0, -1, 0, 0, -1, 0, 0, -1, 0, 1, 0, 0, 1, 0, 0,
  1, 0, 0, 1, 0, 0, -1, 0, 0, -1, 0, 0, -1, 0, 0, -1, 0, 1, 0, 0, 1, 0, 0, 1, 0, 0, 1, 0, 0, -1, 0,
  0, -1, 0, 0, -1, 0, 0, -1, 0, 0,
]);
const CUBE_INDICES = new Uint16Array(
  Array.from({ length: 6 }, (_, face) => {
    const base = face * 4;
    return [base, base + 1, base + 2, base, base + 2, base + 3];
  }).flat(),
);

async function loadImageData(src) {
  const image = new Image();
  image.src = src;
  await image.decode();
  const canvas = document.createElement("canvas");
  canvas.width = image.naturalWidth;
  canvas.height = image.naturalHeight;
  const context = canvas.getContext("2d", { willReadFrequently: true });
  context.drawImage(image, 0, 0);
  return context.getImageData(0, 0, canvas.width, canvas.height);
}

/** Distance to the silhouette edge in texels, capped at 4, as a 0..1 weight. */
function edgeWeights(alpha, width, height) {
  const solid = (col, row) =>
    col >= 0 &&
    row >= 0 &&
    col < width &&
    row < height &&
    alpha[row * width + col] > ALPHA_THRESHOLD;
  const weights = new Float32Array(width * height);
  for (let row = 0; row < height; row += 1) {
    for (let col = 0; col < width; col += 1) {
      if (!solid(col, row)) continue;
      let distance = 4;
      rings: for (let ring = 1; ring < 4; ring += 1) {
        for (let dy = -ring; dy <= ring; dy += 1) {
          for (let dx = -ring; dx <= ring; dx += 1) {
            if (Math.max(Math.abs(dx), Math.abs(dy)) !== ring) continue;
            if (!solid(col + dx, row + dy)) {
              distance = ring;
              break rings;
            }
          }
        }
      }
      weights[row * width + col] = 1 - (distance - 1) / 3;
    }
  }
  return weights;
}

function buildInstances(imageData) {
  const { width, height, data } = imageData;
  const alpha = new Uint8Array(width * height);
  const depth = new Float32Array(width * height);
  for (let i = 0; i < width * height; i += 1) {
    alpha[i] = data[i * 4 + 3];
    depth[i] = data[i * 4] / 255;
  }
  const edges = edgeWeights(alpha, width, height);
  const spacing = (2 / (width - 1)) * FIT_SCALE;

  const depthAt = (col, row) => {
    const clampedCol = Math.min(width - 1, Math.max(0, col));
    const clampedRow = Math.min(height - 1, Math.max(0, row));
    const at = clampedRow * width + clampedCol;
    return alpha[at] > ALPHA_THRESHOLD ? depth[at] : 0;
  };

  const offsets = [];
  const extras = [];
  const surface = [];
  for (let row = 0; row < height; row += 1) {
    for (let col = 0; col < width; col += 1) {
      const at = row * width + col;
      if (alpha[at] <= ALPHA_THRESHOLD) continue;
      const x = ((col / (width - 1)) * 2 - 1) * FIT_SCALE;
      const y = (1 - (row / (height - 1)) * 2) * FIT_SCALE;
      const z = (depth[at] * DEPTH_SCALE - DEPTH_SCALE * 0.45) * FIT_SCALE;
      offsets.push(x, y, z);
      const seed = ((at * 2654435761) % 4294967296) / 4294967296;
      extras.push(seed, data[at * 4 + 1] / 255, data[at * 4 + 2] / 255, edges[at]);

      const texel = 2 / (width - 1);
      const gradX = ((depthAt(col + 1, row) - depthAt(col - 1, row)) * DEPTH_SCALE) / (2 * texel);
      const gradY = ((depthAt(col, row - 1) - depthAt(col, row + 1)) * DEPTH_SCALE) / (2 * texel);
      const inverseLength = 1 / Math.hypot(gradX, gradY, 1);
      surface.push(-gradX * inverseLength, -gradY * inverseLength, inverseLength);
    }
  }
  return {
    count: offsets.length / 3,
    offset: new Float32Array(offsets),
    data: new Float32Array(extras),
    surface: new Float32Array(surface),
    spacing,
  };
}

function accentColors(host) {
  const styles = getComputedStyle(host);
  const parse = (name, fallback) => {
    const value = styles.getPropertyValue(name).trim();
    const match = /^#([0-9a-f]{6})$/i.exec(value);
    if (!match) return fallback;
    const hex = parseInt(match[1], 16);
    return [((hex >> 16) & 255) / 255, ((hex >> 8) & 255) / 255, (hex & 255) / 255];
  };
  return {
    neutral: parse("--avatar-accent-neutral", [0.35, 0.55, 0.45]),
    listening: parse("--avatar-accent-listening", [0.29, 0.87, 0.5]),
    speaking: parse("--avatar-accent-speaking", [0.91, 0.47, 0.98]),
    approval: parse("--avatar-accent-approval", [0.98, 0.75, 0.14]),
    error: parse("--avatar-accent-error", [0.97, 0.44, 0.44]),
    off: [0.3, 0.32, 0.34],
  };
}

/**
 * Creates the block avatar inside the given host element. Resolves to null
 * when WebGL is unavailable so the caller can keep the CSS face. The returned
 * handle exposes setState(state), setLevels({ mic, voice }) and destroy().
 */
export async function createBlockAvatar({
  host,
  depthSrc = "avatar-depth.png",
  reducedMotion = false,
  demo = false,
  demoPin = null,
  random = Math.random,
} = {}) {
  if (!host) return null;
  let imageData;
  try {
    imageData = await loadImageData(depthSrc);
  } catch {
    return null;
  }

  let renderer;
  try {
    renderer = new Renderer({
      dpr: Math.min(window.devicePixelRatio || 1, DPR_CAP),
      alpha: true,
      antialias: true,
      premultipliedAlpha: true,
    });
  } catch {
    return null;
  }
  const gl = renderer.gl;
  if (!gl) return null;

  const canvas = gl.canvas;
  canvas.className = "avatar-canvas";
  canvas.setAttribute("aria-hidden", "true");
  host.append(canvas);
  host.classList.add("blocks-active");

  const instances = buildInstances(imageData);
  const params = createAvatarParams({ reducedMotion, random });
  const accents = accentColors(host);

  const camera = new Camera(gl, { fov: 38, near: 0.1, far: 20 });
  camera.position.set(0, 0, 3.1);
  const scene = new Transform();

  const uniforms = {
    uTime: { value: 0 },
    uCube: { value: instances.spacing * 0.44 },
    uAssembly: { value: 0 },
    uDrift: { value: 0 },
    uOrbit: { value: 0 },
    uRigid: { value: 0 },
    uGlitch: { value: 0 },
    uShimmer: { value: 0 },
    uScan: { value: 0 },
    uRipple: { value: 0 },
    uMouth: { value: 0 },
    uAccent: { value: accents.neutral },
    uAccentGlow: { value: 0.3 },
    uBright: { value: 1 },
    uDesat: { value: 0 },
    uPulse: { value: 0 },
  };

  const geometry = new Geometry(gl, {
    position: { size: 3, data: CUBE_POSITIONS },
    normal: { size: 3, data: CUBE_NORMALS },
    index: { data: CUBE_INDICES },
    offset: { instanced: 1, size: 3, data: instances.offset },
    data: { instanced: 1, size: 4, data: instances.data },
    snormal: { instanced: 1, size: 3, data: instances.surface },
  });
  const program = new Program(gl, { vertex: VERTEX, fragment: FRAGMENT, uniforms });
  const mesh = new Mesh(gl, { geometry, program });
  mesh.setParent(scene);

  let disposed = false;
  let rafId = 0;
  let lastFrameAt = 0;
  let demoIndex = 0;
  let demoNextAt = 0;
  let levels = { mic: 0, voice: 0 };
  let restoreAttempted = false;
  const analysers = { mic: null, voice: null };
  let analyserScratch = null;

  function measureLevel(analyser) {
    if (!analyser) return 0;
    if (!analyserScratch || analyserScratch.length < analyser.fftSize) {
      analyserScratch = new Uint8Array(analyser.fftSize);
    }
    const samples = analyserScratch.subarray(0, analyser.fftSize);
    analyser.getByteTimeDomainData(samples);
    let sum = 0;
    for (let i = 0; i < samples.length; i += 1) {
      const deviation = (samples[i] - 128) / 128;
      sum += deviation * deviation;
    }
    return Math.min(1, Math.sqrt(sum / samples.length) * 3.5);
  }

  /** Speech-like synthetic levels so demo mode animates without audio. */
  function demoLevels(seconds) {
    const burst = Math.max(0, Math.sin(seconds * 5.9)) * (0.55 + 0.45 * Math.sin(seconds * 1.4));
    return { mic: burst * burst, voice: burst * burst };
  }

  function resize() {
    const size = Math.max(1, Math.min(host.clientWidth, host.clientHeight || host.clientWidth));
    renderer.setSize(size, size);
    camera.perspective({ aspect: 1 });
  }
  const observer = new ResizeObserver(resize);
  observer.observe(host);
  resize();

  function frame(now) {
    if (disposed) return;
    rafId = requestAnimationFrame(frame);

    const idle = params.state === "ready" || params.state === "disconnected";
    const budget = idle && !demo ? 1000 / IDLE_FPS : 0;
    if (now - lastFrameAt < budget) return;
    lastFrameAt = now;

    if (demo && now >= demoNextAt) {
      params.setState(demoPin ?? DEMO_SEQUENCE[demoIndex % DEMO_SEQUENCE.length]);
      demoIndex += 1;
      demoNextAt = now + DEMO_STEP_MS;
    }

    const seconds = now / 1000;
    const frameLevels = demo
      ? demoLevels(seconds)
      : {
          mic: analysers.mic ? measureLevel(analysers.mic) : levels.mic,
          voice: analysers.voice ? measureLevel(analysers.voice) : levels.voice,
        };
    const values = params.update(now, frameLevels);
    uniforms.uTime.value = seconds;
    uniforms.uAssembly.value = values.assembly;
    uniforms.uDrift.value = values.drift;
    uniforms.uOrbit.value = values.orbit;
    uniforms.uRigid.value = values.rigid;
    uniforms.uGlitch.value = values.glitch;
    uniforms.uShimmer.value = values.shimmer;
    uniforms.uScan.value = values.scan;
    uniforms.uRipple.value = values.ripple;
    uniforms.uMouth.value = values.mouth;
    uniforms.uAccentGlow.value = values.accentGlow;
    uniforms.uBright.value = values.bright;
    uniforms.uDesat.value = values.desat;
    uniforms.uPulse.value = values.pulse;
    uniforms.uAccent.value = accents[values.accent] ?? accents.neutral;

    scene.rotation.y = Math.sin(seconds * 0.55) * 0.1 * values.sway;
    scene.rotation.x = Math.sin(seconds * 0.34) * 0.035 * values.sway;
    scene.position.z = Math.sin(seconds * 0.8) * 0.025 * values.breath;

    renderer.render({ scene, camera });
  }

  function start() {
    if (!disposed && !rafId) rafId = requestAnimationFrame(frame);
  }
  function stop() {
    if (rafId) cancelAnimationFrame(rafId);
    rafId = 0;
  }
  function onVisibility() {
    if (document.hidden) stop();
    else start();
  }
  document.addEventListener("visibilitychange", onVisibility);

  function fallbackToCss() {
    destroy();
  }

  canvas.addEventListener("webglcontextlost", (event) => {
    event.preventDefault();
    stop();
    if (restoreAttempted) {
      fallbackToCss();
      return;
    }
    restoreAttempted = true;
    setTimeout(() => {
      if (disposed) return;
      const ext = gl.getExtension("WEBGL_lose_context");
      if (ext) ext.restoreContext();
      else fallbackToCss();
    }, 1000);
  });
  canvas.addEventListener("webglcontextrestored", () => {
    if (!disposed) start();
  });

  function destroy() {
    if (disposed) return;
    disposed = true;
    stop();
    observer.disconnect();
    document.removeEventListener("visibilitychange", onVisibility);
    host.classList.remove("blocks-active");
    canvas.remove();
    gl.getExtension("WEBGL_lose_context")?.loseContext();
  }

  start();

  return {
    setState(state) {
      if (!demo) params.setState(state);
    },
    setLevels(next) {
      levels = { mic: next?.mic ?? 0, voice: next?.voice ?? 0 };
    },
    setAnalysers(next) {
      analysers.mic = next?.mic ?? null;
      analysers.voice = next?.voice ?? null;
    },
    destroy,
    get demo() {
      return demo;
    },
    get cubeCount() {
      return instances.count;
    },
  };
}
