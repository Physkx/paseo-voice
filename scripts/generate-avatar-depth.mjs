/**
 * Generates public/avatar-depth.png, the source texture for the block avatar.
 *
 * Channel layout, consumed by public/avatar-blocks.js:
 * - R: depth, 0 is far (skull edge), 255 is near (nose tip)
 * - G: mouth and jaw articulation weight for speech displacement
 * - B: eye and brow emphasis weight for accent glow
 * - A: silhouette, cubes exist only where alpha is above the threshold
 *
 * The current face is a procedural generic heightfield so the pipeline stays
 * reproducible with no external tools. Decision 4 in DECISIONS.md allows
 * replacing it with an orthographic depth render of a CC0 MakeHuman head that
 * follows the same channel layout. Keep the resolution small; every opaque
 * texel becomes one rendered cube instance.
 *
 * Usage: node scripts/generate-avatar-depth.mjs [--debug]
 * --debug also writes a grayscale preview of each channel next to the PNG.
 */

import { deflateSync } from "node:zlib";
import { writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const SIZE = 96;
const OUTPUT = join(dirname(fileURLToPath(import.meta.url)), "..", "public", "avatar-depth.png");

const clamp01 = (value) => Math.min(1, Math.max(0, value));

/** Smooth radial falloff, 1 at the centre, 0 at radius 1. */
function bump(dx, dy, radiusX, radiusY) {
  const d = (dx / radiusX) ** 2 + (dy / radiusY) ** 2;
  if (d >= 1) return 0;
  const t = 1 - d;
  return t * t * (3 - 2 * t);
}

/**
 * Head silhouette half width for a vertical position. y runs from -1 at the
 * crown to 1 at the chin.
 */
function halfWidth(y) {
  if (y < -0.98 || y > 0.98) return 0;
  const skull = 0.74 * Math.sqrt(Math.max(0, 1 - (y / 0.99) ** 2));
  const jawTaper = y > 0.25 ? 1 - 0.4 * ((y - 0.25) / 0.75) ** 1.4 : 1;
  const templeTuck = y > -0.35 && y < 0.2 ? 1 - 0.05 * bump(0, y + 0.05, 1, 0.3) : 1;
  return skull * jawTaper * templeTuck;
}

/** Side profile height for a vertical position, before features. */
function profile(y) {
  const forehead = 0.82 * bump(0, y + 0.55, 1, 0.75);
  const midface = 0.72 * bump(0, y - 0.05, 1, 0.85);
  const chin = 0.55 * bump(0, y - 0.8, 1, 0.45);
  return Math.max(forehead, midface, chin);
}

/** Full face field for one texel. Returns depth, mouth, eye, alpha in 0..1. */
function facePoint(x, y) {
  const w = halfWidth(y);
  if (w <= 0.01 || Math.abs(x) > w) return { depth: 0, mouth: 0, eye: 0, alpha: 0 };

  const across = x / w;
  const dome = Math.sqrt(Math.max(0, 1 - across * across));
  let depth = dome * profile(y) * 0.62;

  const browRidge = 0.1 * bump(Math.abs(x) - 0.2, y + 0.3, 0.26, 0.09);
  const eyeSocket =
    -0.15 * (bump(x - 0.24, y + 0.14, 0.13, 0.09) + bump(x + 0.24, y + 0.14, 0.13, 0.09));
  const noseRise = y > -0.2 && y < 0.34 ? ((y + 0.2) / 0.54) ** 1.6 : 0;
  const noseRidge = 0.28 * noseRise * bump(x, 0, 0.09 + 0.05 * noseRise, 1);
  const nostrils = 0.09 * bump(x, y - 0.3, 0.16, 0.05);
  const philtrum = -0.03 * bump(x, y - 0.38, 0.05, 0.05);
  const upperLip = 0.11 * bump(x, y - 0.46, 0.2, 0.045);
  const lowerLip = 0.12 * bump(x, y - 0.56, 0.17, 0.05);
  const mouthCorner =
    -0.04 * (bump(x - 0.23, y - 0.5, 0.07, 0.06) + bump(x + 0.23, y - 0.5, 0.07, 0.06));
  const chinBump = 0.09 * bump(x, y - 0.8, 0.16, 0.12);
  const cheekbone =
    0.06 * (bump(x - 0.36, y - 0.06, 0.16, 0.16) + bump(x + 0.36, y - 0.06, 0.16, 0.16));

  depth +=
    browRidge +
    eyeSocket +
    noseRidge +
    nostrils +
    philtrum +
    upperLip +
    lowerLip +
    mouthCorner +
    chinBump +
    cheekbone;

  const lipsCore = bump(x, y - 0.51, 0.24, 0.1);
  const jawRegion = 0.55 * bump(x, y - 0.72, 0.3, 0.26);
  const mouth = clamp01(lipsCore + jawRegion);

  const eye =
    clamp01(bump(x - 0.24, y + 0.14, 0.11, 0.07) + bump(x + 0.24, y + 0.14, 0.11, 0.07)) +
    0.35 * bump(Math.abs(x) - 0.2, y + 0.3, 0.24, 0.07);

  const edgeSoftness = clamp01((w - Math.abs(x)) / 0.05);
  return { depth: clamp01(depth), mouth, eye: clamp01(eye), alpha: edgeSoftness };
}

function buildPixels() {
  const pixels = new Uint8Array(SIZE * SIZE * 4);
  for (let row = 0; row < SIZE; row += 1) {
    for (let col = 0; col < SIZE; col += 1) {
      const x = (col / (SIZE - 1)) * 2 - 1;
      const y = (row / (SIZE - 1)) * 2 - 1;
      const point = facePoint(x * 1.08, y * 1.12);
      const at = (row * SIZE + col) * 4;
      pixels[at] = Math.round(point.depth * 255);
      pixels[at + 1] = Math.round(point.mouth * 255);
      pixels[at + 2] = Math.round(point.eye * 255);
      pixels[at + 3] = Math.round(point.alpha * 255);
    }
  }
  return pixels;
}

const CRC_TABLE = new Uint32Array(256).map((_, n) => {
  let c = n;
  for (let k = 0; k < 8; k += 1) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
  return c >>> 0;
});

function crc32(bytes) {
  let crc = 0xffffffff;
  for (const byte of bytes) crc = CRC_TABLE[(crc ^ byte) & 0xff] ^ (crc >>> 8);
  return (crc ^ 0xffffffff) >>> 0;
}

function chunk(type, data) {
  const out = Buffer.alloc(12 + data.length);
  out.writeUInt32BE(data.length, 0);
  out.write(type, 4, "ascii");
  data.copy(out, 8);
  out.writeUInt32BE(crc32(out.subarray(4, 8 + data.length)), 8 + data.length);
  return out;
}

function encodePng(pixels, width, height) {
  const header = Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]);
  const ihdr = Buffer.alloc(13);
  ihdr.writeUInt32BE(width, 0);
  ihdr.writeUInt32BE(height, 4);
  ihdr[8] = 8;
  ihdr[9] = 6;
  const raw = Buffer.alloc(height * (1 + width * 4));
  for (let row = 0; row < height; row += 1) {
    const from = row * width * 4;
    Buffer.from(pixels.buffer, from, width * 4).copy(raw, row * (1 + width * 4) + 1);
  }
  return Buffer.concat([
    header,
    chunk("IHDR", ihdr),
    chunk("IDAT", deflateSync(raw, { level: 9 })),
    chunk("IEND", Buffer.alloc(0)),
  ]);
}

function writeDebugChannel(pixels, channel, path) {
  const gray = new Uint8Array(SIZE * SIZE * 4);
  for (let i = 0; i < SIZE * SIZE; i += 1) {
    const value = pixels[i * 4 + 3] > 96 ? pixels[i * 4 + channel] : 0;
    gray[i * 4] = value;
    gray[i * 4 + 1] = value;
    gray[i * 4 + 2] = value;
    gray[i * 4 + 3] = 255;
  }
  writeFileSync(path, encodePng(gray, SIZE, SIZE));
}

const pixels = buildPixels();
writeFileSync(OUTPUT, encodePng(pixels, SIZE, SIZE));
const opaque = pixels.filter((_, i) => i % 4 === 3 && pixels[i] > 96).length;
console.log(`wrote ${OUTPUT} (${SIZE}x${SIZE}, ${opaque} cube texels)`);

if (process.argv.includes("--debug")) {
  writeDebugChannel(pixels, 0, "/tmp/avatar-depth-r.png");
  writeDebugChannel(pixels, 1, "/tmp/avatar-depth-g.png");
  writeDebugChannel(pixels, 2, "/tmp/avatar-depth-b.png");
  console.log("wrote /tmp/avatar-depth-r.png, -g.png, -b.png channel previews");
}
