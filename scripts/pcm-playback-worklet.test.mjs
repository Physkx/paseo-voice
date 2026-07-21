import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import vm from "node:vm";

const workletUrl = new URL("../public/pcm-playback-worklet.js", import.meta.url);
const playbackCapacitySamples = 48_000;

async function createPlaybackWorklet() {
  const registrations = new Map();
  class AudioWorkletProcessor {
    constructor() {
      this.port = { onmessage: null };
    }
  }

  const context = vm.createContext({
    ArrayBuffer,
    AudioWorkletProcessor,
    Int16Array,
    registerProcessor(name, Processor) {
      registrations.set(name, Processor);
    },
  });
  const source = await readFile(workletUrl, "utf8");
  new vm.Script(source, { filename: workletUrl.pathname }).runInContext(context);

  assert.deepEqual([...registrations.keys()], ["pcm-playback"]);
  const Processor = registrations.get("pcm-playback");
  const processor = new Processor();
  assert.equal(typeof processor.port.onmessage, "function");

  return {
    post(data) {
      processor.port.onmessage({ data });
    },
    process(outputs) {
      return processor.process([], outputs, {});
    },
    render(frameCount, channelCount = 1) {
      const channels = Array.from({ length: channelCount }, () =>
        new Float32Array(frameCount).fill(Number.NaN),
      );
      const alive = processor.process([], [channels], {});
      return { alive, channels: channels.map((channel) => [...channel]) };
    },
  };
}

function pcm16(samples) {
  return new Int16Array(samples).buffer;
}

test("delivered flush control cuts off queued audio and starts a new playback boundary", async () => {
  const worklet = await createPlaybackWorklet();
  worklet.post(pcm16([8192, 16384, 24576]));

  assert.deepEqual(worklet.render(1, 2), {
    alive: true,
    channels: [[0.25], [0.25]],
  });

  // Audio delivered before the control remains ordered; the worklet infers no cross-direction cutoff.
  worklet.post(pcm16([-32768]));
  assert.deepEqual(worklet.render(1, 2), {
    alive: true,
    channels: [[0.5], [0.5]],
  });

  worklet.post({ type: "flush" });
  assert.deepEqual(worklet.render(3, 2), {
    alive: true,
    channels: [
      [0, 0, 0],
      [0, 0, 0],
    ],
  });
  assert.deepEqual(worklet.render(2, 2), {
    alive: true,
    channels: [
      [0, 0],
      [0, 0],
    ],
  });

  worklet.post(pcm16([-16384]));
  assert.deepEqual(worklet.render(3, 2), {
    alive: true,
    channels: [
      [-0.5, 0, 0],
      [-0.5, 0, 0],
    ],
  });
});

test("invalid messages do not throw or poison later playback", async () => {
  const worklet = await createPlaybackWorklet();
  const detached = new ArrayBuffer(4);
  structuredClone(detached, { transfer: [detached] });

  for (const message of [
    new ArrayBuffer(0),
    new ArrayBuffer(3),
    detached,
    new Int16Array([8192]),
    new Uint8Array([0, 32]),
    { type: "audio", samples: [8192] },
    null,
    undefined,
    "pcm16",
  ]) {
    assert.doesNotThrow(() => worklet.post(message));
  }

  worklet.post(pcm16([8192, -8192]));
  assert.deepEqual(worklet.render(3), {
    alive: true,
    channels: [[0.25, -0.25, 0]],
  });
});

test("overflow preserves buffered order and permanently drops only incoming excess", async () => {
  const worklet = await createPlaybackWorklet();
  worklet.post(pcm16([8192, 16384, 24576, -8192]));
  assert.deepEqual(worklet.render(2), {
    alive: true,
    channels: [[0.25, 0.5]],
  });

  const acceptedIncoming = playbackCapacitySamples - 2;
  const incoming = new Int16Array(playbackCapacitySamples);
  incoming.fill(-16384);
  incoming[acceptedIncoming - 1] = 8192;
  incoming[acceptedIncoming] = 16384;
  incoming[acceptedIncoming + 1] = 24576;
  worklet.post(incoming.buffer);

  assert.deepEqual(worklet.render(4), {
    alive: true,
    channels: [[0.75, -0.25, -0.5, -0.5]],
  });
  const bulk = worklet.render(acceptedIncoming - 3);
  assert.equal(bulk.alive, true);
  assert.ok(bulk.channels[0].every((sample) => sample === -0.5));
  assert.deepEqual(worklet.render(4), {
    alive: true,
    channels: [[0.25, 0, 0, 0]],
  });
  assert.deepEqual(worklet.render(2), {
    alive: true,
    channels: [[0, 0]],
  });

  worklet.post(pcm16([-8192]));
  assert.deepEqual(worklet.render(2), {
    alive: true,
    channels: [[-0.25, 0]],
  });
});

test("process stays alive without output channels and leaves audio queued", async () => {
  const worklet = await createPlaybackWorklet();
  worklet.post(pcm16([8192]));

  assert.equal(worklet.process([]), true);
  assert.equal(worklet.process([[]]), true);
  assert.deepEqual(worklet.render(2), {
    alive: true,
    channels: [[0.25, 0]],
  });
});
