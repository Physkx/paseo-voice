import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import vm from "node:vm";

const workletUrl = new URL("../public/pcm-playback-worklet.js", import.meta.url);
async function createPlaybackWorklet() {
  const registrations = new Map();
  const messages = [];
  class AudioWorkletProcessor {
    constructor() {
      this.port = {
        onmessage: null,
        postMessage(message) {
          messages.push(structuredClone(message));
        },
      };
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
    messages,
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

function audio(samples, epoch = 0) {
  return { type: "audio", epoch, pcm: pcm16(samples) };
}

test("versioned audio renders completely and returns consumed capacity", async () => {
  const worklet = await createPlaybackWorklet();
  worklet.post(audio([8192, -8192]));

  assert.deepEqual(worklet.render(3), {
    alive: true,
    channels: [[0.25, -0.25, 0]],
  });
  assert.deepEqual(worklet.messages, [{ type: "consumed", epoch: 0, samples: 2 }]);
});

test("delivered flush control cuts off queued audio and starts a new playback boundary", async () => {
  const worklet = await createPlaybackWorklet();
  worklet.post(audio([8192, 16384, 24576]));

  assert.deepEqual(worklet.render(1, 2), {
    alive: true,
    channels: [[0.25], [0.25]],
  });

  // Audio delivered before the control remains ordered; the worklet infers no cross-direction cutoff.
  worklet.post(audio([-32768]));
  assert.deepEqual(worklet.render(1, 2), {
    alive: true,
    channels: [[0.5], [0.5]],
  });

  worklet.post({ type: "flush", epoch: 1 });
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

  worklet.post(audio([-16384], 1));
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

  worklet.post(audio([8192, -8192]));
  assert.deepEqual(worklet.render(3), {
    alive: true,
    channels: [[0.25, -0.25, 0]],
  });
});

test("a frame that cannot fit is rejected whole instead of spliced into playback", async () => {
  const worklet = await createPlaybackWorklet();
  const full = new Int16Array(48_000);
  full.fill(-16384);
  full[0] = 8192;
  full[1] = 16384;
  full[47_999] = 24576;
  worklet.post({ type: "audio", epoch: 0, pcm: full.buffer });
  assert.deepEqual(worklet.render(2), {
    alive: true,
    channels: [[0.25, 0.5]],
  });

  worklet.post(audio([16384, 24576, -8192]));
  const bulk = worklet.render(47_997);
  assert.equal(bulk.alive, true);
  assert.ok(bulk.channels[0].every((sample) => sample === -0.5));
  assert.deepEqual(worklet.render(3), {
    alive: true,
    channels: [[0.75, 0, 0]],
  });

  worklet.post(audio([-8192]));
  assert.deepEqual(worklet.render(2), {
    alive: true,
    channels: [[-0.25, 0]],
  });
});

test("process stays alive without output channels and leaves audio queued", async () => {
  const worklet = await createPlaybackWorklet();
  worklet.post(audio([8192]));

  assert.equal(worklet.process([]), true);
  assert.equal(worklet.process([[]]), true);
  assert.deepEqual(worklet.render(2), {
    alive: true,
    channels: [[0.25, 0]],
  });
});
