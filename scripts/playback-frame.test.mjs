import assert from "node:assert/strict";
import test from "node:test";

import { createPlaybackController } from "../public/playback-frame.js";

test("burst audio waits for enough worklet capacity to preserve the complete frame", () => {
  const calls = [];
  const port = {
    onmessage: null,
    postMessage(message, transfer) {
      calls.push({ message: structuredClone(message, { transfer }), transfer });
    },
  };
  const playback = createPlaybackController(port);
  const first = new ArrayBuffer(96_000);
  const second = new ArrayBuffer(4_800);

  assert.equal(playback.enqueue(first), true);
  assert.equal(playback.enqueue(second), true);
  assert.equal(calls.length, 1);
  assert.equal(first.byteLength, 0);
  assert.equal(second.byteLength, 4_800);

  port.onmessage({ data: { type: "consumed", epoch: 0, samples: 2_399 } });
  assert.equal(calls.length, 1);
  assert.equal(second.byteLength, 4_800);

  port.onmessage({ data: { type: "consumed", epoch: 0, samples: 1 } });
  assert.equal(calls.length, 2);
  assert.equal(second.byteLength, 0);
  assert.equal(calls[1].message.type, "audio");
  assert.equal(calls[1].message.epoch, 0);
  assert.equal(calls[1].message.pcm.byteLength, 4_800);
});

test("backlog overflow flushes the response once instead of splicing more audio", () => {
  const calls = [];
  const overflows = [];
  const port = {
    onmessage: null,
    postMessage(message, transfer) {
      calls.push({ message: structuredClone(message, { transfer }), transfer });
    },
  };
  const playback = createPlaybackController(port, {
    onOverflow() {
      overflows.push("overflow");
    },
  });

  for (let frame = 0; frame < 31; frame += 1) {
    assert.equal(playback.enqueue(new ArrayBuffer(96_000)), true);
  }
  assert.equal(calls.length, 1);

  assert.equal(playback.enqueue(new ArrayBuffer(2)), false);
  assert.deepEqual(overflows, ["overflow"]);
  assert.deepEqual(calls.at(-1).message, { type: "flush", epoch: 1 });

  assert.equal(playback.enqueue(new ArrayBuffer(2)), false);
  assert.deepEqual(overflows, ["overflow"]);
  assert.equal(calls.length, 2);
});

test("flush discards pending audio and ignores stale capacity credits", () => {
  const calls = [];
  const port = {
    onmessage: null,
    postMessage(message, transfer) {
      calls.push({ message: structuredClone(message, { transfer }), transfer });
    },
  };
  const playback = createPlaybackController(port);
  const pending = new ArrayBuffer(4_800);

  assert.equal(playback.enqueue(new ArrayBuffer(96_000)), true);
  assert.equal(playback.enqueue(pending), true);
  playback.flush();
  assert.deepEqual(calls.at(-1).message, { type: "flush", epoch: 1 });
  assert.equal(pending.byteLength, 4_800);

  port.onmessage({ data: { type: "consumed", epoch: 0, samples: 48_000 } });
  assert.equal(calls.length, 2);

  assert.equal(playback.enqueue(new ArrayBuffer(2)), true);
  assert.deepEqual(calls.at(-1).message, {
    type: "audio",
    epoch: 1,
    pcm: new ArrayBuffer(2),
  });
});

test("recovery after an overflow accepts a later response without another flush", () => {
  const calls = [];
  const port = {
    onmessage: null,
    postMessage(message, transfer) {
      calls.push({ message: structuredClone(message, { transfer }), transfer });
    },
  };
  const playback = createPlaybackController(port);

  for (let frame = 0; frame < 31; frame += 1) {
    assert.equal(playback.enqueue(new ArrayBuffer(96_000)), true);
  }
  assert.equal(playback.enqueue(new ArrayBuffer(2)), false);
  assert.deepEqual(calls.at(-1).message, { type: "flush", epoch: 1 });

  playback.recover();
  assert.equal(playback.enqueue(new ArrayBuffer(2)), true);
  assert.deepEqual(calls.at(-1).message, {
    type: "audio",
    epoch: 1,
    pcm: new ArrayBuffer(2),
  });
});

test("only bounded even ArrayBuffers enter playback", () => {
  const calls = [];
  const port = {
    onmessage: null,
    postMessage(message, transfer) {
      calls.push({ message: structuredClone(message, { transfer }), transfer });
    },
  };
  const playback = createPlaybackController(port);
  const detached = new ArrayBuffer(2);
  structuredClone(detached, { transfer: [detached] });

  for (const frame of [
    new ArrayBuffer(0),
    new ArrayBuffer(3),
    new ArrayBuffer(96_002),
    detached,
    new Uint8Array(2),
    { byteLength: 2 },
    null,
    "pcm16",
  ]) {
    let result;
    assert.doesNotThrow(() => {
      result = playback.enqueue(frame);
    });
    assert.equal(result, false);
  }
  assert.equal(calls.length, 0);

  assert.equal(playback.enqueue(new ArrayBuffer(96_000)), true);
  assert.equal(calls.length, 1);
});
