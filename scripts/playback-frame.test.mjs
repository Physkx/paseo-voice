import assert from "node:assert/strict";
import test from "node:test";
import { MessageChannel } from "node:worker_threads";

import { enqueuePlaybackFrame } from "../public/playback-frame.js";

test("accepted PCM transfers ownership to the playback worklet", () => {
  const channel = new MessageChannel();
  const calls = [];
  const port = {
    postMessage(message, transfer) {
      calls.push({ message, transfer });
      channel.port1.postMessage(message, transfer);
    },
  };
  const frame = new Int16Array([8192, -8192]).buffer;

  try {
    assert.equal(enqueuePlaybackFrame(port, frame), true);
    assert.equal(calls.length, 1);
    assert.strictEqual(calls[0].message, frame);
    assert.equal(calls[0].transfer.length, 1);
    assert.strictEqual(calls[0].transfer[0], frame);
    assert.equal(frame.byteLength, 0);
    assert.equal(enqueuePlaybackFrame(port, frame), false);
    assert.equal(calls.length, 1);
  } finally {
    channel.port1.close();
    channel.port2.close();
  }
});

test("only bounded even ArrayBuffers produce the app speaking decision", () => {
  const calls = [];
  const port = {
    postMessage(message, transfer) {
      calls.push({ message, transfer });
    },
  };
  const states = [];
  const receive = (frame) => {
    const enqueued = enqueuePlaybackFrame(port, frame);
    if (enqueued) states.push("speaking");
    return enqueued;
  };
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
      result = receive(frame);
    });
    assert.equal(result, false);
  }
  assert.deepEqual(states, []);
  assert.equal(calls.length, 0);

  assert.equal(receive(new ArrayBuffer(96_000)), true);
  assert.deepEqual(states, ["speaking"]);
  assert.equal(calls.length, 1);
});

test("enqueue failures report false without an app speaking decision", () => {
  for (const port of [
    null,
    {},
    {
      postMessage() {
        throw new Error("worklet unavailable");
      },
    },
  ]) {
    let state = "ready";
    let result;
    assert.doesNotThrow(() => {
      result = enqueuePlaybackFrame(port, new ArrayBuffer(2));
      if (result) state = "speaking";
    });
    assert.equal(result, false);
    assert.equal(state, "ready");
  }
});
