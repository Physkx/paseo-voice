import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import vm from "node:vm";

const workletUrl = new URL("../public/pcm-capture-worklet.js", import.meta.url);

async function createCaptureWorklet() {
  const registrations = new Map();
  class AudioWorkletProcessor {
    constructor() {
      const messages = [];
      this.port = {
        messages,
        onmessage: null,
        postMessage(data, transfer) {
          messages.push({ data, transfer });
        },
      };
    }
  }

  const context = vm.createContext({
    ArrayBuffer,
    AudioWorkletProcessor,
    Boolean,
    Int16Array,
    Math,
    registerProcessor(name, Processor) {
      registrations.set(name, Processor);
    },
  });
  const source = await readFile(workletUrl, "utf8");
  new vm.Script(source, { filename: workletUrl.pathname }).runInContext(context);

  assert.deepEqual([...registrations.keys()], ["pcm-capture"]);
  const Processor = registrations.get("pcm-capture");
  const processor = new Processor();
  assert.equal(typeof processor.port.onmessage, "function");

  return {
    get messages() {
      return processor.port.messages;
    },
    setActive(active) {
      processor.port.onmessage({ data: { type: "set-active", active } });
    },
    post(message) {
      processor.port.onmessage({ data: message });
    },
    process(inputs) {
      return processor.process(inputs, [], {});
    },
  };
}

test("capture processor registers under the app worklet name and stays idle until active", async () => {
  const worklet = await createCaptureWorklet();
  const input = [[new Float32Array([0.5])]];

  assert.equal(worklet.process(input), true);
  assert.equal(worklet.messages.length, 0);
  worklet.post({ type: "ignored", active: true });
  assert.equal(worklet.process(input), true);
  assert.equal(worklet.messages.length, 0);
});

test("active capture converts bounded mono samples and transfers the exact PCM16 buffer", async () => {
  const worklet = await createCaptureWorklet();
  worklet.setActive(true);

  assert.equal(worklet.process([[new Float32Array([-2, -1, -0.5, 0, 0.5, 1, 2])]]), true);
  assert.equal(worklet.messages.length, 1);
  const [{ data, transfer }] = worklet.messages;
  assert.deepEqual([...new Int16Array(data)], [-32768, -32768, -16384, 0, 16383, 32767, 32767]);
  assert.equal(transfer.length, 1);
  assert.equal(transfer[0], data);

  worklet.setActive(false);
  assert.equal(worklet.process([[new Float32Array([1])]]), true);
  assert.equal(worklet.messages.length, 1);
});

test("active capture tolerates missing and empty input channels", async () => {
  const worklet = await createCaptureWorklet();
  worklet.setActive(true);

  assert.equal(worklet.process([]), true);
  assert.equal(worklet.process([[]]), true);
  assert.equal(worklet.process([[new Float32Array()]]), true);
  assert.equal(worklet.messages.length, 0);
});
