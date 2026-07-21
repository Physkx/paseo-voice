// 48,000 PCM16 samples, matching the fixed worklet queue.
const MAX_WORKLET_SAMPLES = 48_000;
const MAX_PLAYBACK_FRAME_BYTES = 96_000;
// One minute of generated speech may wait outside the fixed two-second worklet queue.
const MAX_PENDING_SAMPLES = 1_440_000;

function validPlaybackFrame(frame) {
  return (
    frame instanceof ArrayBuffer &&
    frame.byteLength > 0 &&
    frame.byteLength % 2 === 0 &&
    frame.byteLength <= MAX_PLAYBACK_FRAME_BYTES
  );
}

export function createPlaybackController(port, { onOverflow = () => {} } = {}) {
  let epoch = 0;
  let availableSamples = MAX_WORKLET_SAMPLES;
  let pendingSamples = 0;
  let failed = false;
  const pending = [];

  function drain() {
    while (pending.length > 0 && pending[0].byteLength / 2 <= availableSamples) {
      const frame = pending.shift();
      const samples = frame.byteLength / 2;
      availableSamples -= samples;
      pendingSamples -= samples;
      port.postMessage({ type: "audio", epoch, pcm: frame }, [frame]);
    }
  }

  function failOverflow() {
    pending.length = 0;
    pendingSamples = 0;
    failed = true;
    epoch += 1;
    availableSamples = MAX_WORKLET_SAMPLES;
    port.postMessage({ type: "flush", epoch });
    onOverflow();
  }

  port.onmessage = (event) => {
    const message = event.data;
    if (
      message?.type !== "consumed" ||
      message.epoch !== epoch ||
      !Number.isSafeInteger(message.samples) ||
      message.samples <= 0
    ) {
      return;
    }
    availableSamples = Math.min(MAX_WORKLET_SAMPLES, availableSamples + message.samples);
    drain();
  };

  return {
    enqueue(frame) {
      if (!validPlaybackFrame(frame) || failed) return false;
      const samples = frame.byteLength / 2;
      if (pendingSamples + samples > MAX_PENDING_SAMPLES) {
        failOverflow();
        return false;
      }
      pending.push(frame);
      pendingSamples += samples;
      drain();
      return true;
    },
    flush() {
      pending.length = 0;
      pendingSamples = 0;
      failed = false;
      epoch += 1;
      availableSamples = MAX_WORKLET_SAMPLES;
      port.postMessage({ type: "flush", epoch });
    },
    recover() {
      failed = false;
    },
    dispose() {
      pending.length = 0;
      pendingSamples = 0;
      failed = true;
      epoch += 1;
      availableSamples = MAX_WORKLET_SAMPLES;
      port.postMessage({ type: "flush", epoch });
      port.onmessage = null;
    },
  };
}
