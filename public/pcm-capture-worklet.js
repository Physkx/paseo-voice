/**
 * Captures mono audio at the AudioContext rate (the app creates the context
 * at 24000 Hz so no resampling is needed) and posts Int16 PCM blocks to the
 * main thread. Capture only runs while `active` is true (push-to-talk).
 */
class PcmCaptureProcessor extends AudioWorkletProcessor {
  constructor() {
    super();
    this.active = false;
    this.port.onmessage = (event) => {
      if (event.data && event.data.type === "set-active") {
        this.active = Boolean(event.data.active);
      }
    };
  }

  process(inputs) {
    if (!this.active) return true;
    const channel = inputs[0] && inputs[0][0];
    if (!channel || channel.length === 0) return true;
    const out = new Int16Array(channel.length);
    for (let i = 0; i < channel.length; i += 1) {
      const s = Math.max(-1, Math.min(1, channel[i]));
      out[i] = s < 0 ? s * 0x8000 : s * 0x7fff;
    }
    this.port.postMessage(out.buffer, [out.buffer]);
    return true;
  }
}

registerProcessor("pcm-capture", PcmCaptureProcessor);
