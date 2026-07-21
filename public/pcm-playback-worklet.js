/**
 * Plays queued Int16 PCM at the AudioContext rate (24000 Hz). The main
 * thread posts ArrayBuffers of pcm16 to enqueue and {type:"flush"} to drop
 * everything (barge-in). The queue holds at most 48,000 samples (two seconds,
 * 96 KB); an incoming buffer is truncated to the remaining capacity.
 * A flush cuts off audio only when its control reaches this port. The broker
 * sends that control after accepting PTT; this processor makes no
 * cross-direction transport ordering assumption.
 */
const MAX_QUEUED_SAMPLES = 48_000;

class PcmPlaybackProcessor extends AudioWorkletProcessor {
  constructor() {
    super();
    this.samples = new Int16Array(MAX_QUEUED_SAMPLES);
    this.readIndex = 0;
    this.writeIndex = 0;
    this.queuedSamples = 0;
    this.port.onmessage = (event) => {
      const data = event.data;
      if (data && data.type === "flush") {
        this.readIndex = 0;
        this.writeIndex = 0;
        this.queuedSamples = 0;
        return;
      }
      if (data instanceof ArrayBuffer && data.byteLength > 0 && data.byteLength % 2 === 0) {
        const incoming = new Int16Array(data);
        const accepted = Math.min(incoming.length, MAX_QUEUED_SAMPLES - this.queuedSamples);
        for (let i = 0; i < accepted; i += 1) {
          this.samples[this.writeIndex] = incoming[i];
          this.writeIndex += 1;
          if (this.writeIndex === MAX_QUEUED_SAMPLES) this.writeIndex = 0;
        }
        this.queuedSamples += accepted;
      }
    };
  }

  process(_inputs, outputs) {
    const channels = outputs[0];
    if (!channels || channels.length === 0) return true;
    const out = channels[0];
    if (!out) return true;
    const written = Math.min(out.length, this.queuedSamples);
    for (let i = 0; i < written; i += 1) {
      out[i] = this.samples[this.readIndex] / 32768;
      this.readIndex += 1;
      if (this.readIndex === MAX_QUEUED_SAMPLES) this.readIndex = 0;
    }
    this.queuedSamples -= written;
    for (let i = written; i < out.length; i += 1) out[i] = 0;
    for (let channel = 1; channel < channels.length; channel += 1) {
      channels[channel].set(out);
    }
    return true;
  }
}

registerProcessor("pcm-playback", PcmPlaybackProcessor);
