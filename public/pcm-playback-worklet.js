/**
 * Plays queued Int16 PCM at the AudioContext rate (24000 Hz). The main
 * thread posts ArrayBuffers of pcm16 to enqueue and {type:"flush"} to drop
 * everything (barge-in).
 */
class PcmPlaybackProcessor extends AudioWorkletProcessor {
  constructor() {
    super();
    this.queue = [];
    this.offset = 0;
    this.port.onmessage = (event) => {
      const data = event.data;
      if (data && data.type === "flush") {
        this.queue = [];
        this.offset = 0;
        return;
      }
      if (data instanceof ArrayBuffer) {
        this.queue.push(new Int16Array(data));
      }
    };
  }

  process(_inputs, outputs) {
    const out = outputs[0][0];
    if (!out) return true;
    let written = 0;
    while (written < out.length && this.queue.length > 0) {
      const head = this.queue[0];
      const available = head.length - this.offset;
      const needed = out.length - written;
      const take = Math.min(available, needed);
      for (let i = 0; i < take; i += 1) {
        out[written + i] = head[this.offset + i] / 32768;
      }
      written += take;
      this.offset += take;
      if (this.offset >= head.length) {
        this.queue.shift();
        this.offset = 0;
      }
    }
    for (let i = written; i < out.length; i += 1) out[i] = 0;
    return true;
  }
}

registerProcessor("pcm-playback", PcmPlaybackProcessor);
