// 48,000 PCM16 samples, matching the fixed worklet queue.
const MAX_PLAYBACK_FRAME_BYTES = 96_000;

/** Transfer one bounded PCM16 frame to the playback worklet. */
export function enqueuePlaybackFrame(port, frame) {
  if (
    !(frame instanceof ArrayBuffer) ||
    frame.byteLength === 0 ||
    frame.byteLength % 2 !== 0 ||
    frame.byteLength > MAX_PLAYBACK_FRAME_BYTES ||
    typeof port?.postMessage !== "function"
  ) {
    return false;
  }
  try {
    port.postMessage(frame, [frame]);
    return true;
  } catch {
    return false;
  }
}
