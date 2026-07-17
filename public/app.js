/**
 * Paseo Voice browser client. Deliberately dumb: microphone in, speaker out,
 * push-to-talk control frames. No secrets, no OpenAI contact, no build step.
 *
 * Wire protocol to the broker (/ws):
 * - client -> broker: binary = pcm16 24 kHz mono mic audio (while PTT held);
 *   JSON text = {type: "hello" | "ptt_start" | "ptt_end" | "text_turn"}
 * - broker -> client: binary = pcm16 24 kHz assistant audio; JSON text =
 *   state / transcript_delta / transcript_done / user_transcript / tool /
 *   proposal / flush_audio / error / mode
 *
 * ?loopback=1 skips the server and echoes mic audio to the speaker, to test
 * the audio path alone.
 */

const $ = (id) => document.getElementById(id);
const connPill = $("conn-pill");
const modePill = $("mode-pill");
const statePill = $("state-pill");
const pttButton = $("ptt");
const pttLabel = $("ptt-label");
const transcriptBox = $("transcript");
const activityBox = $("activity");
const proposalBanner = $("proposal-banner");
const proposalText = $("proposal-text");

const loopback = new URLSearchParams(location.search).get("loopback") === "1";

let socket = null;
let audioContext = null;
let captureNode = null;
let playbackNode = null;
let micReady = false;
let talking = false;
let assistantEntry = null;

function logActivity(text, cls = "") {
  const line = document.createElement("div");
  line.textContent = `${new Date().toLocaleTimeString()} ${text}`;
  if (cls) line.className = cls;
  activityBox.append(line);
  activityBox.scrollTop = activityBox.scrollHeight;
}

function addTranscript(who, text) {
  const entry = document.createElement("div");
  entry.className = `entry ${who}`;
  const label = document.createElement("div");
  label.className = "who";
  label.textContent = who === "user" ? "you" : "paseo voice";
  const body = document.createElement("div");
  body.textContent = text;
  entry.append(label, body);
  transcriptBox.append(entry);
  transcriptBox.scrollTop = transcriptBox.scrollHeight;
  return body;
}

function setPill(el, text, cls) {
  el.textContent = text;
  el.className = `pill ${cls ?? ""}`;
}

async function initAudio() {
  audioContext = new AudioContext({ sampleRate: 24000 });
  await audioContext.audioWorklet.addModule("pcm-capture-worklet.js");
  await audioContext.audioWorklet.addModule("pcm-playback-worklet.js");

  playbackNode = new AudioWorkletNode(audioContext, "pcm-playback", {
    numberOfInputs: 0,
    outputChannelCount: [1],
  });
  playbackNode.connect(audioContext.destination);

  const stream = await navigator.mediaDevices.getUserMedia({
    audio: { channelCount: 1, echoCancellation: true, noiseSuppression: true },
  });
  const source = audioContext.createMediaStreamSource(stream);
  captureNode = new AudioWorkletNode(audioContext, "pcm-capture", {
    numberOfOutputs: 0,
  });
  source.connect(captureNode);

  captureNode.port.onmessage = (event) => {
    if (!talking) return;
    if (loopback) {
      playbackNode.port.postMessage(event.data);
      return;
    }
    if (socket && socket.readyState === WebSocket.OPEN) {
      socket.send(event.data);
    }
  };

  micReady = true;
  pttButton.disabled = false;
  $("setup").classList.add("hidden");
  logActivity(loopback ? "microphone ready (loopback mode, no server)" : "microphone ready");
}

function flushPlayback() {
  if (playbackNode) playbackNode.port.postMessage({ type: "flush" });
}

function startTalking() {
  if (!micReady || talking) return;
  talking = true;
  pttButton.classList.add("active");
  pttLabel.textContent = "Listening...";
  flushPlayback();
  if (!loopback && socket && socket.readyState === WebSocket.OPEN) {
    socket.send(JSON.stringify({ type: "ptt_start" }));
  }
}

function stopTalking() {
  if (!talking) return;
  talking = false;
  pttButton.classList.remove("active");
  pttLabel.textContent = "Hold to talk";
  if (!loopback && socket && socket.readyState === WebSocket.OPEN) {
    socket.send(JSON.stringify({ type: "ptt_end" }));
  }
}

function handleServerJson(msg) {
  switch (msg.type) {
    case "mode":
      setPill(modePill, `mode: ${msg.mode}`, msg.mode === "real" ? "ok" : "warn");
      if (msg.mode === "mock") $("text-turn-box").classList.remove("hidden");
      return;
    case "state":
      setPill(
        statePill,
        msg.state + (msg.detail ? ` (${msg.detail})` : ""),
        msg.state === "error" ? "error" : "",
      );
      if (msg.state !== "responding") assistantEntry = null;
      return;
    case "transcript_delta":
      if (!assistantEntry) assistantEntry = addTranscript("assistant", "");
      assistantEntry.textContent += msg.text;
      transcriptBox.scrollTop = transcriptBox.scrollHeight;
      return;
    case "transcript_done":
      if (assistantEntry) assistantEntry.textContent = msg.text;
      assistantEntry = null;
      return;
    case "user_transcript":
      addTranscript("user", msg.text);
      return;
    case "tool":
      logActivity(`tool ${msg.phase}: ${msg.name}`);
      return;
    case "proposal":
      if (msg.echo) {
        proposalText.textContent = msg.echo;
        proposalBanner.classList.remove("hidden");
      } else {
        proposalBanner.classList.add("hidden");
      }
      return;
    case "flush_audio":
      flushPlayback();
      return;
    case "error":
      logActivity(`error: ${msg.message}`, "error");
      setPill(statePill, "error", "error");
      return;
    default:
      return;
  }
}

function connect() {
  if (loopback) {
    setPill(connPill, "loopback", "warn");
    setPill(modePill, "mode: local", "warn");
    return;
  }
  const proto = location.protocol === "https:" ? "wss" : "ws";
  socket = new WebSocket(`${proto}://${location.host}/ws`);
  socket.binaryType = "arraybuffer";

  socket.addEventListener("open", () => {
    setPill(connPill, "connected", "ok");
    socket.send(JSON.stringify({ type: "hello" }));
    logActivity("connected to broker");
  });
  socket.addEventListener("close", () => {
    setPill(connPill, "disconnected", "error");
    logActivity("disconnected; retrying in 3 s", "error");
    setTimeout(connect, 3000);
  });
  socket.addEventListener("error", () => {
    setPill(connPill, "socket error", "error");
  });
  socket.addEventListener("message", (event) => {
    if (typeof event.data === "string") {
      try {
        handleServerJson(JSON.parse(event.data));
      } catch {
        logActivity(`bad frame: ${event.data.slice(0, 80)}`, "error");
      }
      return;
    }
    if (playbackNode) playbackNode.port.postMessage(event.data);
  });
}

$("enable-mic").addEventListener("click", () => {
  initAudio().catch((err) => {
    logActivity(`microphone failed: ${err.message}`, "error");
  });
});

pttButton.addEventListener("pointerdown", (event) => {
  event.preventDefault();
  pttButton.setPointerCapture(event.pointerId);
  startTalking();
});
pttButton.addEventListener("pointerup", () => stopTalking());
pttButton.addEventListener("pointercancel", () => stopTalking());

document.addEventListener("keydown", (event) => {
  if (event.code === "Space" && !event.repeat && event.target === document.body) {
    event.preventDefault();
    startTalking();
  }
});
document.addEventListener("keyup", (event) => {
  if (event.code === "Space" && event.target === document.body) {
    event.preventDefault();
    stopTalking();
  }
});

$("text-form").addEventListener("submit", (event) => {
  event.preventDefault();
  const input = $("text-input");
  const text = input.value.trim();
  if (!text || !socket || socket.readyState !== WebSocket.OPEN) return;
  addTranscript("user", text);
  socket.send(JSON.stringify({ type: "text_turn", text }));
  input.value = "";
});

connect();
