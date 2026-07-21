import { readFile } from "node:fs/promises";

const indexUrl = new URL("../../public/index.html", import.meta.url);
let nextHarnessId = 0;

const requiredElementIds = [
  "conn-pill",
  "mode-pill",
  "state-pill",
  "setup",
  "enable-mic",
  "ptt",
  "ptt-label",
  "ptt-hint",
  "transcript",
  "activity",
  "proposal-banner",
  "proposal-text",
  "confirm-proposal",
  "cancel-proposal",
  "text-form",
  "text-input",
  "host-select",
  "host-cwd",
  "host-provider",
  "avatar",
  "avatar-state",
  "bound-thread",
  "response-destination",
  "queue-count",
  "agent-count",
  "agent-grid",
  "dashboard-empty",
  "voice-mode-toggle",
  "voice-mode-description",
  "dictation-preview",
  "dictation-preview-text",
  "dictation-review-actions",
  "recording-mode",
  "silence-stop",
  "silence-period",
  "cancel-dictation",
  "dictation-settings",
  "microphone-select",
  "microphone-status",
  "echo-cancellation",
  "noise-suppression",
  "auto-gain",
  "pre-roll",
  "sound-cues",
  "hold-shortcut",
  "toggle-shortcut",
  "cancel-shortcut",
  "shortcut-status",
  "stt-provider",
  "stt-provider-status",
  "cleanup-provider",
  "cleanup-provider-status",
  "insert-dictation",
  "discard-dictation",
];

const requiredTags = new Map([
  ["enable-mic", "BUTTON"],
  ["ptt", "BUTTON"],
  ["confirm-proposal", "BUTTON"],
  ["cancel-proposal", "BUTTON"],
  ["text-form", "FORM"],
  ["text-input", "TEXTAREA"],
  ["host-select", "SELECT"],
  ["voice-mode-toggle", "INPUT"],
  ["recording-mode", "SELECT"],
  ["silence-stop", "INPUT"],
  ["silence-period", "SELECT"],
  ["cancel-dictation", "BUTTON"],
  ["microphone-select", "SELECT"],
  ["insert-dictation", "BUTTON"],
  ["discard-dictation", "BUTTON"],
]);

class FakeEventTarget extends EventTarget {
  constructor() {
    super();
    this.listeners = new Map();
  }

  addEventListener(type, listener, options) {
    super.addEventListener(type, listener, options);
    if (!listener) return;
    if (!this.listeners.has(type)) this.listeners.set(type, new Set());
    this.listeners.get(type).add(listener);
  }

  removeEventListener(type, listener, options) {
    super.removeEventListener(type, listener, options);
    this.listeners.get(type)?.delete(listener);
  }

  removeAllListeners() {
    for (const [type, listeners] of this.listeners) {
      for (const listener of listeners) super.removeEventListener(type, listener);
    }
    this.listeners.clear();
  }

  get listenerCount() {
    return [...this.listeners.values()].reduce((count, listeners) => count + listeners.size, 0);
  }
}

class FakeClassList {
  constructor(element) {
    this.element = element;
  }

  add(...names) {
    for (const name of names) this.element.classes.add(name);
  }

  remove(...names) {
    for (const name of names) this.element.classes.delete(name);
  }

  contains(name) {
    return this.element.classes.has(name);
  }

  toggle(name, force) {
    const enabled = force ?? !this.contains(name);
    if (enabled) this.add(name);
    else this.remove(name);
    return enabled;
  }
}

class FakeElement extends FakeEventTarget {
  constructor(document, tagName, id = "") {
    super();
    this.ownerDocument = document;
    this.tagName = tagName.toUpperCase();
    this.id = id;
    this.children = [];
    this.parentNode = null;
    this.classes = new Set();
    this.classList = new FakeClassList(this);
    this.dataset = {};
    this.attributes = new Map();
    this._textContent = "";
    this._value = "";
    this.checked = false;
    this.disabled = false;
    this.selected = false;
    this.isContentEditable = false;
    this.selectionStart = 0;
    this.selectionEnd = 0;
    this.scrollTop = 0;
    this.scrollHeight = 0;
    this.clientWidth = 320;
    this.clientHeight = 320;
  }

  get className() {
    return [...this.classes].join(" ");
  }

  set className(value) {
    this.classes = new Set(String(value).split(/\s+/).filter(Boolean));
  }

  get textContent() {
    return this._textContent;
  }

  set textContent(value) {
    this._textContent = String(value);
    this.children = [];
  }

  get value() {
    return this._value;
  }

  set value(value) {
    const nextValue = String(value);
    if (this.tagName !== "SELECT") {
      this._value = nextValue;
      return;
    }
    const match = this.options.find((option) => option.value === nextValue) ?? null;
    for (const option of this.options) option.selected = option === match;
    this._value = match?.value ?? "";
  }

  get options() {
    return this.tagName === "SELECT"
      ? this.children.filter((child) => child.tagName === "OPTION")
      : [];
  }

  get selectedIndex() {
    return this.options.findIndex((option) => option.selected);
  }

  setAttribute(name, value) {
    this.attributes.set(name, String(value));
  }

  getAttribute(name) {
    return this.attributes.get(name) ?? null;
  }

  append(...nodes) {
    for (const node of nodes) {
      if (!(node instanceof FakeElement)) continue;
      node.parentNode = this;
      this.children.push(node);
      if (this.tagName === "SELECT" && node.tagName === "OPTION") {
        if (node.selected) {
          for (const option of this.options) option.selected = option === node;
        } else if (!this.options.some((option) => option.selected)) {
          node.selected = true;
        }
        this._value = this.options.find((option) => option.selected)?.value ?? "";
      }
    }
    this.scrollHeight = this.children.length;
  }

  replaceChildren(...nodes) {
    for (const child of this.children) child.parentNode = null;
    this.children = [];
    if (this.tagName === "SELECT") this._value = "";
    this.append(...nodes);
  }

  querySelector(selector) {
    const tagName = selector.toUpperCase();
    for (const child of this.children) {
      if (child.tagName === tagName) return child;
      const nested = child.querySelector(selector);
      if (nested) return nested;
    }
    return null;
  }

  setSelectionRange(start, end) {
    this.selectionStart = start;
    this.selectionEnd = end;
  }

  focus() {
    this.ownerDocument.activeElement = this;
  }

  setPointerCapture() {}

  getContext() {
    return null;
  }

  remove() {
    if (!this.parentNode) return;
    this.parentNode.children = this.parentNode.children.filter((child) => child !== this);
    this.parentNode = null;
  }
}

class FakeDocument extends FakeEventTarget {
  constructor() {
    super();
    this.elements = new Map();
    this.activeElement = null;
    this.visibilityState = "visible";
    this.hidden = false;
  }

  createElement(tagName) {
    return new FakeElement(this, tagName);
  }

  getElementById(id) {
    return this.elements.get(id) ?? null;
  }

  addElement(id, tagName = "div") {
    const element = new FakeElement(this, tagName, id);
    if (this.elements.has(id)) throw new Error(`Duplicate fixture element ID: ${id}`);
    this.elements.set(id, element);
    return element;
  }

  registerElement(element) {
    if (!element.id) return;
    if (this.elements.has(element.id))
      throw new Error(`Duplicate fixture element ID: ${element.id}`);
    this.elements.set(element.id, element);
  }

  dispose() {
    for (const element of this.elements.values()) element.removeAllListeners();
    this.removeAllListeners();
    this.elements.clear();
    this.activeElement = null;
  }
}

class FakeClock {
  constructor() {
    this.now = 1_000;
    this.nextTimerId = 1;
    this.timers = new Map();
  }

  setTimeout(callback, delay = 0, ...args) {
    const id = this.nextTimerId++;
    this.timers.set(id, { at: this.now + Number(delay), callback, args });
    return id;
  }

  clearTimeout(id) {
    this.timers.delete(id);
  }

  clear() {
    this.timers.clear();
  }

  get pendingTimerCount() {
    return this.timers.size;
  }

  tick(milliseconds) {
    const target = this.now + milliseconds;
    while (true) {
      const due = [...this.timers.entries()]
        .filter(([, timer]) => timer.at <= target)
        .sort((left, right) => left[1].at - right[1].at || left[0] - right[0])[0];
      if (!due) break;
      const [id, timer] = due;
      this.timers.delete(id);
      this.now = timer.at;
      timer.callback(...timer.args);
    }
    this.now = target;
  }
}

class FakeStorage {
  constructor(entries = {}) {
    this.values = new Map(Object.entries(entries));
  }

  getItem(key) {
    return this.values.get(key) ?? null;
  }

  setItem(key, value) {
    this.values.set(key, String(value));
  }

  removeItem(key) {
    this.values.delete(key);
  }

  entries() {
    return [...this.values.entries()];
  }
}

function createBrowserEvent(type, init = {}) {
  const event = new Event(type, { bubbles: init.bubbles, cancelable: true });
  for (const [name, value] of Object.entries(init)) {
    if (name === "bubbles") continue;
    Object.defineProperty(event, name, { configurable: true, enumerable: true, value });
  }
  return event;
}

function fixtureAttributes(source) {
  const attributes = new Map();
  const attributePattern = /([^\s"'<>/=]+)(?:\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s"'=<>`]+)))?/g;
  for (const match of source.matchAll(attributePattern)) {
    attributes.set(match[1].toLowerCase(), match[2] ?? match[3] ?? match[4] ?? "");
  }
  return attributes;
}

function applyFixtureAttributes(element, attributes) {
  for (const [name, value] of attributes) element.setAttribute(name, value);
  element.id = attributes.get("id") ?? "";
  if (attributes.has("class")) element.className = attributes.get("class");
  if (attributes.has("value")) element.value = attributes.get("value");
  element.checked = attributes.has("checked");
  element.disabled = attributes.has("disabled");
  element.selected = attributes.has("selected");
}

function parseFixtureHtml(html) {
  const document = new FakeDocument();
  const moduleScripts = [];
  const stack = [];
  const voidTags = new Set([
    "AREA",
    "BASE",
    "BR",
    "COL",
    "EMBED",
    "HR",
    "IMG",
    "INPUT",
    "LINK",
    "META",
    "PARAM",
    "SOURCE",
    "TRACK",
    "WBR",
  ]);
  const tagPattern = /<(\/)?([a-z][\w-]*)([^>]*)>/gi;

  for (const match of html.matchAll(tagPattern)) {
    const closing = match[1] === "/";
    const tagName = match[2].toUpperCase();
    if (closing) {
      const opened = stack.pop();
      if (opened?.tagName !== tagName) {
        throw new Error(`Malformed fixture HTML near closing ${tagName}`);
      }
      continue;
    }

    const element = document.createElement(tagName);
    applyFixtureAttributes(element, fixtureAttributes(match[3]));
    document.registerElement(element);
    stack.at(-1)?.append(element);
    if (tagName === "SCRIPT" && element.getAttribute("type") === "module") {
      moduleScripts.push(element);
    }
    const selfClosing = /\/\s*$/.test(match[3]);
    if (!selfClosing && !voidTags.has(tagName)) stack.push(element);
  }
  if (stack.length > 0) throw new Error(`Malformed fixture HTML: unclosed ${stack.at(-1).tagName}`);

  const missing = requiredElementIds.filter((id) => !document.getElementById(id));
  if (missing.length > 0)
    throw new Error(`Missing required fixture element IDs: ${missing.join(", ")}`);
  for (const [id, tagName] of requiredTags) {
    const element = document.getElementById(id);
    if (element.tagName !== tagName) {
      throw new Error(`Fixture element #${id} must be ${tagName}, received ${element.tagName}`);
    }
  }
  if (moduleScripts.length !== 1) {
    throw new Error(`Expected one module script in index.html, received ${moduleScripts.length}`);
  }
  const moduleSource = moduleScripts[0].getAttribute("src");
  if (!moduleSource) throw new Error("The index module script must have a src attribute");
  const moduleUrl = new URL(moduleSource, indexUrl);
  const publicDirectoryUrl = new URL("./", indexUrl);
  if (moduleUrl.protocol !== "file:" || !moduleUrl.href.startsWith(publicDirectoryUrl.href)) {
    throw new Error("The index module script must resolve inside public/");
  }

  const submitButton = document.getElementById("text-form").querySelector("button");
  if (!submitButton || submitButton.getAttribute("type") !== "submit") {
    throw new Error("The text form must contain its submit button");
  }
  return { document, moduleUrl, submitButton };
}

async function createDocument() {
  return parseFixtureHtml(await readFile(indexUrl, "utf8"));
}

async function settleMicrotasks() {
  for (let index = 0; index < 12; index += 1) await Promise.resolve();
}

/** Installs shared browser globals, so app harness instances and their tests must run serially. */
export async function createAppHarness({
  storage: initialStorage = {},
  getUserMediaErrors: initialGetUserMediaErrors = [],
  deferredPermissionQueries = 0,
} = {}) {
  const { document, moduleUrl, submitButton } = await createDocument();
  const clock = new FakeClock();
  const storage = new FakeStorage(initialStorage);
  const sockets = [];
  const mediaDevices = new FakeEventTarget();
  const mediaTracks = [];
  const audioContexts = [];
  const audioNodes = [];
  const audioWorkletModules = [];
  const audioWorkletNodes = [];
  const permissionStatuses = [];
  const getUserMediaCalls = [];
  const getUserMediaErrors = [...initialGetUserMediaErrors];
  const pendingPermissionQueries = [];
  let permissionQueryCount = 0;
  const window = new FakeEventTarget();
  window.devicePixelRatio = 1;

  class FakeAudioTrack extends FakeEventTarget {
    constructor() {
      super();
      this.readyState = "live";
      this.stopCount = 0;
      mediaTracks.push(this);
    }

    getSettings() {
      return { deviceId: "fake-microphone" };
    }

    stop() {
      if (this.readyState === "ended") return;
      this.stopCount += 1;
      this.readyState = "ended";
    }
  }

  class FakeMediaStream {
    constructor() {
      this.track = new FakeAudioTrack();
    }

    getAudioTracks() {
      return [this.track];
    }

    getTracks() {
      return [this.track];
    }
  }

  mediaDevices.getUserMedia = async (constraints) => {
    getUserMediaCalls.push(constraints);
    const errorName = getUserMediaErrors.shift();
    if (errorName) {
      const error = new Error(`Fake getUserMedia failure: ${errorName}`);
      error.name = errorName;
      throw error;
    }
    return new FakeMediaStream();
  };
  mediaDevices.enumerateDevices = async () => [
    {
      kind: "audioinput",
      deviceId: "default",
      groupId: "fake-default-group",
      label: "System default",
    },
    {
      kind: "audioinput",
      deviceId: "fake-microphone",
      groupId: "fake-default-group",
      label: "Fake microphone",
    },
  ];

  class FakeAudioNode {
    constructor(kind = "node") {
      this.kind = kind;
      this.connections = [];
      this.disconnectCount = 0;
      audioNodes.push(this);
    }

    connect(node) {
      this.connections.push(node);
      return node;
    }

    disconnect() {
      this.connections = [];
      this.disconnectCount += 1;
    }

    get disconnected() {
      return this.disconnectCount > 0;
    }
  }

  class FakeMessagePort {
    constructor() {
      this.onmessage = null;
      this.messages = [];
    }

    postMessage(message, transfer = []) {
      this.messages.push({ message: structuredClone(message, { transfer }), transfer });
    }
  }

  class FakeAudioWorkletNode extends FakeAudioNode {
    constructor(context, processorName) {
      super("worklet");
      this.context = context;
      this.processorName = processorName;
      this.port = new FakeMessagePort();
      audioWorkletNodes.push(this);
    }
  }

  class FakeAudioContext {
    constructor({ sampleRate }) {
      this.sampleRate = sampleRate;
      this.state = "running";
      this.closeCount = 0;
      this.destination = new FakeAudioNode("destination");
      this.audioWorklet = {
        addModule: async (moduleUrl) => {
          audioWorkletModules.push(moduleUrl);
        },
      };
      audioContexts.push(this);
    }

    get currentTime() {
      return clock.now / 1_000;
    }

    createMediaStreamSource() {
      return new FakeAudioNode("media-source");
    }

    createAnalyser() {
      const analyser = new FakeAudioNode("analyser");
      analyser.fftSize = 512;
      analyser.getByteTimeDomainData = (samples) => samples.fill(128);
      return analyser;
    }

    createOscillator() {
      const oscillator = new FakeAudioNode("oscillator");
      oscillator.frequency = { value: 0 };
      oscillator.start = () => {};
      oscillator.stop = () => {};
      return oscillator;
    }

    createGain() {
      const gain = new FakeAudioNode("gain");
      gain.gain = {
        setValueAtTime: () => {},
        exponentialRampToValueAtTime: () => {},
      };
      return gain;
    }

    async close() {
      if (this.state === "closed") return;
      this.closeCount += 1;
      this.state = "closed";
    }
  }

  class FakeWebSocket extends FakeEventTarget {
    static CONNECTING = 0;
    static OPEN = 1;
    static CLOSING = 2;
    static CLOSED = 3;

    constructor(url) {
      super();
      this.url = url;
      this.readyState = FakeWebSocket.CONNECTING;
      this.binaryType = "blob";
      this.sent = [];
      this.sendError = null;
      this.closeTimer = null;
      this.closeCount = 0;
      sockets.push(this);
    }

    open() {
      this.readyState = FakeWebSocket.OPEN;
      this.dispatchEvent(createBrowserEvent("open"));
    }

    send(payload) {
      if (this.sendError) {
        const error = this.sendError;
        this.sendError = null;
        throw error;
      }
      if (this.readyState !== FakeWebSocket.OPEN) throw new Error("WebSocket is not open");
      this.sent.push(payload);
    }

    close() {
      if ([FakeWebSocket.CLOSING, FakeWebSocket.CLOSED].includes(this.readyState)) return;
      this.closeCount += 1;
      this.readyState = FakeWebSocket.CLOSING;
      this.closeTimer = clock.setTimeout(() => {
        this.closeTimer = null;
        this.readyState = FakeWebSocket.CLOSED;
        this.dispatchEvent(createBrowserEvent("close"));
      }, 0);
    }

    disconnect() {
      this.close();
    }

    receive(frame) {
      const data =
        typeof frame === "string" || frame instanceof ArrayBuffer ? frame : JSON.stringify(frame);
      this.dispatchEvent(createBrowserEvent("message", { data }));
    }

    sentJson() {
      return this.sent.filter((payload) => typeof payload === "string").map(JSON.parse);
    }

    failNextSend(error = new Error("synchronous send failure")) {
      this.sendError = error;
    }

    dispose() {
      if (this.closeTimer !== null) clock.clearTimeout(this.closeTimer);
      this.closeTimer = null;
      this.readyState = FakeWebSocket.CLOSED;
      this.removeAllListeners();
    }
  }

  class UnavailableImage {
    async decode() {
      throw new Error("Images are unavailable in the fake browser");
    }
  }

  const globals = {
    document,
    window,
    navigator: {
      mediaDevices,
      permissions: {
        query: async () => {
          const status = new FakeEventTarget();
          status.state = "granted";
          permissionStatuses.push(status);
          permissionQueryCount += 1;
          if (permissionQueryCount <= deferredPermissionQueries) {
            return new Promise((resolve) => pendingPermissionQueries.push({ resolve, status }));
          }
          return status;
        },
      },
    },
    location: { protocol: "http:", host: "voice.test", search: "" },
    localStorage: storage,
    WebSocket: FakeWebSocket,
    AudioContext: FakeAudioContext,
    AudioWorkletNode: FakeAudioWorkletNode,
    Image: UnavailableImage,
    matchMedia: () => ({ matches: false }),
    performance: { now: () => clock.now },
    setTimeout: clock.setTimeout.bind(clock),
    clearTimeout: clock.clearTimeout.bind(clock),
  };
  const previousGlobals = new Map();
  for (const [name, value] of Object.entries(globals)) {
    previousGlobals.set(name, Object.getOwnPropertyDescriptor(globalThis, name));
    Object.defineProperty(globalThis, name, {
      configurable: true,
      enumerable: true,
      writable: true,
      value,
    });
  }

  let restored = false;
  const restore = async () => {
    if (restored) return;
    restored = true;
    for (const socket of sockets) socket.dispose();
    clock.clear();
    for (const node of audioNodes) {
      node.disconnect();
      if (node.port) node.port.onmessage = null;
    }
    for (const track of mediaTracks) {
      track.stop();
      track.removeAllListeners();
    }
    await Promise.all(audioContexts.map((context) => context.close()));
    for (const status of permissionStatuses) status.removeAllListeners();
    mediaDevices.removeAllListeners();
    window.removeAllListeners();
    document.dispose();
    for (const [name, descriptor] of previousGlobals) {
      if (descriptor) Object.defineProperty(globalThis, name, descriptor);
      else delete globalThis[name];
    }
  };

  try {
    const harnessUrl = new URL(moduleUrl);
    harnessUrl.searchParams.set("harness", String(nextHarnessId));
    nextHarnessId += 1;
    await import(harnessUrl.href);
    await settleMicrotasks();
  } catch (error) {
    await restore();
    throw error;
  }

  return {
    audioNodes,
    audioWorkletModules,
    audioWorkletNodes,
    audioContexts,
    clock,
    document,
    entryUrl: moduleUrl,
    getUserMediaCalls,
    mediaTracks,
    mediaDevices,
    permissionStatuses,
    storage,
    sockets,
    submitButton,
    window,
    get socket() {
      return sockets.at(-1);
    },
    element(id) {
      return document.getElementById(id);
    },
    dispatch(target, type, init = {}) {
      const element = typeof target === "string" ? document.getElementById(target) : target;
      const event = createBrowserEvent(type, init);
      element.dispatchEvent(event);
      return event;
    },
    async enableMicrophone() {
      document.getElementById("enable-mic").dispatchEvent(createBrowserEvent("click"));
      await settleMicrotasks();
      await settleMicrotasks();
    },
    async pagehide({ persisted = false } = {}) {
      window.dispatchEvent(createBrowserEvent("pagehide", { persisted }));
      await settleMicrotasks();
      await settleMicrotasks();
    },
    async pageshow({ persisted = false } = {}) {
      window.dispatchEvent(createBrowserEvent("pageshow", { persisted }));
      await settleMicrotasks();
      await settleMicrotasks();
    },
    get pendingPermissionQueryCount() {
      return pendingPermissionQueries.length;
    },
    resolvePermissionQuery(state = "granted") {
      const pending = pendingPermissionQueries.shift();
      if (!pending) throw new Error("No deferred permission query is pending");
      pending.status.state = state;
      pending.resolve(pending.status);
    },
    settle: settleMicrotasks,
    restore,
  };
}
