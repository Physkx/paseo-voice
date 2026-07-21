import test from "node:test";
import assert from "node:assert/strict";

import {
  availableDeviceId,
  buildMicrophoneConstraints,
  canFallbackFromSavedDevice,
  cueFrequency,
  defaultMicrophoneTransition,
  effectiveMicrophonePermission,
  persistedDeviceId,
} from "../public/microphone-config.js";

test("saved-device fallback never repeats a denied permission request", () => {
  assert.equal(canFallbackFromSavedDevice("NotAllowedError", "denied"), false);
  assert.equal(canFallbackFromSavedDevice("NotAllowedError", "unknown"), false);
  assert.equal(canFallbackFromSavedDevice("OverconstrainedError", "unknown"), true);
  assert.equal(canFallbackFromSavedDevice("NotReadableError", "granted"), true);
});

test("existing labelled devices prove permission when the Permissions API is unavailable", () => {
  assert.equal(effectiveMicrophonePermission("unknown", [{ label: "Desk microphone" }]), "granted");
  assert.equal(effectiveMicrophonePermission("unknown", [{ label: "" }]), "unknown");
  assert.equal(effectiveMicrophonePermission("denied", [{ label: "Desk microphone" }]), "denied");
});

test("default fingerprint reconnects only when the default group changes", () => {
  const baseline = defaultMicrophoneTransition(null, [
    { kind: "audioinput", deviceId: "default", groupId: "group-a", label: "Default" },
  ]);
  assert.deepEqual(baseline, { fingerprint: "group-a", reconnect: false });

  const unrelatedChange = defaultMicrophoneTransition("group-a", [
    { kind: "audioinput", deviceId: "default", groupId: "group-a", label: "Renamed" },
    { kind: "audioinput", deviceId: "added-device", groupId: "group-c" },
  ]);
  assert.deepEqual(unrelatedChange, { fingerprint: "group-a", reconnect: false });

  const changed = defaultMicrophoneTransition("group-a", [
    { kind: "audioinput", deviceId: "default", groupId: "group-b", label: "Default" },
  ]);
  assert.deepEqual(changed, { fingerprint: "group-b", reconnect: true });

  const unavailable = defaultMicrophoneTransition("group-a", [
    { kind: "audioinput", deviceId: "default", groupId: "", label: "Default" },
  ]);
  assert.deepEqual(unavailable, { fingerprint: "group-a", reconnect: false });
});

test("stale devices and system default resolve without pinning hardware", () => {
  const devices = [{ deviceId: "present" }];
  assert.equal(availableDeviceId("missing", devices), null);
  assert.equal(availableDeviceId("present", devices), "present");
  assert.equal(persistedDeviceId(null, "physical-default"), null);
  assert.equal(persistedDeviceId("present", "present"), "present");
});

test("reloadable processing preferences map to exact browser constraints", () => {
  assert.deepEqual(
    buildMicrophoneConstraints("mic-1", {
      echoCancellation: false,
      noiseSuppression: true,
      autoGainControl: false,
    }),
    {
      channelCount: 1,
      echoCancellation: false,
      noiseSuppression: true,
      autoGainControl: false,
      deviceId: { exact: "mic-1" },
    },
  );
});

test("sound can be disabled", () => {
  assert.equal(cueFrequency("success", false), null);
  assert.equal(cueFrequency("success", true), 660);
});
