/** Build browser microphone constraints from explicit non-content preferences. */
export function buildMicrophoneConstraints(deviceId, processing) {
  return {
    channelCount: 1,
    echoCancellation: processing.echoCancellation,
    noiseSuppression: processing.noiseSuppression,
    autoGainControl: processing.autoGainControl,
    ...(deviceId ? { deviceId: { exact: deviceId } } : {}),
  };
}

/** System default is represented by no persisted device identifier. */
export function persistedDeviceId(requestedDeviceId, resolvedDeviceId) {
  return requestedDeviceId && resolvedDeviceId ? resolvedDeviceId : null;
}

/** A stale saved identifier must fall back to the system default. */
export function availableDeviceId(savedDeviceId, devices) {
  return savedDeviceId && devices.some((device) => device.deviceId === savedDeviceId)
    ? savedDeviceId
    : null;
}

/** Retry the default once only when failure can still be device-specific. */
export function canFallbackFromSavedDevice(errorName, permissionState) {
  if (permissionState === "denied") return false;
  return (
    permissionState === "granted" || ["NotFoundError", "OverconstrainedError"].includes(errorName)
  );
}

/** Label visibility after setup is fallback evidence that capture permission remains granted. */
export function effectiveMicrophonePermission(permissionState, devices) {
  return permissionState === "unknown" && devices.some((device) => device.label)
    ? "granted"
    : permissionState;
}

/** Track the ephemeral group behind the system-default input without using labels. */
export function defaultMicrophoneTransition(previousFingerprint, devices) {
  const defaultDevice = devices.find(
    (device) => device.kind === "audioinput" && device.deviceId === "default",
  );
  const observedFingerprint =
    typeof defaultDevice?.groupId === "string" && defaultDevice.groupId
      ? defaultDevice.groupId
      : null;
  return Object.freeze({
    fingerprint: observedFingerprint ?? previousFingerprint ?? null,
    reconnect:
      previousFingerprint !== null &&
      observedFingerprint !== null &&
      previousFingerprint !== observedFingerprint,
  });
}

/** Sound-disabled mode creates no oscillator frequency. */
export function cueFrequency(kind, enabled) {
  if (!enabled) return null;
  return { start: 520, stop: 390, success: 660, error: 220 }[kind] ?? 440;
}
