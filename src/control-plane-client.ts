export const CONTROL_PLANE_PROTOCOL_VERSION = 1;
export const CONTROL_PLANE_MAX_FRAME_BYTES = 131_072;

export type ControlPlaneTransport = (frame: Uint8Array, timeoutMs: number) => Promise<Uint8Array>;

export interface ControlPlaneClient {
  request(operation: Record<string, unknown>): Promise<Record<string, unknown>>;
}

export class ControlPlaneProtocolError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ControlPlaneProtocolError";
  }
}

function frame(payload: Uint8Array): Uint8Array {
  if (payload.byteLength > CONTROL_PLANE_MAX_FRAME_BYTES) {
    throw new ControlPlaneProtocolError("control-plane request exceeds frame limit");
  }
  const framed = new Uint8Array(payload.byteLength + 4);
  new DataView(framed.buffer).setUint32(0, payload.byteLength);
  framed.set(payload, 4);
  return framed;
}

function decode(frameBytes: Uint8Array): Record<string, unknown> {
  if (frameBytes.byteLength < 4) {
    throw new ControlPlaneProtocolError("control-plane response is missing its length");
  }
  const length = new DataView(
    frameBytes.buffer,
    frameBytes.byteOffset,
    frameBytes.byteLength,
  ).getUint32(0);
  if (length > CONTROL_PLANE_MAX_FRAME_BYTES) {
    throw new ControlPlaneProtocolError("control-plane response exceeds frame limit");
  }
  if (frameBytes.byteLength !== length + 4) {
    throw new ControlPlaneProtocolError("control-plane response length does not match");
  }
  let value: unknown;
  try {
    value = JSON.parse(new TextDecoder().decode(frameBytes.subarray(4)));
  } catch {
    throw new ControlPlaneProtocolError("control-plane response is not JSON");
  }
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new ControlPlaneProtocolError("control-plane response is not an object");
  }
  const response = value as Record<string, unknown>;
  if (
    response["version"] !== CONTROL_PLANE_PROTOCOL_VERSION ||
    typeof response["request_id"] !== "string"
  ) {
    throw new ControlPlaneProtocolError("control-plane response envelope is invalid");
  }
  return response;
}

export function createControlPlaneClient(options: {
  transport: ControlPlaneTransport;
  timeoutMs?: number;
  nextRequestId: () => string;
}): ControlPlaneClient {
  const timeoutMs = options.timeoutMs ?? 5_000;
  return {
    async request(operation) {
      const requestId = options.nextRequestId();
      const payload = new TextEncoder().encode(
        JSON.stringify({
          version: CONTROL_PLANE_PROTOCOL_VERSION,
          request_id: requestId,
          ...operation,
        }),
      );
      const response = decode(await options.transport(frame(payload), timeoutMs));
      if (response["request_id"] !== requestId) {
        throw new ControlPlaneProtocolError("control-plane response request ID does not match");
      }
      return response;
    },
  };
}
