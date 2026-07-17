import { readFile } from "node:fs/promises";
import { describe, expect, it } from "vitest";
import {
  CONTROL_PLANE_PROTOCOL_VERSION,
  ControlPlaneProtocolError,
  createControlPlaneClient,
} from "../src/control-plane-client.js";

function framed(value: unknown): Uint8Array {
  const payload = new TextEncoder().encode(JSON.stringify(value));
  const result = new Uint8Array(payload.length + 4);
  new DataView(result.buffer).setUint32(0, payload.length);
  result.set(payload, 4);
  return result;
}

describe("control-plane client", () => {
  it("uses the shared protocol fixture and a bounded length-delimited request", async () => {
    const fixtures = JSON.parse(
      await readFile(new URL("../docs/RUST_PROTOCOL_FIXTURES.json", import.meta.url), "utf8"),
    ) as {
      valid: Array<{ request: Record<string, unknown> }>;
    };
    const fixture = fixtures.valid[0]!.request;
    const client = createControlPlaneClient({
      nextRequestId: () => String(fixture["request_id"]),
      transport: async (bytes, timeoutMs) => {
        expect(timeoutMs).toBe(5_000);
        const length = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getUint32(0);
        expect(length).toBe(bytes.byteLength - 4);
        expect(JSON.parse(new TextDecoder().decode(bytes.subarray(4)))).toEqual(fixture);
        return framed({
          version: CONTROL_PLANE_PROTOCOL_VERSION,
          request_id: fixture["request_id"],
          result: { status: "healthy" },
        });
      },
    });

    await expect(client.request({ op: "health" })).resolves.toMatchObject({
      result: { status: "healthy" },
    });
  });

  it("propagates transport timeout or child exit without retrying", async () => {
    let calls = 0;
    const client = createControlPlaneClient({
      nextRequestId: () => "request",
      transport: async () => {
        calls += 1;
        throw new Error("child exited");
      },
    });
    await expect(client.request({ op: "health" })).rejects.toThrow("child exited");
    expect(calls).toBe(1);
  });

  it("rejects truncated, trailing, mismatched, and version-mismatched responses", async () => {
    const invalid = [
      new Uint8Array(),
      new Uint8Array([0, 0, 0, 10, 123]),
      new Uint8Array([0, 0, 0, 0, 0]),
      framed({ version: 2, request_id: "request" }),
      framed({ version: 1, request_id: "different" }),
    ];
    for (const response of invalid) {
      const client = createControlPlaneClient({
        nextRequestId: () => "request",
        transport: async () => response,
      });
      await expect(client.request({ op: "health" })).rejects.toBeInstanceOf(
        ControlPlaneProtocolError,
      );
    }
  });
});
