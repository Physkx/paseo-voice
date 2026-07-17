import WebSocket from "ws";
import type { RealtimeSocket, SocketFactory } from "./realtime.js";

/** Production socket factory for the OpenAI realtime WebSocket. */
export const wsSocketFactory: SocketFactory = (url, headers): RealtimeSocket => {
  const socket = new WebSocket(url, { headers });
  return {
    send: (data) => socket.send(data),
    close: () => socket.close(),
    onOpen: (cb) => socket.on("open", cb),
    onMessage: (cb) => socket.on("message", (data) => cb(data.toString())),
    onClose: (cb) => socket.on("close", (code, reason) => cb(code, reason.toString())),
    onError: (cb) =>
      socket.on("error", (err) => cb(err instanceof Error ? err : new Error(String(err)))),
  };
};
