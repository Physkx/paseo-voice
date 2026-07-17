import { describe, expect, it } from "vitest";
import { loadConfig } from "../src/config.js";
import { nullLogger } from "../src/log.js";
import { cleanForSpeech, summarise } from "../src/summarise.js";

const config = await loadConfig({
  env: {},
  readFile: async () => {
    throw new Error("no file");
  },
});

const longText = "The tests now pass. ".repeat(100);

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

describe("cleanForSpeech", () => {
  it("strips markdown constructs", () => {
    const input =
      "## Result\n\nAll **four** branches merged.\n\n```bash\ngit push\n```\n\n| a | b |\n[link](https://x)";
    const out = cleanForSpeech(input);
    expect(out).not.toContain("#");
    expect(out).not.toContain("**");
    expect(out).not.toContain("```");
    expect(out).not.toContain("|");
    expect(out).toContain("code block omitted");
    expect(out).toContain("link");
  });
});

describe("summarise", () => {
  it("returns model summary on success", async () => {
    let requestedUrl = "";
    let requestBody: Record<string, unknown> = {};
    const result = await summarise(
      { text: longText, sessionTitle: "auth" },
      {
        config,
        log: nullLogger,
        fetchFn: async (url, init) => {
          requestedUrl = String(url);
          requestBody = JSON.parse(String(init?.body)) as Record<string, unknown>;
          return jsonResponse({
            choices: [
              { message: { content: "Auth tests all pass now. Nothing is waiting on you." } },
            ],
          });
        },
      },
    );
    expect(result.degraded).toBe(false);
    expect(result.spokenText).toContain("Auth tests all pass");
    expect(requestedUrl).toBe("http://127.0.0.1:1234/v1/chat/completions");
    expect(requestBody["model"]).toBe("qwen3.5-9b-instruct-nvfp4");
  });

  it("falls back on HTTP error", async () => {
    const result = await summarise(
      { text: longText },
      { config, log: nullLogger, fetchFn: async () => jsonResponse({}, 500) },
    );
    expect(result.degraded).toBe(true);
    expect(result.spokenText.length).toBeGreaterThan(0);
    expect(result.spokenText.length).toBeLessThanOrEqual(config.summariseThresholdChars);
  });

  it("falls back on network failure", async () => {
    const result = await summarise(
      { text: longText },
      {
        config,
        log: nullLogger,
        fetchFn: async () => {
          throw new Error("ECONNREFUSED");
        },
      },
    );
    expect(result.degraded).toBe(true);
  });

  it("falls back on empty content", async () => {
    const result = await summarise(
      { text: longText },
      {
        config,
        log: nullLogger,
        fetchFn: async () => jsonResponse({ choices: [{ message: { content: "" } }] }),
      },
    );
    expect(result.degraded).toBe(true);
  });

  it("falls back on timeout", async () => {
    const result = await summarise(
      { text: longText },
      {
        config,
        log: nullLogger,
        timeoutMs: 20,
        fetchFn: (_url, init) =>
          new Promise((_resolve, reject) => {
            init?.signal?.addEventListener("abort", () => reject(new Error("aborted")));
          }),
      },
    );
    expect(result.degraded).toBe(true);
  });
});
