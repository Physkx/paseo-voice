import type { Config } from "./config.js";
import type { Logger } from "./log.js";

/**
 * Compresses a long agent reply into a short spoken brief using the Spark 9B
 * behind LM Studio (OpenAI-compatible chat completions). The summariser must
 * never block or break the voice loop: any failure degrades to a cleaned tail
 * of the original text with degraded=true so the voice model can say the
 * summariser was offline.
 */
export interface SummariseInput {
  text: string;
  sessionTitle?: string;
}

export interface SummariseResult {
  spokenText: string;
  degraded: boolean;
}

export interface SummariseDeps {
  fetchFn: typeof fetch;
  config: Config;
  log: Logger;
  timeoutMs?: number;
}

const SYSTEM_PROMPT = [
  "You compress coding-agent output into a spoken brief.",
  "Reply with 2 to 3 short sentences of plain spoken language.",
  "No markdown, no code blocks, no bullet lists, no emoji.",
  "Lead with the outcome. Include concrete results (tests passing, files changed, errors).",
  "If the agent is waiting on the user (a question or a permission), end by saying so explicitly.",
].join(" ");

/** Strip markdown noise so a fallback tail reads acceptably out loud. */
export function cleanForSpeech(text: string): string {
  return text
    .replace(/```[\s\S]*?```/g, " code block omitted. ")
    .replace(/^#+\s*/gm, "")
    .replace(/^[|].*$/gm, " ")
    .replace(/[*_`>#]/g, "")
    .replace(/\[([^\]]+)\]\([^)]*\)/g, "$1")
    .replace(/[ \t]+/g, " ")
    .replace(/\n{2,}/g, ". ")
    .replace(/\n/g, " ")
    .replace(/\s{2,}/g, " ")
    .trim();
}

function fallback(input: SummariseInput, config: Config): SummariseResult {
  const cleaned = cleanForSpeech(input.text);
  const tail = cleaned.slice(-config.summariseThresholdChars);
  return { spokenText: tail, degraded: true };
}

export async function summarise(
  input: SummariseInput,
  deps: SummariseDeps,
): Promise<SummariseResult> {
  const { config, log } = deps;
  const url = `${config.sparkBaseUrl.replace(/\/$/, "")}/chat/completions`;
  try {
    const response = await deps.fetchFn(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        model: config.sparkModel,
        temperature: 0.2,
        max_tokens: 220,
        messages: [
          { role: "system", content: SYSTEM_PROMPT },
          {
            role: "user",
            content: `Session: ${input.sessionTitle ?? "unknown"}\n\nAgent output:\n${input.text}`,
          },
        ],
      }),
      signal: AbortSignal.timeout(deps.timeoutMs ?? 10_000),
    });
    if (!response.ok) {
      log.warn("summariser HTTP error, using fallback", { status: response.status });
      return fallback(input, config);
    }
    const data: unknown = await response.json();
    const content =
      data !== null &&
      typeof data === "object" &&
      "choices" in data &&
      Array.isArray((data as { choices: unknown }).choices)
        ? ((data as { choices: Array<{ message?: { content?: unknown } }> }).choices[0]?.message
            ?.content ?? null)
        : null;
    if (typeof content !== "string" || content.trim().length === 0) {
      log.warn("summariser returned empty content, using fallback");
      return fallback(input, config);
    }
    return { spokenText: cleanForSpeech(content), degraded: false };
  } catch (err) {
    log.warn("summariser unreachable, using fallback", {
      error: err instanceof Error ? err.message : String(err),
    });
    return fallback(input, config);
  }
}
