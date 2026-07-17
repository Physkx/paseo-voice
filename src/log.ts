const LEVELS = ["debug", "info", "warn", "error"] as const;
export type LogLevel = (typeof LEVELS)[number];

export interface Logger {
  debug(msg: string, fields?: Record<string, unknown>): void;
  info(msg: string, fields?: Record<string, unknown>): void;
  warn(msg: string, fields?: Record<string, unknown>): void;
  error(msg: string, fields?: Record<string, unknown>): void;
  child(bindings: Record<string, unknown>): Logger;
}

/** Placeholder for secret values in any log output. Never log the value itself. */
export function redacted(value: string | null | undefined): string {
  if (value == null || value.length === 0) return "<unset>";
  return `<redacted:${value.length}ch>`;
}

export function createLogger(
  minLevel: LogLevel = "info",
  sink: (line: string) => void = (line) => process.stderr.write(line + "\n"),
  bindings: Record<string, unknown> = {},
): Logger {
  const threshold = LEVELS.indexOf(minLevel);
  const emit = (level: LogLevel, msg: string, fields?: Record<string, unknown>) => {
    if (LEVELS.indexOf(level) < threshold) return;
    sink(
      JSON.stringify({
        time: new Date().toISOString(),
        level,
        msg,
        ...bindings,
        ...fields,
      }),
    );
  };
  return {
    debug: (msg, fields) => emit("debug", msg, fields),
    info: (msg, fields) => emit("info", msg, fields),
    warn: (msg, fields) => emit("warn", msg, fields),
    error: (msg, fields) => emit("error", msg, fields),
    child: (childBindings) => createLogger(minLevel, sink, { ...bindings, ...childBindings }),
  };
}

export const nullLogger: Logger = {
  debug: () => {},
  info: () => {},
  warn: () => {},
  error: () => {},
  child: () => nullLogger,
};
