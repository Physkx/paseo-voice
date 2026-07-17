import { execFile as nodeExecFile } from "node:child_process";

export interface ExecResult {
  stdout: string;
  stderr: string;
}

export interface ExecOptions {
  env?: Record<string, string>;
  timeoutMs?: number;
  maxBufferBytes?: number;
}

export class ExecError extends Error {
  constructor(
    message: string,
    readonly file: string,
    readonly exitCode: number | null,
    readonly stdout: string,
    readonly stderr: string,
  ) {
    super(message);
    this.name = "ExecError";
  }
}

/**
 * Promisified execFile. Never a shell, so arguments are never re-parsed and
 * secrets passed via env cannot leak through argv.
 */
export type ExecFn = (file: string, args: string[], opts?: ExecOptions) => Promise<ExecResult>;

export const realExec: ExecFn = (file, args, opts = {}) =>
  new Promise((resolve, reject) => {
    nodeExecFile(
      file,
      args,
      {
        env: opts.env,
        timeout: opts.timeoutMs ?? 15000,
        maxBuffer: opts.maxBufferBytes ?? 8 * 1024 * 1024,
        windowsHide: true,
      },
      (error, stdout, stderr) => {
        if (error) {
          const code = typeof error.code === "number" ? error.code : null;
          reject(new ExecError(`${file} failed: ${error.message}`, file, code, stdout, stderr));
          return;
        }
        resolve({ stdout, stderr });
      },
    );
  });
