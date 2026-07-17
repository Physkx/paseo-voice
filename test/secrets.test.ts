import { describe, expect, it } from "vitest";
import { loadConfig, type Config } from "../src/config.js";
import { nullLogger } from "../src/log.js";
import { loadSecrets, parseBwsEnvFile } from "../src/secrets.js";
import type { ExecFn } from "../src/exec.js";

const baseConfig = (overrides: Partial<Config> = {}): Promise<Config> =>
  loadConfig({
    env: {},
    readFile: async () => JSON.stringify(overrides),
  });

describe("parseBwsEnvFile", () => {
  it("parses export with double quotes", () => {
    expect(parseBwsEnvFile('export BWS_ACCESS_TOKEN="abc.123"')).toBe("abc.123");
  });
  it("parses bare assignment", () => {
    expect(parseBwsEnvFile("BWS_ACCESS_TOKEN=tok-xyz\n")).toBe("tok-xyz");
  });
  it("parses single quotes and ignores comments", () => {
    expect(parseBwsEnvFile("# token below\nexport BWS_ACCESS_TOKEN='q1' # note")).toBe("q1");
  });
  it("returns null when absent", () => {
    expect(parseBwsEnvFile("OTHER=1\n")).toBeNull();
  });
});

describe("loadSecrets", () => {
  it("dev mode reads from process env", async () => {
    const config = await baseConfig({ devMode: true });
    const secrets = await loadSecrets(config, {
      execFile: async () => {
        throw new Error("must not exec in dev mode");
      },
      readFile: async () => {
        throw new Error("must not read files in dev mode");
      },
      env: { OPENAI_API_KEY: "sk-dev", PASEO_PASSWORD: "pw-dev" },
      log: nullLogger,
    });
    expect(secrets).toEqual({ openaiApiKey: "sk-dev", paseoPassword: "pw-dev" });
  });

  it("fetches configured secrets via bws with token in child env only", async () => {
    const config = await baseConfig({
      bwsSecretIdOpenai: "id-openai",
      bwsSecretIdPaseo: "id-paseo",
    });
    const calls: Array<{ file: string; args: string[]; env?: Record<string, string> }> = [];
    const execFile: ExecFn = async (file, args, opts) => {
      calls.push({ file, args, env: opts?.env });
      const id = args[2];
      return { stdout: JSON.stringify({ id, value: `value-of-${id}` }), stderr: "" };
    };
    const secrets = await loadSecrets(config, {
      execFile,
      readFile: async () => 'export BWS_ACCESS_TOKEN="tok"',
      env: { PATH: "/usr/bin" },
      log: nullLogger,
    });
    expect(secrets.openaiApiKey).toBe("value-of-id-openai");
    expect(secrets.paseoPassword).toBe("value-of-id-paseo");
    for (const call of calls) {
      expect(call.file).toBe("bws");
      expect(call.args.join(" ")).not.toContain("tok");
      expect(call.env?.["BWS_ACCESS_TOKEN"]).toBe("tok");
    }
  });

  it("resolves nulls when env file missing", async () => {
    const config = await baseConfig({ bwsSecretIdOpenai: "id-openai" });
    const secrets = await loadSecrets(config, {
      execFile: async () => {
        throw new Error("no token, must not exec");
      },
      readFile: async () => {
        throw Object.assign(new Error("ENOENT"), { code: "ENOENT" });
      },
      env: {},
      log: nullLogger,
    });
    expect(secrets).toEqual({ openaiApiKey: null, paseoPassword: null });
  });

  it("degrades to null on bws failure instead of throwing", async () => {
    const config = await baseConfig({ bwsSecretIdOpenai: "id-openai" });
    const secrets = await loadSecrets(config, {
      execFile: async () => {
        throw new Error("bws exploded");
      },
      readFile: async () => "BWS_ACCESS_TOKEN=tok",
      env: {},
      log: nullLogger,
    });
    expect(secrets.openaiApiKey).toBeNull();
  });

  it("unconfigured ids resolve null without exec", async () => {
    const config = await baseConfig({});
    let execCount = 0;
    const secrets = await loadSecrets(config, {
      execFile: async () => {
        execCount += 1;
        return { stdout: "{}", stderr: "" };
      },
      readFile: async () => "BWS_ACCESS_TOKEN=tok",
      env: {},
      log: nullLogger,
    });
    expect(execCount).toBe(0);
    expect(secrets).toEqual({ openaiApiKey: null, paseoPassword: null });
  });
});
