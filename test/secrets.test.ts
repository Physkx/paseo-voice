import { describe, expect, it } from "vitest";
import { loadConfig, type Config } from "../src/config.js";
import { createLogger, nullLogger } from "../src/log.js";
import { loadSecrets, parseBwsEnvFile } from "../src/secrets.js";
import { ExecError, type ExecFn } from "../src/exec.js";

const baseConfig = (overrides: Partial<Config> = {}): Promise<Config> =>
  loadConfig({
    env: {},
    readFile: async () => JSON.stringify(overrides),
  });

const missingRead = async () => {
  throw new Error("must not read files for this provider");
};

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
  it("environment provider reads from process env", async () => {
    const config = await baseConfig({ secretProvider: "environment" });
    const secrets = await loadSecrets(config, {
      execFile: async () => {
        throw new Error("must not exec for environment provider");
      },
      readFile: async () => {
        throw new Error("must not read files for environment provider");
      },
      env: { OPENAI_API_KEY: "sk-dev", PASEO_PASSWORD: "pw-dev" },
      log: nullLogger,
    });
    expect(secrets).toEqual({ openaiApiKey: "sk-dev", paseoPassword: "pw-dev" });
  });

  it("environment provider treats empty values as missing", async () => {
    const config = await baseConfig({ secretProvider: "environment" });
    const secrets = await loadSecrets(config, {
      execFile: async () => {
        throw new Error("must not exec for environment provider");
      },
      readFile: async () => {
        throw new Error("must not read files for environment provider");
      },
      env: { OPENAI_API_KEY: "", PASEO_PASSWORD: "" },
      log: nullLogger,
    });
    expect(secrets).toEqual({ openaiApiKey: null, paseoPassword: null });
  });

  it("onepassword provider resolves configured references with exact JSON strings", async () => {
    const config = await baseConfig({
      secretProvider: "onepassword",
      onePasswordBin: "custom-op",
      onePasswordSecretRefOpenai: "op://vault/openai/key",
      onePasswordSecretRefPaseo: "op://vault/paseo/password",
    });
    const env = {
      PATH: "example-path",
      HOME: "~",
      OP_SERVICE_ACCOUNT_TOKEN: "dummy-service-account-token",
      WSL_INTEROP: "example-interop",
    };
    const calls: Array<{
      file: string;
      args: string[];
      env?: Record<string, string>;
      timeoutMs?: number;
    }> = [];
    const execFile: ExecFn = async (file, args, opts) => {
      calls.push({ file, args, env: opts?.env, timeoutMs: opts?.timeoutMs });
      const reference = args[3];
      return {
        stdout: JSON.stringify(
          reference === "op://vault/openai/key" ? "  sk-value\n" : "  paseo-value\t",
        ),
        stderr: "",
      };
    };

    const secrets = await loadSecrets(config, {
      execFile,
      readFile: missingRead,
      env,
      log: nullLogger,
    });

    expect(secrets).toEqual({
      openaiApiKey: "  sk-value\n",
      paseoPassword: "  paseo-value\t",
    });
    expect(calls).toEqual([
      {
        file: "custom-op",
        args: ["read", "--format", "json", "op://vault/openai/key"],
        env,
        timeoutMs: 20_000,
      },
      {
        file: "custom-op",
        args: ["read", "--format", "json", "op://vault/paseo/password"],
        env,
        timeoutMs: 20_000,
      },
    ]);
  });

  it("onepassword provider degrades each secret independently with sanitized logs", async () => {
    const openaiRef = "op://private-vault/openai/key";
    const paseoRef = "op://private-vault/paseo/password";
    const config = await baseConfig({
      secretProvider: "onepassword",
      onePasswordSecretRefOpenai: openaiRef,
      onePasswordSecretRefPaseo: paseoRef,
    });
    const logs: string[] = [];
    const log = createLogger("debug", (line) => logs.push(line));
    const calls: string[] = [];
    const execFile: ExecFn = async (_file, args) => {
      const reference = args[3] ?? "";
      calls.push(reference);
      if (reference === openaiRef) {
        throw new ExecError(
          `op failed for ${openaiRef}`,
          "op",
          1,
          "",
          `vault metadata for ${openaiRef}`,
        );
      }
      return { stdout: JSON.stringify("paseo-value"), stderr: "" };
    };

    const secrets = await loadSecrets(config, {
      execFile,
      readFile: missingRead,
      env: {},
      log,
    });

    expect(secrets).toEqual({ openaiApiKey: null, paseoPassword: "paseo-value" });
    expect(calls).toEqual([openaiRef, paseoRef]);
    expect(logs.join("\n")).toContain('"category":"nonzero_exit"');
    expect(logs.join("\n")).toContain('"role":"openaiApiKey"');
    expect(logs.join("\n")).not.toContain("private-vault");
    expect(logs.join("\n")).not.toContain("vault metadata");
  });

  it("onepassword provider reports subprocess timeouts without logging the reference", async () => {
    const reference = "op://private-vault/openai/key";
    const config = await baseConfig({
      secretProvider: "onepassword",
      onePasswordSecretRefOpenai: reference,
    });
    const logs: string[] = [];
    await loadSecrets(config, {
      execFile: async () => {
        throw new ExecError("op failed", "op", null, "", "", true);
      },
      readFile: missingRead,
      env: {},
      log: createLogger("debug", (line) => logs.push(line)),
    });

    expect(logs.join("\n")).toContain('"category":"timeout"');
    expect(logs.join("\n")).not.toContain("private-vault");
  });

  it("onepassword provider rejects non-string JSON without logging output", async () => {
    const config = await baseConfig({
      secretProvider: "onepassword",
      onePasswordSecretRefOpenai: "op://vault/openai/key",
    });
    const logs: string[] = [];
    const secrets = await loadSecrets(config, {
      execFile: async () => ({
        stdout: '{"unexpected":"sensitive-output"}',
        stderr: "",
      }),
      readFile: missingRead,
      env: {},
      log: createLogger("debug", (line) => logs.push(line)),
    });

    expect(secrets.openaiApiKey).toBeNull();
    expect(logs.join("\n")).toContain('"category":"invalid_output"');
    expect(logs.join("\n")).not.toContain("sensitive-output");
  });

  it("onepassword provider finishes the first read before starting the second", async () => {
    const openaiRef = "op://vault/openai/key";
    const paseoRef = "op://vault/paseo/password";
    const config = await baseConfig({
      secretProvider: "onepassword",
      onePasswordSecretRefOpenai: openaiRef,
      onePasswordSecretRefPaseo: paseoRef,
    });
    const calls: string[] = [];
    let finishFirst: ((value: { stdout: string; stderr: string }) => void) | undefined;
    const execFile: ExecFn = async (_file, args) => {
      const reference = args[3] ?? "";
      calls.push(reference);
      if (reference === openaiRef) {
        return new Promise((resolve) => {
          finishFirst = resolve;
        });
      }
      return { stdout: JSON.stringify("paseo-value"), stderr: "" };
    };

    const loading = loadSecrets(config, {
      execFile,
      readFile: missingRead,
      env: {},
      log: nullLogger,
    });
    await Promise.resolve();
    expect(calls).toEqual([openaiRef]);

    if (!finishFirst) throw new Error("first read did not start");
    finishFirst({ stdout: JSON.stringify("openai-value"), stderr: "" });
    await expect(loading).resolves.toEqual({
      openaiApiKey: "openai-value",
      paseoPassword: "paseo-value",
    });
    expect(calls).toEqual([openaiRef, paseoRef]);
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
