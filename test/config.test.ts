import { describe, expect, it } from "vitest";
import { ConfigError, defaultConfigPath, describeConfig, loadConfig } from "../src/config.js";

const missingFile = async () => {
  throw Object.assign(new Error("ENOENT"), { code: "ENOENT" });
};

describe("loadConfig", () => {
  it("returns defaults when no file and no env", async () => {
    const config = await loadConfig({ env: {}, readFile: missingFile });
    expect(config.listenHost).toBe("127.0.0.1");
    expect(config.listenPort).toBe(8790);
    expect(config.openaiModel).toBe("gpt-realtime-2.1");
    expect(config.sparkBaseUrl).toBe("http://127.0.0.1:1234/v1");
    expect(config.sparkModel).toBe("qwen3.5-9b-instruct-nvfp4");
    expect(config.secretProvider).toBe("bitwarden");
  });

  it("file overrides defaults", async () => {
    const config = await loadConfig({
      env: {},
      readFile: async () => JSON.stringify({ listenPort: 9000, openaiVoice: "cedar" }),
    });
    expect(config.listenPort).toBe(9000);
    expect(config.openaiVoice).toBe("cedar");
  });

  it("env overrides file", async () => {
    const config = await loadConfig({
      env: {
        PASEO_VOICE_LISTEN_PORT: "9100",
        PASEO_VOICE_SECRET_PROVIDER: "onepassword",
      },
      readFile: async () => JSON.stringify({ listenPort: 9000, secretProvider: "environment" }),
    });
    expect(config.listenPort).toBe(9100);
    expect(config.secretProvider).toBe("onepassword");
  });

  it("loads 1Password CLI settings from environment overrides", async () => {
    const config = await loadConfig({
      env: {
        PASEO_VOICE_ONEPASSWORD_BIN: "custom-op",
        PASEO_VOICE_ONEPASSWORD_SECRET_REF_OPENAI: "op://vault/openai/key",
        PASEO_VOICE_ONEPASSWORD_SECRET_REF_PASEO: "op://vault/paseo/password",
      },
      readFile: missingFile,
    });
    expect(config.onePasswordBin).toBe("custom-op");
    expect(config.onePasswordSecretRefOpenai).toBe("op://vault/openai/key");
    expect(config.onePasswordSecretRefPaseo).toBe("op://vault/paseo/password");
  });

  it("rejects a 1Password secret reference without the op scheme", async () => {
    await expect(
      loadConfig({
        env: { PASEO_VOICE_ONEPASSWORD_SECRET_REF_OPENAI: "vault/openai/key" },
        readFile: missingFile,
      }),
    ).rejects.toThrow(/op:\/\//);
  });

  it("rejects non-numeric numeric env", async () => {
    await expect(
      loadConfig({ env: { PASEO_VOICE_LISTEN_PORT: "abc" }, readFile: missingFile }),
    ).rejects.toThrow(ConfigError);
  });

  it("rejects invalid boolean env", async () => {
    await expect(
      loadConfig({ env: { PASEO_VOICE_MOCK: "maybe" }, readFile: missingFile }),
    ).rejects.toThrow(/PASEO_VOICE_MOCK/);
  });

  it("rejects malformed JSON file with a clear error", async () => {
    await expect(loadConfig({ env: {}, readFile: async () => "{ not json" })).rejects.toThrow(
      /not valid JSON/,
    );
  });

  it("rejects out-of-range values via schema", async () => {
    await expect(
      loadConfig({ env: {}, readFile: async () => JSON.stringify({ listenPort: 70000 }) }),
    ).rejects.toThrow(ConfigError);
  });

  it("rejects removed devMode configuration with migration guidance", async () => {
    await expect(
      loadConfig({ env: {}, readFile: async () => JSON.stringify({ devMode: true }) }),
    ).rejects.toThrow(/secretProvider.*environment/);
  });

  it("rejects removed PASEO_VOICE_DEV with migration guidance", async () => {
    await expect(
      loadConfig({ env: { PASEO_VOICE_DEV: "1" }, readFile: missingFile }),
    ).rejects.toThrow(/PASEO_VOICE_SECRET_PROVIDER=environment/);
  });

  it("honours PASEO_VOICE_CONFIG for the file path", () => {
    expect(defaultConfigPath({ PASEO_VOICE_CONFIG: "/tmp/x.json" })).toBe("/tmp/x.json");
  });

  it("describeConfig never includes secret ids verbatim", async () => {
    const config = await loadConfig({
      env: {
        PASEO_VOICE_BWS_SECRET_ID_OPENAI: "0b1d0000-aaaa-bbbb-cccc-000000000000",
        PASEO_VOICE_ONEPASSWORD_SECRET_REF_OPENAI: "op://private-vault/item/key",
      },
      readFile: missingFile,
    });
    const described = JSON.stringify(describeConfig(config));
    expect(described).not.toContain("0b1d0000");
    expect(described).not.toContain("private-vault");
    expect(described).toContain('"bwsSecretIdOpenai":"set"');
    expect(described).toContain('"onePasswordSecretRefOpenai":"set"');
  });
});
