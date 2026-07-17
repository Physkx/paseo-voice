import { readFile } from "node:fs/promises";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { ExecError, type ExecFn, type ExecOptions } from "../src/exec.js";
import { createPaseoClient } from "../src/paseo.js";

const fixturesDir = join(import.meta.dirname, "fixtures");
const fixture = (name: string) => readFile(join(fixturesDir, name), "utf8");

interface RecordedCall {
  file: string;
  args: string[];
  opts?: ExecOptions;
}

function fakeExec(stdout: string | ((args: string[]) => string)): {
  exec: ExecFn;
  calls: RecordedCall[];
} {
  const calls: RecordedCall[] = [];
  const exec: ExecFn = async (file, args, opts) => {
    calls.push({ file, args, opts });
    return { stdout: typeof stdout === "function" ? stdout(args) : stdout, stderr: "" };
  };
  return { exec, calls };
}

const PASSWORD = "test-password-not-in-argv";

describe("createPaseoClient", () => {
  it("listSessions parses live ls shape and maps fields", async () => {
    const { exec, calls } = fakeExec(await fixture("ls.json"));
    const client = createPaseoClient({
      exec,
      password: PASSWORD,
      env: { PATH: "/bin", HOME: "/h" },
    });
    const sessions = await client.listSessions();
    expect(sessions).toHaveLength(2);
    expect(sessions[0]).toMatchObject({
      id: "11111111-2222-3333-4444-555555555555",
      name: "Fix the failing auth tests",
      provider: "claude/claude-fable-5",
      status: "running",
    });
    expect(sessions[1]?.thinking).toBeNull();
    const call = calls[0]!;
    expect(call.args).toEqual(["ls", "-g", "--json"]);
    expect(call.args.join(" ")).not.toContain(PASSWORD);
    expect(call.opts?.env?.["PASEO_PASSWORD"]).toBe(PASSWORD);
  });

  it("listSessions tolerates rows with missing optional fields", async () => {
    const { exec } = fakeExec(JSON.stringify([{ id: "x" }]));
    const client = createPaseoClient({ exec, password: PASSWORD });
    const sessions = await client.listSessions();
    expect(sessions[0]).toMatchObject({ id: "x", name: "(untitled)", status: "unknown" });
  });

  it("listSessions rejects non-array JSON", async () => {
    const { exec } = fakeExec("{}");
    const client = createPaseoClient({ exec, password: PASSWORD });
    await expect(client.listSessions()).rejects.toMatchObject({ code: "CLI_BAD_JSON" });
  });

  it("readLogText uses tail and text filter, no json flag", async () => {
    const logsText = await fixture("logs-text.txt");
    const { exec, calls } = fakeExec(logsText);
    const client = createPaseoClient({ exec, password: PASSWORD });
    const text = await client.readLogText("abc", 5);
    expect(text).toContain("All done. Here's the summary.");
    expect(calls[0]!.args).toEqual(["logs", "abc", "--tail", "5", "--filter", "text"]);
    expect(calls[0]!.args).not.toContain("--json");
  });

  it("inspect returns loose record", async () => {
    const { exec } = fakeExec(
      JSON.stringify({ Id: "x", Status: "running", PendingPermissions: [] }),
    );
    const client = createPaseoClient({ exec, password: PASSWORD });
    const info = await client.inspect("x");
    expect(info["Status"]).toBe("running");
  });

  it("listPendingPermissions handles empty and unknown row shapes", async () => {
    const empty = createPaseoClient({ exec: fakeExec("[]").exec, password: PASSWORD });
    expect(await empty.listPendingPermissions()).toEqual([]);

    const rows = [{ agentName: "auth-fix", tool: "Bash", extra: 1 }, { weird: true }];
    const client = createPaseoClient({
      exec: fakeExec(JSON.stringify(rows)).exec,
      password: PASSWORD,
    });
    const permissions = await client.listPendingPermissions();
    expect(permissions[0]!.description).toBe("auth-fix, Bash");
    expect(permissions[1]!.description).toContain("weird");
  });

  it("maps structured CLI error JSON to PaseoCliError with code", async () => {
    const errorJson = await fixture("cli-error.json");
    const exec: ExecFn = async () => {
      throw new ExecError("paseo failed: exit 1", "paseo", 1, errorJson, "");
    };
    const client = createPaseoClient({ exec, password: PASSWORD });
    await expect(client.listSessions()).rejects.toMatchObject({
      name: "PaseoCliError",
      code: "DAEMON_NOT_RUNNING",
    });
  });

  it("maps unstructured failure to CLI_FAILED with stderr snippet", async () => {
    const exec: ExecFn = async () => {
      throw new ExecError("paseo failed", "paseo", 7, "", "something broke badly");
    };
    const client = createPaseoClient({ exec, password: PASSWORD });
    await expect(client.listSessions()).rejects.toMatchObject({ code: "CLI_FAILED" });
    await expect(client.listSessions()).rejects.toThrow(/something broke badly/);
  });

  it("sendMessage uses --prompt and --no-wait", async () => {
    const { exec, calls } = fakeExec('{"ok":true}');
    const client = createPaseoClient({ exec, password: PASSWORD });
    await client.sendMessage("abc", "also run the linter");
    expect(calls[0]!.args).toEqual([
      "send",
      "abc",
      "--prompt",
      "also run the linter",
      "--no-wait",
      "--json",
    ]);
  });

  it("startRun uses --detach with options and longer timeout", async () => {
    const { exec, calls } = fakeExec('{"id":"new"}');
    const client = createPaseoClient({
      exec,
      password: PASSWORD,
      env: { HOME: "/tmp/test-home" },
    });
    await client.startRun("fix tests", {
      provider: "codex",
      cwd: "~/dev/repo",
      title: "Voice run",
    });
    const call = calls[0]!;
    expect(call.args.slice(0, 3)).toEqual(["run", "fix tests", "--detach"]);
    expect(call.args).toContain("--provider");
    expect(call.args).toContain("--title");
    expect(call.args).toContain("/tmp/test-home/dev/repo");
    expect(call.args).not.toContain("~/dev/repo");
    expect(call.opts?.timeoutMs).toBe(60_000);
  });

  it("remote host goes through PASEO_HOST env, never argv", async () => {
    const { exec, calls } = fakeExec("[]");
    const client = createPaseoClient({
      exec,
      password: PASSWORD,
      host: "paseo-host.example:443",
    });
    await client.listSessions();
    const call = calls[0]!;
    expect(call.opts?.env?.["PASEO_HOST"]).toBe("paseo-host.example:443");
    expect(call.args.join(" ")).not.toContain("paseo-host");
  });
});
