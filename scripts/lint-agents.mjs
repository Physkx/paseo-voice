#!/usr/bin/env node
/** Consistency checks for the repository's routed agent documents. */
import { existsSync, readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";

const ROOT = process.cwd();
const failures = [];
const warnings = [];

const requiredFiles = [
  "AGENTS.md",
  "CLAUDE.md",
  "docs/agents/state.md",
  "docs/agents/publish-boundary.md",
];
const lineCaps = {
  "AGENTS.md": 120,
  "docs/agents/state.md": 100,
  "docs/agents/publish-boundary.md": 60,
};
const playbookSections = [
  "## Use when",
  "## Preconditions",
  "## Files you will touch",
  "## Steps",
  "## Validate",
  "## Done means",
  "## Do not",
];

const readText = (relativePath) => readFileSync(join(ROOT, relativePath), "utf8");
const pathExists = (relativePath) => existsSync(join(ROOT, relativePath.replace(/\/$/, "")));
const fail = (message) => failures.push(message);

for (const relativePath of requiredFiles) {
  if (!pathExists(relativePath)) fail(`missing required file ${relativePath}`);
}

for (const [relativePath, cap] of Object.entries(lineCaps)) {
  if (!pathExists(relativePath)) continue;
  const count = readText(relativePath).split("\n").length;
  if (count > cap) warnings.push(`${relativePath}: ${count} lines (soft cap ${cap})`);
}

if (pathExists("CLAUDE.md") && readText("CLAUDE.md").trim() !== "@AGENTS.md") {
  fail("CLAUDE.md must contain exactly @AGENTS.md");
}

const playbookDirectory = join(ROOT, "docs/agents/playbooks");
const playbooks = existsSync(playbookDirectory)
  ? readdirSync(playbookDirectory).filter((name) => name.endsWith(".md"))
  : [];

for (const name of playbooks) {
  const relativePath = `docs/agents/playbooks/${name}`;
  const text = readText(relativePath);
  for (const section of playbookSections) {
    if (!text.includes(section)) fail(`${relativePath}: missing section "${section}"`);
  }
  const count = text.split("\n").length;
  if (count > 100) warnings.push(`${relativePath}: ${count} lines (soft cap 100)`);
}

if (pathExists("AGENTS.md")) {
  const agents = readText("AGENTS.md");
  const routerStart = agents.indexOf("## Task router");
  const routerEnd = agents.indexOf("\n## ", routerStart + 1);
  const router = routerStart === -1 ? "" : agents.slice(routerStart, routerEnd);
  if (routerStart === -1) fail("AGENTS.md is missing its task router");
  for (const match of router.matchAll(/`((?:docs\/|DECISIONS|README|CONTRIBUTING)[^`]+)`/g)) {
    if (!pathExists(match[1])) fail(`AGENTS.md router references missing path ${match[1]}`);
  }
}

const packageScripts = new Set(Object.keys(JSON.parse(readText("package.json")).scripts ?? {}));
const agentDocuments = [
  "AGENTS.md",
  "docs/agents/state.md",
  "docs/agents/publish-boundary.md",
  ...playbooks.map((name) => `docs/agents/playbooks/${name}`),
];

for (const relativePath of agentDocuments) {
  if (!pathExists(relativePath)) continue;
  for (const match of readText(relativePath).matchAll(/pnpm (?:run )?([\w:-]+)/g)) {
    const command = match[1];
    if (!["install", "exec"].includes(command) && !packageScripts.has(command)) {
      fail(`${relativePath}: no package script "${command}"`);
    }
  }
}

for (const warning of warnings) console.log(`WARN  ${warning}`);
for (const failure of failures) console.log(`FAIL  ${failure}`);
console.log(
  `lint-agents: ${failures.length > 0 ? `${failures.length} failure(s)` : "OK"}` +
    ` (${agentDocuments.length} files checked, ${warnings.length} warning(s))`,
);
process.exit(failures.length > 0 ? 1 : 0);
