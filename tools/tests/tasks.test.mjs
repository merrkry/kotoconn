import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { delimiter, join, resolve } from "node:path";
import { test } from "node:test";

const root = resolve(import.meta.dirname, "../..");
const nx = join(root, "node_modules/nx/dist/bin/nx.js");
const zigbuild = [
  "zigbuild",
  "--target-dir",
  "target",
  "--target",
  "x86_64-unknown-linux-gnu.2.36",
];
const debugBuild = { tool: "cargo", args: [...zigbuild, "--workspace", "--locked"] };
const releaseBuild = {
  tool: "cargo",
  args: [...zigbuild, "--release", "--locked", "-p", "kotoconn-cli", "-p", "kotoconn-tun-traffic"],
};

function runTask(t, target, args = [], failBuild = false) {
  const directory = mkdtempSync(join(tmpdir(), "kotoconn-tasks-"));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  const log = join(directory, "calls.jsonl");
  writeFileSync(log, "");

  for (const tool of ["cargo", "docker", "uv"]) {
    writeFileSync(
      join(directory, tool),
      `#!${process.execPath}
const fs = require("node:fs");
fs.appendFileSync(process.env.KOTOCONN_TASK_LOG, JSON.stringify({ tool: ${JSON.stringify(tool)}, args: process.argv.slice(2) }) + "\\n");
process.exit(${tool === "docker" && failBuild ? 17 : 0});
`,
      { mode: 0o755 },
    );
  }

  const result = spawnSync(
    process.execPath,
    [nx, "run", target, "--skipNxCache", "--output-style=static", "--", ...args],
    {
      cwd: root,
      encoding: "utf8",
      // The subprocess includes Nx startup and external command execution.
      timeout: 60_000,
      env: {
        ...process.env,
        PATH: `${directory}${delimiter}${process.env.PATH}`,
        NX_DAEMON: "false",
        NX_TUI: "false",
        KOTOCONN_TASK_LOG: log,
        KOTOCONN_RUST_TARGET: "x86_64-unknown-linux-gnu.2.36",
      },
    },
  );
  assert.ifError(result.error);

  const calls = readFileSync(log, "utf8").trim().split("\n").filter(Boolean).map(JSON.parse);
  return { ...result, calls };
}

test("TUN parameters reach only the runner, after its container build", (t) => {
  const result = runTask(t, "e2e:tun", ["--case", "generic-relay", "--profile", "stress"]);
  assert.equal(result.status, 0, result.stdout + result.stderr);
  assert.deepEqual(result.calls, [
    debugBuild,
    { tool: "docker", args: ["buildx", "bake", "-f", "docker-bake.hcl"] },
    {
      tool: "uv",
      args: [
        "run",
        "--locked",
        "python",
        "e2e/tun.py",
        "--case",
        "generic-relay",
        "--profile",
        "stress",
      ],
    },
  ]);
});

test("a failed build prevents the E2E runner from starting", (t) => {
  const result = runTask(t, "e2e:test", [], true);
  assert.notEqual(result.status, 0);
  assert.deepEqual(result.calls, [
    debugBuild,
    { tool: "docker", args: ["buildx", "bake", "-f", "docker-bake.hcl"] },
  ]);
});

test("benchmarks build release artifacts and preserve runner arguments", (t) => {
  const result = runTask(t, "benchmarks:run", ["--duration", "2"]);
  assert.equal(result.status, 0, result.stdout + result.stderr);
  assert.deepEqual(result.calls, [
    releaseBuild,
    { tool: "docker", args: ["buildx", "bake", "-f", "docker-bake.hcl", "release"] },
    { tool: "uv", args: ["run", "--locked", "python", "benchmarks/run.py", "--duration", "2"] },
  ]);
});
