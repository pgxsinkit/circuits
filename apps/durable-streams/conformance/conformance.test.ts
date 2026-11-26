/**
 * Run the server conformance suite against the Rust server.
 *
 * Two modes:
 *   - CI / default: builds nothing, but spawns the release binary itself,
 *     mirroring the Caddy harness. Build it first with
 *     `cargo build --profile release-durable-streams -p durable-streams` (the
 *     workspace profile the log server is released with), then run
 *     `bun run test:conformance`; the repository root's
 *     `bun run test:durable-streams:conformance` does both. The binary is
 *     located through `cargo metadata` (honouring CARGO_TARGET_DIR and the
 *     workspace target directory); RUST_SERVER_BIN overrides it outright, which
 *     is what a CI job that downloads a prebuilt binary should use.
 *   - Manual: set RUST_SERVER_URL to point at an already-running server, e.g.
 *     RUST_SERVER_URL=http://localhost:4562 bun run test:conformance
 */
import { execFileSync, spawn } from "node:child_process";
import type { ChildProcess } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import * as path from "node:path";

import { runConformanceTests } from "@durable-streams/server-conformance-tests";
import { afterAll, beforeAll, describe } from "vitest";

// Manual mode: run against an externally-started server. Otherwise spawn our own.
const externalUrl = process.env.RUST_SERVER_URL;
const port = Number(process.env.RUST_SERVER_PORT ?? 0);
const longPollTimeoutMs = 500;
// The binary drains live HTTP requests for up to 25s before joining WAL committers.
const shutdownTimeoutMs = 35_000;

const config = {
  baseUrl: externalUrl ?? "",
  longPollTimeoutMs,
  // This server implements the core protocol only; the subscription/control-plane
  // suite is out of scope, so skip it rather than report it as failing.
  subscriptions: false,
};

let server: ChildProcess | null = null;
let listening = false;
let startupError: Error | undefined;
// Hoisted so afterAll can remove it: a full suite run leaves up to ~1 GB of
// stream data under this directory, and nothing else ever collects it.
let dataDir: string | null = null;

beforeAll(async () => {
  if (!externalUrl) {
    const binary = resolveServerBinary();
    dataDir = mkdtempSync(path.join(tmpdir(), `ds-rust-conformance-`));
    // Extra server flags for the run-configuration matrix (CI runs the suite
    // once per config — see README "Run-configuration matrix" + ci.yml). E.g.
    // RUST_SERVER_ARGS="--durability memory" or "--read-offload always" or
    // "--tail-cache-bytes 65536". Whitespace-separated; empty = the default
    // (wal, resident cache off on Linux).
    const extraArgs = (process.env.RUST_SERVER_ARGS ?? ``).trim().split(/\s+/).filter(Boolean);
    server = spawn(
      binary,
      [
        `--host`,
        `127.0.0.1`,
        `--port`,
        String(port),
        `--data-dir`,
        dataDir,
        // Must match config.longPollTimeoutMs so the suite's timeout assertions hold.
        `--long-poll-timeout-ms`,
        String(longPollTimeoutMs),
        ...extraArgs,
      ],
      { stdio: [`ignore`, `pipe`, `pipe`] },
    );
    let output = "";
    server.stdout?.on(`data`, (data: Buffer) => {
      if (listening) return;
      output = (output + data.toString()).slice(-4096);
      const address = output.match(/durable-streams-server listening on (http:\/\/127\.0\.0\.1:\d+) /)?.[1];
      if (address) {
        config.baseUrl = address;
        listening = true;
      }
    });
    server.on(`error`, (error) => {
      startupError = error;
    });
    server.stderr?.on(`data`, (d: Buffer) => process.stderr.write(`[rust] ${d.toString()}`));
    server.on(`exit`, (code) => {
      if (code) process.stderr.write(`[rust] server exited with code ${code}\n`);
    });
  }
  await waitForServer(15000);
}, 20000);

afterAll(async () => {
  let shutdownTimedOut = false;
  try {
    if (server?.pid && server.exitCode === null && server.signalCode === null) {
      const child = server;
      await new Promise<void>((resolve) => {
        const timeout = setTimeout(() => {
          shutdownTimedOut = true;
          child.kill("SIGKILL");
        }, shutdownTimeoutMs);
        child.once("exit", () => {
          clearTimeout(timeout);
          resolve();
        });
        child.kill(`SIGTERM`);
      });
    }
  } finally {
    // Only remove storage this fixture owns, after the child exits even if SIGKILL was necessary.
    if (dataDir) {
      rmSync(dataDir, { recursive: true, force: true });
      dataDir = null;
    }
  }
  if (shutdownTimedOut) throw new Error(`Conformance server did not shut down within ${shutdownTimeoutMs}ms`);
  if (server?.pid && server.exitCode !== 0) {
    throw new Error(`Conformance server exited with ${server.signalCode ?? server.exitCode}`);
  }
}, shutdownTimeoutMs + 10_000);

function resolveServerBinary(): string {
  if (process.env.RUST_SERVER_BIN) return process.env.RUST_SERVER_BIN;
  const metadata = JSON.parse(
    execFileSync(`cargo`, [`metadata`, `--locked`, `--format-version=1`, `--no-deps`], {
      cwd: path.join(__dirname, `..`),
      encoding: `utf8`,
    }),
  ) as { target_directory: string };
  return path.join(metadata.target_directory, `release-durable-streams`, `durable-streams-server`);
}

describe(`Rust Server Implementation`, () => {
  runConformanceTests(config);
});

async function waitForServer(timeoutMs: number): Promise<void> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (startupError) throw startupError;
    if (server && (server.exitCode !== null || server.signalCode !== null)) {
      throw new Error(`Owned conformance server exited before readiness (${server.exitCode ?? server.signalCode})`);
    }
    try {
      // A child-owned bind must precede the probe, so an unrelated listener cannot pass readiness.
      if (externalUrl || listening) {
        await fetch(config.baseUrl);
        return;
      }
    } catch {
      // The process has bound but may still be entering its serving loop.
    }
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new Error(`Rust server did not become ready within ${timeoutMs}ms`);
}
