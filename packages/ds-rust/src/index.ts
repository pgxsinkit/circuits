// Drop-in replacement for `@durable-streams/server`'s DurableStreamTestServer, backed by the
// Rust log server built from this repository (apps/durable-streams, crate `durable-streams`).
// Same constructor options and `start()` / `stop()` surface, but the server is a spawned native
// binary instead of an in-process Node store — the same wire protocol the production server speaks.
//
// Binary resolution:
//   1. $DS_RUST_BIN, when set: an explicit path, which must exist.
//   2. Otherwise the workspace build: <target>/debug/durable-streams-server, where <target> is
//      $CARGO_TARGET_DIR (a relative path is taken from the repository root) or <repo>/target.
// Nothing else is searched: a `durable-streams-server` on $PATH, in ~/.cargo/bin or from crates.io
// is some other build, and picking one up silently would test the wrong server. A missing binary
// is an error that names the command to build it. The vitest global setup builds it before the
// workers start.
//
// Semantics mapping vs the Node test server:
//   - `dataDir` omitted (the Node "in-memory" mode) → a fresh temp dir, deleted on stop().
//     On Linux we additionally pass `--durability memory` (no WAL/fsync — matches the Node
//     server's non-durable semantics); the flag is Linux-only, so macOS runs `wal`.
//   - `port: 0` → the wrapper picks a free port itself (the binary logs the *requested*
//     address, so OS-assigned ports would be unreadable); bind races are retried.

import { type ChildProcess, spawn } from "node:child_process";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const BIN_NAME = "durable-streams-server";
/** The repository root (the Cargo workspace root): this file is packages/ds-rust/src/index.ts. */
const REPO_ROOT = join(dirname(fileURLToPath(import.meta.url)), "../../..");

export interface TestServerOptions {
  /** Listen port; 0 (default) picks a free port. */
  port?: number;
  /** Listen address (default 127.0.0.1). */
  host?: string;
  /** Storage directory. Omitted = ephemeral temp dir, removed on stop() (the Node server's in-memory mode). */
  dataDir?: string;
  /** `live=long-poll` block time in ms (server default 30000). */
  longPollTimeout?: number;
  /**
   * Storage durability passed explicitly to the server. `wal` is supported on every host
   * used by this suite; `memory` is retained for the Linux-only ephemeral compatibility mode.
   */
  durability?: "wal" | "memory";
}

/** Where the workspace's debug build of the log server lands (it may not exist yet). */
export function workspaceServerBinary(): string {
  const targetDir = process.env.CARGO_TARGET_DIR
    ? resolve(REPO_ROOT, process.env.CARGO_TARGET_DIR)
    : join(REPO_ROOT, "target");
  return join(targetDir, "debug", BIN_NAME);
}

/**
 * Locate the server binary: $DS_RUST_BIN, else the workspace build. Never builds, installs or
 * searches anywhere else; a missing binary throws with the command that builds it.
 */
export function ensureServerBinary(): string {
  const override = process.env.DS_RUST_BIN;
  if (override) {
    if (!existsSync(override)) throw new Error(`DS_RUST_BIN=${override} does not exist`);
    return override;
  }
  const bin = workspaceServerBinary();
  if (!existsSync(bin)) {
    throw new Error(
      `${bin} does not exist: build the log server with \`cargo build -p durable-streams\` ` +
        `from the repository root (${REPO_ROOT}), or point DS_RUST_BIN at a durable-streams-server binary`,
    );
  }
  return bin;
}

/** Ask the OS for a currently-free port (tiny race window; bind failures are retried). */
function freePort(host: string): Promise<number> {
  return new Promise((resolve, reject) => {
    const srv = createServer();
    srv.once("error", reject);
    srv.listen(0, host, () => {
      const addr = srv.address();
      if (addr === null || typeof addr === "string") {
        srv.close(() => reject(new Error("could not allocate a port")));
        return;
      }
      srv.close(() => resolve(addr.port));
    });
  });
}

export class DurableStreamTestServer {
  private readonly opts: TestServerOptions;
  private proc: ChildProcess | undefined;
  private tempDir: string | undefined;
  private url_: string | undefined;
  // These are fixed after the first successful start so crash/restart cannot accidentally point
  // at a new store. In particular, `port: 0` is resolved once by the wrapper, not by the server.
  private bin: string | undefined;
  private host: string | undefined;
  private port: number | undefined;
  private dataDir: string | undefined;

  constructor(opts: TestServerOptions = {}) {
    this.opts = opts;
  }

  get url(): string | undefined {
    return this.url_;
  }

  /** PID of the spawned `durable-streams-server` process, once started. */
  get pid(): number | undefined {
    return this.proc?.pid;
  }

  /** Read-only test-fixture seam for lifecycle assertions; never use this as a production storage API. */
  get testStoragePath(): string | undefined {
    return this.dataDir;
  }

  /** Spawn the server and resolve with its base URL once it reports listening. */
  async start(): Promise<string> {
    if (this.proc) throw new Error("already started");
    this.bin ??= ensureServerBinary();
    this.host ??= this.opts.host ?? "127.0.0.1";
    if (!this.dataDir) {
      this.dataDir = this.opts.dataDir;
      if (this.dataDir === undefined) {
        this.dataDir = mkdtempSync(join(tmpdir(), "ds-rust-"));
        this.tempDir = this.dataDir;
      }
    }

    // A restart must use the precise resolved endpoint, not a freshly selected port. First boot
    // retains the small bind-race retry that `port: 0` historically provided.
    if (this.port !== undefined) return this.startAt(this.port);
    // Bind-conflict retry: freePort()'s reservation is released before the spawn, so another
    // process can steal it; the binary exits immediately on a failed bind and we re-roll.
    let lastErr: unknown;
    for (let attempt = 0; attempt < 5; attempt++) {
      const port = this.opts.port && this.opts.port !== 0 ? this.opts.port : await freePort(this.host);
      try {
        this.url_ = await this.startAt(port);
        return this.url_;
      } catch (e) {
        lastErr = e;
        if (this.opts.port && this.opts.port !== 0) break; // fixed port: don't re-roll
      }
    }
    throw new Error(`durable-streams-server failed to start: ${String(lastErr)}`);
  }

  private startAt(port: number): Promise<string> {
    if (!this.bin || !this.host || !this.dataDir) throw new Error("server start was not initialized");
    const args = ["--host", this.host, "--port", String(port), "--data-dir", this.dataDir];
    if (this.opts.longPollTimeout !== undefined) {
      args.push("--long-poll-timeout-ms", String(this.opts.longPollTimeout));
    }
    // Explicit caller intent wins. The historical ephemeral default remains memory-only on Linux;
    // recovery tests request WAL explicitly so no host silently downgrades their persistence lane.
    const durability = this.opts.durability ?? (this.tempDir && process.platform === "linux" ? "memory" : undefined);
    if (durability) args.push("--durability", durability);
    return this.spawnOnce(this.bin, args, this.host, port).then((url) => {
      this.port = port;
      this.url_ = url;
      return url;
    });
  }

  private spawnOnce(bin: string, args: string[], host: string, port: number): Promise<string> {
    return new Promise((resolve, reject) => {
      const proc = spawn(bin, args, { stdio: ["ignore", "pipe", "pipe"] });
      let out = "";
      let settled = false;
      const timer = setTimeout(() => {
        if (settled) return;
        settled = true;
        proc.kill("SIGKILL");
        reject(new Error(`did not report listening within 15s\n${out}`));
      }, 15_000);
      const onData = (chunk: Buffer) => {
        out += chunk.toString();
        if (!settled && out.includes("listening on")) {
          settled = true;
          clearTimeout(timer);
          this.proc = proc;
          resolve(`http://${host}:${port}`);
        }
      };
      proc.stdout?.on("data", onData);
      proc.stderr?.on("data", onData);
      proc.once("exit", (code) => {
        if (this.proc === proc) this.proc = undefined;
        if (!settled) {
          settled = true;
          clearTimeout(timer);
          reject(new Error(`exited early (code ${code})\n${out}`));
        }
      });
      proc.once("error", (e) => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        reject(e);
      });
    });
  }

  private waitForExit(proc: ChildProcess, timeoutMs: number): Promise<void> {
    if (proc.exitCode !== null || proc.signalCode !== null) return Promise.resolve();
    return new Promise((resolve, reject) => {
      const onExit = () => {
        clearTimeout(timer);
        resolve();
      };
      const timer = setTimeout(() => {
        proc.removeListener("exit", onExit);
        reject(new Error(`durable-streams-server (pid ${proc.pid ?? "unknown"}) did not exit within ${timeoutMs}ms`));
      }, timeoutMs);
      proc.once("exit", onExit);
    });
  }

  /**
   * Simulate an abrupt storage-process failure and restore the same durable store in place.
   * Resolves only after SIGKILL has reaped and the exact same binary reports ready again at the
   * original host/port/data directory. `stop()` still owns final process and temp-dir cleanup.
   */
  async crashAndRestart(): Promise<string> {
    const proc = this.proc;
    if (!proc || proc.exitCode !== null || proc.signalCode !== null) throw new Error("server is not running");
    proc.kill("SIGKILL");
    await this.waitForExit(proc, 15000);
    if (this.proc === proc) this.proc = undefined;
    if (this.port === undefined) throw new Error("server endpoint was not initialized");
    return this.startAt(this.port);
  }

  /** Terminate the server (SIGTERM, escalating to SIGKILL) and remove an ephemeral data dir. */
  async stop(): Promise<void> {
    const proc = this.proc;
    this.proc = undefined;
    if (proc && proc.exitCode === null && !proc.killed) {
      proc.kill("SIGTERM");
      try {
        await this.waitForExit(proc, 3000);
      } catch {
        proc.kill("SIGKILL");
        await this.waitForExit(proc, 15000);
      }
    }
    if (this.tempDir) {
      rmSync(this.tempDir, { recursive: true, force: true });
      this.tempDir = undefined;
    }
    // `stop()` ends the public lifecycle. A later `start()` must behave like a fresh server: it
    // re-resolves the configured host/port/data directory and, when the wrapper owns storage,
    // creates and owns a new temporary directory. Only crashAndRestart deliberately bypasses this
    // reset to preserve one exact running store across a simulated process crash.
    this.bin = undefined;
    this.host = undefined;
    this.port = undefined;
    this.dataDir = undefined;
    this.url_ = undefined;
  }
}
