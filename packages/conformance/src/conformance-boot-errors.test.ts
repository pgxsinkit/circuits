// The boot-time error taxonomy (issue #13): a Postgres failure at boot is either a configuration
// the engine cannot work around, or a condition that may clear on its own — and the two get
// opposite treatment.
//
//   FATAL      → exit 78 (`EX_CONFIG`) immediately, with a NAMED message. Bad credentials, a missing
//                privilege, an unknown database, `wal_level` ≠ `logical`. Retrying repeats the same
//                refusal forever, and a crash-loop with a clear reason is what an operator can act
//                on.
//   RETRYABLE  → back off (1 s → 30 s, jittered) and keep trying, indefinitely, while `GET /ready`
//                reports `503 {"status":"waiting"}`. Connection refused, DNS, a timeout, "the
//                database system is starting up". Kubernetes gates traffic on readiness, so a
//                restart buys nothing — and a database that comes up after its engine is the normal
//                case, not a failure.
//
// The engine binary is spawned directly (`spawnRawEngine`) rather than through `bootHarness`, which
// waits for a successful boot by construction: here the point is precisely what happens when the
// boot does NOT succeed.

import { DurableStreamTestServer } from "@circuits/ds-rust";
import pgpkg from "pg";
import { afterEach, describe, expect, it } from "vitest";

import { buildEngine, spawnRawEngine, type RawEngine } from "./harness.js";

/** A port nothing listens on — a connect here is refused at once (the retryable case). */
const DEAD_PG_PORT = 5;

/**
 * An address in the RFC 5737 / RFC 1918 blackhole range: packets go out and nothing ever comes
 * back, so the connect neither succeeds nor is refused — it HANGS on the kernel's SYN retry
 * schedule, for minutes. This is the shape a firewalled database or a stale Service IP has, and it
 * is the one that used to leave the process alive long past its grace period.
 */
const BLACKHOLE_PG = "postgres://u:p@10.255.255.1:5432/db";

function adminUrl(): string {
  const url = process.env.CIRCUITS_TEST_PG_URL;
  if (!url) throw new Error("CIRCUITS_TEST_PG_URL not set (vitest globalSetup should boot Postgres)");
  return url;
}

/**
 * The shared test Postgres under a role that does not exist — SQLSTATE `28000`, the same
 * `invalid_authorization_specification` class a wrong password produces.
 *
 * A wrong PASSWORD cannot be tested here: the harness's ephemeral Postgres is `initdb --auth=trust`
 * (see `vitest.global-setup.ts`), so it accepts any password for a role that exists. A missing role
 * is refused by the very same code path, with the same SQLSTATE class and the same "authentication
 * failed" verdict, and it does not depend on the cluster's `pg_hba` at all.
 */
function badRoleUrl(): string {
  const u = new URL(adminUrl());
  u.username = "nobody_by_this_name";
  u.password = "irrelevant-under-trust-auth";
  return u.toString();
}

/** The shared test Postgres, pointed at a database that does not exist (SQLSTATE `3D000`). */
function unknownDatabaseUrl(): string {
  const u = new URL(adminUrl());
  u.pathname = "/no_such_database_here";
  return u.toString();
}

function unreachableUrl(): string {
  const u = new URL(adminUrl());
  u.port = String(DEAD_PG_PORT);
  return u.toString();
}

let ds: DurableStreamTestServer | undefined;
let engine: RawEngine | undefined;
let scratchDb: string | undefined;

afterEach(async () => {
  engine?.proc.kill("SIGKILL");
  engine = undefined;
  await ds?.stop().catch(() => {});
  ds = undefined;
  if (scratchDb) {
    // The DS-unreachable case gets its OWN database: its boot reaches Postgres and creates a
    // publication there before it ever touches storage, and the shared cluster must not collect
    // that litter.
    const a = new pgpkg.Client({ connectionString: adminUrl() });
    await a.connect().catch(() => {});
    await a.query(`DROP DATABASE IF EXISTS ${scratchDb} WITH (FORCE)`).catch(() => {});
    await a.end().catch(() => {});
    scratchDb = undefined;
  }
});

/** A throwaway database on the shared test cluster, dropped in `afterEach`. */
async function scratchDbUrl(): Promise<string> {
  scratchDb = `boot_${process.pid}_${Date.now().toString(36)}`.toLowerCase();
  const a = new pgpkg.Client({ connectionString: adminUrl() });
  await a.connect();
  await a.query(`CREATE DATABASE ${scratchDb}`);
  await a.end();
  const u = new URL(adminUrl());
  u.pathname = `/${scratchDb}`;
  const url = u.toString();
  // One real table: `CIRCUITS_PG_TABLES='*'` refuses a schema with no primary-keyed base
  // tables, and that refusal would fire before the boot ever reached durable-streams.
  const d = new pgpkg.Client({ connectionString: url });
  await d.connect();
  await d.query("CREATE TABLE items (id int PRIMARY KEY, n int)");
  await d.query("ALTER TABLE items REPLICA IDENTITY FULL");
  await d.end();
  return url;
}

interface SpawnOpts {
  /** Override the durable-streams URL (default: a real server booted for the test). */
  dsUrl?: string;
}

/** Spawn the binary against a real durable-streams server and the given Postgres URL. */
async function spawnAgainst(pgUrl: string, opts: SpawnOpts = {}): Promise<RawEngine> {
  buildEngine();
  let dsUrl = opts.dsUrl;
  if (dsUrl === undefined) {
    ds = new DurableStreamTestServer({ port: 0 });
    dsUrl = await ds.start();
  }
  engine = spawnRawEngine({
    CIRCUITS_DS_URL: dsUrl,
    CIRCUITS_BIND: "127.0.0.1:0",
    CIRCUITS_LOG: process.env.CIRCUITS_LOG ?? "info",
    CIRCUITS_PG_URL: pgUrl,
    CIRCUITS_PG_TABLES: "*",
    CIRCUITS_PG_SLOT: `boot_errors_${process.pid}_${Date.now().toString(36)}`,
  });
  return engine;
}

/** Poll `/ready` until it reports `status`, or fail with what it actually said. */
async function waitForReady(url: string, status: string, timeoutMs = 20000): Promise<void> {
  let saw = "never answered";
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const res = await fetch(`${url}/ready`).catch(() => undefined);
    if (res) {
      const body = (await res.json()) as { status: string };
      saw = `${res.status} ${body.status}`;
      if (body.status === status) return;
    }
    await new Promise((r) => setTimeout(r, 50));
  }
  throw new Error(`/ready never reported '${status}' (last: ${saw})`);
}

// `/ready` reports `waiting` from the moment the engine is constructed, which is before its first
// connection attempt has failed and been logged. A test that reads the log as soon as it sees
// `waiting` races that first warning, so it waits for the line itself.
async function waitForStderr(e: RawEngine, text: string, timeoutMs = 20000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (e.stderr().includes(text)) return;
    await new Promise((r) => setTimeout(r, 50));
  }
  throw new Error(`stderr never contained '${text}'\n${e.stderr()}`);
}

describe("boot-time error taxonomy", () => {
  it("exits 78 quickly with a named message when authentication is refused", async () => {
    const e = await spawnAgainst(badRoleUrl());
    const exit = await e.waitForExit(20000);
    expect(exit.code, `expected EX_CONFIG (78), got ${JSON.stringify(exit)}\n${e.stderr()}`).toBe(78);
    // The message must name the CLASS of failure, not just echo a driver error.
    expect(e.stderr()).toContain("boot refused");
    expect(e.stderr()).toContain("authentication failed");
    // ...and it must never report the boot as resolved.
    expect(e.stderr()).not.toContain("postgres mode:");
  });

  it("exits 78 with a named message when the database does not exist", async () => {
    const e = await spawnAgainst(unknownDatabaseUrl());
    const exit = await e.waitForExit(20000);
    expect(exit.code, `expected EX_CONFIG (78), got ${JSON.stringify(exit)}\n${e.stderr()}`).toBe(78);
    expect(e.stderr()).toContain("unknown database");
  });

  it("exits 78 with a named message when the connection string cannot be parsed", async () => {
    // The one no-SQLSTATE failure that MUST NOT be retried: to the classifier a `Config::from_str`
    // failure is indistinguishable from "the database is not up yet" (no server answer at all), so
    // it is refused at configuration time instead — before the HTTP port is even bound.
    const e = await spawnAgainst("postgres://someone@dbhost:notaport/app");
    const exit = await e.waitForExit(20000);
    expect(exit.code, `expected EX_CONFIG (78), got ${JSON.stringify(exit)}\n${e.stderr()}`).toBe(78);
    expect(e.stderr()).toContain("boot refused");
    expect(e.stderr()).toContain("unusable Postgres URL");
    // It must never have entered the retry loop.
    expect(e.stderr()).not.toContain("Postgres not ready");
  });

  it("does not hang past the grace when the Postgres address is a blackhole", async () => {
    // The regression this exists for: a connect that HANGS (rather than being refused) used to be
    // awaited un-raced, so the port closed after the drain window and the process then sat there —
    // alive, unreachable, and long past its grace — until the kernel gave up on the SYN retries.
    // Two things fix it and both are asserted here: the connect has a timeout, and the whole setup
    // is raced against the shutdown token.
    const e = await spawnAgainst(BLACKHOLE_PG);
    const url = await e.waitForBinding(20000);
    await waitForReady(url, "waiting");

    const signalledAt = Date.now();
    e.signal("SIGTERM");
    const exit = await e.waitForExit(30000);
    const took = Date.now() - signalledAt;
    expect(exit.code, `expected a clean exit, got ${JSON.stringify(exit)}\n${e.stderr()}`).toBe(0);
    expect(took, "a terminating engine must not wait out a hanging connect (nor its 25s grace)").toBeLessThan(6000);
  });

  it("waits for durable-streams that is not up yet instead of exiting EX_CONFIG", async () => {
    // Storage coming up after the engine is as ordinary as a database coming up after it, and it
    // used to be fatal: a catalog fold against a closed port carries no Postgres error, so the boot
    // taxonomy called it "not a transient Postgres condition" and exited 78.
    const e = await spawnAgainst(await scratchDbUrl(), { dsUrl: "http://127.0.0.1:1" });
    const url = await e.waitForBinding(20000);
    await waitForReady(url, "waiting");

    // Retrying, and saying so by name.
    await waitForStderr(e, "durable-streams is unreachable");
    expect(e.stderr()).toContain("retrying in");
    expect(e.stderr()).not.toContain("boot refused");
    // Liveness is unmoved, as with an absent database.
    expect((await fetch(`${url}/health`)).status).toBe(200);

    e.signal("SIGTERM");
    const exit = await e.waitForExit(30000);
    expect(exit.code, `expected a clean exit, got ${JSON.stringify(exit)}\n${e.stderr()}`).toBe(0);
  });

  it("stays up retrying with /ready = 503 waiting when Postgres is unreachable, and exits 0 on SIGTERM", async () => {
    const e = await spawnAgainst(unreachableUrl());

    // The HTTP surface comes up BEFORE Postgres, precisely so readiness is answerable while waiting.
    const url = await e.waitForBinding(20000);

    let saw = { code: 0, status: "" };
    const deadline = Date.now() + 15000;
    while (Date.now() < deadline) {
      const res = await fetch(`${url}/ready`).catch(() => undefined);
      if (res) {
        saw = { code: res.status, status: ((await res.json()) as { status: string }).status };
        if (saw.status === "waiting") break;
      }
      await new Promise((r) => setTimeout(r, 50));
    }
    expect(saw, `expected 503 waiting while Postgres is unreachable\n${e.stderr()}`).toEqual({
      code: 503,
      status: "waiting",
    });

    // Liveness must stay 200: the process is fine, its database is not — restarting fixes nothing.
    expect((await fetch(`${url}/health`)).status).toBe(200);

    // It must be RETRYING, not stuck: the log names the class of failure and the next delay.
    await waitForStderr(e, "Postgres not ready");
    expect(e.stderr()).toContain("retrying in");

    // And a pod terminated while still waiting for its database exits cleanly, not with a kill.
    e.signal("SIGTERM");
    const exit = await e.waitForExit(30000);
    expect(exit.code, `expected a clean exit while still waiting, got ${JSON.stringify(exit)}\n${e.stderr()}`).toBe(0);
  });
});
