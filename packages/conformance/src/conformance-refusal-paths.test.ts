// Process qualification for backlog 0007: compiled-counts recovery and two fatal Postgres
// configurations. These exercise the native binary, real pgoutput and storage, without faults.
import { execFileSync } from "node:child_process";
import { appendFileSync, existsSync, mkdtempSync, rmSync } from "node:fs";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { DurableStreamTestServer } from "@circuits/ds-rust";
import type { StreamEnvelope } from "@circuits/protocol";
import pgpkg from "pg";
import { describe, expect, it } from "vitest";

import { foldStream, waitFor, type ShapeResp } from "./engine-native.js";
import { buildEngine, spawnRawEngine, type RawEngine } from "./harness.js";

function adminUrl(): string {
  const url = process.env.CIRCUITS_TEST_PG_URL;
  if (!url) throw new Error("CIRCUITS_TEST_PG_URL not set (integration globalSetup must boot Postgres)");
  return url;
}

async function withPg<T>(url: string, body: (client: pgpkg.Client) => Promise<T>): Promise<T> {
  const client = new pgpkg.Client({ connectionString: url, connectionTimeoutMillis: 5000 });
  let result: { value: T } | undefined;
  const failures: unknown[] = [];
  try {
    await client.connect();
    result = { value: await body(client) };
  } catch (error) {
    failures.push(error);
  }
  try {
    await client.end();
  } catch (error) {
    failures.push(error);
  }
  if (failures.length === 1) throw failures[0];
  if (failures.length > 1) throw new AggregateError(failures, "Postgres operation and connection cleanup failed");
  if (!result) throw new Error("Postgres operation returned no result");
  return result.value;
}

interface Fixture {
  pgUrl: string;
  dsUrl: string;
  slot: string;
  spawn(extraEnv?: Record<string, string>): RawEngine;
}

async function withFixture<T>(pgAdmin: string, body: (fixture: Fixture) => Promise<T>): Promise<T> {
  const db = `refusal_${process.pid}_${Date.now().toString(36)}`;
  const slot = `slot_${db}`;
  const url = new URL(pgAdmin);
  url.pathname = `/${db}`;
  const pgUrl = url.toString();
  const engines: RawEngine[] = [];
  const ds = new DurableStreamTestServer({ port: 0, durability: "wal" });
  const circuitDir = mkdtempSync(join(tmpdir(), "circuits-refusal-counts-"));
  let result: { value: T } | undefined;
  const failures: unknown[] = [];
  try {
    await withPg(pgAdmin, async (c) => await c.query(`CREATE DATABASE ${db}`));
    await withPg(pgUrl, async (c) => {
      await c.query(`
        CREATE TABLE items (id integer PRIMARY KEY, cohort integer NOT NULL);
        ALTER TABLE items REPLICA IDENTITY FULL;
        CREATE TABLE other (id integer PRIMARY KEY, n integer NOT NULL);
        ALTER TABLE other REPLICA IDENTITY FULL;
        INSERT INTO items VALUES (1, 10);
        INSERT INTO other VALUES (1, 100);
      `);
    });
    const dsUrl = await ds.start();
    buildEngine();
    const value = await body({
      pgUrl,
      dsUrl,
      slot,
      spawn(extraEnv = {}) {
        const engine = spawnRawEngine({
          CIRCUITS_BIND: "127.0.0.1:0",
          CIRCUITS_DS_URL: dsUrl,
          CIRCUITS_PG_URL: pgUrl,
          CIRCUITS_PG_TABLES: "items,other",
          CIRCUITS_PG_SLOT: slot,
          CIRCUITS_PG_POLL_MS: "25",
          CIRCUITS_LOG: "info",
          CIRCUITS_TRACE: "1",
          CIRCUITS_DBSP_COUNTS: "",
          CIRCUITS_DBSP_DIR: circuitDir,
          CIRCUITS_SCHEMA_RECONCILE_SECS: "3600",
          ...extraEnv,
        });
        engines.push(engine);
        return engine;
      },
    });
    result = { value };
  } catch (error) {
    failures.push(error);
  }
  // Capture the body result first; cleanup must run to completion and retain any original
  // setup/assertion failure alongside teardown failures, rather than overwrite it in finally.
  const exits = await Promise.allSettled(
    engines.map(async (engine) => {
      engine.signal("SIGKILL");
      await engine.waitForExit(10000);
    }),
  );
  for (const exit of exits) {
    if (exit.status === "rejected") failures.push(exit.reason);
  }
  try {
    await ds.stop();
  } catch (error) {
    failures.push(error);
  }
  try {
    await withPg(pgAdmin, async (c) => {
      try {
        // PostgreSQL may still be releasing the walsender after its engine exited.
        await waitFor(async () => {
          const rows = await c.query("SELECT active_pid FROM pg_replication_slots WHERE slot_name = $1", [slot]);
          return rows.rowCount === 0 || rows.rows[0].active_pid === null;
        }, "replication slot release");
        await c.query(
          "SELECT pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
          [slot],
        );
      } catch (error) {
        failures.push(error);
      }
      await c.query(`DROP DATABASE IF EXISTS ${db} WITH (FORCE)`);
    });
  } catch (error) {
    failures.push(error);
  }
  // Do not remove files a process may still have open when reaping itself failed.
  if (exits.every((exit) => exit.status === "fulfilled")) {
    try {
      rmSync(circuitDir, { recursive: true, force: true });
    } catch (error) {
      failures.push(error);
    }
  }
  if (failures.length === 1) throw failures[0];
  if (failures.length > 1) throw new AggregateError(failures, "refusal fixture operation and cleanup failed");
  if (!result) throw new Error("refusal fixture returned no result");
  return result.value;
}

async function create(url: string, route: "shapes" | "aggregate", body: unknown): Promise<ShapeResp> {
  const response = await fetch(`${url}/${route}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(5000),
  });
  expect(response.status, await response.clone().text()).toBe(200);
  return (await response.json()) as ShapeResp;
}

async function assertCounts(url: string, shape: ShapeResp, expected: number): Promise<void> {
  const response = await fetch(`${url}/graph`);
  expect(response.status).toBe(200);
  const graph = (await response.json()) as {
    shapes: { id: string; circuit?: { label: string; counts: boolean } }[];
    arrangements: {
      counts: { id: string; table: string; seeded: boolean; groupCols: string[] }[];
      consumers: { index: string; dependentKind: string; dependentId: string }[];
    };
  };
  expect(graph.shapes.find((s) => s.id === shape.shapeId)?.circuit).toEqual({ label: "counts", counts: true });
  expect(graph.arrangements.counts).toContainEqual({
    id: "arr:counts:public.items",
    input: "arr:input:public.items",
    table: "public.items",
    seeded: true,
    groupCols: ["cohort"],
  });
  expect(graph.arrangements.consumers).toContainEqual({
    index: "arr:counts:public.items",
    dependentKind: "circuit-agg",
    dependentId: shape.shapeId,
    connectingCol: "",
  });
  await waitFor(
    async () => Number((await foldStream(shape.streamUrl)).get("agg")?.value) === expected,
    `COUNT=${expected}`,
  );
}

async function changes(dsUrl: string): Promise<StreamEnvelope[]> {
  const all: StreamEnvelope[] = [];
  let offset = "-1";
  for (let page = 0; page < 100; page++) {
    const response = await fetch(`${dsUrl}/changes/0?offset=${encodeURIComponent(offset)}`);
    if (response.status === 404 || response.status === 204) return all;
    expect(response.status).toBe(200);
    const text = (await response.text()).trim();
    if (text) all.push(...(JSON.parse(text) as StreamEnvelope[]));
    const next = response.headers.get("stream-next-offset");
    if (response.headers.has("stream-up-to-date") || !next || next === offset) return all;
    offset = next;
  }
  throw new Error("change log exceeded the bounded fixture's 100-page read");
}

async function trigger(pgUrl: string, sql: string): Promise<string> {
  return await withPg(pgUrl, async (c) => {
    await c.query("BEGIN");
    const xid = String((await c.query("SELECT pg_current_xact_id()::text AS xid")).rows[0].xid);
    await c.query(sql);
    await c.query("COMMIT");
    return xid;
  });
}

async function expectRebuild(engine: RawEngine): Promise<void> {
  const exit = await engine.waitForExit(20000);
  expect(exit, engine.stderr()).toEqual({ code: 75, signal: null });
  expect(engine.stderr()).toContain("which has a counts pipeline");
}

async function expectBootRefusal(engine: RawEngine, cause: string): Promise<void> {
  let stdout = "";
  engine.proc.stdout!.on("data", (chunk: Buffer) => {
    stdout += chunk.toString();
  });
  const exit = await engine.waitForExit(20000);
  expect(exit, engine.stderr()).toEqual({ code: 78, signal: null });
  expect(engine.stderr()).toContain("boot refused");
  expect(engine.stderr()).toContain(cause);
  expect(engine.stderr()).not.toContain("postgres mode:");
  expect(stdout).not.toContain("ENGINE_LISTENING");
  await expect(engine.waitForListening(1000)).rejects.toThrow("before printing ENGINE_LISTENING");
}

async function freePort(): Promise<number> {
  const server = createServer();
  return await new Promise<number>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (!address || typeof address === "string") {
        server.close(() => reject(new Error("could not allocate a PostgreSQL port")));
        return;
      }
      server.close((error) => (error ? reject(error) : resolve(address.port)));
    });
  });
}

async function withReplicaCluster<T>(body: (url: string) => Promise<T>): Promise<T> {
  // These are runtime test-fixture files, owned and removed with the ephemeral cluster.
  const dir = mkdtempSync(join(tmpdir(), "circuits-replica-pg-"));
  const data = join(dir, "data");
  let initialized = false;
  let result: { value: T } | undefined;
  const failures: unknown[] = [];
  try {
    execFileSync("initdb", ["-D", data, "-U", "postgres", "--auth=trust", "--no-sync"], { stdio: "pipe" });
    initialized = true;
    let port = 0;
    let started = false;
    for (let attempt = 0; attempt < 5 && !started; attempt++) {
      port = await freePort();
      appendFileSync(
        join(data, "postgresql.conf"),
        `\nwal_level = replica\nlisten_addresses = '127.0.0.1'\nport = ${port}\n` +
          `unix_socket_directories = '${dir}'\nfsync = off\nsynchronous_commit = off\n`,
      );
      try {
        execFileSync("pg_ctl", ["-D", data, "-l", join(dir, "postgres.log"), "-w", "-t", "10", "start"], {
          stdio: "pipe",
        });
        started = true;
      } catch (error) {
        // A startup timeout can leave a live postmaster. Stop it in cleanup rather than
        // change the port beneath it and retry against the same running directory.
        if (attempt === 4 || existsSync(join(data, "postmaster.pid"))) throw error;
      }
    }
    const url = `postgres://postgres@127.0.0.1:${port}/postgres`;
    await withPg(url, async (c) => expect((await c.query("SHOW wal_level")).rows[0].wal_level).toBe("replica"));
    result = { value: await body(url) };
  } catch (error) {
    failures.push(error);
  }
  let removable = !initialized;
  if (initialized) {
    try {
      execFileSync("pg_ctl", ["-D", data, "-m", "immediate", "-w", "-t", "10", "stop"], { stdio: "pipe" });
      removable = true;
    } catch (error) {
      // A failed start may never install a postmaster. A still-present PID file instead
      // means shutdown failed: preserve its directory and retain the failure alongside
      // any setup/test failure, then report both after all safe cleanup has run.
      removable = !existsSync(join(data, "postmaster.pid"));
      if (!removable) failures.push(error);
    }
  }
  if (removable) {
    try {
      rmSync(dir, { recursive: true, force: true });
    } catch (error) {
      failures.push(error);
    }
  }
  if (failures.length === 1) throw failures[0];
  if (failures.length > 1) throw new AggregateError(failures, "replica cluster operation and cleanup failed");
  if (!result) throw new Error("replica cluster returned no result");
  return result.value;
}

describe("process refusal and counts recovery paths", () => {
  it("rebuilds counts after ADD COLUMN, replays the triggering transaction, and restores unrelated shapes", async () => {
    await withFixture(adminUrl(), async (f) => {
      const env = { CIRCUITS_DBSP_COUNTS: "items:cohort" };
      const engine = f.spawn(env);
      const url = await engine.waitForListening();
      const aggregate = await create(url, "aggregate", { table: "items", fn: "count" });
      const other = await create(url, "shapes", { table: "other" });
      await assertCounts(url, aggregate, 1);
      await withPg(f.pgUrl, async (c) => await c.query("INSERT INTO items VALUES (2, 20)"));
      await assertCounts(url, aggregate, 2);

      const xid = await trigger(
        f.pgUrl,
        "ALTER TABLE items ADD COLUMN label text; INSERT INTO items VALUES (3, 10, 'trigger');",
      );
      await expectRebuild(engine);
      expect(engine.stderr()).toContain("schema drift on public.items");
      // Relation drift exits inline, before this transaction's Commit can be appended or acked.
      expect((await changes(f.dsUrl)).some((e) => e.headers.txid === xid)).toBe(false);

      const restarted = f.spawn(env);
      const recoveredUrl = await restarted.waitForListening();
      // The replayed Relation may still be resolving the table after readiness. This xid
      // was absent before exit 75; observing its committed output proves inline Relation
      // handling finished before replacement shapes are admitted.
      await waitFor(async () => (await changes(f.dsUrl)).some((e) => e.headers.txid === xid), "triggering xid replay");
      const recovered = await create(recoveredUrl, "aggregate", { table: "items", fn: "count" });
      await assertCounts(recoveredUrl, recovered, 3);
      const retired = await fetch(`${recoveredUrl}/shapes/${aggregate.shapeId}`);
      expect(retired.status).toBe(404);
      const rows = await create(recoveredUrl, "shapes", { table: "items" });
      expect((await foldStream(rows.streamUrl)).get("3")?.label).toBe("trigger");
      await withPg(f.pgUrl, async (c) => {
        await c.query("INSERT INTO items VALUES (4, 20, 'live')");
        await c.query("UPDATE other SET n = 101 WHERE id = 1");
      });
      await assertCounts(recoveredUrl, recovered, 4);
      await waitFor(async () => (await foldStream(other.streamUrl)).get("1")?.n === 101, "restored shape live update");
      expect((await fetch(`${recoveredUrl}/ready`)).status).toBe(200);
      expect(restarted.proc.exitCode).toBeNull();

      // ADD COLUMN matches the fresh schema at restart, so it alone does not execute the xid
      // guard again. TRUNCATE is replayed unconditionally and independently qualifies that guard.
      const truncateXid = await trigger(f.pgUrl, "TRUNCATE items; INSERT INTO items VALUES (5, 10, 'after truncate');");
      await expectRebuild(restarted);
      expect(restarted.stderr()).toContain("TRUNCATE on public.items");
      const replayed = f.spawn(env);
      const replayedUrl = await replayed.waitForListening();
      await waitFor(
        async () => (await changes(f.dsUrl)).some((e) => e.headers.txid === truncateXid),
        "TRUNCATE xid replay",
      );
      expect(replayed.stderr()).toContain("Not restarting again.");
      expect(replayed.stderr()).toContain("boot seed snapshot already reflects");
      // A replayed TRUNCATE also retires dependents created during replay (backlog 0006).
      // Recreate only after its pgoutput handling has demonstrably completed.
      const reseeded = await create(replayedUrl, "aggregate", { table: "items", fn: "count" });
      await assertCounts(replayedUrl, reseeded, 1);
      await withPg(f.pgUrl, async (c) => await c.query("INSERT INTO items VALUES (6, 20, 'still live')"));
      await assertCounts(replayedUrl, reseeded, 2);
      expect((await fetch(`${replayedUrl}/ready`)).status).toBe(200);
      expect(replayed.proc.exitCode).toBeNull();
    });
  });

  it("refuses a matching publication's column list without readiness or shape admission", async () => {
    await withFixture(adminUrl(), async (f) => {
      await withPg(f.pgUrl, async (c) => {
        await c.query(`CREATE PUBLICATION ${f.slot}_pub FOR TABLE items (id, cohort), other`);
        const publication = await c.query(
          "SELECT pr.prattrs IS NOT NULL AS listed FROM pg_publication_rel pr JOIN pg_publication p ON p.oid = pr.prpubid WHERE p.pubname = $1 AND pr.prrelid = 'items'::regclass",
          [`${f.slot}_pub`],
        );
        expect(publication.rows).toEqual([{ listed: true }]);
        // Hold setup at its publication read so native admission can be observed before exit.
        await c.query("BEGIN");
        await c.query("LOCK TABLE pg_publication IN ACCESS EXCLUSIVE MODE");
        // withPg always closes this connection; an assertion failure also releases the
        // transaction's lock by disconnect, retaining the original failure if close fails.
        const engine = f.spawn();
        const url = await engine.waitForBinding();
        expect((await fetch(`${url}/ready`)).status).toBe(503);
        const refused = await fetch(`${url}/shapes`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ table: "items" }),
        });
        expect(refused.status).toBe(503);
        await c.query("COMMIT");
        await expectBootRefusal(engine, `publication '${f.slot}_pub' has a column list on public.items`);
        expect(engine.stderr()).toContain("the engine requires whole rows");
      });
    });
  });

  it("refuses an isolated replica-wal cluster with the named setting and exit 78", async () => {
    await withReplicaCluster(async (pgAdmin) => {
      await withFixture(pgAdmin, async (f) => {
        const engine = f.spawn();
        await expectBootRefusal(engine, "wal_level is 'replica', but logical replication needs 'logical'");
        expect(engine.stderr()).toContain("RESTART Postgres");
      });
    });
  });
});
