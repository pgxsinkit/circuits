import { createServer, request } from "node:http";

import type { Schema, StreamEnvelope } from "@circuits/protocol";
import { afterEach, describe, expect, it } from "vitest";

import { createShape, foldStream, pgQuery, type ShapeResp, sleep, waitFor } from "./engine-native.js";
import { bootHarness, drainEngine, type Harness } from "./harness.js";

const schema: Schema = {
  tables: {
    items: {
      columns: { id: { type: "int" }, n: { type: "int" }, payload: { type: "text" } },
      primaryKey: "id",
    },
    members: {
      columns: { id: { type: "int" }, active: { type: "bool" }, payload: { type: "text" } },
      primaryKey: "id",
    },
  },
};

interface Memory {
  process: { rss_bytes: number };
  resources: {
    pending: { memory_bytes: number; items: number; spill_bytes: number };
    replay: { active: number; queued: number; peak: number; pages: number; bytes: number };
  };
}

// Hold actual HTTP requests at the service boundary. Shape POSTs park snapshot installation;
// non-live changes GETs park replay, while the sequencer's live reads continue independently.
async function startProxy(upstreamUrl: string) {
  const upstream = new URL(upstreamUrl);
  let holdShapes = false;
  let holdReplay = false;
  let holdClosedTail = false;
  let closedTailReads = 0;
  let replayReads = 0;
  let replayActive = 0;
  let replayPeak = 0;
  const held = new Map<object, { path: string; replay: boolean; deliver(): void }>();
  const release = (replay: boolean) => {
    for (const entry of [...held.values()]) if (entry.replay === replay) entry.deliver();
  };
  const server = createServer((incoming, outgoing) => {
    const target = new URL(incoming.url ?? "/", upstream);
    const replay =
      incoming.method === "GET" && target.pathname.startsWith("/changes/") && !target.searchParams.has("live");
    if (replay) {
      replayReads += 1;
      replayActive += 1;
      replayPeak = Math.max(replayPeak, replayActive);
    }
    let forwarded: ReturnType<typeof request> | undefined;
    const key = {};
    outgoing.once("close", () => {
      held.delete(key);
      if (replay) replayActive -= 1;
      forwarded?.destroy();
    });
    const deliver = () => {
      held.delete(key);
      if (outgoing.destroyed) return;
      forwarded = request(
        target,
        { method: incoming.method, headers: { ...incoming.headers, host: upstream.host } },
        (response) => {
          const sendResponse = () => {
            held.delete(key);
            if (outgoing.destroyed) return;
            outgoing.writeHead(response.statusCode ?? 502, response.headers);
            response.pipe(outgoing);
          };
          if (
            holdClosedTail &&
            incoming.method === "GET" &&
            target.pathname === "/changes/0" &&
            target.searchParams.has("live") &&
            response.statusCode === 204 &&
            response.headers["stream-closed"] === "true"
          ) {
            closedTailReads += 1;
            held.set(key, { path: target.pathname, replay: false, deliver: sendResponse });
          } else sendResponse();
        },
      );
      forwarded.on("error", (error) => {
        if (outgoing.destroyed) return;
        if (!outgoing.headersSent) outgoing.writeHead(502);
        outgoing.end(String(error));
      });
      incoming.pipe(forwarded);
    };
    if ((holdShapes && incoming.method === "POST" && target.pathname.startsWith("/shape/")) || (holdReplay && replay)) {
      held.set(key, { path: target.pathname, replay, deliver });
    } else deliver();
  });
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("resource proxy did not bind");
  return {
    url: `http://127.0.0.1:${address.port}`,
    shapes: (hold: boolean) => {
      holdShapes = hold;
      if (!hold) release(false);
    },
    replay: (hold: boolean) => {
      holdReplay = hold;
      if (!hold) release(true);
    },
    shapePaths: () => new Set([...held.values()].filter((entry) => !entry.replay).map((entry) => entry.path)),
    replayHeld: () => [...held.values()].filter((entry) => entry.replay).length,
    replayReads: () => replayReads,
    replayPeak: () => replayPeak,
    closedTail: (hold: boolean) => {
      holdClosedTail = hold;
      if (!hold) release(false);
    },
    closedTailReads: () => closedTailReads,
    close: async () => {
      holdShapes = false;
      holdReplay = false;
      holdClosedTail = false;
      release(false);
      release(true);
      server.closeAllConnections();
      await new Promise<void>((resolve, reject) => server.close((error) => (error ? reject(error) : resolve())));
    },
  };
}

let h: Harness | undefined;
let proxy: Awaited<ReturnType<typeof startProxy>>;
afterEach(async () => {
  proxy?.shapes(false);
  proxy?.replay(false);
  proxy?.closedTail(false);
  await h?.shutdown();
  h = undefined;
});

async function boot(engineEnv: Record<string, string> = {}) {
  h = await bootHarness(schema, {
    engineEnv: {
      CIRCUITS_PENDING_MEMORY_BYTES: "16384",
      CIRCUITS_BACKFILL_APPEND_BYTES: "16384",
      CIRCUITS_REPLAY_CONCURRENCY: "1",
      CIRCUITS_SHAPE_IDLE_SECS: "1",
      CIRCUITS_SHAPE_DORMANT_TTL_SECS: "0",
      CIRCUITS_RETENTION_SWEEP_SECS: "1",
      ...engineEnv,
    },
    wrapEngineDs: async (upstream) => (proxy = await startProxy(upstream)),
  });
}

async function memory(): Promise<Memory> {
  const res = await fetch(`${h!.engineUrl}/memory`);
  if (!res.ok) throw new Error(`memory -> ${res.status}`);
  return (await res.json()) as Memory;
}

async function pendingEmpty() {
  await waitFor(async () => {
    const pending = (await memory()).resources.pending;
    return pending.items === 0 && pending.memory_bytes === 0 && pending.spill_bytes === 0;
  }, "pending payloads and spill files to be released");
}

async function purge(shape: ShapeResp) {
  expect((await fetch(`${h!.engineUrl}/shapes/${shape.shapeId}?purge=true`, { method: "DELETE" })).ok).toBe(true);
}

async function park(shape: ShapeResp) {
  expect((await fetch(`${h!.engineUrl}/shapes/${shape.shapeId}`, { method: "DELETE" })).ok).toBe(true);
  await waitFor(async () => {
    const res = await fetch(`${h!.engineUrl}/shapes/${shape.shapeId}`);
    return res.ok && ((await res.json()) as { state: string }).state === "dormant";
  }, "retained plain shape to become dormant");
}

async function assertOracle(shape: ShapeResp, sql = "SELECT id, n, payload FROM items ORDER BY id") {
  const expected = await pgQuery(h!, sql);
  const rows = await foldStream(shape.streamUrl);
  expect([...rows.keys()].sort((a, b) => Number(a) - Number(b))).toEqual(expected.map((row) => String(row.id)));
  for (const row of expected) expect(rows.get(String(row.id))).toMatchObject(row);
}

async function payloadHistory(shape: ShapeResp): Promise<unknown[]> {
  const payloads: unknown[] = [];
  let offset = "-1";
  for (let page = 0; page < 100; page += 1) {
    const res = await fetch(`${shape.streamUrl}?offset=${encodeURIComponent(offset)}`);
    if (res.status === 204) return payloads;
    if (!res.ok) throw new Error(`shape history -> ${res.status}`);
    for (const envelope of (await res.json()) as StreamEnvelope[]) {
      if (envelope.key === "1" && envelope.headers.operation !== "delete") payloads.push(envelope.value?.payload);
    }
    const next = res.headers.get("stream-next-offset");
    if (res.headers.has("stream-up-to-date") || !next || next === offset) return payloads;
    offset = next;
  }
  throw new Error("shape history did not reach its tail");
}

describe("pending payload and dormant replay resource controls", () => {
  it.each([
    { segment: 0, reason: "the current segment is gone" },
    { segment: 1, reason: "the required successor is gone" },
  ])(
    "refuses a zero-range wake at a closed tail when $reason",
    async ({ segment }) => {
      await boot({ CIRCUITS_CHANGES_SEGMENT_BYTES: "4096", CIRCUITS_CHANGES_SEGMENT_SECS: "0" });
      await pgQuery(h!, "INSERT INTO items VALUES (1, 1, 'before')");
      await drainEngine(h!);
      const shape = await createShape(h!, { table: "items" });
      proxy.closedTail(true);
      await pgQuery(h!, "INSERT INTO items VALUES (2, 2, $1)", ["rotate".repeat(2000)]);
      await waitFor(() => proxy.closedTailReads() > 0, "live cursor to reach and remain at the closed segment tail");
      await park(shape);
      const closed = await fetch(`${h!.dsUrl}/changes/0`, { method: "HEAD" });
      expect(closed.headers.get("stream-closed")).toBe("true");
      const cursor = (await fetch(`${h!.engineUrl}/tables/_any/offset`).then((res) => res.json())) as {
        segment: number;
        offset: string;
      };
      expect(cursor).toMatchObject({ segment: 0, offset: closed.headers.get("stream-next-offset") });
      expect((await fetch(`${h!.dsUrl}/changes/${segment}`, { method: "DELETE" })).status).toBe(204);
      const touched = await fetch(`${h!.engineUrl}/shapes/${shape.shapeId}/rows`);
      expect(touched.status).toBe(404);
      expect(await touched.text()).toContain("recreate the subscription");
      expect((await fetch(`${h!.engineUrl}/shapes/${shape.shapeId}`)).status).toBe(404);
      await waitFor(
        async () => (await fetch(shape.streamUrl)).status === 404,
        "unresumable retained stream retirement",
      );
      const catalog = (await fetch(`${h!.dsUrl}/meta/catalog?offset=-1`).then((res) => res.json())) as {
        t: string;
        id?: string;
      }[];
      expect(catalog.some((event) => event.t === "dropped" && event.id === shape.shapeId)).toBe(true);
      expect((await memory()).resources.replay.active).toBe(0);
    },
    60000,
  );

  it("shares a pending RAM budget across slow shape seeds, and releases it over repeated cycles", async () => {
    await boot();
    await pgQuery(h!, "INSERT INTO items SELECT i, i, repeat('s', 4096) FROM generate_series(1, 64) AS i");
    await drainEngine(h!);
    const rss: number[] = [(await memory()).process.rss_bytes];
    const heldCounters: Memory["resources"]["pending"][] = [];
    for (let cycle = 0; cycle < 2; cycle += 1) {
      proxy.shapes(true);
      const plain = createShape(h!, { table: "items", where: { col: "n", op: "gte", value: 0 } });
      const second = createShape(h!, { table: "items", where: { col: "id", op: "gte", value: 0 } });
      // Attach rejection handlers immediately so teardown after a failed assertion cannot leave
      // in-flight creates as unhandled promise rejections.
      void plain.catch(() => {});
      void second.catch(() => {});
      try {
        await waitFor(() => proxy.shapePaths().size === 2, "both snapshots to reach their first append");
        await pgQuery(
          h!,
          "INSERT INTO items SELECT i, i, repeat('w', 4096) FROM generate_series($1::int, $2::int) AS i",
          [1000 + cycle * 128, 1127 + cycle * 128],
        );
        await pgQuery(h!, "UPDATE items SET payload = 'newest' WHERE id = 1");
        await drainEngine(h!);
        const report = await memory();
        expect(report.resources.pending.items).toBeGreaterThan(128);
        expect(report.resources.pending.spill_bytes).toBeGreaterThan(128 * 4096);
        expect(report.resources.pending.memory_bytes).toBeLessThanOrEqual(16384);
        heldCounters.push(report.resources.pending);
        rss.push(report.process.rss_bytes);
      } finally {
        proxy.shapes(false);
      }
      const [shape, other] = await Promise.all([plain, second]);
      await drainEngine(h!);
      await assertOracle(shape);
      await assertOracle(other);
      await pendingEmpty();
      await purge(shape);
      await purge(other);
      rss.push((await memory()).process.rss_bytes);
    }
    // RSS is observed directly, not substituted with retained-payload estimates. Allocator slack,
    // source pages, transaction groups and derived state are outside the pending budget.
    expect(rss.every((value) => value > 0)).toBe(true);
    process.stderr.write(
      `pending cycles (RSS bytes: baseline, held, drained, held, drained): ${JSON.stringify({ rss, heldCounters })}\n`,
    );
  }, 60000);

  it("spills raw inner and outer changes during subquery admission, preserving cancellation and recency", async () => {
    await boot({ CIRCUITS_PENDING_MEMORY_BYTES: "0" });
    await pgQuery(h!, "INSERT INTO members SELECT i, true, repeat('m', 4096) FROM generate_series(1, 64) AS i");
    await pgQuery(h!, "INSERT INTO items SELECT i, i, repeat('s', 4096) FROM generate_series(1, 64) AS i");
    await drainEngine(h!);
    const where = {
      col: "n",
      in: { table: "members", project: "id", where: { col: "active", op: "eq", value: true } },
    };
    for (let cycle = 0; cycle < 2; cycle += 1) {
      proxy.shapes(true);
      const controller = new AbortController();
      const pending = createShape(h!, { table: "items", where }, controller.signal).then(
        (shape) => ({ shape }),
        (error: unknown) => ({ error }),
      );
      try {
        await waitFor(() => proxy.shapePaths().size > 0, "subquery outer snapshot append");
        await pgQuery(h!, "UPDATE members SET active = NOT active, payload = repeat('r', 4096) WHERE id % 2 = 0");
        await pgQuery(h!, "UPDATE items SET payload = repeat('o', 4096) || $1", [String(cycle)]);
        await drainEngine(h!);
        const report = await memory();
        expect(report.resources.pending.memory_bytes).toBe(0);
        expect(report.resources.pending.spill_bytes).toBeGreaterThan(64 * 4096);
        expect(report.resources.pending.items).toBeGreaterThan(0);
        if (cycle === 0) {
          controller.abort();
          expect(await pending).toHaveProperty("error");
        }
      } finally {
        proxy.shapes(false);
      }
      if (cycle === 1) {
        const outcome = await pending;
        if (!("shape" in outcome)) throw outcome.error;
        await drainEngine(h!);
        await assertOracle(
          outcome.shape,
          "SELECT i.id, i.n, i.payload FROM items i JOIN members m ON i.n = m.id WHERE m.active ORDER BY i.id",
        );
        await purge(outcome.shape);
      }
      await pendingEmpty();
      await waitFor(async () => {
        const nodes = (await fetch(`${h!.engineUrl}/subqueries`).then((res) => res.json())) as { nodes: unknown[] };
        return nodes.nodes.length === 0;
      }, "cancelled or purged membership contributors to disappear");
    }
  }, 60000);

  it("admits one replay, coalesces touches, and cancels both queued and scanning purges without resurrection", async () => {
    await boot({ CIRCUITS_CHANGES_APPEND_BYTES: "65536", CIRCUITS_CHANGES_SEGMENT_BYTES: "131072" });
    await pgQuery(h!, "INSERT INTO items VALUES (1, 1, 'before')");
    await drainEngine(h!);
    const shapes: ShapeResp[] = [];
    for (let n = 0; n < 3; n += 1) {
      shapes.push(await createShape(h!, { table: "items", where: { col: "n", op: "gte", value: n } }));
    }
    await Promise.all(shapes.map(park));
    await pgQuery(h!, "INSERT INTO items SELECT i, i, repeat('d', 4096) FROM generate_series(2, 64) AS i");
    await drainEngine(h!);
    proxy.replay(true);
    const reads: Promise<Response>[] = [];
    const touch = (shape: ShapeResp) => fetch(`${h!.engineUrl}/shapes/${shape.shapeId}/rows`);
    reads.push(touch(shapes[0]!));
    try {
      await waitFor(() => proxy.replayHeld() === 1, "first replay GET to block at storage");
      reads.push(touch(shapes[0]!), touch(shapes[0]!), touch(shapes[1]!), touch(shapes[2]!));
      await waitFor(async () => (await memory()).resources.replay.queued === 2, "two distinct wakes to queue");
      const admitted = await memory();
      expect(admitted.resources.replay.active).toBe(1);
      expect(admitted.resources.pending.items).toBe(0);
      expect(proxy.replayReads()).toBe(1);
      await purge(shapes[1]!);
      await waitFor(
        async () => (await memory()).resources.replay.queued === 1,
        "queued purge to release its admission wait",
      );
      await purge(shapes[0]!);
      await waitFor(
        () => proxy.replayReads() === 2 && proxy.replayHeld() === 1,
        "scanning purge to free the permit for the surviving wake",
      );
    } finally {
      proxy.replay(false);
    }
    const responses = await Promise.all(reads);
    expect(responses.slice(0, 4).map((response) => response.status)).toEqual([503, 503, 503, 503]);
    expect(responses[4]!.status).toBe(200);
    for (const response of responses) await response.text();
    await drainEngine(h!);
    await assertOracle(shapes[2]!, "SELECT id, n, payload FROM items WHERE n >= 2 ORDER BY id");
    await waitFor(async () => {
      const replay = (await memory()).resources.replay;
      return replay.active === 0 && replay.queued === 0;
    }, "all replay permits to be returned");
    const report = await memory();
    expect(report.resources.replay.peak).toBe(1);
    expect(report.resources.replay.pages).toBeGreaterThan(0);
    expect(report.resources.replay.bytes).toBeGreaterThan(0);
    expect(proxy.replayPeak()).toBe(1);
    for (const shape of shapes.slice(0, 2))
      expect((await fetch(`${h!.engineUrl}/shapes/${shape.shapeId}`)).status).toBe(404);
    await pendingEmpty();
  }, 60000);

  it("finishes a fixed-horizon replay while writers continue and clips the crossing page before draining newer changes", async () => {
    await boot();
    await pgQuery(h!, "INSERT INTO items VALUES (1, 1, 'before')");
    await drainEngine(h!);
    const shape = await createShape(h!, { table: "items" });
    const rss: number[] = [(await memory()).process.rss_bytes];
    for (let cycle = 0; cycle < 2; cycle += 1) {
      await park(shape);
      await pgQuery(h!, "UPDATE items SET payload = $1 WHERE id = 1", [`dormant-${cycle}`]);
      await drainEngine(h!);
      proxy.replay(true);
      const waking = fetch(`${h!.engineUrl}/shapes/${shape.shapeId}/rows`);
      let writing = true;
      let writer: Promise<void> | undefined;
      try {
        await waitFor(() => proxy.replayHeld() === 1, "fixed horizon to be captured before the storage read");
        // These updates fall after admission. The delayed ordinary GET will now return a page
        // crossing the horizon. Applying that whole page and then older pending deltas reverses
        // same-key recency; exact prefix clipping must leave them to the pending drain.
        await pgQuery(h!, "UPDATE items SET payload = $1 WHERE id = 1", [`after-admission-${cycle}`]);
        await drainEngine(h!);
        writer = (async () => {
          let n = 0;
          while (writing) {
            await pgQuery(h!, "UPDATE items SET payload = $1 WHERE id = 1", [`writer-${cycle}-${n++}`]);
            await sleep(10);
          }
        })();
        proxy.replay(false);
        const response = await Promise.race([
          waking,
          sleep(10000).then(() => {
            throw new Error("replay chased a continuously growing log instead of its captured endpoint");
          }),
        ]);
        expect(response.status).toBe(200);
        await response.text();
        rss.push((await memory()).process.rss_bytes);
      } finally {
        writing = false;
        proxy.replay(false);
        await writer;
      }
      await drainEngine(h!);
      await assertOracle(shape);
      const history = await payloadHistory(shape);
      expect(history.filter((value) => value === `after-admission-${cycle}`)).toHaveLength(1);
      expect(history.indexOf(`dormant-${cycle}`)).toBeLessThan(history.indexOf(`after-admission-${cycle}`));
      let previous = -1;
      for (const value of history.slice(history.indexOf(`dormant-${cycle}`))) {
        const writerMatch = typeof value === "string" ? value.match(new RegExp(`^writer-${cycle}-(\\d+)$`)) : null;
        const rank =
          value === `dormant-${cycle}`
            ? 0
            : value === `after-admission-${cycle}`
              ? 1
              : writerMatch
                ? 2 + Number(writerMatch[1])
                : -1;
        expect(rank, `same-key stream order regressed at ${String(value)}`).toBeGreaterThan(previous);
        previous = rank;
      }
      await pendingEmpty();
    }
    expect((await memory()).resources.replay.peak).toBe(1);
    expect(rss.every((value) => value > 0)).toBe(true);
    process.stderr.write(
      `replay cycles (RSS bytes: baseline, wake, wake): ${JSON.stringify({ rss, replay: (await memory()).resources.replay })}\n`,
    );
  }, 60000);
});
