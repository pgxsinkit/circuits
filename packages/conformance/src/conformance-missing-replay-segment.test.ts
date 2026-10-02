// Losing an old change-log segment makes a retained shape unresumable. A provisional joining
// subscription cannot veto its retirement, and a proxy's false status cannot prove real loss.
import { createServer, request } from "node:http";

import type { Schema } from "@circuits/protocol";
import { afterEach, describe, expect, it } from "vitest";

import { createShape, foldStream, pgQuery, type ShapeResp, waitFor } from "./engine-native.js";
import { bootHarness, drainEngine, type Harness } from "./harness.js";

const schema: Schema = {
  tables: {
    items: { columns: { id: { type: "int" }, n: { type: "int" }, payload: { type: "text" } }, primaryKey: "id" },
  },
};
const predicate = { col: "n", op: "gte", value: 10 };

interface ReplayFaultProxy {
  url: string;
  failReplay(path: string, status: number, headStatus?: number): void;
  blockRetirement(path?: string): void;
  readHits(): number;
  headHits(): number;
  retirementHits(): number;
  holdClosedTail(path?: string): void;
  heldTailReads(): number;
  close(): Promise<void>;
}

async function startReplayFaultProxy(upstreamUrl: string): Promise<ReplayFaultProxy> {
  const upstream = new URL(upstreamUrl);
  let readPath: string | undefined;
  let readStatus = 503;
  let readFailures = 0;
  let headStatus = 503;
  let headFailures = 0;
  let retiredPath: string | undefined;
  let reads = 0;
  let heads = 0;
  let retirements = 0;
  let heldTailPath: string | undefined;
  let heldTails = 0;
  let tailWaiters: Array<() => void> = [];
  const releaseTailReads = () => {
    const waiters = tailWaiters;
    tailWaiters = [];
    for (const deliver of waiters) deliver();
  };
  const server = createServer((incoming, outgoing) => {
    const target = new URL(incoming.url ?? "/", upstream);
    let status: number | undefined;
    if (
      target.pathname === readPath &&
      incoming.method === "GET" &&
      !target.searchParams.has("live") &&
      readFailures > 0
    ) {
      readFailures -= 1;
      reads += 1;
      status = readStatus;
    } else if (target.pathname === readPath && incoming.method === "HEAD" && headFailures > 0) {
      headFailures -= 1;
      heads += 1;
      status = headStatus;
    } else if (
      target.pathname === retiredPath &&
      (incoming.method === "DELETE" || String(incoming.headers["stream-closed"] ?? "") === "true")
    ) {
      retirements += 1;
      status = 503;
    }
    if (status !== undefined) {
      incoming.resume();
      outgoing.writeHead(status, { "content-type": "text/plain" });
      outgoing.end("injected replay or retirement response");
      return;
    }
    const forwarded = request(
      target,
      { method: incoming.method, headers: { ...incoming.headers, host: upstream.host } },
      (response) => {
        const deliver = () => {
          outgoing.writeHead(response.statusCode ?? 502, response.headers);
          response.pipe(outgoing);
        };
        // Hold the empty live read after the rotation pointer landed. The sequencer has
        // published the closed segment's tail and can process DeactivateShape while this
        // response waits, giving the shape a legitimate saved cursor beyond the pointer.
        if (
          target.pathname === heldTailPath &&
          incoming.method === "GET" &&
          target.searchParams.has("live") &&
          response.statusCode === 204 &&
          response.headers["stream-closed"] === "true"
        ) {
          heldTails += 1;
          tailWaiters.push(deliver);
        } else deliver();
      },
    );
    forwarded.on("error", (error) => {
      if (!outgoing.headersSent) outgoing.writeHead(502);
      outgoing.end(String(error));
    });
    incoming.pipe(forwarded);
  });
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("replay proxy did not bind");
  return {
    url: `http://127.0.0.1:${address.port}`,
    failReplay: (path, status, confirmationStatus) => {
      readPath = `/${path}`;
      readStatus = status;
      readFailures = 1;
      headStatus = confirmationStatus ?? 503;
      headFailures = confirmationStatus === undefined ? 0 : 3;
    },
    blockRetirement: (path) => {
      retiredPath = path ? `/${path}` : undefined;
    },
    readHits: () => reads,
    headHits: () => heads,
    retirementHits: () => retirements,
    holdClosedTail: (path) => {
      heldTailPath = path ? `/${path}` : undefined;
      if (!path) releaseTailReads();
    },
    heldTailReads: () => heldTails,
    close: async () => {
      heldTailPath = undefined;
      releaseTailReads();
      await new Promise<void>((resolve, reject) => server.close((error) => (error ? reject(error) : resolve())));
    },
  };
}

interface CatalogEvent {
  t: string;
  id?: string;
  resume?: { segment: number; offset: string };
  pos?: { segment: number; offset: string };
}

async function catalog(h: Harness): Promise<CatalogEvent[]> {
  const events: CatalogEvent[] = [];
  let offset = "-1";
  for (let page = 0; page < 100; page += 1) {
    const res = await fetch(`${h.dsUrl}/meta/catalog?offset=${encodeURIComponent(offset)}`);
    if (res.status === 204) return events;
    if (!res.ok) throw new Error(`catalog read -> ${res.status}`);
    events.push(...((await res.json()) as CatalogEvent[]));
    const next = res.headers.get("stream-next-offset");
    if (res.headers.has("stream-up-to-date") || !next || next === offset) return events;
    offset = next;
  }
  throw new Error("catalog did not reach its tail");
}

let h: Harness | undefined;
let proxy: ReplayFaultProxy;
afterEach(async () => {
  await h?.shutdown();
  h = undefined;
});

async function bootReplayHarness(): Promise<void> {
  h = await bootHarness(schema, {
    engineEnv: {
      CIRCUITS_SHAPE_IDLE_SECS: "1",
      CIRCUITS_SHAPE_DORMANT_TTL_SECS: "0",
      CIRCUITS_RETENTION_SWEEP_SECS: "1",
      CIRCUITS_CHANGES_SEGMENT_BYTES: "4096",
      CIRCUITS_CHANGES_SEGMENT_SECS: "0",
      CIRCUITS_CHANGES_RETAIN_SECS: "0",
    },
    wrapEngineDs: async (upstream) => (proxy = await startReplayFaultProxy(upstream)),
  });
  await pgQuery(h, "INSERT INTO items (id, n, payload) VALUES (1, 10, 'before')");
  await drainEngine(h);
}

async function parkShape(shape: ShapeResp): Promise<{ segment: number; offset: string }> {
  const released = await fetch(`${h!.engineUrl}/shapes/${shape.shapeId}?subscription=creator`, { method: "DELETE" });
  expect(released.status).toBe(200);
  await waitFor(async () => {
    const res = await fetch(`${h!.engineUrl}/shapes/${shape.shapeId}`);
    return res.ok && ((await res.json()) as { state: string }).state === "dormant";
  }, "the shape to become dormant");
  await waitFor(
    async () => (await catalog(h!)).some((event) => event.t === "dormant" && event.id === shape.shapeId),
    "the dormant resume position to become durable",
  );
  return (await catalog(h!)).findLast((event) => event.t === "dormant" && event.id === shape.shapeId)!.resume!;
}

async function dormantShape(): Promise<{ shape: ShapeResp; segment: number }> {
  await bootReplayHarness();
  const shape = await createShape(h!, { table: "items", where: predicate, subscription: "creator" });
  const resume = await parkShape(shape);
  return { shape, segment: resume.segment };
}

async function rotatePast(segment: number): Promise<void> {
  await pgQuery(h!, "INSERT INTO items (id, n, payload) VALUES (2, 20, $1)", ["large".repeat(2000)]);
  await drainEngine(h!);
  // A restart must checkpoint on the successor before the old segment is deliberately removed.
  await waitFor(
    async () => (await catalog(h!)).some((event) => event.t === "offset" && event.pos!.segment > segment),
    "the sequencer checkpoint to pass the dormant segment",
  );
}

describe("a dormant shape's missing replay segment", () => {
  it("retires a cursor at a closed segment tail when its successor was lost", async () => {
    await bootReplayHarness();
    const shape = await createShape(h!, { table: "items", where: predicate, subscription: "creator" });
    proxy.holdClosedTail("changes/0");
    await pgQuery(h!, "INSERT INTO items (id, n, payload) VALUES (2, 20, $1)", ["rotate".repeat(2000)]);
    await waitFor(async () => proxy.heldTailReads() > 0, "the sequencer to publish the closed segment's tail");
    const resume = await parkShape(shape);
    expect(resume.segment).toBe(0);
    const closed = await fetch(`${h!.dsUrl}/changes/0`, { method: "HEAD" });
    expect(closed.headers.get("stream-closed")).toBe("true");
    expect(resume.offset).toBe(closed.headers.get("stream-next-offset"));
    // The replay cursor is after the pointer, so it must derive changes/1 from the closed
    // tail. Let the live sequencer advance, then rotate again and checkpoint beyond changes/1.
    proxy.holdClosedTail();
    await drainEngine(h!);
    await pgQuery(h!, "INSERT INTO items (id, n, payload) VALUES (3, 30, $1)", ["rotate again".repeat(1000)]);
    await drainEngine(h!);
    await waitFor(
      async () => (await catalog(h!)).some((event) => event.t === "offset" && event.pos!.segment > 1),
      "the live checkpoint to pass the successor",
    );
    expect((await fetch(`${h!.dsUrl}/changes/1`, { method: "HEAD" })).status).toBe(200);
    expect((await fetch(`${h!.dsUrl}/changes/1`, { method: "DELETE" })).status).toBe(204);

    const replacement = await createShape(h!, { table: "items", where: predicate, subscription: "rejoining" });
    expect(replacement.shapeId).not.toBe(shape.shapeId);
    expect((await fetch(`${h!.engineUrl}/shapes/${shape.shapeId}`)).status).toBe(404);
    expect((await catalog(h!)).some((event) => event.t === "dropped" && event.id === shape.shapeId)).toBe(true);
    await waitFor(async () => (await fetch(shape.streamUrl)).status === 404, "the unresumable stream to retire");
    expect([...(await foldStream(replacement.streamUrl)).keys()].sort()).toEqual(["1", "2", "3"]);
  }, 60000);

  it("retires despite a provisional joining claim and finishes retirement after a crash", async () => {
    const { shape, segment } = await dormantShape();
    await rotatePast(segment);
    const keptDefinition = { table: "items", where: { col: "id", op: "gte", value: 0 }, subscription: "kept" };
    const kept = await createShape(h!, keptDefinition);
    expect((await fetch(`${h!.dsUrl}/changes/${segment}`, { method: "DELETE" })).status).toBe(204);
    expect((await fetch(`${h!.dsUrl}/changes/${segment}`, { method: "HEAD" })).status).toBe(404);
    proxy.blockRetirement(shape.streamPath);

    const replacement = await createShape(h!, { table: "items", where: predicate, subscription: "rejoining" });
    expect(replacement.shapeId).not.toBe(shape.shapeId);
    expect((await fetch(`${h!.engineUrl}/shapes/${shape.shapeId}`)).status).toBe(404);
    expect((await catalog(h!)).some((event) => event.t === "dropped" && event.id === shape.shapeId)).toBe(true);
    await waitFor(async () => proxy.retirementHits() > 0, "the detached retirement to attempt storage cleanup");
    expect((await fetch(shape.streamUrl)).status, "failed cleanup remains an explicit durable retirement debt").toBe(
      200,
    );

    await h!.restartEngine();
    expect((await fetch(`${h!.engineUrl}/shapes/${shape.shapeId}`)).status).toBe(404);
    expect((await createShape(h!, keptDefinition)).shapeId).toBe(kept.shapeId);
    await pgQuery(h!, "INSERT INTO items (id, n, payload) VALUES (3, 30, 'after restart')");
    await drainEngine(h!);
    expect([...(await foldStream(kept.streamUrl)).keys()].sort()).toEqual(["1", "2", "3"]);

    proxy.blockRetirement();
    await waitFor(async () => (await fetch(shape.streamUrl)).status === 404, "the old stream retirement to complete");
    await waitFor(
      async () => (await catalog(h!)).some((event) => event.t === "retired" && event.id === shape.shapeId),
      "retirement completion to become durable",
    );
    expect((await createShape(h!, { table: "items", where: predicate, subscription: "rejoining" })).shapeId).toBe(
      replacement.shapeId,
    );
  }, 90000);

  it("returns a terminal read error after durably retiring a shape with missing history", async () => {
    const { shape, segment } = await dormantShape();
    await rotatePast(segment);
    expect((await fetch(`${h!.dsUrl}/changes/${segment}`, { method: "DELETE" })).status).toBe(204);
    const read = await fetch(`${h!.engineUrl}/shapes/${shape.shapeId}/rows`);
    expect(read.status).toBe(404);
    expect(await read.text()).toContain("recreate the subscription");
    expect((await fetch(`${h!.engineUrl}/shapes/${shape.shapeId}`)).status).toBe(404);
    expect((await catalog(h!)).some((event) => event.t === "dropped" && event.id === shape.shapeId)).toBe(true);
    await waitFor(async () => (await fetch(shape.streamUrl)).status === 404, "the retired stream to disappear");
  }, 60000);

  it.each([
    { name: "a false replay 404 with storage confirming the segment exists", status: 404, headStatus: undefined },
    { name: "a transient replay 503", status: 503, headStatus: undefined },
    { name: "a replay 404 whose confirming HEADs are unavailable", status: 404, headStatus: 503 },
  ])(
    "preserves the dormant shape after $name",
    async ({ status, headStatus }) => {
      const { shape, segment } = await dormantShape();
      await pgQuery(h!, "INSERT INTO items (id, n, payload) VALUES (2, 20, 'while dormant')");
      await drainEngine(h!);
      proxy.failReplay(`changes/${segment}`, status, headStatus);
      const refused = await fetch(`${h!.engineUrl}/shapes`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ table: "items", where: predicate, subscription: "rejoining" }),
      });
      expect(refused.status).toBe(503);
      expect(refused.headers.get("retry-after")).toBe("1");
      expect(await refused.text()).toContain("reactivation is unavailable");
      expect(proxy.readHits()).toBe(1);
      expect(proxy.headHits()).toBe(headStatus === undefined ? 0 : 3);
      const retained = await fetch(`${h!.engineUrl}/shapes/${shape.shapeId}`);
      expect(retained.status).toBe(200);
      expect(((await retained.json()) as { state: string }).state).toBe("dormant");
      expect((await fetch(shape.streamUrl, { method: "HEAD" })).status).toBe(200);
      expect((await catalog(h!)).some((event) => event.t === "dropped" && event.id === shape.shapeId)).toBe(false);

      // The failed join gave its provisional claim back; the same subscription retries onto the
      // same retained handle once storage answers normally, including the missed live change.
      const retried = await createShape(h!, { table: "items", where: predicate, subscription: "rejoining" });
      expect(retried.shapeId).toBe(shape.shapeId);
      expect([...(await foldStream(shape.streamUrl)).keys()].sort()).toEqual(["1", "2"]);
    },
    60000,
  );
});
