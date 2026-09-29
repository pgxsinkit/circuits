// ADR-0007 makes retirement mandatory — a shape the engine has removed must not keep a live public
// stream — but a retirement storage refuses is only half-done, and the engine has by then already
// forgotten the shape. So the promise has to outlive the process that made it: the `Dropped` record
// is the durable intent, and a boot that finds one with no `Retired` after it finishes the job with
// no client involved at all.
//
// This is the crash half of `conformance-catalog-durability.test.ts` (which covers the same-process
// retry). It uses the same ordinary HTTP proxy in front of durable-streams and no engine internals:
// SQL, the native control plane, `GET /metrics`, and a SIGKILL.

import { createServer, request } from "node:http";

import type { Schema } from "@circuits/protocol";
import { afterEach, describe, expect, it } from "vitest";

import { createShape, foldStream, pgQuery, waitFor } from "./engine-native.js";
import { bootHarness, drainEngine, type Harness } from "./harness.js";

const schema: Schema = {
  tables: {
    items: { columns: { id: { type: "int" }, n: { type: "int" } }, primaryKey: "id" },
  },
};

interface RetirementFaultProxy {
  url: string;
  failedRetirements(): number;
  failRetirements(fail: boolean): void;
  close(): Promise<void>;
}

/** Reject exactly the requests a retirement makes (`POST`/`DELETE` on `shape/*`), pass the rest. */
async function startRetirementFaultProxy(upstreamUrl: string): Promise<RetirementFaultProxy> {
  let fail = false;
  let failures = 0;
  const upstream = new URL(upstreamUrl);
  const server = createServer((incoming, outgoing) => {
    const target = new URL(incoming.url ?? "/", upstream);
    // A retirement is a close (`POST` + `stream-closed`) and a `DELETE`, and nothing else: an
    // ordinary append is a bare `POST`, and it must keep working, or "retirements fail" would also
    // mean "no shape can be created", which is a different fault.
    const closing = String(incoming.headers["stream-closed"] ?? "").toLowerCase() === "true";
    if (fail && target.pathname.startsWith("/shape/") && (incoming.method === "DELETE" || closing)) {
      failures += 1;
      incoming.resume();
      outgoing.writeHead(503, { "content-type": "text/plain" });
      outgoing.end("shape retirement temporarily unavailable");
      return;
    }
    const forwarded = request(
      target,
      { method: incoming.method, headers: { ...incoming.headers, host: upstream.host } },
      (response) => {
        outgoing.writeHead(response.statusCode ?? 502, response.headers);
        response.pipe(outgoing);
      },
    );
    forwarded.on("error", (error) => {
      if (!outgoing.headersSent) outgoing.writeHead(502, { "content-type": "text/plain" });
      outgoing.end(String(error));
    });
    incoming.pipe(forwarded);
  });

  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => resolve());
  });
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("retirement fault proxy did not bind TCP");

  return {
    url: `http://127.0.0.1:${address.port}`,
    failedRetirements: () => failures,
    failRetirements: (next) => {
      fail = next;
    },
    close: () => new Promise<void>((resolve, reject) => server.close((error) => (error ? reject(error) : resolve()))),
  };
}

async function pendingRetirements(engineUrl: string): Promise<number> {
  const res = await fetch(`${engineUrl}/metrics`);
  if (!res.ok) return -1;
  const body = (await res.json()) as { gauges?: { retirements_pending?: number } };
  return body.gauges?.retirements_pending ?? -1;
}

let h: Harness | undefined;
afterEach(async () => {
  await h?.shutdown();
  h = undefined;
});

describe("a retirement outlives the process that owed it", () => {
  it("finishes a pending retirement after a crash, from the catalog alone", async () => {
    let proxy: RetirementFaultProxy | undefined;
    h = await bootHarness(schema, {
      wrapEngineDs: async (upstreamUrl) => {
        proxy = await startRetirementFaultProxy(upstreamUrl);
        return proxy;
      },
    });

    await pgQuery(h, "INSERT INTO items (id, n) VALUES (1, 10)");
    await drainEngine(h);
    const shape = await createShape(h, { table: "items" });

    // The mandatory retirement (ADR-0005 `TRUNCATE` → retire every dependent) cannot complete.
    proxy!.failRetirements(true);
    await pgQuery(h, "TRUNCATE items");
    await waitFor(
      () => Promise.resolve(proxy!.failedRetirements() >= 2),
      "the proxy to reject both the close and the delete",
    );
    await waitFor(
      async () => (await fetch(`${h!.engineUrl}/shapes/${shape.shapeId}`)).status === 404,
      "the engine to forget the retired shape",
    );

    // SIGKILL: whatever the running engine still had queued is gone, and the shape record with it.
    // Only the durable catalog remembers that this stream owes a retirement.
    await h.restartEngine();
    expect(
      (await fetch(shape.streamUrl)).status,
      "the stream is still there — the restart alone must not be what deletes it",
    ).not.toBe(404);
    await waitFor(
      async () => (await pendingRetirements(h!.engineUrl)) >= 1,
      "the fresh boot to pick the unfinished retirement out of the catalog",
    );

    // Recovery is entirely external, and no client ever asks for anything.
    proxy!.failRetirements(false);
    await waitFor(async () => {
      const response = await fetch(shape.streamUrl);
      return response.status === 404 || response.status === 410;
    }, "the orphaned stream to be retired by the restarted engine");
    await waitFor(
      async () => (await pendingRetirements(h!.engineUrl)) === 0,
      "the retirement queue to report itself empty",
    );

    // ...and the completion is recorded, so the NEXT boot has nothing left to do.
    await h.restartEngine();
    await new Promise((resolve) => setTimeout(resolve, 500));
    expect(await pendingRetirements(h.engineUrl), "a completed retirement is not re-queued forever").toBe(0);
  }, 90000);

  it("never re-mints the id of a shape whose stream is still being retired", async () => {
    // The id is not free the moment the record goes: `shape/sN` is still there until the retirement
    // lands. Re-minting it would hand the new shape the dead one's stream — `ensure_stream` is
    // idempotent, so the PUT succeeds, the backfill appends alongside rows the new shape's predicate
    // never matched (pre-`TRUNCATE` rows: ADR-0005 violated), and the pending retirement would then
    // close and delete the LIVE shape's stream out from under the engine.
    let proxy: RetirementFaultProxy | undefined;
    h = await bootHarness(schema, {
      wrapEngineDs: async (upstreamUrl) => {
        proxy = await startRetirementFaultProxy(upstreamUrl);
        return proxy;
      },
    });

    await pgQuery(h, "INSERT INTO items (id, n) VALUES (1, 10)");
    await drainEngine(h);
    const old = await createShape(h, { table: "items" });
    expect((await foldStream(old.streamUrl)).has("1")).toBe(true);

    proxy!.failRetirements(true);
    await pgQuery(h, "TRUNCATE items");
    await waitFor(
      () => Promise.resolve(proxy!.failedRetirements() >= 2),
      "the proxy to reject both the close and the delete",
    );
    await waitFor(
      async () => (await fetch(`${h!.engineUrl}/shapes/${old.shapeId}`)).status === 404,
      "the engine to forget the retired shape",
    );

    // Crash and come back with the retirement still owed, then let a client recreate its shape —
    // the exact moment the id counter has to remember what the records no longer do.
    await h.restartEngine();
    await waitFor(
      async () => (await pendingRetirements(h!.engineUrl)) >= 1,
      "the fresh boot to pick the unfinished retirement out of the catalog",
    );
    await pgQuery(h, "INSERT INTO items (id, n) VALUES (7, 70)");
    await drainEngine(h);
    const fresh = await createShape(h, { table: "items" });

    expect(fresh.shapeId, "a new shape must not take the id of one still being retired").not.toBe(old.shapeId);
    expect(fresh.streamUrl, "nor its stream").not.toBe(old.streamUrl);
    expect(
      [...(await foldStream(fresh.streamUrl)).keys()].sort(),
      "the new stream carries only what the table holds now",
    ).toEqual(["7"]);

    // Storage recovers: the queued retirement must take the OLD stream and leave the live one alone.
    proxy!.failRetirements(false);
    await waitFor(async () => {
      const response = await fetch(old.streamUrl);
      return response.status === 404 || response.status === 410;
    }, "the orphaned stream to be retired");
    await waitFor(
      async () => (await pendingRetirements(h!.engineUrl)) === 0,
      "the retirement queue to report itself empty",
    );

    expect((await fetch(`${h.engineUrl}/shapes/${fresh.shapeId}`)).status, "the live shape is untouched").toBe(200);
    await pgQuery(h, "INSERT INTO items (id, n) VALUES (8, 80)");
    await drainEngine(h);
    await waitFor(
      async () => (await foldStream(fresh.streamUrl)).has("8"),
      "the live shape to keep receiving changes after the retirement landed",
    );
  }, 90000);
});
