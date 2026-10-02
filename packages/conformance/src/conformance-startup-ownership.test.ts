// A second process on an occupied replication slot must wait before startup writes. Once the
// owner leaves, the successor reads the current durable catalog, including changes during the wait.
import { createServer, request } from "node:http";

import { DurableStreamTestServer } from "@circuits/ds-rust";
import type { Schema } from "@circuits/protocol";
import { afterEach, describe, expect, it } from "vitest";

import { createShape, foldStream, pgQuery, waitFor } from "./engine-native.js";
import { bootHarness, drainEngine, spawnRawEngine, type Harness, type RawEngine } from "./harness.js";

const schema: Schema = {
  tables: { items: { columns: { id: { type: "int" }, n: { type: "int" } }, primaryKey: "id" } },
};
let h: Harness | undefined;
let second: RawEngine | undefined;
let coldStore: DurableStreamTestServer | undefined;
let proxy: { url: string; writes: string[]; close(): Promise<void> } | undefined;

afterEach(async () => {
  if (second) {
    second.signal("SIGKILL");
    await second.waitForExit().catch(() => {});
    second = undefined;
  }
  await proxy?.close();
  proxy = undefined;
  await coldStore?.stop();
  coldStore = undefined;
  await h?.shutdown();
  h = undefined;
});

async function observeStorage(upstreamUrl: string) {
  const upstream = new URL(upstreamUrl);
  const writes: string[] = [];
  const server = createServer((incoming, outgoing) => {
    if (!["GET", "HEAD"].includes(incoming.method ?? "GET")) writes.push(`${incoming.method} ${incoming.url}`);
    const forward = request(
      new URL(incoming.url ?? "/", upstream),
      {
        method: incoming.method,
        headers: { ...incoming.headers, host: upstream.host },
      },
      (response) => {
        outgoing.writeHead(response.statusCode ?? 502, response.headers);
        response.pipe(outgoing);
      },
    );
    forward.on("error", (error) => {
      if (!outgoing.headersSent) outgoing.writeHead(502);
      outgoing.end(String(error));
    });
    incoming.pipe(forward);
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("storage observer did not bind TCP");
  return {
    url: `http://127.0.0.1:${address.port}`,
    writes,
    close: () =>
      new Promise<void>((resolve) => {
        server.close(() => resolve());
        server.closeAllConnections();
      }),
  };
}

async function startSecond(dsUrl = h!.dsUrl): Promise<string> {
  proxy = await observeStorage(dsUrl);
  second = spawnRawEngine({
    CIRCUITS_DS_URL: proxy.url,
    CIRCUITS_BIND: "127.0.0.1:0",
    CIRCUITS_LOG: "info",
    CIRCUITS_PG_URL: h!.pgUrl,
    CIRCUITS_PG_TABLES: "*",
    CIRCUITS_PG_SLOT: h!.slot,
    CIRCUITS_SHUTDOWN_GRACE_SECS: "5",
    CIRCUITS_SHUTDOWN_DRAIN_SECS: "0",
  });
  const url = await second.waitForBinding();
  // Named process state orders the observation: the second process has encountered the owner.
  await waitFor(
    () => second!.stderr().includes("another engine is on this slot"),
    "second engine's occupied-slot wait",
  );
  return url;
}

async function expectWaiting(url: string): Promise<void> {
  const ready = await fetch(`${url}/ready`);
  expect(ready.status).toBe(503);
  const create = await fetch(`${url}/shapes`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ table: "items" }),
  });
  expect(create.status).toBe(503);
  expect(create.headers.get("retry-after")).toBe("1");
  expect(proxy!.writes).toEqual([]);
  const [identity] = await pgQuery(
    h!,
    "SELECT relreplident::text AS identity FROM pg_class WHERE oid = 'newcomer'::regclass",
  );
  expect(identity?.identity).toBe("d");
}

describe("startup waits for replication-slot ownership", () => {
  it("waits without changing storage or replica identity, then restores a shape the owner created while waiting", async () => {
    h = await bootHarness(schema);
    await pgQuery(h, "CREATE TABLE newcomer (id int PRIMARY KEY, n int)");
    await drainEngine(h);
    const url = await startSecond();
    await expectWaiting(url);

    const late = await createShape(h, { table: "items", where: { col: "n", op: "gte", value: 0 } });
    await pgQuery(h, "CREATE TABLE later_table (id int PRIMARY KEY, n int)");
    await pgQuery(h, "INSERT INTO items VALUES (1, 10)");
    await drainEngine(h);
    await expectWaiting(url);

    h.signalEngine();
    expect((await h.waitForEngineExit()).code).toBe(0);
    await second!.waitForListening();
    expect((await fetch(`${url}/shapes/${late.shapeId}`)).status).toBe(200);
    h.engineUrl = url;
    await createShape(h, { table: "later_table" });
    await pgQuery(h, "UPDATE items SET n = 20 WHERE id = 1");
    await drainEngine(h);
    expect([...(await foldStream(late.streamUrl)).values()]).toEqual([{ id: 1, n: 20 }]);
    const [identity] = await pgQuery(
      h,
      "SELECT relreplident::text AS identity FROM pg_class WHERE oid = 'newcomer'::regclass",
    );
    expect(identity?.identity).toBe("f");
  });

  it("an engine with an empty catalog waits on the active slot and shuts down without binding or writing", async () => {
    h = await bootHarness(schema);
    await pgQuery(h, "CREATE TABLE newcomer (id int PRIMARY KEY, n int)");
    coldStore = new DurableStreamTestServer({ port: 0 });
    const coldUrl = await coldStore.start();
    const url = await startSecond(coldUrl);
    await expectWaiting(url);
    expect((await fetch(`${coldUrl}/meta/catalog`, { method: "HEAD" })).status).toBe(404);
    expect((await fetch(`${coldUrl}/changes/0`, { method: "HEAD" })).status).toBe(404);

    second!.signal();
    expect((await second!.waitForExit(4000)).code).toBe(0);
    expect(proxy!.writes).toEqual([]);
    expect((await fetch(`${h.engineUrl}/ready`)).status).toBe(200);
  });
});
