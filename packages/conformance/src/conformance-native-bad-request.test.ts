// Resolved-schema mistakes are caller errors. Refusing them must not leave an acknowledged
// subscription, a shape record, or a shape stream; a schema that is still resolving is retryable.
import type { Schema } from "@circuits/protocol";
import { afterEach, describe, expect, it } from "vitest";

import { createShape, foldStream, lockTable, pgQuery, tableLockWaiters, waitFor } from "./engine-native.js";
import { bootHarness, drainEngine, type Harness } from "./harness.js";

const schema: Schema = {
  tables: {
    items: { columns: { id: { type: "int" }, n: { type: "int" } }, primaryKey: "id" },
  },
};
let h: Harness | undefined;
afterEach(async () => {
  await h?.shutdown();
  h = undefined;
});

async function post(body: unknown): Promise<Response> {
  return fetch(`${h!.engineUrl}/shapes`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
}

describe("native: schema request errors", () => {
  it("refuses unknown tables and columns with 400, without leaving shapes or streams", async () => {
    h = await bootHarness(schema);
    const invalid = [
      { body: { table: "missing" }, error: "unknown table 'public.missing'" },
      { body: { table: "items", columns: ["id", "missing"] }, error: "unknown column 'missing'" },
      {
        body: { table: "items", where: { col: "id", in: { table: "missing", project: "id" } } },
        error: "unknown table 'public.missing'",
      },
    ];
    for (const { body, error } of invalid) {
      const response = await post({ ...body, subscription: "refused-claim" });
      expect(response.status, await response.clone().text()).toBe(400);
      expect((await response.json()) as { error: string }).toMatchObject({ error: expect.stringContaining(error) });
      expect(response.headers.get("retry-after")).toBeNull();
    }
    const graph = (await fetch(`${h.engineUrl}/graph`).then((response) => response.json())) as { shapes: unknown[] };
    expect(graph.shapes).toEqual([]);
    // At most one id can have been allocated by each refusal; none may have created its stream.
    for (let id = 1; id <= invalid.length; id++) {
      expect((await fetch(`${h.dsUrl}/shape/s${id}`, { method: "HEAD" })).status).toBe(404);
    }

    await pgQuery(h, "INSERT INTO items (id, n) VALUES ($1, $2)", [1, 10]);
    await drainEngine(h);
    const valid = await createShape(h, { table: "items", columns: ["id"], subscription: "refused-claim" });
    expect(await foldStream(valid.streamUrl)).toEqual(new Map([["1", { id: 1 }]]));
    await pgQuery(h, "INSERT INTO items (id, n) VALUES ($1, $2)", [2, 20]);
    await drainEngine(h);
    expect(await foldStream(valid.streamUrl)).toEqual(
      new Map([
        ["1", { id: 1 }],
        ["2", { id: 2 }],
      ]),
    );
  }, 60000);

  it.each([{ project: "missing" }, { project: "id", where: { col: "missing", op: "eq", value: 1 } }])(
    "preserves late inner-column refusal for a concurrent identical joiner: %j",
    async (inner) => {
      h = await bootHarness(schema);
      const lock = await lockTable(h, "items");
      // A valid seed holds initialization admission. This registers the invalid creator's share
      // before its late inner-column compilation, making the join deterministic without sleeps.
      const blocker = post({ table: "items", where: { col: "id", in: { table: "items", project: "id" } } });
      const graph = async () =>
        (await fetch(`${h!.engineUrl}/graph`).then((response) => response.json())) as {
          shapes: { id: string; streamPath: string; columns: string[] | null }[];
        };
      const invalid = { table: "items", columns: ["id"], where: { col: "id", in: { table: "items", ...inner } } };
      let failed: { id: string; streamPath: string } | undefined;
      const answers: Promise<Response>[] = [];
      try {
        await waitFor(async () => (await tableLockWaiters(h!, "items")).length > 0, "the admission holder to seed");
        answers.push(post({ ...invalid, subscription: "invalid-creator" }));
        await waitFor(async () => {
          failed = (await graph()).shapes.find((shape) => shape.columns?.includes("id"));
          return failed !== undefined;
        }, "the invalid creator to register its pending share");
        answers.push(post({ ...invalid, subscription: "invalid-joiner" }));
        await waitFor(async () => {
          const state = (await fetch(`${h!.engineUrl}/shapes/${failed!.id}`).then((response) => response.json())) as {
            subscriptions: number;
          };
          return state.subscriptions === 2;
        }, "the identical request to join the pending share");
      } finally {
        await lock.release();
      }
      const responses = await Promise.all(answers);
      for (const response of responses) {
        expect(response.status, await response.clone().text()).toBe(400);
        expect((await response.json()) as { error: string }).toMatchObject({
          error: expect.stringContaining("unknown column 'missing'"),
        });
      }
      expect((await blocker).status).toBe(200);
      await waitFor(async () => (await graph()).shapes.length === 1, "failed creator and joiner compensation");
      expect((await fetch(`${h.dsUrl}/${failed!.streamPath}`, { method: "HEAD" })).status).toBe(404);
      // Restart must not resurrect the failed share. The valid subquery is also retired at boot
      // because its inner-node state is not persisted.
      await h.restartEngine();
      expect((await graph()).shapes).toEqual([]);
      expect((await fetch(`${h.dsUrl}/${failed!.streamPath}`, { method: "HEAD" })).status).toBe(404);
      await createShape(h, { table: "items", subscription: "invalid-creator" });
      await createShape(h, { table: "items", subscription: "invalid-joiner" });
    },
    60000,
  );

  it("answers retryable 503 before validating columns while schema resolution is blocked", async () => {
    h = await bootHarness(schema, { engineEnv: { CIRCUITS_SCHEMA_RECONCILE_SECS: "1" } });
    await pgQuery(h, "ALTER TABLE items REPLICA IDENTITY DEFAULT");
    const lock = await lockTable(h, "items");
    try {
      // The reconciler is held in its bounded identity-restoration ALTER, with the resolve lock
      // already active. The schema's missing column cannot honestly be classified yet.
      await waitFor(async () => (await tableLockWaiters(h!, "items")).length > 0, "identity restoration to block");
      const resolving = await post({ table: "items", columns: ["missing"] });
      expect(resolving.status, await resolving.clone().text()).toBe(503);
      expect(resolving.headers.get("retry-after")).toBe("1");
      await waitFor(async () => {
        const tables = (await fetch(`${h!.engineUrl}/tables`).then((response) => response.json())) as {
          tables: { table: string; unresolved: boolean }[];
        };
        return tables.tables.some((table) => table.table === "public.items" && table.unresolved);
      }, "the bounded restoration to park the table unresolved");
      const unresolved = await post({ table: "items", columns: ["missing"] });
      expect(unresolved.status, await unresolved.clone().text()).toBe(503);
      expect(unresolved.headers.get("retry-after")).toBe("1");
    } finally {
      await lock.release();
    }
    await waitFor(async () => {
      const response = await post({ table: "items", columns: ["missing"] });
      await response.text();
      return response.status === 400;
    }, "schema recovery to restore deterministic request validation");
    await createShape(h, { table: "items", columns: ["id"] });
  }, 60000);
});
