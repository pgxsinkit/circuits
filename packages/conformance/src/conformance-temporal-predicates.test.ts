import type { Row, Schema } from "@circuits/protocol";
import pgpkg from "pg";
import { afterEach, describe, expect, it } from "vitest";

import { createShape, foldStream, pgQuery, waitFor } from "./engine-native.js";
import { bootHarness, drainEngine, type Harness } from "./harness.js";

const schema: Schema = {
  tables: {
    records: { columns: { id: { type: "int" }, visible: { type: "bool" } }, primaryKey: "id" },
    events: {
      columns: {
        id: { type: "int" },
        record_id: { type: "int" },
        starts: { type: "text" },
        local: { type: "text" },
        day: { type: "text" },
        marks: { type: "text" },
        name: { type: "text" },
      },
      primaryKey: "id",
    },
  },
};
const ddl = `
  CREATE TABLE records (id integer PRIMARY KEY, visible boolean NOT NULL);
  ALTER TABLE records REPLICA IDENTITY FULL;
  CREATE TABLE events (
    id integer PRIMARY KEY, record_id integer NOT NULL, starts timestamptz NOT NULL,
    local timestamp, day date, marks timestamptz[], name text NOT NULL
  );
  ALTER TABLE events REPLICA IDENTITY FULL;
`;
const window = {
  and: [
    { col: "starts", op: "gte", value: "2026-10-19T07:00:00Z" },
    { col: "starts", op: "lt", value: "2026-10-26T07:00:00Z" },
  ],
};
let h: Harness | undefined;
afterEach(async () => {
  await h?.shutdown();
  h = undefined;
});

describe.each(["America/Los_Angeles", "Asia/Kolkata"])("temporal predicates with %s defaults", (zone) => {
  it("keeps snapshot and replicated values in UTC ISO across live updates and reconnects", async () => {
    h = await bootHarness(schema, {
      ddl,
      beforeEngine: async ({ pgUrl }) => {
        const client = new pgpkg.Client({ connectionString: pgUrl });
        await client.connect();
        try {
          await client.query(`
            DO $$ BEGIN
              EXECUTE format('ALTER DATABASE %I SET timezone TO %L', current_database(), '${zone}');
              EXECUTE format('ALTER ROLE %I IN DATABASE %I SET datestyle TO %L', current_user, current_database(), 'SQL, DMY');
            END $$
          `);
        } finally {
          await client.end();
        }
      },
    });
    expect((await pgQuery(h, "SHOW timezone"))[0]!.TimeZone).toBe(zone);
    expect((await pgQuery(h, "SHOW datestyle"))[0]!.DateStyle).toBe("SQL, DMY");
    await pgQuery(
      h,
      `
      INSERT INTO records VALUES (1, true);
      INSERT INTO events VALUES
        (1, 1, '2026-10-19 16:00:00+00', '2026-10-19 16:00:00', '2026-10-19',
         ARRAY['2026-10-19 09:00:00-07']::timestamptz[], 'seed'),
        (2, 1, '2026-10-19 06:00:00+00', NULL, NULL, NULL, 'outside')
    `,
    );
    await drainEngine(h);
    const shape = await createShape(h, { table: "events", where: window });
    const membership = await createShape(h, {
      table: "events",
      where: {
        and: [
          ...window.and,
          {
            col: "record_id",
            in: { table: "records", project: "id", where: { col: "visible", op: "eq", value: true } },
          },
        ],
      },
    });
    const expectedTemporal = {
      starts: "2026-10-19 16:00:00+00",
      local: "2026-10-19 16:00:00",
      day: "2026-10-19",
      marks: '{"2026-10-19 16:00:00+00"}',
    };
    const temporal = (row: Row | undefined) =>
      Object.fromEntries(Object.keys(expectedTemporal).map((key) => [key, row?.[key]]));
    expect(temporal((await foldStream(shape.streamUrl)).get("1"))).toEqual(expectedTemporal);

    await pgQuery(h, "UPDATE events SET name = 'live' WHERE id IN (1, 2)");
    await drainEngine(h);
    for (const stream of [shape, membership]) {
      await waitFor(async () => (await foldStream(stream.streamUrl)).get("1")?.name === "live", "live timestamp row");
      const rows = await foldStream(stream.streamUrl);
      expect([...rows.keys()]).toEqual(["1"]);
      expect(temporal(rows.get("1"))).toEqual(expectedTemporal);
    }

    const walsender = async () =>
      (await pgQuery(h!, "SELECT active_pid FROM pg_replication_slots WHERE slot_name = $1", [h!.slot]))[0]?.active_pid;
    const previous = await walsender();
    expect(previous).toBeTruthy();
    await pgQuery(h, "SELECT pg_terminate_backend($1)", [previous]);
    await waitFor(async () => {
      const current = await walsender();
      return current != null && current !== previous;
    }, "replication reconnect");
    await pgQuery(h, "UPDATE events SET name = 'reconnected' WHERE id = 1");
    await drainEngine(h);
    const reconnected = (await foldStream(shape.streamUrl)).get("1");
    expect(temporal(reconnected)).toEqual(expectedTemporal);
    expect(reconnected?.name).toBe("reconnected");

    for (const value of ["tomorrow", "2026-10-19 07:00:00 America/Los_Angeles"]) {
      const response = await fetch(`${h.engineUrl}/shapes`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ table: "events", where: { col: "starts", op: "gte", value } }),
      });
      expect(response.status).toBe(400);
      const body = await response.text();
      expect(body).toContain("'starts'");
      expect(body).toContain(value);
    }
  }, 90000);
});
