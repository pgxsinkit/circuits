import { afterAll, beforeAll } from "vitest";

import { startPostgresFixture } from "./packages/conformance/src/postgres-fixture";

let postgres: ReturnType<typeof startPostgresFixture> | undefined;
let previousUrl: string | undefined;

beforeAll(() => {
  previousUrl = process.env.CIRCUITS_TEST_PG_URL;
  postgres = startPostgresFixture();
  process.env.CIRCUITS_TEST_PG_URL = postgres.url;
});

// With stack-ordered hooks this runs after the file's fixtures have stopped their engines and
// restored any temporary custom-cluster override. At most maxWorkers default clusters are live.
afterAll(() => {
  if (previousUrl === undefined) delete process.env.CIRCUITS_TEST_PG_URL;
  else process.env.CIRCUITS_TEST_PG_URL = previousUrl;
  postgres?.stop();
});
