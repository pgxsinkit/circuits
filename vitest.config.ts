import { defineConfig } from "vitest/config";

// Two projects, and every test file is in exactly one of them.
//
// `unit`: the files below, which need neither the engine, a log server nor a Postgres server.
// `bun run test` runs them next to the Rust tests. ds-rust's binary.test.ts checks which log server
// binary the wrapper resolves, which needs the workspace build to exist but starts nothing, so this
// project's setup builds it.
//
// `integration`: every other file. They boot the stack, through vitest.global-setup.ts, and
// `bun run test:integration` runs them. A new test file lands here unless it is listed below.
const unitFiles = [
  "packages/client/src/subset.test.ts",
  "packages/client/src/tables.test.ts",
  "packages/conformance/src/harness-mechanics.test.ts",
  "packages/ds-rust/src/binary.test.ts",
  "packages/protocol/src/protocol.test.ts",
];

// Sibling agent worktrees live under .claude/worktrees and carry their own copies of the test files
// (without node_modules) — never collect them from this checkout. The log server's protocol
// conformance suite is its own package with its own config, run by
// `bun run test:durable-streams:conformance` against a release build — not by this run. `tmp/` is
// scratch: a copy of the repository left there must not have its tests collected.
const exclude = ["**/node_modules/**", "**/.claude/worktrees/**", "apps/durable-streams/**", "tmp/**"];

const common = {
  pool: "forks",
  testTimeout: 60000,
  hookTimeout: 60000,
} as const;

export default defineConfig({
  test: {
    // Conformance tests each boot an engine subprocess and a log server; keep memory bounded. Set for
    // the whole run, not per project: projects that run together must agree on it (vitest refuses two
    // `maxWorkers` in one sequence group), and `vitest run packages/conformance` runs files of both.
    maxWorkers: 4,
    projects: [
      {
        test: {
          ...common,
          name: "unit",
          include: unitFiles,
          exclude,
          globalSetup: ["./vitest.unit-setup.ts"],
        },
      },
      {
        test: {
          ...common,
          name: "integration",
          exclude: [...exclude, ...unitFiles],
          globalSetup: ["./vitest.global-setup.ts"],
        },
      },
    ],
  },
});
