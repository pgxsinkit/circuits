import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    globalSetup: ["./vitest.global-setup.ts"],
    // Sibling agent worktrees live under .claude/worktrees and carry their own copies of the
    // test files (without node_modules) — never collect them from this checkout. The log server's
    // protocol conformance suite is its own package with its own vitest (4.x) and config, run by
    // `bun run test:durable-streams:conformance` against a release build — not by this run.
    exclude: ["**/node_modules/**", "**/.claude/worktrees/**", "apps/durable-streams/**"],
    // Conformance tests each boot an engine subprocess + pglite; keep memory bounded.
    pool: "forks",
    poolOptions: { forks: { maxForks: 4 } },
    testTimeout: 60000,
    hookTimeout: 60000,
  },
});
