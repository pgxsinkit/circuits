// Global setup for the `unit` project: build the log server, and nothing else. ds-rust's
// binary.test.ts checks that the wrapper resolves the binary this workspace builds, so the binary has
// to exist; no process is started and no Postgres is needed. The `integration` project's setup,
// vitest.global-setup.ts, builds it the same way along with the engine.
import { execFileSync } from "node:child_process";

export default function setup(): void {
  execFileSync("cargo", ["build", "-p", "durable-streams"], { stdio: "inherit" });
}
