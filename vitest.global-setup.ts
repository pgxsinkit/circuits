// Global setup for the `integration` project: build the engine and the log server once (so parallel
// workers don't race the cargo lock, and @circuits/ds-rust runs the log server built from this
// workspace). Each test file starts its own PostgreSQL cluster in vitest.integration-setup.ts.
// Logical-slot snapshot creation waits for writers across databases, so database isolation alone
// cannot isolate a booting engine from a different file's intentionally long-running transaction.
import { execFileSync } from "node:child_process";

import { requirePostgres19, validationEnvironment } from "./scripts/validation-env";

export default function setup() {
  Object.assign(process.env, validationEnvironment());
  requirePostgres19(process.env);
  // One `cargo build` per package, not `-p circuits-engine -p durable-streams` in one invocation:
  // cargo unifies features across the packages of one build, and the log server's
  // (tracing/release_max_level_info, hashbrown/raw) would give the engine a feature set it has
  // nowhere else, recompiling its whole dependency graph a second time.
  execFileSync("cargo", ["build", "--locked", "-p", "circuits-engine"], { stdio: "inherit" });
  execFileSync("cargo", ["build", "--locked", "-p", "durable-streams"], { stdio: "inherit" });
  process.env.CIRCUITS_ENGINE_PREBUILT = "1";
}
