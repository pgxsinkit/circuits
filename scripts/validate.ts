import { spawnSync } from "node:child_process";
import { appendFileSync, readFileSync } from "node:fs";
import { availableParallelism, platform, release, totalmem } from "node:os";
import { resolve } from "node:path";

import { validationEnvironment } from "./validation-env";

// CI obtains its matrix from this definition. Each suite starts its own server and fresh storage.
const protocolConfigurations: Record<string, string[]> = {
  "wal-default": [],
  "wal-resident-cache": ["--tail-cache-bytes", "65536"],
  "wal-read-offload-always": ["--read-offload", "always"],
  memory: ["--durability", "memory"],
  "wal-small-chunk": ["--max-chunk-bytes", "4096"],
};

const lane = process.argv[2] ?? "full";
const configuration = process.argv[3];
const environment = validationEnvironment();
if (lane !== "commit") environment.CI = "true";
// Qualification owns its backends. Explicit CI artifact selection is handled only by the protocol lane.
for (const key of [
  "RUST_SERVER_URL",
  "RUST_SERVER_BIN",
  "RUST_SERVER_PORT",
  "RUST_SERVER_ARGS",
  "DS_RUST_BIN",
  "DS_MEMORY",
]) {
  delete environment[key];
}

function run(command: string[], overrides: NodeJS.ProcessEnv = {}): void {
  console.log(`Running ${command.join(" ")}`);
  const result = spawnSync(command[0]!, command.slice(1), {
    stdio: "inherit",
    env: { ...environment, ...overrides },
  });
  if (result.error) throw result.error;
  if (result.status !== 0) {
    throw new Error(`Command failed (${result.signal ?? result.status}): ${command.join(" ")}`);
  }
}

function resources(): void {
  console.log(`Validation lane ${lane}${configuration ? ` ${configuration}` : ""}`);
  console.log(
    `${platform()} ${release()}, ${availableParallelism()} available CPUs, ${Math.round(totalmem() / 1024 ** 3)} GiB RAM`,
  );
  run(["bun", "--version"]);
  run(["node", "--version"]);
  if (platform() === "linux") {
    console.log(
      readFileSync("/proc/self/status", "utf8")
        .split("\n")
        .find((line) => line.startsWith("Cpus_allowed_list:")),
    );
    console.log(
      readFileSync("/proc/self/limits", "utf8")
        .split("\n")
        .find((line) => line.startsWith("Max open files")),
    );
  }
}

function rust(): void {
  run(["bun", "run", "format"]);
  run(["bun", "run", "engine:test", "--locked"]);
  run(["bun", "run", "durable-streams:build", "--profile", "release-durable-streams", "--locked"]);
  run(["bun", "run", "test:durable-streams", "--profile", "release-durable-streams", "--locked"]);
  run(["bun", "run", "lint"]);
}

function lifecycle(): void {
  if (platform() !== "linux") {
    throw new Error("Runner-sized lifecycle validation requires Linux and taskset.");
  }
  const allowed = readFileSync("/proc/self/status", "utf8").match(/^Cpus_allowed_list:\s*(.+)$/m)?.[1];
  if (!allowed) throw new Error("Cannot determine the permitted CPU affinity for lifecycle validation.");
  const cpus = allowed
    .split(",")
    .flatMap((range) => {
      const [first, last = first] = range.split("-").map(Number);
      return Array.from({ length: last! - first! + 1 }, (_, index) => first! + index);
    })
    .slice(0, 4);
  console.log(
    `Additional lifecycle coverage on CPUs ${cpus.join(",")}; the full harness also runs with ordinary host concurrency.`,
  );
  run([
    "taskset",
    "--cpu-list",
    cpus.join(","),
    "bun",
    "run",
    "test:integration:harness",
    "packages/conformance/src/conformance-retention.test.ts",
    "packages/conformance/src/conformance-pending-replay-bounds.test.ts",
    "packages/conformance/src/conformance-missing-replay-segment.test.ts",
  ]);
}

function harness(): void {
  run(["bun", "run", "typecheck"]);
  run(["bun", "run", "test:unit:ts"]);
  run(["bun", "run", "test:integration:harness"]);
  run(["bun", "run", "validate:lifecycle"]);
}

function protocol(binary?: string): void {
  const names = configuration ? [configuration] : Object.keys(protocolConfigurations);
  for (const name of names) {
    if (!Object.hasOwn(protocolConfigurations, name)) throw new Error(`Unknown protocol configuration ${name}`);
  }
  if (!binary) {
    run(["bun", "run", "durable-streams:build", "--profile", "release-durable-streams", "--locked"]);
    binary = releaseBinary();
  }
  for (const name of names) {
    console.log(`Protocol configuration ${name}, binary ${binary}`);
    try {
      run(["bun", "run", "--cwd", "apps/durable-streams", "test:conformance"], {
        RUST_SERVER_BIN: binary,
        RUST_SERVER_ARGS: protocolConfigurations[name]!.join(" "),
      });
    } catch (error) {
      console.error(`Reproduce this configuration with bun run validate:protocol ${name}`);
      throw error;
    }
  }
}

function releaseBinary(): string {
  return resolve(process.env.CARGO_TARGET_DIR ?? "target", "release-durable-streams", "durable-streams-server");
}

function full(): void {
  run(["bun", "install", "--frozen-lockfile"]);
  run(["bun", "run", "validate:rust"]);
  run(["bun", "run", "validate:harness"]);
  protocol(releaseBinary());
  run(["bun", "run", "test:images"]);
}

try {
  if (lane === "commit") {
    run(["bun", "run", "format"]);
    run(["bun", "run", "check"]);
  } else if (lane === "matrix") {
    const matrix = JSON.stringify({ config: Object.keys(protocolConfigurations) });
    if (process.env.GITHUB_OUTPUT) appendFileSync(process.env.GITHUB_OUTPUT, `matrix=${matrix}\n`);
    console.log(matrix);
  } else {
    resources();
    const lanes: Record<string, () => void> = {
      rust,
      harness,
      lifecycle,
      protocol: () => protocol(process.argv[4]),
      full,
    };
    if (!Object.hasOwn(lanes, lane)) throw new Error(`Unknown validation lane ${lane}`);
    lanes[lane]!();
  }
} catch (error) {
  console.error(error instanceof Error ? error.message : error);
  const script = lane === "commit" ? "validate" : `validate:${lane}`;
  console.error(`Validation failed. Reproduce with bun run ${script}${configuration ? ` ${configuration}` : ""}`);
  process.exitCode = 1;
}
