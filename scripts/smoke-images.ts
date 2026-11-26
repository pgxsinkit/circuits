import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import { promisify } from "node:util";

import type { StreamEnvelope } from "../packages/protocol/src/envelope.js";
import { validationEnvironment } from "./validation-env.js";

const execute = promisify(execFile);
const cancelled = new AbortController();
const commandEnv = validationEnvironment();
// The host-side HTTP reader also talks only to the pod's loopback publications.
process.env.NO_PROXY = commandEnv.NO_PROXY;
process.env.no_proxy = commandEnv.no_proxy;
// Emergent's database image from infra/charts/postgres/values.yaml.
const postgresImage =
  "ghcr.io/cloudnative-pg/postgresql:19.0-202611201710-minimal-trixie@sha256:28f6604b4e142cadd8b0154ae82a5541e321d6f33b27fc5a054ced3dde53e6e8";

async function command(binary: string, args: string[], timeout = 60_000, input?: string): Promise<string> {
  const running = execute(binary, args, { env: commandEnv, timeout, maxBuffer: 4 * 1024 * 1024 });
  if (input !== undefined) running.child.stdin!.end(input);
  const result = await running;
  return result.stdout.trim();
}

async function waitFor<T>(description: string, probe: () => Promise<T | undefined>, timeout = 30_000): Promise<T> {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    cancelled.signal.throwIfAborted();
    const value = await probe();
    if (value !== undefined) return value;
    await delay(100, undefined, { signal: cancelled.signal });
  }
  throw new Error(`Timed out waiting for ${description} after ${timeout} ms`);
}

async function request(url: string, options?: RequestInit): Promise<Response> {
  return fetch(url, {
    ...options,
    signal: AbortSignal.any([cancelled.signal, AbortSignal.timeout(5000)]),
  });
}

async function ready(url: string): Promise<void> {
  let observation = "no request completed";
  try {
    await waitFor(`${url} readiness`, async () => {
      try {
        const response = await request(url);
        observation = `HTTP ${response.status} ${(await response.text()).slice(0, 500)}`;
        return response.ok ? true : undefined;
      } catch (error) {
        observation = String(error);
        cancelled.signal.throwIfAborted();
        const code = (error as { code?: string }).code;
        if (
          code === "ECONNREFUSED" ||
          code === "ConnectionRefused" ||
          code === "ECONNRESET" ||
          code === "ConnectionClosed" ||
          (error instanceof Error && error.name === "TimeoutError")
        )
          return undefined;
        if (error instanceof TypeError && (error.cause as { code?: string } | undefined)?.code === "ECONNREFUSED")
          return undefined;
        throw error;
      }
    });
  } catch (error) {
    throw new Error(`${url} readiness failed; last response was ${observation}`, { cause: error });
  }
}

interface Shape {
  shapeId: string;
  streamPath: string;
  streamUrl: string;
}

async function readStream(url: string): Promise<{ envelopes: StreamEnvelope[]; offset: string }> {
  const envelopes: StreamEnvelope[] = [];
  let offset = "-1";
  for (let page = 0; page < 100; page++) {
    const response = await request(`${url}?offset=${encodeURIComponent(offset)}`);
    const body = await response.text();
    assert([200, 204].includes(response.status), `GET ${url} returned ${response.status}: ${body}`);
    if (body.trim()) envelopes.push(...(JSON.parse(body) as StreamEnvelope[]));
    const next = response.headers.get("stream-next-offset");
    if (next) offset = next;
    if (response.status === 204 || response.headers.has("stream-up-to-date")) return { envelopes, offset };
    assert(next, `GET ${url} did not supply the next offset`);
  }
  throw new Error(`GET ${url} exceeded 100 pages`);
}

async function main(): Promise<void> {
  const [engineImage, streamImage] = process.argv.slice(2);
  assert(engineImage && streamImage, "Usage: bun scripts/smoke-images.ts <engine image> <log server image>");
  assert.equal(process.platform, "linux", "Image qualification requires Linux Podman");
  await command("podman", ["info", "--format", "{{.Host.Arch}}"]);
  const runId = `circuits-smoke-${randomUUID()}`;
  const engine = `${runId}-engine`;
  const streams = `${runId}-streams`;
  const postgres = `${runId}-postgres`;
  const initializePostgres = `${runId}-initdb`;
  const volume = `${runId}-data`;
  const pgVolume = `${runId}-pgdata`;
  const volumes: string[] = [];
  const containers: string[] = [];
  let podCreated = false;
  const pgPassword = randomUUID();

  try {
    const pgVersion = await command("podman", [
      "run",
      "--rm",
      "--network",
      "none",
      postgresImage,
      "postgres",
      "--version",
    ]);
    assert(/\b19\.0\b/.test(pgVersion), `Expected PostgreSQL 19.0; found ${pgVersion}`);
    await command("podman", [
      "pod",
      "create",
      "--name",
      runId,
      "--publish",
      "127.0.0.1::7010",
      "--publish",
      "127.0.0.1::4437",
    ]);
    podCreated = true;
    await command("podman", ["pod", "start", runId]);
    const infra = await command("podman", ["pod", "inspect", "--format", "{{.InfraContainerID}}", runId]);
    const publishedUrl = async (port: number) => {
      const mapping = await command("podman", ["port", infra, `${port}/tcp`]);
      assert(/^127\.0\.0\.1:\d+$/.test(mapping), `Expected one localhost mapping for ${port}; got ${mapping}`);
      return `http://${mapping}`;
    };
    const streamUrl = await publishedUrl(4437);
    const engineUrl = await publishedUrl(7010);
    const internalStreamUrl = "http://127.0.0.1:4437";
    const pgUrl = `postgres://postgres:${pgPassword}@127.0.0.1:5432/postgres`;
    await command("podman", ["volume", "create", pgVolume]);
    volumes.push(pgVolume);
    containers.push(initializePostgres);
    await command(
      "podman",
      [
        "run",
        "--interactive",
        "--rm",
        "--network",
        "none",
        "--name",
        initializePostgres,
        "--volume",
        `${pgVolume}:/var/lib/postgresql/data:U`,
        postgresImage,
        "initdb",
        "-D",
        "/var/lib/postgresql/data/pgdata",
        "-U",
        "postgres",
        "--auth-host=scram-sha-256",
        "--auth-local=trust",
        "--pwfile=/dev/stdin",
      ],
      60_000,
      `${pgPassword}\n`,
    );
    const query = (sql: string) =>
      command(
        "podman",
        [
          "exec",
          postgres,
          "psql",
          "-XAt",
          "--set",
          "ON_ERROR_STOP=1",
          "--host",
          "/tmp",
          "--username",
          "postgres",
          "--dbname",
          "postgres",
          "--command",
          sql,
        ],
        10_000,
      );
    const readyPg = () =>
      waitFor("PostgreSQL readiness", async () => {
        const result = await execute("podman", ["exec", postgres, "pg_isready", "-h", "/tmp", "-U", "postgres"], {
          env: commandEnv,
          timeout: 5000,
        }).catch((error: unknown) => {
          if ([1, 2].includes((error as { code: number }).code)) return undefined;
          throw error;
        });
        return result ? true : undefined;
      });
    containers.push(postgres);
    await command("podman", [
      "run",
      "--detach",
      "--name",
      postgres,
      "--pod",
      runId,
      "--volume",
      `${pgVolume}:/var/lib/postgresql/data:U`,
      postgresImage,
      "postgres",
      "-D",
      "/var/lib/postgresql/data/pgdata",
      "-p",
      "5432",
      "-h",
      "127.0.0.1",
      "-k",
      "/tmp",
      "-c",
      "wal_level=logical",
      "-c",
      "max_replication_slots=32",
      "-c",
      "max_wal_senders=32",
      "-c",
      "fsync=on",
      "-c",
      "synchronous_commit=on",
      "-c",
      "full_page_writes=on",
    ]);
    await readyPg();
    const settings = await query(
      "SELECT json_build_object('fsync', current_setting('fsync'), 'synchronous_commit', current_setting('synchronous_commit'), 'full_page_writes', current_setting('full_page_writes'), 'wal_level', current_setting('wal_level'), 'max_replication_slots', current_setting('max_replication_slots'), 'scram_password', (SELECT rolpassword LIKE 'SCRAM-SHA-256$%' FROM pg_authid WHERE rolname = 'postgres'), 'scram_host_rules', (SELECT bool_and(auth_method = 'scram-sha-256') FROM pg_hba_file_rules WHERE type LIKE 'host%'))",
    );
    assert.deepEqual(JSON.parse(settings), {
      fsync: "on",
      synchronous_commit: "on",
      full_page_writes: "on",
      wal_level: "logical",
      max_replication_slots: "32",
      scram_password: true,
      scram_host_rules: true,
    });
    await query("CREATE TABLE items (id integer PRIMARY KEY, n integer NOT NULL); INSERT INTO items VALUES (1, 10)");

    await command("podman", ["volume", "create", volume]);
    volumes.push(volume);
    containers.push(streams);
    await command("podman", [
      "run",
      "--detach",
      "--name",
      streams,
      "--pod",
      runId,
      "--volume",
      `${volume}:/var/lib/durable-streams:U`,
      streamImage,
      "--host",
      "0.0.0.0",
      "--port",
      "4437",
      "--data-dir",
      "/var/lib/durable-streams",
    ]);
    await ready(`${streamUrl}/health`);
    containers.push(engine);
    await command("podman", [
      "run",
      "--detach",
      "--name",
      engine,
      "--pod",
      runId,
      "--env",
      `CIRCUITS_PG_URL=${pgUrl}`,
      "--env",
      `CIRCUITS_DS_URL=${internalStreamUrl}`,
      "--env",
      "CIRCUITS_BIND=0.0.0.0:7010",
      "--env",
      "CIRCUITS_PG_TABLES=public.items",
      "--env",
      "CIRCUITS_PG_SLOT=image_smoke",
      "--env",
      "CIRCUITS_SHUTDOWN_DRAIN_SECS=0",
      engineImage,
    ]);
    await ready(`${engineUrl}/ready`);
    const createShape = async () => {
      const response = await request(`${engineUrl}/shapes`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ table: "items", subscription: runId }),
      });
      assert(response.ok, `Shape create returned ${response.status}: ${await response.clone().text()}`);
      return (await response.json()) as Shape;
    };
    const shape = await createShape();
    const internalShapeUrl = new URL(shape.streamUrl);
    assert.equal(internalShapeUrl.origin, internalStreamUrl);
    assert.equal(internalShapeUrl.pathname, `/${shape.streamPath}`);
    assert.equal(internalShapeUrl.search, "");
    const shapeUrl = new URL(internalShapeUrl.pathname, streamUrl).href;
    const waitValue = async (value: number) =>
      waitFor(`replicated row n=${value}`, async () => {
        const history = await readStream(shapeUrl);
        const rows = new Map<string, StreamEnvelope["value"]>();
        for (const envelope of history.envelopes) {
          if (envelope.headers.operation === "delete") rows.delete(envelope.key);
          else rows.set(envelope.key, envelope.value);
        }
        const row = rows.get("1");
        return rows.size === 1 && row?.id === 1 && row.n === value && Object.keys(row).length === 2
          ? history
          : undefined;
      });
    await waitValue(10);
    await query("UPDATE items SET n = 20 WHERE id = 1");
    const beforeRestart = await waitValue(20);
    console.log("Candidate images backfilled and replicated a committed update with PostgreSQL durability enabled.");

    await command("podman", ["stop", "--time", "35", engine]);
    assert.equal(await command("podman", ["inspect", "--format", "{{.State.ExitCode}}", engine]), "0");
    await command("podman", ["stop", "--time", "30", streams]);
    assert.equal(await command("podman", ["inspect", "--format", "{{.State.ExitCode}}", streams]), "0");
    await command("podman", ["stop", "--time", "30", postgres]);
    assert.equal(await command("podman", ["inspect", "--format", "{{.State.ExitCode}}", postgres]), "0");
    await command("podman", ["start", postgres]);
    await readyPg();
    await command("podman", ["start", streams]);
    await ready(`${streamUrl}/health`);
    await command("podman", ["start", engine]);
    await ready(`${engineUrl}/ready`);
    const restored = await createShape();
    assert.equal(restored.shapeId, shape.shapeId, "Restart must preserve shape identity");
    assert.equal(restored.streamPath, shape.streamPath, "Restart must preserve stream identity");
    assert.equal(restored.streamUrl, shape.streamUrl);
    assert.deepEqual(await readStream(shapeUrl), beforeRestart, "Restart must preserve the stream history and offset");
    await query("UPDATE items SET n = 30 WHERE id = 1");
    const afterRestart = await waitValue(30);
    assert.deepEqual(afterRestart.envelopes.slice(0, beforeRestart.envelopes.length), beforeRestart.envelopes);
    assert(afterRestart.envelopes.length > beforeRestart.envelopes.length);

    const purge = await request(`${engineUrl}/shapes/${encodeURIComponent(shape.shapeId)}?purge=true`, {
      method: "DELETE",
    });
    assert(purge.ok, `Purge returned ${purge.status}: ${await purge.text()}`);
    await waitFor("purged stream to reach 404", async () => {
      const response = await request(shapeUrl);
      await response.text();
      return response.status === 404 ? true : undefined;
    });
    const metadata = await request(`${engineUrl}/shapes/${encodeURIComponent(shape.shapeId)}`);
    assert.equal(metadata.status, 404);
    await metadata.text();
    console.log(
      "Candidate images preserved stream identity and history across restart, replicated again, and completed purge.",
    );
  } catch (error) {
    for (const name of containers) {
      const logs = await execute("podman", ["logs", "--tail", "100", name], {
        env: commandEnv,
        timeout: 10_000,
        maxBuffer: 4 * 1024 * 1024,
      })
        .then(({ stdout, stderr }) => stdout + stderr)
        .catch(String);
      console.error(`${name} logs\n${logs.replaceAll(pgPassword, "<REDACTED>")}`);
    }
    const failure = new Error(String(error).replaceAll(pgPassword, "<REDACTED>"));
    if (error instanceof Error && error.stack) failure.stack = error.stack.replaceAll(pgPassword, "<REDACTED>");
    throw failure;
  } finally {
    for (const name of containers.reverse()) {
      await command("podman", ["rm", "--ignore", "--force", "--time", "35", name]).catch((error: unknown) => {
        console.error(error);
        process.exitCode = 1;
      });
    }
    if (podCreated)
      await command("podman", ["pod", "rm", "--force", "--ignore", runId]).catch((error: unknown) => {
        console.error(error);
        process.exitCode = 1;
      });
    for (const name of volumes.reverse())
      await command("podman", ["volume", "rm", name]).catch((error: unknown) => {
        console.error(error);
        process.exitCode = 1;
      });
  }
}

for (const signal of ["SIGINT", "SIGTERM"] as const) {
  process.once(signal, () => cancelled.abort(new Error(`Image qualification received ${signal}`)));
}

await main().catch((error: unknown) => {
  console.error(error);
  process.exitCode = 1;
});
