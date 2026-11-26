import { execFileSync, spawnSync } from "node:child_process";
import { appendFileSync, existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

/** A cluster, rather than a database, isolates logical-slot snapshot creation from open writers. */
export function startPostgresFixture(): { url: string; stop(): void } {
  const dir = mkdtempSync(join(tmpdir(), "circuits-test-pg-"));
  const data = join(dir, "data");
  let stopped = false;
  const stop = () => {
    if (stopped) return;
    if (existsSync(join(data, "postmaster.pid"))) {
      try {
        execFileSync("pg_ctl", ["-D", data, "-m", "immediate", "-w", "stop"], { stdio: "ignore" });
      } catch (error) {
        const status = spawnSync("pg_ctl", ["-D", data, "status"], { stdio: "ignore" });
        if (status.status !== 3) {
          throw new Error(`Could not stop test PostgreSQL; preserved its directory at ${dir}`, { cause: error });
        }
      }
    }
    rmSync(dir, { recursive: true, force: true });
    stopped = true;
  };

  try {
    execFileSync("initdb", ["-D", data, "-U", "postgres", "--auth=trust", "--no-sync"], { stdio: "ignore" });
    appendFileSync(
      join(data, "postgresql.conf"),
      "\nwal_level = logical\nmax_replication_slots = 80\nmax_wal_senders = 80\n" +
        "listen_addresses = '127.0.0.1'\nunix_socket_directories = ''\n" +
        "fsync = off\nsynchronous_commit = off\nfull_page_writes = off\n",
    );
    // PostgreSQL requires a concrete TCP port. Retry the existing bind-allocation race only.
    for (let attempt = 0; attempt < 8; attempt++) {
      const port = 55432 + Math.floor(Math.random() * 4000);
      const log = join(dir, "log");
      writeFileSync(log, "");
      try {
        execFileSync("pg_ctl", ["-D", data, "-l", log, "-o", `-p ${port}`, "-w", "start"], { stdio: "ignore" });
        return { url: `postgres://postgres@127.0.0.1:${port}/postgres`, stop };
      } catch (error) {
        const bindCollision = /could not bind (?:IPv4|IPv6) address[^\n]*Address already in use/i.test(
          readFileSync(log, "utf8"),
        );
        if (!bindCollision || attempt === 7) throw error;
      }
    }
    throw new Error("failed to start ephemeral postgres for tests");
  } catch (error) {
    try {
      stop();
    } catch (cleanupError) {
      throw new AggregateError([error, cleanupError], "PostgreSQL fixture startup and cleanup failed");
    }
    throw error;
  }
}
