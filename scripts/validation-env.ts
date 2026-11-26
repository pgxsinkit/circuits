import { execFileSync } from "node:child_process";
import { existsSync } from "node:fs";
import { delimiter } from "node:path";

/** Environment shared by native qualification and candidate-image checks. */
export function validationEnvironment(source: NodeJS.ProcessEnv = process.env): NodeJS.ProcessEnv {
  const postgresBin = "/usr/lib/postgresql/19/bin";
  const bypass = [
    ...new Set(
      [source.no_proxy, source.NO_PROXY, "localhost,127.0.0.1,::1"]
        .flatMap((list) => (list ?? "").split(","))
        .filter(Boolean),
    ),
  ].join(",");
  return {
    ...source,
    PATH: existsSync(`${postgresBin}/initdb`)
      ? [postgresBin, source.PATH].filter(Boolean).join(delimiter)
      : source.PATH,
    NO_PROXY: bypass,
    no_proxy: bypass,
  };
}

export function requirePostgres19(environment: NodeJS.ProcessEnv = validationEnvironment()): void {
  for (const binary of ["initdb", "pg_ctl"]) {
    const version = execFileSync(binary, ["--version"], { env: environment, encoding: "utf8" }).trim();
    if (!/\(PostgreSQL\) 19\./.test(version)) {
      throw new Error(`Validation requires PostgreSQL 19 server binaries on PATH; ${binary} resolved to ${version}`);
    }
    console.log(version);
  }
}
