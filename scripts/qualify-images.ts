import { execFileSync } from "node:child_process";
import { appendFileSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

// Keep the IDs produced by this build. A concurrent local build can move a tag.
const directory = mkdtempSync(join(tmpdir(), "circuits-images-"));
const images = ["engine", "durable-streams"] as const;
const labels: string[] = [];
// Podman forwards the host proxy variables into build stages. A loopback proxy
// is reachable there only when the build shares the host network namespace.
const loopbackProxy = ["http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "all_proxy"].some(
  (name) => /^\w+:\/\/(?:[^/@]*@)?(?:127\.0\.0\.1|localhost|\[::1\])(?=[:/]|$)/i.test(process.env[name] ?? ""),
);
const { GITHUB_SHA, GITHUB_SERVER_URL, GITHUB_REPOSITORY, GITHUB_REF_TYPE, GITHUB_REF_NAME, GITHUB_OUTPUT } =
  process.env;
if (GITHUB_SHA && GITHUB_SERVER_URL && GITHUB_REPOSITORY) {
  const version = GITHUB_REF_TYPE === "tag" ? GITHUB_REF_NAME : `sha-${GITHUB_SHA.slice(0, 7)}`;
  labels.push(
    "--label",
    `org.opencontainers.image.source=${GITHUB_SERVER_URL}/${GITHUB_REPOSITORY}`,
    "--label",
    `org.opencontainers.image.revision=${GITHUB_SHA}`,
    "--label",
    `org.opencontainers.image.version=${version}`,
  );
}

try {
  const ids = images.map((name) => {
    const file = join(directory, name);
    execFileSync(
      "podman",
      [
        "build",
        ...(loopbackProxy ? ["--network", "host"] : []),
        "--platform",
        "linux/amd64",
        "--file",
        `container/Containerfile.${name}`,
        "--tag",
        `localhost/circuits-${name}:candidate`,
        "--iidfile",
        file,
        ...labels,
        ".",
      ],
      { stdio: "inherit" },
    );
    return readFileSync(file, "utf8").trim();
  });

  console.log(`Testing candidate images. Reproduce with bun scripts/smoke-images.ts ${ids.join(" ")}`);
  execFileSync(process.execPath, ["scripts/smoke-images.ts", ...ids], { stdio: "inherit" });
  if (GITHUB_OUTPUT) {
    appendFileSync(GITHUB_OUTPUT, `engine=${ids[0]}\nstreams=${ids[1]}\n`);
  }
} finally {
  rmSync(directory, { recursive: true, force: true });
}
