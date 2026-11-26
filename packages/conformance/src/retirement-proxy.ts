import { createServer, request } from "node:http";

/** Hold the engine's DELETE before forwarding it, keeping the real closed stream readable. */
export async function holdStreamDeletion(upstreamUrl: string) {
  const upstream = new URL(upstreamUrl);
  let heldPath: string | undefined;
  let deletionStarted = false;
  const held = new Set<() => void>();
  const release = () => {
    heldPath = undefined;
    for (const forward of held) forward();
    held.clear();
  };
  const server = createServer((incoming, outgoing) => {
    const target = new URL(incoming.url ?? "/", upstream);
    const forward = () => {
      const forwarded = request(
        target,
        { method: incoming.method, headers: { ...incoming.headers, host: upstream.host } },
        (response) => {
          outgoing.writeHead(response.statusCode ?? 502, response.headers);
          response.pipe(outgoing);
        },
      );
      forwarded.on("error", (error) => {
        if (!outgoing.headersSent) outgoing.writeHead(502);
        outgoing.end(String(error));
      });
      outgoing.on("close", () => forwarded.destroy());
      incoming.pipe(forwarded);
    };
    if (incoming.method === "DELETE" && target.pathname === heldPath) {
      deletionStarted = true;
      held.add(forward);
      outgoing.on("close", () => held.delete(forward));
    } else forward();
  });
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("retirement proxy did not bind");
  return {
    url: `http://127.0.0.1:${address.port}`,
    hold: (path: string) => {
      heldPath = `/${path}`;
      deletionStarted = false;
    },
    deletionStarted: () => deletionStarted,
    release,
    close: async () => {
      release();
      server.closeAllConnections();
      await new Promise<void>((resolve, reject) => server.close((error) => (error ? reject(error) : resolve())));
    },
  };
}
