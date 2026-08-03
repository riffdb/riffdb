import { readFile } from "node:fs/promises";
import { createServer, type ServerResponse } from "node:http";
import { resolve } from "node:path";

import { CliApplicationTransport } from "@riffdb/application";

import {
  TicketDeskReactiveClient,
  type ReactiveApplicationTransport,
  createTicketPageWatchSseRelay,
  createTicketQueueWatchSseRelay,
} from "./generated/client.js";

const [endpoint, credentialFile, riffdbPath] = process.argv.slice(2);
if (endpoint === undefined || credentialFile === undefined || riffdbPath === undefined) {
  throw new Error("usage: server ENDPOINT CREDENTIAL_FILE RIFFDB_PATH");
}

const session = process.env["RIFFDB_TICKETDESK_SESSION"] ?? "ticketdesk-local-session";
const transport = new CliApplicationTransport({ riffdbPath, endpoint, credentialFile });
const application = new TicketDeskReactiveClient(
  transport as unknown as ReactiveApplicationTransport,
);
const publicRoot = resolve(process.cwd(), "public");

const server = createServer(async (request, response) => {
  try {
    const url = new URL(request.url ?? "/", "http://127.0.0.1");
    if (request.method === "GET" && url.pathname === "/") {
      response.setHeader("set-cookie", `ticketdesk_session=${session}; HttpOnly; SameSite=Strict; Path=/`);
      await serve(response, "index.html", "text/html; charset=utf-8");
      return;
    }
    if (request.method === "GET" && url.pathname === "/app.css") {
      await serve(response, "app.css", "text/css; charset=utf-8");
      return;
    }
    if (request.method === "GET" && url.pathname === "/app.js") {
      await serve(response, "app.js", "text/javascript; charset=utf-8");
      return;
    }
    if (request.method === "GET" && url.pathname === "/events/queue") {
      requireSession(request.headers.cookie);
      await stream(
        response,
        createTicketQueueWatchSseRelay(
          async () => hasSession(request.headers.cookie),
          application.watchTicketQueueWatch({
            organization_id: required(url, "organization_id"),
            project_id: required(url, "project_id"),
          }),
        ),
      );
      return;
    }
    if (request.method === "GET" && url.pathname === "/events/ticket") {
      requireSession(request.headers.cookie);
      await stream(
        response,
        createTicketPageWatchSseRelay(
          async () => hasSession(request.headers.cookie),
          application.watchTicketPageWatch({
            organization_id: required(url, "organization_id"),
            ticket_id: required(url, "ticket_id"),
          }),
        ),
      );
      return;
    }
    response.writeHead(404).end();
  } catch {
    if (!response.headersSent) {
      response.writeHead(400, { "content-type": "application/json" });
    }
    response.end('{"error":"request rejected"}');
  }
});

server.listen(0, "127.0.0.1", () => {
  const address = server.address();
  if (address === null || typeof address === "string") throw new Error("invalid address");
  process.stdout.write(`ticketdesk-reactive-web-ready-v1\t${address.port}\n`);
});

async function serve(response: ServerResponse, name: string, contentType: string): Promise<void> {
  const body = await readFile(resolve(publicRoot, name));
  response.writeHead(200, { "cache-control": "no-store", "content-type": contentType });
  response.end(body);
}

async function stream(response: ServerResponse, frames: AsyncIterable<string>): Promise<void> {
  response.writeHead(200, {
    "cache-control": "no-cache, no-transform",
    connection: "keep-alive",
    "content-type": "text/event-stream",
    "x-accel-buffering": "no",
  });
  for await (const frame of frames) response.write(frame);
  response.end();
}

function required(url: URL, name: string): string {
  const value = url.searchParams.get(name);
  if (value === null || value.length === 0 || value.length > 128) throw new Error("invalid parameter");
  return value;
}

function hasSession(cookie: string | undefined): boolean {
  return cookie?.split(";").some((part) => part.trim() === `ticketdesk_session=${session}`) ?? false;
}

function requireSession(cookie: string | undefined): void {
  if (!hasSession(cookie)) throw new Error("unauthorized");
}
