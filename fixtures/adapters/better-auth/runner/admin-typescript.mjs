import { createServer } from "node:http";
import { once } from "node:events";
import { readFile } from "node:fs/promises";
import { pathToFileURL } from "node:url";

const runtime = await import(pathToFileURL(process.env.RIFFDB_CONFORMANCE_TYPESCRIPT_RUNTIME));
const generated = await import(pathToFileURL(process.env.RIFFDB_CONFORMANCE_TYPESCRIPT_GENERATED));
const { DriverApplicationTransport, DriverGeneratedApplicationTransport } = runtime;
const { BetterAuthAcceptanceClient } = generated;

const readIdentity = async (path) => {
  const document = JSON.parse(await readFile(path, "utf8"));
  return { ...document, contractVersion: BigInt(document.contractVersion) };
};
const adminDriver = await DriverApplicationTransport.connect({
  socketPath: process.env.RIFFDB_BETTER_AUTH_ADMIN_SOCKET,
  identity: await readIdentity(process.env.RIFFDB_BETTER_AUTH_ADMIN_IDENTITY),
});
const writerDriver = await DriverApplicationTransport.connect({
  socketPath: process.env.RIFFDB_BETTER_AUTH_WRITER_SOCKET,
  identity: await readIdentity(process.env.RIFFDB_BETTER_AUTH_WRITER_IDENTITY),
});
const admin = new BetterAuthAcceptanceClient(new DriverGeneratedApplicationTransport(adminDriver), 3);
const writer = new BetterAuthAcceptanceClient(new DriverGeneratedApplicationTransport(writerDriver), 3);
const require = (condition, label) => {
  if (!condition) throw new Error(`TypeScript Better Auth admin assertion: ${label}`);
};
const encode = (value) => JSON.stringify(value, (_key, item) => typeof item === "bigint" ? item.toString() : item);
const organizationId = process.env.RIFFDB_BETTER_AUTH_ADMIN_ORGANIZATION_ID;
const filteredUserId = process.env.RIFFDB_BETTER_AUTH_FILTER_USER_ID;
const concurrentUserId = process.env.RIFFDB_BETTER_AUTH_CONCURRENT_USER_ID;
const id = (value) => `018f0f8b-7c6d-7e31-8a4f-${value.toString(16).padStart(12, "0")}`;

const executeAdminQuery = async ({ mode, needle, userId, limit, offset }) => {
  const parameters = {
    organization_id: organizationId,
    needle,
    user_id: userId,
    limit,
    offset: BigInt(offset),
  };
  const invoke = () => {
    if (mode === "contains") return admin.adminUsersContainsAsc(parameters);
    if (mode === "starts_with") return admin.adminUsersStartsWithAsc(parameters);
    if (mode === "ends_with_desc") return admin.adminUsersEndsWithDesc(parameters);
    throw new Error("unsupported admin route mode");
  };
  for (let attempt = 0; attempt < 100; attempt += 1) {
    try {
      return await invoke();
    } catch (error) {
      const message = String(error);
      const lifecycleRetry = message.includes("RDB-QUERY-0102") || message.includes("RDB-PROJECTION-0103");
      if (!lifecycleRetry || attempt === 99) throw error;
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
  }
  throw new Error("unsupported admin route mode");
};

const server = createServer(async (request, response) => {
  try {
    const url = new URL(request.url ?? "/", "http://127.0.0.1");
    if (request.method !== "GET" || url.pathname !== "/admin/users") {
      response.writeHead(404).end();
      return;
    }
    const result = await executeAdminQuery({
      mode: url.searchParams.get("mode") ?? "contains",
      needle: url.searchParams.get("needle") ?? "@example.test",
      userId: url.searchParams.has("user_id") ? url.searchParams.get("user_id") : null,
      limit: Number(url.searchParams.get("limit") ?? "50"),
      offset: url.searchParams.get("offset") ?? "0",
    });
    response.writeHead(200, { "content-type": "application/json" });
    response.end(encode(result.value));
  } catch (error) {
    response.writeHead(500, { "content-type": "application/json" });
    response.end(encode({ error: error instanceof Error ? error.message : "unknown" }));
  }
});

const fetchPage = async (port, parameters) => {
  const query = new URLSearchParams(parameters);
  const response = await fetch(`http://127.0.0.1:${port}/admin/users?${query}`);
  const value = await response.json();
  require(response.status === 200, `admin route returned ${response.status}: ${encode(value)}`);
  return value;
};

try {
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const address = server.address();
  require(typeof address === "object" && address !== null, "HTTP address");
  const port = address.port;

  const first = await fetchPage(port, { mode: "contains", needle: "@example.test", limit: "2", offset: "0" });
  require(first.outcome === "Found" && first.users.length === 2 && first.total.value === "3", "multi-page exact total");
  require(first.users[0].email === "alpha@example.test" && first.users[1].email === "alpine@example.test", "ascending page order");

  const empty = await fetchPage(port, { mode: "contains", needle: "@example.test", user_id: filteredUserId, offset: "1" });
  require(empty.outcome === "Found" && empty.users.length === 0 && empty.total.value === "1", "exact-end filtered page");

  const atEnd = await fetchPage(port, { mode: "contains", needle: "@example.test", limit: "2", offset: "3" });
  require(atEnd.users.length === 0 && atEnd.total.value === "3", "offset at end");
  const beyond = await fetchPage(port, { mode: "contains", needle: "@example.test", limit: "499", offset: "4096" });
  require(beyond.users.length === 0 && beyond.total.value === "3", "maximum offset beyond end");

  const starts = await fetchPage(port, { mode: "starts_with", needle: "al", offset: "0" });
  require(starts.users.length === 2 && starts.total.value === "2", "starts-with shape");
  const unicode = await fetchPage(port, { mode: "contains", needle: "éta@", offset: "0" });
  require(unicode.users.length === 1 && unicode.users[0].email === "béta@example.test", "Unicode byte boundary");
  const descending = await fetchPage(port, { mode: "ends_with_desc", needle: "example.test", offset: "0" });
  require(descending.users.length === 3 && descending.total.value === "3", "ends-with descending shape");
  require(descending.users[0].email > descending.users[1].email, "descending value order");

  const filtered = await fetchPage(port, { mode: "contains", needle: "@example.test", user_id: filteredUserId, offset: "0" });
  require(filtered.users.length === 1 && filtered.total.value === "1" && filtered.users[0].user_id === filteredUserId, "typed optional filter");
  let unauthorized = false;
  try {
    await writer.adminUsersContainsAsc({
      organization_id: organizationId,
      needle: "@example.test",
      user_id: null,
      limit: 50,
      offset: 0n,
    });
  } catch {
    unauthorized = true;
  }
  require(unauthorized, "ordinary application role cannot invoke the admin query");
  const explicitNull = await admin.adminUsersContainsAsc({
    organization_id: organizationId,
    needle: "@example.test",
    user_id: null,
    limit: 50,
    offset: 0n,
  });
  require(explicitNull.value.users.length === 3 && explicitNull.value.total.value === 3n, "explicit null filter omission");

  const create = writer.createUserAccountSessions({
    request_id: id(905),
    signups: [{
      organization_id: organizationId,
      user_id: concurrentUserId,
      account_id: id(906),
      session_id: id(907),
      email: "concurrent@example.test",
      provider: "password",
      provider_account_id: "concurrent@example.test",
      token_digest: "sha256:admin-concurrent",
      expires_at: { seconds: 2_000_000_000n, nanos: 0 },
    }],
  });
  const during = executeAdminQuery({ mode: "contains", needle: "@example.test", userId: null, limit: 50, offset: 0 });
  const [created, concurrentPage] = await Promise.all([create, during]);
  require(created.outcome.outcome === "UserAccountSessionsCreated", "concurrent generated write");
  require([3n, 4n].includes(concurrentPage.value.total.value), "concurrent snapshot total");
  require(BigInt(concurrentPage.value.users.length) === concurrentPage.value.total.value, "page and total share one snapshot");
  const after = await fetchPage(port, { mode: "contains", needle: "@example.test", offset: "0" });
  require(after.users.length === 4 && after.total.value === "4", "post-write current snapshot");

  console.log(encode({
    schema: "riffdb.better-auth-admin-observation/v1",
    transport: "public_tls",
    route: "GET /admin/users",
    generated_typescript: true,
    exact_total: true,
    direct_offset: true,
    typed_filter: true,
    operator_family: ["contains", "ends_with", "starts_with"],
    order_family: ["value_asc_key_asc", "value_desc_key_asc"],
    concurrent_snapshot: true,
    unicode_binary_utf8: true,
  }));
} finally {
  server.close();
  await Promise.race([once(server, "close"), new Promise((resolve) => setTimeout(resolve, 1000))]);
  await adminDriver.shutdown();
  await writerDriver.shutdown();
}
