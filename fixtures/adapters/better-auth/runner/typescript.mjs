import { readFile } from "node:fs/promises";
import { pathToFileURL } from "node:url";

const runtime = await import(pathToFileURL(process.env.RIFFDB_CONFORMANCE_TYPESCRIPT_RUNTIME));
const generated = await import(pathToFileURL(process.env.RIFFDB_CONFORMANCE_TYPESCRIPT_GENERATED));
const { DriverApplicationTransport, DriverGeneratedApplicationTransport } = runtime;
const { BetterAuthAcceptanceClient } = generated;

const identityDocument = JSON.parse(await readFile(process.env.RIFFDB_CONFORMANCE_IDENTITY, "utf8"));
const identity = { ...identityDocument, contractVersion: BigInt(identityDocument.contractVersion) };
const driver = await DriverApplicationTransport.connect({
  socketPath: process.env.RIFFDB_CONFORMANCE_SOCKET,
  identity,
});
const id = (value) => `018f0f8b-7c6d-7e31-8a4f-${value.toString(16).padStart(12, "0")}`;
const require = (condition, label) => { if (!condition) throw new Error(`TypeScript Better Auth cascade assertion: ${label}`); };

try {
  const client = new BetterAuthAcceptanceClient(new DriverGeneratedApplicationTransport(driver), 3);
  const organization_id = id(301);
  const user_id = process.env.RIFFDB_BETTER_AUTH_USER_ID;
  const session_id = id(304);
  const future = { seconds: 2_000_000_000n, nanos: 0 };
  const signup = {
    request_id: id(310),
    signups: [{
      email: "typescript@example.test", user_id, provider: "password",
      account_id: id(303), expires_at: future, session_id,
      token_digest: "sha256:typescript-session", organization_id,
      provider_account_id: "typescript@example.test",
    }],
  };
  const created = await client.createUserAccountSessions(signup);
  require(created.outcome.outcome === "UserAccountSessionsCreated", "typed signup");
  const session = await client.getSession({ organization_id, user_id, session_id });
  require(session.value.outcome === "Found" && session.value.session.token_digest === "sha256:typescript-session", "named secret read");
  const user = await client.getUser({ organization_id, user_id });
  require(user.value.outcome === "Found", "named exact read");

  const verification_token_id = id(320);
  const issued = await client.issueVerificationToken({
    user_id, expires_at: future, request_id: id(321),
    token_digest: "sha256:typescript-verification", organization_id, verification_token_id,
  });
  require(issued.outcome.outcome === "VerificationTokenIssued", "token issue");
  const consume = { user_id, request_id: id(322), organization_id, verification_token_id };
  const consumed = await client.consumeVerificationToken(consume);
  const replayedConsume = await client.consumeVerificationToken(consume);
  require(consumed.outcome.outcome === "VerificationTokenConsumed" && replayedConsume.replayed, "atomic consume replay");

  const deletion = { user_ids: [user_id], request_id: id(330), organization_id };
  const deleted = await client.deleteUsers(deletion);
  const replayedDelete = await client.deleteUsers(deletion);
  require(deleted.outcome.outcome === "UserAccountsDeleted" && replayedDelete.replayed, "cascade replay");
  const missingSession = await client.getSession({ organization_id, user_id, session_id });
  require(missingSession.value.outcome === "Missing", "session cleanup");

  const recreated = await client.createUserAccountSessions({ ...signup, request_id: id(340) });
  require(recreated.outcome.outcome === "UserAccountSessionsCreated", "recreate");
  for (let ordinal = 0; ordinal < 9; ordinal += 1) {
    const overflowToken = await client.issueVerificationToken({
      user_id, expires_at: future, request_id: id(350 + ordinal),
      token_digest: `sha256:typescript-overflow-${ordinal}`, organization_id,
      verification_token_id: id(370 + ordinal),
    });
    require(overflowToken.outcome.outcome === "VerificationTokenIssued", "overflow setup");
  }
  const overflow = await client.deleteUsers({ user_ids: [user_id], request_id: id(390), organization_id });
  require(overflow.outcome.outcome === "CascadeLimitExceeded", "typed overflow");
  const retained = await client.getUser({ organization_id, user_id });
  require(retained.value.outcome === "Found", "overflow zero mutation");

  console.log(JSON.stringify({
    schema: "riffdb.adapter-cascade-observation/v1", language: "typescript",
    signup: true, named_exact_read: true, named_secret_read: true,
    session_account_cleanup: true, bounded_full_user_delete: true,
    atomic_token_consume: true, idempotent_replay: true,
    typed_overflow: true, overflow_zero_mutation: true,
  }));
} finally {
  await driver.shutdown();
}
