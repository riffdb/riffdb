# Driver Host

`riffdb-driverd` is the long-lived Rust transport owner for generated
server-side TypeScript and Go bindings. It connects to one remote RiffDB
application over verified TLS, retains the protected capability credential,
pools HTTP/2 connections, and exposes only the intersection of one exact
generated application catalog and the selected symbolic role over a private
Unix socket. An operation generated for a different role is absent rather than
being sent remotely and rejected after the fact.

The host is not a generic gRPC proxy. Its local protocol cannot express kernel
entity/index reads, administration, contract deployment, ad-hoc RiffQL, raw
Protobuf, credentials, remote endpoints, numeric compiler IDs, field masks, or
arbitrary methods.

## Configuration

Pass exactly one protected TOML path to `riffdb-driverd`. The file must be a
regular, non-symlink file with no group or other permissions. Every configured
path is absolute and normalized.

```toml
[driver]
socket = "/run/myapp/riffdb-driver.sock"

[application]
lock_file = "/srv/myapp/riffdb.application.lock.json"
manifest_file = "/srv/myapp/generated/riffdb.application.exact.json"
operation_catalog_file = "/srv/myapp/generated/mcp/tools.json"
database = "ea"
role = "EaApplication"

[remote]
endpoint = "https://riffdb.internal:7443"
tls_trust_root = "/run/secrets/riffdb-ca.pem"
tls_server_name = "riffdb.internal"
credential_file = "/run/secrets/ea-application.credential"
pool_connections = 4
streams_per_connection = 64
```

The socket parent directory must already exist, be a real directory, and have
no group or other permission bits. The socket is created as mode `0600`.
Another live owner, a symlink, or a regular file at the path stops startup. A
private same-owner stale socket can be reclaimed. Linux peer credentials are
checked before a request is read.

The endpoint is always HTTPS. There is no cleartext fallback, native-root mode,
trust-all mode, client-certificate setting, cipher-suite setting, or TLS
provider setting on this surface.

## Exact handshake

Before binding the socket, the host authenticates across every pooled TLS
channel and verifies the database alias, active contract lineage/version, and
bundle hash against the exact lock. Every local connection then begins with
protocol V2 `handshake`. Both sides prove:

- protocol version and driver build identity;
- application manifest, active contract, generated module/operation catalog,
  and per-invocation schema hashes;
- selected database alias and symbolic role-definition hash;
- application value and structured-error registry hashes; and
- a digest of the host-configured verified remote endpoint identity.

The local protocol never carries the endpoint or bearer credential itself. A
mismatch closes the application session before any remote operation runs.

`generated/mcp/tools.json` retains the least-sufficient V2 through V5 profile
when no generated command is present. A nonempty command registry selects the
strict V6 successor and records that predecessor profile. V6 command entries
bind the compiler's command ID and underscore MCP name, exact input and outcome
schemas, the service-owned operation envelope, and the mechanically composed
hosted output schema. Command and visible-query `mcp_descriptor` objects contain
only `name`, `inputSchema`, `outputSchema`, and the four standard safety hints.
The catalog is a generated expectation for parity checking; the live policy
grant still decides visibility and the service still owns dispatch.

Moving an exact package to V6 changes its MCP artifact hash and enclosing lock
identity but not the application-lock schema. Preview and review that hash
change before `application lock --write`; old V2 through V5 catalogs remain
readable and carry no V6 parity claim. Reactive entries retain their closed
operation kind and action, and contextual reactions still pin the declared
reaction and target command identity.

## Calls, batches, and backpressure

An `invoke` selects one generated operation and repeats the exact generated
input-schema hash. Values use the shared tagged application-value registry;
integers and exact decimals never pass through floating point. Fixed-dimension
vectors cross this local JSON boundary as bounded arrays of exact IEEE-754
binary32 component bits; the host reconstructs and revalidates the canonical
finite vector before application dispatch. Query calls may carry an opaque
cursor, a read-after-commit frontier, and the one closed `admission_head`
consistency value. Deadline and retry bounds are explicit. Driver protocol V4
adds that stronger value; V1 through V3 remain readable but fail closed rather
than accept or downgrade it.

For a compiler-proven covering named query, a generated client also advertises
that it accepts the V2 compact result arm. The host returns the compiler-sealed
entity symbol, field order once, and bounded positional rows. The generated
operation-specific decoder checks that exact outcome, result field, entity,
layout, row width, value type, and bound before constructing typed results
directly. Callers cannot request a physical index or supply a layout. Queries
without a complete safe cover, and older clients that do not opt in, continue
to receive the legacy named-record result. CLI and MCP also retain that legacy
shape.

A `batch` selects one generated command, at most 4,096 independently
idempotent inputs, concurrency from 1 through 384, and a contiguous resume
checkpoint. Each item remains an ordinary command with its own durable outcome
and error. The batch is not one atomic transaction and cannot submit arbitrary
writes.

Pool connections, HTTP/2 stream admission, local connection count,
per-connection operations, response queues, and frame sizes are bounded.
Saturation returns the typed `RDB-CAPACITY-0101` result rather than creating an
unbounded queue. A peer has five minutes to complete its initial handshake.
After an exact handshake succeeds, the session remains attached until the
caller closes it or the host drains. This matters for generated applications
whose least-authority role sessions may legitimately remain quiet for more than
five minutes; the 256-connection host ceiling bounds that retained resource.

## Cancellation and uncertainty

Local requests and batches have explicit identities and may be cancelled from
the same or another authenticated local session. Batch cancellation propagates
to each in-flight ordinary command. Cancelling a read stops waiting without a
commit claim. Cancelling or timing out a command after submission reports
`outcome_uncertain: true` and prescribes resolution with the same idempotency
identity. It never reports that the command failed merely because the local
caller stopped waiting.

On SIGINT/SIGTERM integration, the host stops accepting work, asks established
connections to drain, allows a bounded 30-second completion interval, then
closes the private socket. Remote command uncertainty remains represented by
the same structured result.

## Reactive operations

The same `invoke` envelope supports generated durable stream pulls,
acknowledgements, negative acknowledgements, seeks, status, contextual work
items and reactions, plus resumable live-query updates. Cursors, lease tokens,
and causation tokens remain opaque. A live-query invocation returns one typed
snapshot, patch, reset, checkpoint, or terminal update; the caller reconnects
with the returned cursor.

Structured failures preserve the stable code, category, retryability, recovery
action, checked operation and symbol path, authorized contract identity, trace
identity, incident identity, and command uncertainty flag. Local parsing never
classifies failures from prose.

## Reimport operator host

Application reimport uses a different process, socket, protocol, capability,
and target-language binding: `riffdb-operator-driverd`. It is not a mode of
`riffdb-driverd`, and the normal generated application client has no reimport
method. The Rust process owns verified TLS, the Capability V7 credential, the
exact portability artifacts, and the campaign identity. A target-language
operator sees only the protected socket and the public handshake identity.

The operator configuration is a protected regular TOML file (mode `0600`),
and every referenced path is absolute, normalized, non-symlinked, and
non-writable by group or other users:

```toml
[driver]
socket = "/run/riffdb/reimport-018f.sock"

[campaign]
database = "restored"
campaign_id = "018f2f85-3c20-7a31-8f11-112233445566"
contract_lineage = "orders"
scope = "whole_application"
portability_manifest = "/srv/orders/riffdb.portability.json"
export_manifest = "/srv/orders/export-manifest.json"
export_receipt = "/srv/orders/export-receipt.json"

[remote]
endpoint = "https://riffdb.internal:7443"
tls_trust_root = "/run/secrets/riffdb-ca.pem"
tls_server_name = "riffdb.internal"
credential_file = "/run/secrets/orders-reimport.credential"
```

Start the protected host with exactly that path:

```bash
riffdb-operator-driverd /run/riffdb/orders-reimport.toml
```

The V1 local protocol admits only `start`, `apply_page`, `status`, and
`cancel`. `start` must repeat the exact manifest and receipt already loaded by
the Rust host; `apply_page` carries one bounded canonical export page and its
exact hash. The host rejects alternate campaign, database, manifest, receipt,
page, cursor, or completion identities before invoking the server. It cannot
express an application command, raw row write, caller-selected idempotency,
remote endpoint, bearer credential, or storage operation.

The language bindings are deliberately separate from generated application
facades:

```go
session, err := operator.ConnectOperator(ctx, socketPath, operator.OperatorIdentity{
    Database: "restored", CampaignID: campaignID,
    PortabilityManifestHash: portabilityHash,
})
```

```ts
import { OperatorTransport } from "@riffdb/application/operator";

const operator = await OperatorTransport.connect(socketPath, {
  database: "restored", campaignId, portabilityManifestHash,
});
```

```python
from riffdb_application.operator import OperatorIdentity, OperatorTransport

operator = OperatorTransport.connect(
    socket_path,
    OperatorIdentity("restored", campaign_id, portability_manifest_hash),
)
```

One operator session is serial and bounded because it advances one durable
campaign. Closing the local connection does not cancel or roll back the
campaign; reconnect with the same identity and call `status`. Target-language
bindings decode structured public errors and never receive credentials or
secret diagnostic values.

## Cross-language conformance

The alpha freezes one shared corpus under `fixtures/driver`. Rust, Go,
TypeScript, and Python each create and replay a command, perform a
read-after-commit query, and observe the same typed idempotency-reuse failure
against one verified-TLS daemon, database, contract, and symbolic role. The
same gate revokes that role and proves the retained host no longer supplies
authority. Every language observes the same typed `RDB-AUTH-0215` capability
revocation rather than a generic authorization denial. Local installed-artifact cells cover bounded cancellation,
uncertainty, malformed frames, reactive cursor/lease handling, and shutdown.

Run the complete proof with:

```bash
./scripts/driver-conformance --all-languages --remote
```

`fixtures/driver/manifests/*-v1.json` states each language's exact transport,
platform claims, feature availability, and corpus digest. An unsupported cell
is named explicitly; it is not filled by handwritten application glue.

The POC driver host is Linux-only. Generated Go and long-lived TypeScript
bindings speak this protocol through one retained local session. Their public
runtimes expose only the protected socket and public exact-handshake identity;
they have no endpoint, TLS, bearer-credential, gRPC, remote retry classifier,
kernel, or administration surface. Application authors must not hand-write
protocol frames. See [Go Applications](GO.md) and
[TypeScript Applications](../getting-started/TYPESCRIPT-APPLICATIONS.md).
