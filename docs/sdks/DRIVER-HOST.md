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
protocol V1 `handshake`. Both sides prove:

- protocol version and driver build identity;
- application manifest, active contract, generated module/operation catalog,
  and per-invocation schema hashes;
- selected database alias and symbolic role-definition hash;
- application value and structured-error registry hashes; and
- a digest of the host-configured verified remote endpoint identity.

The local protocol never carries the endpoint or bearer credential itself. A
mismatch closes the application session before any remote operation runs.

`generated/mcp/tools.json` now uses
`riffdb-generated-application-operations/v2`. In addition to JSON schemas and
transport-safe names, each entry carries its exact contract/query/reactive
source symbol. Reactive entries also carry their closed operation kind and
action; contextual reaction entries pin the declared reaction and target
command identity. Descriptions and normalized names are not dispatch
authority.

## Calls, batches, and backpressure

An `invoke` selects one generated operation and repeats the exact generated
input-schema hash. Values use the shared tagged application-value registry;
integers and exact decimals never pass through floating point. Query calls may
carry an opaque cursor and read-after-commit frontier. Deadline and retry
bounds are explicit.

A `batch` selects one generated command, at most 4,096 independently
idempotent inputs, concurrency from 1 through 384, and a contiguous resume
checkpoint. Each item remains an ordinary command with its own durable outcome
and error. The batch is not one atomic transaction and cannot submit arbitrary
writes.

Pool connections, HTTP/2 stream admission, per-connection local operations,
response queues, frame sizes, and idle time are bounded. Saturation returns the
typed `RDB-CAPACITY-0101` result rather than creating an unbounded queue. An
idle local connection is closed after five minutes and can re-handshake.

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

## Cross-language conformance

The alpha freezes one shared corpus under `fixtures/driver`. Rust, Go,
TypeScript, and Python each create and replay a command, perform a
read-after-commit query, and observe the same typed idempotency-reuse failure
against one verified-TLS daemon, database, contract, and symbolic role. The
same gate revokes that role and proves the retained host no longer supplies
authority. Local installed-artifact cells cover bounded cancellation,
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
