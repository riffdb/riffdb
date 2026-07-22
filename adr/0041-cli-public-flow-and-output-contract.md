# ADR-0041: CLI Public Flow and Output Contract

- **Status:** Accepted
- **Direction approved:** 2026-07-22
- **Exact text accepted:** 2026-07-22
- **Accepted:** 2026-07-22
- **Acceptance reference:** `21a8cfb`
- **Requires:** ADR-0006, ADR-0007, ADR-0008, ADR-0009, ADR-0011, ADR-0018,
  ADR-0028, ADR-0031, ADR-0037, and ADR-0040
- **Amends:** SPEC Sections 5.2, 5.4, 16.1, 16.2, and 16.6; ADR-0009's
  credential-delivery ownership and exact `base64`/`zeroize` direct-owner rows
  only as expressly described below, without broadening its isolated
  `riffdb_auth::bootstrap_secret` CLI exception; ADR-0037's exact public-client
  dependency allowlist only to add `zeroize`; ADR-0040's WP-137 SDK ownership
  and required ADRs; and WP-150's exact dependency, credential, configuration,
  and output evidence; adds WP-137 as a WP-135 predecessor, adds WP-135 and
  WP-137 as WP-150 predecessors, freeze a launchable WP-135 public runner, and
  reserve a separately reviewed WP-155 before WP-200 for the required public
  backup/restore flow
- **Decision deadline:** Before WP-137 freezes the CLI-consumed
  credential/retry/error surface, or before WP-150 adds dependencies or merges
  a CLI configuration, credential, retry, or machine-output interface,
  whichever occurs first

The human maintainer accepted this exact text and its ADR-only companion
reconciliation on 2026-07-22, with revision `21a8cfb` as the acceptance
reference. The same governance change must reconcile the authoritative SPEC,
work-package registry, and DAG before implementation begins.

## Context

WP-150 must expose its assigned POC operator and budget-demo operations through
the public Rust SDK over loopback gRPC. It must not become a storage,
authentication, policy, service, or command-runtime path. The existing
authoritative material fixes that boundary and the bootstrap credential
procedure, but it does not yet freeze the CLI's dependency graph, bounded
configuration grammar, normal-token source, machine-readable representation,
or retry behavior.

Those choices are security and compatibility boundaries. A token accepted in
argv can appear in process listings. An unrestricted TOML or environment model
can introduce ambiguous credentials and unbounded parsing. Generic JSON
serialization can lose `u64` precision, erase RiffDB value variants, or change
when generated Protobuf code changes. A retry that replaces a request,
capability, or idempotency identity can create a second operation rather than
resolve the first one.

There are also three current boundary gaps:

1. ADR-0009 permits `riffdb-cli` to depend on only the isolated
   `riffdb_auth::bootstrap_secret` module. Its root
   `riffdb_auth::load_capability_token_file` function returns an auth-owned
   decoded token and is part of the server authentication boundary, so calling
   it from the CLI would contradict that accepted exception. Reimplementing
   normal protected-file handling in each public caller would also create
   divergent security boundaries. Normal credential-file delivery therefore
   needs one presentation-only owner in the public Rust client, shared by the
   CLI and MCP stdio, while the server remains the sole canonical decoder and
   authenticator.
2. SPEC Section 16.1 assigns backup and restore commands to the POC CLI, but no
   public API-neutral service or gRPC operation exists for either command.
   Implementing them in WP-150 would require direct database-file access or a
   new public and durable design outside WP-150's allowed paths. This is a
   missing POC work-package boundary, not permission to drop the commands from
   POC acceptance.
3. The public client's transport classification and checked public-error model
   are client-owned boundaries. WP-150 cannot safely copy their private logic,
   and its allowed paths cannot add the required client APIs. WP-137
   must therefore land the narrow operation-specific retry helpers and
   serialization-neutral public-error views before WP-150 starts. Retry
   classification remains private to the client.

## Decision

### Dependency and authority boundary

WP-137 is additionally a required predecessor of WP-135 so the public comparison
runner can load an exact protected normal credential without copying the
filesystem boundary. WP-135 and WP-137 are required predecessors of WP-150.
WP-135 freezes the public comparison adapter and workload preflight that WP-150
may launch without editing comparison-owned sources. In addition to its public
gRPC parity work, WP-137 owns the public Rust client's operation-specific retry
helpers for Execute, bootstrap capability create, and normal capability create.
Their closed classifications remain private to the helpers. It also owns
checked, serialization-neutral public-error views or
re-exports sufficient to render the accepted public code, safe message, recovery
action, structured details, and optional incident ID. Those interfaces remain
SDK-owned even though WP-150 owns its versioned CLI output DTOs.

WP-137 exposes no generic arbitrary-RPC retry helper, callback-based retry
engine, status-only retry predicate, raw generated error message, or unchecked
transport detail. WP-150 consumes only the three helpers' checked terminal
responses or `ClientError` values and the SDK-owned public-error surface. It
neither receives nor duplicates their private classification and does not import
a private client module to reach it.

`riffdb-cli` remains a safe-Rust binary/library package and has exactly these
direct production dependencies for WP-150:

```toml
base64 = { version = "=0.22.1", default-features = false, features = ["alloc"] }
clap = { version = "=4.6.3", default-features = false, features = ["derive", "std", "help", "usage", "error-context"] }
riffdb-auth = { version = "0.1.0", path = "../riffdb-auth", default-features = false }
riffdb-client-rust = { version = "0.1.0", path = "../riffdb-client-rust", default-features = false }
serde = { version = "=1.0.229", default-features = false, features = ["derive", "std"] }
serde_json = { version = "=1.0.150", default-features = false, features = ["std"] }
tokio = { version = "=1.52.0", default-features = false, features = ["macros", "rt-multi-thread"] }
toml = { version = "=1.1.3", default-features = false, features = ["parse", "serde", "std"] }
zeroize = { version = "=1.8.1", default-features = false, features = ["alloc"] }
```

This decision makes the following exact amendments to ADR-0009's direct-owner table;
versions and features remain byte-for-byte unchanged and no other owner or
purpose is added:

| Dependency | Complete direct first-party owner set after acceptance | Added purpose |
|---|---|---|
| `base64` | `riffdb-auth`, `riffdb-proto`, `riffdb-service`, `riffdb-api-mcp`, `riffdb-cli` | Auth-only credential decoding; Proto structural outcome-locator validation; service-owned canonical outcome-locator tuple encoding/decoding; MCP locator/Value/opaque-byte presentation; and CLI structural machine input/output bytes |
| `zeroize` | `riffdb-auth`, `riffdb-client-rust`, `riffdb-cli` | bounded public-delivery credential buffers only |

ADR-0037's exact `riffdb-client-rust` allowlist gains only that same reviewed
`zeroize` edge. The public client gains no direct `base64` or `riffdb-auth`
dependency, and neither the client nor CLI gains token encoding or decoding
ownership.

Default features remain empty. The CLI has no direct Tonic dependency and uses
`riffdb-client-rust` for every database request. It has no direct dependency on
or source import from
`riffdb-api-grpc`, `riffdb-catalog`, `riffdb-commit`, `riffdb-conflict`,
`riffdb-contract-*`, `riffdb-errors`, `riffdb-idempotency`, `riffdb-policy`,
`riffdb-proto`, `riffdb-runtime`, `riffdb-server`, `riffdb-service`,
`riffdb-storage-*`, or `riffdb-types`. A future direct foundational type edge
requires its own review rather than being hidden behind this decision.
The isolated `riffdb-auth` crate edge has existing transitive storage/type
dependencies; that transitivity grants no CLI import or symbol access beyond
the exact `bootstrap_secret` architecture boundary below.

The exact 2026-07-22 scratch lock-graph review of this direct allowlist is
relative to the then-current root lock, builds on Rust 1.97.0, passed
`cargo deny check` for advisories, licenses, bans, and sources, and found no
native `links`, `-sys`, `cc`, CMake, or bindgen edge. It adds no duplicate-
version family; the existing `hashbrown` and `syn` duplicates remain. Newly
resolved build scripts are limited to `serde`, `serde_json`, and `zmij` and
perform no network or native-compiler work. The active third-party unsafe
inventory added by this graph is in `anstyle`, `clap_lex`, `serde_json`,
`winnow`, and `zmij`; enabling Tokio multithreading also expands already
accepted Tokio unsafe code. `toml_parser`'s optional unsafe path is disabled and
its active configuration conditionally forbids unsafe code. ADR-0008's rmcp
scratch selected serde_json 1.0.151 while this exact CLI edge pins 1.0.150, so
the combined WP-140/WP-150 graph is already known to require the fresh lock,
feature, build-script, native, license, and unsafe review. Accepting this record
accepts only the inventory above; every first-party crate remains
`#![forbid(unsafe_code)]`, and any changed resolved graph requires fresh human
review before the dependency or lockfile merges.

The sole auth-crate access is the isolated
`riffdb_auth::bootstrap_secret` module. Architecture tests reject every
root-level `riffdb-auth` function or type, every other module, every wildcard
import or re-export, and specifically
`riffdb_auth::load_capability_token_file`. In particular, the CLI cannot import
an authenticator, raw or decoded normal token, digest provider, capability
issuer, policy fact, storage reader, credential repository, or server
entropy/key API. The server's normal public gRPC path remains authoritative.

### Public-client protected normal credential loader

WP-137 adds exactly one protected normal-credential file loader at
`crates/riffdb-client-rust/src/credential_file.rs`, re-exported at the
`riffdb_client_rust` crate root with this exact signature:

```rust
pub fn load_protected_bearer_credential(
    path: &std::path::Path,
) -> Result<BearerCredential, BearerCredentialFileError>;
```

`BearerCredentialFileError` is a closed, public, copyable, redaction-safe enum
with exactly `UnsupportedPlatform`, `ProtectedFileRejected`, and
`InvalidPresentation`. Its `Display` and `Debug` contain fixed safe text and no
path, credential bytes, operating-system error, metadata, ownership value, or
source chain. The loader returns the existing redacted `BearerCredential`
directly. It exposes no string, byte slice, decoded token, callback, reader,
file handle, metadata value, or auth-owned type.

On Linux the loader implements ADR-0009's exact protected-file procedure. It
reads at most 65,537 bytes from `/proc/self/status`, requires EOF at or before
65,536 bytes, and accepts exactly one canonical `Uid:` line whose second of
four canonical `u32` fields is the effective user ID. It obtains final-component
`symlink_metadata`, requires a regular file, and opens with safe
`OpenOptionsExt::custom_flags(O_NOFOLLOW | O_NONBLOCK)`, using ADR-0009's exact
Linux values `O_NOFOLLOW = 0o00400000` and `O_NONBLOCK = 0o00004000`. The opened
handle must be regular, owned by that effective user, have `mode & 0o077 == 0`,
and have the same device/inode pair as the pre-open metadata. Only after those
checks does it read 44 bytes of capacity, require EOF after exactly 43 bytes,
and reject a line feed or any other trailing byte. Pre-open or open-time final
symlinks, special files, inode replacement, malformed or over-limit proc data,
wrong ownership or mode, short or long reads, and every I/O failure fail closed.
Intermediate-component symlinks retain ADR-0009's accepted POC treatment.
Non-Linux targets return `UnsupportedPlatform` before reading a credential.

The 43 bytes receive only the existing `BearerCredential::new` presentation
checks: exact length and the unpadded URL-safe alphabet. The loader deliberately
does not base64-decode, re-encode, hash, authenticate, infer a principal, or
consult capability state. The auth-owned server decoder remains the sole owner
of the full ADR-0009 canonical-token checks and rejects presentation-valid text
whose decoded form is not canonical. All loader-owned credential buffers are
bounded and zeroized on every return path. WP-137 adds the already reviewed
exact dependency
`zeroize = { version = "=1.8.1", default-features = false, features = ["alloc"] }`
to `riffdb-client-rust`; this adds no dependency to the CLI's direct allowlist.

For crash-safe normal-create reread verification, WP-137 also adds this sole
non-exposing comparison to `BearerCredential`:

```rust
#[must_use]
pub fn has_same_presentation(&self, other: &BearerCredential) -> bool;
```

It compares the complete checked authorization presentation for exact equality,
returns only a Boolean, and has no raw accessor or constant-time authentication
claim. The CLI and MCP stdio consume the same loader. Neither may reproduce its
filesystem checks or call an auth-owned normal-token loader.

### Bounded configuration

The effective client configuration has exactly four fields:

| Field | CLI flag | Environment | TOML key | Default |
|---|---|---|---|---|
| Endpoint | `--endpoint` | `RIFFDB_ENDPOINT` | `client.endpoint` | `http://127.0.0.1:7443` |
| Output mode | `--output` | `RIFFDB_OUTPUT` | `client.output` | `human` |
| Maximum attempts | `--max-attempts` | `RIFFDB_MAX_ATTEMPTS` | `client.max_attempts` | `3` |
| Normal credential file | `--credential-file` | `RIFFDB_CREDENTIAL_FILE` | `client.credential_file` | absent |

For each field, precedence is explicit flag, then the named environment
variable, then the explicit TOML document, then the table default. A flag or
environment value that is present but empty or invalid rejects; it never falls
through to a lower-precedence value. `output` is the closed lowercase set
`human` or `json`. `max_attempts` is a canonical unsigned decimal integer in
`1..=10`.

There is no home-directory, current-directory, XDG, platform, or parent-path
configuration discovery. A TOML file is read only when selected by `--config`
or, if that flag is absent, `RIFFDB_CONFIG`; the flag wins when both are set.
The complete file is at most 65,536 bytes, must be UTF-8, and has this sole
shape:

```toml
[client]
endpoint = "http://127.0.0.1:7443"
output = "human"
max_attempts = 3
credential_file = "/home/operator/.config/riffdb/token"
```

The table and every key are optional. Any other top-level item, table, key,
duplicate key/table, wrong type, invalid enum, or out-of-bound value rejects the
whole document. The parser consumes the whole document. Raw tokens and
bootstrap credential documents are forbidden TOML values.

Only the documented `RIFFDB_CONFIG`, `RIFFDB_ENDPOINT`, `RIFFDB_OUTPUT`,
`RIFFDB_MAX_ATTEMPTS`, `RIFFDB_CREDENTIAL_FILE`, and
`RIFFDB_CAPABILITY_TOKEN` names are read. Other `RIFFDB_*` variables are
ignored: enumerating and rejecting ambient process variables would make an
otherwise explicit invocation depend on unrelated launcher state. Adding a
new recognized name is a reviewed configuration-interface change.

Every path accepted as CLI input is at most 4,096 platform-encoded bytes. This
includes configuration, credential input/output, contract source, CLI JSON
input, demo, backup, restore, and any future CLI-owned input path. On the
POC's Linux path this is the exact `OsStrExt::as_bytes()` length. A TOML path
must also be valid UTF-8 by construction; argv paths need not be UTF-8. Empty
paths and paths containing a NUL byte reject. A future path category may not
silently choose a larger bound.

The endpoint is at most the existing 512-byte `Audience` bound and must be an
ASCII URI of the form `http://<literal-loopback-IP>:<nonzero-port>`. Its scheme
is exact lowercase `http`; its host parses as an IPv4 or IPv6 literal for which
`IpAddr::is_loopback()` is true; and it has an explicit decimal port in
`1..=65535`. User information, a DNS name, path (including a trailing slash),
query, fragment, whitespace, implicit port, HTTPS, and any other scheme reject
locally. The accepted endpoint text is passed byte-for-byte to the public
client; the CLI performs no URI normalization and does not inject or assert the
server's separately configured audience. Remote transport and TLS credential
policy remain outside the POC.

### Bounded command input and output

A complete contract source, CLI-owned JSON input document, or general stdin
document is at most 1,048,576 bytes before parsing. The 65,536-byte
configuration bound and the exact 132-byte bootstrap-document bound remain
tighter special cases. Readers enforce the applicable bound while reading and
probe for one excess byte; they do not first buffer an unbounded file or stdin
stream. UTF-8-required inputs reject invalid UTF-8 without lossy conversion.

The 1,048,576-byte JSON limit is an intentional aggregate CLI presentation cap,
not a claim that every value at an individual SDK scalar maximum has a JSON
spelling below that cap. Base64 expansion and JSON escaping can make an SDK-
valid byte or string value too large for this CLI input path. Such a document
fails locally as `input_too_large` even though a caller using another bounded
public SDK construction may represent the decoded value. Within the aggregate
cap, the CLI applies exactly the SDK's per-field, depth, cardinality, and
semantic validation and never tightens one silently. This presentation limit
does not change a contract, service, gRPC, or canonical-value maximum.

Recursive Values, records, lists, request collections, page limits, and string
or byte fields use the same depth, cardinality, and scalar bounds as the
corresponding checked public SDK validators. CLI parsing must reject a value
before request construction when those public bounds are exceeded. It must not
invent a looser parallel validator, silently truncate a collection, or use a
generic recursive JSON conversion that bypasses the checked public model.

The checked terminal output model supplied to either renderer is at most
4,194,304 bytes in its bounded canonical representation before rendering.
Rendering uses a bounded staging buffer and also rejects a JSONL or human
rendering that would exceed 4,194,304 bytes before writing any stdout byte.
Oversize input, model, or rendering produces a closed safe local error; it does
not emit a partial object, partial human result, server text, or debug value.

### Credential sources and retention

A normal bearer token may come from exactly one of:

1. `RIFFDB_CAPABILITY_TOKEN`, containing an exact 43-byte v1 token
   presentation and converted directly to
   `riffdb_client_rust::BearerCredential`; or
2. the resolved normal credential-file path, loaded only through
   `riffdb_client_rust::load_protected_bearer_credential`.

There is no raw-token flag, positional argument, TOML key, response echo,
diagnostic, trace field, or shell-command construction. Supplying both sources
rejects instead of silently choosing one. Environment storage itself cannot be
zeroized by the process. The CLI accepts the standard-library-returned value
only when its encoded form is exactly 43 bytes, creates no second raw copy before
that check, moves the accepted copy promptly into `Zeroizing`, constructs the
public client's redacted `BearerCredential`, and drops the raw copy. A
credential-file branch receives only `BearerCredential` and never receives file
text. The environment source is a local-development convenience, not a claim of
production secret custody. Presentation validation at either client source is
not authentication; the server performs the accepted canonical decode, digest
lookup, lifecycle checks, audience checks, and authorization.

Bootstrap credential generation and input use only ADR-0009's exact
`bootstrap_secret` APIs and exact 132-byte document. Input is bounded stdin or
an ADR-0009 protected file, never argv or TOML. Generation requires an explicit
output path and completes the exact retained-file procedure below before the
first RPC. A bootstrap request's public `CapabilityId` is the retained
document's exact ID.

`capability.bootstrap` additionally accepts optional
`--bearer-output <path>`. When present, the CLI derives no new credential: it
borrows the same retained canonical token presentation already needed for
bootstrap metadata, constructs a redacted public-client `BearerCredential`, and
durably writes the exact 43-byte presentation to a protected output file. In
generation mode, the generated 132-byte bootstrap output and optional 43-byte
bearer output are both created exclusively, file-synced, closed,
directory-synced, and protected-reread before any bootstrap RPC. In existing-
input mode, the bounded stdin document or existing ADR-0009 protected file is
never recreated or overwritten; the CLI retains its parsed credential in
memory for the complete helper call, and only the optional bearer output goes
through exclusive durable creation and reread. A protected-file input is loaded
through the existing auth-owned protected loader, while a stdin input is read
through the existing exact bounded reader. The bearer reread uses only
`riffdb_client_rust::load_protected_bearer_credential` and
`has_same_presentation`; the CLI performs no base64 operation, decode, digest,
or authentication. A failure leaves any already created file for explicit
operator handling, performs no automatic overwrite/delete, and makes no RPC.
This is the reviewed bridge from the retained bootstrap credential to later
ordinary authenticated public calls; it does not reissue or transform the
token. The acceptance demo always supplies this option.

A normal `capability create` also requires an explicit credential output path.
The token returned by the one successful normal-create response is never
printed. The CLI retains it using the same procedure, adjusted to the exact
43-byte normal-token document:

1. Create the leaf exclusively without overwrite using `create_new` and
   requested mode `0o600`.
2. Write the complete exact document and successfully call `sync_all` on the
   file.
3. Close the file, treating a close failure as an error where the platform
   reports one.
4. Open and verify the containing directory, successfully call `sync_all` on
   it, and close it.
5. For a bootstrap document, reread through the isolated ADR-0009
   `bootstrap_secret` loader and compare the complete exact document. For a
   normal credential, reread through
   `riffdb_client_rust::load_protected_bearer_credential` and require
   `has_same_presentation` against the checked credential constructed from the
   create-response token after promptly moving that token into `Zeroizing`.
   The latter Boolean proves exact checked presentation equality without
   returning file bytes.
6. Only then report command success, without including credential bytes.

For a bootstrap document, this procedure completes before any RPC. For a
normal-create response, it completes before reporting success. A create,
write, file-sync, close, directory-open, directory-sync, protected-reread, or
comparison failure is a local failure. The CLI never deletes or overwrites the
retained file automatically, never prints either credential form, and never
claims normal-create token recoverability after a response was lost.

### Public-only command flow

Every database operation, including bootstrap, contract validation and
deployment, command execution and outcome resolution, entity/commit/projection
inspection, capability administration, health, and demo orchestration, uses
`riffdb-client-rust` against the configured loopback endpoint. Local work is
limited to bounded argument/configuration/input parsing, output rendering,
credential generation or retention, and launching the already owned budget
comparison flow. The CLI does not open a database file, instantiate a service,
authenticate a principal, evaluate policy, compile or execute a command, or
construct an internal semantic result.

### WP-135 public runner handoff

WP-135 creates the nested-workspace package
`riffdb-budget-comparison-riffdb-grpc` and evidence binary
`riffdb-budget-public` under
`examples/budget-comparison/riffdb-grpc/`. It depends on WP-137 and uses only
the public Rust client/protocol/foundational value surface plus the existing
comparison core. Production code has no auth, service, server, policy, commit,
catalog, runtime, conflict, idempotency, storage, or compiler dependency. The
binary is a long-lived comparison/evidence runner, not a shipped RiffDB server
or a private CLI library.

The package exposes its adapter from `src/lib.rs`, its named runner from
`src/bin/riffdb-budget-public.rs`, and has only these normal dependency owners:
`riffdb-budget-comparison-core`, `riffdb-client-rust`, `riffdb-proto`,
`riffdb-types`, and these exact third-party rows:

```toml
tokio = { version = "=1.52.0", default-features = false, features = ["macros", "rt-multi-thread"] }
tonic = { version = "=0.14.6", default-features = false, features = ["channel", "codegen"] }
```

The root comparison process harness may use
its existing test-only bootstrap dependencies; none becomes a normal dependency
of the adapter package.

The exact subprocess invocation is:

```text
riffdb-budget-public \
  --protocol riffdb.budget.public-run/v1 \
  --case sequential|contention|same_key_replay \
  --endpoint http://<literal-loopback-ip>:<nonzero-port> \
  --credential-file <protected-43-byte-file>
```

It accepts no stdin, TOML, raw-token argument, bootstrap document, ambient
credential, database path, or internal service handle. Each case runs against a
fresh already bootstrapped database with the exact Budget contract active. The
adapter never resets storage or deploys a contract. A same-key replay test uses
explicit commit-notification synchronization and deliberately discards the first
response; final process-level post-commit connection-loss proof remains
WP-190/WP-200 evidence rather than a WP-135 failpoint claim.

After the executable name, argv is exactly the eight arguments shown above in
that exact flag order. The first seven are exact UTF-8 flag/value arguments;
the final credential path remains an `OsStr` and need not be UTF-8. Every flag
occurs once. A missing, duplicate, unknown, reordered, combined
`--flag=value`, positional, non-UTF-8 first-seven, empty, or additional argument
is invalid invocation. The complete platform-byte argv payload is at most 8,192
bytes. The protocol and case values must equal their shown closed spellings.
The endpoint is at most 512 bytes and passes the exact literal-loopback HTTP
grammar defined for the CLI. The credential path is nonempty, has no NUL, is at
most 4,096 platform bytes, and is loaded only through WP-137's protected
loader. The runner reads no environment variable and performs no configuration
or path discovery.

On success the runner writes exactly one compact JSON line, at most 4,096 bytes,
with this closed shape and key order:

```json
{"schema":"riffdb.budget.public-run/v1","adapter":"riffdb-public-grpc-v1","case":"sequential","workload_version":1,"status":"passed"}
```

Only the checked `case` spelling varies. Exit `0` requires that exact object and
shared-oracle success. Exit `1` is a checked API/oracle/preflight failure; exit
`2` is invalid invocation/configuration. Both failure exits write no stdout.
Exit `1` writes exactly `riffdb budget public run failed\n`; exit `2` writes
exactly `riffdb budget public invocation invalid\n`. A signal or other code is
invalid runner behavior. The three fixtures
`public-run-v1-success.jsonl`, `public-run-v1-checked-error.txt`, and
`public-run-v1-invalid-invocation.txt` under the package's `fixtures/` directory
freeze those complete stdout/stderr bytes. WP-150 parses this closed protocol
and never relays child stdout or stderr.

The exact CLI command is
`riffdb demo budget --runner <path> --case
sequential|contention|same_key_replay`, whose machine identity remains
`demo.budget`. The runner path is command-specific rather than configuration.
This command requires the resolved normal `--credential-file`/configuration
path; an environment-only bearer or bootstrap document returns fixed local
error `demo_requires_credential_file`. WP-150 launches the path directly with
no shell, clears the child environment, closes stdin, and passes only the exact
protocol, case, endpoint, and normal credential-file path. It drains stdout and
stderr concurrently with independent 4,096-byte limits, kills and reaps the
child after a fixed 180-second deadline or either overflow, accepts only the
exit/output combinations above, and renders its own checked
`riffdb.cli.output/v1` result. Correctness tests inject process and deadline
hooks rather than sleeping.

SPEC Section 16.1's backup and restore CLI commands remain required for POC
exit, but are not part of WP-150. Reserve a separately reviewed **WP-155: Public
backup and restore administration** after WP-150 and before WP-200. Its later
decision must atomically define the API-neutral operation, public
protocol/client surface, offline/quiescence model, authorization and audit
points, destructive restore confirmation, integrity-before-readiness proof,
hard dependencies, exact allowed paths, and acceptance evidence, then add the
complete WP-155 registry entry and make WP-200 depend on it. This ADR does not
add an incomplete `work_packages.yaml` entry or a premature WP-200 edge.

WP-150 does not expose hidden filesystem copies, redb access, offline repair,
or an unstated administrative RPC to simulate those commands. WP-155 must add
the resulting CLI commands through the reviewed public boundary and reuse
WP-070's accepted durable backup/restore implementation; it may not create a
second storage format. Deferring interface design to WP-155 does not remove or
weaken SPEC Sections 16.1 or 18.5 and does not permit WP-200 to exit without the
commands.

### Retry and uncertainty identity

The CLI invokes WP-137's three typed public-client retry helpers and consumes
only their checked terminal response or `ClientError`. It does not observe the
helpers' private dispositions, classify Tonic statuses, transport errors, or
response text itself, and never retries through a generic arbitrary-RPC loop.
The helpers never retry a definitive validation,
authorization, contract, idempotency-reuse, or declared business outcome.
`max_attempts` includes the initial call. Every transport attempt receives a
fresh outer ADR-0018 `RequestId`; the CLI does not reuse one across attempts and
does not substitute an MCP/JSON-RPC/process ID.

All semantic request bytes remain identical across a retry unless the public
operation's accepted recovery contract explicitly constructs a distinct
outcome-resolution request. In particular:

- A command retry retains the exact command, expected contract behavior,
  canonical submitted input, and contract-declared idempotency-key value. It
  never generates or changes an idempotency key to escape uncertainty.
- Bootstrap retains the exact crash-durable 132-byte document, including its
  `CapabilityId` and token, while using a fresh `RequestId`. After an uncertain
  response it replays or resolves that original bootstrap operation; it never
  generates a replacement credential.
- Normal capability creation chooses and retains one `CapabilityId` before its
  first request. A retry retains that ID. If the original response committed
  but its one-time token was lost, the exact replay result
  `AlreadyCreatedTokenUnavailable` is surfaced and no replacement capability,
  token, revoke, or second create is attempted automatically.
- Expected versions, cursors, page limits, and query inputs are not silently
  refreshed or rewritten. A caller-visible follow-up after a definitive
  mismatch is a new operation, not an attempt of the old operation.

The CLI does not report success from timeout, connection loss, a retained local
file, or inference from later unrelated state. Outcome-unknown and bootstrap
uncertainty remain explicit until resolved through their public operations.

### Machine output v1

Human output is the default and is not a compatibility interface. Machine mode
is selected by `--output json` (or its lower-precedence configuration forms)
and emits compact UTF-8 JSON Lines. One invocation writes exactly one JSON
object followed by one LF to stdout. It emits no banner, progress record,
credential, log, ANSI escape, or additional whitespace on stdout. Help,
version, and argument-parser failures that occur before a valid output mode is
selected remain bounded text on stderr and write nothing to stdout.

Every machine-mode terminal object has exactly one of these envelope shapes,
with keys serialized in the shown order:

```json
{"schema":"riffdb.cli.output/v1","command":"command.execute","ok":true,"result":{}}
{"schema":"riffdb.cli.output/v1","command":"command.execute","ok":false,"error":{}}
```

`schema` is always the exact string `riffdb.cli.output/v1`. `command` is the
lowercase dotted clap command path. `ok` is a JSON Boolean. Exactly one of
`result` or `error` is present. Each command owns one closed serializable output
DTO rather than serializing generated Protobuf or arbitrary Rust debug values.

WP-150 freezes exactly these command paths, in this order:

```text
contract.validate
contract.deploy
command.execute
command.outcome
entity.get
commit.show
projection.query
capability.bootstrap
capability.create
capability.revoke
server.health
demo.budget
```

`capability.bootstrap` owns both the retained-document generation/input modes
and the public bootstrap invocation; it is not split into an unreviewed second
machine-output identity. `demo.budget` launches only the already owned checked
budget-comparison flow. WP-150 adds no other v1 command path. The separately
reviewed WP-155 decision will freeze its backup/restore paths before their
fixtures or implementation merge.

SPEC Section 16.1 requires `contract generate` only when needed. WP-150 deploys
the checked-in POC Budget contract and uses the already generic/generated public
SDK surface delivered upstream, so no CLI-side generation step is needed for
this POC flow. Omitting that conditional path from the exact v1 registry neither
drops an active acceptance operation nor authorizes later code generation under
WP-150.

The exact key set, presence rules, and representative success, public-error,
local-error, and uncertainty objects for every applicable terminal branch of
each WP-150 command must be checked in as reviewed golden fixtures before
implementation of that command merges.
No map, flattened catch-all, or arbitrary diagnostic text is permitted.

WP-150 therefore begins with one interface-only PR. It contains the complete
clap command/argument grammar, configuration examples, exit-code registry,
closed result/error DTO declarations, and golden JSONL bytes for every
applicable terminal branch of all 12 command identities, including the runner
handoff. It contains no network call, credential read/write, subprocess launch,
or command implementation. One explicit human acceptance covers that complete
registry; only then may command implementations merge against it. Accepting
this ADR selects the envelope and scalar rules but deliberately does not accept
those not-yet-authored command-specific bytes or arguments. A later registry
change repeats the compatibility review rather than being hidden in an
implementation PR.

The following scalar and RiffDB-value rules are part of v1 and apply to every
command fixture:

- Every `u64`, including versions, sequences, epochs, positions, counts whose
  semantic type is `u64`, and unsigned 64-bit RiffDB values, is a canonical
  base-10 JSON string with no sign or leading zero except `"0"`.
- Every `i64`, including timestamp seconds and signed 64-bit RiffDB values, is
  a canonical base-10 JSON string with `-` only for negative values and no
  leading zero except `"0"`.
- `u32`, `i32`, and smaller bounded integers are JSON numbers. Boolean and
  textual values retain their JSON scalar types.
- A UUID is canonical lowercase hyphenated text. A typed content hash is
  fixed-length lowercase hexadecimal without `0x`. It is never emitted through
  a generic bytes rule.
- Other byte strings, including opaque cursors and decimal two's-complement
  coefficients, use canonical RFC 4648 standard-alphabet base64 with required
  `=` padding. Capability tokens are not ordinary bytes and are never emitted.
- Optional absence omits the key. It is not JSON `null`. Empty repeated values
  are `[]`. Explicit RiffDB `Null` is represented only by its tagged value.
- Closed enum spellings are lowercase snake case without their Protobuf enum
  type prefix. Unknown enum numbers never reach output.
- Object keys follow the reviewed DTO declaration order. Canonical record output
  retains increasing `field_id` order; submitted record input preserves its
  bounded list order until the shared service resolves identities and rejects
  duplicates. Other repeated values retain their public semantic order. JSON
  object order is not used to express a set.

A RiffDB `Value` is never converted to untagged native JSON. It uses exactly
one of these recursive structural forms:

```json
{"type":"null"}
{"type":"bool","value":true}
{"type":"i64","value":"-1"}
{"type":"u64","value":"1"}
{"type":"decimal","coefficient_twos_complement":"AQ==","scale":2}
{"type":"money","currency":"USD","amount":{"coefficient_twos_complement":"AQ==","scale":2}}
{"type":"string","value":"text"}
{"type":"bytes","value":"AQI="}
{"type":"uuid","value":"01234567-89ab-7def-8123-456789abcdef"}
{"type":"date","days_since_unix_epoch":1}
{"type":"timestamp","seconds":"1","nanos":2}
{"type":"enum","type_id":1,"variant_id":2}
{"type":"enum","type_id":1,"variant_id":2,"name":"approved"}
{"type":"list","values":[]}
{"type":"record","fields":[{"field_id":1,"value":{"type":"u64","value":"1"}}]}
{"type":"record","fields":[{"name":"amount","value":{"type":"u64","value":"1"}}]}
{"type":"record","fields":[{"field_id":1,"name":"amount","value":{"type":"u64","value":"1"}}]}
```

The forms are closed: no extra key is emitted or accepted by CLI-owned JSON
input conversion, and every required key is present. Enum `name` is optional and
is omitted when absent. Each record field has an optional nonzero `field_id`, an
optional bounded source `name`, and requires at least one; when both are present
the shared service requires them to resolve to the same field under ADR-0031.
`scale`, `nanos`, enum IDs, and present field IDs are `u32` JSON numbers. Money's
`amount` is the exact untagged decimal structure shown, not a nested generic
Value. A later tag, field rename, integer representation, byte alphabet,
envelope field, or omission-rule change requires a new output schema version and
human review; it is not a transparent refactor.

Errors use a command-owned closed DTO derived only through WP-137's SDK-owned
checked public-error view/re-export or a bounded CLI-local error registry. The
CLI does not duplicate the public-error registry, parse generated Protobuf
error bytes, or classify transport statuses. Internal source chains, transport
debug strings, paths containing credentials, response bodies, and arbitrary
server text are discarded. Public-error fields retain the accepted public
code, safe message, recovery action, structured details, and optional canonical
incident UUID under exact fixture-defined keys. Local errors use stable
lowercase snake-case codes and fixed safe messages. The golden fixtures are the
compatibility authority for those exact objects.

## Options Considered

1. **Reviewed public-client graph, explicit configuration, protected
   credentials, and versioned structural JSON:** Accepted. This keeps the CLI
   useful while making its security and compatibility boundaries testable.
2. **Serialize generated Protobuf messages directly:** Rejected. Generated
   representation, 64-bit JSON behavior, oneofs, bytes, and presence are not
   the CLI compatibility contract and may drift with tooling.
3. **Represent RiffDB Values as ordinary JSON:** Rejected. `i64` versus `u64`,
   decimal scale, money currency, bytes versus strings, enum IDs, record field
   IDs, and explicit null would be lost or ambiguous.
4. **Accept tokens through argv or TOML:** Rejected. Argv is process-visible and
   TOML turns general configuration into secret material. A protected file path
   may be configured; the raw token may not.
5. **Call `riffdb_auth::load_capability_token_file` from the CLI:** Rejected.
   It contradicts ADR-0009's exact isolated-module exception and crosses an
   auth-owned decoded token into a public transport client.
6. **Duplicate the protected-file loader in the CLI and MCP:** Rejected. That
   would create multiple owners for the no-follow, inode, ownership, mode, and
   bounded read checks. The public client is the one shared delivery owner.
7. **Import all of `riffdb-auth` for convenience:** Rejected. It would expose
   authenticators, issuers, key custody, and server-only operations to a public
   transport client.
8. **Implement backup/restore by opening redb or copying its files:** Rejected.
   It is a privileged storage bypass with undefined quiescence, identity,
   credential-key, authorization, audit, and crash semantics.
9. **Copy retry/error classification into the CLI or expose a generic retry
   loop:** Rejected. Transport disposition and public-error validation are SDK
   responsibilities, and a generic loop cannot enforce each operation's
   uncertainty identity.
10. **Automatically discover configuration:** Rejected for the POC. It makes an
   invocation depend on ambient files and complicates provenance and tests.

## Consequences

- WP-137 becomes a hard predecessor and leaves WP-150 a small, audited
  public-client consumer instead of a second transport-policy owner.
- Configuration, credentials, JSON output, and retry behavior are bounded and
  reproducible.
- Machine output becomes a public compatibility surface. Every command needs
  reviewed fixtures, and incompatible improvements require a v2 schema.
- The public Rust client gains one presentation-only protected normal-credential
  loader shared by CLI and MCP stdio. ADR-0009's sole CLI auth exception remains
  exactly `riffdb_auth::bootstrap_secret`; architecture tests prove that no
  decoded token or auth authority crosses the public-client boundary.
- Bootstrap and normal-create credential durability need filesystem failpoints,
  making this CLI more involved than a thin argument wrapper.
- Unknown ambient `RIFFDB_*` variables are ignored intentionally; typos in a
  documented variable are caught only when they replace a required value or by
  deployment tooling.
- WP-150 does not claim backup/restore completion. The new WP-155 owns their
  separately reviewed public design and CLI completion, and becomes mandatory
  before WP-200 can claim POC exit.

## Compatibility

This decision changes no public Protobuf message, RPC, durable record, contract
source, IR, hashing rule, storage key, or database migration. It adds the
source-additive Rust SDK functions
`load_protected_bearer_credential` and
`BearerCredential::has_same_presentation`, a new CLI configuration interface,
the `capability.bootstrap --bearer-output <path>` delivery option, the
`riffdb.budget.public-run/v1` WP-135 child-process protocol, and the
`riffdb.cli.output/v1` machine-output surface. The loader's exact
presentation and protected-file behavior is a reviewed public SDK contract;
it is not a token-decoding or authentication compatibility promise. Human
output is additive and explicitly unstable.

Accepting this ADR requires companion authoritative edits that (1) preserve
ADR-0009's exact sole CLI auth access to
`riffdb_auth::bootstrap_secret`, explicitly forbid the CLI and public client
from calling the root auth normal-token loader, and amend only its
credential-delivery ownership to permit the presentation-only public-client
loader defined here; the public loader guarantees protected presentation bytes,
while the server remains the sole owner of canonical decoded authentication,
and `--bearer-output` is byte-for-byte canonical only because the auth-owned
bootstrap helper produced those same bytes; the complete direct-owner table
adds `riffdb-proto`, `riffdb-service`, `riffdb-api-mcp`, and `riffdb-cli` to the existing `riffdb-auth` `base64`
owner and adds the exact public-client/CLI `zeroize` owners listed above; (2)
amend ADR-0037 to add only the exact public-client `zeroize` edge;
(3) amend ADR-0040 and WP-137 to require ADR-0009, ADR-0037, and this record and
to own the exact loader, comparison, dependency, paths, and tests; (4) make
WP-137 a WP-135 predecessor, name the WP-135 runner artifact and
`riffdb.budget.public-run/v1` protocol, make WP-135 and WP-137 WP-150
predecessors, and record the exact process fixtures, requirements,
dependencies, and acceptance evidence for both packages; and (5) reserve the separately reviewed
WP-155 before WP-200 without adding its registry entry or WP-200 edge until that
later decision defines them completely. This is not an implied amendment: until
that reconciliation is accepted, ADR-0009 and ADR-0037's existing dependency
wording and ADR-0009's auth-access wording remain authoritative. This record
does not authorize WP-150 to invent backup/restore RPCs or WP-155 to proceed
before its public API and operational semantics are reviewed.

Adding optional result fields is not automatically compatible in a closed CLI
v1 DTO. Such a change requires updated fixtures and human compatibility review;
an incompatible scalar, Value, envelope, or field-presence change requires a
new schema string. New commands may use v1 only when their exact result/error
fixtures obey all v1 rules.

## Security

- All database authority remains behind public gRPC, shared authentication,
  policy, service, runtime, and commit coordination.
- Raw tokens and bootstrap documents never enter argv, TOML, stdout, stderr,
  human output, JSON, tracing, metrics, panic text, or demo command echo.
- Normal protected-file reads use the public client's independently bounded
  presentation loader with ADR-0009's exact Linux object checks; bootstrap
  reads remain inside `riffdb_auth::bootstrap_secret`. Generated files use
  exclusive mode-`0600` creation, file and directory synchronization, close,
  protected reread, and exact non-exposing comparison. Generation mode durably
  retains and rereads both requested outputs before the bootstrap RPC. Existing-
  input mode never rewrites its stdin or protected-file source and durably
  retains and rereads only the requested bearer output.
- Loopback plaintext is a closed POC constraint. DNS, remote hosts, proxy
  inference, TLS ambiguity, and URI credentials fail before connecting.
- JSON renders only checked public-safe or bounded local DTOs. It never renders
  `Debug`, an internal source error, arbitrary transport text, or an unrestricted
  map.
- Contract, JSON, stdin, path, recursive collection, output-model, and rendered
  output bounds are enforced before an RPC or partial stdout write; oversize
  data cannot turn the CLI into an unbounded parser or output relay.
- `zeroize` limits retention of CLI-owned credential buffers but makes no claim
  about inherited environment storage, generated Tonic metadata copies, kernel
  buffers, or allocator behavior.
- The public-client loader performs no base64 decode, digest, capability lookup,
  principal construction, or authorization. Presentation-valid but
  noncanonical token encodings still fail at the auth-owned server decoder.
- `demo budget` requires a normal protected credential file, invokes its
  reviewed runner path directly without a shell, clears inherited environment,
  caps each child output stream at 4,096 bytes, and kills and reaps the child at
  180 seconds. Child output is parsed as the closed runner protocol and is never
  relayed as database or CLI authority.
- The completed scratch `cargo deny`, feature-tree, license, build-script,
  native-link, and unsafe-inventory review above is part of acceptance evidence.
  The real workspace resolution must match it or stop for a fresh human review
  before the dependency or lockfile merges.

## Testing

Before WP-150, WP-137 must provide:

- Linux protected-bearer-loader tests at 42, 43, and 44 bytes; every invalid
  alphabet byte; missing final EOF; final and raced symlinks; FIFO, directory,
  and device handles; owner and each forbidden mode bit; pre-open/opened
  device/inode mismatch; unreadable files; and unavailable, malformed,
  duplicate-`Uid:`, and 65,536/65,537-byte `/proc/self/status` boundaries;
- presentation-boundary tests proving the loader returns only a redacted
  `BearerCredential`, accepts a 43-byte alphabet-valid but noncanonical decoded
  presentation for server-side rejection, performs no base64 decode or auth
  lookup, zeroizes bounded temporary storage, and returns
  `UnsupportedPlatform` without a credential read on non-Linux targets;
- exact `has_same_presentation` equal/different tests plus canary checks over
  loader errors, `Display`, `Debug`, and source chains, proving that neither the
  path nor token is exposed;
- exhaustive operation-specific Execute, bootstrap-create, and normal-create
  helper schedules, including private transport classification, fresh
  RequestIds, stable semantic identities, uncertain exhaustion, and
  token-unavailable behavior; callers observe only checked terminal
  `Result`/`ClientError` values and never the helpers' private dispositions;
- SDK public-error view/re-export tests proving every accepted public code,
  recovery action, structured-detail branch, optional incident ID, and safe
  bound is available without generated-message parsing or internal errors;
- architecture tests rejecting a generic arbitrary-RPC retry API and any
  CLI-facing unchecked transport or server-error text; and
- dependency/source architecture tests proving `riffdb-client-rust` uses no
  `riffdb-auth` API, exports exactly the one protected normal-file loader, and
  that CLI and MCP stdio can consume it without private client modules.

WP-140 must add source and process tests proving its stdio credential-file path
uses `riffdb_client_rust::load_protected_bearer_credential`, never imports
`riffdb-auth`, never receives raw file text, and presents the returned credential
through the same public gRPC client metadata path as the CLI. Its environment
credential path may construct `BearerCredential` directly under ADR-0008's
separate bounds but does not decode or authenticate the token locally.

WP-150 must provide at least:

- dependency and source architecture tests freezing the exact direct allowlist,
  features, sole `riffdb_auth::bootstrap_secret` symbol boundary, explicit
  rejection of `riffdb_auth::load_capability_token_file`, use of the public
  protected-bearer loader for normal files, and absence of
  storage/service/policy/server imports;
- configuration table tests for every precedence permutation, missing/empty/
  invalid values, the 65,536-byte boundary, unknown and duplicate TOML items,
  ignored unknown environment names, no discovery, every endpoint rejection,
  and equal/one-byte-over checks for the general 4,096-byte path boundary;
- streaming equal/one-byte-over tests for the 1,048,576-byte contract, JSON,
  and general-stdin bounds, plus the tighter configuration and bootstrap bounds,
  proving no unbounded pre-read and no RPC on rejection;
- recursive input tests at and beyond every public SDK depth, collection,
  string, byte, and page bound, proving the CLI and SDK accept and reject the
  same per-field structures without truncation when their aggregate encoding is
  within the CLI's 1,048,576-byte limit, and proving the CLI locally rejects an
  aggregate that exceeds that intentionally stricter limit before any RPC;
- fixed golden JSONL fixtures plus semantic assertions for every applicable
  terminal success, public error, local error, and uncertainty branch of every
  command;
- equal/one-byte-over tests for the 4,194,304-byte checked output-model and
  rendered-output bounds in both modes, proving oversize and rendering failure
  leave stdout empty;
- exhaustive scalar and recursive Value fixtures, including `u64::MAX`,
  `i64::MIN`, zero, negative timestamp seconds, minimal/maximal decimal
  coefficients, empty/nested values, UUID case, hash case, padded base64, and
  optional-versus-null behavior;
- secret-canary tests over stdout, stderr, JSON, human formatting, debug output,
  failure messages, and `scripts/demo --dry-run`;
- filesystem failpoint tests at create, partial write, file sync, close,
  directory open, directory sync, protected reread, and compare, proving no
  bootstrap RPC occurs before durable revalidation of every newly created
  output file, generation mode's bootstrap document and `--bearer-output`
  retain byte-identical presentations of the one helper-produced token,
  existing-input mode never recreates or overwrites its source, normal reread
  comparison uses only `BearerCredential::has_same_presentation`, and no normal
  token is returned to CLI code from the loader or printed;
- retry tests proving fresh `RequestId` per attempt, byte-identical semantic
  input, retained command idempotency identity, retained bootstrap ID/token,
  retained normal-create `CapabilityId`, explicit
  `AlreadyCreatedTokenUnavailable`, and no automatic replacement;
- public process integration for all WP-150 acceptance-demo operations against
  `riffdbd`, including direct launch of `riffdb-budget-public` for all three
  cases, exact one-line runner JSON/exit-code validation, no-shell and
  cleared-environment assertions, 4,096-byte stdout/stderr overflow cases, the
  180-second kill-and-reap boundary, and an architecture assertion that the CLI
  never opens the redb data path; and
- `cargo test -p riffdb-cli` and `./scripts/demo --dry-run`, followed by the
  workspace formatting, Clippy, test, documentation, dependency-policy, and
  generated-artifact checks required by AGENTS.md.

WP-155 must add separately reviewed process, authorization/audit,
quiescence/offline, destructive-restore, backup-integrity, restored-readiness,
failure, and CLI conformance evidence. WP-150 tests are not evidence that the
POC backup/restore obligation has been completed.

## Requirements and Work Packages

- **Requirements:** `API-001`, `ID-005`, `MCP-047`, `OUT-001`, `POC-001`,
  `POC-008`, and `POC-010`
- **Defines or blocks:** the accepted WP-135/WP-137-to-WP-150 dependencies,
  `WP-150`, and the reserved `WP-155` packaging boundary
- **Consumed by:** `WP-137`, `WP-150`, reserved `WP-155`, and `WP-200`
- **Final evidence:** `WP-150` public CLI integration, reserved `WP-155`
  backup/restore integration, and the `WP-200` POC acceptance/demo run

The companion WP-135 reconciliation adds WP-137 as a hard dependency, removes
`POC-004` from this evidence package because its required post-commit
connection-loss failpoint becomes explicit WP-190/WP-200 evidence, adds `OUT-001` for the
same-key replay it actually proves, and adds
ADR-0005, ADR-0009, ADR-0011, ADR-0015, ADR-0018, ADR-0028, ADR-0031,
ADR-0037, ADR-0040, and ADR-0041 to its existing ADR-0006 and ADR-0007
requirements. It must add the nested-workspace package and process fixture:

```text
examples/budget-comparison/riffdb-grpc/Cargo.toml
examples/budget-comparison/riffdb-grpc/src/lib.rs
examples/budget-comparison/riffdb-grpc/src/bin/riffdb-budget-public.rs
examples/budget-comparison/riffdb-grpc/fixtures/public-run-v1-success.jsonl
examples/budget-comparison/riffdb-grpc/fixtures/public-run-v1-checked-error.txt
examples/budget-comparison/riffdb-grpc/fixtures/public-run-v1-invalid-invocation.txt
examples/budget-comparison/tests/public_comparison.rs
```

Its package is `riffdb-budget-comparison-riffdb-grpc`, its binary is
`riffdb-budget-public`, the root nested-workspace manifest adds
`riffdb-grpc` as an explicit member and declares the `public_comparison` test
despite `autotests = false`, and its accepted process commands are:

```bash
cargo build -p riffdb-server --bin riffdbd --target-dir target/wp135-root
cargo build --manifest-path examples/budget-comparison/Cargo.toml -p riffdb-budget-comparison-riffdb-grpc --bin riffdb-budget-public --target-dir target/wp135-comparison
RIFFDB_BUDGET_RIFFDBD_BIN="$PWD/target/wp135-root/debug/riffdbd" RIFFDB_BUDGET_RUNNER_BIN="$PWD/target/wp135-comparison/debug/riffdb-budget-public" cargo test --manifest-path examples/budget-comparison/Cargo.toml -p riffdb-budget-comparison --test public_comparison --target-dir target/wp135-comparison -- --test-threads=1
```

This proves discarded-response uncertainty through the public runner. Actual
post-commit TCP-loss and restart evidence remains owned by WP-190 and WP-200.

The companion WP-190 reconciliation adds `POC-004` and `TXN-044` to its
requirements and ADR-0040 and ADR-0041 to its `required_adrs`; it preserves all
declared dependencies, allowed paths, acceptance commands, and other evidence.
Its existing ignored `full_recovery_matrix` process test must include two named,
explicitly synchronized cases. The first lets `riffdbd` durably commit one
command and then loses the TCP response before a complete public response frame
is received. The second terminates the server process after the same durable
point and before response release, then restarts it. In each case a fresh public
client invocation through ADR-0040's exact Execute retry helper retains the
same command, raw idempotency key, canonical input, and capability, uses a fresh
RequestId after reconnect or restart, and must return the original outcome with
`replayed=true`, the original commit sequence/provenance identity, and no second
authoritative mutation, event, outbox intent, provenance record, commit record,
or sequence. Explicit hooks or barriers, not sleeps or socket timing guesses,
establish the post-commit/pre-response point. The WP-190 exit gate must name
both cases. WP-200 consumes that machine-readable evidence in its existing
`POC-004` sign-off rather than reconstructing it in release scripts.

The companion WP-200 reconciliation preserves every existing required ADR and
adds ADR-0040 and ADR-0041 for the final public cross-transport, CLI, and release
evidence. ADR-0039 separately requires its own addition. No existing WP-200
dependency, allowed path, deliverable, acceptance command, or earlier required
ADR is removed.

The companion WP-137 reconciliation adds ADR-0009, ADR-0037, and ADR-0041 to its
required ADRs and adds `ID-005` and `MCP-047` to its requirements. In addition
to `crates/riffdb-client-rust/**` and `Cargo.lock`, its sole auth path is the
test-only staged-owner assertion accepted in ADR-0040. Within its complete
implementation-path evidence, the dependency/credential subset must name
exactly:

```text
Cargo.lock
crates/riffdb-proto/Cargo.toml
crates/riffdb-service/Cargo.toml
crates/riffdb-client-rust/Cargo.toml
crates/riffdb-client-rust/src/credential_file.rs
crates/riffdb-client-rust/src/lib.rs
crates/riffdb-client-rust/src/metadata.rs
crates/riffdb-client-rust/tests/credential_file.rs
crates/riffdb-auth/tests/architecture.rs
```

ADR-0040 separately freezes the exact cursor/generation subset and the complete
allowed source, schema, fixture, fuzz, script, and integration-test inventory;
this dependency/credential subset does not replace either list.

No auth manifest or production-source path is added to WP-137. WP-150 retains its existing
`crates/riffdb-cli/**` and `Cargo.lock` paths and may consume only the merged
public-client surface plus the accepted isolated bootstrap module; it cannot
edit either credential owner. WP-140 consumes the same public-client loader
inside its already reviewed MCP paths and does not gain an auth dependency.

The companion WP-150 reconciliation preserves `WP-130` and adds `WP-135` and
`WP-137` as hard dependencies. It adds `ID-005`, `OUT-001`, and `POC-008` to
the existing `API-001`, `POC-001`, and `POC-010` requirements because the
package's credential, same-key replay, public comparison, and architecture
fixtures directly claim those guarantees. It deliberately does not add
`TXN-044` or `POC-004`: actual post-commit process/TCP loss remains mandatory
WP-190/WP-200 evidence under the exact reconciliation above. No existing
dependency, allowed path, deliverable, or acceptance command is removed.

WP-150's complete required ADR list becomes ADR-0005, ADR-0006, ADR-0007,
ADR-0009, ADR-0011, ADR-0018, ADR-0028, ADR-0031, ADR-0037, ADR-0040, and
ADR-0041. No other ADR is implied by the CLI's direct implementation surface.

WP-150 treats `examples/budget-comparison/**` as a read-only upstream artifact.
Its interface-only checkpoint and process proof live in already allowed paths:

```text
crates/riffdb-cli/fixtures/**
crates/riffdb-cli/tests/public_process.rs
scripts/demo
```

After the interface-only PR freezes the exact command grammar, configuration,
exit codes, result DTOs, and golden JSONL bytes, WP-150 adds this process-level
acceptance evidence without changing the runner protocol:

```bash
cargo test -p riffdb-cli
./scripts/demo --dry-run
cargo build -p riffdb-server --bin riffdbd --target-dir target/wp150-root
cargo build --manifest-path examples/budget-comparison/Cargo.toml -p riffdb-budget-comparison-riffdb-grpc --bin riffdb-budget-public --target-dir target/wp150-comparison
RIFFDB_TEST_RIFFDBD_BIN="$PWD/target/wp150-root/debug/riffdbd" RIFFDB_TEST_BUDGET_RUNNER_BIN="$PWD/target/wp150-comparison/debug/riffdb-budget-public" CARGO_TARGET_DIR=target/wp150-root cargo test -p riffdb-cli --test public_process -- --ignored --exact public_process_all_acceptance_operations
```

## Decision Deadline

The exact ADR text, dependency audit, unchanged bootstrap-only CLI auth
exception, public-client normal-bearer loader and comparison, output shapes,
input/output bounds, WP-137 SDK ownership, dual bootstrap credential delivery,
the WP-135 public-runner protocol and acceptance process, WP-150 dependency,
the interface-only CLI fixture checkpoint, and required WP-155 reservation must
be accepted and reconciled into the authoritative files
before WP-137 freezes the CLI-facing credential/retry/error interface or WP-150
changes `crates/riffdb-cli/Cargo.toml`, `Cargo.lock`, or any stable
configuration/output behavior. WP-150 cannot be developed alongside a merged
CLI surface because the dependency, credential, and machine-output choices are
the interfaces under decision. WP-155 requires its own later human review; that
review must add its complete registry entry and the WP-200 dependency before
WP-200 begins final POC acceptance.
