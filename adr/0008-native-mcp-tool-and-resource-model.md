# ADR-0008: Native MCP Tool and Resource Model

- **Status:** Accepted
- **Direction approved:** 2026-07-12
- **Exact text accepted:** 2026-07-22
- **Accepted:** 2026-07-22
- **Acceptance reference:** `21a8cfb`
- **Partially resolved by:** ADR-0020 for command tool-name grammar,
  normalization, collision handling, and compiler/catalog ownership; ADR-0024
  for the exact provenance resource locator
- **Paired accepted record:** ADR-0040 for the public gRPC parity bridge required by
  production stdio
- **Amends:** ADR-0009's protected-file/direct-owner boundary, including
  the exact semantic, structural, and presentation-only `base64` owners added
  here and in paired ADR-0040; ADR-0037's
  public-client dependency allowlist for the narrow client-owned normal-bearer
  loader; and, through paired ADR-0040, ADR-0018/ADR-0009's exact server
  entropy-purpose set for one nonsemantic process generation
- **Decision deadline:** Before WP-137 changes the public protocol or WP-140
  freezes an MCP compatibility fixture

The human maintainer accepted this exact text and its ADR-only companion
reconciliation on 2026-07-22, with revision `21a8cfb` as the acceptance
reference. The separately named WP-137 and WP-140 exact-byte checkpoints remain
required before their generated public artifacts merge.

## Context

MCP resource identity, protocol presentation, transport composition, and
authorization are public and security boundaries. Stdio and Streamable HTTP must
not become separate semantic implementations, and neither may gain a storage,
policy, catalog, runtime, or commit-coordinator bypass.

ADR-0020 already owns the exact dynamic command tool name. ADR-0024 already owns
the exact provenance locator. The remaining decisions include protocol and SDK
versions, dependency features, transport topology and audience, every other
resource locator, MCP cursor text, identity separation, progress, cancellation,
and compatibility fixtures.

There is also a real interface conflict. SPEC Sections 9.7, 12.6, and 12.9 make
an outcome resource the cancellation/uncertainty recovery mechanism, while the
current API-neutral resolve operation accepts the caller's raw idempotency key.
The specified hash-only URI cannot be dereferenced by an adapter without either
a checked service lookup form or a forbidden storage scan. This decision keeps
the resource and defines a narrow service-owned lookup form; it does not pretend
the existing interface can already serve it.

## Decision

### Protocol and dependency baseline

The POC implements MCP protocol version **2025-11-25**. Initialization reports
that exact version and accurate server implementation metadata. A syntactically
valid different client offer receives `2025-11-25` as the server-selected
version under protocol negotiation and the client disconnects if it cannot use
that version; malformed version input rejects. RiffDB never silently advertises
or enables another protocol version.

`riffdb-api-mcp` is the sole first-party owner of the MCP SDK boundary. The
workspace pins `rmcp = "=2.2.0"`, disables its default features, and enables
exactly `server`, `transport-io`, and `transport-streamable-http-server` across
the complete WP-140 build. `server` is the sole unconditional rmcp feature. The
crate feature `stdio` adds only `rmcp/transport-io`; `streamable-http` adds only
the optional service/auth edges and
`rmcp/transport-streamable-http-server`. Default crate features are empty. No
rmcp client, sampling, roots, elicitation, task, OAuth, TLS, key-value, or
unrelated transport feature is enabled merely for convenience.

The feature-bearing manifest rows are exact; unrelated direct dependency rows
are not implied by this excerpt:

```toml
[features]
default = []
stdio = ["rmcp/transport-io"]
streamable-http = [
  "dep:riffdb-auth",
  "dep:riffdb-service",
  "rmcp/transport-streamable-http-server",
]

[dependencies]
base64 = { version = "=0.22.1", default-features = false, features = ["alloc"] }
riffdb-auth = { version = "0.1.0", path = "../riffdb-auth", default-features = false, optional = true }
riffdb-service = { version = "0.1.0", path = "../riffdb-service", default-features = false, optional = true }
rmcp = { version = "=2.2.0", default-features = false, features = ["server"] }
```

Ungated `riffdb-api-mcp` common code owns protocol constants, bounded MCP
presentation DTOs, schema/reference validation, canonical rendering,
fingerprints, locators, cursors, result shaping, observer state, and the sole
MCP-side canonical standard-base64/base64url presentation edge. Its exact
`base64` dependency is presentation-only: it never decodes a bearer credential,
authenticates a token, or receives digest-key material. That common slice has no
`riffdb-service`, `riffdb-auth`, server, policy, runtime, commit, catalog, or
storage dependency. HTTP-only modules are compiled solely by
`streamable-http` and alone may use optional `riffdb-service` and
`riffdb-auth::CredentialAuthenticator` edges. `riffdb-mcp-stdio` enables only
`stdio`, uses `riffdb-client-rust` for public gRPC, and never enables the
`riffdb-api-grpc` server feature. Both backends convert checked results into the
same private bounded presentation DTOs and call the same renderer/validator;
those DTOs carry no authority and are not API-neutral semantic types.

Before the dependency or lockfile merges, a separate human-visible lock-graph
review records exact direct and transitive versions, enabled features, duplicate
versions, build scripts, native code, unsafe-code inventory, licenses, advisories,
and `cargo deny` results. The reviewed feature names are copied verbatim into the
WP-140 PR. An unexpected cryptographic/native dependency or required unsafe
exception stops the merge for human review.

The pre-merge review must specifically confirm the default-off graph's
`schemars`/`pastey`, Tokio I/O, HTTP/body/bytes/SSE/tower, `rand` 0.10, UUID-v4,
and build-script edges. `rmcp`'s registry build must be tested to prove its
workspace-local git-hook setup is inert for RiffDB. SDK randomness and UUID-v4
are confined to MCP transport/session identifiers; they never construct a
RiffDB UUIDv7, idempotency identity, logical time, canonical value, runtime
randomness, provenance identity, or commit-order signal. The reviewed 2.2.0
source currently has no direct unsafe block, but the full transitive unsafe and
native inventory remains required.

The 2026-07-22 exact-root scratch resolution added 37 lock entries and built on
Rust 1.97.0. Important active versions are Tokio 1.52.0, futures 0.3.33,
schemars 1.2.1, serde 1.0.229, serde_json 1.0.151, chrono 0.4.45, rand 0.10.2,
rand_core 0.10.1, getrandom 0.4.3, chacha20 0.10.1, UUID 1.24.0, and
sse-stream 0.2.5. The root already contained the `syn` 2/3 family. This
resolution adds the target-specific locked `r-efi` 6.0.0 beside 5.3.0 and adds
third versions to the already duplicated `getrandom`, `rand`, and `rand_core`
families. `cargo deny check` passed advisories, licenses, bans, and sources. No
active Linux dependency declares a native `links` edge or invokes `cc`, CMake,
or bindgen.

Relative to the reviewed root lock, new or newly versioned custom-build targets
are `getrandom 0.4.3`, `iana-time-zone-haiku 0.1.2`, `ref-cast 1.0.26`,
`rmcp 2.2.0`, `serde 1.0.229`, `serde_json 1.0.151`, `wasm-bindgen 0.2.126`,
`wasm-bindgen-shared 0.2.126`, and `zmij 1.0.23`; the Haiku, WASM, Windows, and
`r-efi` entries are target-specific and do not enter the active Linux graph.
The rmcp registry `build.rs` git-hook branch is inert outside an rmcp source
checkout and was inert in the RiffDB scratch build.

The added active dependency sources for which the conservative `cargo-geiger`
inventory reports used unsafe code are exactly `chacha20 0.10.1`,
`chrono 0.4.45`, `dyn-clone 1.0.20`, `getrandom 0.4.3`, `rand 0.10.2`,
`ref-cast 1.0.26`, `serde_json 1.0.151`, `uuid 1.24.0`, and `zmij 1.0.23`;
cfg-gated source can be counted even when a target does not compile that block.
`rmcp 2.2.0` itself contains no unsafe block, and every first-party crate still
forbids unsafe code. Chacha20 is reached only as rand's userspace RNG for MCP
transport/session behavior and is not RiffDB token, digest, canonical-value,
command-runtime, or durable-state cryptography. Accepting this record accepts
that reviewed dependency unsafe/cryptographic footprint for the exact
resolution only. ADR-0041's CLI graph pins serde_json 1.0.150, so a combined
WP-140/WP-150 resolution is already known to differ from at least one scratch
lock and must receive the required fresh human-visible lock review. Any real
lockfile, feature tree, build-script report, or unsafe inventory difference
likewise stops before merging the dependency.

Both transports return the same initialization result. It has protocol version
`2025-11-25`, server implementation name `riffdb`, and server implementation
version equal to the exact workspace package version of `riffdb-api-mcp`
(`0.1.0` for the POC fixture). The only advertised server capabilities are
`tools: { listChanged: true }` and
`resources: { subscribe: true, listChanged: true }`. Optional implementation
title, description, icons, website URL, instructions, prompts, logging,
completions, experimental capabilities, and every deferred capability are
absent rather than emitted as empty or false-valued extensions. A release that
changes the package version updates the initialization fixture; a transport may
not report an SDK version or transport-binary version instead.

The `rmcp` 2.2.0 registry knows three older protocol versions and one newer
version in addition to this POC baseline, and its default server initializer
would echo any other known offer. A future separately reviewed SDK may know a
different set. Each
transport therefore owns a bounded typed negotiation wrapper outside
`serve_server`; RiffDB does not fork or patch `rmcp`. The wrapper permits the
lifecycle's pre-initialization ping. Stdio passes that checked request to
`serve_server`. In stateful HTTP, rmcp's outer tower rejects a no-session
non-initialize POST before creating `serve_server`, so the RiffDB HTTP wrapper
itself handles exactly one structurally valid pre-initialization JSON-RPC
`ping` request after the ordinary route, Origin, authority, pre-authentication
rate, credential, and bounded-body checks. It returns HTTP 200 with the same
checked request ID and exact empty JSON object result, `Content-Type:
application/json`, `Cache-Control: no-store`, and no MCP session ID. It creates
no session, binding, observer, subscription, cancellation state, or service
call and does not advance initialization. A notification, missing/invalid ID,
extra ping parameter, batch, duplicate field, or other pre-initialization method
does not enter this path. For an initialize request the wrapper validates the
complete bounded JSON-RPC shape and requested protocol text. Exact
`2025-11-25` passes unchanged. Any other syntactically valid requested version
is replaced, only in the typed value delivered to `rmcp`, by one fixed private
sentinel `riffdb-unsupported`, which is proven absent from the reviewed SDK
`KNOWN_VERSIONS`; `rmcp` then selects RiffDB's configured `2025-11-25` fallback.
An initialization `MCP-Protocol-Version` header may be absent; when present it
must occur once and equal the body offer, and the wrapper rewrites it
consistently. Malformed, mismatched, duplicate, or oversized initialization
rejects. The original client bytes are never used as a string-replacement target,
and no request can cause RiffDB to advertise a newer version. The lock review and
tests assert the sentinel remains unknown and fallback behavior remains exact.
After initialization, every HTTP POST, GET, or DELETE requires exactly one
`MCP-Protocol-Version: 2025-11-25` header; absence, duplication, or another value
rejects before `rmcp`.

Streamable HTTP uses rmcp's stateful server mode. Stateless
`serve_directly` mode is forbidden for the POC because it does not perform the
same unsupported-offer negotiation and cannot support the accepted bounded
session, subscription, observer, cancellation, or authentication-lifetime
model. During stateful initialization rmcp checks equality of the wrapper's
typed body and optional header before `serve_server` performs the sentinel
fallback. Tests freeze that exact ordering; an SDK change that validates the
private sentinel as an ordinary known header first stops for review.

### Transport topology and authentication

Production stdio is an ordinary public gRPC client:

```text
MCP host -> riffdb-mcp stdio -> public RiffDB Rust client -> gRPC -> riffdbd
```

It links neither `riffdb-service` nor any authority-bearing server crate. It
uses the same public messages, status/error checks, retry rules, and gRPC-scoped
capability as any other client. Credentials come only from an environment
variable or protected local configuration, never an argv value. Stdout contains
only MCP frames; bounded redacted diagnostics go to stderr.

The stdio bridge recognizes exactly `RIFFDB_MCP_CONFIG`,
`RIFFDB_MCP_ENDPOINT`, `RIFFDB_MCP_CAPABILITY_TOKEN`, and
`RIFFDB_MCP_CREDENTIAL_FILE`. Other environment names, including other
`RIFFDB_*` names, have no stdio meaning. `--config` and `--endpoint` are the only
configuration flags; there is no raw-token, credential-file, audience, tenant,
or principal flag. Configuration-file selection is `--config`, then
`RIFFDB_MCP_CONFIG`, then absent; there is no current-directory, home-directory,
XDG, or parent-path discovery. The selected path is nonempty, NUL-free, and at
most 4,096 platform-encoded bytes. The complete file is at most 65,536 bytes,
must be UTF-8, is consumed completely, rejects duplicate or unknown keys/tables,
and has only this optional-key shape:

```toml
[mcp]
endpoint = "http://127.0.0.1:7443"
credential_file = "/home/operator/.config/riffdb/token"
```

Endpoint precedence is `--endpoint`, `RIFFDB_MCP_ENDPOINT`, `mcp.endpoint`, then
`http://127.0.0.1:7443`. A present empty or invalid higher-precedence value
rejects rather than falling through. Endpoint text is at most the 512-byte
`Audience` bound and has the exact POC form
`http://<loopback-IP-literal>:<nonzero-port>` with no user information, path,
query, fragment, DNS name, implicit port, whitespace, or normalization.

The credential comes from exactly one of: the exact 43-byte canonical token in
`RIFFDB_MCP_CAPABILITY_TOKEN`; or a credential-file path selected from
`RIFFDB_MCP_CREDENTIAL_FILE` then `mcp.credential_file`. Supplying both rejects.
The path has the same 4,096-byte path bound. A raw token is forbidden in TOML
and argv.

The stdio crate must not depend on `riffdb-auth`, whose server-side crate graph
includes authentication, digest custody, and storage interfaces. WP-137 instead
adds exactly
`riffdb_client_rust::load_protected_bearer_credential(&Path) ->
Result<BearerCredential, BearerCredentialFileError>` in
`crates/riffdb-client-rust/src/credential_file.rs` and re-exports it at the
client root. It applies ADR-0009's exact Linux pre-open/opened-inode,
effective-user ownership, mode, bounded-read, and exact 43-byte/no-line-feed
rules and returns the existing redacted `BearerCredential` without exposing raw
text or bytes. Its closed error has only `UnsupportedPlatform`,
`ProtectedFileRejected`, and `InvalidPresentation`, with fixed redacted
`Debug`/`Display` and no source chain. Environment input uses
`BearerCredential::new` directly. The public client treats the 43 bytes as
opaque bearer metadata and owns no base64 decoder, HMAC, token digest,
capability identity, or authentication result; `riffdb-auth` remains the only
authoritative canonical-token parser when the server receives the request.
Loader-owned staging buffers are bounded and zeroized on every return path.
Cross-crate fixtures prove the client loader accepts every canonical ADR-0009
test token and that presentation-valid but noncanonical decoded material still
fails at server authentication. The bridge promptly moves its bounded secret
copy into public-client metadata and never prints it.

The helper adds only the already reviewed exact `zeroize = "=1.8.1"` dependency
and no `riffdb-auth`, storage, service, policy, or server edge. Acceptance
explicitly amends ADR-0009's direct-owner table to add `riffdb-client-rust` as a
`zeroize` owner for bounded public-delivery credential buffers and amends
ADR-0037's client dependency allowlist by that edge only. Every public gRPC
request still authenticates through `CredentialAuthenticator` against the
separately configured exact gRPC audience; endpoint text does not let the bridge
select or assert an audience.

Streamable HTTP is an adapter over the same API-neutral service instance composed
by `riffdbd`. Its credential extraction calls only `CredentialAuthenticator`,
then enters the normal service authorization, durable audit, obligation, and
redaction path. It does not call the gRPC adapter in process and does not create
an actor or policy decision.

The POC HTTP endpoint is disabled by default and binds only an IPv4 or IPv6
loopback address. The route is exactly `/mcp`: no trailing slash, alternate
prefix, wildcard suffix, query-selected endpoint, or caller-selected route is
accepted. Its configured protected-resource/audience URI is an absolute HTTP
URI with a loopback IP literal, explicit port, exact `/mcp` path, and no user
information, query, or fragment. Authentication uses those configured bytes;
the request `Host`, forwarding headers, and origin never synthesize or replace
the audience. Remote binding, TLS, and OAuth protected-resource behavior remain
post-POC work.

Before authentication or body allocation, each request must carry exactly one
effective HTTP authority (`Host` for HTTP/1.1 or `:authority` for HTTP/2) whose
visible ASCII value is byte-for-byte the canonical IP-literal-and-explicit-port
authority from the configured protected-resource URI. A request with neither,
both, duplication, optional whitespace, user information, another IP spelling,
or any mismatch receives HTTP 421 with `Cache-Control: no-store`, no JSON-RPC
body, and no authentication challenge. This equality check prevents Host-based
routing and DNS-rebinding ambiguity but still does not construct the audience.

Every Streamable HTTP POST, GET, and DELETE accepts a capability in exactly one
HTTP field named `Authorization` under HTTP's case-insensitive field-name rules.
Multiple field lines, a comma-combined value, obsolete folding, or a second
credential header rejects. The field value is exactly 50 ASCII bytes:
case-sensitive `Bearer ` followed immediately by ADR-0009's 43 canonical
unpadded base64url token bytes. Leading or trailing optional whitespace, tabs,
another authentication scheme, parameters, padding, or non-ASCII rejects. The
adapter removes only the exact seven-byte scheme, passes the remaining bytes to
the auth-owned canonical token parser and `CredentialAuthenticator` with the
server-configured database, environment, and MCP audience, stores the resulting
`AuthenticatedPrincipal` only as a private request extension, and removes the
`Authorization` field before `rmcp` receives the request. It owns no token
decoder or authentication cache. Missing or malformed credentials receive the
same HTTP 401 response with `WWW-Authenticate: Bearer`,
`Cache-Control: no-store`, no JSON-RPC body, and no token-format distinction.

The default rmcp `LocalSessionManager` is forbidden: it inserts a UUIDv4 session
ID with replacement semantics and has no RiffDB capacity, collision, lifetime,
or capability-binding state. `riffdb-api-mcp` instead implements rmcp's public
`SessionManager` trait with a private RiffDB-owned manager. One `create_session`
call samples exactly one candidate through rmcp's reviewed `session_id()` source,
validates it as nonempty visible ASCII of at most 128 bytes, and reserves it by
insert-if-absent under one bounded map critical section before calling public
`create_local_session` and starting its worker. It never retries, evicts,
overwrites, or uses a second ID source. The 128-entry bound counts both Pending
and Active states; capacity, invalid-ID, or collision failure creates no worker
or live session. The manager clones the public local handle before awaiting and
holds no map guard across `.await`.

The adapter intercepts each successful initialization response before release
and atomically promotes that exact Pending ID to Active, binding it to the
authenticated `CapabilityId` and injected creation/idle clock state. Only the
one reserved initialization may initialize or promote an ID; `has_session` and
every ordinary stream/message method treat Pending as absent. Initialization,
response validation, or promotion failure removes the reservation, closes any
started worker, and creates no binding. Close, expiry, DELETE, cancellation,
transport/worker termination, or initialization abandonment removes either
state exactly once.

Every later POST, GET, or DELETE reauthenticates its presented credential and
must match the Active binding; a different capability cannot reuse another
session or SSE stream even when it resolves to the same principal. Each service
call and each emitted notification obtains fresh authentication/current-policy
evidence; a live SSE handler retains only its own bounded zeroizing credential
for that purpose. The private manager uses the injected monotonic clock, the
128-session server bound, 300-second idle and 900-second lifetime bounds.
Capacity exhaustion denies a new initialization without evicting a live or
pending session. Session IDs and credential material are absent from telemetry
and public errors. An SDK-produced session ID must be unique among all Pending
or Active reservations; failure leaves no reservation, worker, or binding.
The local `SessionConfig` sets `keep_alive = None` and `sse_retry = None`, and
the outer `StreamableHttpServerConfig` sets `sse_keep_alive = None` and
`sse_retry = None`. RiffDB's injected clock and bounded observer own liveness;
no SDK timer or priming/retry event creates an unreviewed frame, keeps a session
alive, or substitutes for the accepted idle and lifetime bounds.

An initialization request must not carry `Mcp-Session-Id`. Every later POST,
GET, or DELETE carries exactly one canonical `Mcp-Session-Id` field equal to the
bound text; absence, duplication, comma combination, whitespace, malformed text,
unknown/expired state, or mismatch receives the same existence-blind HTTP 404
with `Cache-Control: no-store` and no JSON-RPC body. The header is removed before
the application handler except for the SDK's typed session lookup; no alternate
query, cookie, or body session selector is accepted.

An `Origin` field is optional so non-browser clients remain usable. When absent,
processing continues without a CORS allow header. When present, there must be
exactly one field and one origin of at most 512 visible ASCII bytes. Its exact
bytes must be a member of a server-configured allowlist of at most 16 entries and
8,192 total bytes. Each configured entry is an absolute lowercase-`http` origin
with a loopback IP literal and explicit nonzero port, and no user information,
path, query, fragment, trailing slash, or normalization. `null`, multiple or
space-separated origins, comma combination, duplicate fields, whitespace,
malformed text, non-loopback origins, and unlisted origins receive HTTP 403 with
`Cache-Control: no-store`, no JSON-RPC body, no authentication challenge, and no
service request. An allowed origin is echoed byte-for-byte in
`Access-Control-Allow-Origin` with `Vary: Origin`; wildcard origin is forbidden.
Origin validation happens before session lookup, credential parsing, body
allocation, rate-limit bucket creation, or MCP dispatch. These rules apply to
POST, GET, DELETE, and any supported preflight; unsupported methods do not
weaken origin validation. Validation order is exact: route/method framing, then
Origin, then effective authority, then the pre-authentication rate limit and
credential, then protocol/session headers, bounded body, and MCP dispatch.

For the same authenticated principal, capability, active catalog, and policy
state, stdio and HTTP expose equivalent tools, resources, schemas, contents,
errors, pagination, cancellation, and notifications. Production `riffdbd`
composition of HTTP remains WP-185; WP-140 supplies the adapter, registration
hook, and loopback conformance harness.

### Canonical resource locators

The version-one resource inventory is:

| Resource | Exact locator |
|---|---|
| Active contract | `riffdb://contract/active` |
| Contract version | `riffdb://contract/<lineage>/<version>` |
| Entity schema | `riffdb://entity/<lineage>/<entity-id>/schema` |
| Command plan | `riffdb://command/<lineage>/<command-id>/plan` |
| Command documentation | `riffdb://command/<lineage>/<command-id>/docs` |
| Persisted outcome | `riffdb://outcome/<principal>/<lineage>/<command-id>/<tool-name>/<key-hash>` |
| Commit | `riffdb://commit/<sequence>` |
| Provenance | `riffdb://provenance/<provenance-id>` |
| Projection status | `riffdb://projection/<lineage>/<projection-id>/status` |
| Server health | `riffdb://server/health` |

Discovery maps each service descriptor to exactly one MCP inventory surface.
The structural registry is exact:

| Service descriptor | MCP surface | Exact URI or RFC 6570 URI template | MIME type |
|---|---|---|---|
| `active_contract` | `resources/list` | `riffdb://contract/active` | `application/json` |
| `contract_version` | `resources/list` | the canonical concrete contract-version locator above | `application/json` |
| `entity_schema` | `resources/list` | the canonical concrete entity-schema locator above | `application/schema+json` |
| `command_plan` | `resources/list` | the canonical concrete command-plan locator above | `application/json` |
| `command_documentation` | `resources/list` | the canonical concrete command-documentation locator above | `text/markdown` |
| `command_outcome` | `resources/templates/list` | `riffdb://outcome/{principal}/<lineage>/<command-id>/<tool-name>/{key_hash}` with the descriptor-owned segments encoded canonically | `application/json` |
| `commit.class_template` | `resources/templates/list` | `riffdb://commit/{sequence}` | `application/json` |
| `commit.commit_sequence` | `resources/list` | the canonical concrete commit locator above | `application/json` |
| `provenance.class_template` | `resources/templates/list` | `riffdb://provenance/{provenance_id}` | `application/json` |
| `provenance.provenance_id` | `resources/list` | the canonical concrete provenance locator above | `application/json` |
| `projection_status` | `resources/list` | the canonical concrete projection-status locator above | `application/json` |
| `server_health` | `resources/list` | `riffdb://server/health` | `application/json` |

Template variable names and braces are literal compatibility bytes. Template
expansion is not trusted: the resulting URI must pass the same complete
canonical parser and semantic-owner checks as a directly supplied resource URI.
`principal` must decode to a checked principal identifier, `key_hash` must be
the exact 50-character digest tuple, `sequence` must be a nonzero canonical
unsigned decimal, and `provenance_id` must be the exact lowercase UUIDv7 form.
Unknown variables, partial expansion, alternate operators, query expansion,
and extra path text reject. An unexpanded template is never a readable URI.

`CommandOutcomeResource` is therefore template metadata only. It fixes the
lineage, stable command ID, and compiler-owned tool-name segments but cannot
choose a principal, key digest, or concrete outcome locator. Concrete outcome
URIs enter MCP only as checked links returned by Execute/GetOutcome or as a
caller-supplied fully expanded URI; neither `resources/list` nor
`resources/templates/list` mints one. Commit and provenance class-template
descriptors likewise authorize only their template records, while their exact-
identity branches authorize only concrete list records. No descriptor appears
in both list surfaces.

MCP list pagination never filters a mixed service page. An initial or
continuation `resources/list` call invokes full `DiscoverResources` with exact
service page limit 500 and `ResourceDiscoveryKind::Concrete`; an initial or
continuation `resources/templates/list` call uses the same limit with
`ResourceDiscoveryKind::Template`. The lowercase 32-hex-character MCP cursor is
only the canonical presentation of that call's exact 16-byte service cursor.
The service binds kind, representation, and limit, so a cursor copied between
the two MCP methods rejects and neither adapter decodes, wraps, skips, buffers,
or synthesizes a transport page. Empty pages remain terminal and carry no
cursor. Compact watcher refreshes instead use
`ResourceDiscoveryKind::All`; they cannot lend their cursors to either public
list method.

Resource content remains service-mediated and uses this closed read registry:

| Resource kind | Fresh API-neutral operation or algorithm |
|---|---|
| Active contract | `GetActiveContract` |
| Contract version | `GetContractVersion` with the exact locator identity |
| Entity schema | full `DiscoverResources` under fresh authorization, requiring one exact matching descriptor and fence |
| Command plan or documentation | the exact fenced `DiscoverResources` plus `ExplainCommand` algorithm below |
| Persisted outcome | `ResolveCommandOutcome` with the checked locator selector |
| Commit | `GetCommit` |
| Provenance | `TraceProvenance` |
| Projection status | `GetProjectionStatus` |
| Server health | `Health` |

Inventory metadata and retained schema bytes are not content-release authority.
Every read reruns the named shared-service path, current authorization, required
audit lifecycle, obligations, bounds, and redaction before the common MCP
renderer releases content. A missing or nonunique exact descriptor during a
discovery-based read fails closed rather than substituting the active catalog.

`<lineage>` and `<principal>` are the exact UTF-8 bytes of the checked typed
identifier. Each byte outside RFC 3986's ASCII unreserved set
`ALPHA / DIGIT / "-" / "." / "_" / "~"` is percent-encoded with an uppercase
two-digit hexadecimal escape. A parser rejects lowercase escapes, malformed or
overlong escapes, escaped unreserved bytes, invalid UTF-8, or a decoded value
that its semantic owner rejects. It never normalizes text.

`<version>`, `<entity-id>`, `<command-id>`, `<sequence>`, and `<projection-id>`
are nonzero unsigned decimal integers with no sign and no leading zero.
`<tool-name>` is the exact compiler-owned ADR-0020 ASCII MCP command tool name;
all of its bytes are already URI-unreserved. The provenance form remains exactly
the ADR-0024 56-byte lowercase UUIDv7 locator.

Every locator uses the lowercase `riffdb` scheme, the exact lowercase authority
shown in the table, the exact path segment count, and no port, user information,
empty segment, trailing slash, query, or fragment. Producers emit only canonical
text and parsers reject alternate spellings instead of repairing them.

Typed stable IDs are used instead of source names in entity, command, and
projection locators so renaming display text cannot retarget a resource. Every
read reparses to a checked transport-neutral selector, calls the corresponding
API-neutral service operation, repeats current authorization, and applies all
obligations before content leaves the service. A locator is neither an
authorization proof nor a storage key. Locators other than provenance are at
most 2,048 encoded bytes. The outcome form has a tighter computed maximum of
1,745 bytes: 17 fixed prefix bytes, worst-case three-byte percent escapes for
each byte of the 256-byte principal and lineage, four separators, a 10-byte
`u32` command ID, the 128-byte tool name, and the 50-byte digest tuple.
The narrower ADR-0024 provenance bound still applies.

A command-plan or command-documentation URI remains stable-ID-only, but the
existing ExplainCommand operation selects by source name. Its read algorithm is
therefore exact and service-mediated. First, rerun policy-filtered
DiscoverResources in ADR-0040's compact-observation representation to exact end
within the same three-call/1,024-item bound and obtain the one visible command-
resource descriptor plus its catalog fence. Under
ADR-0040/WP-137 that descriptor is self-contained with lineage, contract
version, stable command ID, and exact source command from the same active bundle.
Next call ExplainCommand with
`Exact(lineage, version)` and that source command, and require the returned
contract identity and command ID to equal the descriptor. Finally perform the
conditional DiscoverResources check with the prior fence and require
`catalog_unchanged` before releasing plan or deterministically shaped
documentation. Catalog change, authorization failure, absence, ambiguity, or
any identity mismatch fails closed without content. A cached source name is never authority, the
adapter never joins through DiscoverCommandTools, and it never substitutes the
then-current active version for the descriptor's exact version. ADR-0040 must
freeze these additive descriptor fields and total conversions; the URI grammar
does not gain a source-name or version segment.

### Outcome locator and lookup

`<key-hash>` is the unpadded RFC 4648 base64url encoding of exactly 37 bytes:

```text
u8 digest_scheme + u32_be digest_key_id + 32 digest bytes
```

Version one requires scheme `1`, a nonzero key ID, and the existing
`IdempotencyKeyDigest` bytes. Its canonical text is exactly 50 ASCII characters;
padding, standard-base64 `+` or `/`, whitespace, alternate encodings, and unknown
schemes reject. The tuple is not a raw idempotency key and is not a secret, but
it is a sensitive correlation value and is redacted from telemetry and errors.

The service mints a checked opaque `OutcomeResourceLocator` only after Execute
or GetOutcome has received the raw key, resolved the exact durable identity, and
is authorized to release the terminal result. The locator is then carried with
that result. Its principal, lineage, stable command ID, and digest come from
that exact resolved identity; its tool name comes from the terminal row's exact
historical executable-plan bundle, not from the current active mapping. The
adapter never computes an HMAC and never receives digest-key material.

The locator is operation-envelope metadata, not a declared business-outcome
field. Under accepted ADR-0013, the bundle continues to carry only the canonical
declared-outcome union. The shared service owns the separately versioned generic
`riffdb.command-operation-envelope/v1` JSON Schema, and MCP mechanically inserts
the exact bundle outcome union at its `outcome` property. Neither service nor MCP
generates a second command-specific outcome schema.

The version-one envelope is a closed object whose required properties, in exact
order, are `status`, `commit_sequence`, `contract_version`, `plan_hash`,
`outcome`, `provenance_uri`, `durability_mode`, and `outcome_uri`. Its root
`oneOf` has three closed branches in this order:

1. `status` is constant `committed`; commit sequence is a nonzero canonical
   decimal string, provenance and outcome URI are canonical locators, and
   durability is `sync` or `group`.
2. `status` is constant `replayed`; all other fields have the same required
   shapes as committed and retain the original terminal values.
3. `status` is constant `executed_read_only`; commit sequence, provenance URI,
   durability, and outcome URI are JSON null.

Every branch requires a nonzero JSON-integer contract version, a 64-character
lowercase hexadecimal plan hash, and `outcome` matching the exact inserted
compiler union. The service schema uses Draft 2020-12, has
`additionalProperties: false`, and is canonicalized by the existing generated-
schema rules. Each dynamic command tool advertises this mechanically composed
schema and validates the entire `structuredContent` against it. It may
additionally emit the outcome URI as a standard resource-link content item.

The fixed `riffdb.command.get_outcome` tool cannot change `outputSchema` after
seeing its arguments. It therefore advertises the separate, invocation-
independent `riffdb.command-get-outcome-result/v1` schema. That closed schema has
exactly two branches: `{ "status": "not_found" }`; or a replay branch with
required properties, in order, `status` (constant `replayed`),
`commit_sequence`, `contract_version`, `plan_hash`, `outcome_type`, `outcome`,
`provenance_uri`, `durability_mode`, and `outcome_uri`. Metadata has the same
shape and semantics as the dynamic replay envelope, `outcome_type` is the exact
checked 1-to-256-byte grammar-v1 ASCII source identifier
`[A-Za-z_][A-Za-z0-9_]*` other than reserved `tx`, and `outcome` is the static
record branch of the tagged canonical-Value representation below. The fixed
tool may emit `outcome_uri` as the same standard resource link. It never inserts
a selected command's declared union into its advertised schema and never changes
the schema when the active catalog changes.

An authorized persisted-outcome resource uses exactly the replay branch of this
same static result representation. A missing resource returns the bounded
resource-read failure rather than a synthetic `not_found` content document.

The tagged Value is a closed `oneOf` discriminated by exact lowercase `kind`.
Every object has only the properties listed here:

| Kind | Remaining properties |
|---|---|
| `null` | none |
| `bool` | `value`, JSON Boolean |
| `i64` | `value`, canonical signed decimal string |
| `u64` | `value`, canonical unsigned decimal string |
| `decimal` | `precision` in `1..=38`, `scale` in `0..=precision`, and canonical signed-decimal-string `coefficient` whose magnitude is below `10^precision` |
| `money` | exact three-uppercase-ASCII `currency` plus the same `precision`, `scale`, and `coefficient` properties |
| `string` | exact UTF-8 `value`, at most 1,048,576 bytes |
| `bytes` | `value`, canonical padded RFC 4648 standard base64 for at most 1,048,576 decoded bytes |
| `timestamp` | canonical signed-decimal-string `seconds` and JSON-integer `nanos` in `0..=999999999` |
| `date` | JSON-integer `days_since_unix_epoch` in the signed 32-bit range |
| `uuid` | `value`, canonical lowercase hyphenated UUID text |
| `enum` | nonzero JSON-integer `type_id` and `variant_id`, each in the unsigned 32-bit range |
| `list` | `values`, an ordered array of at most 65,535 tagged Values |
| `record` | `fields`, an array of at most 65,535 `{ "field_id": <nonzero-u32>, "value": <tagged-Value> }` objects in strictly increasing field-ID order |

Signed decimal strings are `0` or `-?[1-9][0-9]*`; unsigned decimal strings are
`0` or `[1-9][0-9]*`. Numeric range, UTF-8 byte length, decoded byte length,
base64 re-encoding, decimal cross-field rules, record ordering, complete encoded
Value size, and maximum nesting depth 32 are authoritative semantic checks even
where JSON Schema cannot express them. No display name, floating-point number,
JSON object map, lossy integer, or schema-selected field alias is substituted.
WP-137 freezes both exact Draft 2020-12 documents as the service-owned canonical
sources
`crates/riffdb-service/schema/riffdb.command-operation-envelope-v1.schema.json`
and
`crates/riffdb-service/schema/riffdb.command-get-outcome-result-v1.schema.json`.
Their exact IDs are `riffdb.command-operation-envelope/v1` and
`riffdb.command-get-outcome-result/v1`. Every command-tool discovery page carries
the required `OperationSchemaCatalog` with both checked IDs, 32-byte schema
hashes, and byte-identical canonical JSON. HTTP consumes the API-neutral catalog
and stdio consumes its public gRPC conversion. WP-140 uses one common
`riffdb-api-mcp` parser/composer for both transports and never maintains an MCP
copy. Each source is at most 65,536 bytes and their complete artifact charges
together are at most 131,584 bytes. A full discovery page carries both bodies;
a compact page or `catalog_unchanged` carries only the exact ordered IDs and
hashes in its required fence. A changed identity cannot return
`catalog_unchanged`. Any schema revision requires a new schema ID/version and
compatibility review.

The generic envelope schema is not embedded in or hashed into a compiled bundle.
This decision changes no compiler schema artifact, contract bundle
encoding/hash, plan hash, canonical input hash, or declared outcome. Any change
to the bundle outcome union remains compiler-owned; any change to the generic
envelope, fixed GetOutcome result, or tagged-Value mapping requires a new
service-envelope version and MCP compatibility review.

Dereference is a second closed lookup variant of the existing API-neutral
ResolveCommandOutcome operation, not a new generic read. The service reconstructs
the complete durable identity only from trusted database/environment
configuration, the authenticated principal (whose stable ID must exactly equal
`<principal>`), ADR-0026's trusted `OperationTenantScope::grammar_v1_global()`,
the URI's checked `<lineage>` and stable `<command-id>`, and the decoded digest
tuple. Together these are exactly the URI/context-supplied components missing
from the trusted database/environment components of ADR-0005 identity; contract
version and tool text do not replace an identity component. The second safe
point still requires the stored owner tenant to be Global. Future tenant-mapped
grammar requires an accepted locator-v2 decision rather than adding a caller-
selected tenant segment to this v1 URI.

Resolution performs one canonical idempotency-key lookup. On a found terminal
row, it requires the stored identity lineage and command ID to equal the URI,
uses the row's exact stored contract version/bundle/plan reference for one direct
historical-version lookup, and requires that bundle's compiler-owned ADR-0020
tool name to equal `<tool-name>`. It never searches active commands by name,
enumerates historical versions, or scans idempotency records. The redundant tool
segment is a public readability and anti-misbinding check, not lookup authority.
The operation performs ADR-0026's initial existence-blind authorization before
lookup and the same terminal disclosure authorization used for raw-key
resolution before release. Unknown, stale, tool-mismatched, identity-mismatched,
unreadable-key, missing-historical-bundle, not-found, and denied cases fail closed
without revealing which component matched. The adapter cannot construct a
storage key, inspect digest material, scan records, or call storage directly.

This requires reviewed additive service/public fields: a checked opaque locator
on terminal Execute/GetOutcome results and a locator branch on GetOutcome input.
ADR-0040 defines exact additive public fields
`ExecuteCommandResponse.optional string outcome_uri = 9` and
`GetOutcomeRequest.optional string outcome_uri = 5`, with the latter exclusive
with the three legacy raw-lookup fields. Whether the locator shares the existing
service-operation/audit tag is accepted together with the explicit amendments
to ADR-0006, ADR-0007, ADR-0026, and ADR-0028. WP-137 and WP-140 must not
implement a hash-only adapter shortcut.

The public wire field remains compatibility-optional for an upgraded ordinary
SDK reading a pre-WP-137 server: absence is the legacy response shape, while a
present value must pass every canonical/status/identity relation. A WP-137
server always emits it for a durable result. MCP initialization proves the
bridge by successfully calling the new WP-137 discovery RPCs and fails if they
are unimplemented; after that proof both MCP transports strictly reject a
durable result without the locator. MCP's operation schemas may therefore
require `outcome_uri` without making the general v1 SDK reject a valid legacy
response.

### Cursor presentation

The API-neutral cursor remains exactly 16 opaque random bytes. Every MCP cursor
string is exactly 32 lowercase hexadecimal characters, two per byte. Prefixes,
hyphens, uppercase, whitespace, percent encoding, odd length, and base64 forms
reject. MCP adapters may encode/decode only this presentation; they cannot
inspect cursor state. Cursor text is sensitive opaque state and is redacted from
telemetry.

### Tools, schemas, content, and identities

ADR-0020's compiled command tool name is consumed verbatim for discovery and
dispatch. Input schemas and declared-outcome unions are the compiler's
transport-neutral Draft 2020-12 artifacts. The advertised output schema is only
the exact mechanical ADR-0013 composition of that union with the service-owned
generic operation envelope above. MCP does not rederive names, fields, types,
outcomes, or envelope semantics. Every input is structurally validated against
the advertised schema before service conversion; the service still performs
authoritative schema selection and semantic validation.

The 14 fixed tools use one versioned `riffdb-api-mcp` registry; a transport may
not derive their public contract from Rust debug output, generated Protobuf
serialization, runtime `schemars`, or reflection. In exact `FixedToolKind` order
the source mappings are:

| Fixed tool | Public request/result mapping |
|---|---|
| `riffdb.contract.validate` | `ValidateContractRequest` / `ValidateContractResponse` |
| `riffdb.contract.get_active` | `GetActiveContractRequest` / `GetActiveContractResponse` |
| `riffdb.contract.explain_command` | `ExplainCommandRequest` / `ExplainCommandResponse` |
| `riffdb.contract.deploy` | `DeployContractRequest` / `DeployContractResponse` |
| `riffdb.command.get_outcome` | `GetOutcomeRequest` / `GetOutcomeResponse` |
| `riffdb.entity.get` | `GetEntityRequest` / `GetEntityResponse` |
| `riffdb.entity.scan_index` | `ScanIndexRequest` / `ScanIndexResponse` |
| `riffdb.commit.get` | `GetCommitRequest` / `GetCommitResponse` |
| `riffdb.commit.scan` | `ScanCommitsRequest` / `ScanCommitsResponse` |
| `riffdb.provenance.trace` | `TraceProvenanceRequest` / `TraceProvenanceResponse` |
| `riffdb.projection.query` | `QueryProjectionRequest` / `QueryProjectionResponse` |
| `riffdb.projection.status` | `GetProjectionStatusRequest` / `GetProjectionStatusResponse` |
| `riffdb.outbox.list_pending` | `ListPendingOutboxDeliveriesRequest` / `ListPendingOutboxDeliveriesResponse` |
| `riffdb.server.health` | `HealthRequest` / `HealthResponse` |

The complete operation/fixed manifest contains exactly 29 unique artifacts:
the generic command envelope, the existing GetOutcome result, 14 fixed inputs,
and 13 new fixed results.
The GetOutcome result reuses the service-owned
`riffdb.command-get-outcome-result/v1` artifact and is not copied. A fixed
artifact ID is exactly
`riffdb.fixed-tool/<exact-tool-name>/input/v1` or
`riffdb.fixed-tool/<exact-tool-name>/result/v1`. Canonical manifest order is the
generic command envelope, the reused GetOutcome result, then each fixed tool in
enum order with input before result and the duplicate GetOutcome result omitted.
Each fixed artifact is at most 65,536 canonical UTF-8 bytes; all 27 new fixed
artifacts together are at most 1,048,576 bytes.

Full MCP `tools/list` output uses one explicit additive budget ledger. The
API-neutral full discovery response is at most 2,621,440 bytes; fixed-schema
bodies materialized from local `FixedToolKind` entries add at most 1,048,576
bytes; and every remaining MCP tool key, title, description, annotation,
JSON-RPC field, array delimiter, link, and framing byte is charged to an exact
524,288-byte adapter allowance. The three inclusive charges total 4,194,304
bytes. Canonical schema sources are parsed and emitted as JSON objects rather
than escaped JSON strings, and a body already charged in the service response
is not charged a second time merely because composition changes its container.
WP-137 freezes the lower service ceiling and a one-maximum-dynamic-item fixture;
WP-140's separately accepted 27-artifact registry must prove the complete
maximum fixed, dynamic, and mixed-page ledgers before any full list response is
written. Exceeding any component or aggregate charge fails before serialization
reaches a transport sink.

Every fixed input omits transport `request_id`; the adapter supplies one fresh
RiffDB RequestId through the accepted transport-specific source. Root objects
and result branches are closed. `u32`/`i32` and narrower integers are JSON
integers; `u64`/`i64` are canonical decimal strings. UUID bytes use canonical
lowercase hyphenated UUID text, typed hashes use fixed lowercase hexadecimal,
MCP cursors use 32 lowercase hexadecimal characters, and other opaque bytes use
canonical padded RFC 4648 base64. `Value` and `ValueRecord` use this ADR's tagged
representation. Optional absence omits the property. A closed oneof has exactly
one explicitly named branch. Enums use a checked lowercase registry with no
numeric fallback. Schema documents are JSON objects, not escaped JSON strings.
Unknown properties, aliases, alternate number/byte spellings, unknown enums,
lossy values, and response branches inconsistent with the request reject.

The registry fixture freezes, for every fixed tool, its exact name, RPC/service
operation, bounded title, description, annotations, input/result schema IDs,
hashes, canonical lengths, source paths, and converter ID. WP-140 begins with an
interface-only PR containing all 27 canonical sources, that fixture, and request-
to-service plus service-to-structured-content goldens for every result branch.
The same PR adds
`crates/riffdb-api-mcp/fixtures/resource-registry-v1.json`, freezing every
descriptor-to-list mapping, exact concrete/template URI, MIME type, stable
presentation name, bounded title/description, subscription flag, content
converter, and result-content golden in the structural order above. A human
maintainer must accept those exact bytes and mappings before fixed-tool or
resource presentation implementation proceeds. A later byte, field,
annotation, converter, or mapping change is public MCP compatibility review.

Declared business outcomes use `isError=false`. Transport, malformed-input,
authorization, idempotency misuse, unavailability, and internal failures use
the bounded protocol/tool-error layer appropriate to where they occur. Structured
results conform to `outputSchema`; optional compact JSON or Markdown text is
rendered only from already redacted structured output with bounded escaping and
injection-safe formatting.

MCP JSON-RPC request IDs, MCP session IDs, progress tokens, subscription IDs,
and transport connection identities are transport-only. They are never a RiffDB
`RequestId`, `AgentSessionId`, idempotency identity, canonical input, provenance
identity, cursor, or commit-order signal. Every hosted HTTP service call obtains
a fresh `RequestId` from the injected server source; stdio obtains one through
the public client. A retry preserves the caller idempotency key but uses a fresh
RequestId. A separately supplied RiffDB `AgentSessionId`, when supported by a
credential/request-context carrier, must pass the shared exact UUIDv7 validation
and is never inferred from an MCP session.

### Discovery, authorization, and redaction

Tool and resource discovery calls the shared policy-filtered discovery operations.
Visibility never grants execution or read authority. Every invocation and
resource read repeats current authorization, records the required durable audit,
applies current obligations, and denies stale names or narrowed policy. Bootstrap
is never exposed through MCP.

Tools and resources list-change notifications are server capabilities, not
client capabilities. After initialization, the adapter sends them when the
corresponding advertised `listChanged: true` inventory actually changes; it does
not wait for a nonexistent client negotiation bit. Resource update notifications
are different: they are sent only for an exact URI that the client explicitly
subscribed to after the advertised `subscribe: true` capability, and stop after
unsubscribe, cancellation, authentication loss, or session termination.

The subscribable v1 resource set is exactly active contract, command plan,
projection status, and server health, matching SPEC MCP-031's selected set.
Every other v1 resource kind rejects subscribe rather than allocating an
unsupported or useless handle. There are at most eight distinct subscribed URIs
per session. Repeating subscribe is idempotent; unsubscribe of an absent URI is
the bounded protocol result; a URI that becomes hidden is removed without
disclosing why.

WP-137 must add one conditional shape to each existing discovery operation, not
a seventh semantic operation or generic resource read. An initial
compact-observation `DiscoverCommandTools` or `DiscoverResources` request may
carry its prior exact `DiscoveryCatalogFence`; a cursor and prior fence are
mutually exclusive. Full representation always returns a byte-fitted page and
the complete operation catalog where applicable. After
fresh authentication, initial discovery authorization, and a fresh
`BegunInvocation::reauthorize` safe point, the result is either a closed
`catalog_unchanged` branch carrying the equal current catalog fence and no
items/cursor/schema bodies, or the compact first page for the new fence. Both branches run
normal terminal invocation completion, including durable audit when required.
Continuations retain the existing cursor semantics.

`catalog_unchanged` proves only equality of public catalog identity. It does not
prove that the caller previously observed that fence, that two discovery
operations had the same view, or that a policy-visible inventory is equal, and
it grants no invocation/read authority. A fabricated, cross-operation, or
different-credential prior fence may therefore receive this branch but gains no
inventory or authority from it. Ordinary gRPC callers must not infer more.
ADR-0040's interface-first tag table freezes these request/result fields and
WP-137 extends the API-neutral DTOs narrowly; no MCP type enters the service.

Only an MCP transport may retain its already materialized visible fingerprints
across `catalog_unchanged`, and only under all POC invariants together: stdio
loads one credential once and never reloads or switches it; HTTP binds the exact
`CapabilityId` to the session; `CapabilityGrantV1` is immutable after creation;
revocation or expiry makes fresh authentication fail and terminates the session;
and the accepted visibility policy is static for that server generation. A
visibility-policy implementation change requires a freshly and independently
sampled server generation.
Mutable
grants or mutable policy are forbidden from reusing this optimization until an
accepted design adds an authorization/visibility epoch to the public fence.
Every authentication, reauthorization, or transport failure discards the
pending refresh and retained inference rather than emitting from it. The
required 16-byte process generation and ordered operation-schema identity are
part of public fence equality. The generation is a fresh independent 128-bit
sample, not a uniqueness proof; acceptance explicitly tolerates its negligible
collision probability because retained observation state grants no read or
invocation authority. On the ordinary differing-generation path, a transparent
stdio gRPC reconnect to a restarted server discards old cursors/inference and
performs a fresh compact scan before notifying.

On a changed fence, either transport completely pages both policy-filtered
discovery operations in compact-observation representation before notifying.
The conditional initial request and every continuation use page limit 500, as
required by the exact cursor-bound normalized query. One maximum-size inventory
therefore yields exactly 500, 500, and 24 items in at most three public calls.
An over-limit inventory may return up to 500 items in the third response. The
adapter structurally validates the bounded response but rejects as soon as it
encounters item 1,025, or when the accepted 1,024th item is followed by a
cursor; it retains and fingerprints at most 1,024 visible items and requires
exact end for acceptance. Representation is also bound into the
server-side cursor state, so changing either the request limit or representation
on continuation rejects.
Expiry, authorization failure, mid-page fence change, or cursor failure
discards the refresh without a partial
notification; it retries only from a new initial request at the next five-second
tick and never retains a cursor across ticks.

The adapter retains compact exact MCP-visible fingerprints, not full schemas or
resource bodies. A tool fingerprint is the complete canonically ordered MCP tool
descriptor with each full input/output schema document replaced by its existing
compiler or fixed-schema hash; it therefore includes exact name, title,
description, annotations, stable target metadata, and both schema identities. A
resource fingerprint is the complete canonically ordered MCP list descriptor
with any full schema replaced by its existing schema hash. Canonical structural
equality is used; no new hash domain is invented.
Hidden candidates, hidden counts, global bundle bytes, and fields not presented
by MCP are excluded. The adapter sends `notifications/tools/list_changed` or
`notifications/resources/list_changed` only for the corresponding visible-
fingerprint change, so a hidden-only catalog change leaks neither a notification
nor a count. It then rereads each still-visible subscribed active-contract or
command-plan resource through GetActiveContract or the exact descriptor-fenced
read algorithm above and sends `notifications/resources/updated` only when that
exact authorized content changed. Command-plan updates are driven only by a
visible catalog-fence refresh.

HTTP's server-composed post-durability catalog hook may only coalesce one dirty-
catalog marker into each affected observer; it performs no service call, emit,
or idle refresh. Production stdio has no such private hook. Both transports run
the same session-observation loop on exact five-second ticks from an injected
monotonic scheduler, never `sleep` in a correctness test. On every tick they call
both conditional discovery operations with their prior common fence; stdio does
so only through the public gRPC client, while HTTP calls the same API-neutral
operations. A changed result drives the complete two-inventory pass above. The
loop also polls every explicitly subscribed projection-status or server-health
URI through GetProjectionStatus or Health; catalog-derived subscriptions are
checked only after a fence change.

The loop makes at most 180 ticks during the 900-second session lifetime and at
most eight subscribed-resource reads per tick. An unchanged tick uses at most
two conditional discovery calls plus those eight reads. A changed tick uses at
most three calls for each discovery inventory plus at most eight reads total
across all subscribed resource kinds. The absolute watcher-generated background
bound is therefore
2,520 watcher-generated background service calls, at most 1,500 service-
returned and structurally validated compact items per inventory per tick, and
at most 1,024 retained/fingerprinted items per inventory per tick. Client-
initiated tool, resource, and invocation calls are
not folded into that watcher count; they remain separately bounded by session
lifetime, rate limits, and in-flight limits. Background calls are
paced across the five-second tick under the same limiter and are sequentially
bounded; each obtains a fresh RequestId and fresh authentication/current-policy
decision, and invalid authentication closes the session. The adapter retains
only the dirty marker, prior typed fence, bounded visible fingerprints,
subscribed URIs, and typed catalog/frontier/lifecycle comparison state; it
retains no snapshot, storage transaction, cursor after its pass, authority proof,
or unrestricted resource body. HTTP and stdio use the same change predicates
and contents; delivery latency is transport-specific and is not semantic
divergence.

Each initialized session owns at most one bounded observation task. A 32-permit
server-wide `McpObserverSemaphore` bounds concurrent observation passes across
all sessions. A due task uses nonblocking acquisition; when no permit is
available it retains one coalesced due marker and retries at the next five-second
tick rather than queuing or spawning work. A successful pass holds one permit
only across its sequential bounded service calls and releases it on completion,
failure, or cancellation. It never waits for another registry permit while
holding this one. Only a successfully authenticated client request refreshes
the session's 300-second idle deadline. Conditional polls, subscription reads,
emitted notifications, output retries, and server-side catalog hooks do not keep
an idle client alive; the 900-second absolute lifetime remains independent.

Notification delivery never blocks the observer or a service call. Each session
retains at most one pending tools-list marker, one pending resources-list marker,
and one latest update marker per subscribed URI: at most ten markers total. A
newer observation replaces the same URI's pending marker. Before emission the
adapter reauthenticates and rereads the current authorized content, so a marker
does not retain stale or unrestricted content. Output backpressure coalesces to
the latest state; it never grows a queue, blocks observation, or emits every
intermediate frontier.

Notifications contain no hidden item count, secret, raw key, capability, policy
reason, unrestricted value, or stale pre-obligation content. Cancellation
releases every nondurable observation, cursor, and subscription handle exactly
once.

### Adapter limits and rate limiting

`riffdb-api-mcp` owns an injected monotonic `McpRateLimiter`; it does not reuse a
semantic wall clock or service cursor clock. Streamable HTTP performs one
pre-authentication token-bucket check keyed by the trusted socket peer IP, never
`Forwarded`/`X-Forwarded-For`. After authentication and target decoding, both
transports perform a second check keyed by the exact tuple `(transport source,
principal, policy-resolved tenant, service operation or compiler-owned tool
name)`. A discovery result, MCP session, or prior allow never bypasses either
applicable check.

The accepted POC defaults are a pre-authentication burst of 32 with refill 8 per
second and a post-authentication burst of 16 with refill 4 per second. The
registry holds at most 4,096 total buckets and removes only buckets idle for at
least 300 seconds; when no expired bucket is available, a new key is denied
rather than evicting an active key or running unbounded. Monotonic regression,
arithmetic overflow, limiter/provider failure, and capacity exhaustion fail
closed with bounded transport-safe errors. Configuration may lower but not raise
these hard limits.

Stdio and HTTP each accept at most 1,048,576 bytes in one complete inbound MCP
message. One complete encoded outbound JSON-RPC message, structured tool result,
resource, or error is at most 4,194,304 bytes including MCP wrapper/text/link
overhead; the adapter must lower the service payload budget to leave room for
that wrapper and never truncate an indivisible item. There are at most 128 live
MCP sessions, 256 in-flight requests server-wide, and 8 in-flight requests per
session. One session is idle for at most 300 seconds and lives at most 900
seconds. Rejected admission allocates no service request, progress task, cursor,
or subscription; cancellation and termination release every count exactly once.

These byte bounds are enforced by first-party transport wrappers on both sides
of `rmcp`; the stock SDK I/O/body collectors and serializers are not the sole
bound. The stdio wrapper reads one framed message into a capacity-limited buffer
and detects one excess byte before JSON decoding. HTTP rejects an over-limit
`Content-Length` before collection and counts chunk bytes into the same limit
with a one-byte excess probe when length is absent or chunked. Neither path
allocates from an untrusted declared length.

For stdio, the first-party `Transport` owns one capacity-limited JSON-plus-
newline serialization and checks the complete frame before stdout. Stateful
HTTP necessarily uses pinned rmcp 2.2.0's reviewed internal JSON-to-String and
SSE-to-Bytes passes. The private SessionManager preflights the typed initialize
response with its absent event ID before returning it. For every later stream
item, after the public local worker has assigned the event ID and before its
`ServerSseMessage` is yielded to rmcp's serializer, the private stream wrapper
performs the same SDK-equivalent typed count/preflight of the exact JSON-RPC
message and SSE `data`/`id` framing and rejects an excess. The wrapper-owned
pre-initialize ping uses a first-party capacity-limited final serializer and
does not enter this SDK path. The outer response-body gate then receives one
complete SDK `Bytes` frame,
requires its actual length to be at most 4,194,304 bytes, and only then yields
that frame to the HTTP server. An excess closes the session before yielding any
byte of that frame; previously admitted frames in the same stream are not
rolled back. This deliberately performs two bounded JSON serializations on HTTP
and does not claim sole serializer ownership. The pinned SDK's temporary String and
Vec are bounded by the already checked typed message, while the complete-frame
gate remains the authoritative network-write boundary. The disabled SDK
keepalive/retry settings above make one admitted typed message correspond to
one checked outbound SSE frame. SSE replay/event queues retain only bounded
typed messages and cannot concatenate an unbounded batch. Tests require exact
predicted-versus-actual frame equality for every output variant; any rmcp or
sse-stream framing change stops an upgrade. Framing, decode, compression, and
outbound-overhead boundary tests cover exact-limit and one-byte-over inputs;
HTTP request compression is disabled in the POC.

The reviewed SDK logs complete requests/results at some levels and logs raw
Streamable HTTP session IDs in some transport paths. Both binaries install a
non-overridable first-party tracing filter that discards every event whose
metadata target is `rmcp` or begins `rmcp::`, regardless of user log directives.
They emit only separately constructed bounded safe RiffDB transport events.
The `transport-io` feature intentionally supplies protocol stdin/stdout through
Tokio. The lock-graph source audit verifies that the pinned SDK has no separate
non-protocol stdout/stderr diagnostic sink that bypasses this filter; a changed
finding stops an SDK upgrade. The first-party bounded transport owns the handles,
frames, and write-before-limit check, while protocol stdout remains free of
diagnostics.

These constants and key fields require explicit review with this ADR. WP-140 may
not substitute an unbounded middleware default, process-global ambient clock,
caller-controlled source address, or rate limiter that runs after protected work.

### Progress, polling, and cancellation

Progress is sent only when negotiated and a client supplied a progress token.
Values are finite, nonnegative, monotonic safe integers. An operation sends at
most 32 progress notifications and only for service-observed milestones or
bounded units of completed work; the adapter does not fabricate time-based
percentages. Completion, failure, or cancellation permanently ends progress for
that request.

Request-scoped adapter-managed polling, if an operation genuinely needs it, is
limited to one in-flight service call, at most 32 observations, no more often
than once per 100 milliseconds under an injected monotonic testable clock, and
an overall 30-second service wait. Configuration may lower these bounds.
Ordinary command execution, deployment, projection waits, and cursor scans do
not gain adapter poll loops merely to fill progress or pages; their existing
bounded service call is authoritative. This 32-observation request bound is
distinct from the exact five-second, 900-second session-observation loop above;
it cannot silently stop a live resource subscription after 32 ticks. No storage
transaction, conflict capability, runtime frame, or synchronous mutex guard is
retained across a poll or await.

Cancellation stops future adapter work and releases nondurable resources. It
does not claim rollback after command submission. A caller resolves uncertainty
through `riffdb.command.get_outcome` using the same raw idempotency key or through
the authorized outcome resource minted for an already resolved identity.

The POC advertises no sampling, roots, elicitation, task execution, MCP Apps, or
prompt capability. Prompts may be added only after the required tool/resource
surface is stable and are not part of WP-140 acceptance.

## Options Considered

1. **Protocol-isolated adapter, public-gRPC stdio, and shared-service HTTP:**
   Accepted; it proves transport parity without a privileged local process.
2. **In-process stdio service or storage access:** rejected; it creates a second
   authority path and cannot prove public-client parity.
3. **General URI parser with normalization:** rejected; multiple spellings would
   create cache, authorization, fixture, and client ambiguity.
4. **Raw idempotency key in an outcome URI:** rejected; it leaks a caller secret
   and changes uncertainty identity handling.
5. **Hash-only outcome URI resolved by adapter/storage scan:** rejected; adapters
   lack trusted identity components and may not access storage.
6. **Remove the outcome resource and use only GetOutcome:** viable simplification
   but not selected here because it contradicts three current SPEC sections and
   requires an explicit product/specification decision.
7. **Opaque random outcome handle registry:** rejected for the POC; it adds a
   durable/nondurable lifecycle and recovery boundary without improving the
   existing keyed identity.
8. **Base64 MCP cursors:** rejected; lowercase fixed-width hex is simpler to
   validate and freezes one textual form for the existing 16 bytes.

## Consequences

- Both MCP transports remain presentations over existing service semantics.
- WP-137 must land before production stdio can cover the full MCP operation set.
- Outcome links require a narrow checked locator form in the existing resolve
  operation and reviewed additive public fields.
- Strict typed-ID locators are longer than ambiguous source-name templates but
  remain bounded and stable across display-name changes.
- The POC accepts loopback-only HTTP and defers remote authentication/TLS.
- Dependency and protocol revisions require a new compatibility review.

## Compatibility

Tool names remain owned by ADR-0020 and provenance text remains owned by
ADR-0024. All other URI bytes, cursor text, fixed-tool names, schemas, structured
result shapes, protocol version, route, and initialization metadata become
public compatibility fixtures only through this accepted record and the
separately required generated-artifact checkpoints.

Acceptance requires companion amendments to SPEC Sections 5.4, 11.1, 12.2
through 12.6, 12.8 through 12.12, and Appendix D for the exact dependency owner,
topology, initialization, authentication, locators, locator lookup, schemas,
notifications, cursor spelling, bounds, protocol, and pin. ADR-0009's complete
direct-owner table must add `riffdb-proto`, `riffdb-service`, and
`riffdb-api-mcp` for exact base64 0.22.1 with default features disabled and
`alloc` enabled, respectively for structural outcome-locator validation,
authoritative locator tuple encoding/decoding, and MCP presentation. ADR-0041
additionally adds the CLI structural-presentation owner. It
also requires the public/service and server-generation amendments described in
ADR-0040, including conditional discovery and the explicit ADR-0018/ADR-0009
entropy-purpose change. No durable
storage key, contract grammar, IR, plan hash, canonical input hash, commit
ordering, or atomicity changes.

The Section 12.6 URI table changes were explicit human-review items: contract
versions gain lineage; entity, command, and projection targets use exact lineage
plus stable numeric ID rather than ambiguous source/display names; the outcome
locator gains exact lineage and stable command ID alongside the complete
ADR-0020 tool name; and the key hash gains the exact versioned digest-tuple
presentation above. Grammar-v1 Global tenant scope remains trusted context under
ADR-0026 rather than a caller-controlled segment. These are not editorial
clarifications and take effect through the accepted companion reconciliation.

## Security

Authentication is not authorization, discovery is not authority, and a resource
locator is not a capability. Current policy and obligations are applied before
each release. Stale, malformed, mismatched, unknown, expired, and unauthorized
requests fail closed with bounded safe errors.

Raw credentials, idempotency keys, digest-key material, capability tokens,
session IDs, partition keys, cursors, internal errors, and unrestricted values
never appear in tool descriptions, resource metadata, stdout diagnostics, logs,
metrics, or traces. Key-hash locators and principal text are treated as sensitive
correlation data even though the keyed digest tuple is not secret. Host, Origin,
protocol, authentication, and session checks precede protected body work and
fail without exposing session or credential existence. User-controlled text is
escaped and bounded before model-facing output.

## Testing

WP-137 freezes the additive public descriptor/message/RPC inventory, outcome-
locator wire fields, exact error carriage, and stdio client support. WP-140 adds:

- initialization fixtures for the exact metadata/capabilities, wrapper-owned
  stateful-HTTP and rmcp-owned stdio pre-initialize ping paths, exact baseline,
  all three older-known versions, the one newer-known version, and a
  syntactically valid unknown-newer version offer, private-
  sentinel fallback, optional matching initialize header, and mandatory post-
  initialize version header;
- exact tool, URI, MIME, and lowercase-cursor goldens plus the human-accepted
  27-source fixed-tool registry and resource/template registry, all schema
  IDs/lengths/hashes, annotations, list-surface mappings, subscription flags,
  and every request/result/content converter branch;
- malformed URI/percent/base64url/decimal/UUID/cursor tables and parser fuzzing;
- raw-key-to-locator minting; exact 1,745-byte maximum; lineage/command/tool
  distinctness; locator lookup; stale tool/key ID; principal/stored-tenant
  mismatch; unreadable key; no-scan proof; existence-blind denial; and two-phase
  disclosure tests;
- equivalent authorized catalogs/content over stdio and loopback HTTP;
- public-client protected-credential exact file-rule fixtures, zeroized staging-
  buffer and redacted wrapper/error tests, canonical-token cross-fixtures, and
  server-authoritative malformed-token rejection without a stdio-to-auth edge;
- stale/unknown/hidden tool denial and invocation-time reauthorization;
- schema/result conformance, invocation-independent fixed GetOutcome schema,
  fixed-schema 65,536-byte individual/1,048,576-byte aggregate bounds, tagged-
  Value round trips/bounds, and business-outcome `isError=false` cases;
- canonical generic-envelope schema and mechanical composition goldens for every
  command, all three status branches, outcome resource-link content, and fixed
  GetOutcome, with no compiler artifact or bundle-hash drift;
- old-client/new-server and new-client/old-server outcome-locator compatibility,
  new-server mandatory emission, initialization rejection of a pre-WP-137
  server, and strict MCP rejection of a missing durable-result locator;
- progress monotonicity/count/rate bounds, cancellation, poll exhaustion, and
  no post-terminal notification schedules using injected time rather than sleeps;
- deterministic pre/post-auth rate-limit schedules, exact composite-key
  separation, bucket/session/in-flight exhaustion and recovery, monotonic clock
  faults, exact Host/Origin/Authorization/protocol/session-header matrices,
  custom SessionManager Pending/Active state, one-candidate insert-if-absent,
  invalid/collision/capacity rejection without overwrite or worker leakage,
  no map guard across await, promotion/abandonment/exact-once cleanup,
  capability-to-session binding, source-header spoofing, and exact custom
  1 MiB/4 MiB transport boundaries;
- list-change and resource-update schedules covering visible versus hidden-only
  changes, compact/full representation separation, 4,096-byte compact item and
  2,621,440-byte full-page limits, the complete
  2,621,440 + 1,048,576 + 524,288 outbound ledger,
  1,024/1,025-item compact discovery, an accepted three-limit-500 sequence of
  exactly 500/500/24 items, over-limit third pages up to 500 items with rejection
  at item 1,025 or a cursor after 1,024, full byte-fitting with fewer items, cursor
  abort with next-tick restart, eight-subscription/observer-semaphore exhaustion, idle
  expiry unaffected by watcher output, coalescing/backpressure, and cancellation;
- `catalog_unchanged` schedules covering fabricated and cross-operation fences,
  a different credential, revoke/expiry, fresh reauthorization and completion,
  zero candidate/page/cursor allocation, process-generation and operation-schema
  identity changes across transparent gRPC reconnect/restart, and the
  prohibition on ordinary-client visibility inference;
- secret and instruction-injection canaries across content, errors, and telemetry;
- raw token, session-ID, request, result, and notification canaries proving the
  non-overridable `rmcp` tracing filter and protocol-only stdout;
- feature/differential architecture checks proving default-empty features,
  byte-identical common rendering, and a production-stdio cargo tree with no
  auth, service, storage, policy, runtime, commit, catalog, server, or
  `riffdb-api-grpc` server-feature dependency; and
- official MCP Inspector smoke tests over both transports.

The lock-graph review and `cargo deny check` are acceptance evidence, not merely
documentation. WP-200 supplies final cross-transport authorization and recovery
evidence.

## Requirements and Work Packages

- **Requirements:** `MCP-001`, `MCP-010`, `MCP-011`, `MCP-020` through
  `MCP-024`, `MCP-030` through `MCP-033`, and `MCP-040` through `MCP-049`
- **Defines or blocks:** `WP-137` public parity bridge, `WP-140`, MCP-aware
  presentation in `WP-180`, and final HTTP composition in `WP-185`
- **Final evidence:** `WP-200`

## Decision Deadline

The exact-text decision deadline was satisfied on 2026-07-22. The `rmcp`
dependency still cannot merge before the separate lock-graph review, and the
record's explicitly deferred interface-only fixture checkpoints still require
their own human acceptance before generated bytes or consumers merge.
