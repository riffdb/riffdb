# ADR-0008: Native MCP Tool and Resource Model

- **Status:** Proposed
- **Direction approved:** 2026-07-12
- **Exact text accepted:** No
- **Partially resolved by:** ADR-0020 for command tool-name grammar,
  normalization, collision handling, and compiler/catalog ownership; ADR-0024
  for the exact provenance resource locator
- **Paired proposal:** ADR-0040 for the public gRPC parity bridge required by
  production stdio
- **Would amend:** ADR-0009's protected-file/direct-owner boundary and
  ADR-0037's public-client dependency allowlist for the narrow client-owned
  normal-bearer loader
- **Decision deadline:** Before WP-137 changes the public protocol or WP-140
  freezes an MCP compatibility fixture

Direction approval is not acceptance of this text. In particular, the outcome-
resource locator amendment below changes already accepted public/service
interfaces and requires explicit human review together with ADR-0040.

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
a checked service lookup form or a forbidden storage scan. This proposal keeps
the resource and defines a narrow service-owned lookup form; it does not pretend
the existing interface can already serve it.

## Proposed Decision

### Protocol and dependency baseline

The POC implements MCP protocol version **2025-11-25**. Initialization reports
that exact version and accurate server implementation metadata. A syntactically
valid different client offer receives `2025-11-25` as the server-selected
version under protocol negotiation and the client disconnects if it cannot use
that version; malformed version input rejects. RiffDB never silently advertises
or enables another protocol version.

`riffdb-api-mcp` is the sole first-party owner of the MCP SDK boundary. The
workspace pins `rmcp = "=2.2.0"`, disables its default features, and enables
exactly `server`, `transport-io`, and `transport-streamable-http-server`. No
client, sampling, roots, elicitation, task, OAuth, TLS, key-value, or unrelated
transport feature is enabled merely for convenience.

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

The `rmcp` 2.2.0 registry knows protocol versions newer than this POC baseline,
and its default server initializer would echo one of those versions. Each
transport therefore owns a bounded typed negotiation wrapper outside
`serve_server`; RiffDB does not fork or patch `rmcp`. The wrapper permits the
lifecycle's pre-initialization ping. For an initialize request it validates the
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

The adapter intercepts each successful initialization response before release
and binds its returned `Mcp-Session-Id` to the authenticated `CapabilityId`.
Every later POST, GET, or DELETE reauthenticates its presented credential and
must match that binding; a different capability cannot reuse another session or
SSE stream even when it resolves to the same principal. Each service call and
each emitted notification obtains fresh authentication/current-policy evidence;
a live SSE handler retains only its own bounded zeroizing credential for that
purpose. Initialization failure creates no binding. The private session map uses
the injected monotonic clock, the 128-session server bound, 300-second idle and
900-second lifetime bounds, and exact-once deletion on expiry, DELETE, transport
termination, or cancellation. Capacity exhaustion denies a new initialization
without evicting a live session. Session IDs and credential material are absent
from telemetry and public errors. An SDK-produced session ID must be nonempty
visible ASCII of at most 128 bytes and unique among live bindings; an invalid or
colliding ID fails initialization and leaves no binding.

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
DiscoverResources to exact end within the same three-call/1,024-item bound and
obtain the one visible command-resource descriptor plus its catalog fence. Under
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
copy. A `catalog_unchanged` response carries no page or schema catalog because it
follows an already validated page in the same MCP session.
The two immutable v1 schema hashes are not folded into `DiscoveryCatalogFence`;
any schema revision requires a new schema ID/version and compatibility review.

The generic envelope schema is not embedded in or hashed into a compiled bundle.
This proposal changes no compiler schema artifact, contract bundle
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
ADR-0040 proposes exact additive public fields
`ExecuteCommandResponse.optional string outcome_uri = 9` and
`GetOutcomeRequest.optional string outcome_uri = 5`, with the latter exclusive
with the three legacy raw-lookup fields. Whether the locator shares the existing
service-operation/audit tag as proposed requires explicit human acceptance
because ADR-0006, ADR-0007, ADR-0026, and ADR-0028 are already Accepted. WP-137
and WP-140 must not implement a hash-only adapter shortcut while that review is
pending.

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
`DiscoverCommandTools` or `DiscoverResources` request may carry its prior exact
`DiscoveryCatalogFence`; a cursor and prior fence are mutually exclusive. After
fresh authentication, initial discovery authorization, and a fresh
`BegunInvocation::reauthorize` safe point, the result is either a closed
`catalog_unchanged` branch carrying the equal current catalog fence and no
items/cursor, or the ordinary first page for the new fence. Both branches run
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
and the accepted visibility policy is static for that session's lifetime. A
visibility-policy implementation change terminates affected sessions. Mutable
grants or mutable policy are forbidden from reusing this optimization until an
accepted design adds an authorization/visibility epoch to the public fence.
Every authentication, reauthorization, or transport failure discards the
pending refresh and retained inference rather than emitting from it.

On a changed fence, either transport completely pages both policy-filtered
discovery operations before notifying. The conditional initial request uses
page limit 500; each continuation uses the lesser of 500 and the remaining
1,024-item observation capacity, so the third request's maximum is 24. One
inventory may inspect at most 1,024 visible items in at most three public calls
and must reach an exact final page; a cursor after item 1,024 is overflow.
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
across all subscribed resource kinds. The absolute session bound is therefore
2,520 service calls and 1,024 inspected items per inventory per tick. Calls are
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

The proposed POC defaults are a pre-authentication burst of 32 with refill 8 per
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
of `rmcp`; the stock SDK I/O/body collectors and serializers are not accepted as
the bound. The stdio wrapper reads one framed message into a capacity-limited
buffer and detects one excess byte before JSON decoding. HTTP rejects an
over-limit `Content-Length` before collection and counts chunk bytes into the
same limit with a one-byte excess probe when length is absent or chunked. Neither
path allocates from an untrusted declared length. Outbound JSON is serialized
once into a capacity-limited staging buffer that includes the JSON-RPC and
newline or SSE-event framing; exceeding 4,194,304 bytes fails before any stdout
or response-body byte is written. SSE replay/event queues count the already
bounded encoded frame and cannot concatenate an unbounded batch. Framing,
decode, compression, and outbound-overhead boundary tests cover exact-limit and
one-byte-over inputs; HTTP request compression is disabled in the POC.

The reviewed SDK logs complete requests/results at some levels and logs raw
Streamable HTTP session IDs in some transport paths. Both binaries install a
non-overridable first-party tracing filter that discards every event whose
metadata target is `rmcp` or begins `rmcp::`, regardless of user log directives.
They emit only separately constructed bounded safe RiffDB transport events.
The lock-graph source audit verifies that the pinned SDK has no direct stdout,
stderr, or alternate logging sink that bypasses this filter; a changed finding
stops an SDK upgrade. Protocol stdout remains owned exclusively by the bounded
stdio writer.

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
   proposed; it proves transport parity without a privileged local process.
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
public compatibility fixtures only when this record is accepted and generated.

Acceptance requires companion amendments to SPEC Sections 11.1, 12.2 through
12.6, 12.8 through 12.12, and Appendix D for the exact topology, initialization,
authentication, locators, locator lookup, schemas, notifications, cursor
spelling, bounds, protocol, and pin. It also requires the public/service
amendments described in ADR-0040, including conditional discovery. No durable
storage key, contract grammar, IR, plan hash, canonical input hash, commit
ordering, or atomicity changes.

The Section 12.6 URI table changes are explicit human-review items: contract
versions gain lineage; entity, command, and projection targets use exact lineage
plus stable numeric ID rather than ambiguous source/display names; the outcome
locator gains exact lineage and stable command ID alongside the complete
ADR-0020 tool name; and the key hash gains the exact versioned digest-tuple
presentation above. Grammar-v1 Global tenant scope remains trusted context under
ADR-0026 rather than a caller-controlled segment. These are not editorial
clarifications and do not take effect while this ADR remains Proposed.

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

- initialization fixtures for the exact metadata/capabilities, pre-initialize
  ping, exact and newer-known version offers, private-sentinel fallback, optional
  matching initialize header, and mandatory post-initialize version header;
- exact tool, schema, fixed-tool, URI, MIME, and lowercase-cursor goldens;
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
  tagged-Value round trips/bounds, and business-outcome `isError=false` cases;
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
  capability-to-session binding, source-header spoofing, and exact custom
  1 MiB/4 MiB transport boundaries;
- list-change and resource-update schedules covering visible versus hidden-only
  changes, 1,024/1,025-item discovery, three-call completion, cursor abort with
  next-tick restart, eight-subscription/observer-semaphore exhaustion, idle
  expiry unaffected by watcher output, coalescing/backpressure, and cancellation;
- `catalog_unchanged` schedules covering fabricated and cross-operation fences,
  a different credential, revoke/expiry, fresh reauthorization and completion,
  zero candidate/page/cursor allocation, and the prohibition on ordinary-client
  visibility inference;
- secret and instruction-injection canaries across content, errors, and telemetry;
- raw token, session-ID, request, result, and notification canaries proving the
  non-overridable `rmcp` tracing filter and protocol-only stdout;
- architecture checks forbidding auth, storage, policy, runtime, commit, catalog,
  server, and in-process service dependencies from production stdio; and
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

The exact locator, outcome-lookup, protocol, SDK feature, audience, transport,
cursor, progress, and fixture text must be accepted before WP-137 or WP-140
publishes the affected interface. The `rmcp` dependency cannot merge before the
separate lock-graph review. Until then, this record is planning input only.
