# ADR-0018: UUIDv7 Generation, Ownership, and Replay Boundaries

- **Status:** Accepted
- **Direction approved:** 2026-07-13
- **Exact text accepted:** 2026-07-13
- **Accepted:** 2026-07-13
- **Requires:** ADR-0004, ADR-0005, ADR-0007, ADR-0009, ADR-0011, and
  ADR-0012
- **Amends:** ADR-0004 database initialization and `CommitIntent` provenance
  ownership; ADR-0007 request and incident source boundaries; ADR-0009
  bootstrap generation and the reviewed `getrandom` direct-owner set; ADR-0011
  UUIDv7 construction; and ADR-0012's no-runtime-entropy boundary
- **Decision deadline:** Pure construction before WP-010 closes; database and
  provenance ownership before WP-060; production sources before their owning
  packages merge

The human maintainer accepted this exact UUIDv7 generation, ownership, and
replay record on 2026-07-13. The maintainer also accepted the exact production
source owners and dependency extension below.

## Context

Accepted ADR-0011 fixes `DatabaseId`, `CapabilityId`, and `ProvenanceId` as
UUIDv7 values in 16-byte network order. The accepted service and idempotency
decisions require a UUIDv7 `RequestId`; the foundational type set also contains
UUIDv7 `AgentSessionId` and `IncidentId`. `riffdb-types` already owns validation,
canonical bytes, display, and newtype separation, but the accepted records do
not yet define how first-party code constructs these values or where operating-
system time and entropy may enter.

Leaving generation implicit would let storage, the deterministic runtime,
transports, or several unrelated crates acquire ambient clock and entropy
authority. It would also leave durable database initialization, provenance
collision behavior, uncertain-commit replay, MCP request correlation, bootstrap
retention, and incident creation to incompatible package-local choices.

The UUID timestamp is useful only to satisfy the UUIDv7 representation. RiffDB
already has separate authoritative clocks and orders: `AdmissionClock`,
authentication and authorization clocks, `AdministrationClock`,
`CommitSequence`, and `AdministrationSequence`. An identifier timestamp must not
silently replace any of them.

## Decision

### Foundational representation and pure construction

`riffdb-types` remains the sole owner of the six UUIDv7 newtypes:

- `RequestId`;
- `AgentSessionId`;
- `IncidentId`;
- `DatabaseId`;
- `CapabilityId`; and
- `ProvenanceId`.

Each type retains its checked `from_bytes([u8; 16])` constructor and canonical
network-order byte access. The common UUIDv7 newtype implementation additionally
exposes this value-only construction operation for each type:

```rust
pub fn from_unix_milliseconds_and_random(
    unix_milliseconds: u64,
    random: [u8; 10],
) -> Result<Self, UuidV7ConstructionError>;
```

`UuidV7ConstructionError` is owned by `riffdb-types`, has the closed case
`UnixMillisecondsOutOfRange`, and exposes only static safe text. The constructor
is synchronous, deterministic, allocation-free, and free of I/O, clock access,
entropy access, global state, or dependencies beyond the foundation crate. It
rejects every timestamp greater than `0xffff_ffff_ffff`; it never truncates,
wraps, clamps, or substitutes a value.

For accepted input, construction is exactly:

```text
bytes[0..6]  = unix_milliseconds as the low 48 bits of u64 big endian
bytes[6]     = 0x70 | (random[0] & 0x0f)
bytes[7]     = random[1]
bytes[8]     = 0x80 | (random[2] & 0x3f)
bytes[9..16] = random[3..10]
```

This places the RFC 9562 version-7 nibble and `10` variant bits explicitly and
uses 74 independent random bits. The masked six high source bits are discarded.
The exact primary golden is:

```text
unix_milliseconds = 0x0123456789ab
random             = 00 01 02 03 04 05 06 07 08 09
UUID bytes         = 01 23 45 67 89 ab 70 01 82 03 04 05 06 07 08 09
canonical text     = 01234567-89ab-7001-8203-040506070809
```

The zero and upper-bound goldens are
`00000000-0000-7000-8000-000000000000` and
`ffffffff-ffff-7fff-bfff-ffffffffffff`, respectively. The value immediately
above the 48-bit timestamp maximum rejects.

No `uuid`, `rand`, or alternative identifier dependency is added. Making the
pure constructor available to a runtime dependency does not grant ambient
authority: runtime can already validate caller-owned bytes, and it receives no
clock, entropy source, or system generator.

### Production system-source contract

A production system generation attempt performs these steps exactly once and in
this order:

1. Read `std::time::SystemTime::now()` once.
2. Require a value at or after `UNIX_EPOCH`, convert it to elapsed whole Unix
   milliseconds using the UUIDv7-required millisecond precision, and reject a
   value above the 48-bit maximum. Sub-millisecond time does not enter the UUID.
3. Fill one fresh `[u8; 10]` with exactly one call through the approved
   `getrandom` provider.
4. Invoke the pure `riffdb-types` constructor.

There is no retry, process-global counter, monotonic clamp, remembered last
timestamp, fallback seed, pseudo-random substitute, or cross-call ordering
state. Two IDs generated in one millisecond are ordered by their random fields.
A host-clock rollback may produce a later-created ID that sorts earlier. RiffDB
therefore claims valid time-sortable UUIDv7 layout and collision resistance, not
strict creation order or global uniqueness.

The embedded timestamp and random fields are never interpreted as trusted
creation time, transaction time, authorization time, issue or expiry time,
deadline, idempotency input, storage order, commit order, or administration
order. Server validation of externally supplied IDs checks canonical UUIDv7
structure, not whether the embedded time is close to the server clock.

Production clock and entropy implementations remain thin and injectable. Tests
use explicit local fakes; no test source, mutable global clock, or deterministic
entropy feature is present in a production graph.

### Exact source and dependency owners

Only these first-party crates directly own a production UUIDv7 system source:

| Crate | UUIDv7 production purposes |
|---|---|
| `riffdb-auth` | Bootstrap `CapabilityId`; separate existing capability-token entropy |
| `riffdb-client-rust` | Outer `RequestId`; convenience `CapabilityId` and `AgentSessionId` generation for callers |
| `riffdb-server` | New-database `DatabaseId` candidate; injected `ProvenanceIdSource`; hosted-MCP `RequestId` source; injected `IncidentIdSource`; existing server cursor IDs |

This record extends ADR-0009's reviewed direct-owner row to exactly this graph:

```toml
getrandom = { version = "=0.3.4", default-features = false }
```

No optional feature is enabled. The already reviewed version, target-specific
transitive graph, licenses, build-script behavior, and external unsafe inventory
are unchanged. `riffdb-client-rust` is the only new direct owner; `riffdb-auth`
and `riffdb-server` retain their accepted ownership. Every first-party crate
continues to forbid unsafe code.

`riffdb-types`, `riffdb-errors`, `riffdb-commit`, every storage crate,
`riffdb-service`, `riffdb-api-grpc`, `riffdb-api-mcp`, `riffdb-mcp-stdio`,
`riffdb-cli`, `riffdb-runtime`, and compiler crates do not directly depend on
`getrandom` for production UUID generation. They receive checked values or a
narrow consumer-owned source port. Any additional direct owner, version,
feature, implementation, native/unsafe inventory, license, or transitive-graph
change requires renewed human dependency review.

### Durable database identity

`DatabaseId` is generated only for a truly uninitialized database. WP-060's
semantic storage boundary first exposes a source-free checked probe with the
closed result `Existing(DatabaseId) | NeedsInitialization`; malformed or partial
metadata is an integrity error, never `NeedsInitialization`. `riffdb-server`
invokes its production source only after `NeedsInitialization`, then passes the
checked candidate back through `riffdb-commit`'s production
`DatabaseInitializationExecutor`, which alone invokes the separate atomic storage
transition. Server composition never receives a storage mutation handle. Storage
accepts no clock or entropy provider. The memory engine uses explicit fixture
candidates.

The initialization transition re-proves true emptiness. It returns the installed
identity or, if another initializer won after the probe, the already durable
identity without comparing it to the losing candidate. Any newly observed
partial/nonempty state is corruption. Thus normal reopen performs no invisible
candidate generation, and a race cannot replace or reinterpret a winner.

WP-070 atomically installs the candidate with the complete required initial
metadata. A store is truly uninitialized only when no RiffDB metadata or
semantic record has been committed. A nonempty or partially initialized store
with a missing, malformed, or non-v7 database identity is corrupt and fails
readiness; it never receives a replacement identity.

A crash before the initialization transaction commits may leave a truly
uninitialized store and a later attempt may propose a different invisible
candidate. Once initialization commits, reopen returns the stored identity and
never regenerates, replaces, repairs, or compares it to a newly generated
candidate. Backup and restore preserve the exact identity. Capability database
binding and ADR-0005 idempotency identity always use this durable value.

Database identity is not a configured cluster identity, a node identity, or a
timestamp. ADR-0019 explicitly defers durable node identity beyond the POC: the
POC defines no `NodeId` type, source, storage key, or initialization transition,
and it cannot reuse `DatabaseId` as a substitute.

### Command provenance identity

`riffdb-commit` owns this synchronous, object-safe consumer port and its internal
source error:

```rust
pub trait ProvenanceIdSource: Send + Sync {
    fn next_provenance_id(&self) -> Result<ProvenanceId, ProvenanceIdSourceError>;
}
```

`riffdb-server` implements the production port with its system UUIDv7 source;
`riffdb-testkit` supplies deterministic explicit fakes. Neither
`riffdb-storage-api` nor a storage engine generates provenance IDs.

For each new terminal application commit, including a declared no-mutation
business rejection, the coordinator requests one `ProvenanceId` only after a
successful deterministic evaluation has produced an `EvaluatedCommand` and
before it opens the authoritative write transaction. The source call therefore
occurs outside deterministic runtime and outside every storage transaction. A
conflict capability may still be held. Transaction-current dependency and
capability validation remains inside the later narrow coordinator transaction
as required by ADR-0003, ADR-0004, and ADR-0009.

The storage-owned checked `CommitIntent` requires the generated
`ProvenanceId`. The coordinator includes it when assembling the intent from the
unchanged `EvaluatedCommand` and exact stored admission. The identifier then
keys one immutable provenance record and is linked from the outcome and commit
record in the same atomic record set.

The source is not invoked for:

- an unjournaled read-only command;
- an ADR-0012 `ExecutionFailed` terminal admission;
- a pending admission that has not produced a successful evaluation;
- an input mismatch, authorization denial, cancellation before execution, or
  other non-commit result; or
- terminal outcome replay.

The pending admission does not store or reserve a `ProvenanceId`. A proven
pre-commit abort exposes no candidate; a later safe resume may generate a
different candidate. After `CommitStatusUnknown`, the coordinator fences writes
and resolves the durable idempotency state before it may invoke the source
again. A committed resolution returns the original stored provenance ID and
never generates another. Only a proven non-commit that remains pending may
resume and obtain a new candidate.

An existing provenance key is an integrity collision. The complete application
transaction aborts with no sequence, mutation, outcome, event, outbox intent,
provenance replacement, or commit record. The coordinator does not silently
overwrite, reinterpret, or automatically regenerate within that attempt. It
returns an opaque internal defect, retains the pending admission, and fails
closed according to storage integrity policy.

`ProvenanceId`, its source, and its random or clock inputs never enter
`TransactionContext`, `EvaluatedCommand`, a declared outcome, a durable event
payload, or command-visible state. Only the final checked intent and durable
records contain the identifier.

### Transport request identity

`RequestId` identifies exactly one transport submission. The invoking client or
adapter generates it before constructing the API-neutral request context:

- Rust SDK operations use the `riffdb-client-rust` system source unless a caller
  supplies an already checked ID through an explicit low-level API.
- The CLI and `riffdb-mcp` stdio bridge use the Rust client source; neither owns
  another OS entropy dependency.
- The hosted MCP HTTP adapter owns a narrow consumer-side `RequestIdSource` port
  in `riffdb-api-mcp`; `riffdb-server` supplies its production implementation.
- In-process service and comparison tests provide explicit checked IDs.

The gRPC adapter validates the required wire value and rejects missing,
malformed, non-v7, or noncanonical input. It never fills, repairs, or replaces a
request ID. `riffdb-service` receives only the checked value in `RequestContext`
and owns no generator.

Every independent submission, including a transport retry, receives a fresh
outer `RequestId`. A mutating retry preserves the original caller idempotency key
and canonical command input. ADR-0005's first pending admission retains its
original `admission_request_id`; a later outer request ID remains current-call
trace and audit metadata and never overwrites deterministic command context.

An MCP protocol request identifier is unrelated transport framing and is never
converted, hashed, padded, or reused as the RiffDB `RequestId`. RiffDB does not
enforce global request-ID uniqueness and never uses the UUID timestamp for
authorization, retry identity, or logical time. The checked `RequestId` value
remains in ADR-0012 `TransactionContext`; only its clock, entropy, and source are
excluded from runtime.

### Capability identity

Normal capability creation remains ADR-0009's caller-selected operation. The
public request supplies a checked UUIDv7 `CapabilityId`; the Rust client may
provide a convenience generator. The server validates but never chooses,
repairs, or substitutes the normal-create ID. Reuse with equal normalized
content and reuse with different content retain ADR-0009's exact
`AlreadyCreatedTokenUnavailable` and detail-free `CapabilityIdConflict`
semantics.

The isolated `riffdb-auth::bootstrap_secret` helper generates the bootstrap
`CapabilityId` and the 32-byte bearer token offline. UUID and token generation
use separate provider fills and separate buffers; neither value is derived from,
hashed into, or reused as the other. The helper emits a credential document only
after both values and the exact document have been validated. Any later failure
drops and zeroizes owned token buffers. WP-150 retains and synchronizes the
accepted credential document before any RPC.

Bootstrap retry reuses the retained capability ID and token but creates a fresh
outer request ID. Normal create retry likewise reuses its caller-selected
capability ID while every transport submission receives a fresh request ID. The UUID timestamp is
untrusted request data and never supplies the coordinator-observed `issued_at`,
`expires_at`, authorization time, or administration order.

### Agent-session and incident identities

`AgentSessionId` is an optional externally supplied untrusted claim. Adapters
validate exact UUIDv7 structure; policy decides whether to accept it for the
exact operation. `riffdb-client-rust` may generate a convenience value through
the same client system source. A validated new-admission value may be frozen in
`AdmittedActorContext` under ADR-0007 and ADR-0012; retry claims never overwrite
that admitted value. Its timestamp grants no trust, age, authorization, or
session-lifetime meaning, and RiffDB enforces no global uniqueness registry.

`IncidentId` is server-generated and is never accepted from request data.
`riffdb-errors` owns a narrow synchronous `IncidentIdSource` port;
`riffdb-server` supplies the production implementation and tests inject explicit
values. Trusted error-containment boundaries request an incident before
constructing an internal error or exposing a public-safe correlation value.
Runtime may return its closed typed faults but receives no incident source and
never generates an incident. An incident identifies one observed internal
failure, is not a command or retry identity, and has no durable outcome or replay
stability promise.

Failure of the incident source itself is a readiness-failing internal condition.
There is no deterministic, request-derived, all-zero, counter, or non-v7
fallback and no false claim that a correlation ID was recorded. Protected output
is withheld and only bounded static emergency telemetry is permitted.

### Failure mapping

| Failure point | Required disposition |
|---|---|
| Pure constructor receives a timestamp above 48 bits | Return `UuidV7ConstructionError::UnixMillisecondsOutOfRange`; no value |
| System clock is before the Unix epoch, above the UUIDv7 range, or unavailable | Source failure; no clamp, retry, or value |
| OS entropy fill fails | Source failure; no fallback or partial value |
| Rust client request/capability/session generation fails | Typed local client error before request transmission |
| Bootstrap generation fails | No complete credential document and no RPC; owned token material is zeroized |
| New database candidate generation fails | Server startup fails before storage initialization and readiness |
| Hosted MCP request generation fails | Fail before `RequestContext`; no service invocation or durable service audit |
| Provenance generation fails | Opaque internal defect; no write transaction or application sequence; pending admission remains recoverable; an existing `started` invocation receives its required failed terminal audit |
| Durable provenance-key collision | Abort the complete transaction, retain pending state, emit an opaque incident, and perform no automatic replacement |
| Incident generation fails | Fail readiness, withhold protected output, and use no fabricated incident value |

No identifier-source failure is mapped to `OutcomeUnknown` unless an independent
authoritative commit already reached unknown status. Once commit status is
unknown, idempotency recovery, not identifier regeneration, determines the
result.

### Deterministic runtime boundary

UUIDv7 values may cross deterministic interfaces only where an accepted context
already names them. In particular, the original admitted `RequestId` and an
optional validated `AgentSessionId` remain value fields in ADR-0012's immutable
context. This record excludes their generators, clocks, entropy, source errors,
and generation history; it does not remove the values.

`riffdb-runtime` and `riffdb-invariant` have no dependency on `getrandom`,
`SystemTime`, a UUID source trait, a server composition type, or a test entropy
feature. Equal plan, snapshot, input, transaction context, and budget therefore
remain sufficient for equal evaluation.

### Explicit deferrals

This decision does not introduce:

- a strict-monotonic or cross-process UUID generator;
- a durable UUID counter, clock rollback ledger, or collision-repair tool;
- a global `RequestId`, `AgentSessionId`, or incident registry;
- a server-generated normal capability ID;
- derivation of an ID from request data, idempotency identity, commit sequence,
  token material, or another UUID;
- semantic decoding of UUID timestamps;
- an alternate time-sortable identifier under `ID-001`;
- UUID generation inside storage, runtime, policy, service, or protocol wire
  conversion; or
- recorded deterministic command randomness.

Any such feature requires an accepted decision that preserves the current
durable, authorization, and deterministic-runtime boundaries.

## Consequences

- Foundation code has one exact UUIDv7 layout and no ambient authority.
- Production entropy is limited to three reviewed composition/client/auth owners.
- Storage persists checked identifiers but never invents them.
- Provenance identity is generated late enough to stay outside deterministic
  evaluation and early enough to be a complete atomic-intent field.
- Replay returns durable identity; pre-commit invisible candidates may change.
- Clients receive ergonomic first-party IDs without teaching the server to trust
  UUID timestamps or request uniqueness.
- Incident correlation has an explicit server source rather than ad hoc fixture
  bytes or reuse of another domain ID.

## Compatibility

The 16 network-order bytes, version and variant bits, 48-bit timestamp placement,
random-bit placement, pure-constructor bound, and canonical lower-case hyphenated
display are stable identifier compatibility boundaries. Durable and public
Protobuf fields continue to carry exactly 16 bytes and must validate with the
one foundational constructor; this ADR adds no field or wire tag.

Existing stored IDs remain valid without migration. Replacing a system provider
while retaining the exact constructor and source contract does not reinterpret
them. Changing the byte construction, accepting an alternate version or variant,
adding semantic meaning to embedded time, changing the provenance allocation or
replay point, or replacing a durable database identity requires an accepted
compatibility decision and updated golden, crash, and recovery fixtures.

The direct dependency version and feature set are security/dependency
compatibility boundaries. A change requires the review specified above even
when Cargo would classify it as semver-compatible.

## Security

UUIDs are nonsecret identifiers, not credentials, signatures, authorization
proofs, uniqueness proofs, or trusted clocks. Their coarse timestamp may be
visible wherever the ID is visible. No policy decision depends on it.

Externally supplied request, session, and normal capability IDs remain untrusted
until structurally validated and, where applicable, authorized. Capability token
entropy never shares a buffer or derivation with capability-ID entropy. Raw token
material retains ADR-0009 redaction and zeroization rules.

Provider injection prevents deterministic crates from acquiring ambient OS
authority and permits exact failure testing. No identifier, source error, random
buffer, or clock debug value is logged as free-form diagnostic data. Durable-key
collisions fail closed rather than overwriting unrelated records.

## Testing

WP-010 freezes, for all six UUIDv7 newtypes:

- the primary, zero, and maximum construction goldens above;
- rejection of `0x1_0000_0000_0000` milliseconds;
- property tests for version, variant, timestamp-byte preservation, canonical
  display, byte round trip, ordering, and newtype separation;
- byte-decoder rejection of invalid version and variant; and
- architecture tests proving no clock, entropy, `uuid`, `rand`, or `getrandom`
  dependency enters `riffdb-types`, runtime, or invariant crates.

Provider tests use injected clocks and entropy sources to prove one clock sample,
one exact ten-byte fill, operation order, pre-epoch/maximum boundaries, entropy
failure, no retry, and no global monotonic state. Dependency tests freeze the
three direct owners, exact version/features, transitive graph, license, build
script, and unsafe inventory.

Package evidence additionally proves:

- WP-060 memory initialization exposes the source-free identity probe, invokes no
  provider, and requires the correct typed `DatabaseId` and `ProvenanceId` in
  initialization/intent constructors;
- WP-070 atomically initializes once, preserves identity across reopen and
  backup/restore, rejects partial metadata, and aborts provenance collisions
  without sequence or partial state;
- WP-100 source-call counts cover new commit, declared rejection, changed-state
  retry, proven abort, unknown status, committed replay, read-only execution, and
  `ExecutionFailed`;
- WP-110 bootstrap tests use disjoint UUID/token fills and zeroize on every
  later failure;
- WP-130 client/server tests cover local failures, fresh retry request IDs,
  caller-selected capability IDs, session convenience generation, and strict
  gRPC validation without substitution;
- WP-140 proves MCP protocol IDs are unrelated, hosted and stdio calls generate
  fresh outer IDs, and source failure never reaches the service;
- WP-150 proves bootstrap retries retain capability identity/token while changing
  the outer request ID only after the credential file and directory are durable;
- WP-180 integrates the consumer-owned incident source with explicit fakes and
  proves safe exposure, label redaction, source-failure handling, and no ambient
  entropy; WP-130 proves the server provider implementation and WP-185 proves
  production composition;
- WP-185 architecture tests compose the server sources without giving runtime,
  storage, service, policy, or adapters direct OS providers; and
- WP-190 process tests cover database initialization crashes, provenance commit
  uncertainty and replay, collision abort, and retained bootstrap recovery.

## ADR Amendments

This accepted record makes these exact companion interpretations authoritative:

1. **ADR-0011:** UUIDv7 newtypes own the pure constructor and exact bytes above;
   system sources remain outside `riffdb-types`.
2. **ADR-0004:** storage probes identity without a source, initialization consumes
   a checked `DatabaseId`, and `CommitIntent` requires a coordinator-supplied
   `ProvenanceId`; storage creates neither.
3. **ADR-0005:** every retry uses a fresh outer `RequestId`, but committed replay
   returns the original stored `ProvenanceId`; neither ID joins idempotency identity.
4. **ADR-0007:** service receives checked IDs only; MCP owns a consumer-side
   request source port, and incident generation is a server-injected error
   boundary rather than transport or service policy.
5. **ADR-0009:** bootstrap ID and token generation are disjoint, normal
   `CapabilityId` stays caller-selected, and the approved `getrandom` direct
   owners are exactly auth, client, and server.
6. **ADR-0012:** identifier generation is orchestration/transport metadata outside
   runtime. The accepted `RequestId` and optional `AgentSessionId` values remain
   in immutable context; their providers never do.

ADR-0006 remains the sole owner of public and durable Protobuf encoding. This
record changes no message, field number, service, RPC, error tag, or stored
envelope.

## Requirements and Work Packages

- **Requirements:** `SYS-003`, `ID-001`, `ID-004`, `ID-005`, `STO-002`,
  `STO-012`, `TXN-011`, `LOG-001`, `OUT-001`, `REC-001`, `REC-002`, `SEC-001`
- **Defines or blocks:** focused `WP-010` completion; `WP-060`; `WP-065`;
  `WP-070`; `WP-080`; `WP-100`; `WP-110`; `WP-120`; `WP-127`; `WP-130`;
  `WP-140`; `WP-150`; `WP-180`; `WP-185`; and `WP-190`
- **Final evidence:** `WP-190`, `WP-200`

Exact package ownership is:

| Work package | Ownership or required evidence |
|---|---|
| WP-010 | Pure constructor/error/goldens for every UUIDv7 newtype; `IncidentIdSource` interface in `riffdb-errors`; no production provider or new dependency |
| WP-040 | No generation work and no dependency; canonical business UUID values remain unchanged |
| WP-060 | Source-free existing/needs-initialization probe, checked candidate transition, and required intent provenance ID; no clock or entropy port |
| WP-065 | Existing 16-byte durable database/provenance fields and malformed/golden fixtures only; no source semantics |
| WP-070 | Atomic initialization, persistence, reopen/recovery, backup/restore, and collision behavior |
| WP-080 | Architecture proof that runtime receives values only and no source/provider |
| WP-100 | Consumer-owned `DatabaseInitializationExecutor` plus `ProvenanceIdSource`, timing, failure, replay, and unknown-status orchestration |
| WP-110 | Auth-owned bootstrap ID/token production with separate entropy and cleanup |
| WP-120 | Checked `RequestContext`/claims and source-free service behavior; no generator |
| WP-127 | Existing public 16-byte field validation only; no generation policy or source |
| WP-130 | Client system source/convenience APIs; one private checked server system-source primitive plus request/incident wrappers; exact dependency graph and gRPC non-substitution tests |
| WP-140 | MCP consumer-side request source port and fresh-ID protocol evidence |
| WP-150 | Retained bootstrap identity/token and fresh retry request behavior |
| WP-180 | Incident-source integration and redaction/failure evidence without owning a system provider |
| WP-185 | Database/provenance wrappers over the WP-130 server primitive and final production injection of database, provenance, MCP request, incident, and cursor sources |
| WP-190 | Process crash, uncertain-status, replay, and initialization recovery matrix |
| WP-200 | Final public-path, dependency, recovery, and compatibility evidence |

No package may edit a neighboring owner to hide an unavailable source. If an
implementation needs another direct dependency owner, an ID in a different
durable state, a different source timing point, or a fallback behavior, it stops
for human review.

## Accepted Decisions

Acceptance of this exact record decides:

1. one pure RFC 9562 UUIDv7 constructor shared by every UUIDv7 newtype;
2. system-clock plus exactly ten source bytes, without monotonic global state;
3. exactly auth/client/server as direct `getrandom` production owners;
4. caller-supplied atomic database initialization and permanent stored identity;
5. coordinator-owned late provenance generation with durable replay and
   fail-closed collision behavior;
6. fresh transport request identity distinct from idempotency and MCP protocol
   framing;
7. caller-selected normal capability identity and disjoint retained bootstrap
   ID/token generation;
8. externally supplied, policy-validated agent-session identity;
9. server-injected incident identity outside deterministic runtime; and
10. no semantic use of UUID timestamps or random fields.

## Decision Deadline

The pure constructor and source interfaces must merge through the focused
WP-010 completion before packages publish competing generation helpers. WP-060
must freeze database initialization and intent provenance ownership before
WP-070 or WP-100. WP-110 must implement bootstrap generation before WP-150;
WP-130 must freeze the client/server provider graph before WP-140; and WP-185
must compose the exact production sources before integrated WP-190 evidence.
