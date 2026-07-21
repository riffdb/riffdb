# ADR-0009: Opaque Server-Side POC Capabilities

- **Status:** Accepted
- **Direction approved:** 2026-07-12
- **Exact text accepted:** Yes; dependency graph amended 2026-07-20
- **Accepted:** 2026-07-13
- **Requires:** ADR-0004 and ADR-0007 accepted before or in the same governance
  change
- **Specification amendments:** SPEC Sections 5.2, 10.2, 10.6, 11, 13.1 through
  13.3, and 13.5;
  SPEC Appendix A; ADR-0005's operational-secret
  custody boundary, ADR-0007's service-result, request-context, and durable-audit
  ownership, ADR-0011's keyed-domain registry, and work-package interface
  ownership
- **Amends:** ADR-0005 operational-secret custody, ADR-0007 service ownership and
  durable-audit lifecycle, and ADR-0011 keyed hash-domain registry
- **Decision deadline:** Before WP-060 capability types or persistence ports merge

The human maintainer accepted this exact opaque-token record on 2026-07-13 as
part of the atomic semantic-interface governance batch. The specification and
work-package metadata are reconciled in that same change.

The maintainer also accepted the audit, bootstrap-file durability, and recovery
clarifications recorded below on 2026-07-13. They are part of this exact record
and supersede the narrower earlier wording identified in their sections.

The maintainer accepted the P1 readiness/composition amendment on 2026-07-13;
the exact clock, digest-inventory, and WP-130/WP-185 ownership changes below are
also part of this accepted record.

On 2026-07-20 the maintainer accepted the narrow Tonic 0.14.6 feature-unification
exception below. It changes no token format, semantic owner, or first-party
safe-Rust rule.

## Context

The POC needs revocable, inspectable authorization without treating client
claims as provenance. Token generation, lookup hashing, storage, scope,
audience, rotation, expiry, revocation, and administrative audit are
cryptographic, durable, and security boundaries.

The specification currently shows two incompatible storage shapes. Section
10.2 places a complete capability record under an opaque token hash, while
Appendix A shows a stable capability record keyed by `CapabilityId` plus a
separate token-digest lookup. A capability must be revocable by its stable ID
without an unbounded scan, so one exact shape is required before WP-060 exposes
the persistence port.

Accepted ADR-0011 also requires a separately accepted keyed domain for every
capability lookup digest. Accepted ADR-0013 defines the executable requirement
as `InvokeCommand(lineage, CommandId)`, so a bare `CommandId` allowlist is not a
complete authorization identity. Finally, WP-060 and WP-070 precede WP-110:
storage-facing record types cannot be owned only by the later authentication or
policy crates without creating a dependency cycle.

## Decision

### Ownership and trust boundaries

`riffdb-types` owns foundational authorization values used on both sides of the
storage boundary: `ActorKind`, `Audience`, `ApprovalId`,
`CapabilityTokenDigest`, the capability keyed-hash domain, and their hard
limits. `riffdb-storage-api` owns
the bounded `StoredCapabilityRecord`, token-lookup record, capability create and
revoke persistence operations, bootstrap marker, and administration-audit
record. These are semantic storage records, not policy decisions.

`riffdb-auth` owns raw-token generation and parsing, typed operational-secret
custody, digest-key configuration, credential resolution,
expiry/revocation authentication checks, and the privately constructible
`AuthenticatedPrincipal`, `NewlyIssuedCapabilityToken`, and checked bounded
`BootstrapDigestCandidates`. Its key-custody implementation serves two disjoint
typed namespaces: capability-token lookup and the ADR-0005 idempotency digest.
It never gives either consumer raw operational key bytes. `riffdb-idempotency`
continues to own command identity, canonical idempotency framing, and recovery;
it consumes only the idempotency-specific keyed-digest capability.

`riffdb-policy` owns the operation request, deny-by-default grant evaluation,
`Decision`, derived obligations, the privately constructible value-only
`AuthorizedCapabilityMutationPreparation`, bounded value-only
`TransactionCurrentCapabilityFacts`, the synchronous `AuthorizationClock`, and
the pure `TransactionCurrentCapabilityVerifier`. Policy has no storage-API
dependency.
`riffdb-service` owns API-neutral capability create/revoke/bootstrap request and
result types and maps typed control-plane executor results to those public-safe
semantic results.
`riffdb-commit` owns those executor request/results, translates the lower
storage transition without exposing it to the service, and is the only
component that assigns an `AdministrationSequence` or drives an authoritative
capability create, revoke, or bootstrap transaction. It also owns ADR-0007's
synchronous `AdministrationClock` used for service audit, catalog transitions,
and the bootstrap exception defined below. `riffdb-proto` remains the sole owner
of the durable
Protobuf payload and later public RPC fields under ADR-0006.

`riffdb-auth` separately owns the narrow synchronous credential-resolution clock
port used only for initial authentication. `riffdb-server` owns the concrete
operating-system providers that implement the separate auth-owned authentication,
policy-owned authorization, and commit-owned admission and administration clock
interfaces. WP-130 composes all four for the runnable P1 graph. None enters
deterministic command execution.

Transport adapters may temporarily hold a raw bearer credential only to call
`riffdb-auth`. They receive an `AuthenticatedPrincipal`, not a capability record
constructor. The command runtime, compiler, MCP catalog code, storage engines,
commit intent, provenance, metrics, and audit records never receive raw token or
digest-key material. MCP and gRPC invoke the same authentication, policy, and
application-service boundaries; MCP never reads capability storage directly.

One narrowly reviewed dependency exception supports first-operator bootstrap:
`riffdb-cli` may use an explicitly isolated `riffdb-auth::bootstrap_secret`
module for offline token generation, canonical token validation, protected-file
loading, and zeroizing secret value types. That module exposes no authenticator,
policy, storage, capability-record, credential-repository, or server-only key
provider API. All bootstrap operations still travel over the public loopback
gRPC API and the shared application service. Architecture tests enforce both the
module allowlist and the absence of any other CLI-to-auth import. No other
first-party transport or client gains an auth-crate dependency through this
exception.

### Raw token format

A normal POC capability token is 32 bytes generated by `riffdb-auth` with the
operating-system cryptographic random source outside deterministic command
execution. Its public text is the RFC 4648 URL-safe base64 encoding using the
alphabet `A-Z`, `a-z`, `0-9`, `-`, and `_`, without `=` padding. The canonical
text is therefore exactly 43 ASCII bytes.

Decoding rejects a token unless all of these are true:

1. The input is exactly 43 ASCII bytes.
2. Every byte is in the URL-safe alphabet; padding, whitespace, standard-base64
   `+` or `/`, and surrounding bearer text are not part of the token value.
3. Decoding consumes the complete input and produces exactly 32 bytes.
4. Re-encoding the decoded bytes produces byte-for-byte identical text.

Transport code removes and validates the case-sensitive `Bearer `
authentication scheme before constructing the raw-token secret wrapper. A
future token text format must have a disjoint, accepted representation; a v1
decoder never guesses another alphabet, padding rule, prefix, or length.

### Capability digest and hash domain

ADR-0011's keyed registry is extended with exactly one entry:

| Typed digest | Domain | Mode | Owner |
|---|---|---|---|
| `CapabilityTokenDigest` | `riffdb.capability-token/v1` | HMAC-SHA-256 | one capability-token lookup |

The HMAC input uses the unchanged ADR-0011 keyed frame:

```text
ASCII "RIFFDB-HMAC"
+ byte 0x00
+ scheme byte 0x01
+ domain length as u16 big endian
+ ASCII "riffdb.capability-token/v1"
+ payload length as u64 big endian, exactly 32
+ the decoded 32 raw token bytes
```

The encoded base64url text is not the HMAC payload. Environment, database,
audience, and policy fields are authenticated by the stored record checks and
are not appended to the digest payload.

`CapabilityTokenDigest` contains digest scheme byte `0x01`, a nonzero
`DigestKeyId` encoded as `u32` big endian, and exactly 32 HMAC bytes. A
`DigestKeyId` is an operator/provider promise that identifies immutable key
material across process lifetimes and database restores; there is no second
ambiguous key-version field. Changing the bytes under an existing key ID is
forbidden.
Changing the algorithm, frame, payload, or domain requires a new digest scheme
and storage-key version.

The typed digest is not a bearer credential, but its `Debug` and `Display`
representations still redact the digest bytes. No implicit conversion to an
idempotency digest, raw `[u8; 32]`, or unkeyed content hash is provided.

### Digest-key provider and rotation

The POC key provider supplies one current write key followed by zero or more
readable previous keys. The list contains at most eight entries, the current key
is first, every `DigestKeyId` is nonzero and unique, and every key contains
exactly 32 secret bytes. Configuration order is newest to oldest and is not
derived from numeric key IDs. One loaded document with duplicate IDs, a missing
current key, more than eight entries, or identical material assigned to two IDs
is rejected before readiness.

The POC does not persist a key-material commitment and therefore cannot prove at
restart that an operator kept the bytes behind a previously used key ID
unchanged. Key-ID immutability across restarts and restores is an external
operator/provider invariant, not a readiness claim. Violating it causes old
tokens to stop resolving or, in the negligible collision case, an integrity
failure; it is never treated as a valid rotation. A future persisted key
fingerprint registry requires a separate durable-format decision.

Capability-token and idempotency-key digest providers are distinct typed
configuration namespaces behind one auth-owned operational-secret custody
boundary. They use different configuration keys or files and different typed
provider handles even when their numeric `DigestKeyId` values happen to be
equal. `riffdb-server` loads both through that custody boundary and requires the
provider to cross-check all readable material; reusing the same secret bytes
across the namespaces rejects readiness. WP-130 owns that production loading and
cross-check and derives separate bounded, value-only readable scheme/key-ID
inventories for structural startup validation. Domain separation remains
mandatory and is not a substitute for independent operational key material, rotation,
retirement, or backup handling. `riffdb-idempotency` supplies its already
canonical ADR-0005 frame to its typed handle and receives only its typed digest;
raw operational keys never enter that crate.

Creation uses only the current key. Authentication decodes the token once,
computes a candidate digest for every configured readable key, performs every
corresponding bounded token-index lookup, and then evaluates the complete match
set. Zero matches produces the generic unauthenticated result. Exactly one
cross-linked match may proceed. Multiple matches are a storage-integrity defect,
produce only a redacted incident publicly, and prevent that request from being
authorized.

Authentication never rewrites a digest or capability record. Rotation consists
of adding a new current key, retaining old keys as readable, issuing replacement
capabilities, and revoking or allowing old capabilities to expire. A key may be
removed only when no active, unexpired record references it. Startup refuses
readiness when an active, unexpired capability uses an unsupported digest scheme
or a key ID absent from the readable configuration. Missing keys referenced
only by expired or revoked records do not prevent startup because those records
remain readable for audit without authenticating a token.

Digest keys are supplied through an injected provider backed in the POC by an
environment secret or a protected local secret file. The protected-file loader
is Linux-only. Each typed namespace must configure exactly one of its environment
document or file path; missing or simultaneous sources reject readiness. It
obtains the process effective user ID by reading at most
65,537 bytes from `/proc/self/status`, requiring EOF at or before 65,536 bytes,
and requiring exactly one line beginning `Uid:`. After the colon that line must
contain exactly four canonical unsigned decimal `u32` tokens separated by one
or more ASCII spaces or tabs, with no sign and no leading zero except the value
`0`; its second token is the effective user ID. An unavailable, over-limit, or
malformed proc record rejects the load.

The loader then obtains final-component metadata with `symlink_metadata` and
requires a regular file. It opens for reading with the Linux flags
`O_NOFOLLOW = 0o00400000` and `O_NONBLOCK = 0o00004000` through safe
`std::os::unix::fs::OpenOptionsExt::custom_flags`. `O_NOFOLLOW` makes a final
symlink fail at open; `O_NONBLOCK` prevents a raced FIFO/device replacement from
blocking before handle validation. The opened handle must be a regular file
owned by the effective user ID with `mode & 0o077 == 0`, and its device/inode
pair must equal the pre-open metadata. Only then does the loader read the
document limit plus one byte, require EOF within the limit, and parse the exact
document. A pre-open or open-time final symlink, device/inode mismatch,
nonregular handle, ownership or mode mismatch, unsupported platform, or read
error fails closed.

This exact Linux-only standard-library rule gives atomic no-follow behavior for
the final component and a bounded open for special files without a native Rust
dependency or first-party unsafe code. It deliberately permits symlinks in
intermediate path components and treats matching device/inode metadata as the
POC object-identity check; it makes no stronger filesystem/adversary claim.
Digest keys are not accepted through command-line arguments, stored in RiffDB
tables or backups,
included in crash reports, or exposed through configuration diagnostics.
Restoring a database with active capabilities also requires restoring its
readable key configuration through the separate protected channel.

The capability digest-key document is exact ASCII and at most 1,024 bytes:

```text
riffdb-capability-digest-keys-v1<LF>
<key-id>:<64-lowercase-hex-bytes><LF>
...
```

`<LF>` denotes the single byte `0x0a`; it is not literal angle-bracket text. The
header is followed by one through eight entry lines; the first
entry is the current write key and later entries are readable previous keys in
newest-to-oldest configuration order. `key-id` is canonical unsigned decimal in
`1..=4294967295` with no leading zero. The key is exactly 64 lowercase hexadecimal
characters decoding to 32 bytes. The document ends after the final line feed.
Carriage returns, blank lines, whitespace, comments, a byte-order mark, uppercase
hexadecimal, duplicate IDs, duplicate key bytes, trailing data, and an over-limit
document reject. The environment-secret form carries this same complete
document, not a second grammar.

The idempotency digest-key document uses the identical bounds and entry grammar,
but its exact first line is
`riffdb-idempotency-digest-keys-v1<LF>`. The two documents have separately named
configuration inputs and cannot be substituted for one another. Their parsed
provider sets are cross-checked for reused key bytes before readiness. This
exactly supplies ADR-0005's previously unspecified POC idempotency-key custody;
changing either document grammar or ownership requires a new review.

### Audience and durable server identity

`Audience` is an exact, nonempty visible-ASCII identity of at most 512 bytes
selected
from server configuration. It is normally the configured absolute gRPC endpoint
URI or the configured MCP protected-resource URI. The configured byte string is
the identity: RiffDB performs no case folding, percent-decoding, default-port
rewriting, path cleanup, or other URI normalization during authentication.
The value may contain bytes `0x21..=0x7e` only. Configuration must reject
duplicates; a listener additionally rejects a value that its own endpoint or
protected-resource parser does not accept before exposing it as an `Audience`.

The configured audience catalog contains at most 32 entries and at most 16,384
ASCII bytes in total before configuration-envelope overhead. It is sorted and
duplicate-free after exact validation. A process with no configured audience is
not ready to serve authenticated gRPC or MCP traffic.

Every capability contains one through eight audiences, sorted and duplicate
free by exact ASCII bytes. A create request may select only values already in
the server's configured audience catalog; it cannot introduce an arbitrary
audience. A trusted listener injects exactly one configured audience into the
authentication context. HTTP never derives it from `Host`, forwarded headers,
the request target, or any other caller-controlled field. The POC MCP audience
is the configured canonical resource URI ending in `/mcp`; production stdio
uses the configured gRPC audience.

The capability binds the durable `DatabaseId`, not an ephemeral process,
listener, or node identifier. Restart preserves the binding. A deliberate
restore that preserves the database identity preserves the binding only when
the protected digest keys are also supplied. A record whose database ID differs
from storage metadata is corruption; a token presented under a different
configured environment or audience is simply unauthenticated publicly.

### Durable storage records and keys

The POC uses two cross-linked capability records plus one singleton bootstrap
marker. The redb table name supplies the namespace; capability-table keys still
begin with an explicit key-format byte.

This stable-ID record plus digest-index layout explicitly replaces SPEC Section
10.2's illustrative single `capabilities` row keyed by opaque-token hash. On
acceptance, that table becomes the `capabilities` and `capability_tokens` rows
defined below. SPEC Section 13.2 and Appendix A must likewise describe the
versioned grant, lifecycle, cross-link, and bootstrap records here rather than
their earlier illustrative single-record sketch. This is a deliberate
specification amendment, not permission for storage to choose either layout.

```text
capabilities key:
  0x01
  + CapabilityId as 16 UUID network-order bytes

capability_tokens key:
  0x01
  + digest scheme byte 0x01
  + DigestKeyId as u32 big endian
  + 32 digest bytes

metadata bootstrap-marker key:
  ASCII "capability_bootstrap/v1"
```

The first key is exactly 17 bytes and the second is exactly 38 bytes. Unknown
key-format or digest-scheme bytes fail closed. The token lookup value contains
only its `CapabilityId`. The complete capability record repeats the typed digest
reference. Integrity checking requires a one-to-one relationship: both sides
exist, refer to each other, and no digest or capability ID has a second mapping.
Authentication returns no partially validated record when the relationship is
missing or inconsistent. Capability and token-lookup values are separate
ADR-0006 `StoredEnvelope` values carrying `CapabilityRecordV1` and
`CapabilityTokenLookupV1`; the lookup value is not an unversioned raw UUID.

The bootstrap-marker value is an ADR-0006 `StoredEnvelope` carrying
`riffdb.storage.v1.CapabilityBootstrapMarkerV1` with exactly `DatabaseId`, the
bootstrap `CapabilityId`, and its authoritative capability-transition
`AdministrationSequence`, in that
order. The marker remains after revocation. It must cross-check against the
database metadata, target capability record, and bootstrap administration-audit
entry. The immediately preceding principal-less service-audit `started` record
must link that transition sequence. The target record's creation sequence and
`issued_at` must equal the capability-administration entry's sequence and
timestamp. For every revoked record, its revoking
sequence and `revoked_at` must equal the corresponding revoke audit entry.

Expired and revoked records and their lookup rows remain durable during the POC
for audit and integrity checking. Capability garbage collection and lookup-key
migration are deferred. Neither table contains raw token text, raw token bytes,
digest-key bytes, client provenance claims, or arbitrary policy text.

`AdministrationSequence` is nonzero and one-based. The metadata allocator starts
as `Next(1)`, advances through nonzero values with ADR-0004's exact
`Next(nonzero) | Exhausted` semantics, and never wraps. Every committed
administration record uses the current value and atomically advances it. An ordinary catalog,
capability, or standalone service-audit transition allocates one. New bootstrap
allocates two consecutive values in one transaction: its principal-less service-
audit `started` record first and its authoritative capability-administration
record second. Exact bootstrap replay allocates one new value for its invocation
`started` record but no new capability-transition sequence. A proven-aborted,
denied, conflicting, not-found, malformed, or mismatched operation writes no
capability transition and consumes no capability-transition sequence. Any
required authenticated invocation `started`, `denied`, `failed`, or other
terminal service-audit record remains separately sequenced under ADR-0007;
malformed/unauthenticated traffic and invalid principal-less bootstrap attempts
remain bounded telemetry and consume none. Consequently committed audit entries
are contiguous across the shared administration sequence space; startup fails
closed on a gap, duplicate, zero value, overflow, or mismatch with the metadata
counter.

### Authenticated invocation audit clarification

ADR-0007 owns the exact durable-audit lifecycle. After successful
`RequestContext` construction, a command becomes intrinsically audit-required
only after its checked plan is classified as a mutation; a closed control-plane
mutation or administrative read/stream is intrinsically required once its closed
operation is classified, with bounded target resolution inside that scope. An
allowed standard read joins that scope only when policy returns the durable-audit
obligation. Every explicit policy
denial after context construction is audited regardless of the allow-path scope:
standalone `denied` before `started`, terminal `denied` after it. An unknown or
missing Execute target before plan classification remains bounded telemetry, as
does malformed or unauthenticated traffic before context construction.

An allowed audited operation performs initial current authorization, durable
`started`, bounded cancellation-aware permit acquisition, a fresh exact-facts
authorization, and immediate synchronous admission with no intervening `.await`
or fallible audit write. A post-start denial, failure, or cancellation submits no
protected work and receives that invocation's one terminal phase. Every started
invocation selects exactly one terminal phase; phase `0x04` means cancellation
without a known authoritative result or released protected output. Stream audit
ends at establishment, while later per-item authorization remains mandatory and
cannot add another terminal phase. A complete policy-filtered and bounded result
must exist before `succeeded` and is released only after that append is durable.

Service-audit targets remain independent of the exact closed result link:
`None`, `Command { commit_sequence, provenance_id }`, or
`ControlPlane { administration_sequence }`. A command link is all-or-nothing.
Neither `RequestContext` nor `AuthenticatedPrincipal.authentication_time` is an
audit timestamp source or fallback. Every required append obtains its timestamp
from the commit-owned `AdministrationClock`, except that a normal capability
create/revoke administration record uses the same transaction-current
`AuthorizationClock` value used by its verifier and transition.

Every append is attempted through ADR-0007's `AdministrationAuditExecutor` and
uses ADR-0007's exact seven-case failure matrix. In particular, a required
standalone/start outage returns `StorageUnavailable` without protected
admission; denial remains `AuthorizationDenied`; a terminal outage after a known
command commit or durable `ExecutionFailed` returns `OutcomeUnknown`; a
pre-commit failure/cancellation or read establishment outage returns
`StorageUnavailable`; and a control-plane outage returns that operation's typed
unavailable or uncertain recovery result. No original safe result bypasses that
mapping. An administration-clock failure is the same fail-closed audit outage:
it records an incident, makes readiness unhealthy, performs no recursive audit,
and releases no permission, protected data, or success.

ADR-0007's earlier alternative permitting unvalidated provenance claims in an
untrusted audit field is superseded for the POC. Structurally invalid claims
reject at their validation boundary; other unapproved claims are discarded
after policy evaluation. They are never copied into durable service audit,
command provenance, or administration records.

### Startup and recovery integrity

After the source-free identity probe and any required commit-owned initialization
transition, WP-130 samples and validates exactly one canonical startup value
through the policy-owned `AuthorizationClock`. It also loads and cross-checks the
two typed digest providers, then derives an independently typed bounded readable
scheme/key-ID inventory for capability tokens and for idempotency identities.
Those value-only inputs contain no secret bytes or provider handles.

WP-130 passes the checked time and inventories into ADR-0004's exclusive
`StructuralEvidenceSession`. Before that session may yield dormant ports, the
redb structural pass over authoritative metadata and records must establish all
of the following:

1. Empty application and administration sequence spaces each have `next = 1`.
   Otherwise the next application sequence is exactly the checked successor of
   the last contiguous commit key and embedded sequence, and the next
   administration sequence is exactly the checked successor of the last
   contiguous audit key and embedded sequence. If either last value is the
   maximum representable nonzero sequence, its metadata is instead the explicit
   exhausted semantic state and no numeric successor is present.
2. Application commits and administration records independently start at one,
   contain no zero, gap, duplicate, key/payload mismatch, or value beyond the
   checked sequence domain, and agree with their respective metadata counters.
3. Every capability and token-lookup row has the one-to-one reciprocal link
   defined above. Every bootstrap marker, bootstrap `started` record,
   capability-administration record, capability creation/revocation field, and
   timestamp/sequence cross-link exists and agrees exactly. No transition may
   point to the wrong capability or record kind.
4. Every active, unexpired capability uses the supported v1 digest scheme and a
   key ID present in the readable capability-key configuration. Expired or
   revoked records remain structurally and cross-link valid even when their old
   key is no longer configured.
5. Independently, every ADR-0005 `Pending`, `StoredOutcome`, and
   `ExecutionFailed` idempotency identity uses a supported scheme and a key ID
   present in the readable idempotency-key configuration. The POC has no expiry
   or key migration for these records, so any reference prevents retirement.

The pass may scan in bounded chunks but must consume every page and exact end
marker, and it performs no authoritative write. It samples no clock, opens no
digest provider, and receives no secret material. A failure is an opaque
integrity/readiness failure: recovery never repairs a
counter from a table, fills a sequence gap, synthesizes an audit or cross-link,
deletes an offending row, or silently downgrades an unsupported live key. Any
future repair or migration requires a separately reviewed offline procedure and
durable-format decision.

A matching `Exhausted` allocator is canonical and is not reported as corrupt.
It still leaves authoritative readiness false because that sequence space cannot
accept another operation. An exhausted-state mismatch is corruption. Neither
case is repaired during startup.

These checks are storage-structural evidence only. While the same session keeps
mutation frozen, `riffdb-catalog` separately IR-validates all historical bundle
bytes and the active relation and returns ADR-0004's opaque
`ValidatedCatalogHistory`. Only WP-130 may combine that matching catalog value
with `StructurallyOpened` dormant ports and activate the authoritative P1 graph.
A clock/provider/scan/catalog mismatch or failure drops the open attempt. WP-185
reuses the resulting graph for P2 and cannot substitute another provider set or
readiness pass.

The v1 semantic capability record contains, in this order:

1. `CapabilityId`.
2. `capability_revision: u64`, starting at 1 and incremented exactly once by
   the active-to-revoked transition.
3. The `CapabilityTokenDigest` scheme, key ID, and digest bytes.
4. `DatabaseId` and `Environment`.
5. Stable `ActorId` principal and closed `ActorKind`.
6. Canonically sorted audiences.
7. `issued_at` and `expires_at` canonical timestamps.
8. The administration sequence that created it.
9. The create-operation `RequestId`, stored as `creation_request_id`.
10. `CapabilityGrantV1`.
11. Lifecycle state: active, or revoked with `revoked_at`, the revoking
    administration sequence, and one closed `RevocationReasonCode`.

`ActorKind` has exact v1 values human `0x01`, agent `0x02`, and service
`0x03`; zero and unknown values reject. Lifecycle state has exact v1 tags active
`0x01` and revoked `0x02`. Active rejects revocation fields; revoked requires all
revocation fields. `RevocationReasonCode` has requested `0x01`, replaced `0x02`,
suspected compromise `0x03`, and policy change `0x04`; it carries no
caller-authored string. Zero and unknown lifecycle or reason tags reject.

The complete durable payload uses a dedicated
`riffdb.storage.v1.CapabilityRecordV1` Protobuf message under ADR-0006's
`StoredEnvelope`. Its generated descriptor, field numbers, canonical set order,
schema hash, golden bytes, and the dedicated
`riffdb.storage.v1.CapabilityTokenLookupV1` cross-link record are reviewed in a
proto-owner interface PR after this semantic record is accepted and before
WP-070 stores a production record. The lookup payload contains exactly one
`CapabilityId`. No other crate creates a competing serialization.

Capability lifecycle entries in the shared administration-audit table use a key
of byte `0x01` followed by `AdministrationSequence` as `u64` big endian. Their
value is a dedicated
`riffdb.storage.v1.CapabilityAdministrationAuditV1` envelope containing, in
order:

1. Administration sequence.
2. Create/revoke/bootstrap `RequestId`.
3. Operation tag: bootstrap `0x01`, create capability `0x02`, or revoke
   capability `0x03`.
4. Coordinator-observed timestamp.
5. Optional initiator containing stable principal, `ActorKind`, authorizing
   `CapabilityId`, and authorizing capability revision, absent only for
   bootstrap.
6. Target `CapabilityId` and resulting capability revision.
7. Optional validated `ApprovalId`.
8. Optional `RevocationReasonCode`, present only for revoke.

For normal create/revoke, field 4 is the exact transaction-current
`AuthorizationClock` value also used by the verifier and as `issued_at` or
`revoked_at`. For bootstrap it is the compound transition's one
`AdministrationClock` sample. Timestamps are canonical but need not be
monotonic; the nonzero `AdministrationSequence` alone orders transitions.

`ApprovalId` is exact, nonempty visible ASCII of at most 256 bytes and is not
normalized. Only a privately constructed validated value enters an audit or
provenance record. The capability administration audit contains no raw token,
token digest, digest-key ID, free-form reason, complete grant, or client
provenance claim. The target capability record is the authoritative grant
detail. Capability lifecycle transitions and their audit record commit
atomically. Other administration operations may use separately registered
payload messages in the same sequence space. They do not reuse the capability
operation tags.

The storage-facing transition results are closed and owned by
`riffdb-storage-api`:

- Create: `Created { capability_id, revision, administration_sequence }`,
  `AlreadyCreated { capability_id, revision }`, `CapabilityIdConflict`, or
  internal-only `TokenDigestCollision`.
- Revoke: `Revoked { capability_id, revision, administration_sequence }`,
  `AlreadyRevoked { capability_id, revision, administration_sequence }`, or
  `CapabilityNotFound`.
- Bootstrap: `BootstrapCreated { capability_id, revision,
  administration_sequence, invocation_started_sequence }`, `BootstrapReplayed {
  capability_id, revision, administration_sequence,
  invocation_started_sequence }`, or `BootstrapConflict`. In both success
  variants `administration_sequence` is the original authoritative bootstrap
  transition; `invocation_started_sequence` identifies this invocation's
  principal-less started record.

`riffdb-service`, not `riffdb-auth`, maps normal `AlreadyCreated` to the typed
API-neutral `AlreadyCreatedTokenUnavailable` result because storage never
possesses the returned raw token. `Created` is combined by the service with the
still-owned generated token only after the coordinator proves durable success;
no lower layer constructs a public response. `riffdb-commit` first translates
the storage variants into its typed control-plane executor result;
`CapabilityIdConflict` and the revoke/bootstrap variants are mapped from that
result by the same service boundary. The service never receives or switches on a
storage transition type.
`AlreadyCreatedTokenUnavailable` is ordinary closed capability-administration
result data, not a new `PublicErrorKind`; malformed requests, authorization
denial, storage unavailability, and internal integrity defects continue through
the accepted public-error registry. All non-created variants perform no write
to capability state and allocate no capability-transition sequence; exact
bootstrap replay still appends the required invocation-started service-audit
record. Unknown durable tags still reject; these Rust transition variants are
not themselves durable encodings or public wire tags.

The service-owned normal-create result is closed: `Created` carries capability
ID, revision, administration sequence, and the one auth-owned newly issued token;
`AlreadyCreatedTokenUnavailable` carries capability ID and revision; and
`CapabilityIdConflict` carries no existing-record detail. Bootstrap has distinct
`BootstrapCreated` and `BootstrapReplayed` data carrying only capability ID,
revision, and administration sequence, plus detail-free `BootstrapConflict`; it
never echoes the caller-retained token. Revoke likewise maps the three storage
states above without exposing a capability record. WP-127 assigns their public
wire tags and fields before WP-130 implements total conversions.

### Grant and permission model

`CapabilityGrantV1` contains exactly:

- One `TenantScope`.
- One `PartitionScopeV1`.
- A canonical sorted set of `PermissionV1` atoms.
- A canonical sorted set of entity-field visibility entries.
- `max_scan_rows` in `1..=500`.
- A canonical sorted set of permission tags that require a validated approval.

Tenant and partition are separate scopes. `PartitionScopeV1` has exact v1 tags
all partitions `0x01` and explicit partitions `0x02`. The explicit form contains
a sorted, duplicate-free set of `ScopedPartitionV1` entries; the all form
contains no entries. One entry is the exact tuple of contract lineage and one
complete validated `PartitionKey`, encoded as lineage length `u32` big endian,
exact lineage bytes, partition-key length `u32` big endian, and exact key bytes.
Entries sort by those complete encoded bytes. Contract lineage is required
because ADR-0016 partition keys contain lineage-local stable aggregate IDs.
Zero, unknown tags, an empty explicit set, entries on the all form, or a key
whose aggregate is absent from that exact lineage's validated bundle reject. It
never authorizes with `PartitionKeyHash`, because ADR-0016 defines that hash as
an observability identity rather than an authorization proof. An explicit
partition set has at most 1,024 entries. A command's compiler-produced lineage
and partition are derived before policy evaluation and must be inside this
scope. For delegation, explicit set A is a subset of explicit set B exactly when
every complete scoped entry in A occurs in B; every explicit set is a subset of
all partitions, while all partitions is a subset only of itself.

A tenant-scoped capability may authorize an operation only when the exact
compiled plan or service schema provides a statically validated tenant mapping.
Grammar v1 defines no tenant annotation. Therefore a grammar-v1 command without
such separately reviewed metadata is authorized only under global tenant scope;
policy never guesses that an organization, entity-key component, or caller
claim is a tenant. This is a fail-closed POC limitation, not permission to ignore
tenant scope.

The immutable v1 permission registry is:

| Tag | Permission | Stable parameter |
|---:|---|---|
| `0x01` | Validate contract source | none |
| `0x02` | Read active or historical contract metadata | none |
| `0x03` | Explain a command | contract lineage plus `CommandId` |
| `0x04` | Deploy a contract | none |
| `0x05` | Invoke a command and resolve that principal's outcome | contract lineage plus `CommandId` |
| `0x06` | Read one entity | contract lineage plus `EntityTypeId` |
| `0x07` | Scan one index | contract lineage plus `IndexId` |
| `0x08` | Query one projection | contract lineage plus `ProjectionId` |
| `0x09` | Read one projection's status | contract lineage plus `ProjectionId` |
| `0x0a` | Read one commit | none |
| `0x0b` | Scan commits | none |
| `0x0c` | Subscribe to commits | none |
| `0x0d` | Read provenance | none |
| `0x0e` | Inspect outbox status | none |
| `0x0f` | Read server health | none |
| `0x10` | Read server statistics | none |
| `0x11` | Create a capability | none |
| `0x12` | Revoke a capability | none |
| `0x13` | Administer capabilities in this database and environment | none |

Zero, unknown tags, missing parameters, extra parameters, duplicate atoms, bare
stable IDs without lineage where listed, and noncanonical ordering reject.
An unparameterized atom encodes only its tag. A parameterized atom encodes its
tag, contract-lineage byte length as `u32` big endian, the exact lineage bytes,
and its stable ID as `u32` big endian. Permission atoms sort by those complete
encoded bytes. Entity-field entries use the same lineage and entity-ID encoding,
then a strictly increasing list of `FieldId` values encoded as `u32` big endian;
entries sort by lineage then entity ID. Empty lineage, duplicate values, and
noncanonical order reject.
Command invocation authorizes the command's complete declared input and outcome
schema. The POC does not generate a different command outcome schema per
principal. If policy cannot disclose the complete declared outcome, invocation
is denied rather than returning a schema-invalid partial outcome.

Projection permission `0x08` likewise authorizes the complete declared
projection result schema, subject to tenant, partition, prefix, and row-limit
constraints. Grammar v1 does not define per-field projection grants. If the
complete group and measure row cannot be disclosed, the projection query is
denied rather than returning a partial row that violates its generated schema.

An entity-field visibility entry is the exact tuple of contract lineage,
`EntityTypeId`, and a sorted set of `FieldId` values. Entity-key identity is
authorized by the matching entity-read or index-scan permission and partition
scope; non-key fields require explicit visibility. Absence of an entry permits
no non-key fields. A newly added field is denied until explicitly granted.
Commit, provenance, health, statistics, projection-status, and outbox APIs expose
only their separately defined public-redacted DTOs; no capability grants access
to internal record bytes or hidden error sources.

Permission and field sets are grants and constraints, not serialized runtime
obligations. Policy derives a closed, canonical obligation set for each request:
effective tenant scope, exact partition constraint, field mask, row limit,
validated approval identity when used, audit class, and output classification.
There is at most one obligation of each kind and they occur in that order. The
v1 audit classes are standard read `0x01`, command mutation `0x02`,
administrative read `0x03`, and control-plane mutation `0x04`. The v1 output
classifications are public metadata `0x01`, policy-filtered application data
`0x02`, and administrative redacted data `0x03`. Zero or unknown obligation,
audit, or output-classification tags fail closed. Redaction and field filtering
are complete before an adapter serializes, logs, measures, or renders a value.

`Decision::Deny` contains a closed `PolicyCode`, not a free-form safe-reason
string. The v1 codes are missing permission `0x01`, tenant scope mismatch
`0x02`, partition scope mismatch `0x03`, field visibility denied `0x04`,
approval required `0x05`, delegation exceeds authority `0x06`, and inactive or
stale capability `0x07`. Each maps to the same existing generic public
authorization-denied error and has static internal safe text. Internal
evaluation context stays in trusted tracing under an incident ID.
Client-supplied approval references are untrusted. A required approval is
satisfied only by a privately constructible `ValidatedApproval` containing a
validated `ApprovalId` from the configured API-neutral approval verifier. The
approval-required set contains only tags `0x01..=0x13` from the permission table
and has at most 19 entries. The POC default provider validates none, so any
operation requiring approval fails closed unless a reviewed local provider is
configured. Any later provider must return a privately constructible proof bound
to the stable principal, authorizing capability ID and revision, exact operation
and target, canonical request fingerprint, approval ID, and checked validity
window. The service rechecks that binding at the operation's authorization safe
point; an approval for a different request, target, revision, or expired window
cannot be replayed. Production approval integration is deferred.

### Creation, delegation, and revocation

The public create request supplies a UUIDv7 `CapabilityId`, a UUIDv7 `RequestId`,
the target principal and actor kind, a requested duration, configured audience
selections, and the requested grant. Supplying the target capability ID lets an
operator revoke a committed capability after an uncertain create response
without recovering its token. IDs, request metadata, audience selections, and
grant fields are untrusted until validated.

`CapabilityId` plus the normalized requested record is the capability-create
operation identity. `RequestId` identifies one transport invocation for tracing
and audit as required by ADR-0005; it is not an idempotency component. Every
retry must supply a fresh outer `RequestId`, although RiffDB does not enforce
global request-ID uniqueness.

A creator needs permission `0x11` or `0x13`. Permission `0x11` is ordinary
subset delegation: it may delegate only a subset of its current effective
authority:

- Database and environment are identical.
- Audiences are a subset.
- Global tenant scope may delegate global or one tenant; tenant scope may
  delegate only the same tenant.
- Partition scope is equal or narrower.
- Permission atoms and field visibility are subsets.
- `max_scan_rows` does not increase.
- Expiry does not exceed the creator's expiry or the process hard maximum.
- Every approval-required permission tag inherited from the creator remains
  required in the child; a child may add requirements but never remove one.

Permission `0x11` itself follows the same subset rule. Permission `0x13` is
explicit root capability-management authority for the capability's exact
database and environment. It may create a capability with any recognized v1
grant, configured audience, tenant scope, or partition scope in that database
and environment, including another `0x13` capability, subject to all hard bounds
and validated approval policy. Its child's expiry is bounded by the process hard
maximum but not by the creator's expiry so an administrator can rotate before
expiration. Permission `0x13` does not by itself authorize contract deployment,
command execution, data reads, or any other operation; those require their own
permission atom. Neither creation permission can grant an unknown or future
permission, and there is no hidden delegation authority.

Normal creation validates and normalizes the request, generates the token and
digest outside the coordinator, and submits a value-only create operation. The
coordinator obtains the transaction-current `AuthorizationClock` value, passes
the exact proposed interval through the verifier, and reuses that value as
`issued_at` and the administration-audit timestamp. It atomically writes the
capability record, token lookup, and one administration-audit record, assigning
one administration sequence only when that complete record set can commit. The
raw token is returned only after durable success. A pre-commit failure drops the
token and exposes no successful creation.

If the durable create commits but its response is lost, the token is
intentionally unrecoverable. Repeating creation with the same `CapabilityId`
and normalized requested record, using the required fresh retry `RequestId`, does
not create another token or administration sequence and returns the typed result
`AlreadyCreatedTokenUnavailable` with only the capability ID and existing
revision. The normalized requested record comprises database, environment,
principal, actor kind, requested duration, audiences, and grant; it excludes
token material, digest, issued and expiry timestamps, revision, and assigned
administration sequence. Reuse of a `CapabilityId` with a different normalized
record returns the generic safe `CapabilityIdConflict` result and does not
reveal the existing record. The caller revokes the known ID
and creates a replacement with a new ID. RiffDB never stores an encrypted
recovery copy, derives a replacement token from request data, or returns the
original token twice.

Revocation targets a `CapabilityId`, not a token digest. Permission `0x12` may
revoke only a capability whose database, environment, tenant, partition,
permission, field, and approval authority is within the caller's delegation
scope. Permission `0x13` may revoke any capability in its database and
environment. Revocation is the only v1 lifecycle transition, is irreversible,
increments the capability revision, retains the lookup row, and atomically
appends one administration-audit record.
Repeating revocation of an already revoked capability returns its existing
state and creates no new administration sequence.

### One-time bootstrap

Bootstrap is a narrow exception to normal server-side token generation so a
lost first response cannot strand an otherwise empty database. It is available
only through the bootstrap mode of `AdminService.CreateCapability` on the
configured loopback gRPC listener while server bootstrap is explicitly enabled;
it is never exposed through MCP. `CreateCapabilityRequest` carries a closed
create mode whose public wire values are unspecified `0`, normal `1`, and
bootstrap `2`; zero and unknown values reject. No request message contains raw
token bytes.

The first-party CLI's offline bootstrap-credential command generates the
canonical token through the isolated auth helper, generates a UUIDv7
`CapabilityId`, and writes this exact ASCII document:

```text
riffdb-bootstrap-credential-v1<LF>
capability-id:<canonical-lowercase-hyphenated-UUIDv7><LF>
token:<43-canonical-token-bytes><LF>
```

`<LF>` is byte `0x0a`. The document is exactly 132 bytes and ends after the
third line feed. It rejects carriage returns, a byte-order mark, missing or
additional lines, whitespace around values, a non-v7 or noncanonical UUID,
noncanonical token text, trailing bytes, and an over-limit read. The CLI may
read the same complete document from standard input, reading at most 133 bytes,
requiring EOF at exactly 132 bytes, or from a protected local credential file
checked by this ADR's loader.
Neither the token nor the complete document is accepted as an argv value.

For generated credentials, the CLI requires an explicit output path, creates a
new file without overwriting an existing path, requests mode `0o600`, writes the
complete document, calls `sync_all` on the file, and closes it. After close, it
opens the containing directory, verifies that the opened handle is a directory,
calls `sync_all` on that directory, closes the directory handle, and successfully
rereads the credential through the protected-file loader before attempting
gRPC. The containing directory for a leaf path with no explicit parent is the
current directory used to resolve that output path. A file or directory sync
failure, an unsupported directory-sync operation, a non-directory parent
handle, close failure where reported, or protected reread failure aborts before
any bootstrap request. The file remains for explicit operator inspection or
removal. The CLI may display the nonsecret `CapabilityId`, but never echoes the
token. This file is intentionally retained across an uncertain response and is
not a RiffDB database artifact.

On gRPC, bootstrap supplies exactly one binary metadata entry named
`riffdb-bootstrap-token-bin`. Its decoded metadata value is exactly the 43
canonical ASCII token bytes from the document; the adapter marks it sensitive
where supported and moves it immediately into the auth-owned secret wrapper.
Bootstrap rejects a missing, repeated, malformed, or over-limit entry and also
rejects ordinary `authorization` metadata. Normal create requires ordinary
authentication and rejects any `riffdb-bootstrap-token-bin` entry. The request's
`CapabilityId` must equal the document's retained ID before transmission. The
trusted loopback gRPC adapter strips the bootstrap metadata and calls the
auth-owned bootstrap preparation entry point. That entry point validates once,
computes the bounded typed digest candidate for every readable capability key,
marks the current-key candidate, and drops the raw secret. The adapter then
invokes the same API-neutral bootstrap service operation with only that checked
typed candidate set. New bootstrap persists the current-key digest; replay must
match exactly one readable candidate to the existing digest. No service DTO,
coordinator operation, durable record, response, or error contains the raw
token. The server does not claim to measure caller entropy. No other create path
accepts caller-selected token bytes.

The bootstrap target principal must have `ActorKind::Human`. The caller supplies
the stable target `ActorId`, but there is no authenticated initiating principal
before the first capability exists; the audit initiator is therefore absent and
the bootstrap operation's trusted loopback ingress plus target human identity are
recorded. Agent or service bootstrap targets reject. Subsequent delegation may
create agent or service capabilities through ordinary authorized administration.

Bootstrap performs no standalone audit append before its typed transition;
doing so would itself violate the administration-record emptiness condition.
For a structurally valid candidate that becomes a successful new bootstrap, the
coordinator checks every emptiness condition in SPEC Section 13.3 against
transaction-current state. In that same serializable transaction it allocates
two consecutive administration sequences and atomically writes, in order, a
principal-less `ServiceAuditPhase::Started` record for this request and the
authoritative capability-bootstrap administration record, together with the
bootstrap marker, capability record, and token lookup. The coordinator obtains
one canonical `AdministrationClock` value for the compound transition, and both
audit records use that exact value. The started record links the authoritative
transition sequence; the marker and capability creation fields link that same
second sequence. This is one typed bootstrap transition, not generic audit
authority to mutate capability state.

The bootstrap grant must contain permission `0x13`; it may also contain explicit
requested v1 permissions and scopes. There is no marker-backed exemption and no
serializable wildcard permission atom. The marker contains the bootstrap
`CapabilityId` and administration sequence, but no request ID or raw token.

If the response is lost, the CLI still has the token and capability ID.
Repeating bootstrap with that capability ID, the same normalized requested
record, and a token that resolves to the same digest returns the existing
capability without another capability-transition sequence; the retry uses a
fresh `RequestId`. The typed bootstrap transition appends a new principal-less
started service-audit record linked to the original transition using one new
`AdministrationClock` sample, and the service appends a separate succeeded
record with that same link and its own new clock sample before releasing the
replay result. A successful new invocation follows the same post-commit
succeeded-append rule. A crash may therefore leave an orphan started record for
an invocation whose result was not released; recovery receives its own started
and succeeded records and never rewrites the earlier one. A
different capability ID, token, or requested record after the marker exists
fails closed without an administration sequence or durable audit record.
Malformed, structurally invalid, failed-emptiness, and mismatched principal-less
attempts remain bounded transport-security telemetry for the same reason. Once
bootstrap succeeds, no second bootstrap operation can succeed; explicit `0x13`
administration is the replacement and rotation mechanism.

If commit status for the compound transition is unknown, the coordinator fences
authoritative writes and readiness fails closed. The service emits no inferred
terminal audit, releases no success, and returns only the safe uncertain control-
plane classification. The operator retries with the retained credential after
recovery. If the authoritative transition committed but the separate succeeded
append fails, success is likewise withheld and the same exact replay path
recovers it; the committed capability is never rolled back or reported as a
proven failure.

Failure to obtain or validate the compound transition's administration-clock
value is an audit outage: no bootstrap work is submitted, no sequence or record
is written, readiness is unhealthy, and the safe result is `StorageUnavailable`.
Failure of the separate terminal clock follows ADR-0007's committed control-plane
terminal-outage mapping. Neither case falls back to the bootstrap credential,
request context, authentication time, or a caller timestamp.

### Time, authentication, and reauthorization

Requested capability duration is at least one second and at most 2,592,000
seconds (30 days). The recommended CLI default is 28,800 seconds (8 hours).
For normal create, `issued_at` is the exact fresh transaction-current
`AuthorizationClock` value accepted by the verifier. Bootstrap uses the compound
transition's `AdministrationClock` value. It is never a client value, an
authentication timestamp, or command `tx.time`. `expires_at` is checked addition
of the accepted duration. A capability is temporally eligible exactly when:

```text
issued_at <= authorization_time < expires_at
```

Authorization time comes from the synchronous policy-owned
`AuthorizationClock` outside deterministic command runtime. Initial credential
resolution instead uses auth's narrow authentication-clock interface. The
commit-owned `AdministrationClock` supplies service-audit, catalog, and bootstrap
transition timestamps, and the separate commit-owned `AdmissionClock` supplies
the accepted command-admission value. WP-130 production composition provides all
four wall-clock interfaces; neither caller nor a transport adapter supplies a
timestamp. Clock values must be canonical but need not be monotonic. A time
before `issued_at`,
equality with or passage beyond `expires_at`, arithmetic failure, or an invalid
timestamp fails closed; sequence values, not timestamps, establish durable order.

Failure to obtain or validate an authentication- or authorization-clock value is
an internal service defect and creates a redacted incident. Before
`RequestContext` construction it remains bounded transport-security telemetry
and creates no durable audit. At an initial current-policy safe point for a known
audit-required operation, it appends standalone `failed` using a fresh
`AdministrationClock` value. At a post-start safe point, it appends that
invocation's terminal `failed` and performs no protected admission or capability
transition. If those required audit clock/append operations fail, ADR-0007's
audit-outage mapping supersedes the original `InternalDefect`; no authentication
time or request timestamp is reused. An `AdministrationClock` failure itself
cannot be audited recursively and follows the same outage mapping directly.

Credential resolution validates the token, resolves its unique cross-linked
record, and checks digest scheme/key, database, environment, trusted audience,
time, and active lifecycle before constructing `AuthenticatedPrincipal`.
`AuthenticatedPrincipal` contains capability ID and revision, stable principal,
actor kind, audience, tenant scope, and the authentication time; it contains no
raw token or key material and cannot be constructed by a transport adapter.
Its authentication time is evidence of the initial credential check only. It is
never reused as the current time for expiry at a later authorization safe point.

The POC does not cache positive capability records. The application service
reloads by capability ID and reauthorizes with current policy after request
schema validation and immediately before command admission or another
authoritative operation. MCP discovery and invocation are separate checks. A
bounded wait is rechecked after waking and before returning data; a paginated or
streaming operation rechecks before each externally visible page or item.
Transport-specific adapters cannot omit these service-owned checks.

Every one of those checks obtains a fresh value from the injected authorization
clock, then evaluates `issued_at <= authorization_time < expires_at` against the
reloaded record. A positive decision, capability record, or clock value is not
cached across safe points. This applies even when two checks occur in one unary
request and even when the principal's original authentication time is still
inside its validity interval.

Capability create and revoke have no durable pending admission. Initial service
authorization returns a privately constructible value-only
`AuthorizedCapabilityMutationPreparation`; it contains the exact authorizing
identity/revision, audience, requested mutation, delegation facts, and validated
approval binding but no policy engine, storage handle, clock, or obligation.
`TransactionCurrentCapabilityFacts` contains exactly capability ID and revision,
lifecycle, database and environment, principal and actor kind, the canonical
bounded audiences, issued and expiry timestamps, and the complete checked grant.
It contains no token digest, lookup-key material, storage key, transition
request, sequence allocator, or write handle.
After any queue wait and inside the short authoritative transaction, the commit
coordinator obtains one fresh authorization time from `AuthorizationClock`,
reloads the authorizing capability record in its transaction-current state,
mechanically copies every verifier-relevant checked field into policy-owned
`TransactionCurrentCapabilityFacts`, and passes the facts, time, preparation, and
the exact proposed target interval to the policy-owned pure
`TransactionCurrentCapabilityVerifier`. For create, that interval has
`issued_at` equal to this exact clock value and `expires_at` equal to checked
addition of the requested duration. The verifier checks the interval and all
duration bounds. For revoke, the same exact clock value becomes `revoked_at`.
The matching capability-administration record uses that same value; commit does
not sample `AdministrationClock` for either normal transition. The lowering is a
total field copy and performs no allow/deny, lifecycle, expiry, audience,
permission, delegation, approval, or target-interval predicate. The verifier
rechecks lifecycle, revision, expiry, audience, exact create/revoke permission,
delegation predicate, validated approval binding, and the complete proposed
interval and returns only a typed allow or deny. It performs no storage/time I/O
itself, writes, sequence assignment, transport,
obligation/redaction work, or general operation dispatch. Commit owns the
transaction and transition and must not duplicate those policy predicates. The
service's fresh authorization immediately followed by synchronous executor
acceptance remains ADR-0007's non-retroactive invocation-admission boundary. The
transaction-current verifier is the final authorization check for the capability
transition, and the serialized commit makes that allowed transition durable. A
revocation, expiry, or policy-relevant record change visible before the verifier
check denies the transition and assigns no administration sequence.

Revocation committed before a safe point denies that continuation. Revocation
is not retroactive after command admission and does not roll back or cancel a
coordinator transaction already begun. A later request must authenticate and
authorize again. Another valid capability for the same stable principal and
tenant may resolve that principal's idempotent command outcome because ADR-0005
intentionally excludes `CapabilityId` from idempotency identity. Outcome
resolution still requires a fresh full permission `0x05` decision for the exact
contract lineage and command, the same stable principal and authorization-
resolved tenant scope as the stored identity, and all current disclosure
obligations; possession of an idempotency key or capability ID alone reveals
nothing.

### Public failures, secret handling, and constant-time claims

Malformed or missing credentials, no digest match, unsupported credential
format, expired or revoked records, and database, environment, or audience
mismatch all produce the same bounded unauthenticated transport result. They do
not reveal which check failed or whether a capability exists. A successfully
authenticated principal that lacks an operation grant receives the existing
generic authorization-denied `PublicError`. Storage cross-link corruption,
multiple digest matches, unsupported live key configuration, and impossible
lifecycle state produce a redacted internal incident rather than an
authentication oracle.

Core-owned raw token text and decoded bytes live in a non-`Clone`,
nonserializable secret wrapper with redacted `Debug` and `Display`. Owned core
token and digest-key buffers are zeroized on drop by the reviewed zeroization
provider. This guarantee does not pretend that generated Protobuf/Tonic values
are non-`Clone` or zeroize their allocations: an authorization metadata value,
the dedicated bootstrap metadata value, and a newly created token in the normal
successful public response may be copied transiently by bounded transport
serialization. Those copies are the unavoidable credential-delivery boundary,
are released promptly, and never enter a core semantic or durable type.

Adapters never place any such value in tracing fields, errors, panic messages,
metrics, audit, provenance, URLs, command arguments, or generated MCP content.
Normal create is the only server response that carries a newly generated token;
replay and bootstrap responses never echo one. Compatibility fixtures use
explicit nonsecret test vectors rather than captured production credentials.

"Never persisted by RiffDB" means no credential-bearing raw token is written to
the database, backup, audit, provenance, logs, metrics, or crash artifacts.
Checked-in compatibility fixtures may contain only conspicuously labeled,
fixed noncredential token vectors; they never contain output captured from an
entropy provider or a live create/bootstrap flow.
It does not forbid an operator from keeping a token in an environment secret or
protected local credential file as explicitly allowed by MCP-047. RiffDB never
writes that operator credential file as a side effect of database persistence.
For this ADR, a protected local credential file must pass the exact Linux
pre-open/opened-inode, effective-user ownership, mode, and bounded-read checks
defined above before RiffDB reads it. A normal capability credential file is
exactly the 43 canonical token bytes with no line feed. A bootstrap credential
file uses the exact three-line document defined above.

No raw token equality comparison is used for authentication. HMAC construction
uses the reviewed provider. Any direct in-memory verification of an expected HMAC
uses that provider's constant-time verification API. The redb/B-tree lookup of a
pseudorandom keyed digest is an indexed lookup and is not claimed to be
constant-time; requiring a constant-time storage comparator would contradict the
bounded lookup design without improving resistance to guessing a 256-bit random
token. All readable-key candidates are nevertheless computed and looked up
before selecting the match set, so configuration order is not exposed by an
early successful return.

### Bounds

The v1 hard bounds are:

| Boundary | Limit |
|---|---:|
| Raw token bytes | exactly 32 |
| Canonical token text | exactly 43 ASCII bytes |
| Bootstrap credential document | exactly 132 bytes |
| Readable digest keys | 8 |
| Digest-key document | 1,024 bytes |
| Audience bytes | 512 |
| Configured audiences | 32 entries / 16,384 total bytes |
| Audiences per capability | 8 |
| Approval ID bytes | 256 |
| Permission atoms | 8,192 |
| Explicit partition keys | 1,024 |
| Entity-field visibility entries and total listed fields | 65,535 each |
| Approval-required permission tags | 19 |
| Maximum scan rows | 500 |
| Capability lifetime | 2,592,000 seconds |
| Encoded capability payload | 1 MiB |

All counts, strings, key lengths, nested records, and encoded sizes are checked
with checked arithmetic before allocation. Repeated values use canonical order
and reject duplicates. The 1 MiB semantic payload bound is below ADR-0006's 16
MiB absolute envelope bound.

### Reviewed and proposed dependencies

Capability HMAC uses the already reviewed `sha2` 0.11.0 and `hmac` 0.13.0
configuration from ADR-0011 through the central `riffdb-types` keyed-hash
implementation. This ADR adds no alternative cryptographic algorithm or provider.

ADR-0018 subsequently amended only the direct `getrandom` owner/purpose set of
this reviewed production graph. The exact version, features, transitive graph,
licenses, build-script behavior, and external unsafe inventory remain unchanged:

| Crate | Exact version and features | Direct first-party owner | Reviewed purpose |
|---|---|---|---|
| `getrandom` | `=0.3.4`, `default-features = false`, no optional features | `riffdb-auth`, `riffdb-client-rust`, and `riffdb-server` only | capability-token/bootstrap-ID entropy, client UUIDv7 sources, and server database/provenance/hosted-MCP-request/incident/cursor UUIDv7 sources outside deterministic runtime |
| `base64` | `=0.22.1`, `default-features = false`, `features = ["alloc"]` | `riffdb-auth` only | strict canonical URL-safe token text |
| `zeroize` | `=1.8.1`, `default-features = false`, `features = ["alloc"]` | `riffdb-auth` only | owned core secret-buffer cleanup |

The accepted WP-130 Tonic graph is an explicit narrow exception to the table's
`base64` feature statement, not to its direct-owner statement. Every Tonic edge
uses exact version `=0.14.6` with default features disabled:

| First-party owner/edge | Exact features and companion |
|---|---|
| `riffdb-api-grpc` production | `tonic` feature `codegen`; exact `tonic-prost` |
| `riffdb-api-grpc` build/dev generation | exact `tonic-prost-build` feature `transport` |
| `riffdb-client-rust` production | `tonic` features `channel`, `codegen`; exact `tonic-prost` |
| `riffdb-server` production | `tonic` features `router`, `server` |

This graph may feature-unify the one locked `base64 = 0.22.1` instance with
`std`. `riffdb-auth` remains the sole first-party crate with a direct `base64`
dependency and the sole owner of token encoding/decoding semantics. API, client,
server, and generated transport code MUST NOT call `base64` directly. The graph
admits no second base64 version, TLS, compression, transport-wide production
feature, new cryptography, or first-party unsafe code. Any such change, any
version change, or any broader feature requires renewed human dependency review.

All three are licensed `MIT OR Apache-2.0`. In the auth-owned review slice under
the direct features in the first table, `base64` and `zeroize` have no build
script, native code, or transitive dependency. `base64` forbids unsafe code.
`zeroize` contains localized reviewed unsafe volatile-write
code implementing its cleanup guarantee. `getrandom` contains target-gated
unsafe operating-system/libc calls and has a build script used only for
sanitizer and old-Windows configuration detection; version `0.3.4` is already in
the root lockfile through the test-only `proptest` graph.

The isolated locked review manifest at `/tmp/riffdb-cap-dep-review` produced the
normal all-target graph `getrandom -> cfg-if 1.0.4, libc 0.2.186, r-efi 5.3.0,
wasip2 1.0.4+wasi-0.2.12 -> wit-bindgen 0.57.1`, with `base64` and `zeroize` as
leaves. The Linux production target uses the `cfg-if` and `libc` branches; the
other entries are target-specific lock coverage. This exact command passed
against the repository policy during review:

```bash
cargo deny --manifest-path /tmp/riffdb-cap-dep-review/Cargo.toml --locked check --config /home/user/dev/riffdb/deny.toml advisories licenses bans sources
```

WP-110 must reproduce the auth-owned feature slice and equivalent locked
`cargo deny` result using the checked-out repository's `deny.toml`; ADR-0018
assigns WP-130 the extended three-owner graph and root-workspace check. The
temporary review path is evidence, not a repository input.

This review grants no unsafe-code exception to first-party crates: every
first-party crate remains `#![forbid(unsafe_code)]`. It also grants no native
first-party code and no `rand`, JWT/token framework, password KDF, signature
library, or additional constant-time dependency. Production entropy and secret
custody remain behind thin injected providers, so another reviewed provider can
replace these crates without changing semantic interfaces.

Any version, feature, direct owner, transitive graph, build-script behavior,
native-code inventory, license, or unsafe inventory change requires a new human
dependency review before merge. This text approves no compatible-range
substitution or undisclosed lockfile change.

Test entropy is injected through private test-only sources; production
composition uses only the three approved direct owners and their narrow injected
consumer ports.

### Explicit deferrals

The following are outside this decision and fail closed rather than being
partially implemented:

- OAuth, remote TLS resource-server behavior, refresh tokens, token exchange,
  self-contained claims, and cross-node key distribution.
- HSM/KMS integration, online digest migration, automatic rekey-on-read, and
  capability garbage collection.
- Token recovery, encrypted token escrow, introspection, and list APIs.
- Contract-authored policy, tenant annotations, and partial per-principal
  command outcome schemas.
- A production approval authority. The POC default validates no approval.
- ADR-0008's exact MCP command naming, resource-URI, and presentation fixtures.
  ADR-0007 in the same acceptance batch owns transport-neutral durable service
  audit, including this record's bootstrap exception; no audit semantics remain
  deferred from WP-120.
- Cancellation of a command already admitted to the commit coordinator.

## Options Considered

1. **Opaque random token, domain-separated HMAC, stable record plus lookup:**
   Selected. It gives bounded authentication, stable revocation identity, and no
   client claims.
2. **One complete record keyed only by token digest:** Rejected. Revocation by
   capability ID would require another index or an unbounded scan and conflicts
   with the stable-ID storage shape in SPEC Appendix A.
3. **Plain cryptographic hash:** Rejected. It gives a leaked store a verifier for
   weak or accidentally malformed tokens and does not use the accepted keyed
   framing.
4. **Self-contained signed token:** Rejected. It adds claim/version/key
   distribution surface and weakens immediate server-side revocation.
5. **Persist or deterministically regenerate the raw token:** Rejected. Either
   exposes a bearer secret at rest or makes request metadata a token oracle.
6. **Put key ID in the token text:** Rejected for v1. Trying the bounded readable
   key set keeps the public token opaque and permits rotation without another
   token syntax.
7. **Authorize with partition hashes:** Rejected. Hashes are not authorization
   proofs under ADR-0016.
8. **Treat a client approval string as approved:** Rejected. It contradicts the
   trust boundary and fails open when no approval authority exists.
9. **Server-generated bootstrap token with no recovery protocol:** Rejected. A
   crash after atomic creation but before the first response could permanently
   strand an otherwise empty database.

## Consequences

- A focused foundational interface change must add the audience, actor-kind,
  approval ID, typed capability digest, and hash domain before WP-060. Raw-token
  and secret-buffer types remain auth-owned and arrive in WP-110.
- WP-060 owns complete neutral persistence records and ports without depending on
  the later auth or policy crates.
- WP-065 adds the exact durable records after semantic acceptance and before
  WP-070 persistence; WP-127 later completes the public create/revoke schema.
- `CapabilityId` is supplied in create requests and remains the bounded
  administrative handle even when the one-time token response is uncertain.
- The CLI has one architecture-tested dependency on the auth-owned offline
  bootstrap-secret module; all administration still uses public loopback gRPC.
- Capabilities are immediately revocable but require one storage record reload
  and one fresh authorization-clock sample at every defined safe point. Normal
  create/revoke reuses the transaction-current sample for the verified transition
  timestamp; bootstrap and required audit use the separate commit-owned
  administration clock.
- Backup/restore documentation must state that digest keys are separate protected
  material and that active capabilities are unusable without them.
- The same governance commit that accepts this record must reconcile the SPEC
  Section 10.2 table, Section 13.2 record sketch, Appendix A, ADR-0011 keyed-domain
  registry, ADR index, and work-package metadata.

## Compatibility

The raw-token byte count and text alphabet, HMAC frame/domain/payload, digest
scheme and key ID representation, table and key layouts, cross-link invariant,
durable record fields, actor/revocation tags, permission tags and parameter
identity, canonical set ordering, audience identity, partition and field scope,
time interval, lifecycle transition, bootstrap credential document, gRPC
metadata name, create mode, bootstrap replay, digest-key document, and
uncertain-create result, four-clock production ownership,
canonical/nonmonotonic timestamp semantics, exact clock reuse/sample rules,
absence of an authentication-time
audit fallback, audit scope/phase/result-link lifecycle and failure mappings,
sequence-counter validation, and no-repair recovery policy are durable, public,
or security compatibility boundaries. Typed readable-inventory inputs, the one
startup authorization-clock value, exact-end structural evidence, and
WP-130-only matching activation are security compatibility boundaries.

Changing one of those requires an accepted versioned decision, new Protobuf
schema/type or key version as applicable, golden fixtures, and a restartable
migration or explicit refusal policy. Adding a readable digest key without
changing old records is compatible. Adding a new permission requires a new
recognized record/schema version; an old reader never ignores an unknown grant.

## Security

Authentication and authorization are deny-by-default. Tenant, database,
environment, audience, principal, actor kind, partition scope, field visibility,
approval, and lifecycle state come from validated server records or trusted
configuration, never caller provenance. Discovery does not grant access.

Secret data is removed before logs, metrics, public errors, MCP text, audit, or
provenance. Capability and digest keys never enter command evaluation. Bounded
candidate lookup prevents rotation configuration from becoming a denial-of-
service multiplier. Stable public failures do not disclose existence or failure
reason. Capability management cannot bypass coordinator ordering, durable audit,
or delegation checks.

The POC remains local-development safe, not an internet authorization system.
Loopback MCP HTTP, protected credential handling, explicit audiences, and short
expiry reduce exposure but do not replace the deferred OAuth/TLS security review.

## Testing

- Entropy-source injection proves exactly 32 bytes are requested and production
  composition cannot use the deterministic source.
- Token fixtures cover every accepted alphabet boundary and reject padding,
  whitespace, alternate alphabets, truncation, extension, and noncanonical text.
- Deterministic HMAC fixtures freeze the exact frame/domain/payload, key ID, and
  cross-domain inequality with the idempotency-key domain.
- Memory and redb conformance tests cover both key layouts, cross-link equality,
  duplicate/missing links, unknown versions, canonical record order, payload
  bounds, and retained expired/revoked rows.
- Rotation tests cover current-key writes, every readable previous key, missing
  live keys at startup/reopen, key retirement, zero/one/multiple match behavior,
  and no rekey on authentication.
- Authentication matrices cover token format, database, environment, audience,
  exact expiry boundaries, clock rollback before issuance, clock-provider
  failure mapping, and revocation. Multi-safe-point tests advance the injected
  clock after initial authentication and prove the stored authentication time is
  never reused.
- Policy matrices cover every permission tag, lineage separation, global and
  explicit partition scopes, fail-closed tenant mapping, fields, row limits,
  approvals, complete command outcomes, and delegation subset rules.
- Transaction-current verifier matrices change lifecycle, revision, expiry,
  audience, permission, delegation, and approval binding after queueing and prove
  typed denial without a sequence. Clock tests prove commit samples fresh time,
  passes and verifies the exact proposed target interval, reuses that one value
  for `issued_at`/`revoked_at` and the administration record, never calls
  `AdministrationClock` for normal create/revoke, accepts canonical clock
  rollback only where interval checks permit it, and duplicates no policy
  predicate.
- Revocation-safe-point tests cover transport authentication, service reload,
  command admission, MCP stale discovery, wait wakeup, pages, streams, and the
  non-retroactive admitted-command boundary.
- Process crash tests cover capability create before/during/after atomic commit,
  lost response followed by a fresh-`RequestId`
  `AlreadyCreatedTokenUnavailable` retry with no retained/reissued normal-create
  token, revoke replay, bootstrap before/during/after marker commit, same-token
  bootstrap recovery with a fresh request ID, independent application/
  administration counter mismatches, and refusal without repair.
- Bootstrap fixtures freeze the three-line credential document, stdin EOF rules,
  protected-file mode/owner/inode checks, file-sync/close/directory-sync/reread
  ordering, directory-sync failure and unsupported behavior, create-mode wire
  values, and exact `riffdb-bootstrap-token-bin` carriage. They prove no RPC is
  submitted before successful directory sync and protected reread, reject
  ordinary authorization on bootstrap and bootstrap metadata on normal create,
  and prove neither response nor logs echo the bootstrap token.
- Linux protected-file tests inject bounded `/proc/self/status` input and cover
  malformed/multiple/missing `Uid:` records, pre-open symlinks, inode swaps,
  open-time symlink races, nonblocking special-file rejection, nonregular
  handles, owner/mode mismatch, limit plus one, and successful exact reads. They
  freeze the Linux flag values and final-component no-follow behavior without
  claiming intermediate-component rejection.
- Administration-audit assertions prove the new-bootstrap started and
  authoritative records receive consecutive sequences atomically, their links
  and marker agree, both use one bootstrap administration-clock sample, exact
  replay adds only its own independently timed started/succeeded service records,
  failed principal-less candidates add none, and no raw token or digest enters
  audit. The ordinary invocation matrix proves the exact intrinsic scope, every
  explicit post-context denial, initial/start/permit/fresh-auth/synchronous-
  admission order, one terminal per started invocation, phase-`0x04` meaning,
  complete filtering before success, establishment-only stream audit, independent
  targets and closed result links, the seven audit-outage mappings, no request-
  authentication-time fallback, and no record for an allowed ordinary standard
  read.
- Recovery fixtures corrupt each capability/lookup/bootstrap/audit cross-link,
  each key/payload sequence, and each next-sequence counter independently; every
  case refuses readiness without modifying the database, while a repeated clean
  recovery is read-only and idempotent. Exact-end and typed-inventory tests prove
  WP-070 cannot yield `StructurallyOpened` on truncation, unreadable retained
  idempotency state, or an unreadable active/unexpired capability, and that no
  raw key or provider enters storage.
- Secret canaries traverse Debug/Display, tracing, metrics, public errors, panic
  containment, MCP text, provenance, backup manifests, and crash diagnostics.
- Dependency and architecture checks prevent raw-token types in runtime, commit
  intent, storage payloads, MCP catalog code, or direct transport-storage paths;
  separately, they allow only the CLI's isolated bootstrap-secret module and
  forbid every server authenticator/repository symbol from its dependency graph.
- Lockfile and feature-tree assertions freeze the exact three-crate dependency
  review, direct owners, feature sets, licenses, and absence of unreviewed
  transitive dependencies.
- WP-065 proto generation and golden records prove deterministic durable
  descriptors, envelopes, schema hashes, unknown-version refusal, and clean
  regeneration. WP-127 freezes public request/result/create-mode messages;
  WP-130 proves total service-to-wire conversions, gRPC metadata behavior, four
  disjoint wall-clock adapters, digest-provider/inventory composition, and the
  matching structural/catalog readiness gate.

### Exact specification and manifest reconciliation

Acceptance updates SPEC Section 5.2 to add the isolated CLI bootstrap-secret
dependency described above; name `riffdb-service` as owner of capability-
administration request/result semantics; name `riffdb-policy` as owner of the
value-only mutation preparation and transaction-current facts, synchronous
authorization clock, and pure transaction-current verifier without a storage
dependency; name `riffdb-commit` as owner of the synchronous administration
clock; and permit `riffdb-commit` to depend only on those narrow policy
interfaces while forbidding its use of the general authorizer, obligations/
redaction, or policy-owned reader. It updates Section 10.2,
Section 10.6, Section 13.2, and Appendix A to use the stable-ID record plus digest
lookup, versioned lifecycle/grant records, bootstrap marker, ordered
administration audit, and fail-closed read-only recovery validation defined
here. Sections 13.1 and 13.5 discard rather than durably audit unapproved claims
and use the exact post-`RequestContext` audit lifecycle above. Section 13.3 and
the Section 11 public protocol description gain the exact bootstrap mode,
credential document, metadata carriage, and normal-create result behavior.
ADR-0005 gains the exact typed idempotency-key custody document; ADR-0007 gains
the same service ownership, audit clarification, and narrow CLI exception;
ADR-0011 gains the capability-token domain above.

The manifest delta is exact:

- WP-010 adds ADR-0009 and delivers foundational `ActorKind`, `Audience`,
  `ApprovalId`, `CapabilityTokenDigest`, keyed domain, bounds, redaction, and
  canonicalization fixtures under its existing paths. It does not own raw-token
  buffers or any Protobuf record.
- WP-020 retains the accepted phase-zero public protocol and durable-envelope
  baseline. It is not retroactively made dependent on ADR-0009.
- WP-060 adds ADR-0009 and delivers the bounded storage-owned capability,
  token-lookup, bootstrap-marker, administration-audit, and closed transition
  semantics, including exact standalone audit phases/result links, coordinator-
  owned timestamp inputs, and the read-only integrity report needed by this
  clarification, plus in-memory conformance tests. It defines no Protobuf
  message.
- WP-065 depends on WP-020 and WP-060; requires ADR-0004, ADR-0005, ADR-0006,
  ADR-0007, ADR-0009, ADR-0010, ADR-0011, ADR-0012, ADR-0013, ADR-0014,
  ADR-0016, and ADR-0017; and may edit only `Cargo.lock`, `proto/**`,
  `crates/riffdb-proto/**`, `fixtures/proto/**`, `scripts/generate-proto*`,
  `crates/riffdb-storage-api/Cargo.toml`,
  `crates/riffdb-storage-api/src/lib.rs`, and
  `crates/riffdb-storage-api/src/proto_codec/**`. It owns durable semantic-
  record messages, envelopes, descriptors, schema hashes, wire-structural
  validation, golden bytes, bounds, historical registrations, and checked
  storage DTO mappings, including every durable record defined here. Its
  acceptance commands are `cargo test -p riffdb-proto -p riffdb-storage-api`
  and `./scripts/generate-proto --check` with a clean generated diff.
- WP-070 retains its existing dependencies and additionally depends on WP-065;
  it requires ADR-0009, persists only the WP-065-reviewed records, and refuses
  to yield `StructurallyOpened` on either sequence-counter mismatch or any
  capability, bootstrap, audit cross-link, digest-inventory, or exact-end failure
  without repairing the database. It does not claim complete catalog-aware
  readiness by itself.
- WP-100 retains ADR-0009 and uses the auth-owned typed idempotency digest
  provider without receiving operational key bytes. It owns the authoritative
  capability transition orchestration, compound bootstrap audit transition, and
  complete mechanical checked-record-to-policy-facts lowering plus the post-
  queue transaction-current verifier/authorization-clock call and exact normal-
  transition timestamp reuse. It owns `AdministrationClock`, its sample rules,
  sequence allocation for standalone `failed`/`denied` records and normal
  start/terminal pairs, and audit-outage executor behavior, but no public result
  or duplicated policy predicate.
- WP-110 adds `Cargo.lock` to its allowed paths, requires ADR-0009, and owns the
  exact dependency graph, auth secret/key-custody implementations, isolated
  bootstrap-secret module, authentication, policy-owned authorization clock,
  value-only capability-mutation preparation and transaction-current facts, and
  pure transaction-current verifier described here. Its deliverables stop
  claiming ownership of durable records or public results.
- WP-120 adds ADR-0009 and owns capability create/revoke/bootstrap service DTOs,
  checked principal-less bootstrap context, typed control-plane-executor-result
  mapping, initial/start/permit/fresh-clock/synchronous-admission safe points,
  exact audit scope and failure mappings, standalone and terminal bootstrap audit
  orchestration without an authentication-time fallback, and semantic result and
  redaction tests. It never receives a storage transition.
- WP-127 is the public API schema-completion package. It depends on WP-020 and
  WP-120, requires ADR-0006, ADR-0007, ADR-0009, ADR-0010, ADR-0011,
  ADR-0012, ADR-0013, and ADR-0017, and may edit only `Cargo.lock`, `proto/**`,
  `crates/riffdb-proto/**`, `fixtures/proto/**`, and
  `scripts/generate-proto*`. It owns public messages,
  descriptors, schema hashes, wire-structural validation, and golden fixtures,
  including create mode and the closed capability-administration results. It
  does not depend on `riffdb-service` or implement semantic conversions. Its
  requirements include `API-001`, `VAL-003`, and projection-fixture `POC-006`;
  existing assignments are retained. Its
  acceptance commands are `cargo test -p riffdb-proto` and
  `./scripts/generate-proto --check` with a clean generated diff.
- WP-130 retains every existing dependency and additionally depends on WP-127;
  it adds ADR-0009 and `Cargo.lock` to its allowed paths and owns total
  service-to-Protobuf conversions, exact gRPC bootstrap metadata extraction,
  normal bearer handling, adapter tests, ADR-0018's exact extended direct-owner
  graph, production composition of both typed digest providers, cross-namespace
  key-material checks, readable inventories, the startup authorization-clock
  value, and matching structural/catalog readiness activation. It never
  implements capability policy or persistence.
- WP-140 retains ADR-0009 and proves MCP cannot discover or invoke bootstrap and
  cannot bypass the shared service.
- WP-150 adds ADR-0009 and delivers the offline bootstrap credential generator,
  exact stdin/protected-file flow including required parent-directory sync before
  submission, public-loopback-gRPC invocation, and module-boundary architecture
  tests. Its only auth dependency is
  `riffdb-auth::bootstrap_secret`.
- WP-180, WP-185, WP-190, and WP-200 retain ADR-0009. WP-130 composes both typed
  key namespaces, cross-checks readable support for all three idempotency states
  and every active unexpired capability plus the complete structural/catalog
  result before readiness, and supplies the approved production entropy provider
  and concrete authentication-, authorization-, admission-, and administration-
  clock providers. WP-185 reuses that same provider/core graph for MCP, workers,
  and observability without a second readiness path. WP-190
  supplies process evidence for both sequence spaces and all cross-links without
  repair. `riffdb-server` direct `getrandom` use is limited to ADR-0018's exact
  database, provenance, hosted-MCP request, incident, and cursor source set; the
  corresponding consumer ports retain their accepted semantic owners.
- WP-065 and WP-127 are explicit P1 gate members; their hard downstream edges do
  not replace any existing P1 package or dependency.

## Requirements and Work Packages

- **Requirements:** `ID-005`, `SEC-001` through `SEC-004`, `API-001`,
  `MCP-011`, `MCP-030`, `MCP-041`, `MCP-043`, `MCP-046` through `MCP-048`,
  `STO-002`, `STO-010` through `STO-012`, `STO-020` through `STO-022`,
  `REC-001`, and `REC-002`
- **Defines or blocks:** foundational follow-up in `WP-010`; semantic records in
  `WP-060`; durable schema in `WP-065`; storage in `WP-070`; authorization in
  `WP-110`; service semantics in `WP-120`; public schema completion in
  `WP-127`; adapters and production P1 composition in `WP-130`; CLI flow in
  `WP-150`; required ADR for
  `WP-010`, `WP-060`, `WP-065`, `WP-070`, `WP-100`, `WP-110`, `WP-120`,
  `WP-127`, `WP-130`, `WP-140`, `WP-150`, `WP-180`, `WP-185`, `WP-190`, and
  `WP-200`
- **Final evidence:** `WP-190`, `WP-200`

## Decision Deadline

Exact semantic acceptance is required before WP-060 merges a capability record,
lookup key, grant vocabulary, or persistence port. The durable proto-owner record
must merge through WP-065 before WP-070 persists it, and public completion must
merge through WP-127 before WP-130 exposes it. In the same governance commit,
the specification, ADR cross-references, and manifest apply the exact delta
above, including the additional WP-070 and WP-130 dependencies while leaving
WP-020 unchanged. Acceptance of this exact text also accepts only the reviewed
dependency graph above; any change reopens dependency review. No implementation
or fixture may choose different bytes, tags, transitions, schema owners, result
owners, token carriage, clock semantics, or trust boundaries implicitly.

WP-130 must supply the complete provider inventories and composed readiness
evidence before its runnable P1 exit gate. WP-185 consumes that established core
and cannot postpone or duplicate this security boundary.
