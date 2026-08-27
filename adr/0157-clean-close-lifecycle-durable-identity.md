# ADR-0157: Clean-Close Lifecycle Durable Identity

- **Status:** Accepted
- **Direction approved:** 2026-08-26 through accepted ADR-0156
- **Exact text accepted:** Yes, 2026-08-26
- **Successor registry digest accepted:** Yes, 2026-08-26 —
  `713f2aabc242d6d2263fd27675586453ab41c754aef84b48d4233060fe03a131`
- **Accepted:** 2026-08-26
- **Acceptance reference:** Maintainer exact-text acceptance in the current
  Codex session for upstream commit `c5c73858`
- **Decision deadline:** Before WP-704 adds a meta key, Protobuf message,
  registry transition, or lifecycle write
- **Requires:** ADR-0006, ADR-0019, ADR-0061, ADR-0072, ADR-0085, ADR-0103,
  ADR-0104, ADR-0112, and ADR-0156
- **Amends if accepted:** ADR-0156 section 2 by freezing its deferred durable
  key, fields, tags, hashes, lifecycle states, bounds, and migration behavior
- **Defines or blocks:** WP-704

This record is authoritative for WP-704. It adds no new direction beyond
ADR-0156; it closes the durable interface that record deliberately left
review-gated.

## Context

ADR-0156 accepted an automatic conventional clean-start fast path and one
private engine-atomic clean-close record. It requires the exact storage key,
Protobuf fields and tags, canonical hash domains, byte ceiling, registry
transition, dirty-consumption representation, and old-reader behavior to be
accepted before implementation.

A bare persisted Boolean is insufficient. Startup must durably make the prior
clean close ineligible before activating any writer, and a crash at that
boundary must leave an unambiguous state. Deleting a certificate represents
dirty state but loses a monotonic process-generation witness. A single
versioned lifecycle record with explicit `DIRTY` and `CLEAN` states preserves
that witness, keeps absence reserved for predecessor databases, and permits one
Immediate replacement at each activation and graceful close.

The record must not become an authenticated-table design by accident. Its
bounded state binding covers only the fixed startup roots that can be reread
without a population walk. Catalog bundles and initial authority roots are
validated separately by the sealed startup typestate. Entity, index, history,
event, audit, projection, outbox, provenance, and idempotency populations do not
enter the certificate hash.

## Proposed Decision

### 1. Freeze one key and one top-level record

The redb `META` table gains exactly one key:

```text
clean_close_certificate/v1
```

Its value is one ordinary canonical `StoredEnvelope` whose `record_type` is:

```text
riffdb.storage.v1.StoredCleanCloseLifecycleV1
```

The message and enum are added to
`proto/riffdb/storage/v1/clean_close_lifecycle_v1.proto` with these exact
definitions:

```protobuf
syntax = "proto3";

package riffdb.storage.v1;

enum StoredCleanCloseLifecycleStateV1 {
  STORED_CLEAN_CLOSE_LIFECYCLE_STATE_V1_UNSPECIFIED = 0;
  STORED_CLEAN_CLOSE_LIFECYCLE_STATE_V1_DIRTY = 1;
  STORED_CLEAN_CLOSE_LIFECYCLE_STATE_V1_CLEAN = 2;
}

message StoredCleanCloseLifecycleV1 {
  bytes database_id = 1;
  fixed64 history_incarnation = 2;
  bytes record_registry_digest = 3;
  fixed64 lifecycle_generation = 4;
  StoredCleanCloseLifecycleStateV1 state = 5;
  bytes clean_state_binding_hash = 6;
  bytes lifecycle_hash = 7;
}
```

No field is optional, repeated, a map, text, a timestamp, or application data.
No unknown enum value or unknown field is accepted by the canonical durable
decoder. Tags 1 through 7 are permanently occupied by the meanings above.

The semantic payload ceiling is exactly 192 bytes. `database_id` is exactly 16
bytes and must decode as the existing nonzero `DatabaseId` canonical bytes.
Both digest/hash fields, when present, are exactly 32 bytes.
`history_incarnation` and `lifecycle_generation` are nonzero. The registry
digest must equal the registry identity under which this record is readable.

For `DIRTY`, `clean_state_binding_hash` must be empty and is omitted by canonical
Protobuf encoding. For `CLEAN`, it must contain exactly 32 bytes. State zero,
an unknown state, zero generation/incarnation, wrong lengths, a dirty nonempty
binding, a clean empty binding, noncanonical wire bytes, a mismatched envelope
schema hash, or a mismatched self-hash is invalid lifecycle evidence.

### 2. Freeze the two domain-separated hashes

Both hashes use the existing `riffdb_types::hash(HashDomain::Schema, bytes)`
primitive; no dependency or cryptographic trust claim is added.

The lifecycle self-hash label is the exact ASCII byte string including its
terminal NUL:

```text
riffdb-clean-close-lifecycle-v1\0
```

The preimage is the label followed in order by:

1. `1_u32.to_be_bytes()`;
2. the 16 database-ID bytes;
3. `history_incarnation.to_be_bytes()`;
4. the 32 registry-digest bytes;
5. `lifecycle_generation.to_be_bytes()`;
6. one state byte (`0x01` dirty or `0x02` clean);
7. one binding-presence byte (`0x00` dirty or `0x01` clean); and
8. for clean state only, the 32 clean-state-binding bytes.

`lifecycle_hash` must equal the resulting 32-byte digest. No Protobuf encoding,
length delimiter, omitted default, or envelope byte participates in this
self-hash.

The clean-state binding label is the exact ASCII byte string including its
terminal NUL:

```text
riffdb-clean-close-bounded-roots-v1\0
```

The binding preimage begins with that label and `1_u32.to_be_bytes()`, then
contains the exact bounded-root item stream defined in section 3, followed by
the exact 32-byte SHA-256 digest of the selected active recyclable-journal
extent header's complete canonical encoded header slot. The latter digest is a
binding to existing checksummed journal state, not a new journal identity or a
substitute for normal journal recovery. The complete preimage is hashed once
with `HashDomain::Schema` to produce `clean_state_binding_hash`.

### 3. Freeze the bounded-root item stream

The bounded-root stream contains only fixed-inventory singleton keys. Items are
emitted in the following exact order:

1. `META/format_version`;
2. `META/database_id`;
3. `META/next_application_sequence`;
4. `META/next_administration_sequence`;
5. `META/capability_bootstrap/v1`;
6. `META/record_registry/v2`;
7. `META/history_incarnation/v1`;
8. `META/index_epoch_rows_repaired/v1`;
9. `META/retention_watermark/v1`;
10. `META/retention_holds/v1`;
11. `META/changelog_v2_rotation_receipt/v1`;
12. `CATALOG_ACTIVE/[0x01]`.

Those twelve fixed items are followed by one `u32` big-endian active-query-
module count and then every `QUERY_MODULE_ACTIVE` row in exact redb key order,
each using the item framing below with table tag `0x03` and a required present
value. The count must not exceed `MAX_RETAINED_QUERY_MODULES`; duplicate,
noncanonical, out-of-order, or key/pointer-mismatched rows make clean
certification ineligible. The referenced bounded module bodies and hashes are
validated separately while constructing the clean startup catalog proof.

`META/validated_prefix_checkpoint/v1` is deliberately excluded: a clean fast
open neither earns nor requires a current complete-validation proof. The
clean-close lifecycle key itself is excluded to avoid recursive hashing.

Every item is framed as:

```text
table_tag: u8
key_length: u32 big-endian
key: key_length bytes
value_tag: u8
[value_length: u32 big-endian, value: value_length bytes]
```

Table tags are `0x01` for `META`, `0x02` for `CATALOG_ACTIVE`, and `0x03` for
`QUERY_MODULE_ACTIVE`. `value_tag` is `0x00` for an absent fixed item, with no
length or value following, and `0x01` for present, followed by the length and
the complete exact stored value bytes. Query-module rows may not use the absent
tag. Present values must first pass their ordinary bounded canonical envelope
and semantic decode; the binding never blesses malformed bytes. Key bytes are
the exact bytes shown above, including the catalog singleton `[0x01]` key.

The implementation must check every length conversion and aggregate preimage
addition. The maximum retained binding preimage is 16 MiB, covering the
existing bounded retention-hold document and at most
`MAX_RETAINED_QUERY_MODULES` active pointers.
Exceeding it makes clean certification ineligible without weakening graceful
shutdown durability; the next startup takes complete validation.

Adding, removing, renaming, or reinterpreting a bounded-root item, table tag,
framing rule, journal-header selection rule, hash label, or preimage field
requires a successor lifecycle record and separately accepted ADR. A registry
successor alone may not silently change this V1 hash.

### 4. Freeze lifecycle transitions

The lifecycle state machine is:

```text
ABSENT --complete validation--> DIRTY(1)
DIRTY(n) --complete validation before activation--> DIRTY(n + 1)
CLEAN(n) --verified fast startup before activation--> DIRTY(n + 1)
DIRTY(n) --successful final graceful close--> CLEAN(n + 1)
```

Every arrow that writes a state is one redb transaction with Immediate
durability. Addition is checked. Generation exhaustion prevents operational
readiness and clean certification; it never wraps, resets, or accepts another
writer generation.

An absent record is valid only as predecessor state and always requires
complete validation. Immediately before first operational activation under the
successor registry it becomes `DIRTY(1)`. A valid dirty record always requires
complete validation and is replaced by its dirty successor immediately before
activation so one process generation owns one distinct durable transition.

A valid clean record is not modified until engine status, its hashes and
bindings, and every ADR-0156 bounded readiness root have validated. It is then
replaced by the dirty successor before any writer or derived worker activates.
A crash before that commit leaves the database byte-logically unchanged and the
clean record reusable. A crash after it leaves durable dirty state.

Only the shared final shutdown coordinator may replace dirty with clean. It
must derive the binding from the same final read transaction used to verify the
drained dual frontier and selected empty journal header, and the Immediate clean
commit must remain the final authoritative mutation. A process that started
dirty may still earn a clean close after complete validation and ordinary
operation. A failure to write clean leaves dirty state and is nonfatal to work
already acknowledged.

Malformed lifecycle bytes cannot provide a trustworthy generation. They force
complete validation; after successful validation the implementation may replace
them with `DIRTY(1)` as an explicit compatibility reset and emit only a bounded
redacted diagnostic. No fast path follows that reset in the same startup.

### 5. Freeze registry and compatibility behavior

The current readable record-registry digest at the implementation revision is
the sole predecessor. Adding `StoredCleanCloseLifecycleV1` and its descriptor
closure produces one generated successor digest checked into the durable-format
manifest and compatibility fixtures. The successor digest value is derived by
the existing generator and must receive human exact-byte acceptance in the
WP-704 interface-first commit before any runtime code uses it.

Migration from the predecessor registry adds no lifecycle row and performs no
population scan. The first successor startup therefore takes complete
validation and writes `DIRTY(1)` before activation. Downgrade to a predecessor
binary follows the existing unknown-registry refusal; it must not ignore or
delete the lifecycle row.

Backup copies the lifecycle row as ordinary database metadata only when the
backup boundary already permits that database state. Restore, history-
incarnation rotation, registry migration, and any operation that replaces the
database make a carried clean record ineligible; their accepted protocols may
delete it or leave it to fail bindings, but may not rewrite it clean. A
byte-for-byte copy of a fully closed database inside the same conventional
local trust boundary may retain clean eligibility when every binding and
journal header remains exact.

## Options Considered

1. **Explicit dirty/clean record with checked generation:** selected. It makes
   the activation crash boundary durable and preserves one process-generation
   witness in one meta key.
2. **Delete clean certificate to mean dirty:** rejected because it discards the
   generation witness and makes lifecycle diagnostics and transition schedules
   less falsifiable.
3. **Boolean clean field only:** rejected because it does not bind database,
   format, registry, frontiers, catalog roots, or journal state.
4. **Hash all authoritative tables:** rejected by ADR-0156; it recreates a
   population scan or requires an authenticated-tree design.
5. **Persist process-local catalog proofs:** rejected. Proofs are nonserializable
   authority and must be rebuilt from bounded canonical catalog inputs.

## Consequences

- The durable format gains one meta key, one enum, one top-level record, one
  registry successor, and two domain-separated semantic hashes.
- Every activation and graceful close adds one small Immediate redb commit.
- Lifecycle generation exhaustion is an explicit terminal readiness condition.
- Clean binding cost is independent of population but may include the bounded
  retention-hold and active catalog/module singleton bytes.
- The selected journal header remains governed and validated by its existing
  format; the lifecycle stores only its digest inside a larger bounded proof.
- Predecessor databases open through complete validation once before they can
  participate in the clean fast path.

## Compatibility

This is an additive private durable-format and registry change. It changes no
application API, public Protobuf, contract or query IR, entity/index encoding,
driver protocol, MCP schema, outcome, event, provenance, or public cursor.
Predecessor bytes remain readable through migration; predecessor binaries
refuse the successor registry normally.

## Security

The lifecycle hashes detect accidental or malformed record/binding changes and
stale bounded roots. They are unkeyed and do not authenticate hostile offline
writes. No secret, signing key, entropy, clock, node identity, or operator-
supplied assertion enters the state machine.

Only private sealed startup/shutdown code can construct transitions. Any
invalid evidence selects complete validation, and any integrity failure found
there or during a bounded root/operational row check remains fail closed. The
record contains database identity, hashes, and lifecycle counters and must not
be rendered through public errors, MCP, or unbounded diagnostics.

## Standing Design Tests

- **Interface safety:** No application, agent, operator argument, transport,
  configuration, or backend caller can construct, select, preserve, or consume
  lifecycle state. The only public addition requested by ADR-0156 asks for more
  validation through offline scrub.
- **Scale:** Hash construction reads twelve singleton items, at most
  `MAX_RETAINED_QUERY_MODULES` active pointers, and one fixed-size journal
  header under a 16 MiB preimage bound. It performs no unbounded or live-data-
  proportional walk and retains no entity, index, history, event, audit,
  provenance, projection, outbox, capability, or idempotency collection.

## Testing

- Exact Protobuf descriptor, field/tag, enum, canonical-byte, semantic-bound,
  hash-preimage, registry-digest, and durable-manifest fixtures.
- Property tests reconstruct both hashes independently and mutate every field,
  framing byte, item presence, order, journal header, and bound edge.
- Deterministic state-machine schedules for every transition, checked
  generation overflow, malformed reset, missing predecessor, and concurrent
  activation/shutdown refusal.
- Process crashes immediately before and after every Immediate dirty and clean
  commit, followed by exact startup-mode and no-extra-write assertions.
- Migration, downgrade refusal, backup/copy, restore, incarnation rotation, and
  registry mismatch fixtures.
- Architecture tests proving population tables are absent from binding
  construction and lifecycle constructors remain private to startup/shutdown.

## Requirements and Work Packages

- **Requirements:** `STO-023`, `REC-004`, `PERF-019`
- **Defines or blocks:** WP-704
- **Final evidence:** WP-705

## Decision Deadline

Exact human acceptance, followed by acceptance of the generator-derived
successor registry digest in WP-704's interface-first commit, is required before
the meta key, Protobuf file, semantic lifecycle type, codec, migration, or
startup/shutdown transition is implemented.
