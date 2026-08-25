# ADR-0151: Atomic Unary Delete and Return of the Revalidated Preimage

- **Status:** Accepted
- **Direction approved:** Yes
- **Exact text accepted:** Yes, 2026-08-25
- **Accepted:** 2026-08-25
- **Acceptance reference:** Maintainer exact-text acceptance in the current
  Codex session for commit `a9093c56`
- **Decision deadline:** Before WP-691 admits an ordinary delete binding or a
  command outcome dependency on a deleted row
- **Requires:** ADR-0002, ADR-0003, ADR-0005, ADR-0012, ADR-0023, ADR-0031,
  ADR-0055, ADR-0059, ADR-0107, ADR-0124, ADR-0126, and ADR-0129
- **Amends if accepted:** ADR-0107's first executable delete shape, which is
  currently confined to a compiler-bounded collection expansion
- **Defines or blocks:** WP-691 and WP-692

This record is authoritative for WP-691 and WP-692.

## Context

An atomic consume operation commonly needs to delete one exact row and return
the value that was consumed. A verification code, one-time token, lease ticket,
queue claim, or idempotent handoff must not first read a row through RiffQL and
then delete it through a separate command: another writer can update or consume
the row between those operations, and the returned value may not be the value
whose deletion committed.

RiffDB already has almost all of the required internal evidence. A delete
mutation carries the exact prior entity image and expected entity version. The
commit coordinator validates that transaction-current version and applies the
delete, declared outcome, provenance, events, commit record, and persisted
idempotency result atomically. On a validation conflict, the whole deterministic
command is reevaluated from a new snapshot.

The public compiler/runtime shape is narrower than those semantics. Command IR
has a delete binding and expressions can name its fields, but checked IR rejects
an ordinary delete without a collection-expansion plan. The ordinary runtime
also rejects every delete binding. The collection runtime can materialize each
deleted row's preimage, but its final whole-command outcome is evaluated after
the repeated records have been discarded, and collection elements are
deliberately forbidden from escaping their template. A separate read binding
does not repair the problem and creates two aliases for one transaction target.
The compiler currently exposes this unsupported combination as
`RDB-C023 internal_invariant` instead of a source diagnostic.

The missing capability is generic atomic delete-and-return-preimage semantics,
not authentication logic. No application-specific entity, outcome, route, or
adapter belongs in RiffDB.

## Proposed Decision

### 1. Admit one ordinary exact-key delete binding

An ordinary idempotent command may declare exactly one compiler-resolved
`delete` binding outside a `bulk command`. The initial executable shape is:

- one complete partition-local primary key derived from typed command input;
- one entity whose structural deletion policy is `no_inbound`;
- one declared missing-row business outcome;
- one success outcome and any explicitly declared bounded event payloads; and
- the existing command input, outcome, authorization, idempotency, mutation,
  event, provenance, conflict, and transaction-size ceilings.

This is not a general transaction or a shorthand for a query followed by a
write. The compiler owns the exact entity, key, partition, deletion policy,
fields, outcome shapes, event shapes, authority, and worst-case graph. Callers
supply typed values only.

Indexed-restrict and bounded-cascade deletion remain on ADR-0107/ADR-0126's
collection execution path in the first slice. Extending them to ordinary
preimage-return commands requires a real consumer plus a shared delete-evidence
schedule; it may not be inferred from this no-inbound case. Cross-partition,
set-null, orphaning, physical erasure, runtime-discovered deletion, and
application-selected policy remain forbidden.

### 2. Define a delete binding as the exact transaction preimage

For an ordinary delete binding, every binding field and complete-record
expression denotes the exact entity preimage materialized from the command's
owned read snapshot. It never denotes a tombstone, a post-delete lookup, a
separately fetched copy, or a value reconstructed from command input.

The same immutable materialized record supplies:

- declared requirements that run before effects;
- the prior image placed in the delete mutation;
- explicit event payload expressions; and
- the terminal success outcome, including a complete-record return when its
  declared schema and size bounds permit it.

No set instruction may target a delete binding. A command may not bind the same
entity key separately for read or mutation merely to preserve a second copy.
The existing duplicate-target and read/write-overlap checks remain fail closed.
The preimage is command-local deterministic evidence and is unavailable after
the command except through an authorized declared outcome, event, provenance,
or retained history surface that already owns such data.

The compiler records every referenced preimage field in the binding's accessed
field set and authorization requirement. Complete-record use remains explicit
and bounded. Unreferenced fields need not be exposed to expression evaluation,
although storage and the commit coordinator may retain the complete prior image
required for index removal, changelog validation, and history.

### 3. Make the committed outcome and delete prove the same version

Evaluation may construct the success outcome from the snapshot preimage, but it
does not make that outcome durable or releasable by itself. The evaluated
command graph binds the exact delete target, expected entity version, prior
image identity, and declared outcome. The commit coordinator remains the only
owner of transaction-current validation and commit sequencing.

If the row is unchanged, the coordinator atomically commits the deletion and
that outcome. If another command updates or deletes the row first, validation
rejects the evaluated graph before either mutation or outcome is committed.
Bounded whole-command reevaluation then observes the winning state:

- an update winner causes the delete and returned outcome to be rebuilt from
  the updated transaction-current preimage; or
- a delete winner causes the declared missing outcome with no deletion.

A stale preimage can therefore be evaluated transiently but can never become a
persisted or released success outcome. The command does not add a compare-and-
swap escape, read-committed window, or last-write-wins rule. Existing conflict
ownership, retry ceilings, cancellation, uncertainty recovery, and
transaction-current validation remain unchanged.

### 4. Preserve exactly-once consume and replay semantics

Two concurrent invocations with different idempotency keys against the same
row may both evaluate a present snapshot, but at most one exact delete graph can
commit. The loser reevaluates and returns the declared missing outcome. They
cannot both return a committed consumed outcome.

Replay of the winning idempotency identity returns the already persisted
declared outcome byte-for-byte, including authorized preimage fields, without
rereading current entity state or attempting another delete. Replay of the
missing result likewise returns its persisted missing outcome. A request that
reuses an idempotency identity with different input remains the existing typed
idempotency mismatch.

The returned result's commit sequence is the sequence that deleted the row.
There is no separately observable read sequence and no success response before
the atomic durability boundary.

### 5. Apply secret authority to preimage outputs without a bypass

A secret field on a deleted row remains secret. Returning it requires the same
compiler-declared `reveals` annotation, generated secret-output metadata, role
grant, plan/module identity binding, response redaction, logging prohibition,
and fresh release-time authorization as any other command secret output.

Deletion does not implicitly reveal all fields and possession of delete
authority does not imply reveal authority. A missing, denied, conflicted, or
replayed request must not leak whether an undisclosed field existed through
diagnostics, metrics, tracing, MCP text, timing class, or alternate outcome
shape beyond the explicitly authorized declared business outcome.

### 6. Replace the internal invariant with a checked public diagnosis

WP-691 must remove `RDB-C023` for every well-formed source shape governed by
this ADR. A supported unary no-inbound delete lowers successfully. Unsupported
ordinary restrict/cascade, multiple-delete, repeated-element-return, duplicate-
target, and cross-partition shapes receive source-spanned compiler diagnostics
with bounded generic remediation.

An internal invariant remains appropriate only for malformed checked IR or a
compiler/runtime disagreement. It is not the public feature-unavailable
response for a valid source construct.

### 7. Keep the first implementation slice independent

WP-691 implements the compiler, checked-IR validation, ordinary runtime,
authorization, secret-flow, deterministic concurrency, idempotency, and memory/
redb parity for the generic unary no-inbound shape. It reuses the existing
delete mutation and prior-image validation; it does not change transaction
ordering or add a second storage primitive.

WP-692 adds crash/replay, generated binding, public transport, generic example,
handbook, and external-consumer acceptance. Adapter source and authentication
semantics remain in the external repository. This command blocker may proceed
independently of ADR-0150's later access-path consolidation and ADR-0152's
aggregate expansion.

## Options Considered

1. **Read through RiffQL, then issue a delete command:** rejected because the
   returned row can differ from the row whose deletion commits.
2. **Add a second read binding before the delete:** rejected because it creates
   duplicate aliases for one target and still lacks a single checked semantic
   identity for the returned preimage.
3. **Return a caller-supplied copy of the deleted value:** rejected because
   command input is not proof of transaction-current entity state.
4. **Let a bulk element escape into the whole-command result:** rejected because
   it weakens ADR-0107's bounded template rules and does not model a unary
   operation honestly.
5. **Admit one unary delete whose binding is the revalidated preimage:** proposed
   because it reuses the existing atomic graph and validation boundary without
   exposing transactions or storage.

## Consequences

- One-time consume, pop, and delete-and-return workflows become one generated
  atomic command.
- The ordinary runtime gains deletion support for one deliberately narrow
  structural policy; bulk restrict and cascade semantics do not silently move.
- Outcome field access and delete validation share one record/version identity.
- Secret deletion results remain explicit permission widenings.
- Multiple-row delete-and-return remains unavailable until a real result shape
  can preserve ADR-0107's aggregate bounds and element-escape prohibition.

## Compatibility

This Proposed ADR changes no current bytes or behavior. If accepted, WP-691
admits a combination of existing grammar and command-IR concepts that current
checked construction rejects: an ordinary plan containing one delete binding
without collection expansion and outcome expressions referencing that binding.
Its binding mode, expressions, accessed fields, secret reveals, outcome schema,
delete check, plan hash, and canonical bundle encoding already have versioned
representations; no existing valid artifact is reinterpreted.

The newly valid combination requires a new runtime release. An older compiler
or decoder continues to fail closed rather than execute it incorrectly. Golden
fixtures must prove old valid plans remain byte-identical, the newly admitted
plan has a distinct exact identity, malformed combinations remain rejected,
and mixed compiler/runtime deployment cannot activate an unsupported plan. If
implementation proves a new grammar tag, IR field, durable outcome field,
mutation encoding, capability, wire value, or generated schema identity is
necessary, WP-691 stops for ADR-0124 classification and exact human review.

Persisted outcomes, delete mutations, entity transitions, changelog entries,
history, and replay retain their existing encodings. This ADR does not permit
physical erasure of retained history or secrets.

## Security

The application surface remains one generated command with typed input and a
closed outcome union. The caller cannot select a table, key expression,
predicate, transaction, return field, reveal, deletion policy, retry rule, or
durability mode. Compiler authority includes every possible branch and release
still performs fresh authorization.

Preimage values are never emitted through compiler/runtime diagnostics,
metrics, traces, or public errors. The committed outcome is redacted before
logging and MCP rendering under existing rules. Conflict and missing paths have
bounded value-free diagnostics and release no transient evaluated outcome.

## Standing Design Tests

- **Interface safety (AGENTS.md boundary 11):** applications invoke only a
  compiler-declared exact-key delete command. Atomicity, transaction-current
  validation, idempotency, durability, policy, provenance, and secret authority
  are mandatory and have no opt-out or callback surface.
- **Scale:** the initial command reads and deletes exactly one partition-local
  entity, performs fixed structural no-inbound proof work, and returns a
  schema-bounded outcome. It requires no scan, collection growth, cross-
  partition work, or row-proportional retained runtime state.

## Testing

- Compiler and source-span snapshots for supported unary no-inbound delete and
  every unsupported policy/count/target combination.
- IR encode/decode/hash fixtures proving the preimage dependencies, accessed
  fields, secret reveals, delete check, and distinct plan identity.
- Pure-runtime and memory/redb tests proving returned scalar and complete-record
  fields equal the exact prior image removed from entity and every index.
- Deterministic two-request schedules in which delete/delete yields exactly one
  consumed outcome and update/delete either returns the updated preimage or a
  typed retry/refusal, never a stale committed result.
- Idempotent replay before and after restart, uncertain response recovery, and
  crash arms around evaluation, staging, durability fence, publication, and
  response.
- Secret reveal grant/deny/revoke/redaction tests for deleted fields across
  gRPC, SDK, CLI, and MCP paths.
- Generic one-time-token acceptance plus the external adapter loopback, with no
  adapter code, framework names, or authentication policy added to RiffDB.

## Requirements and Work Packages

- **Future requirements after exact acceptance:** `BLK-028` through `BLK-035`
- **Compiler/runtime/concurrency implementation:** WP-691
- **Recovery, generated surfaces, documentation, and external acceptance:**
  WP-692

## Decision Deadline

Exact human acceptance is required before WP-691 admits ordinary delete IR or
changes runtime treatment of a delete binding. A change to transaction-current
validation, conflict ownership, atomic outcome sequencing, durable encoding,
or secret authority must stop for a separately classified human decision.
