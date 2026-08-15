# ADR-0126: Compiler-Bounded One-Hop Cascade Deletion

- **Status:** Accepted
- **Direction approved:** 2026-08-15 (maintainer, in session)
- **Exact text accepted:** Yes — 2026-08-15, maintainer acceptance as written
- **Decision deadline:** Before WP-625 changes contract grammar, executable IR,
  deletion planning, conflict ownership, or command runtime semantics
- **Requires:** ADR-0002, ADR-0003, ADR-0005, ADR-0012, ADR-0013,
  ADR-0055, ADR-0107, ADR-0111, ADR-0112, and ADR-0124
- **Amends if accepted:** ADR-0107's prohibition on cascade and `BLK-003`,
  `BLK-005`, `BLK-010`, and `BLK-013` only as stated below
- **Defines or blocks:** WP-625 through WP-627

The maintainer accepted this exact text on 2026-08-15 before implementation
began.

## Context

RiffDB's checked delete supports only `no_inbound` and one indexed `restrict`
policy. That boundary correctly prevents dangling references, unindexed
discovery, and runtime-selected mutation sets, but it cannot implement a common
account-lifecycle operation. A Better Auth user has independently referenced
account, session, and verification rows. Deleting those rows in application
code as separate commands exposes intermediate states and races; removing the
references weakens stored integrity; and the current one-index restrict policy
cannot prove all three families absent.

A generic cascade would be worse. Recursive traversal, engine-owned foreign-key
behavior, caller-selected policy, or an unbounded reverse scan would hide the
command's authority, cost, conflict scope, and durable mutation graph. The
acceptable addition must remain a compiled command whose complete relationship
families, access paths, maxima, partition, aggregate conflict owner, outcomes,
and authorization requirements are visible before deployment.

This decision deliberately changes ADR-0107's rule that every mutation key is
computable solely from command input before staging. It permits one narrow class
of data-dependent key discovery from exact compiler-named reverse-index
prefixes. Because that is a new language construct and changes dependency
visibility, exact human acceptance is mandatory.

## Proposed Decision

### 1. One explicit, exhaustive cascade policy

The contract language adds one `cascade` deletion policy. Its first source form
is a closed block of entries:

```riff
entity User {
  key (organization_id: uuid, user_id: uuid)
  delete_policy cascade {
    relationship Account.account_user using Account.by_user maximum 32
    relationship Session.session_user using Session.by_user maximum 64
    relationship VerificationToken.token_user
      using VerificationToken.by_user maximum 32
  }
}
```

Each entry names exactly one direct inbound relationship, one exact source
entity index, and one positive compiler-fixed maximum. The source entity named
on both sides must agree. Entries are canonically ordered by stable relationship
ID; source order is retained only for diagnostics. The policy must enumerate
every declared inbound relationship to the target exactly once and may not name
an outbound, duplicate, indirect, or foreign-target relationship.

The selected index must begin with the complete source fields of the
relationship in canonical order, route to the same partition as the target,
and enumerate every referencing row under one exact prefix. A partial,
filtered, unique-only, projection, text, vector, runtime-selected, or unindexed
path is rejected. The compiler never substitutes another index at runtime.

Every source entity selected for automatic deletion must itself declare
`delete_policy no_inbound`. Cascaded entities cannot have inbound references,
use another cascade, or participate in recursive, cyclic, set-null, orphaning,
or multi-level removal. The target and all source entities must belong to one
declared aggregate with structurally identical partition and aggregate conflict
key derivations from the target key. Cross-aggregate and cross-partition
cascade are rejected.

### 2. One binding and one distinct overflow outcome

Cascade remains available only inside an explicit `bulk command`, under
ADR-0107's ordinary checked delete binding. A cascade binding must declare one
business outcome distinct from the absent-target outcome:

```riff
bulk command DeleteUsers {
  input request_id: uuid
  input organization_id: uuid
  input user_ids: list<uuid, 1..1>
  idempotency_key request_id

  for user_id in user_ids {
    delete User(organization_id, user_id) as user
      else UserMissing {}
      cascade CascadeLimitExceeded {}
  }

  return UserDeleted {}
}
```

The caller supplies only the root key and ordinary command input. It cannot
select relationships, indexes, traversal depth, maxima, ordering, child keys,
authorization behavior, durability, or a cascade mode. The discovered keys are
not exposed as command-language values and cannot drive another read, mutation,
event loop, predicate, or dynamic dispatch.

The first cascade format permits at most one cascade delete template per
command and at most 32 relationship entries in one policy. For the declared
collection maximum, the compiler proves:

```text
root_count_maximum * (1 + sum(relationship maxima)) <= 256
```

The 256 bound counts target and child entity removals. Existing per-record,
index, command-input, mutation-graph, and 16 MiB transaction bounds also apply;
the compiler may require lower maxima. No request can raise them.

### 3. Bounded authoritative enumeration, never a scan fallback

After acquiring the complete aggregate conflict capability derived from the
submitted root key, evaluation reads the target and probes each declared
reverse-index prefix for at most `maximum + 1` rows. The probe order is stable
relationship ID, then canonical source entity key. Every prefix observation,
returned predecessor, and index epoch/range dependency enters the ordinary
transaction-current evidence.

If any prefix returns the extra row, evaluation selects
`CascadeLimitExceeded`, persists that declared outcome with zero entity or index
mutations, and does not continue probing later relationships. Which entry
overflowed is retained only in bounded internal evidence; the public outcome
does not reveal a relationship name, child key, row value, or count. A changed
prefix or predecessor invalidates validation and causes bounded whole-command
reevaluation, never partial continuation or an unbounded infrastructure retry.

Absence of the declared index, an incomplete prefix, malformed evidence,
unexpected relationship state, or an over-budget graph is fail-closed
integrity/resource behavior under existing typed boundaries. It is never
reinterpreted as an empty collection or a successful delete.

### 4. Conflict ownership and race semantics do not change

The compiler derives the same aggregate conflict key for target deletion and
every command that can create or mutate a cascaded child. The conflict manager
acquires that complete key before enumeration; no child key causes incremental
or partial capability acquisition. The commit coordinator remains the only
owner of validation, sequence assignment, and publication.

A child create that commits first is visible to the later cascade and is
removed. A cascade that commits first removes the target, so a racing or later
child create fails its transaction-current target-reference validation and
cannot survive as a dangling row. If the required common conflict-key proof is
not structural and exact, compilation fails; runtime reordering is not a
fallback.

### 5. Authorization and row policy cover every removed row

Role compilation derives delete authority, field visibility, row-policy plans,
relationship/index access, and maximum work for the target plus every cascaded
entity. Before staging, the shared authorization path evaluates the current
target and each exact child predecessor. Commit-time validation rechecks the
current capability revision, target row, every child row, and every
policy-dependent relationship observation.

One denied, hidden, or missing-proof child prevents the entire command; there
is no partial authorized subset and no application-side filtering. A stale or
disappeared child invalidates the evaluated graph and causes whole-command
reevaluation under the same bounds. Unauthorized diagnostics do not confirm
which relationship or row exists. MCP, CLI, gRPC, and all generated SDKs invoke
the same symbolic command and receive the same declared outcome/error family.

### 6. Canonical atomic graph and retained history

On success, mutations are materialized in this order for hashing, validation,
provenance, changelog emission, and recovery:

1. submitted root element order;
2. stable relationship ID;
3. canonical child entity key;
4. all child removals; then
5. the target removal.

Every removal reuses the existing checked entity-delete path: the exact current
predecessor derives all primary, secondary, unique, and relationship-index
removals. Graph-final reference validation permits a child reference and its
target to disappear only because both exact predecessors are in this one sealed
graph. No transient state is observable.

One invocation retains one idempotency identity, canonical caller input hash,
authorization decision, deterministic context, declared outcome, commit
sequence, provenance, audit lifecycle, changelog, and atomic durable graph.
Discovered child keys do not alter caller input identity; they are sealed into
the evaluated mutation/dependency graph that the coordinator validates and
persists. Retry returns the original terminal result. Cancellation, crash,
recovery, backup, replication, and projection masking can expose only the
complete acknowledged graph or its complete absence.

Cascade emits no implicit domain event and exposes no child collection or count
to the command expression language. Existing explicitly declared events remain
bounded and must be computable without iterating discovered children. Current
entity/index state is removed, while immutable command, outcome, event,
provenance, audit, changelog, and historical evidence remain retained. Cascade
is not physical erasure, retention, privacy redaction, or history purge.

### 7. Version and activation boundary

Cascade requires bundle, grammar, and executable IR V13. A V13 policy records
the exhaustive stable relationship IDs, exact index IDs, maxima, aggregate
conflict proof, canonical ordering, and overflow outcome identity. V1 through
V12 readers and least-sufficient writers remain active. Contracts without
cascade continue to emit the least sufficient older identity; a cascade plan
cannot be downcompiled.

This is a `new_domain_identity` under ADR-0124 for the bundle, grammar, and
executable-IR domains. The topology, format registries, exact lock, generated
artifacts, old/current fixtures, and handbook must rotate together. Adding or
changing a cascade policy changes contract and affected command plan identity
and requires the existing explicit application evolution ceremony; it is never
silently activated for an existing command.

The decision expects no new public wire field or durable tombstone/command-
graph encoding: V13 carries the plan, and successful execution expands to the
already accepted ordered delete mutations. If implementation proves that a
wire field, durable envelope/tag, changelog class, transaction ordering rule,
or conflict-ownership rule must change, work stops for a separately accepted
ADR amendment. An implementation package may not broaden this record.

## Options Considered

1. **Delete children in adapter/application code:** rejected because separate
   commands expose intermediate state, race with child creation, and cannot
   share one outcome or idempotency identity.
2. **Remove references or use set-null/orphaning:** rejected because it weakens
   declared integrity and changes the application data model to fit a storage
   limitation.
3. **Storage-engine or recursive cascade:** rejected because traversal, bounds,
   authorization, ordering, and durable evidence become hidden runtime policy.
4. **One command with caller-supplied child keys:** rejected because omission
   can leave undeclared dependents and correctness becomes an application
   convention.
5. **Compiler-bounded one-hop indexed cascade:** proposed because every family,
   path, maximum, conflict owner, policy, and outcome stays explicit while the
   exact child keys may come from authoritative current state.

## Consequences

- Better Auth can express full account removal as one compiled command without
  weakening Account, Session, or VerificationToken references.
- Static dependency visibility expands narrowly: exact child keys are
  data-dependent, but their complete source families, paths, maxima, authority,
  locality, and worst-case graph are compiler-owned and reviewable.
- Hot user aggregates serialize account/session/token creation with deletion;
  that is the intended correctness cost and is visible in explain output.
- Large or recursive ownership graphs still require explicit application
  redesign or multiple safe commands; this surface is not general garbage
  collection.
- Set-null, orphaning, cross-partition deletion, recursive cascade, arbitrary
  joins/scans, callbacks, generic transactions, and physical erasure remain
  unavailable.

## Compatibility

V13 is an additive executable-IR family with retained V1-V12 readers and
least-sufficient writing. Cascade source cannot be consumed by older compilers
or runtimes and fails before activation. Existing contract, command, generated
client, protocol, durable database, backup, and changelog semantics remain
unchanged for non-cascade plans.

A successor contract introducing cascade changes bundle/plan/role identities
and its generated outcome union. Migration review must account for existing
dependent cardinality exceeding a new maximum before activation. No automatic
deletion occurs at deployment or migration time.

## Security

Default is deny. The caller cannot choose traversal, index, depth, maximum,
child identity, partial authorization, or history behavior. Every discovered
row passes the same role and transaction-current row-policy path as an explicit
delete. Diagnostics and public outcomes are bounded and value-free. Secret
fields may be read internally only as existing predecessor evidence needed to
derive exact index removals and are redacted from errors, logs, metrics,
provenance presentation, MCP text, and outcomes.

## Standing Design Tests

- **Interface safety (AGENTS.md boundary 11):** applications and agents invoke
  one generated symbolic command with a root key and idempotency identity. They
  cannot express a generic cascade, omit one declared relationship, raise a
  bound, select child keys, opt out of references/row policy/atomicity, or ask
  for physical erasure. Unsafe variants remain unrepresentable.
- **Scale:** enumeration is one-partition, exact-indexed, one-hop, and capped at
  32 relationship entries, 256 removed rows, the existing index/record limits,
  and 16 MiB. It requires no full-state scan, whole-database memory, recursive
  traversal, co-located global authority, or physical rewrite and remains
  routable to one future aggregate/partition leader.

## Testing

- Grammar/formatter/source-span snapshots for missing, duplicate, indirect,
  cyclic, non-exhaustive, wrong-target, wrong-index, partial-prefix,
  cross-partition, cross-aggregate, excessive-entry, and excessive-bound cases.
- V1-V12 byte-frozen decoders plus V13 bundle/schema/command/role/plan-hash
  goldens, malformed-byte properties, topology checks, and explicit old-reader
  refusal.
- Pure-model differential tests for canonical enumeration, `maximum` versus
  `maximum + 1`, multiple relationships, empty sets, missing targets, unique
  index removal, delete/recreate, and stable outcome selection.
- Deterministic schedules for child create/update/delete versus cascade,
  capability/row-policy revocation, index-epoch drift, and root recreation;
  no acknowledged delete may leave an old dependent row.
- Process crash arms before/after enumeration, graph sealing, staging, fence,
  changelog publication, response, replay, checkpoint, follower apply,
  backup/restore, and restart.
- Rust, Go, TypeScript, Python, CLI, and MCP conformance for the same command,
  outcomes, authorization failures, idempotent replay, and bounds.
- Better Auth acceptance from a fresh package install: empty query at frontier
  zero, signup, account/session/token creation, full user deletion, post-delete
  absence, idempotent retry, and concurrent child-create exclusion.

## Requirements and Work Packages

- **Provisional requirements:** `DEL-001` through `DEL-012`, registered in
  `SPEC.md` only after exact acceptance.
- **Defines or blocks:** WP-625 (syntax, V13 IR, compiler proof, diagnostics and
  fixtures), WP-626 (runtime, storage, concurrency, recovery and replication),
  and WP-627 (Better Auth command, all-language and real package-arrival
  lifecycle).
- **Final evidence:** WP-627 and the final alpha qualification.

## Decision Deadline

Exact acceptance is required before any cascade syntax, AST/HIR/IR node,
format/version constant, compiler lowering, generated surface, authorization
derivation, conflict capability, runtime enumeration, durable graph, or Better
Auth deletion claim changes.
