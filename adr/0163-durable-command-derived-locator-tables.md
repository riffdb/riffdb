# ADR-0163: Durable Command-Derived Locator Tables

- **Status:** Proposed
- **Direction approved:** 2026-08-27 (separate locator tables, with the
  registry-chain migration and its one-time revalidation cost accepted)
- **Exact text accepted:** Not yet — maintainer acceptance wording pending
- **Decision deadline:** Before any implementation adds a durable table, a
  `RegistryMigration` variant, or a record-registry digest link
- **Requires:** ADR-0004, ADR-0006, ADR-0019, ADR-0085, ADR-0099, ADR-0156,
  and ADR-0157
- **Amends if accepted:** ADR-0156 section 5 by discharging its deferred
  fail-closed local-check obligation for the command-derived locator kinds.
  Deliberately amends nothing in ADR-0085; preserving that record's derivation
  unchanged is the reason for the placement chosen below.

## Context

A command's outcome, provenance record, events and service audits are stored
inside its command segment row in `COMMITS` (ADR-0099). The tables that name
those facts by their own keys therefore hold no physical rows at all. Measured
on a 359,318-command database:

```
idempotency 0    provenance 0    events 0    event_routes 0    outbox 0
commits 6382     entities 359318
```

A process-local transient index (`CommandDerivedIndexes`) maps each lookup key
to its owning segment. It is rebuilt by walking the whole `COMMITS` table,
decoding every segment and re-deriving every manifest key. When that index is
`Dormant` — which is exactly the state ADR-0156 bounded startup leaves it in —
the accessors `command_derived_member`, `indexed_command_at` and
`indexed_command_audit` return `Ok(None)`.

That index is not a cache. It is the only durable-to-logical locator these keys
have. Six read paths consequently answer **absent for data that is durably
present**:

| Path | Index kind | Dormant behaviour |
| --- | --- | --- |
| `read_stored_outcome` (`reads.rs`) | Idempotency | `Ok(None)` for a committed outcome |
| `command_outcome_from_write_indexes` (`application.rs`) | Idempotency | write path: a committed command reads as never-admitted |
| `read_provenance` (`reads.rs`) | Provenance | `Ok(None)`; its locator branch is unreachable at zero rows |
| `provenance_exists` (`application.rs`) | Provenance | uniqueness reservation reports the id free |
| `service_audit_sequences_for` (`store.rs`) | AuditRequest | returns an empty sequence list |
| `read_durable_event` (`reads.rs`) | (`indexed_command_at`) | `Ok(None)` for a durable event |

The second is the most serious defect in this area: `read_admission` treats
`Ok(None)` as "never admitted", so a durably committed command can be executed
again. ADR-0156 section 5 already forbids this class outright — corruption or
cold state "must not be converted into absence, a business outcome, partial
output, skipped work, or automatic repair" — and imposes the obligation this
record discharges:

> If an existing path relied exclusively on startup to establish one of those
> facts, WP-704 must add the equivalent fail-closed local check before that
> path becomes eligible for clean fast startup.

That obligation was never discharged. Bounded startup satisfied section 5 only
because a readiness-path caller (outbox recovery) rebuilt the index
incidentally, through `ensure_transient_indexes_ready`, at 96% of a bounded
start's wall clock. The correctness of every cold-cache point read rested on
that accident.

The sixth defect needs no durable row and is already fixed separately: an event
id carries its commit sequence, so `COMMITS` is directly addressable and
`read_durable_event` gained the same `command_member_at_access` fallback
`read_commit` already had. It is the only one of the six whose key was
sequence-addressable.

## Decision

### 1. Add three durable locator tables

Add `idempotency_locators`, `provenance_locators` and
`audit_by_request_locators`. Each row maps a derived key to the commit sequence
of the command that owns it, using the existing `StoredCommandLocatorV1` record
already present in the readable record registry and already accepted by
`read_stored_outcome`, `read_provenance`, startup validation and validated-prefix
checkpoint verification. No new durable message type is introduced.

`audit_by_request_locators` carries the locator rather than the existing
`StoredServiceAuditRequestIndexV1`, so that all three tables have one shape and
one validation rule.

### 2. Do not put these rows in the tables they index

The obvious placement — locator rows inside `IDEMPOTENCY`, `PROVENANCE` and
`AUDIT_BY_REQUEST` — is rejected. It was implemented and reverted.

ADR-0085's O(1) validated-prefix checkpoint counts are derived from those
tables' raw row counts:

```rust
let idempotency = table_row_count(transaction, IDEMPOTENCY)?;
let Some(terminal_outcomes) = idempotency.checked_sub(execution_failed_rows) else { ... };
idempotency_count: below_s(terminal_outcomes),
audit_by_request_count: below_bound(table_row_count(transaction, AUDIT_BY_REQUEST)?),
```

with the stated contract that "`IDEMPOTENCY` stores both terminal classes and
`idempotency_count` admits only `StoredOutcome` rows". Locator rows are a third
class. They inflate the counts, so checkpoint counts silently become wrong. The
86-arm recovery matrix detects this immediately, including the named invariant
`command_derived_checkpoint_tables_are_physically_empty`, which asserts
`audit_by_request`, `event_routes` and `idempotency` are physically empty.

The physical emptiness of those tables is load-bearing for ADR-0085. Separate
tables keep both that invariant and ADR-0085's derivation literally true, with
no new census counter.

The alternative — teaching the derivation to subtract a per-table locator census
— was rejected on failure mode rather than effort. Its cost is permanent and
silent: a mis-seeded census yields a wrong checkpoint with no symptom. This
record's cost is loud, one-time, plannable and self-limiting. Overloading one
table with two meanings is also the same error that made the transient index
both a cache and the sole locator, which is the defect being fixed.

### 3. Fail closed, never absent

A locator that is missing where the reader expected one, does not decode, or
names a segment that does not contain the key MUST fail as `CorruptData`. It
MUST NOT be reported as absence, an empty page, a business outcome, or a
retryable "not admitted". Only a genuinely absent key is absence.

This is the entire point of the record. Reintroducing absent-for-present while
fixing absent-for-present is the worst available outcome, so each table carries
tests for all three malformations.

### 4. Write locators with the command, not after it

Locator rows are written in the same redb transaction and the same journal frame
as their command segment, so they are atomic with it and add no additional
fsync. They use an insert that does not pre-read: each key is proven new before
the write — an idempotency identity key by the admission reservation, a
provenance id by its uniqueness reservation, and an audit-by-request key by a
freshly allocated administration sequence that cannot collide. Paying a read per
row to re-establish that measured 12–16% of seed throughput by itself.

### 5. Migrate through the existing record-registry chain

Adding tables in this engine is done through the authoritative record-registry
digest chain, not a `StorageFormatVersion` bump. This record adds one link:

- a new `RegistryMigration` variant (the twentieth);
- a new digest constant, with today's current digest becoming the new
  `PRE_*_REGISTRY_DIGEST`;
- an idempotent `install_*` step paired with `publish_record_registry`;
- the cascading predecessor arms extended at all three open sites;
- `TABLE_NAMES` 39 to 42 with its architecture test;
- compatibility fixtures.

The three tables are additive and empty on install. Existing databases remain
readable; commands written after the migration carry locators, and commands
written before them remain resolvable through the transient index.

## Consequences

- The five remaining absent-for-present defects become fail-closed or correct,
  discharging ADR-0156 section 5's obligation for these kinds.
- **Every existing clean-close certificate is invalidated, so each existing
  database takes exactly one complete-validation startup after upgrade.** The
  certificate binds `META_RECORD_REGISTRY`, and the migration changes that
  digest. At production scale that one start is the twenty-minute pass this work
  exists to remove. It is one-time, per database, and already the documented
  behaviour for registry changes ("Restore and incompatible format/registry
  changes invalidate clean-close evidence"). It is accepted deliberately, and it
  is the main operational cost of this record. The startup reason code names it
  (`binding_mismatch` or `bounded_roots_unavailable`), so it is diagnosable
  rather than mysterious.
- **Write cost, measured.** On an idle N1 (8 vCPU Xeon, load 0.23, ext4), four
  interleaved reps per arm, 115,690 commands, one `IDEMPOTENCY` locator row per
  command: baseline mean 2492.2 ops/s (2465.6–2523.7), locators mean 2338.6
  ops/s (2330.7–2351.3) — **−6.16% seed throughput**, with non-overlapping
  groups and within-arm spread of 2.3% and 0.9%. This is measured for **one**
  locator table; three tables will cost more and that extrapolation is **not**
  measured.
- **Bytes and fsyncs, measured** and unchanged by the placement: +135.9 bytes of
  engine storage and +15.6 bytes of B-tree metadata per command per table
  (+4.96% database size for one table), +180.0 bytes of journal frame per
  command (+5.78%), and **no additional fsync** — 6,396 versus 6,409 mean
  physical flushes across four runs per arm, inside both groups' spread, because
  the rows join their segment's frame.
- **Readiness, predicted, to be checked on landing.** With cold-cache reads
  correct, outbox normalization may be skipped on the readiness path when
  bounded startup proved no in-flight `Delivering` entry (ADR-0156 Amendment 3).
  `outbox_recovery` should fall from 33.32 s to under 50 ms at 115,690 retained
  commands and from 102.45 s to under 50 ms at 359,318; `process_to_ready`
  should fall from 34.75 s to about 1.43 s and from 106.68 s to about 4.23 s.
- **Readiness remains LINEAR in retained commands.** The coefficient falls from
  about 300 to about 12 microseconds per command, roughly 25x, but does not
  reach independence: the residual is dominated by `store_open`, independently
  measured at about 11.6 microseconds per command. This narrows, and does not
  satisfy, ADR-0156's consequence that "Clean restart no longer scales with live
  entity/index population or retained history".
- The real-daemon tripwire `assert_readiness_path_rebuild_census(..., 1)` is
  pinned exactly and MUST be changed to 0 as part of this work. It fails in both
  directions by design, so the change is a deliberate act rather than a silent
  one.
- `EVENT_ROUTES` and `OUTBOX` gain no locator table. Their manifest kinds have
  no index read call sites, so a locator there would buy only rebuild removal,
  and both tables are counted by ADR-0085's derivation, which this record
  declines to disturb.

## Known separate defects, deliberately not fixed here

- **`ProtectedEventReplayReader`** (`consumer.rs`) opens `EVENT_ROUTES` and
  `EVENTS` directly from a raw write transaction and returns an empty page,
  independent of transient index state. It was considered for inclusion and
  excluded because it is **not** the same shape: it never consults the index, so
  no locator this record adds can help it. A route is keyed by partition hash
  plus event id, and a partition hash is not derivable from a commit sequence,
  so fixing it requires making `EVENT_ROUTES` non-empty — a fourth locator table
  that would re-open exactly the ADR-0085 counting question this record avoids,
  for `event_routes_count`. It needs its own record.
- **`engine_repaired_at_open`** unconditionally declines the bounded path when
  redb reports a repair, without distinguishing an in-memory allocator rebuild
  over unchanged roots from a rolled-back partial commit. This was reviewed and
  **deliberately left as-is**: measurement showed repairs do not occur after a
  clean close, so the condition only fires after an unclean shutdown, which is
  precisely when complete validation is wanted. Narrowing it would trade a
  fail-closed default for a case never observed to matter. The reason code that
  makes it diagnosable is retained. Recorded here so it is not later
  rediscovered as an oversight.

## Relationship to concurrent columnar records

ADR-0160 and ADR-0162 also list ADR-0085 as required, but they do not interact
with this record and their migration story is not shared with it. Both operate
on rebuildable **derived provider state** registered under ADR-0124: ADR-0160
states that "Authoritative storage, events, changelog, backup identity ... remain
byte-exact", and ADR-0162 that "Existing authoritative ... bytes remain
unchanged". Their ADR-0085 dependency is frontier and checkpoint survival, not
the count derivation. This record is the only proposal that touches authoritative
durable tables and the authoritative record-registry digest chain, and it is the
only one whose upgrade invalidates clean-close certificates.

## Options Considered

1. **Separate locator tables:** selected. Keeps ADR-0085's derivation and the
   physical-emptiness invariant literally true; costs a registry-chain migration
   and one complete-validation start per existing database.
2. **Locator rows in the indexed tables plus a per-table census:** rejected.
   Cheaper to build and needs no migration, but amends ADR-0085's derivation and
   the emptiness invariant, and its failure mode is a silently wrong checkpoint.
3. **Warm the transient index on demand at the read sites:** rejected as the
   primary fix. It needs no durable change, but converts a startup stall into a
   first-read stall of the same magnitude and leaves the locator absence in the
   durable format.
4. **Full physical records rather than locators:** rejected. It duplicates the
   segment payload the fused capsule layout exists to avoid.

## Testing

- Per table: a missing locator, an undecodable locator, and a locator naming a
  segment that does not contain the key each produce `CorruptData`, never
  absence.
- Admission: commit a command, drop the transient index, re-admit the same
  idempotency key, and assert it is recognised as already admitted.
- The 86-arm recovery matrix stays green, including
  `command_derived_checkpoint_tables_are_physically_empty`.
- Readiness is measured at 115,690 and 359,318 retained commands against the
  predictions above, with a miss reported as a miss.

## Requirements and Work Packages

- **Requirements:** `STO-023`, `REC-004`, and `PERF-019`
- **Defines or blocks:** the ADR-0156 section 5 local-check obligation and the
  readiness-path outbox skip permitted by ADR-0156 Amendment 3
