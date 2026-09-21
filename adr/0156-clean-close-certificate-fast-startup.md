# ADR-0156: Clean-Close Certificate Fast Startup

- **Status:** Accepted
- **Obligations:**
  - `OBL-0156-1` WP-704 moves every population fact skipped by bounded clean
    startup onto the fail-closed operational path that first uses it, while
    preserving a zero-rebuild readiness path.
    Proof: `assert_readiness_path_rebuild_census`
- **Direction approved:** 2026-08-26
- **Exact text accepted:** Yes, 2026-08-26
- **WP-705/WP-760 ownership amendment accepted:** 2026-09-02 (maintainer)
- **WP-705 dirty-evidence boundary direction approved:** 2026-09-02
- **WP-705 dirty-evidence boundary exact text accepted:** 2026-09-02 (maintainer)
- **WP-705 atomic crash-evidence interpretation accepted:** 2026-09-02 (maintainer)
- **WP-705 repair-evidence split accepted:** 2026-09-02 (maintainer)
- **PERF-019 lifecycle-evidence amendment accepted:** 2026-09-03 (maintainer)
- **Accepted:** 2026-08-26
- **Acceptance reference:** Maintainer exact-text acceptance in the current
  Codex session for upstream commit `c5c73858`
- **Decision deadline:** Before WP-704 changes startup validation scope or adds
  the durable clean-close record
- **Requires:** ADR-0019, ADR-0050, ADR-0061, ADR-0073, ADR-0085, ADR-0101,
  ADR-0103, ADR-0104, and ADR-0112
- **Amends if accepted:** ADR-0019's unconditional complete-pass rule,
  ADR-0073's rejection of clean-shutdown markers, ADR-0085's rejection of a
  validation-free clean fast path, and `STO-012`
- **Defines or blocks:** WP-704 and WP-705

This record is authoritative for WP-704 and WP-705. The exact durable field/tag
amendment it requires remains a separate implementation blocker.

## Context

RiffDB currently performs complete storage-structural and catalog-semantic
validation before every production readiness transition, regardless of whether
the prior process shut down gracefully. ADR-0019 deliberately omitted durable
clean/unclean state. ADR-0073 reaffirmed complete validation while making its
walk linear. ADR-0085 later added a proof-carrying validated-prefix checkpoint,
but explicitly retained full passes over current entities, indexes, catalog,
and capabilities.

That policy catches latent corruption before any application operation can
observe the database, but clean startup remains proportional to live state. A
production-scale database with approximately 1.1 million entities demonstrated
both consequences: the historical evidence locator index exceeded its 512 MiB
heap ceiling, and a bounded implementation still had to inspect millions of
live entity and index rows. A long soak or ordinary database growth therefore
turns process restart into a full offline scrub.

Conventional local databases distinguish recovery from exhaustive integrity
verification. A clean close proves that all acknowledged writes were drained
through the engine's durability boundary and no writer remained active; a
missing or invalid clean record requires recovery and stronger inspection.
Latent media corruption is detected by engine checksums, record decoding, and
validation when affected data is accessed, with an explicit scrub operation
available when complete assurance is required. They do not normally reread and
semantically cross-check every live row before accepting connections.

The accepted every-startup rule provides a stronger guarantee, but its cost is
not justified for RiffDB's conventional single-host alpha trust model. In
particular, a cryptographic signature does not solve this cheaply: a key stored
beside the database provides no adversarial offline-write boundary, while a
signature over table contents still requires either a full rescan or
transactionally maintained authenticated roots. External signing keys,
Merkleized authoritative tables, and hostile offline mutation are separate
security and storage designs.

This ADR intentionally weakens the timing of exhaustive corruption discovery:
a clean reopen may discover corruption only when the affected row is read,
mutated, rebuilt, delivered, or explicitly scrubbed. It does not weaken atomic
acknowledgement, crash recovery, idempotency, authorization, schema safety, or
fail-closed handling once malformed evidence is encountered.

## Proposed Decision

### 1. Adopt the conventional local-database trust model

RiffDB will distinguish a verified clean prior close from every other startup.
The clean fast path protects against process crashes, incomplete shutdown,
torn or malformed clean-close records, stale durable frontiers, wrong database
identity, and incompatible durable-format state. It does **not** claim to prove
the absence of adversarial offline file rewriting or latent corruption in
unread database pages.

No public request, application contract, operator flag, environment variable,
or transport option may select the fast path or suppress validation. Startup
selects it solely from engine-open evidence and one private durable record. A
missing, malformed, stale, incompatible, or contradictory record selects the
existing complete fail-closed validation path.

### 2. Add one engine-atomic clean-close certificate

Storage format gains one versioned private meta record,
`clean_close_certificate/v1`. Its canonical checksummed envelope binds at
least:

- `DatabaseId` and history incarnation;
- storage-format version and readable record-registry digest;
- application and administration durable frontiers and allocator states;
- the published standard-profile frontier, redb checkpoint frontier, journal
  generation, and proof that no published journal suffix remains unapplied;
- the active catalog identity, or the exact bootstrap-required empty-catalog
  state;
- a monotonically checked certificate generation; and
- a domain-separated hash over all preceding certificate fields.

The record is a certificate of lifecycle ordering, not a cryptographic
signature over database contents. It introduces no signing key, secret,
cryptographic dependency, public identity, operator credential, or remote trust
claim. Existing envelope checksum and canonical hash primitives protect the
record against malformed or accidental byte changes; they do not authenticate
an attacker with database-file write access.

The exact Protobuf fields, tags, size ceiling, registry transition, and
compatibility fixture must be accepted before WP-704 freezes the durable record.
Older binaries must reject the successor registry digest normally. Existing
databases without the record take the complete-validation path and require no
eager rewrite.

### 3. Freeze the clean-shutdown ordering ceremony

A graceful shutdown may write the certificate only after all of the following
have completed successfully:

1. public admission is closed and no new operation can enter;
2. all application, administration, outbox, projection-control, migration, and
   maintenance writers are stopped or drained through their existing durable
   boundaries;
3. every acknowledged standard-profile journal frame is applied to redb, the
   published and checkpoint frontiers agree, and journal reclamation state is
   valid;
4. any retained validated-prefix checkpoint is left internally consistent, but
   no population-wide checkpoint refresh or complete validation is required to
   make this shutdown clean;
5. allocator, catalog, bootstrap, history-incarnation, registry, and database
   identities used by the certificate are reread from the final authoritative
   transaction; and
6. the certificate is committed with Immediate durability as the final
   authoritative database mutation of the process generation.

No authoritative or derived database write may follow the certificate commit.
A failure, cancellation, uncertainty, timeout, panic, forced termination, or
post-certificate write attempt leaves the database without usable clean-close
evidence. Failure to write the certificate does not roll back previously
acknowledged work; it makes the next startup perform complete validation.

Shutdown must not scan all entities, indexes, or retained history merely to
produce the certificate. Moving the startup scrub to shutdown is not an
accepted implementation.

A process opened through the clean fast path has not earned a new
validated-prefix proof and therefore must not refresh that checkpoint merely by
closing cleanly. The prior checkpoint may remain available for a later dirty
fallback if its bindings still verify. A startup that used complete validation
may continue to refresh it at the accepted write points. If no usable prefix
checkpoint exists after a later dirty crash, complete validation starts from
the retained beginning as it does today.

### 4. Consume the certificate before activating writers

After engine open, startup evaluates the certificate while all operational
ports and writers remain dormant. The fast path is eligible only when:

- the engine reports no repair or incomplete transaction requiring recovery;
- the certificate envelope, canonical bytes, field hash, database identity,
  incarnation, format, and registry bindings are exact;
- the current allocator, catalog, bootstrap, journal generation, published
  frontier, and redb checkpoint frontier exactly match its final-close values;
- no migration, restore, retention, backup-replacement, or maintenance state
  requires a stronger gate; and
- all small bounded startup roots named below validate successfully.

Immediately before operational activation, startup atomically consumes the
certificate or replaces it with a private active/dirty generation state. That
transition must become durable before any writer, delivery worker, projection
worker, maintenance operation, or public mutation can activate. A crash before
the transition cannot have performed a database write and may reuse the clean
certificate. A crash after it makes the next startup dirty and therefore
ineligible for the fast path.

Exclusive process ownership and the existing single-writer boundaries remain
mandatory. A copied live database, a certificate copied without its matching
database state, or a second process cannot create clean eligibility.

### 5. Keep a bounded readiness root; defer population-wide inspection

The clean fast path still validates before readiness:

- engine and durable-format identity;
- permanent database identity and history incarnation;
- allocator/frontier consistency named by the certificate;
- journal/checkpoint emptiness and generation consistency;
- bootstrap lifecycle;
- active catalog pointer, the complete bounded active-to-genesis catalog chain,
  canonical bundle hashes, currently executable plans and modules, and their
  process-local catalog proofs;
- capability key configuration and every authority record that must be loaded
  to construct the initial authorization view; and
- any other bounded root whose corruption could grant authority or select an
  unsafe decoder before an ordinary row access occurs.

The fast path does not enumerate every entity, secondary-index entry,
historical plan reference, idempotency record, event, outbox row, provenance
row, audit row, projection row, or other population-sized table before
readiness. It does not construct the complete historical evidence stream merely
to discard it.

Every operational path that reads or mutates a row must continue to validate
its versioned envelope, checksum, canonical encoding, physical-key identity,
schema ownership, bounds, and locally required reciprocal evidence before the
row can influence authorization, policy, execution, output, delivery,
projection, or mutation. If an existing path relied exclusively on startup to
establish one of those facts, WP-704 must add the equivalent fail-closed local
check before that path becomes eligible for clean fast startup. Corruption
discovered after readiness remains an internal integrity incident; it must not
be converted into absence, a business outcome, partial output, skipped work, or
automatic repair.

### 6. Preserve complete validation for dirty and explicit verification paths

The existing exact-end storage-structural and catalog-semantic validation path
remains authoritative and is required when clean eligibility is absent or
fails. It retains the historical evidence sequence, collector rejections,
pagination, checkpoint verification, recovery matrix, and fail-closed bounds.
The high-cardinality evidence cursor must remain memory-bounded because dirty
recovery and explicit verification still require it.

Offline backup verification, restore, format migration, retention maintenance,
and an explicit operator integrity-scrub operation must be able to request the
complete path without forging dirty process state. Restore and incompatible
format/registry changes invalidate clean-close evidence. A successful scrub
does not itself create a clean certificate while an operational writer may
still run.

This ADR adds no public option to acknowledge unsafe writes, skip durability,
or force readiness after a failed certificate or integrity check.

### 7. Make the changed assurance visible and measurable

Health and safe operator diagnostics may report only the closed startup mode
`clean_certificate` or `complete_validation`, plus bounded timing and incident
identifiers. They must not expose hashes, frontiers, table counts, paths, keys,
or validation details through public errors, MCP text, logs, or metrics.

The handbook must state plainly that clean startup is not a full integrity
scrub and that latent corruption may be found on access. It must document the
complete offline verification operation, when dirty startup is selected, and
the recovery implications of forced termination.

PERF-013 evidence will report clean-certificate and complete-validation curves
separately. The million-row reproduction must prove that a clean close reopens
without a population-sized evidence plan or population-sized validation walk.
WP-705 must additionally exercise the unchanged complete-validation path after
a genuine writer-process kill at the 65,536-row checkpoint, including truthful
engine-repair observation, recovery, idempotency, atomic either-old-or-new
resolution, and recovery-only no-authority-advance assertions. A separate
dedicated `PERF-014` process fixture must force a natural engine repair boundary,
observe the real repair callback, and compare against equivalent clean state;
the 65,536-row daemon run must not claim repair when redb does not require it.
Its attempted
production-scale complete-path run must preserve the fail-closed
`LimitExceeded` result at the historical-evidence 512 MiB ceiling as a known
ceiling and handoff, not relabel it as passing evidence. Accepted ADR-0182 and
WP-760 exclusively own production-scale dirty readiness without a population
walk and under the compiled 64 MiB recovery-owned-state ceiling. PERF-014
continues to measure genuine unclean recovery and may not use a clean
certificate. PostgreSQL process startup is not a direct comparator for either
RiffDB validation mode unless the harness owns equivalent server lifecycle and
integrity work.

## Options Considered

1. **Engine-atomic conventional clean-close certificate:** selected. It makes
   clean restart proportional to bounded roots and engine open while retaining
   complete validation after every uncertain lifecycle.
2. **Keep complete validation on every startup:** rejected for clean startup.
   Its population-proportional cost and heap hazards do not justify eagerly
   rediscovering every latent defect before ordinary access.
3. **Operator-controlled skip-validation flag:** rejected. It would let an
   application or operator silently opt out of a guarantee and could turn an
   actual dirty or corrupt state into readiness.
4. **Cryptographic signature with a colocated key:** rejected. It adds key
   management without protecting against a principal that can rewrite the
   database and the key or signed certificate together.
5. **External signing key plus authenticated Merkle roots:** deferred. It is a
   valid hostile-offline-write design but requires new cryptographic trust,
   transactionally maintained roots, proof formats, backup semantics, and
   critical dependencies beyond the conventional local-database model.
6. **Full scan during graceful shutdown:** rejected. It moves rather than
   removes population-proportional downtime and makes clean eligibility depend
   on database size.

## Consequences

- Clean restart no longer scales with live entity/index population or retained
  history beyond bounded catalog and authority roots.
- Dirty, uncertain, migrated, restored, or contradictory state retains the
  complete current validation and recovery path.
- Latent corruption outside bounded readiness roots may be detected after
  readiness when affected data is accessed or during an explicit scrub.
- The durable format gains one private versioned lifecycle record and registry
  transition; older databases remain readable through the complete path.
- Shutdown gains a final Immediate commit and must coordinate every writer that
  can invalidate the certificate.
- The memory-bounded historical cursor remains required rather than becoming
  dead code.
- Authenticated hostile-offline-write detection remains explicitly deferred.

## Compatibility

There is no application API, command, query, MCP, gRPC, application-facing CLI
data operation, contract IR, plan, entity key, index key, outcome, event,
provenance, or public wire-format change. The operator CLI gains one additive
offline integrity-scrub operation. The private durable metadata registry and
storage format gain the versioned clean-close record. Existing databases and
backups without it take complete validation. Restore, registry migration, and
history-incarnation changes invalidate it rather than reinterpret it.

The startup typestate and catalog validation interfaces may gain sealed internal
variants distinguishing clean-certificate proof from complete exact-end proof.
Neither variant is publicly constructible, caller-selectable, serializable, or
accepted by an operational component without the shared server startup gate.

## Security

The fast path trusts engine atomicity, durable write ordering, exclusive process
ownership, existing checksum/hash primitives, and the local host/storage trust
boundary. It does not trust caller input or a bare boolean marker. Certificate
contradiction always falls back to complete validation; an integrity failure in
a bounded root or accessed row fails closed.

The accepted assurance change is that unrelated latent corruption or offline
file mutation may remain undiscovered until access or explicit scrub. The
certificate is not an authenticity claim against an attacker with database-file
write access. No secret key is introduced, and no certificate detail is exposed
to application principals or agents.

Security-sensitive bounded roots remain eagerly checked so clean startup cannot
activate with a forged capability view, substituted active catalog, unsafe
decoder selection, mismatched history incarnation, or stale published
frontier. Tests must prove that every row class skipped at clean startup still
fails closed before corrupt bytes can influence an operational result.

## Standing Design Tests

- **Interface safety:** The fast path is private and automatic. No application,
  agent, transport, configuration, or operator request can select it, forge its
  typestate, force readiness, or opt out of durability, authorization,
  idempotency, schema validation, row-local integrity, or dirty recovery.
- **Scale:** Clean eligibility validation is bounded independently of entity,
  index, history, event, audit, provenance, projection, and idempotency row
  counts. Shutdown writes one bounded record without a population scan. Dirty
  and explicit full validation remain streaming and memory-bounded by refusing
  with `LimitExceeded` rather than exceeding a compiled ceiling. WP-760, not
  WP-705, owns production-scale dirty readiness without a population walk under
  ADR-0182's separate 64 MiB recovery-owned-state ceiling.

## Testing

- Golden durable fixtures and old-reader/registry rejection for the certificate.
- Deterministic writer/shutdown schedules proving the certificate is the final
  mutation and is consumed durably before any writer activates.
- Process crashes before and after every drain, checkpoint, certificate commit,
  certificate consumption, readiness, first write, and final shutdown boundary.
- Negative certificate cases for malformed bytes, wrong database/incarnation,
  stale format/registry/catalog/frontier/generation, nonempty journal, engine
  repair, copied state, migration, restore, and write-after-certificate.
- Per-table corruption tests proving clean startup either rejects bounded-root
  corruption or the first affected operational access fails closed without
  partial output or mutation.
- The unchanged complete-validation golden evidence stream and 86-arm recovery
  matrix.
- Production-scale clean reopen proving bounded heap and no population-wide
  evidence walk; genuine 65,536-row kill/reopen evidence proving the unchanged
  complete path, engine repair, recovery, and idempotency remain exact; and a
  production-scale complete-path attempt preserving its fail-closed
  `LimitExceeded` result at the historical-evidence 512 MiB ceiling.
- Backup, restore, retention, migration, and explicit scrub conformance.

## Requirements and Work Packages

- **Requirements:** `STO-023`, `REC-004`, and `PERF-019`
- **Defines or blocks:** WP-704 (durable certificate, startup typestate, local
  integrity closure, and crash recovery) and WP-705 (sealed internal scrub
  primitive, scale evidence, handbook, backup/restore, and final conformance).
  Accepted ADR-0182 and WP-760 solely own the authorized public maintenance
  service, `riffdb storage scrub`, and its receipt beneath `.maintenance`.
- **Final evidence:** WP-705 owns production-scale clean evidence, genuine
  65,536-row complete-validation recovery and idempotency evidence, and the
  fail-closed production-scale complete-path ceiling receipt. It hands that
  ceiling and the sealed internal scrub primitive to WP-760. WP-760 exclusively
  owns production-scale dirty readiness without a population walk and under
  ADR-0182's 64 MiB recovery-owned-state ceiling. WP-578 separately owns the
  uninterrupted `END-009` run, consumes the completed WP-705 and WP-760
  handoffs, and is not a WP-705 closure condition.

## Decision Deadline

Exact human acceptance is required before any implementation changes startup
validation scope, adds the durable record, consumes a clean certificate, or
allows operational readiness without the complete historical evidence stream.

## Amendment 3 — bounded delivery-state assertion and the deferred locator obligation (Accepted 2026-08-27)

ADR-0156 §5 states that the fast path "does not enumerate every entity,
secondary-index entry, historical plan reference, idempotency record, event,
outbox row, provenance row, audit row, projection row, or other
population-sized table before readiness." That claim is not currently met, and
this amendment separates the part now satisfied from the part still owed.

- **Assertion (in effect).** Bounded startup must positively assert that no
  in-flight `Delivering` outbox entry exists before admitting the certificate.
  `Delivering` is written only by a delivery worker holding a lease, and an
  event with no `OUTBOX_STATUS` row canonically means never-attempted `Pending`
  (SPEC 8.6), so `Delivering` is a subset of `OUTBOX_STATUS` rows. The probe
  reads that table only: never `COMMITS`, the undelivered set, or the transient
  population indexes.
- **Bound.** Row count comes from engine metadata; an empty table proves
  absence; a non-empty table is decoded to a fixed bound (4096 rows), above
  which the probe reports ignorance. The probe is never population-proportional.
- **Three outcomes.** Proof of absence admits. A positive observation
  contradicts the certificate about state the certificate does not cover and
  takes the complete path under §6, with the closed reason
  `outbox_delivering_observed`. Ignorance admits the certificate but grants no
  permission: it is not recorded as proof, and normalization still runs.
- **No skip is authorised by this amendment.** Outbox normalization remains on
  the readiness path.
- **Outstanding obligation (§5, undischarged).** In the current durable layout
  IDEMPOTENCY, PROVENANCE, EVENTS, EVENT_ROUTES and OUTBOX hold no physical
  rows; the command-derived index is the sole locator into a command segment.
  With it dormant, `read_stored_outcome` answers absent for a durably committed
  outcome, which §5 forbids. Until a separate accepted record supplies either
  durable locator rows or an equivalent fail-closed local check at the read
  sites, the fast path MUST continue to build that index before readiness, and
  §5's enumeration claim is read as an objective, not a description.
- **Consequence.** Clean restart remains proportional to retained commands
  through the locator-index build. This narrows, and does not remove, the
  Consequences bullet "Clean restart no longer scales with live entity/index
  population or retained history beyond bounded catalog and authority roots."
  The existing bullet "Latent corruption outside bounded readiness roots may be
  detected after readiness" is the precedent an after-readiness locator build
  would extend; this amendment does not authorise it.
- **Tests.** A planted in-flight `Delivering` entry on a certificate-bearing
  database takes the complete path with the named reason; the probe never
  triggers a population rebuild; and a real-daemon restart pins the readiness
  path's rebuild count so both a new accidental rebuild and the eventual fix
  must change it deliberately.

## Amendment 4 — WP-705 complete-path evidence boundary

WP-705 closes ADR-0156's implementation and compatibility work with four
distinct evidence results:

1. a production-scale clean close and reopen proving bounded heap, zero
   population-table evidence walks, and separately attributed open, readiness,
   drain, and final-certificate stages;
2. a genuine writer-process kill and reopen at exactly the 65,536-row checkpoint
   proving that the unchanged complete-validation path reaches exact end,
   preserves the 86-arm recovery matrix, remains idempotent, and resolves the
   in-flight probe atomically as either wholly absent or wholly committed. A
   transaction durable before SIGKILL is not required to disappear. The database
   must exactly match the predeclared equivalently seeded clean twin for the
   observed atomic outcome, the dirty/clean wall-time ratio is recorded against
   that twin, the real engine-repair callback observation is reported truthfully,
   and a second restart proves recovery itself creates no additional command,
   event, outcome, provenance, audit, projection, outbox, or allocator advance;
3. a dedicated `PERF-014` process fixture using a genuine process kill at a
   deterministic real-engine crash boundary, proving that the real repair
   callback is invoked, the recovered state equals its equivalently seeded clean
   peer, and the repair/clean wall-time ratio is recorded without simulating or
   relabelling repair; and
4. a production-scale attempt of that unchanged complete path which records the
   typed fail-closed `LimitExceeded` result at the historical-evidence 512 MiB
   ceiling without retry, a raised ceiling, partial readiness, or a passing
   performance claim, and without application-authority or allocator advance.

The fourth result is a known-ceiling receipt and an input to WP-760. It does not
satisfy production-scale dirty readiness, waive `PERF-013`, `PERF-014`, or
`PERF-019`, or narrow `REC-001`, `REC-002`, or `REC-004`. The existing complete
path remains authoritative until WP-760 implements the separately accepted
ADR-0182 ordering. WP-760 remains downstream of WP-705 and exclusively owns the
production-scale real-daemon dirty-readiness result with zero population-table
enumeration and peak recovery-owned state under 64 MiB. Public scrub ownership
also remains solely with ADR-0182 and WP-760.

This boundary is deliberately non-circular: WP-705 hands WP-760 the sealed
internal complete-path primitive, exact 65,536-row recovery proof, and observed
production-scale ceiling; WP-705 neither calls nor waits for WP-760 behavior.
WP-760 consumes those artifacts to replace the population walk on dirty
readiness and must independently satisfy its 65,536-row and production-scale
acceptance gates before WP-578 may consume the bounded dirty path.

## Amendment 5 — exact PERF-019 lifecycle measurement contract (Accepted 2026-09-03)

WP-705 measures one Linux `riffdbd` daemon process. The harness binds every
sample to the daemon PID and the immutable process start time parsed from
`/proc/<pid>/stat`; PID reuse, a missing process, a changed start time, an
ambiguous process tree, or any measurement of a wrapper or child invalidates
the run.

The conservative heap envelope is the positive change in the daemon's Linux
`VmData` value over each closed lifecycle window and is reported as
`heap_envelope_bytes`. `VmHWM` is retained as a separate peak-RSS diagnostic;
it is not substituted for the heap envelope and cannot make a failing
`VmData` result pass. Every measurement is an unsigned byte count obtained
from one bounded `/proc/<pid>/status` sample tied to the same PID/start-time
identity.

Evidence uses the exact default-feature `riffdbd` binary built with
`cargo build --release --locked`. The source revision, `Cargo.lock`, binary,
harness, referee, and every retained raw input and report are closed by exact
cryptographic digests. A mismatched, uncommitted, missing, extra, or
non-reproducibly selected artifact invalidates the result. The dedicated
crash/repair fixture may use its separately identified test-fixture binary,
but that binary is never presented as the production clean-lifecycle binary.

The only accepted population checkpoints are exactly 65,536 entities and the
canonical production profile. Both checkpoints record the complete closed
stage registry: engine open, bounded-root validation, certificate consumption,
readiness, drain, final certificate commit, and total lifecycle wall time.
Startup and shutdown attribution may not omit, merge away, or relabel a stage.

For each checkpoint and lifecycle window, the positive `VmData` delta MUST be
at most 64 MiB. The canonical-production positive delta MUST additionally be
no more than 16 MiB above the corresponding 65,536-entity delta. Every
prohibited population table named by PERF-019 MUST report exactly zero rows
walked on the clean startup and shutdown paths. Missing counters, overflow,
negative-delta reinterpretation, unknown table names, or an incomplete table
registry fail the evidence rather than becoming zero.

The referee independently verifies the closed source/binary/harness/report
provenance, PID/start-time binding, exact build profile, checkpoint identities,
complete stage and prohibited-table registries, arithmetic, and both byte
thresholds before a receipt may pass. Human-readable summaries are derived
from those closed inputs and are not evidence by themselves.

This amendment is solely the `PERF-019` clean-lifecycle measurement contract
for WP-705. It does not claim or satisfy WP-760's production-scale dirty
readiness or 64 MiB recovery-owned-state guarantee, WP-578's uninterrupted
`END-009` endurance campaign, or any 72-hour receipt. Those remain separate
work packages and are not WP-705 closure conditions.


## Amendment 6 — V2 source clean-start eligibility (Accepted 2026-09-18)

- **Status:** Accepted
- **Accepted:** 2026-09-18
- **Acceptance reference:** Maintainer, in this Codex session on 2026-09-18,
  quoted `docs/architecture/WP-748-CLEAN-START-REVIEW.md` and answered
  “Approve exact amendment”. The exact accepted text follows.

> Qualify ADR-0156's automatic clean-start selection, ADR-0157's lifecycle
> eligibility, and PERF-019 for AuthoritativeStateCatalogV2 source databases.
> A V1 clean-close certificate does not establish complete source-admission
> evidence for such a database. V2 source startup must decline that certificate
> and perform complete ordinary validation, including retained fence evidence,
> before granting source authority. Missing or contradictory admission remains
> corruption; validation must never synthesize Active or clear a fence.
>
> V2 sources may retain the existing V1 DIRTY/CLEAN lifecycle and its exact
> transitions and bytes. CLEAN cannot select a bounded fast startup for a V2
> source. V1 database behavior and every existing hash, key, record, registry
> identity and compatibility fixture remain unchanged. Attached followers keep
> their existing lifecycle rules and gain no local lifecycle writer.
>
> V2 source startup therefore has the cost of complete validation. Documentation
> and evidence must state this limit and must not claim population-independent
> clean startup for V2. This is an explicit qualification of PERF-019 for V2,
> not permission to weaken validation or reinterpret the V1 hash. Restoring a
> bounded V2 clean-start path requires a separately accepted successor lifecycle
> design and its durable-format and crash proofs; this amendment authorizes no
> successor record and no implementation of one.

WP-748 owns the correction and the acceptance evidence in the review document.
The interface gains no bypass, authority, option, or caller-controlled selector.
The tradeoff is complete-validation startup cost for V2 sources.
