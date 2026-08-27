# ADR-0156: Clean-Close Certificate Fast Startup

- **Status:** Accepted
- **Direction approved:** 2026-08-26
- **Exact text accepted:** Yes, 2026-08-26
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
PERF-014 continues to measure genuine unclean recovery and may not use a clean
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
  and explicit full validation remain streaming and memory-bounded rather than
  materializing population-sized plans.

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
  evidence walk, plus dirty recovery proving the complete path still runs.
- Backup, restore, retention, migration, and explicit scrub conformance.

## Requirements and Work Packages

- **Requirements:** `STO-023`, `REC-004`, and `PERF-019`
- **Defines or blocks:** WP-704 (durable certificate, startup typestate, local
  integrity closure, and crash recovery) and WP-705 (operator scrub, scale
  evidence, handbook, backup/restore, and final conformance)
- **Final evidence:** WP-705

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
