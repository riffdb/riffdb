# ADR-0019: POC Operational Metadata Deferral

- **Status:** Accepted
- **Direction approved:** 2026-07-13
- **Exact text accepted:** 2026-07-13
- **Accepted:** 2026-07-13
- **Decision deadline:** Before WP-060 freezes the semantic metadata API

The human maintainer accepted this exact deferral on 2026-07-13. This record
clarifies the POC scope of `STO-012`; it does not silently discard the broader
operational metadata as an implementation convenience.
On 2026-07-13 the maintainer also accepted the P1 readiness ownership amendment
below. It changes which package proves startup readiness, but adds none of the
deferred durable metadata.

## Context

`STO-012` lists storage format version, database and node identity, both durable
sequence allocator states, the active contract, the capability bootstrap
marker, a clean-shutdown marker, and the last successful integrity check as POC
metadata. The first set participates directly in durable compatibility,
database identity, authoritative ordering, catalog selection, or one-time
bootstrap safety. The final three items do not establish any POC command,
recovery, authorization, or compatibility guarantee that is not already proved
by unconditional startup integrity validation.

Freezing a `NodeId`, shutdown marker, or integrity-check record before WP-060
would nevertheless force decisions about semantic types, key names, durable
encoding, source ownership, timestamp meaning, update atomicity, and recovery
interpretation. Placeholder types or reserved wire fields would make those
decisions accidentally durable without improving the standalone proof.

This decision is made before WP-060 publishes its metadata interface and before
WP-065 or WP-070 freezes or persists the corresponding format. No released or
implemented RiffDB durable format contains the deferred metadata, so the POC can
adopt this narrower set without a migration.

## Decision

For the POC, the durable operational metadata required by `STO-012` is exactly:

1. the storage format version;
2. the permanent `DatabaseId`;
3. the application commit allocator state, represented by its accepted
   `Next(nonzero) | Exhausted` semantics;
4. the administration audit allocator state, represented independently by the
   same accepted `Next(nonzero) | Exhausted` semantics;
5. the active contract pointer and its accepted catalog consistency data; and
6. the singleton `capability_bootstrap/v1` marker.

The following operational metadata is deferred beyond the POC:

1. a durable `NodeId`;
2. a durable clean-shutdown marker; and
3. a persisted last-successful-integrity-check value.

Absence of these three deferred items is the only canonical POC representation.
Their absence is not missing metadata, corruption, an initialization condition,
or evidence that the previous process crashed. The POC has one durable database
identity but no separate durable node or process-incarnation identity.

Every production startup runs the complete accepted read-only authoritative
integrity and metadata-consistency validation before readiness. It does so after
both graceful and ungraceful prior termination. The source-free identity probe
and any required commit-owned initialization transition run first. WP-070 then
holds ADR-0004's exclusive `StructuralEvidenceSession`, freezes mutation,
and consumes every bounded authoritative namespace through an exact end marker.
While that same session is live, WP-050 IR-validates all historical catalog
bundles and the active relation and returns its opaque process-local
`ValidatedCatalogHistory`. Only after both exact ends may the session yield
`StructurallyOpened` dormant ports.

Only WP-130 may mechanically consume matching structural and catalog outputs and
activate the production redb/catalog/commit/auth/policy/service/gRPC graph. A
missing, truncated, mismatched, or failed output drops the open attempt and
leaves readiness false. WP-070 does not claim catalog-semantic or complete core
readiness alone, and WP-185 reuses the activated P1 graph rather than running a
second validation path. No marker, remembered integrity result, elapsed-time
rule, or process history may skip or weaken either validation. The validation
reports or fails closed on findings and performs no online repair.

Graceful shutdown remains required as process lifecycle behavior. It stops and
closes the composed components according to their accepted ownership and
durability rules, but it writes no clean-shutdown marker or other substitute
authoritative metadata. This ADR does not define whether a current-process,
non-durable health or telemetry view reports an integrity-check observation;
such a view cannot become recovery evidence.

This decision introduces no `NodeId` or deferred-metadata type, storage key,
DTO, source or clock port, Protobuf message or field, envelope registration,
fixture, public response field, work-package deliverable, dependency, or feature.
No placeholder or reserved field is added. Existing work-package dependencies,
allowed paths, requirements, acceptance commands, and gate membership are
changed only by the separately accepted P1 readiness ownership amendment; this
metadata deferral adds none.

Adding any deferred item later requires a separate accepted ADR that defines its
purpose, owner, source semantics, durable key and encoding, compatibility and
migration behavior, crash meaning, security exposure, and tests. A future record
must not retroactively make its absence corrupt in a POC-format database without
an explicit compatible migration rule.

## Options Considered

1. **Retain only authoritative POC metadata and always validate on startup:**
   Accepted. It proves the required standalone recovery behavior without
   freezing operational records that have no POC consumer.
2. **Implement every metadata item named by the earlier `STO-012` wording:**
   Rejected for the POC. It requires premature identity, time, atomicity, and
   compatibility decisions while providing no basis for skipping integrity
   validation.
3. **Reserve keys, types, or Protobuf fields but leave them unused:** Rejected.
   A reservation is still a compatibility decision and encourages dependent
   code to treat non-semantic scaffolding as a real interface.
4. **Use a shutdown marker or engine-close observation to avoid some startup
   checks:** Rejected. A marker can be stale, torn relative to external process
   events, or semantically ambiguous, and recovery correctness must not depend
   on it.

## Consequences

- WP-060 can freeze a smaller semantic metadata API with no fake operational
  values or unused source ports.
- WP-065 and WP-070 do not need speculative durable records, keys, codecs, or
  migration fixtures for the deferred items.
- Startup pays the bounded cost of complete structural and catalog-semantic
  validation every time. A graceful previous shutdown does not provide a
  fast-path exemption.
- Operators cannot use a durable node identity, durable clean/unclean history,
  or persisted integrity timestamp in the POC. Those are operational hardening
  features for a later stage.
- Database identity, allocator correctness, catalog consistency, bootstrap
  uniqueness, fail-closed readiness, and repeated-restart idempotence are
  unchanged.

## Compatibility

This record changes no current public API, contract language, IR, key encoding,
Protobuf schema, envelope registration, or persisted bytes. It narrows the
pre-implementation POC interpretation of `STO-012` before WP-060 and WP-065
freeze an interface or format. Consequently, no data migration or compatibility
fixture is required now.

A POC database containing the exact retained metadata set is complete even
though it contains no deferred keys. A later stage may add versioned operational
metadata compatibly, but must continue to open existing POC databases under an
explicit migration policy rather than interpreting their canonical absence as
corruption.

## Security

No authorization, capability, provenance, audit, or idempotency decision may
substitute a future `NodeId` for the durable `DatabaseId`. Deferral therefore
does not weaken the accepted database-bound security domains. Always running
integrity validation avoids trusting a forgeable or stale clean-shutdown hint.

The ADR adds no entropy, clock, identifier, secret, native code, cryptography,
or dependency surface. Any future exposure of node identity or integrity history
requires a separate review of information disclosure, spoofing, redaction, and
authorization.

## Testing

Existing package evidence freezes this decision without adding a new
work-package deliverable:

- WP-060 memory conformance accepts exactly the retained metadata set and treats
  absence of all three deferred records as valid.
- WP-065 schema inventory and generated-artifact checks contain no deferred
  message, field, key codec, or envelope registration.
- WP-070 startup tests prove that structural validation runs after both graceful
  close and crash, never performs authoritative repair, validates every retained
  metadata invariant through exact end, yields only dormant ports, and does not
  write a shutdown or integrity-history marker.
- WP-050 tests prove exhaustive session-bound historical catalog IR validation
  and an opaque proof bound to the same `DatabaseId` and process-local session.
- WP-130 composition tests prove only matching structural/catalog outputs
  activate the P1 graph and preserve graceful shutdown without a metadata write
  or integrity-skip path. WP-185 tests prove the P2 extension reuses that graph.
- WP-190 repeated-open and crash tests prove recovery remains idempotent and
  creates no command, event, outcome, audit, or deferred operational record.

Architecture and schema-inventory review must fail if a first-party POC crate
introduces a `NodeId`, deferred key, persistent integrity timestamp, clean-close
flag, or source port without a later accepted ADR.

## Requirements and Work Packages

- **Requirements:** `STO-012`, `REC-001`, `REC-002`
- **Defines or unblocks:** metadata interface in `WP-060`; durable schema review
  in `WP-065`; structural startup evidence in `WP-070`; catalog-semantic evidence
  in `WP-050`; P1 activation and shutdown behavior in `WP-130`; P2 reuse in
  `WP-185`; recovery evidence in `WP-190`
- **Final evidence:** `WP-190`, `WP-200`

Before acceptance, the unresolved operational metadata shape blocked WP-060
from freezing its complete semantic metadata surface and therefore blocked
WP-065 and WP-070 transitively. Acceptance unblocks those packages with the
exact retained set above. WP-010 is not reopened, and WP-185/WP-190 consume only
the already declared lifecycle and recovery interfaces. WP-130 owns complete P1
composition under the separately accepted readiness amendment.

## Accepted Decisions

Acceptance of this exact record decides that:

1. POC durable metadata contains only the six retained categories above;
2. durable node identity, clean-shutdown state, and integrity-check history are
   deferred without placeholders;
3. every startup performs exact-end storage-structural and catalog-semantic
   validation before WP-130 can activate readiness;
4. graceful shutdown writes no semantic marker; and
5. a later addition requires its own accepted compatibility and migration
   decision.

## Decision Deadline

This ADR is accepted before WP-060 freezes its metadata API. WP-060, WP-065, and
WP-070 may now proceed without inventing any deferred type, key, source, wire
field, or durable record. Implementation must stop for human review if any POC
guarantee appears to require one of those deferred items.

WP-130 must complete the composed readiness path without adding a durable ready,
clean-shutdown, or last-integrity marker. WP-185 must extend that path rather
than introducing a P2-only prerequisite for the runnable P1 server.
