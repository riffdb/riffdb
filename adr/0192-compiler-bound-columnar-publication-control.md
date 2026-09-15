---
adr: 0192
title: Schema-Bound Columnar Publication Control
status: accepted
tier: guarantee
date: "2026-09-03"
accepted: "2026-09-03"
requires: [ADR-0006, ADR-0010, ADR-0011, ADR-0017, ADR-0072, ADR-0085, ADR-0086, ADR-0111, ADR-0124, ADR-0130, ADR-0136, ADR-0160, ADR-0181, ADR-0190]
amends:
  - ADR-0011 unkeyed registry by adding the typed riffdb.columnar-projection-spec/v1 SHA-256 domain under its unchanged V1 frame
  - ADR-0190 sections 5, 9, 10, 11, 13, and 14 by replacing generic ProjectionControlOperation publication with one schema-bound columnar control family
  - ADR-0190 requirement applicability by assigning columnar control to PRJ-005 through PRJ-010 while PRJ-001 and PRJ-003 remain unchanged for aggregate ProjectionIdentity and ProjectionApplyHash
  - ADR-0136 sections 5 and 7 by replacing the vector control selector with a fresh common-control rebuild under the exact existing VectorProjectionSourceV1 identity
  - ADR-0181 sections 2 and 3 by permitting only tag-65 StoredVectorProjectionControlV1 as a temporary structurally readable, inert predecessor until mandatory WP-757 removal
supersedes: []
requirements: [PRJ-002, PRJ-004, PRJ-005, PRJ-006, PRJ-007, PRJ-008, PRJ-009, PRJ-010, OQ-017, OQ-019, OQ-020, OQ-021, OQ-022, OQ-024, OQ-053, PERF-007, PERF-008]
packages: [WP-776, WP-711, WP-757]
obligations:
  - id: OBL-0192-1
    package: WP-776
    proof: columnar_hash_domain_and_definition_semantics_are_collision_closed
    says: The dedicated domain, typed digests, exact definition-semantics bytes, source identity, name independence, purpose separation, central registry, and cross-domain golden vectors are collision closed.
  - id: OBL-0192-2
    package: WP-776
    proof: columnar_control_v1_freezes_numeric_registry_and_bounds
    says: Durable tag 67 revision 1, every Protobuf field and enum number, source/key byte, table, payload/envelope bound, fixture, registry transition, and topology identity are exact and collision-free.
  - id: OBL-0192-5
    package: WP-776
    proof: columnar_projection_symbolic_name_resolves_one_schema_bound_source
    says: The accepted symbolic projection_name remains application-facing, configuration resolves against one checked bundle, duplicate aliases refuse before control/filesystem I/O, and policy selection is deterministic from existing checked facts.
  - id: OBL-0192-6
    package: WP-776
    proof: columnar_spec_hash_change_forces_closed_rebuild
    says: Name-only change preserves source/hash, a legacy-fingerprint change creates a new scalar source, and any separately captured definition, provider, policy, vector, or replay change closes serving for a disjoint same-source rebuild.
  - id: OBL-0192-7
    package: WP-711
    proof: columnar_control_publish_requires_prepared_root_and_transaction_current_head
    says: One exact expected-control CAS selects only a completely validated root at the transaction-current authoritative head and never changes generic projection control.
  - id: OBL-0192-8
    package: WP-711
    proof: columnar_control_gate_recovers_every_cas_result_before_acknowledgement
    says: Success, mismatch, storage failure, and unknown commit each resolve the durable selection and install that exact immutable view before the source gate opens or acknowledgement occurs.
  - id: OBL-0192-9
    package: WP-711
    proof: columnar_control_recovery_refuses_corrupt_selected_v2_without_v1_fallback
    says: Every build, sync, CAS, view-install, compaction, and reclamation crash reopens the one durable selection or stays closed degraded, with no selected-V2 fallback.
  - id: OBL-0192-10
    package: WP-757
    proof: epoch_two_removes_legacy_vector_control_bridge
    says: Epoch 2 removes tag-65 source, decoder, table, fixture, manifest entry, and topology exception while permanently reserving every retired numeric identity.
review_triggers:
  - Another durable tag, Protobuf field/enum number, source/key encoding, table, hash-domain label, hash purpose tag, preimage, or bound would replace or collide with an identity frozen here.
  - Contract grammar, Bundle, Executable IR, Lineage Ledger, provider descriptor, query module, cursor, public protocol, or generic projection-control bytes would change.
  - Tag-65 StoredVectorProjectionControlV1 would be semantically decoded, written, current, selected, copied, used for retention/health/recovery, or retained after WP-757.
  - Publication could omit transaction-current head comparison, accept an unvalidated artifact, reopen its capture gate or acknowledge before exact view installation, or infer success from uncertainty.
  - A selected V2 failure could serve V1, another generation, a partial partition set, or mixed layout.
  - An application, operator, SDK, query, MCP request, or transport could supply a durable identity, semantic hash, layout, generation, root, fingerprint, fallback, validation mode, or bound.
---
# ADR-0192: Schema-Bound Columnar Publication Control

## Context

ADR-0190 requires one durable selector but names aggregate `ProjectionControlOperation::PublishCandidate`. Aggregate control identifies event-derived group/measure state and requires `ProjectionApplyHash` markers. Columnar state is entity-row scalar/vector derived state; reusing `ProjectionIdentity` or weakening PRJ-001/PRJ-003 would alias different semantics.

Production vectors have exact compiler-owned `VectorProjectionSourceV1` and tag-65 `StoredVectorProjectionControlV1`; configured scalar columnar state resolves names against a checked contract bundle but has no durable selector. A new contract declaration or stable-ID namespace would unnecessarily rotate Grammar, Bundle, Executable IR, and Lineage Ledger. Existing canonical definition and vector-source facts are sufficient if duplicate aliases refuse and a separate semantic hash binds everything the physical source omits.

ADR-0181 forbids a second readable identity before WP-757, while accepted ADR-0190 already requires a bounded V1/V2 overlap until WP-711 proves activation and WP-757 removes V1. The same ordering requires one narrower structural bridge for databases that already contain tag 65. That record is never semantic migration input: the new control rebuilds from authoritative state. Its decoder remains only so the current database can be structurally opened and validated before WP-757 deletes the record/table. ADR-0181 does not already permit this exception; this record amends it for this one named identity.

## Decision

Follower-only amendment accepted by the maintainer in session 2026-09-15 ("Approve the exact amendment"): decisions 14–16 are qualified by ADR-0178 §4 and the exact "Follower columnar views" text in SPEC §4.10. WP-747 owns the independently validated disposable V2 views; no local authoritative control write or source-artifact checksum claim is permitted. Primary behavior remains unchanged.

1. `ColumnarProjectionSourceV1` is a closed canonical union. Tag `0x01` is Scalar `{ ContractLineage, DefinitionFingerprint }`; tag `0x02` is the exact existing `VectorProjectionSourceV1 { ContractLineage, EntityTypeId, FieldId }`. Bytes are tag, `u16_be` lineage length, lineage UTF-8, then the 32-byte fingerprint or two nonzero `u32_be` IDs. Maximums are 291 Scalar and 267 Vector bytes. Full consumption and re-encoding are required. Names, paths, registration order, bundle version/hash, provider policy, selected layout, frontier, and generation are excluded. The Scalar fingerprint retains ADR-0190's frozen `LAYOUT_VERSION=1` prefix.

2. The accepted `RegisteredDefinition::compute_fingerprint` output is the definition fingerprint for both source variants and is preserved byte-for-byte; the old tag-65 bundle hash is never imported. `ColumnarDefinitionSemanticsV1` is a new private canonical document, not a contract/IR version. It is at most 1,048,576 bytes and contains: `u16_be version=1`; entity `u32_be`; `u32_be` primary-key count; key-codec version and complete-key maximum as `u32_be`; each primary-key field ID in order, `u16_be` type length/type bytes, key-component codec tag (`1` Canonical, `2` OrderedBytes), `u32_be` maximum payload, `u32_be` enum-variant count and sorted `u32_be` IDs; `u32_be` projected count and each ordered field ID plus length-framed type; organization field ID plus length-framed type; and `u32_be` sorted ANN count followed by field/threshold/recall as `u32_be`. Counts are at most 4,096 and every bound is checked before allocation.

3. Type bytes use existing `ValueTypeTag` values: Bool `1`, I64 `2`, U64 `3`, Decimal `4` plus precision/scale bytes, Money `5` plus three currency bytes, String `6` or Bytes `7` plus `u32_be` maximum, Timestamp `8`, Date `9`, UUID `10`, Enum `11` plus nonzero `u32_be` type ID, Optional `12` plus recursively encoded inner type, and Vector `15` plus nonzero `u32_be` `VectorDimension`. List `13`, Record `14`, unknown tags, nested Optional, missing payload, and trailing bytes refuse. This captures primary-key order/schema and the complete projected/org types that the preserved legacy fingerprint does not, including vector dimension.

4. ADR-0011 gains the 52nd `HashDomain::ALL` member `ColumnarProjectionSpec` with exact label `riffdb.columnar-projection-spec/v1`. It uses unchanged SHA-256 framing: 12 bytes `RIFFDB-HASH\0`, scheme `0x01`, `u16_be 34`, the 34 label bytes, `u64_be` payload length, payload. Existing members, labels, order, and typed digests do not change. `ColumnarDefinitionSemanticsHashV1` hashes payload `0x01 || u32_be semantics_length || semantics_bytes`; its payload/frame maxima are 1,048,581/1,048,638 bytes. `ColumnarProjectionSpecHashV1` hashes purpose `0x02` followed by `u16_be version=1`, `u16_be` source length/source, 32-byte legacy fingerprint, 32-byte semantics hash, `u8` descriptor count/descriptors, three replay `u64_be` values, and `u32_be` vector-extension length/bytes. Its payload/frame maxima are 5,225/5,282 bytes. Typed wrappers have no conversion. Central `ALL` uniqueness, identical-payload cross-domain inequality, purpose-tag inequality, and exact golden frames/digests are mandatory.

5. Descriptors are exact 112-byte `ProjectionProviderDescriptorV1` bytes sorted by `(kind, posture)`; Scalar has one, Vector has Exact plus Approximate only when ANN exists. Scalar bounds are candidates 100,000, output 500, measures 16, input 16,384, work 1,000,000, state/row 16,384, diagnostic 4,096, retained epochs 8,192, catch-up lag 100, lease steps 1,000; replay is 86,400 seconds, 1,073,741,824 bytes, 100,000 sequences and vector extension is empty. Vector uses `BoundedRowAdmission` and bounds 500, 499, 0, 16,384, 2,048,000, 16,384, 4,096, 8,192, 100, 1,000. Its at-most-4,636-byte extension is `u32_be` dimension, metric tag `1`/`2`/`3`, `u16_be` ordered source count plus `u32_be` IDs, stale `u64_be`, two `u16_be`-length UTF-8 model strings, and ANN tag `0` or `1` plus threshold/recall as `u32_be`; replay comes from `VectorProductionSpecV1`.

6. `PartitionAligned` is selected only if one existing checked source-wide provider descriptor already proves it, its state identity equals the registered definition, and every checked consumer route/policy binds the same organization field without row refinement. No current artifact supplies that complete source-wide proof, so WP-776 deterministically emits `BoundedRowAdmission` for every Scalar and Vector source; it never infers alignment from names or configuration. Adding a qualifying proof is a spec-hash-changing review trigger.

7. Name-only changes preserve source and spec. For Scalar, a legacy-fingerprint change creates a new source; a semantics-hash-only, descriptor, policy, or replay change keeps the source but changes spec, moves the old published pointer to unservable predecessor, and rebuilds disjointly. Vector source always retains its lineage/entity/field tuple, so any definition/vector-production/descriptor/replay change is same-source spec drift and makes its predecessor unservable. Unrelated bundle changes do neither.

8. Scalar configuration retains `name`, `entity`, `projected_fields`, and `org_scope_field`; the latter three resolve through one active checked bundle. Name is 1..=256 bytes and rejects `/`, `\\`, `.`, and `..`. Missing, duplicate-name, duplicate-source, unknown, unequal, or excessive input refuses before columnar-control or filesystem I/O with bounded guidance. The union of configured Scalar sources and every production Vector source is capped at 256; a 257th refuses the complete columnar adapter before I/O, never truncates or selects a subset, and does not make the otherwise valid at-most-4,096-vector-spec contract invalid. The application/service `projection_name` remains symbolic. No public or operator input supplies source, semantics/spec hash, layout, generation, pointer, or bound.

9. `proto/riffdb/storage/v1/columnar_projection_control_v1.proto` contains only these tag-67 revision-1 symbols. Payload is at most 8,192 bytes; the `riffdb.storage.v1.StoredColumnarProjectionControlV1` V1 envelope is at most 8,289 bytes. Bytes fields named fingerprint/hash are exactly 32 bytes; non-optional integer identities are nonzero.

```proto
message StoredColumnarScalarSourceV1 { string contract_lineage = 1; bytes definition_fingerprint = 2; reserved 3 to 15; }
message StoredColumnarVectorSourceV1 { string contract_lineage = 1; uint32 entity_type_id = 2; uint32 vector_field_id = 3; reserved 4 to 15; }
message StoredColumnarProjectionSourceV1 { oneof source { StoredColumnarScalarSourceV1 scalar = 1; StoredColumnarVectorSourceV1 vector = 2; } reserved 3 to 15; }
message StoredColumnarProjectionFrontierV1 { optional uint64 applied_through = 1; reserved 2 to 15; }
message StoredColumnarProjectionFailureV1 { ColumnarProjectionFailureTargetV1 target = 1; ColumnarProjectionFailureReasonV1 reason = 2; optional uint64 generation = 3; reserved 4 to 15; }
message StoredColumnarProjectionGenerationV1 {
  uint64 generation = 1; ColumnarProjectionLayoutV1 layout = 2; StoredColumnarProjectionFrontierV1 frontier = 3;
  uint64 history_incarnation = 4; optional uint64 artifact_length = 5; optional bytes checksum_bytes = 6;
  bytes definition_fingerprint = 7; bytes spec_hash = 8; optional bytes physical_generation_fingerprint = 9;
  optional StoredColumnarProjectionFrontierV1 snapshot_frontier = 10; ColumnarProjectionGenerationRoleV1 role = 11; reserved 12 to 15;
}
message StoredColumnarProjectionControlV1 {
  StoredColumnarProjectionSourceV1 source = 1; bytes target_definition_fingerprint = 2; bytes target_spec_hash = 3; uint64 highest_generation = 4;
  optional StoredColumnarProjectionGenerationV1 published = 5; optional StoredColumnarProjectionGenerationV1 candidate = 6;
  optional StoredColumnarProjectionGenerationV1 predecessor = 7; ColumnarProjectionLifecycleV1 lifecycle = 8;
  optional StoredColumnarProjectionFailureV1 failure = 9; uint64 replay_age_seconds = 10; uint64 replay_bytes = 11; uint64 replay_backlog = 12; reserved 13 to 31;
}
```

10. Enum symbols/numbers are exact. `ColumnarProjectionLayoutV1`: `COLUMNAR_PROJECTION_LAYOUT_UNSPECIFIED=0`, `COLUMNAR_PROJECTION_LAYOUT_V1=1`, `COLUMNAR_PROJECTION_LAYOUT_V2=2`, reserve `3..15`. `ColumnarProjectionGenerationRoleV1`: `COLUMNAR_PROJECTION_GENERATION_ROLE_UNSPECIFIED=0`, `COLUMNAR_PROJECTION_GENERATION_ROLE_PUBLISHED=1`, `COLUMNAR_PROJECTION_GENERATION_ROLE_CANDIDATE=2`, `COLUMNAR_PROJECTION_GENERATION_ROLE_PREDECESSOR=3`, reserve `4..15`. `ColumnarProjectionLifecycleV1`: `COLUMNAR_PROJECTION_LIFECYCLE_UNSPECIFIED=0`, `COLUMNAR_PROJECTION_LIFECYCLE_BUILDING=1`, `COLUMNAR_PROJECTION_LIFECYCLE_CATCHING_UP=2`, `COLUMNAR_PROJECTION_LIFECYCLE_READY=3`, `COLUMNAR_PROJECTION_LIFECYCLE_REBUILDING=4`, `COLUMNAR_PROJECTION_LIFECYCLE_DEGRADED=5`, `COLUMNAR_PROJECTION_LIFECYCLE_INVALID=6`, reserve `7..15`. `ColumnarProjectionFailureTargetV1`: `COLUMNAR_PROJECTION_FAILURE_TARGET_UNSPECIFIED=0`, `COLUMNAR_PROJECTION_FAILURE_TARGET_CANDIDATE=1`, `COLUMNAR_PROJECTION_FAILURE_TARGET_PUBLISHED=2`, `COLUMNAR_PROJECTION_FAILURE_TARGET_PREDECESSOR=3`, `COLUMNAR_PROJECTION_FAILURE_TARGET_CONTROL=4`, reserve `5..15`. `ColumnarProjectionFailureReasonV1`: `COLUMNAR_PROJECTION_FAILURE_REASON_UNSPECIFIED=0`, `COLUMNAR_PROJECTION_FAILURE_REASON_REPLAY_AGE=1`, `COLUMNAR_PROJECTION_FAILURE_REASON_REPLAY_BYTES=2`, `COLUMNAR_PROJECTION_FAILURE_REASON_REPLAY_BACKLOG=3`, `COLUMNAR_PROJECTION_FAILURE_REASON_SPEC_CHANGED=4`, `COLUMNAR_PROJECTION_FAILURE_REASON_ARTIFACT_INVALID=5`, `COLUMNAR_PROJECTION_FAILURE_REASON_RESOURCE_LIMIT=6`, `COLUMNAR_PROJECTION_FAILURE_REASON_STORAGE=7`, `COLUMNAR_PROJECTION_FAILURE_REASON_CANCELLED=8`, reserve `9..31`. Unknown commit is never persisted as a guessed failure; it is resolved by reread.

Zero and reserved values never store. Source oneof is exactly one. Inside a present frontier, absent `applied_through` means BeforeFirst and presence is nonzero. V1 forbids physical fingerprint; V2 requires it. Candidate has role Candidate. Unprepared means absent `snapshot_frontier`, artifact length, and checksum together, requires Candidate frontier BeforeFirst, and contributes a BeforeFirst fence. Prepared means all three are present: snapshot frontier may itself encode BeforeFirst, length is positive, and checksum is exactly 32 bytes; these fields identify the one fully synced/reopened/validated immutable artifact selected by the control. Published/Predecessor require positive length and exact 32-byte checksum, forbid snapshot frontier, and require matching roles. Failure generation is required and matches its pointer for Candidate/Published/Predecessor, and absent for Control. Unknown fields, noncanonical re-encode, or impossible presence is corruption.

11. Exact durable shapes are:

| State | Lifecycle and pointers | Failure | Servable |
|---|---|---|---|
| Initial V1 | Building; Unprepared or Prepared Candidate V1; no Published/Predecessor | none | none |
| Failed initial V1 | Degraded; retained Unprepared or Prepared Candidate V1; no Published/Predecessor | Candidate and matching generation | none |
| Ready | Ready; Published V1 or V2 only | none | Published |
| V1 plus V2 build | CatchingUp; Published V1 + Unprepared or Prepared Candidate V2 | none | Published V1 |
| V2 compaction/same-spec rebuild | Rebuilding; Published V2 + Unprepared or Prepared Candidate V2 | none | Published V2 |
| Failed candidate | Degraded; valid Published V1/V2 + retained Unprepared or Prepared Candidate; no Predecessor | Candidate and matching generation | Published only |
| Failed unservable candidate | Degraded; unservable Predecessor + retained Unprepared or Prepared Candidate; no Published | Candidate and matching generation | none |
| Selected corruption | Degraded; Published V1/V2 only | Published/ArtifactInvalid | none; V2 never falls back |
| Spec-change rebuild | Rebuilding; different-spec Predecessor + target Unprepared or Prepared Candidate; no Published | none | none |
| Corruption rebuild | Rebuilding; corrupt same-target Predecessor + target Unprepared or Prepared Candidate; no Published | none | none |
| Invalid | Invalid; no pointers | Control and no generation | none |

Published and Candidate hashes match control targets. A spec-change Predecessor has the prior target hashes; a corruption Predecessor retains the exact failed selected artifact and current target hashes. Prepared Candidate, Published, and Predecessor incarnation/layout/role/evidence match the exact selected artifact; Unprepared Candidate has no artifact-match claim. `RecordCandidateFailure` maps Building to Failed initial V1, CatchingUp or same-spec Rebuilding to Failed candidate without changing Published, and either unservable rebuild to Failed unservable candidate without changing Predecessor. A replacement retires/detaches the failed Candidate, allocates a checked Unprepared next generation, clears Candidate failure, and returns respectively to Building, the layout-derived CatchingUp/Rebuilding shape, or the same exact unservable rebuild family. `RetargetInitialCandidate` consumes only Initial V1 or Failed initial V1, requires the target definition fingerprint or target spec hash to differ, retires/detaches the old Candidate, updates the target hashes and replay limits, allocates the checked Unprepared next V1 generation at BeforeFirst with the same history incarnation, clears any Candidate failure, and produces Initial V1 Building with no Published or Predecessor. `RecordPublishedFailure` requires a Published pointer and ArtifactInvalid, retires/detaches any Candidate, clears its failure, and produces Selected corruption. `AllocateUnservableRebuildCandidate` either moves Published to Predecessor or retains the existing Predecessor while retiring its Candidate, updates targets only for spec drift, clears failure, and allocates an Unprepared Candidate; selected corruption therefore retains the same-target corrupt predecessor while drift retains the prior-target predecessor. No other combination exists.

ADR-0200 adds no durable shape to this table. `ResetForCurrentHistoryIncarnation`
consumes one complete expected control only when every present pointer carries the
same nonzero incarnation and that incarnation is strictly less than the
transaction-current authoritative history incarnation. It preserves source,
target hashes, and replay limits; uses checked `highest_generation + 1`; clears
Published, Predecessor, Candidate, and failure; and produces only Initial V1
Building with one Unprepared V1 Candidate at BeforeFirst under the current
incarnation. Equal, future, zero, mixed, pointerless, malformed, or exhausted
state refuses before mutation. For this reset alone, complete generation
identity is `(history_incarnation, generation)`: a numeric generation may repeat
after restore only when incarnation strictly increased, and never within one
incarnation.

12. Retention input is exact: an Unprepared Candidate has effective frontier BeforeFirst; a Prepared Candidate uses its applied frontier, which is at or after its durable snapshot frontier and whose selected immutable artifact contains that snapshot plus every contiguous relevant tail through applied. Ready uses Published frontier; CatchingUp and same-spec Rebuilding use the minimum of Published frontier and Candidate effective frontier; Degraded Candidate failure uses only valid Published frontier; either unservable rebuild uses Candidate effective frontier and never Predecessor; failed initial/unservable Candidate, selected-corruption Degraded, and Invalid contribute none. Replay-budget failure records the failed Candidate and the replacement operation alone detaches it. The builder samples server UTC exactly once after its exact frozen-tail scan; replay age is `max(0, sample - oldest retained LogicalTime)`, uses a strict `>` ceiling, and therefore clamps future logical times or wall-clock rollback to zero. Simultaneous breaches resolve deterministically as ReplayAge, then ReplayBytes, then ReplayBacklog. Every referenced frontier matches history incarnation; BeforeFirst pins from sequence 1. Every control operation that changes this input atomically replaces the same control row consumed by retention.

A stale-incarnation pointer contributes no retention frontier. Capture, serving,
worker publication, writers, and pruning remain closed while reset and its exact
durable reread resolve. Only the resulting current-incarnation Unprepared V1
Candidate contributes BeforeFirst; uncertainty never manufactures a fence from
the attempted replacement.

13. The tag-67 control row is also the durable per-source retention-fence input. While query readiness, writers, and pruning are closed, startup atomically inserts every absent control with a matching Unprepared fresh-V1 Candidate at BeforeFirst in one Immediate transaction. A conflict rereads the complete control. The global retention calculation consumes every tag-67 row before pruning; readiness/writers/pruning reopen only after all at-most-256 active rows exist, so every new source initially pins sequence 1. A builder captures a transactionally consistent authoritative snapshot at S, streams it, applies the retained contiguous tail through H where H>=S, and only then finalizes one complete immutable artifact. `RecordDurableSnapshot` consumes its fields-private fully synced/reopened/validated witness and atomically sets present `snapshot_frontier=S`, Candidate `frontier=H`, positive length, and exact checksum; H need not equal the later transaction-current head. Until that CAS commits, retention remains BeforeFirst; after it commits, the control-selected artifact contains snapshot S plus every relevant sequence through H and retention starts at H. Crash before CAS leaves only an ignorable unselected artifact and BeforeFirst fence; crash after CAS resolves the exact artifact from control without directory search. Its directory is `source-` plus lowercase hex of target spec hash, then ADR-0190 generation name; V1 artifact is `MANIFEST-V1-` plus the control checksum in lowercase hex, V2 uses its fixed `ROOT-V1`, and every other complete/orphan file is ignored. Symbolic names are never path components.

Startup compares every present pointer with transaction-current history before
specification reconciliation or construction. A stale control resets and is
durably reread first; ordinary `RetargetInitialCandidate` then consumes that
fresh control when target hashes drift. Every final Unprepared V1 Candidate,
whether reset- or retarget-born, unconditionally retires collisions at only its
exact final-directory and `.tmp` paths before construction. For each path `P`,
quarantine `Q` is its filename plus `.retired-before-history-` and the current
incarnation as sixteen lowercase hexadecimal digits. Under exclusive startup
ownership and with no captured view, a present `Q` is boundedly removed only
after no control or view references it and the parent is synced; a present
expected-kind `P` is then atomically renamed without replacement to absent `Q`
and the parent is synced. The repeatable state machine rejects symlinks, special
files, escapes, excess material, failed reference proof, or uncertain remove,
rename, or sync. It scans no parent and touches no other root; any failure keeps
all gates closed and no bytes are adopted.

14. `ColumnarProjectionControlRepository` exposes only `InitializeFreshV1`, `ResetForCurrentHistoryIncarnation`, `RecordDurableSnapshot`, `RecordCandidateFrontier`, `AdvancePublishedV1`, `BeginV2Candidate`, `AllocateSameSpecCandidate`, `AllocateUnservableRebuildCandidate`, `PublishPreparedGeneration`, `RecordCandidateFailure`, `ReplaceFailedCandidate`, `RetargetInitialCandidate`, `RecordPublishedFailure`, `RecoverExpectedControl`, and `MarkInvalid`, each consuming complete expected control. `ReplaceFailedCandidate` consumes a complete expected Failed initial V1, failed candidate, or failed unservable candidate control and performs only its already-frozen section-11 replacement transition. `RetargetInitialCandidate` consumes a complete expected Initial V1 or Failed initial V1 and performs only section 11's target-change transition. Allocation alone creates Unprepared Candidate evidence and builders may perform snapshot-plus-tail work without advancing its control. `RecordDurableSnapshot` is the only Unprepared-to-Prepared transition for either layout and installs one complete artifact with distinct snapshot and applied frontiers. Later `RecordCandidateFrontier` accepts only Prepared V1, requires a strictly increasing contiguous frontier not above transaction-current head and a newly completed fields-private checksum-named manifest witness, and atomically replaces frontier, positive artifact length, and checksum while retaining the original snapshot frontier. It never clears or advances beyond the control-selected artifact. Prepared V2 cannot advance because `ROOT-V1` is immutable; if transaction-current head exceeds H before publication, `AllocateSameSpecCandidate` replaces it when Published exists and `AllocateUnservableRebuildCandidate` replaces it when Predecessor exists, retaining that other pointer exactly. Publication requires Candidate frontier equal transaction-current head and exact artifact equality with its consumed prepared witness. Candidate and published failure follow every exact section-11 transition. Operations expose no callback, transaction, raw table/key, caller digest, or async method and assign no application/administration sequence.

`ResetForCurrentHistoryIncarnation` accepts no replacement identity, callback,
transaction, table, key, path, or digest. Its Immediate transaction rereads the
authoritative `history_incarnation/v1` metadata and exact control row, applies
only section 11's reset transition, and otherwise returns the existing mismatch,
storage-failure, or uncertain result for exact durable recovery.

15. Controlled V1 manifests are immutable `MANIFEST-V1-` plus 64 lowercase checksum hex characters, so control checksum and length select exactly one file and byte extent. Initial V1 publishes only after complete sync/reopen/validation at transaction-current head. `AdvancePublishedV1` requires the same published generation, same target hashes, a strictly increasing contiguous frontier not above transaction-current head, and a fully synced/reopened `PreparedColumnarGenerationV1::V1`; it replaces only the Published artifact/frontier and preserves any V2 Candidate byte-exact. Old artifacts remain while a control or captured view references them.

For reset collision handling only, a tag-67 Candidate path is bound by the
complete `(history_incarnation, generation)` identity even when restore rewinds
the numeric generation. Construction begins only after the exact-path retirement
state machine leaves both final and temporary paths absent. A pre-restore V1
manifest or directory is never relabelled, adopted, decoded as current, or
selected from its bytes.

16. `PreparedColumnarGenerationV1` is nonserializable, fields-private, path-free first-party evidence binding source, definition/semantics/spec, role, incarnation, generation/frontiers, artifact length/digest, and process generation. V2 also binds the ADR-0190 root/physical tuple and exists only after full root/member validation and independent logical equality. Publication closes capture, consumes witness/control, requires Candidate frontier equal transaction-current head, and CAS-selects it. Success, mismatch, storage failure, and unknown commit all reread durable control with the gate closed, validate/install exactly its selected immutable view, then reopen/acknowledge or remain closed. Existing captures may finish; selected corrupt V2 has no fallback.

The same closed-gate resolution applies to stale reset, subsequent spec retarget,
and exact-path retirement. Applied, expected-control mismatch, storage failure,
and unknown commit authorize no attempted Candidate by inference; the caller
rereads complete control and proceeds only from the exact fresh
current-incarnation Building shape or a later independently valid
current-incarnation state. No witness exists until fresh construction completes,
and no query, retention, publication, or acknowledgement gate opens earlier.

17. `columnar_projection_controls` contains at most 256 active tag-67 rows keyed by canonical source. Tag-65 source/control/table/fixtures and old name-derived directories remain byte-exact and inert. Structural startup/scrub may scan/decode at most 4,096 tag-65 rows, discards every value, and refuses a 4,097th; decoded data cannot reach common control, paths, selection, retention, health, recovery, metrics, or migration. There is no translation, copy, lifecycle mapping, directory search, or receipt. Cleanup acts only after no tag-67 control/view references a path.

18. ADR-0181 is narrowed only for structurally readable tag-65 `StoredVectorProjectionControlV1` until WP-757. It is never writable/current. WP-757 deletes its source, generated type, decoder, table, fixtures, manifest/topology rows and permanently reserves its numbers; a closed external window requires ADR-0124 review. This is analogous only to ADR-0190's bounded V1/V2 overlap, not a general exception.

19. WP-776 owns the new hash domain/types, exact definition semantics, source/spec derivation, tag-67 control/table/CAS, V1 lifecycle/retention, legacy inertness, config binding, fixtures, and docs. WP-711 depends on it and owns V2 root publication/gating, rebuild/compaction/pruning, crash matrix, and performance. WP-757 removes tag 65 and V1 layout identities. Grammar, Bundle, Executable IR, Lineage Ledger, Segment/Manifest V2, WP-710 fixture, legacy `DefinitionFingerprint`, provider descriptor V1, query IR/modules, cursors, public protocols, generic projection control, and PRJ-001/PRJ-003 do not change.

## Options considered

1. New contract declaration/ID: rejected because checked schema facts already close identity and new IR families conflict with ADR-0181. 2. Aggregate `ProjectionIdentity`: rejected because marker semantics differ. 3. Name/path identity: rejected because rename and traversal become authority. 4. Translate legacy control/files: rejected because derived state can rebuild and translation grants unnecessary authority. 5. Schema-bound source plus complete semantic hash and fresh rebuild: chosen as the least new durable identity.

## Consequences

WP-776 establishes one new control without contract/query/public artifact rotation. Identical Scalar legacy fingerprints intentionally share one source and duplicate registrations refuse; fingerprint changes create a fresh Scalar source, while newly captured definition semantics and provider/policy/replay changes rebuild under the same source without serving the predecessor. Existing derived files are not migrated. Tag 65 is the only temporary readable predecessor and has structural-validation authority only until mandatory WP-757 deletion.

## Standing design tests

- **Interface safety:** applications retain only symbolic projection names and bounded queries; no public value expresses source/control/hash/layout/generation/migration/validation/fallback/bound.
- **Scale:** at most 256 sources/controls, 4,096 structurally scanned tag-65 rows, 1 MiB definition semantics, 8,192-byte payloads, 291-byte keys, two simultaneously present generation pointers, and ADR-0190 artifact limits; rebuild streams by partition/segment under a persisted replay fence.

## Checks

- OBL-0192-1 through OBL-0192-6 freeze foundation identity, formats, fresh rebuild, compatibility, and V1 concurrency.
- OBL-0192-7 through OBL-0192-9 prove transaction-current publication, all-result gate recovery, and the crash matrix.
- OBL-0192-10 proves the bounded ADR-0181 exception cannot survive epoch 2.
