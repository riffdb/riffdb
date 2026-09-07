---
adr: "0209"
title: Epoch-Two Direct Columnar V2 Control Activation
status: proposed
tier: guarantee
date: 2026-09-06
accepted: null
requires: [ADR-0112, ADR-0160, ADR-0181, ADR-0190, ADR-0192, ADR-0195, ADR-0200, ADR-0204]
amends: [ADR-0190, ADR-0192, ADR-0200, ADR-0204]
supersedes: []
requirements: [GOV-001, GOV-002, GOV-003, REC-001, PRJ-004, PRJ-006, PRJ-007, PRJ-008, PRJ-009, PRJ-010, PERF-019]
packages: [WP-757]
obligations:
  - id: OBL-0209-1
    package: WP-757
    proof: epoch_two_columnar_control_lifecycle_is_v2_only
    says: Epoch 2 accepts exactly the closed V2 control shapes and refuses every V1, CatchingUp, missing-fingerprint, mixed-layout, malformed, or unenumerated semantic shape.
  - id: OBL-0209-2
    package: WP-757
    proof: epoch_two_columnar_reset_allocates_transaction_current_v2_candidate
    says: Fresh insertion, stale-incarnation reset, initial retarget, and failed-initial replacement allocate only the exact checked V2 candidate with deterministic physical fingerprint and reread-only uncertainty resolution.
  - id: OBL-0209-3
    package: WP-757
    proof: epoch_two_first_demand_publishes_v2_without_v1_artifact_or_dispatch
    says: Cold startup opens no artifact and first semantic demand builds, validates, publishes, and installs only V2 without any V1 codec, artifact, prepared witness, transition, or fallback.
  - id: OBL-0209-4
    package: WP-757
    proof: epoch_two_unprepared_v2_retires_exact_candidate_paths
    says: Every unprepared V2 candidate construction or restart repeats the exact aggregate-bounded final, temporary, quarantine, rename, sync, reference, and crash protocol before construction.
  - id: OBL-0209-5
    package: WP-757
    proof: epoch_two_prepared_initial_v2_head_advance_allocates_checked_replacement
    says: A prepared initial V2 candidate behind transaction-current head is retired only through the exact checked-next unprepared initial V2 replacement CAS and every refusal or uncertain result resolves by durable reread.
review_triggers:
  - Tag 67, revision 1, its Protobuf declaration, descriptor, schema hash, field or enum number, payload byte, key, table, or bound would change.
  - Epoch 2 would semantically accept V1, CatchingUp, a missing or caller-selected physical fingerprint, mixed layout, or a state outside the closed table.
  - Fresh, reset, retarget, failure, replacement, publication, retention, or uncertainty behavior would differ from this record.
  - V1 source, artifact bytes, filesystem dispatch, prepared witness, repository operation, fallback, or translation would remain reachable in production.
  - ADR-0200 collision cleanup would scan a parent, touch another path, adopt bytes, weaken reference proof or bounds, or omit a rename/remove/sync crash edge.
  - ADR-0195 cold registration, immediate rowless first-demand result, single worker, page-boundary cancellation, or no-demand evidence would change.
  - Implementation would touch a production path outside Decision 10 or alter ADR-0204's 32/26/5/27 facts.
---
# ADR-0209: Epoch-Two Direct Columnar V2 Control Activation

## Context

ADR-0190 makes columnar layout and manifest V2 the sole readable, writable, and current identities
after WP-757, but ADR-0192 and ADR-0200 require every absent or reset tag-67 control to begin with
an unprepared V1 candidate. The only accepted transition from that state to V2 first publishes a
V1 artifact. Removing the V1 manifest codec and runtime dispatch therefore makes first-demand
activation unreachable, while retaining them would make the epoch-2 topology false.

Tag 67 already carries every field required for a direct unprepared V2 candidate. The epoch gate
refuses every epoch-1 database before its control table can open, so epoch 2 can narrow semantic
validation without changing or translating the retained wire record.

## Decision

1. This record amends ADR-0190 Decisions 5, 11, and 14; ADR-0192 Decisions 10 through 16 and 19;
   ADR-0200 Decisions 3 through 7; and ADR-0204 Decisions 5 and 6, OBL-0204-2 and OBL-0204-3,
   and their corresponding consequence, review, and check language only as stated below. The
   transitional WP-711 V1/V2 proofs remain historical evidence for epoch 1. ADR-0195 is required
   and unchanged: registration remains cold, readiness and no-demand close open no artifact, and
   only the first semantic demand wakes the sole server-owned worker.

2. `StoredColumnarProjectionControlV1` remains exact tag 67, revision 1. Its `.proto` source,
   descriptor, generated wire declaration, fields, enum symbols and numbers, payload/envelope
   bounds, fixture, table/key, and schema hash remain byte-exact. In particular wire layout value
   `1` and lifecycle value `2` remain declared only to preserve that descriptor. Epoch-2 semantic
   conversion refuses layout V1 and CatchingUp; those declarations grant no readable identity,
   semantic runtime variant, construction API, transition, artifact dispatch, or compatibility path.

3. The complete epoch-2 semantic state table is:

   | State | Lifecycle and pointers | Failure | Servable |
   | --- | --- | --- | --- |
   | Initial V2 | Building; one unprepared or prepared Candidate V2 | none | none |
   | Failed initial V2 | Degraded; retained Candidate V2; no Published/Predecessor | matching Candidate | none |
   | Ready | Ready; Published V2 only | none | Published |
   | Same-spec rebuild | Rebuilding; Published V2 plus Candidate V2 | none | Published |
   | Failed candidate | Degraded; Published V2 plus retained Candidate V2 | matching Candidate | Published |
   | Unservable rebuild | Rebuilding; Predecessor V2 plus Candidate V2 | none | none |
   | Failed unservable | Degraded; Predecessor V2 plus retained Candidate V2 | matching Candidate | none |
   | Selected corruption | Degraded; Published V2 only | Published/ArtifactInvalid | none |
   | Invalid | Invalid; no pointers | Control with no generation | none |

   A spec-change predecessor has the prior target hashes; a corruption predecessor retains the
   failed current target. Every pointer is layout V2 and carries its exact physical-generation
   fingerprint. Every other pointer, lifecycle, role, failure, or servability combination refuses.

4. Fresh insertion creates generation one, Building, one unprepared Candidate V2 at BeforeFirst
   under the current nonzero history incarnation, present physical fingerprint, and no Published,
   Predecessor, artifact, snapshot frontier, or failure. The fingerprint is exactly the unchanged
   ADR-0190 `PhysicalGenerationFingerprintV1::compute(target_definition_fingerprint)` over tuple
   layout 2, segment 2, encoding registry 1, manifest 2. The existing type and computation move
   byte-exact to `riffdb-types::columnar` as the one common owner and `riffdb-columnar` reexports
   that same type; no duplicate algorithm or conversion is added. No repository caller,
   application, operator, configuration, or persisted selector supplies the fingerprint.

5. `ResetForCurrentHistoryIncarnation` retains ADR-0200's complete expected-control comparison and
   transaction-current metadata read. Only uniformly nonzero stale pointers qualify. It preserves
   source, target hashes, and replay limits; uses checked `highest_generation + 1`; clears every
   pointer and failure; and installs exactly the unprepared Initial V2 shape under transaction-
   current incarnation with the deterministic target fingerprint. Equal, future, zero, mixed,
   pointerless, malformed, exhausted, failed-read, mismatch, and unknown-result cases remain closed.

6. `RetargetInitialCandidate` consumes only Initial V2 or Failed initial V2 with no Published or
   Predecessor, requires a changed target definition or spec, retires the old candidate, uses the
   checked next generation and same incarnation, recomputes the target fingerprint, clears failure,
   and returns Initial V2. `ReplaceFailedCandidate`, same-spec allocation, and unservable allocation
   similarly choose only V2 and deterministically derive the applicable fingerprint. Their
   repository APIs accept no layout or digest. Generation exhaustion refuses before mutation.

   `ReplaceAdvancedInitialV2Candidate` consumes only Building with one prepared Candidate V2 and no
   Published, Predecessor, or failure. In one transaction it requires the complete expected control,
   candidate fingerprint equal to the target computation, candidate incarnation equal to the
   nonzero transaction-current history incarnation, and candidate frontier strictly below the
   transaction-current application head. It retires that candidate and installs checked
   `highest_generation + 1` as one unprepared Candidate V2 at BeforeFirst with the same incarnation,
   targets, limits, and computed fingerprint; lifecycle remains Building. An unprepared, equal-head,
   future-head, stale/mixed-incarnation, wrong-target/fingerprint, malformed, mismatched, or exhausted
   input refuses. Applied, StateChanged, storage failure, and unknown commit resolve only by reread.

7. First semantic demand rereads exact control. With no selection it captures one authoritative
   snapshot, streams the retained contiguous tail under existing bounds, writes/syncs/reopens one
   immutable V2 generation root, records its prepared witness, and publishes only at transaction-
   current head through the existing complete-control CAS and closed capture gate. A moved head
   replaces the immutable candidate with a checked newer V2 candidate. No V1 construction is an
   intermediate state. `BeginV2Candidate`, V1 `RecordCandidateFrontier`, `AdvancePublishedV1`, the
   V1 prepared witness, Manifest V1 codec, V1 engine open/apply/checkpoint, and V1 path dispatch are
   removed. Published V2 opens only its exact validated root and never falls back.

8. ADR-0200's collision protocol transposes without weakening to every unprepared V2 candidate,
   whether fresh, reset, retargeted, failure-replaced, head-advanced, same-spec, compacted,
   spec-change, or corruption-rebuild, and repeats before every construction or restart. For the exact final
   `generation-<sixteen lowercase hexadecimal generation>` directory and its `.tmp` sibling `P`,
   quarantine `Q` is `<P-name>.retired-before-history-<sixteen lowercase hexadecimal current
   incarnation>`. For each pair, a present Q is boundedly removed only after proving no control or
   captured view references it and syncing the parent; a present expected-directory-kind P is then
   atomically renamed without replacement to absent Q and the parent is synced. Every remove,
   rename, and sync edge is restart-repeatable. Symlinks, special files, escapes, excess material,
   failed reference proof, and unknown outcomes keep all gates closed. No parent scan, adoption,
   relabelling, or other path is permitted; construction starts only after both P paths are absent.

   Bounds use checked arithmetic before construction and traversal. At most
   `4096 * 4096 = 16,777,216` segment
   files, 4,096 manifests, one root, and one in-flight member `.tmp` yield 16,781,314 regular member
   files and 1,125,917,170,597,888 member bytes from the existing 64-MiB segment, 4-MiB manifest,
   and 16-MiB root maxima. If `B` and `R` are the control's replay-backlog and replay-byte limits,
   run ceiling `S = R + 4 * B * 4096 * (MAX_OBSERVATION_PAYLOAD + 36)` retains ADR-0192's checked
   budget. At most 4,096 partition lanes add
   `L = 4096 * 4096 * 65,536 * (MAX_PROJECTED_ROW_PAYLOAD + 36) + 4096 * 8` bytes. Scratch thus adds
   at most `4096 + floor(S / 8)` files and `L + S` bytes because each lane/run has an eight-byte
   header. The aggregate walker refuses above `16,785,410 + floor(S / 8)` regular files,
   `1,125,917,170,597,888 + L + S` bytes, or two directories including its root and optional exact
   scratch child. A 4,097th segment for one partition refuses before its file is created. Overflow
   or platform-size conversion failure refuses without creation, traversal, rename, or deletion.

   V2 construction never directly deletes or adopts a preexisting final or `.tmp` directory.
   `TemporaryGenerationGuard`, scratch Drop, and error/cancellation cleanup leave abandoned material
   for this protocol; only successful in-process scratch finalization removes its owned scratch
   before ROOT-V1. Existing unselected-published-generation reclamation remains separately fenced.

9. Candidate write, snapshot, replay-age/bytes/backlog, logical-equality, bound, cancellation,
   storage, sync, reopen, checksum, and root-validation failures retain the existing closed reason
   and pointer class. Failed initial V2 serves nothing; failed same-spec Candidate serves only its
   Published V2; failed unservable Candidate and selected corruption serve nothing. Replacement
   alone allocates a newer candidate. Applied, StateChanged, storage failure, and unknown commit
   authorize no attempted state by inference: exact durable reread under the closed gate alone
   selects retry, install, failure, or continued refusal. Shutdown cancels only at the accepted
   page boundary and causes no publication, checkpoint, frontier advance, or acknowledgement.

   Runtime constants `COLUMNAR_LAYOUT_VERSION_V1` and `COLUMNAR_MANIFEST_FORMAT_VERSION_V1` are
   removed. Exact `LAYOUT_VERSION = 1` remains frozen for the registered-definition fingerprint and
   existing provider-state identity and descriptor bytes required by ADR-0190; it is not a physical
   runtime layout selector. Provider descriptor bytes and the resulting `ColumnarProjectionSpecHashV1`
   semantics remain exact. Encoding-registry V1 and generation-root V1 remain their independently
   current V2 components.

10. Production authority is limited to `crates/riffdb-types/src/columnar.rs`,
    `crates/riffdb-types/src/lib.rs`, `crates/riffdb-storage-api/src/columnar_control.rs`,
    `crates/riffdb-storage-redb/src/columnar_projection_control.rs`,
    `crates/riffdb-storage-redb/src/shared_ports.rs`, `crates/riffdb-columnar/src/apply.rs`,
    `crates/riffdb-columnar/src/checkpoint.rs`, `crates/riffdb-columnar/src/engine.rs`,
    `crates/riffdb-columnar/src/definition.rs`, `crates/riffdb-columnar/src/segment_v2.rs`,
    `crates/riffdb-columnar/src/generation_root.rs`,
    `crates/riffdb-columnar/src/generation_v2.rs`, `crates/riffdb-columnar/src/streaming_v2.rs`,
    `crates/riffdb-columnar/src/prepared_generation.rs`, `crates/riffdb-columnar/src/store.rs`,
    `crates/riffdb-columnar/src/lib.rs`, `crates/riffdb-server/src/columnar_adapter.rs`,
    `crates/riffdb-server/src/columnar_worker.rs`, and `crates/riffdb-server/src/storage.rs`.
    Matching tests, retired V1 fixtures, inventory checks, and handbook pages remain within WP-757.
    The Protobuf and generated wire code do not change. These are surface/internal paths and do not
    broaden ADR-0204 Decision 2's two guarantee paths.

11. After exact acceptance, one separate governance commit adds ADR-0209 to WP-757
    `required_adrs`; narrows its control-retirement deliverable and exit gate to the Decision 3
    V2-only state table; permits only the frozen descriptor-only V1/CatchingUp declarations from
    Decision 2; adds the five obligations above; and adds drift from Decisions 4 through 10 to its
    human-review triggers. Dependencies, requirements, allowed paths, acceptance commands, epoch
    ceremony, tag-65 removal, changelog exception, and ADR-0204's 32/26/5/27 facts stay unchanged.

12. This proposal changes only this ADR and the generated ADR index. It changes no package,
    implementation, runtime behavior, durable/wire byte, fixture, topology, manifest, public
    interface, database, or external state before exact human acceptance and the separate commit.

## Options considered

1. **Direct V2 initialization/reset:** chosen because epoch-2 control and topology are truthful at
   every durable boundary and first demand reuses the already accepted V2 build/publication path.
2. **Promote a fresh V1 candidate before construction:** rejected because it adds an authoritative
   CAS and crash matrix and persists a retired layout in epoch 2 even though it writes no artifact.
3. **Publish V1 before V2:** rejected because it retains every V1 writer, reader, artifact, and
   recovery path that WP-757 must delete and leaves V1 selected across valid crash boundaries.

## Consequences

- New and reset epoch-2 controls are immediately V2-only without changing tag-67 wire identity.
- Epoch-1 databases still require the retained old binary and export/reimport ceremony; no tag-67
  row, V1 artifact, or path crosses the epoch.
- The frozen descriptor-only enum declarations remain a deliberate negative-test burden.
- A future control wire change, another layout, or a compatibility reader requires a new ADR.

## Standing design tests

- **Interface safety:** applications, agents, operators, transports, and configuration cannot
  choose activation, layout, digest, generation, artifact, fallback, retry, reset, or cleanup.
- **Scale:** startup retains at most 256 controls and opens no artifact; first demand retains all
  accepted streaming, replay, root, partition, segment, cleanup, and page-boundary ceilings.

## Checks

- The five obligations prove the closed state table, transaction-current reset and head advance,
  direct cold V2 activation, exact collision protocol, every failure/CAS outcome, and no V1 dispatch.
- Existing ADR-0190 root/publication, ADR-0192 gate/no-fallback, ADR-0195 cold lifecycle,
  ADR-0200 reset uncertainty, and ADR-0204 epoch-first/inventory/rotation proofs remain green.
- `scripts/check-epoch-two-inventory`, `scripts/check-version-topology`, generated and durable-schema
  checks prove frozen descriptor bytes/hash, V2-only topology, retired source, and no extra rotation.
