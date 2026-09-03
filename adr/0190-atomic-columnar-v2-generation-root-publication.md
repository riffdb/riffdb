---
adr: 0190
title: Atomic Columnar V2 Generation-Root Publication
status: accepted
tier: guarantee
date: "2026-09-03"
accepted: "2026-09-03"
requires: [ADR-0010, ADR-0017, ADR-0072, ADR-0085, ADR-0086, ADR-0111, ADR-0124, ADR-0130, ADR-0160, ADR-0181]
amends:
  - ADR-0160 sections 1, 5, and 6 by defining the cross-partition V2 generation root, exact publication CAS, and physical-generation fingerprint
supersedes: []
requirements: [PRJ-001, PRJ-002, PRJ-003, PRJ-004, OQ-017, OQ-019, OQ-020, OQ-021, OQ-022, OQ-024, OQ-053, PERF-007, PERF-008]
packages: [WP-711]
obligations:
  - id: OBL-0190-1
    package: WP-711
    proof: columnar_generation_root_v1_round_trips_and_refuses_noncanonical_inventory
    says: Generation-root V1 canonically binds one bounded complete partition inventory and refuses every unknown, duplicate, reordered, over-bound, mismatched, or corrupt member.
  - id: OBL-0190-2
    package: WP-711
    proof: columnar_v2_generation_root_publication_is_atomic_across_partitions
    says: No request observes a partial partition set or mixed layout before or after candidate publication.
  - id: OBL-0190-3
    package: WP-711
    proof: columnar_v2_publication_requires_complete_root_and_transaction_current_head
    says: Publication consumes a validated immutable root and succeeds only through the existing exact-control CAS at the transaction-current authoritative head.
  - id: OBL-0190-4
    package: WP-711
    proof: columnar_v2_crash_reopens_exactly_one_published_generation
    says: Every build, sync, rename, CAS, view-install, compaction, and retirement crash reopens either the predecessor or one complete selected generation and never falls back from selected corrupt V2 to V1.
  - id: OBL-0190-5
    package: WP-711
    proof: columnar_v2_physical_generation_fingerprint_preserves_frozen_definition_bytes
    says: Segment V2, Manifest V2, the WP-710 fixture, provider descriptors, query artifacts, cursors, and public protocols retain their exact definition bytes while the new root alone binds the complete physical format tuple.
  - id: OBL-0190-6
    package: WP-711
    proof: columnar_v2_compaction_reuses_generation_root_and_queries_validate_once
    says: Compaction republishes through a new generation root and queries consume only immutable open-validated views without per-operation durable reads or hashes.
review_triggers:
  - Publication would bypass ProjectionControlOperation::PublishCandidate, weaken its complete expected-control or transaction-current-head comparison, or acknowledge before durable CAS success and process-local view installation.
  - A selected V2 generation could fall back to V1, mix partitions or layouts, reuse a generation, mutate a published file, or expose a candidate before publication.
  - Provider descriptor, query IR, plan, module, cursor, protocol, authoritative entity, event, commit, backup, export, or changelog bytes would change.
  - An application, operator, query, policy, or transport could select layout, encoding, pruning, fallback, validation, compaction, generation, or a resource bound.
  - V1 would be removed outside WP-757 and ADR-0181, or after the external-database window closes without ADR-0124 retirement review.
  - The existing DefinitionFingerprint algorithm or its leading LAYOUT_VERSION value would change, Segment V2 or Manifest V2 would reinterpret their definition_fingerprint field, or the accepted WP-710 fixture would be regenerated.
---
# ADR-0190: Atomic Columnar V2 Generation-Root Publication

## Context

ADR-0160 freezes partition-scoped Segment V2 and Manifest V2 bytes, but one columnar engine may contain multiple organization partitions. Independently renaming those manifests cannot publish one complete generation: a crash could expose only a prefix, and no frozen artifact names the complete partition set.

The existing projection control transition already owns the required authority boundary. ProjectionControlOperation::PublishCandidate compares the complete expected control and requires the candidate frontier to equal the transaction-current authoritative head. WP-711 needs one immutable filesystem root prepared before that CAS; it does not need a new authoritative storage key or another sequence owner.

## Decision

1. Add COLUMNAR_GENERATION_ROOT_FORMAT_VERSION_V1 with identity 1 and magic RDBCGRT followed by one zero byte. Its canonical filename is ROOT-V1 inside generation- followed by the generation as sixteen lowercase hexadecimal digits; the temporary directory adds the suffix .tmp. Every integer is unsigned big-endian, every variable field is length-prefixed, reserved bits and fields are zero, and the existing checksum_bytes domain-separated SHA-256 covers every preceding byte. Unknown version, flags, trailing bytes, noncanonical order, overflow, or checksum mismatch is unusable derived state.

2. One ColumnarGenerationRootV1 binds: complete encoded length; the exact legacy DefinitionFingerprint already carried by Segment V2 and Manifest V2; one root-only PhysicalGenerationFingerprintV1; history incarnation; never-reused ProjectionGeneration; matched authoritative FrontierPosition; layout V2, Segment V2, encoding-registry V1, and Manifest V2 identities; total partition, segment, and row counts; and the complete partition inventory. Each inventory entry contains the organization-key bytes, Manifest V2 encoded length, and checksum_bytes checksum. Its relative filename is derived only as partition- plus the lowercase checksum hex plus .manifest-v2. Entries are strictly ordered by organization-key bytes and duplicate organizations or filenames are rejected.

3. A root contains at most 4,096 partitions and 16 MiB of encoded bytes. Organization keys retain the existing 1 MiB individual bound; every referenced manifest retains its 4 MiB and 4,096-segment bounds; segments retain ADR-0160 limits. A zero-partition root is canonical only for zero total segments and rows. Total counters use checked u64 arithmetic, equal the referenced manifests exactly, and do not raise any existing query, apply, memory, or response bound.

4. DefinitionFingerprint retains its already-frozen meaning and bytes. RegisteredDefinition::compute_fingerprint continues to prefix its canonical definition payload with LAYOUT_VERSION, and LAYOUT_VERSION remains 1. Production V2 construction passes that unchanged registered-definition fingerprint into the existing definition_fingerprint field of Segment V2 and Manifest V2. The field continues to identify the exact registered projection definition; it is not redefined as a V2-only physical fingerprint. Consequently the accepted WP-710 Segment V2 and Manifest V2 fixture, including its 0x11 test fingerprint, decodes and re-encodes byte-exactly and is never regenerated for activation.

PhysicalGenerationFingerprintV1 is new, root-local evidence and equals hash(HashDomain::CanonicalValue, b"riffdb.columnar.physical-generation/v1\0" || definition_fingerprint || layout_version_be || segment_format_version_be || encoding_registry_version_be || manifest_format_version_be). It does not replace or alter any Segment V2, Manifest V2, V1 checkpoint, provider descriptor, query IR, plan, module, cursor, or public field. The root separately carries every component of that tuple and verifies the digest on decode. Physical layout selection uses explicit format constants and the published generation root, never a global LAYOUT_VERSION flip.

5. V2 activation is selected independently for each database and registered columnar projection through one existing projection-control identity. On the first WP-711 open, the engine durably initializes that control before serving queries while retaining the complete V1 published generation. Building, CatchingUp, Rebuilding, a failed candidate, or any state without a successfully published V2 generation continues ordinary V1 apply, checkpoint, compaction, reopen, and query behavior while the disjoint V2 candidate catches up. Ready with a published V2 generation selects V2 for that database and projection and requires the root for exactly that generation; its absence or invalidity is not evidence for V1 fallback. A V2-selected database writes only V2 successors unless a later accepted decision changes that rule.

6. The builder allocates the candidate through the existing never-reused projection-control transition, captures one authoritative snapshot, streams the retained tail in order, and reaches one candidate frontier. It performs an exact canonical row-by-row comparison between the independent evaluator and a complete decode of every candidate partition at that frontier. The root inventory contains every nonempty organization partition; a zero-partition root represents an empty projection. No omitted partition may be repaired during publication.

7. The candidate is written in its generation-derived temporary directory. Each segment is written, fully synced, checksummed, renamed to its final immutable name, and followed by a directory sync. Each partition manifest is then written, fully synced, checksummed, renamed, and directory-synced. The root is written and fully synced last. The complete directory is renamed to its generation-derived final name and the parent directory is synced. Names use only the allocated generation or checked digest; no clock, randomness, caller text, or unresolved path is used.

8. Before publication, the engine reopens the final candidate directory and fully validates the root, every manifest, every segment, all cross-file identities and counts, and logical equality into one immutable nonserializable process-local generation view. Candidate files and that view remain unreachable by queries. Any write, sync, checksum, rename, reopen, equality, cancellation, ENOSPC, or bound failure records the existing typed failure or rebuild outcome and leaves the predecessor selected.

9. Publication calls ProjectionControlOperation::PublishCandidate with the complete expected control only after step 8. The existing authoritative transaction must find that exact control and candidate and must find candidate.frontier equal to its transaction-current authoritative head. StateChanged, a changed head, an exhausted generation, storage failure, or unknown result does not publish by inference; the worker rereads exact control state and follows the existing retry, degraded, or uncertainty path.

10. Known durable CAS success selects the complete V2 generation across every partition. The process atomically replaces its query-visible generation view with the prevalidated immutable view before reporting publication success or releasing projection-ready evidence. A racing query captures either the complete predecessor view or complete successor view. A crash after durable CAS but before view installation reopens the CAS-selected V2 root; it never rolls control back to V1.

11. Startup and unclean recovery read the authoritative projection control first. A nonready initial V2 control keeps V1 selected. A published V2 pointer requires one exact root, generation, incarnation, definition pair, format tuple, frontier, manifest inventory, and segment set. Missing, extra, stale, partial, mixed, corrupt, or unknown material produces the existing typed degraded/rebuild lifecycle and no rows; it never falls back to stale V1. An uncertain CAS is resolved only from durable control. Unreferenced candidate directories may be reclaimed only after no published or candidate control names them and no process-local view holds them.

12. Cold open, scrub, restore, rebuild, and unclean recovery perform complete validation. Successful open freezes the root, manifests, segment directories, statistics, and decoded lane metadata into immutable process-local views. Query and pruning paths use those views and perform no filesystem read, checksum, hash, decode-proof, or graph reconstruction per operation. Zone-map and dictionary pruning retain ADR-0160's exact truth-table and policy-alignment rules; missing proof scans within the existing bound or returns the existing typed refusal.

13. V2 compaction allocates a fresh candidate generation and repeats steps 6 through 10, including equality, full sync, reopen, transaction-current CAS, and atomic view replacement. It never replaces a root, manifest, or segment in place and never reuses ProjectionGeneration. The predecessor remains readable while captured views exist and is reclaimed only through existing retention and retirement rules after no control or view references it.

14. WP-711 activates V2 per database; it does not globally retire or disable V1. Until WP-757, layout V1 and Manifest V1 remain readable, writable, current, and active for databases or projections without a successful V2 publication and while their V2 candidates build or catch up. Layout V2 and Manifest V2 are also readable, writable, current, and active, but become selected only for the database and projection whose exact generation-root CAS succeeded. Writer selection is least-sufficient from durable per-database lifecycle state, never caller choice or a process-global default.

WP-757 must depend on WP-711 and may remove V1 source, fixtures, and topology identities only through ADR-0181's accepted epoch-2 ceremony while external_databases is empty. After that reset, V2 becomes the sole readable, writable, current layout and manifest identity. If the pre-external window closes first, ADR-0124 governs later retirement and this record grants no deletion authority.

15. The generation root is derived-state evidence only. It grants no storage, sequence, commit, authentication, authorization, policy, freshness, visibility, durability, backup, export, changelog, MCP, or transport authority. It adds no public option or fallback, changes no authoritative record or command ordering, and permits no per-tenant statistic, skip count, filename, digest, encoding, or distribution fact in public errors, logs, metrics, traces, or explain output.

## Options considered

1. Publish each partition manifest independently: rejected because a crash exposes a mixed or partial generation.
2. Replace a nonempty generation directory in place: rejected because portable atomic replacement and durable crash semantics are not available.
3. Add a new authoritative selector record: rejected because the accepted projection-control CAS already owns generation allocation, current-head comparison, and publication.
4. Prepare one immutable root and select it through the existing control CAS: chosen because filesystem completeness precedes the authoritative selection point and recovery has one exact decision.

## Consequences

WP-711 gains one derived durable root codec, fixture, candidate-directory lifecycle, V2 generation view, and production pruning path. It reuses rather than changes projection-control transaction ordering. Root validation is bounded by 4,096 partitions and 16 MiB, while segment bodies remain streaming and partition-scoped.

Across the WP-711 release, V1 and V2 remain active because selection is durable per database and projection: prepublication databases continue all ordinary V1 writes, while successfully published databases use V2 exclusively. A selected V2 corruption causes rebuild/degraded refusal rather than fallback. Only WP-757's separately accepted epoch reset makes V2 globally exclusive and removes V1.

## Standing design tests

- **Interface safety:** applications and agents still submit only compiled bounded queries and cannot select a layout, generation, encoding, statistic, validation mode, pruning policy, compaction, publication, fallback, or bound. No failure returns partial rows or a nearby generation.
- **Scale:** root metadata has fixed count and byte ceilings; segment construction, validation, equality, and compaction stream by partition and segment. No authoritative full-state copy or co-located storage is introduced, although the POC retains one bounded root for atomic local publication.

## Checks

- columnar_generation_root_v1_round_trips_and_refuses_noncanonical_inventory freezes root bytes and every count, order, length, identity, and checksum refusal.
- columnar_v2_generation_root_publication_is_atomic_across_partitions and columnar_v2_publication_requires_complete_root_and_transaction_current_head cover mixed partitions, stale controls, head races, and publication acknowledgment.
- columnar_v2_crash_reopens_exactly_one_published_generation covers every file, directory, CAS, view, compaction, and reclamation crash boundary.
- columnar_v2_physical_generation_fingerprint_preserves_frozen_definition_bytes proves compute_fingerprint still uses LAYOUT_VERSION 1, Segment V2 and Manifest V2 retain the same definition_fingerprint semantics, the WP-710 fixture remains byte-exact, and only the new root binds the physical format tuple.
- columnar_v2_compaction_reuses_generation_root_and_queries_validate_once proves immutable replacement and the pay-once hot path.
