---
adr: "0204"
title: Epoch-Two Storage Reader Retirement and Package Retiering
status: accepted
tier: guarantee
date: 2026-09-05
accepted: "2026-09-06"
requires: [ADR-0112, ADR-0124, ADR-0181, ADR-0190, ADR-0192, ADR-0195, ADR-0197, ADR-0200]
amends: [ADR-0181, ADR-0197]
supersedes: []
requirements: [GOV-001, GOV-002, GOV-003, AFC-005, AFC-006, VER-005]
packages: [WP-757]
obligations:
  - id: OBL-0204-1
    package: WP-757
    proof: epoch_two_format_gate_precedes_retired_reader_dispatch
    says: The immutable epoch comparison refuses every epoch-1 database before any retired storage reader, table open, journal replay, repair, allocator action, or mutation can run, while missing, malformed, unknown, and future identities remain fail-closed.
  - id: OBL-0204-2
    package: WP-757
    proof: epoch_two_removes_legacy_vector_control_bridge
    says: Epoch 2 removes tag-65 vector control without translating its bytes, preserves exact tag-67 control and V2 semantics, and retains cold registration and automatic first-demand activation.
  - id: OBL-0204-3
    package: WP-757
    proof: epoch_two_descriptor_closure_rotations_are_exact
    says: The exact declaration and identity retirements produce only the 27 enumerated retained-record descriptor-closure hash rotations; retained payload bytes, fields, tags, revisions, and semantics stay exact, and predecessor hashes remain reserved and refused.
review_triggers:
  - Epoch-1 bytes could reach a retired decoder, table, migration, journal, repair, or mutation path before the typed epoch refusal.
  - The 32-identity retirement set, 26-declaration deletion set, five historical-row removal set, or 27 retained hash pairs would differ from decision 5.
  - Generated review finds a 28th retained rotation, a different hash, or any retained field, tag, revision, payload byte, semantic, storage key, or descriptor change beyond the exact deletions.
  - Tag-67, WP-711 V2, ADR-0195 cold/first-demand, or WP-778 locator behavior would change beyond the three exact registry-hash rotations.
  - A retired identity would be reused, translated, parked as readable, or accepted as current; the ceremony would weaken; or an unsupported receipt-normalization amendment would enter.
  - Implementation would require a guarantee path other than the two storage-redb files named here.
---
# ADR-0204: Epoch-Two Storage Reader Retirement and Package Retiering

## Context

ADR-0181 accepted one pre-external breaking reset and assigned WP-757 removal of superseded readers, but classified it `surface`. The required edits to `crates/riffdb-storage-redb/src/startup.rs` and `store.rs` are `guarantee`; a trailer cannot replace an accepted decision.

The old WP-757 prototype predates current tag-67, V2, cold-activation, and fresh-locator guarantees. Deleting an unreachable sibling Protobuf declaration changes every importing file-descriptor closure even when a retained message and its payload encoding do not change. Main also appends five historical compatibility rows under duplicate record names. Safe integration therefore needs exact, separate retirement and retained-rotation sets.

## Decision

1. WP-757 is reclassified from `surface` to `guarantee`. Its objective, requirements, dependency on WP-711, ADR-0181 ceremony, allowed paths, and public behavior otherwise stay exact. A separate governance commit adds this ADR to `required_adrs` and raises the package tier before implementation; implementation and closure commits use `Governance-Tier: guarantee`.

2. This record authorizes guarantee-tier edits only to `crates/riffdb-storage-redb/src/startup.rs` and `crates/riffdb-storage-redb/src/store.rs`, solely to remove epoch-1 reader selection, table opening, validation, and dispatch made unreachable by the epoch-2 refusal. Current validation, storage keys, mutation and transaction ordering, conflict ownership, durability, journal and allocator semantics, acknowledgement, recovery, and outcomes do not change. Another guarantee path requires a separately accepted amendment.

3. Immutable format identity is checked before any authoritative table/journal open, initialization, migration, repair, replay, allocator inspection, receipt reconciliation, or mutation. Only the exact epoch-2 identity proceeds. Epoch 1 returns bounded `RDB-FORMAT-0101` naming `riffdb storage preflight` and export/reimport; missing, malformed, unknown, and future forms retain their typed fail-closed classifications and are never treated as empty.

4. No epoch-1 record is decoded to decide refusal. Only the retained epoch-1 binary validates, backs up, and exports that database. Epoch 2 has no fallback, compatibility mode, in-place migration, restore shortcut, best-effort decode, raw copy, repair, relabelling, or caller-selected epoch/decoder.

5. WP-757 first merges main without rebasing and regenerates from source. The following closed facts are bound to main `886c50232465631cb5e7ffffe700fdc0b15d29c9`.

   The 32 unique durable registry identities retired are: `CapabilityRecordV1` through `V7`; `ServiceAuditRecordV1`; `StoredCommandCapsuleV1` through `V5`; `StoredCommandSegmentV1` through `V4`; `StoredCommitRecordV1` and `V2`; `StoredDurableEventV1`; `StoredExecutionFailedV1` and `V2`; `StoredIndexEntryV1`; `StoredIndexEpochV1`; `StoredOutboxIntentV1`; `StoredOutcomeV1` and `V2`; `StoredPendingAdmissionV1` and `V2`; `StoredProvenanceRecordV1`; `StoredValidatedPrefixCheckpointV1`; and tag-65 `StoredVectorProjectionControlV1`. Tag-67 `StoredColumnarProjectionControlV1` is retained.

   Separate from that registry set, exactly 26 obsolete top-level declarations are deleted: `CapabilityRecordV2` through `V7`; `ServiceAuditRecordV1`; `StoredCommandCapsuleV2` through `V5`; `StoredCommandSegmentBodyV1` through `V4`; `StoredCommandSegmentV1` through `V4`; `StoredCommitRecordV1` and `V2`; `StoredExecutionFailedV1` and `V2`; `StoredIndexEntryV1`; `StoredIndexEpochV1`; and `StoredOutboxIntentV1`. Decision 6 separately deletes tag-65 source. A message still embedded by a retained schema is not misrepresented as a registered top-level durable identity.

   Five appended historical compatibility rows are removed, not called rotations: two extra `CapabilityRecordV1` rows with hashes `cb42c4ebbce8280123f8b34d4dcde74ca9483406847531f34d5fb3f18d40b342` and `dee2398ebbc71824471fe5a5f96fcbebdde09e511fe6c11531c2b30210f9e6bf`, plus `CapabilityTokenLookupV1` `e00696f3d2c110c5b2b99685e7987f7204d2f9576f8b41c5fcd5a894c746bc86`, `CapabilityBootstrapMarkerV1` `1204e270169688244bc489071db70c7d2481111a970d5a9da420ad7b9334e4ea`, and `CapabilityAdministrationAuditV1` `28d4a9d5f63eb9f5bacb2042af50d47ea39532bc2ef60c3dc52a450cc7b49a46`. The latter three canonical current hashes remain unchanged.

   Exactly these 27 retained schema hashes rotate (old -> new):

   | Retained record | Old hash | New hash |
   | --- | --- | --- |
   | `StoredEntityRecordV1` | `67eb5bbd2b789438f7d74861cb97a36700a69926a9e2d848508d019a18d4a213` | `6cfa3e7ef6dde5ecba0e8bbf5c5275ea102871c0b0607581dad8d3cafed5414f` |
   | `StoredOutboxStatusV1` | `264d9f5ec757041a342ccd93e48927cd0fab8e8e886ad7d73b9e20e731ecc900` | `48e16810a9c36db08b1144733003b313085be05259b67a950124f0e2603ac8d9` |
   | `StoredIndexEntryV2` | `1be795682748d69f9da74f2d9697dd9a041b7c5e642f757fe4daf902e78368f8` | `2deb05ec7b4c097deefbf4b13cecd8360b7eeaa8af8463b4f4e906b06adb869c` |
   | `StoredOutboxIntentV2` | `4c66e68d3c86f0cb6efdf88625852e86aeff9db6c864569669bc34ccbda6a80c` | `553a696c7aceb364af9fecaf7c84522a29f4e9115841cbecb2ab51f2cd69a817` |
   | `StoredIndexGenerationV2` | `b0bed2d75281f5f1c1882df7d503b6e1c045e1cc69615c00690ce01841037e14` | `312b2013f2fca066c7a6c9c69ad00a3702ad836ebca171fb4ecabf781bf92913` |
   | `StoredCommitRecordV3` | `a37545cfbf3847a43ce248bf70621896a707b7372cb635124a1e9d5c3f5bd083` | `c26dfb7c514a1237762c771842c176d01f203229c047516f06c14eb68178ed8e` |
   | `StoredContractMigrationJournalV1` | `0d050ac47e32fde0be9e81ec004c706b2b8fb342fe93e15ccbaf66ec35854e5a` | `e65a4574b832a9afcf40bedb219e561f2e55ccf3bc7eab97abfba4b95ecd72f0` |
   | `StoredContractMigrationRecordV1` | `717f78db6247f3e9d220af0b44fab6407c0db2be64da6af5a84a354367a64edf` | `54585725acbf582a9697771f0a9f48b5bf45e760f3cdf8248cbdd776c29e710f` |
   | `StoredContractWriteRetirementV1` | `5f8f2c3c510fd24cdb91c5f51026fc5feebf2a56b76ed4b380c944d31577f317` | `0df2d0ede9720f938c8dac532f46d94c5ce33c3c33f88809bd6159cf11ecd0d5` |
   | `StoredRetiredEntityRecordV1` | `da35b66acd172e6a4c8d1252c37f14512b2af7a81c18658f5c4855491e4e789e` | `f94e4d06a032a238efd1cab95ee57e27ae3e8d33507f61fbfd4262c59373920e` |
   | `ServiceAuditRecordV2` | `6ed779e099f4f94478c043c6e2b904b6e80323be7bd652c7ee70398156845374` | `35b31355fb556eede726eeec82ab7a5f0e9593385ce0523b99234d35215797a1` |
   | `StoredProvenanceRecordV2` | `320d8fe4827d70911a69d79341242446e1ae8b353bc0706a7a3fbfab1692f15e` | `4abc9b22997187dab3456350f42d6211944ae2d7d0771f1cef97c91a928ede7f` |
   | `StoredCommandLocatorV1` | `8cb109d2b275f638b279a89dd6fd93f168197bd142bf52f4053b1353cc0bdeb8` | `01d6cbdabf6924f6d2b8f0026956530525d3d34d591a0cbf931ec12df3d82889` |
   | `StoredCommandAuditLocatorV1` | `674bc659f79b759b835cfe1c25298fed46bda1541ea52ad15bab5ef84008358c` | `b295c2349eccc8af1d3f3ce29fa3c9149d1449e0e6c9da6d033522726b2981f6` |
   | `StoredCommandDerivedIndexCheckpointV1` | `5fef28f9aff69c45a4e6b7e0bfce2af78087854414025196f76d847e39ccf617` | `bf709c5308adbd2124876dbd5d2cad1295e86163ffb9d3331de8ccf2e6808ced` |
   | `StoredPendingAdmissionV3` | `916411a1cf4970cfde575048cba9fb5e9b9c2ab9e4c53afb6498b3d0c5bfb1a8` | `c984515a861a004c1130567eae487bb3605356177fd5c4e1069349d6b6f8e4a6` |
   | `StoredExecutionFailedV3` | `8e84d35002e9b788d8654fc24939ae061fbf89d98e0cc4fe133004f8153aa73a` | `9258195207829b0585742b77b98388b08097b5d374143dfe1a5f18c87dc56108` |
   | `StoredOutcomeV3` | `9aea2af175e464b0bc142a5b7d8b6a45d582106b417b8d0ec8a1272cda4914ae` | `488e3ba9fd43c2531d32f9f6697c7b66837af31de708dd9cc8bd44e2dac03afc` |
   | `StoredEntityChainHeadV1` | `839ceb5e3d27e0c15b9cca9ddee57fded34158bf3962d6ff15e56bd5dbba1b8b` | `d5ba8fd4dc753bb1e37243ad714f332b317648589017d53117b65bbf215acaa7` |
   | `StoredChangelogV2RotationReceiptV1` | `80e7cf714634327d304445624f3ad8fb2960b2bf8a2c2b899c168f881aeac356` | `bd6ca0253638e618f172c576b2035693fe11eba131d98c88001cba015fb0819c` |
   | `StoredValidatedPrefixCheckpointV2` | `e4949e1c698e22794c100d09b2a9784da20d15d0ce41c93b123756c88bbae240` | `7c49a0aaedb59a278b198ad46840a98820c3943f5f85a91a469bc60289cbd2aa` |
   | `StoredDurableEventV2` | `021ed7ec95f23d52efd8e30edea55f8841c2f42555d71678addb0fdceb484116` | `7917b4feba007e39fa9c510529d6b36f3dd9e53e333c39bb00f6381883a9c39b` |
   | `CapabilityRecordV8` | `ff4fcbb031676932d6ab40e02b288e21fbb3c196034445c897f5170ff121aef9` | `333df0f9fe3d1bc0898dd3bffd85401bbbdb0997c72b5607b905fb364ba3f27d` |
   | `StoredVectorEvidenceV1` | `de3451baade7dbe010de8f4bf227dbf267931cf9dafef170fde952b14a16d84f` | `4ffdb4f20e06520804b029dfc29e3ab3ccb7154be0bef7cf7d3ad8cfe573d523` |
   | `StoredVectorEvidenceIndexV1` | `6129c2ee589159eab28081ba3d2aff16de85e67a80e6d86d9bc265957d1e7d4e` | `1cd7f5ef12d681473acc7040d6e2adeddd398fbe06742eb89052c6d11ea11fb8` |
   | `StoredCommandCapsuleV6` | `0bc288e0f08ec4b83379f5abb4b872ebfc3ea7b7a64983eedcd53a9da25731bb` | `3844fed9b58af15252ec5076c121ac88e8b6e8a6687f4b8b37d03d8352d0c9be` |
   | `StoredCommandSegmentV5` | `0df52ca7232d2383b820b4b8ab844cf08d35529aa78e4b9b2bdd43b28afbd7dd` | `160ca56f9b7a685c33c4e6e82c55adfc9ba24b27989fb02de1781c9d46d2ea5d` |

   Every old hash above is reserved and refused. The retained messages' own descriptors, tags, revisions, payload bytes, validation, and semantics remain exact; only descriptor-closure-derived registry identity rotates. Every other retained current identity/hash equals merged main. Exact generated review stops on any set or value drift.

6. Tag-65 source/type, decoder/encoder, table/open path, migration bridge, fixtures, manifest/topology row, and identities are deleted and reserved without scan, translation, copy, adoption, relabelling, or receipt. Tag-67 source/type/tag/revision/hash, control transitions, table/key, fresh-current-incarnation reset, V2 selection/publication/retention, and no-fallback semantics remain exact. ADR-0195 cold registration and automatic first semantic-demand activation remain exact: readiness/no-demand close opens no artifact, callers gain no activation control, and cold/building/failure serves no rows.

7. This record narrowly amends ADR-0197 only for the three table rows above: locator, audit-locator, and derived-index-checkpoint payload bytes, fields, tags, revisions, keys, validation, point-read order, arming site, affine witnesses, publication/rebase transitions, bounds, failure algebra, and public/private semantics remain exact while their closure-derived registry hashes rotate. No other WP-778 durable, journal, protocol, transaction, acknowledgement, or outcome fact changes.

8. Each other identity in the closed retirement set loses its top-level reader, fixture, registry/manifest window, topology identity, and dispatch together; applicable declarations/artifacts are deleted only under the exact sets above. Retired numeric and symbolic identities and hashes stay reserved, and each topology domain has one equal readable/writable/current active identity while `external_databases` is empty. No retired identity becomes a placeholder.

9. A retained epoch-1 binary fully validates and backs up seeded TicketDesk state, exports through the accepted bounded portability path with a receipt, and retains old artifacts. Epoch 2 proves typed in-place refusal, initializes a distinct empty database, reimports only through compiled behavior, reconciles entity/event/provenance/authority/contract/hash observations, and passes ordinary startup validation. Hand-authored JSON, physical copy/restore, or fixture-only refusal is insufficient.

10. No public operation, field, MCP/SDK/CLI control, storage selector, force flag, or authority is added. Nothing here imports the unproved 2026-09-03 receipt-normalization text or changes a retained receipt identity, encoding, discriminator, filename, or semantic; only authority already present in accepted ADRs and merged main applies. WP-757 closes only after exact generated-diff, reservation, topology/manifest, three obligations, and complete ceremony-receipt review all pass.

## Options considered

Keeping surface tier, retaining unreachable readers, applying retirement before the epoch gate, or trusting the stale prototype inventory are rejected because they respectively lack authority, preserve attack surface, destabilize refusal, or delete current guarantees. Narrow guarantee retiering plus closed retirement/rotation sets is chosen.

## Consequences

- WP-757 may remove old readers under exact-text guarantee review; 27 retained registry identities rotate mechanically while their payload contracts remain exact.
- Epoch-1 databases require the old binary and full export/reimport ceremony; in-place open, restore, repair, and downgrade remain unavailable.
- A changed main descriptor closure, retirement set, historical row, hash, guarantee path, or current semantic requires a new accepted amendment.

## Standing design tests

- **Interface safety:** applications, agents, and operators cannot select an epoch, decoder, fallback, activation mode, artifact, target mutation, or weaker ceremony; epoch-1 storage receives only typed pre-mutation refusal.
- **Scale:** refusal reads fixed identity only; cold columnar startup is population-independent; the explicit ceremony retains accepted bounded page, receipt, partition, and artifact ceilings.

## Checks

- `epoch_two_format_gate_precedes_retired_reader_dispatch` proves epoch-1 open reaches no table, journal, repair, allocator, receipt, or mutation hook before `RDB-FORMAT-0101`.
- `epoch_two_removes_legacy_vector_control_bridge` proves complete tag-65 absence/reservation and byte-exact tag-67/V2/cold-demand behavior.
- `epoch_two_descriptor_closure_rotations_are_exact` independently derives the 32/26/5 sets and 27 hash pairs, proves all other retained identities and payload contracts equal main, and rejects any drift.
- `epoch_two_ceremony_refuses_in_place_open_and_reconciles_reimport`, `scripts/check-epoch-two-inventory`, `scripts/check-version-topology`, generation/compatibility checks, and corruption fixtures prove the complete ceremony, one-current topology, reservations, and bounded diagnostics.
