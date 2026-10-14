//! Actual native prune/drain qualification and preservation evidence.
use super::engine_compatibility::restore_fixture_bytes;
use super::*;

// req: STO-030, STO-032, END-007, END-008, REC-001
#[test]
fn native_prune_then_outbox_drain_preserves_source_validation_qualification() {
    use riffdb_storage_api::{
        AuthoritativePointReader, OutboxClaimV1, OutboxDestinationIdV1, OutboxPageLimit,
        OutboxRepository, OutboxSucceedV1, OutboxTransitionResultV1, PendingOutboxScanV1,
    };
    use std::sync::atomic::Ordering;
    let directory = crate::test_path::ScopedDirectory::new("prune-outbox-drain-proof");
    restore_fixture_bytes(
        &directory.join(""),
        include_str!(
            "../../../../fixtures/storage/redb-4.2.0-native-commands-and-backup.sparsehex"
        ),
        &[
            "backup/database.redb",
            "backup/format.riffdb",
            "backup/journal.riffextent",
            "backup/manifest.riffdb",
            "db.redb",
            "db.redb.riffdb-format-v1",
            "expected-outcomes.hex",
        ],
    );
    super::native_source_admission::upgrade_fixture(&directory.join(""));
    let path = directory.join("db.redb");
    let store =
        RedbStore::open_with_commit_profile(&path, crate::RedbCommitProfile::Standard).unwrap();
    let mut session = store.begin_structural_evidence(inputs()).unwrap();
    let structural = finish_structural(&mut session);
    let (_, historical) = validate_catalog_history(&mut session).unwrap().into_parts();
    let StructuralOpenOutcome::Clean(opened) = session.finish(structural, historical).unwrap()
    else {
        panic!("actual native command source must validate");
    };
    let (_, _, _, dormant) = opened.into_parts();
    let mut ports = dormant.into_operational_after_catalog_validation().unwrap();
    let outcomes_text = std::fs::read_to_string(directory.join("expected-outcomes.hex")).unwrap();
    assert!(outcomes_text.len() <= 64 * 1024);
    let frozen: Vec<_> = outcomes_text
        .lines()
        .take(3)
        .map(|line| {
            assert!(line.len() <= 32 * 1024 && line.len() % 2 == 0);
            let bytes: Vec<_> = line
                .as_bytes()
                .chunks_exact(2)
                .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
                .collect();
            let outcome = codec::decode_stored_outcome_v1(&bytes)
                .unwrap()
                .into_parts()
                .0;
            assert_eq!(
                ports.read_stored_outcome(outcome.identity()).unwrap(),
                Some(outcome.clone())
            );
            let provenance = ports
                .read_provenance(outcome.provenance_id())
                .unwrap()
                .unwrap();
            (outcome, bytes, provenance)
        })
        .collect();
    assert_eq!(frozen.len(), 2);
    let PendingOutboxScanV1::ExactEnd { items } = ports
        .scan_pending_outbox(
            None,
            OutboxPageLimit::new(NonZeroU16::new(8).unwrap()).unwrap(),
        )
        .unwrap()
    else {
        panic!("two-command fixture has a bounded exact pending set");
    };
    assert_eq!(items.len(), 2);
    let first = items[0].value();
    let second = items[1].value();
    let time = Timestamp::new(1_700_000_000, 0).unwrap();
    let lease = Timestamp::new(1_700_000_060, 0).unwrap();
    let claim = OutboxClaimV1::new(
        first.event_id(),
        first.status().clone(),
        OutboxDestinationIdV1::new("test-sink").unwrap(),
        time,
        lease,
    )
    .unwrap();
    assert_eq!(
        ports.claim_outbox(&claim).unwrap(),
        OutboxTransitionResultV1::Applied(claim.updated().clone())
    );
    // Controlled native transition fixture: completion releases only the first
    // event's existing hold. No campaign/archive qualification is claimed.
    let success = OutboxSucceedV1::new(claim.updated().clone(), time).unwrap();
    assert_eq!(
        ports.succeed_outbox(&success).unwrap(),
        OutboxTransitionResultV1::Applied(success.updated().clone())
    );
    let stages = ports.shared_ports().outbox_stage_census_v1();
    assert_eq!(
        stages.map(|stage| stage[0]),
        [2; 8],
        "actual claim and completion must each be attributed at the native writer"
    );
    let epoch = ports
        .shared
        .source_continuation_epoch
        .load(Ordering::Acquire);
    let before = crate::changelog_v3_roots::validate_retained_history(
        &ports.shared.database.begin_read().unwrap(),
    )
    .unwrap()
    .unwrap();
    ports.emit_logical_bytes_for_test(NonZeroU64::MIN).unwrap();
    let measured_before = ports
        .measure_logical_bytes_for_test(NonZeroU64::MIN)
        .unwrap();
    let tables = [
        crate::journal::JournalTable::Commits,
        crate::journal::JournalTable::Events,
        crate::journal::JournalTable::Outbox,
        crate::journal::JournalTable::OutboxStatus,
        crate::journal::JournalTable::Idempotency,
        crate::journal::JournalTable::Provenance,
        crate::journal::JournalTable::Audit,
    ];
    let prior_rows = {
        let read = ports.begin_composite_read().unwrap();
        tables.map(|table| read.read_range_to(table, &[], None, 32).unwrap())
    };
    let pruned = ports
        .online_retention_step(NonZeroU64::new(1).unwrap(), NonZeroU64::new(1).unwrap())
        .unwrap();
    assert!(matches!(
        pruned,
        crate::OnlineRetentionStep::Advanced {
            through: 1,
            sequences: 1
        }
    ));
    let after_read = ports.begin_composite_read().unwrap();
    let mut removed_values = 0_u64;
    let mut removed_keys = 0_u64;
    for (table, rows) in tables.into_iter().zip(prior_rows) {
        for (key, original) in rows {
            let after = after_read.read_value(table, &key).unwrap();
            if after.as_deref() == Some(original.as_ref()) {
                continue;
            }
            if after.is_none() {
                let length = if table == crate::journal::JournalTable::Commits {
                    crate::history_stub::resolve_commits_value(&original)
                        .unwrap()
                        .len()
                } else {
                    original.len()
                };
                removed_values += u64::try_from(length).unwrap();
                removed_keys += u64::try_from(key.len()).unwrap();
            } else if match table {
                crate::journal::JournalTable::Idempotency
                | crate::journal::JournalTable::Provenance => {
                    riffdb_storage_api::decode_command_locator_v1(&original).is_ok()
                }
                crate::journal::JournalTable::Audit => {
                    riffdb_storage_api::decode_command_audit_locator_v1(&original).is_ok()
                }
                _ => false,
            } {
                removed_values += u64::try_from(original.len()).unwrap();
            }
        }
    }
    drop(after_read);
    assert!(removed_values > 0 && removed_keys > 0);
    assert_eq!(measured_before, (removed_values, removed_keys));
    assert_eq!(
        ports
            .measure_logical_bytes_for_test(NonZeroU64::MIN)
            .unwrap(),
        (0, 0),
        "native pruning empties the delivered eligible prefix, while retained evidence remains"
    );
    assert_eq!(
        ports
            .shared
            .source_continuation_epoch
            .load(Ordering::Acquire),
        epoch
    );
    assert_eq!(
        ports.shared_ports().outbox_stage_census_v1(),
        stages,
        "logical pruning and source control must not be counted as outbox writes"
    );
    let next = OutboxClaimV1::new(
        second.event_id(),
        second.status().clone(),
        OutboxDestinationIdV1::new("test-sink").unwrap(),
        time,
        lease,
    )
    .unwrap();
    assert_eq!(
        ports.claim_outbox(&next).unwrap(),
        OutboxTransitionResultV1::Applied(next.updated().clone())
    );
    let successor_epoch = ports
        .shared
        .source_continuation_epoch
        .load(Ordering::Acquire);
    assert_eq!(
        successor_epoch, epoch,
        "checked canonical prune materialization must not discard the source proof"
    );
    let after = crate::changelog_v3_roots::validate_retained_history(
        &ports.shared.database.begin_read().unwrap(),
    )
    .unwrap()
    .unwrap();
    assert_eq!(after.minimum_resume(), before.minimum_resume());
    assert_eq!(
        after.tail().sequence().get(),
        before.tail().sequence().get() + 2
    );
    assert_eq!(after.tail().frontier(), before.tail().frontier());
    for (outcome, bytes, provenance) in frozen {
        let actual = ports
            .read_stored_outcome(outcome.identity())
            .unwrap()
            .unwrap();
        assert_eq!(actual, outcome);
        assert_eq!(
            codec::encode_stored_outcome_v1(&actual).unwrap().as_bytes(),
            bytes
        );
        assert_eq!(
            ports.read_provenance(outcome.provenance_id()).unwrap(),
            Some(provenance)
        );
    }
}

// req: STO-030, STO-031, STO-032, END-007, END-008, REC-001
#[test]
fn eligible_native_extent_retries_after_global_barrier_release_without_scanning_generation_holes() {
    for blocker in ["reader", "scrub", "writer", "source"] {
        use riffdb_storage_api::{
            OutboxClaimV1, OutboxDestinationIdV1, OutboxPageLimit, OutboxRepository,
            OutboxSucceedV1, OutboxTransitionResultV1, OwnedSnapshotReader, PendingOutboxScanV1,
        };
        let directory = crate::test_path::ScopedDirectory::new("extent-reader-retry");
        restore_fixture_bytes(
            &directory.join(""),
            include_str!(
                "../../../../fixtures/storage/redb-4.2.0-native-commands-and-backup.sparsehex"
            ),
            &[
                "backup/database.redb",
                "backup/format.riffdb",
                "backup/journal.riffextent",
                "backup/manifest.riffdb",
                "db.redb",
                "db.redb.riffdb-format-v1",
                "expected-outcomes.hex",
            ],
        );
        super::native_source_admission::upgrade_fixture(&directory.join(""));
        let path = directory.join("db.redb");
        let store =
            RedbStore::open_with_commit_profile(&path, crate::RedbCommitProfile::Standard).unwrap();
        let mut session = store.begin_structural_evidence(inputs()).unwrap();
        let structural = finish_structural(&mut session);
        let (_, historical) = validate_catalog_history(&mut session).unwrap().into_parts();
        let StructuralOpenOutcome::Clean(opened) = session.finish(structural, historical).unwrap()
        else {
            panic!("native command fixture must validate");
        };
        let (_, _, _, dormant) = opened.into_parts();
        let mut ports = dormant.into_operational_after_catalog_validation().unwrap();
        let PendingOutboxScanV1::ExactEnd { items } = ports
            .scan_pending_outbox(
                None,
                OutboxPageLimit::new(NonZeroU16::new(8).unwrap()).unwrap(),
            )
            .unwrap()
        else {
            panic!("two-command fixture must have an exact pending set");
        };
        assert_eq!(items.len(), 2);
        let time = Timestamp::new(1_700_000_000, 0).unwrap();
        let lease = Timestamp::new(1_700_000_060, 0).unwrap();
        let first = items[0].value();
        let claim = OutboxClaimV1::new(
            first.event_id(),
            first.status().clone(),
            OutboxDestinationIdV1::new("test-sink").unwrap(),
            time,
            lease,
        )
        .unwrap();
        assert_eq!(
            ports.claim_outbox(&claim).unwrap(),
            OutboxTransitionResultV1::Applied(claim.updated().clone())
        );
        let success = OutboxSucceedV1::new(claim.updated().clone(), time).unwrap();
        assert_eq!(
            ports.succeed_outbox(&success).unwrap(),
            OutboxTransitionResultV1::Applied(success.updated().clone())
        );
        assert!(matches!(
            ports
                .online_retention_step(NonZeroU64::new(1).unwrap(), NonZeroU64::new(1).unwrap())
                .unwrap(),
            crate::OnlineRetentionStep::Advanced {
                through: 1,
                sequences: 1
            }
        ));
        let second = items[1].value();
        let claim = OutboxClaimV1::new(
            second.event_id(),
            second.status().clone(),
            OutboxDestinationIdV1::new("test-sink").unwrap(),
            time,
            lease,
        )
        .unwrap();
        assert_eq!(
            ports.claim_outbox(&claim).unwrap(),
            OutboxTransitionResultV1::Applied(claim.updated().clone())
        );
        let mut control = ports.replication_source_control();
        assert!(!control.reclaim_history().unwrap());
        let success = OutboxSucceedV1::new(claim.updated().clone(), time).unwrap();
        assert_eq!(
            ports.succeed_outbox(&success).unwrap(),
            OutboxTransitionResultV1::Applied(success.updated().clone())
        );
        let old_reader = (blocker == "reader").then(|| ports.open_owned_snapshot().unwrap());
        if blocker != "source" {
            assert!(control.reclaim_history().unwrap());
        }
        // Fully drained native prune bytes are immutable historical state. This
        // isolated fixture seals them early and advances empty generations through
        // the existing media protocol, without adding commands or campaign evidence.
        assert_eq!(ports.shared.journal_suffix_activity().unwrap().1, 0);
        ports.shared.join_journal_sealer();
        let identity = crate::store::read_identity_from_read_transaction(
            &ports.shared.database.begin_read().unwrap(),
        )
        .unwrap();
        let active = crate::journal::journal_path(&path);
        let (header, _) = crate::journal::scan_journal_with_media(
            &crate::media::RealJournalMedia,
            &active,
            identity,
            |_| Ok(()),
        )
        .unwrap()
        .unwrap();
        crate::media::JournalMedia::rename(
            &crate::media::RealJournalMedia,
            &active,
            &crate::journal::sealing_journal_path(&path),
        )
        .unwrap();
        let sealed = crate::journal::seal_extent_with_media(
            &crate::media::RealJournalMedia,
            &path,
            identity,
        )
        .unwrap()
        .unwrap();
        let footer = crate::journal::read_sealed_extent_with_media(
            &crate::media::RealJournalMedia,
            &sealed,
            identity,
        )
        .unwrap();
        crate::journal::reset_journal_with_media(&crate::media::RealJournalMedia, &active, &header)
            .unwrap();
        crate::journal::renew_extent_above_with_media(
            &crate::media::RealJournalMedia,
            &active,
            footer.generation + 256,
        )
        .unwrap();
        let mut before = crate::changelog_v3_roots::validate_retained_history(
            &ports.shared.database.begin_read().unwrap(),
        )
        .unwrap();
        let mut epoch = ports.shared.durable_commit_epoch();
        let scrub = (blocker == "scrub").then(|| ports.shared.pin_extent_scrub().unwrap());
        let writer = (blocker == "writer").then(|| {
            crate::store::extent_reclamation::reserve_writer_for_test(&ports.shared).unwrap()
        });
        let held = ports.shared.reclaim_one_extent().unwrap();
        assert!(held.retired_generation.is_none());
        let physical = ports.shared.measure_physical_bytes_for_test().unwrap();
        if blocker == "writer" {
            assert!(
                physical.is_none(),
                "a busy writer makes the capture unknown"
            );
        } else {
            let physical = physical.unwrap();
            assert_eq!(physical.eligible_allocated_bytes, 0);
            assert_eq!(physical.sealed_extents, 1);
            assert!(physical.protected_allocated_bytes > 0);
        }
        assert_eq!(
            (
                held.deleted,
                held.recycled,
                held.held_references,
                held.held_readers
            ),
            (
                0,
                0,
                u64::from(matches!(blocker, "writer" | "source")),
                u64::from(matches!(blocker, "reader" | "scrub"))
            )
        );
        assert!(sealed.exists());
        assert_eq!(ports.shared.durable_commit_epoch(), epoch);
        let still_held = ports.shared.reclaim_one_extent().unwrap();
        assert_eq!((still_held.deleted, still_held.recycled), (0, 0));
        assert_eq!(still_held.held_readers, held.held_readers);
        if blocker != "source" {
            assert_eq!(still_held.held_references, held.held_references);
        }
        assert!(sealed.exists());
        assert_eq!(ports.shared.durable_commit_epoch(), epoch);
        drop(old_reader);
        drop(scrub);
        drop(writer);
        if blocker == "source" {
            assert!(control.reclaim_history().unwrap());
            before = crate::changelog_v3_roots::validate_retained_history(
                &ports.shared.database.begin_read().unwrap(),
            )
            .unwrap();
            epoch = ports.shared.durable_commit_epoch();
        }
        let mut refused_probes = Vec::new();
        for mode in ["expiry", "directory"] {
            let (kind, refused) =
                crate::store::extent_reclamation::physical_bytes::forced_refusal_for_test(
                    &ports.shared,
                    mode,
                )
                .unwrap();
            assert_eq!(kind, StorageErrorKind::LimitExceeded);
            refused_probes.push(refused);
        }
        use std::os::unix::fs::MetadataExt;
        let expected_allocated = std::fs::metadata(&sealed).unwrap().blocks() * 512;
        let (physical, completed_probe) = ports
            .shared
            .finished_physical_probe_for_test()
            .unwrap()
            .unwrap();
        assert_eq!(physical.eligible_allocated_bytes, expected_allocated);
        assert_eq!(physical.protected_allocated_bytes, 0);
        assert_eq!(physical.sealed_extents, 1);
        // Exercise the actual production emitter as well as the measurement
        // helper. The independent collector parses these real numeric records
        // in the native producer/collector integration check.
        let mut physical_observer =
            crate::store::extent_reclamation::physical_bytes::Observer::default();
        for _ in 0..32 {
            physical_observer.tick(&ports.shared);
        }
        let reclaimed = ports.shared.reclaim_one_extent().unwrap();
        assert_eq!(reclaimed.retired_generation, Some(footer.generation));
        assert_eq!(
            reclaimed.deleted + reclaimed.recycled,
            1,
            "released {blocker} must not defer this eligible extent across absent generations"
        );
        assert_eq!(reclaimed.held_readers + reclaimed.held_references, 0);
        assert!(!sealed.exists());
        drop(completed_probe); // Completion must release protection before owner drop.
        drop(refused_probes); // Refusals also release protection while owners remain alive.
        assert_eq!(
            ports
                .shared
                .measure_physical_bytes_for_test()
                .unwrap()
                .unwrap()
                .sealed_extents,
            0
        );
        assert_eq!(ports.shared.durable_commit_epoch(), epoch);
        assert_eq!(
            crate::changelog_v3_roots::validate_retained_history(
                &ports.shared.database.begin_read().unwrap()
            )
            .unwrap(),
            before
        );
    }
}
