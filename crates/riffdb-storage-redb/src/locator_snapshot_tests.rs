//! ADR-0335 Decisions 2 & 3 tests for `<db>.locator.riffsnapshot` persistence,
//! bounded `(C, P]` suffix folding at startup, and fail-safe fallback on
//! corrupt or lineage-mismatched sidecar files.

use super::*;
use crate::locator_snapshot::{RiffLocatorSnapshotV1, crc32c, locator_snapshot_path};
use riffdb_storage_api::AuthoritativePointReader;

fn open_bounded_dirty(path: &std::path::Path) -> crate::RedbOperationalPorts {
    let store = RedbStore::open(path).expect("reopen database");
    let mut session = store
        .begin_structural_evidence(inputs())
        .expect("begin bounded dirty evidence");
    assert!(
        session.bounded_dirty_startup_path(),
        "expected bounded dirty startup path"
    );
    let structural = finish_structural(&mut session);
    let (catalog, historical) = validate_catalog_history(&mut session)
        .expect("validate catalog history")
        .into_parts();
    assert!(matches!(catalog, CatalogHistoryOutcome::Ready(_)));
    let StructuralOpenOutcome::Clean(opened) = session
        .finish(structural, historical)
        .expect("finish bounded startup")
    else {
        panic!("bounded startup must produce clean structural outcome");
    };
    let (_, _, _, dormant) = opened.into_parts();
    dormant
        .into_operational_after_catalog_validation()
        .expect("activate operational ports")
}

// req: REC-001, REC-002, REC-005, STO-032, TXN-042
#[test]
fn checkpoint_locator_snapshot_fold_publishes_complete_at_startup() {
    let root = crate::test_path::ScopedDirectory::new("locator-snapshot-fold");
    let db_path = root.join("db.redb");
    let snapshot_path = locator_snapshot_path(&db_path);
    let id =
        riffdb_types::DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x71; 10])
            .unwrap();
    let mut store = RedbStore::open(&db_path).unwrap();
    store.initialize_projection_source_fixture(id).unwrap();
    let mut session = store.begin_structural_evidence(inputs()).unwrap();
    let structural = finish_structural(&mut session);
    let (_, historical) = session
        .validate_startup_catalog_history()
        .unwrap()
        .into_parts();
    let StructuralOpenOutcome::Clean(opened) = session.finish(structural, historical).unwrap()
    else {
        panic!("fresh initialization must validate");
    };
    let (_, _, _, dormant) = opened.into_parts();
    let mut ports = dormant.into_operational_after_catalog_validation().unwrap();
    super::native_source_command_fixture::activate(&mut ports).unwrap();
    super::native_source_command_fixture::append_journaled(&ports, 1).unwrap();
    super::native_source_command_fixture::append_journaled(&ports, 2).unwrap();
    assert_eq!(
        ports.complete_graceful_close().lifecycle(),
        crate::GracefulLifecycleOutcomeV1::CleanCommitted
    );
    drop(ports);

    let snapshot_bytes = std::fs::read(&snapshot_path).expect("snapshot persisted on close");
    let decoded = RiffLocatorSnapshotV1::decode(&snapshot_bytes).expect("valid v1 snapshot");
    assert_eq!(
        decoded.checkpoint_application_sequence,
        CommitSequence::new(2)
    );
    assert_eq!(decoded.segments.len(), 2);

    // Case 1: Reopen with C == P (no uncheckpointed suffix).
    let ports = open_bounded_dirty(&db_path);
    assert!(
        ports.initial_native_dependency().unwrap().is_none(),
        "C == P reopen with valid locator snapshot must publish LocatorCapability::Complete at readiness"
    );
    {
        let access = ports.begin_composite_read().unwrap();
        let complete = access
            .locator_capability()
            .require_complete()
            .expect("complete capability immediately at startup");
        assert!(
            complete
                .has_command_at(CommitSequence::new(1).unwrap())
                .unwrap()
        );
        assert!(
            complete
                .has_command_at(CommitSequence::new(2).unwrap())
                .unwrap()
        );
    }
    assert!(
        ports
            .read_commit(CommitSequence::new(1).unwrap())
            .unwrap()
            .is_some()
    );
    assert!(
        ports
            .read_commit(CommitSequence::new(2).unwrap())
            .unwrap()
            .is_some()
    );

    // Case 2: Append commands 3 and 4 to the journal suffix without checkpointing,
    // then drop ports dirty so C == 2 in database.redb + snapshot and (2, 4] is in the journal.
    super::native_source_command_fixture::append_journaled(&ports, 3).unwrap();
    super::native_source_command_fixture::append_journaled(&ports, 4).unwrap();
    assert_eq!(ports.shared.journal_suffix_activity().unwrap().1, 2);
    drop(ports);

    let ports = open_bounded_dirty(&db_path);
    assert!(
        ports.initial_native_dependency().unwrap().is_none(),
        "dirty reopen with uncheckpointed suffix (C, P] must fold suffix and publish Complete at readiness"
    );
    {
        let access = ports.begin_composite_read().unwrap();
        let complete = access
            .locator_capability()
            .require_complete()
            .expect("complete capability after folding (2, 4] suffix");
        for seq in 1..=4 {
            assert!(
                complete
                    .has_command_at(CommitSequence::new(seq).unwrap())
                    .unwrap(),
                "missing command at sequence {seq}"
            );
        }
    }
    for seq in 1..=4 {
        assert!(
            ports
                .read_commit(CommitSequence::new(seq).unwrap())
                .unwrap()
                .is_some(),
            "read_commit failed for sequence {seq}"
        );
    }
}

// req: REC-001, REC-002, REC-005, STO-032, TXN-042
#[test]
fn corrupt_or_mismatched_locator_snapshot_falls_back_to_segment_rebuild() {
    let root = crate::test_path::ScopedDirectory::new("locator-snapshot-fallback");
    let db_path = root.join("db.redb");
    let snapshot_path = locator_snapshot_path(&db_path);
    let id =
        riffdb_types::DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x71; 10])
            .unwrap();
    let mut store = RedbStore::open(&db_path).unwrap();
    store.initialize_projection_source_fixture(id).unwrap();
    let mut session = store.begin_structural_evidence(inputs()).unwrap();
    let structural = finish_structural(&mut session);
    let (_, historical) = session
        .validate_startup_catalog_history()
        .unwrap()
        .into_parts();
    let StructuralOpenOutcome::Clean(opened) = session.finish(structural, historical).unwrap()
    else {
        panic!("fresh initialization must validate");
    };
    let (_, _, _, dormant) = opened.into_parts();
    let mut ports = dormant.into_operational_after_catalog_validation().unwrap();
    super::native_source_command_fixture::activate(&mut ports).unwrap();
    super::native_source_command_fixture::append_journaled(&ports, 1).unwrap();
    super::native_source_command_fixture::append_journaled(&ports, 2).unwrap();
    assert_eq!(
        ports.complete_graceful_close().lifecycle(),
        crate::GracefulLifecycleOutcomeV1::CleanCommitted
    );
    drop(ports);

    let valid_bytes = std::fs::read(&snapshot_path).expect("valid snapshot bytes");

    // 1. Truncated snapshot file.
    std::fs::write(&snapshot_path, &valid_bytes[..20]).unwrap();
    {
        let ports = open_bounded_dirty(&db_path);
        let waiter = ports
            .initial_native_dependency()
            .unwrap()
            .expect("truncated snapshot must fall back to background locator builder");
        waiter.wait().unwrap();
        assert!(
            ports
                .read_commit(CommitSequence::new(1).unwrap())
                .unwrap()
                .is_some()
        );
        assert!(
            ports
                .read_commit(CommitSequence::new(2).unwrap())
                .unwrap()
                .is_some()
        );
    }

    // 2. CRC32C-corrupted snapshot file.
    let mut crc_corrupt = valid_bytes.clone();
    let last_idx = crc_corrupt.len() - 1;
    crc_corrupt[last_idx] ^= 0xff;
    std::fs::write(&snapshot_path, &crc_corrupt).unwrap();
    {
        let ports = open_bounded_dirty(&db_path);
        let waiter = ports
            .initial_native_dependency()
            .unwrap()
            .expect("CRC32C-corrupted snapshot must fall back to background locator builder");
        waiter.wait().unwrap();
        assert!(
            ports
                .read_commit(CommitSequence::new(1).unwrap())
                .unwrap()
                .is_some()
        );
        assert!(
            ports
                .read_commit(CommitSequence::new(2).unwrap())
                .unwrap()
                .is_some()
        );
    }

    // 3. Lineage-mismatched snapshot file (valid CRC32C, wrong history_incarnation).
    let mut mismatched = valid_bytes.clone();
    // Offset 8 (magic) + 2 (version) + 16 (database_id) = 26..34 is history_incarnation.
    mismatched[26..34].copy_from_slice(&999_u64.to_le_bytes());
    let payload_len = mismatched.len() - 4;
    let new_crc = crc32c(&mismatched[..payload_len]);
    mismatched[payload_len..].copy_from_slice(&new_crc.to_le_bytes());
    assert!(RiffLocatorSnapshotV1::decode(&mismatched).is_ok());
    std::fs::write(&snapshot_path, &mismatched).unwrap();
    {
        let ports = open_bounded_dirty(&db_path);
        let waiter = ports
            .initial_native_dependency()
            .unwrap()
            .expect("lineage-mismatched snapshot must fall back to background locator builder");
        waiter.wait().unwrap();
        assert!(
            ports
                .read_commit(CommitSequence::new(1).unwrap())
                .unwrap()
                .is_some()
        );
        assert!(
            ports
                .read_commit(CommitSequence::new(2).unwrap())
                .unwrap()
                .is_some()
        );
    }
}
