//! Retained-source physical checkpoint tests, not command acknowledgement proof.
// req: REP-003, REC-001, PERF-007, STO-012

use super::*;
use crate::journal::{JournalFrame, JournalMutation, JournalTable};
use crate::store::{RedbStore, ValidatedCheckpointFrame};
use redb::ReadableDatabase;
use riffdb_storage_api::DatabaseInitializationPort;
use riffdb_storage_api::{
    AdministrationSequenceAllocator, AuthoritativeNamespaceV1 as N, ChangelogLineageV3,
    LeadershipEpochV1, proto_codec::*,
};
use riffdb_types::{AdministrationSequence, DatabaseId, DualFrontier};
use std::sync::{Arc, atomic::Ordering};

#[test]
fn live_checkpoint_materializes_the_original_v3_receipt_with_one_existing_commit() {
    let scope = crate::test_path::ScopedDirectory::new("v3-live-checkpoint");
    let path = std::env::var_os("RIFFDB_V3_LIVE_CHECKPOINT_PATH")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| scope.join("db.redb"));
    let mut store = RedbStore::open(&path).unwrap();
    let database_id =
        DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x71; 10]).unwrap();
    store.initialize_database(database_id).unwrap();
    let lineage = ChangelogLineageV3::new(database_id, 1, LeadershipEpochV1::initial()).unwrap();
    let history = crate::changelog_v3_activation::activate_validated(
        store.shared.database.begin_write().unwrap(),
        lineage,
        DualFrontier::INITIAL,
    )
    .unwrap();
    let sequence = AdministrationSequence::new(1).unwrap();
    let source = JournalFrame::service_audit(
        database_id,
        None,
        None,
        Some(sequence),
        1,
        [0x42; 32],
        vec![
            JournalMutation::put(
                JournalTable::Audit,
                crate::keys::encode_audit_key(sequence),
                b"opaque-audit".to_vec(),
            )
            .unwrap(),
            JournalMutation::replace(
                JournalTable::Meta,
                crate::layout::META_ADMINISTRATION_SEQUENCE.as_bytes(),
                encode_administration_sequence_allocator_v1(
                    AdministrationSequenceAllocator::initial(),
                )
                .unwrap()
                .as_bytes(),
                encode_administration_sequence_allocator_v1(AdministrationSequenceAllocator::Next(
                    AdministrationSequence::new(2).unwrap(),
                ))
                .unwrap()
                .into_bytes(),
            )
            .unwrap(),
            JournalMutation::replace(
                JournalTable::Meta,
                N::NextChangelogTransaction
                    .metadata_key()
                    .unwrap()
                    .as_bytes(),
                encode_changelog_transaction_allocator_v3(history.expected_allocator())
                    .unwrap()
                    .as_bytes(),
                encode_changelog_transaction_allocator_v3(
                    history.expected_allocator().allocate_one().unwrap().1,
                )
                .unwrap()
                .into_bytes(),
            )
            .unwrap(),
        ],
    )
    .unwrap();
    let transaction = store.shared.database.begin_write().unwrap();
    let expected =
        crate::changelog_v3_journal::materialize_recovered_frame(&transaction, &source).unwrap();
    transaction.abort().unwrap();
    let encoded = source.encode().unwrap();
    let lane = crate::journal::JournalLane::open(
        &crate::journal::journal_path(&path),
        &crate::journal::JournalFileHeader::with_frontiers(
            database_id,
            None,
            None,
            source.previous_hash(),
        ),
    )
    .unwrap();
    lane.submit(encoded.clone()).unwrap().wait().unwrap();
    drop(lane);
    let frame = ValidatedCheckpointFrame {
        changelog_binding: Some(expected.binding()),
        database_id,
        predecessor_sequence: None,
        covered_sequence: None,
        predecessor_administration_sequence: None,
        covered_administration_sequence: Some(sequence),
        previous_hash: source.previous_hash(),
        frame_hash: encoded.frame_hash(),
        transition_count: 1,
        command_count: 0,
        audit_count: 1,
        encoded: encoded.clone(),
        mutations: Arc::from(
            source
                .mutations()
                .iter()
                .map(|m| m.composite().unwrap())
                .collect::<Vec<_>>(),
        ),
    };
    let batch = JournalCheckpointBatch {
        database_id,
        checkpoint_sequence: None,
        checkpoint_administration_sequence: None,
        checkpoint_hash: source.previous_hash(),
        last_sequence: None,
        last_administration_sequence: Some(sequence),
        last_hash: encoded.frame_hash(),
        transition_count: 1,
        command_count: 0,
        audit_count: 1,
        encoded_bytes: encoded.as_bytes().len(),
        frames: vec![frame],
    };
    let old_pin = store.shared.database.begin_read().unwrap();
    let runtime = crate::store::JournalRuntime {
        changelog_history: Some(history.advance(&expected).unwrap()),
        lane: Arc::new(
            crate::journal::JournalLane::open(
                &crate::journal::journal_path(&path),
                &crate::journal::JournalFileHeader::with_frontiers(
                    database_id,
                    None,
                    None,
                    source.previous_hash(),
                ),
            )
            .unwrap(),
        ),
        database_id,
        last_sequence: None,
        last_administration_sequence: Some(sequence),
        last_hash: encoded.frame_hash(),
        published_sequence: None,
        published_administration_sequence: Some(sequence),
        published_hash: encoded.frame_hash(),
        suffix_transitions: 1,
        suffix_commands: 0,
        suffix_audits: 1,
        suffix_bytes: encoded.as_bytes().len(),
        suffix_physical_bytes: crate::journal::extent_frame_bytes(encoded.as_bytes().len())
            .unwrap(),
        suffix_frames: batch.frames.clone(),
        unpublished_transitions: 0,
        unpublished_commands: 0,
        unpublished_audits: 0,
        unpublished_bytes: 0,
        reanchor_required: false,
    };
    let transaction = store.shared.database.begin_write().unwrap();
    store
        .shared
        .apply_published_journal_suffix(&transaction, &runtime)
        .unwrap();
    assert_eq!(
        crate::changelog_v3_roots::read_checkpoint_roots_for_write(&transaction).unwrap(),
        Some(history.advance(&expected).unwrap())
    );
    assert_eq!(
        crate::changelog_v3_roots::validate_retained_history(
            &store.shared.database.begin_read().unwrap()
        )
        .unwrap(),
        Some(history)
    );
    transaction.abort().unwrap();
    drop(runtime);
    let epoch = store.shared.durable_commit_epoch.load(Ordering::Acquire);
    // The retained admission binding is mandatory and must match exactly.
    let mut foreign_binding = expected.binding();
    foreign_binding.prior_history_hash[0] ^= 1;
    for binding in [None, Some(foreign_binding)] {
        let mut invalid = batch.clone();
        invalid.frames[0].changelog_binding = binding;
        assert!(store.shared.materialize_checkpoint_batch(&invalid).is_err());
        assert_eq!(
            store.shared.durable_commit_epoch.load(Ordering::Acquire),
            epoch
        );
        assert_eq!(
            crate::changelog_v3_roots::validate_retained_history(
                &store.shared.database.begin_read().unwrap()
            )
            .unwrap(),
            Some(history)
        );
    }
    // A malformed final batch total must abort even after staging all frames.
    let mut invalid = batch.clone();
    invalid.encoded_bytes += 1;
    assert!(store.shared.materialize_checkpoint_batch(&invalid).is_err());
    assert_eq!(
        store.shared.durable_commit_epoch.load(Ordering::Acquire),
        epoch
    );
    assert_eq!(
        crate::changelog_v3_roots::validate_retained_history(
            &store.shared.database.begin_read().unwrap()
        )
        .unwrap(),
        Some(history)
    );
    store.shared.materialize_checkpoint_batch(&batch).unwrap();
    assert_eq!(
        store.shared.durable_commit_epoch.load(Ordering::Acquire),
        epoch + 1
    );
    let pin = store.shared.database.begin_read().unwrap();
    assert_eq!(
        crate::changelog_v3_journal::verify_materialized_frame(&pin, &source)
            .unwrap()
            .encode()
            .unwrap(),
        expected.encode().unwrap()
    );
    assert_eq!(
        crate::changelog_v3_roots::validate_retained_history(&pin).unwrap(),
        Some(history.advance(&expected).unwrap())
    );
    assert_eq!(
        crate::changelog_v3_roots::validate_retained_history(&old_pin).unwrap(),
        Some(history)
    );
    assert_eq!(
        batch.frames[0].encoded.as_bytes(),
        source.encode().unwrap().as_bytes()
    );
    assert!(store.shared.materialize_checkpoint_batch(&batch).is_err());
    assert_eq!(
        store.shared.durable_commit_epoch.load(Ordering::Acquire),
        epoch + 1
    );
}

#[test]
fn live_checkpoint_process_crashes_recover_from_retained_or_materialized_v3_bytes() {
    for edge in ["checkpoint-staged", "checkpoint-committed"] {
        let scope = crate::test_path::ScopedDirectory::new("v3-live-checkpoint-process");
        let path = scope.join("db.redb");
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("store::journal_checkpoint::tests::live_checkpoint_materializes_the_original_v3_receipt_with_one_existing_commit")
            .arg("--nocapture")
            .env("RIFFDB_V3_LIVE_CHECKPOINT_PATH", &path)
            .env("RIFFDB_V3_RECOVERY_EDGE", edge)
            .status().unwrap();
        assert_eq!(status.code(), Some(93), "{edge}");
        let database = redb::Database::open(&path).unwrap();
        let history =
            crate::changelog_v3_roots::validate_retained_history(&database.begin_read().unwrap())
                .unwrap()
                .unwrap();
        assert_eq!(
            history.tail().sequence().get(),
            if edge == "checkpoint-staged" { 1 } else { 2 }
        );
        let journal = crate::journal::journal_path(&path);
        let mut original = None;
        let (_, tail) =
            crate::journal::scan_journal(&journal, history.lineage().database_id(), |frame| {
                original = Some(frame.clone());
                Ok(())
            })
            .unwrap()
            .unwrap();
        assert_eq!(tail.transition_count, 1);
        let source = original.unwrap();
        for _ in 0..2 {
            crate::journal::recover_journal_path(
                &database,
                &journal,
                history.lineage().database_id(),
            )
            .unwrap();
            let pin = database.begin_read().unwrap();
            let receipt =
                crate::changelog_v3_journal::verify_materialized_frame(&pin, &source).unwrap();
            assert_eq!(receipt.binding().sequence.get(), 2);
            assert_eq!(
                crate::changelog_v3_roots::validate_retained_history(&pin)
                    .unwrap()
                    .unwrap()
                    .tail()
                    .sequence()
                    .get(),
                2
            );
        }
    }
}
