//! Retained-source physical checkpoint tests, not command acknowledgement proof.
// req: REP-003, REC-001, PERF-007, STO-012

use super::*;
use crate::journal::{JournalFrame, JournalMutation, JournalTable};
use crate::store::{RedbStore, ValidatedCheckpointFrame};
use redb::ReadableDatabase;
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
    store.initialize_legacy_fixture(database_id).unwrap();
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
                crate::test_audit::canonical_denied_service_audit(sequence),
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
    let extent_generation = lane.generation();
    drop(lane);
    let frame = ValidatedCheckpointFrame {
        source_reclamation: None,
        segment_summaries: Arc::from(Vec::new()),
        extent_generation,
        placement: crate::journal::frame_placement_after(0, &encoded).unwrap(),
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

/// Builds `count` chained service-audit frames that record their changelog
/// binding, writes them to the live extent, and returns the checkpoint batch
/// over them with each frame's independently derived expected receipt.
fn chained_audit_batch(
    store: &RedbStore,
    path: &std::path::Path,
    database_id: DatabaseId,
    history: riffdb_storage_api::ChangelogHistoryStateV3,
    count: u64,
) -> (
    JournalCheckpointBatch,
    Vec<JournalFrame>,
    Vec<riffdb_storage_api::AuthoritativeTransactionV3>,
) {
    chained_audit_batch_with_source_cut(store, path, database_id, history, count, false)
}

fn chained_audit_batch_with_source_cut(
    store: &RedbStore,
    path: &std::path::Path,
    database_id: DatabaseId,
    history: riffdb_storage_api::ChangelogHistoryStateV3,
    count: u64,
    include_source_cut: bool,
) -> (
    JournalCheckpointBatch,
    Vec<JournalFrame>,
    Vec<riffdb_storage_api::AuthoritativeTransactionV3>,
) {
    let catalog_digest = history.lineage().catalog_digest();
    let origin_hash = [0x42; 32];
    let scratch = store.shared.database.begin_write().unwrap();
    let mut sources = Vec::new();
    let mut expected = Vec::new();
    let mut previous_hash = origin_hash;
    let mut allocator = history.expected_allocator();
    for index in 1..=count {
        let sequence = AdministrationSequence::new(index).unwrap();
        let before = if index == 1 {
            AdministrationSequenceAllocator::initial()
        } else {
            AdministrationSequenceAllocator::Next(sequence)
        };
        let after =
            AdministrationSequenceAllocator::Next(AdministrationSequence::new(index + 1).unwrap());
        let next_allocator = allocator.allocate_one().unwrap().1;
        let unbound = JournalFrame::service_audit(
            database_id,
            None,
            (index > 1).then(|| AdministrationSequence::new(index - 1).unwrap()),
            Some(sequence),
            1,
            previous_hash,
            vec![
                JournalMutation::put(
                    JournalTable::Audit,
                    crate::keys::encode_audit_key(sequence),
                    crate::test_audit::canonical_denied_service_audit(sequence),
                )
                .unwrap(),
                JournalMutation::replace(
                    JournalTable::Meta,
                    crate::layout::META_ADMINISTRATION_SEQUENCE.as_bytes(),
                    encode_administration_sequence_allocator_v1(before)
                        .unwrap()
                        .as_bytes(),
                    encode_administration_sequence_allocator_v1(after)
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
                    encode_changelog_transaction_allocator_v3(allocator)
                        .unwrap()
                        .as_bytes(),
                    encode_changelog_transaction_allocator_v3(next_allocator)
                        .unwrap()
                        .into_bytes(),
                )
                .unwrap(),
            ],
        )
        .unwrap();
        // Sequential recovery-path application is the oracle for each receipt.
        let receipt =
            crate::changelog_v3_journal::materialize_recovered_frame(&scratch, &unbound).unwrap();
        let source = unbound.with_changelog(Some(crate::journal::FrameChangelogBinding::new(
            &receipt.binding(),
            catalog_digest,
        )));
        previous_hash = source.encode().unwrap().frame_hash();
        allocator = next_allocator;
        sources.push(source);
        expected.push(receipt);
        if include_source_cut && index == 2 {
            use crate::journal::{SourceReceiptPreimage, SourceReclamationProof};
            use riffdb_storage_api::{
                AuthoritativeTransactionV3, ChangelogHistoryPointV3 as Point,
                ChangelogHistoryStateV3 as History,
            };
            let predecessor = crate::changelog_v3_roots::read_checkpoint_roots_for_write(&scratch)
                .unwrap()
                .unwrap();
            let table = scratch
                .open_table(crate::changelog_v3_activation::HISTORY)
                .unwrap();
            let first = predecessor.minimum_resume().sequence().get();
            let canonical = |sequence: u64| {
                crate::history_stub::resolve_history_value(
                    table
                        .get(sequence.to_be_bytes().as_slice())
                        .unwrap()
                        .unwrap()
                        .value(),
                )
                .unwrap()
                .to_vec()
            };
            let removed = SourceReceiptPreimage::capture(&canonical(first)).unwrap();
            let floor = Point::from_receipt(
                &AuthoritativeTransactionV3::decode(&canonical(first + 1)).unwrap(),
            )
            .unwrap();
            let retained = History::new(
                predecessor.lineage(),
                predecessor.anchor(),
                predecessor.tail(),
                floor,
            )
            .unwrap();
            drop(table);
            let source = JournalFrame::source_reclamation(
                SourceReclamationProof::new(predecessor, retained, vec![removed]).unwrap(),
                previous_hash,
            )
            .unwrap();
            let receipt =
                crate::changelog_v3_journal::materialize_recovered_frame(&scratch, &source)
                    .unwrap();
            previous_hash = source.encode().unwrap().frame_hash();
            allocator = crate::changelog_v3_roots::read_checkpoint_roots_for_write(&scratch)
                .unwrap()
                .unwrap()
                .expected_allocator();
            sources.push(source);
            expected.push(receipt);
        }
    }
    scratch.abort().unwrap();

    let lane = crate::journal::JournalLane::open(
        &crate::journal::journal_path(path),
        &crate::journal::JournalFileHeader::with_frontiers(database_id, None, None, origin_hash),
    )
    .unwrap();
    let mut frames = Vec::new();
    let mut used = 0_usize;
    let mut encoded_bytes = 0_usize;
    for (source, receipt) in sources.iter().zip(&expected) {
        let encoded = source.encode().unwrap();
        lane.submit(encoded.clone()).unwrap().wait().unwrap();
        let placement = crate::journal::frame_placement_after(used, &encoded).unwrap();
        used += placement.padded_len;
        encoded_bytes += encoded.as_bytes().len();
        frames.push(ValidatedCheckpointFrame {
            source_reclamation: source.source_reclamation_proof().cloned().map(Arc::new),
            segment_summaries: Arc::from(Vec::new()),
            extent_generation: lane.generation(),
            placement,
            changelog_binding: Some(receipt.binding()),
            database_id,
            predecessor_sequence: None,
            covered_sequence: None,
            predecessor_administration_sequence: source.predecessor_administration_sequence(),
            covered_administration_sequence: source.covered_administration_sequence(),
            previous_hash: source.previous_hash(),
            frame_hash: encoded.frame_hash(),
            transition_count: source.transition_count(),
            command_count: source.command_count(),
            audit_count: source.audit_count(),
            encoded,
            mutations: Arc::from(
                source
                    .mutations()
                    .iter()
                    .map(|m| m.composite().unwrap())
                    .collect::<Vec<_>>(),
            ),
        });
    }
    drop(lane);
    let count = usize::try_from(count).unwrap();
    let batch = JournalCheckpointBatch {
        database_id,
        checkpoint_sequence: None,
        checkpoint_administration_sequence: None,
        checkpoint_hash: origin_hash,
        last_sequence: None,
        last_administration_sequence: frames.last().unwrap().covered_administration_sequence,
        last_hash: frames.last().unwrap().frame_hash,
        transition_count: frames.len(),
        command_count: 0,
        audit_count: count,
        encoded_bytes,
        frames,
    };
    (batch, sources, expected)
}

// req: REP-003, REC-001, PERF-007
#[test]
fn multi_frame_checkpoint_validates_roots_once_per_batch_and_matches_sequential_receipts() {
    let scope = crate::test_path::ScopedDirectory::new("v3-chained-checkpoint");
    let path = scope.join("db.redb");
    let mut store = RedbStore::open(&path).unwrap();
    let database_id =
        DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x72; 10]).unwrap();
    store.initialize_legacy_fixture(database_id).unwrap();
    let lineage = ChangelogLineageV3::new(database_id, 1, LeadershipEpochV1::initial()).unwrap();
    let history = crate::changelog_v3_activation::activate_validated(
        store.shared.database.begin_write().unwrap(),
        lineage,
        DualFrontier::INITIAL,
    )
    .unwrap();
    let (batch, sources, expected) = chained_audit_batch(&store, &path, database_id, history, 3);
    let epoch = store.shared.durable_commit_epoch.load(Ordering::Acquire);

    // A middle frame whose recorded binding disagrees with the in-transaction
    // chain is refused, and nothing commits.
    let mut invalid = batch.clone();
    let mut foreign = expected[1].binding();
    foreign.prior_history_hash[0] ^= 1;
    invalid.frames[1].changelog_binding = Some(foreign);
    assert!(store.shared.materialize_checkpoint_batch(&invalid).is_err());
    // A middle frame that skips its allocator before-image is refused too.
    let mut invalid = batch.clone();
    invalid.frames[1].mutations = Arc::clone(&batch.frames[0].mutations);
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

    crate::changelog_v3_roots::WRITE_ROOT_READS.with(|reads| reads.set(0));
    store.shared.materialize_checkpoint_batch(&batch).unwrap();
    // Once before the first frame and once after the last, not per frame.
    assert_eq!(
        crate::changelog_v3_roots::WRITE_ROOT_READS.with(std::cell::Cell::get),
        2
    );
    assert_eq!(
        store.shared.durable_commit_epoch.load(Ordering::Acquire),
        epoch + 1
    );

    let pin = store.shared.database.begin_read().unwrap();
    let mut advanced = history;
    for (source, receipt) in sources.iter().zip(&expected) {
        assert_eq!(
            crate::changelog_v3_journal::verify_materialized_frame(&pin, source)
                .unwrap()
                .encode()
                .unwrap(),
            receipt.encode().unwrap()
        );
        advanced = advanced.advance(receipt).unwrap();
    }
    assert_eq!(
        crate::changelog_v3_roots::validate_retained_history(&pin).unwrap(),
        Some(advanced)
    );
}

// req: REC-001, STO-030, STO-031, STO-032
#[test]
fn chained_checkpoint_carries_a_source_cut_floor_into_later_audit_frames() {
    let scope = crate::test_path::ScopedDirectory::new("v3-chained-source-cut");
    let path = scope.join("db.redb");
    let mut store = RedbStore::open(&path).unwrap();
    let database_id =
        DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x73; 10]).unwrap();
    store.initialize_legacy_fixture(database_id).unwrap();
    let lineage = ChangelogLineageV3::new(database_id, 1, LeadershipEpochV1::initial()).unwrap();
    let history = crate::changelog_v3_activation::activate_validated(
        store.shared.database.begin_write().unwrap(),
        lineage,
        DualFrontier::INITIAL,
    )
    .unwrap();
    let (batch, sources, receipts) =
        chained_audit_batch_with_source_cut(&store, &path, database_id, history, 3, true);
    assert_eq!(sources.len(), 4);
    assert!(sources[2].source_reclamation_proof().is_some());
    let original = store.shared.database.begin_read().unwrap();
    let epoch = store.shared.durable_commit_epoch.load(Ordering::Acquire);
    let mut missing_proof = batch.clone();
    missing_proof.frames[2].source_reclamation = None;
    assert!(
        store
            .shared
            .materialize_checkpoint_batch(&missing_proof)
            .is_err()
    );
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
    let current = store.shared.database.begin_read().unwrap();
    let mut expected = history;
    for (source, receipt) in sources.iter().zip(&receipts) {
        expected = expected.advance(receipt).unwrap();
        if let Some(proof) = source.source_reclamation_proof() {
            expected = riffdb_storage_api::ChangelogHistoryStateV3::new(
                expected.lineage(),
                expected.anchor(),
                expected.tail(),
                proof.retained().minimum_resume(),
            )
            .unwrap();
        }
        assert_eq!(
            crate::changelog_v3_journal::verify_materialized_frame(&current, source).unwrap(),
            *receipt
        );
    }
    assert_eq!(
        store.shared.durable_commit_epoch.load(Ordering::Acquire),
        epoch + 1
    );
    assert_eq!(
        crate::changelog_v3_roots::validate_retained_history(&current).unwrap(),
        Some(expected)
    );
    assert_eq!(
        crate::changelog_v3_roots::validate_retained_history(&original).unwrap(),
        Some(history)
    );
}

/// A store activated for V3 with three chained frames durable only in its
/// journal, as after a crash before their checkpoint.
fn store_with_three_journaled_frames(
    label: &str,
) -> (
    crate::test_path::ScopedDirectory,
    std::path::PathBuf,
    RedbStore,
    DatabaseId,
    riffdb_storage_api::ChangelogHistoryStateV3,
    JournalCheckpointBatch,
    Vec<JournalFrame>,
    Vec<riffdb_storage_api::AuthoritativeTransactionV3>,
) {
    let scope = crate::test_path::ScopedDirectory::new(label);
    let path = scope.join("db.redb");
    let mut store = RedbStore::open(&path).unwrap();
    let database_id =
        DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x73; 10]).unwrap();
    store.initialize_legacy_fixture(database_id).unwrap();
    let lineage = ChangelogLineageV3::new(database_id, 1, LeadershipEpochV1::initial()).unwrap();
    let history = crate::changelog_v3_activation::activate_validated(
        store.shared.database.begin_write().unwrap(),
        lineage,
        DualFrontier::INITIAL,
    )
    .unwrap();
    let (batch, sources, expected) = chained_audit_batch(&store, &path, database_id, history, 3);
    (
        scope,
        path,
        store,
        database_id,
        history,
        batch,
        sources,
        expected,
    )
}

fn assert_materialized_as_sequential(
    database: &redb::Database,
    history: riffdb_storage_api::ChangelogHistoryStateV3,
    sources: &[JournalFrame],
    expected: &[riffdb_storage_api::AuthoritativeTransactionV3],
) {
    let pin = database.begin_read().unwrap();
    let mut advanced = history;
    for (source, receipt) in sources.iter().zip(expected) {
        assert_eq!(
            crate::changelog_v3_journal::verify_materialized_frame(&pin, source)
                .unwrap()
                .encode()
                .unwrap(),
            receipt.encode().unwrap()
        );
        advanced = advanced.advance(receipt).unwrap();
    }
    assert_eq!(
        crate::changelog_v3_roots::validate_retained_history(&pin).unwrap(),
        Some(advanced)
    );
}

// req: REC-001, REP-003, PERF-007
#[test]
fn recovery_replay_validates_roots_once_per_transaction_and_matches_sequential_receipts() {
    let (_scope, path, store, database_id, history, _batch, sources, expected) =
        store_with_three_journaled_frames("v3-chained-recovery");
    let shared = Arc::clone(&store.shared);
    drop(store);
    // A real open registers this before recovery; stubs resolve through it.
    shared.register_history_resolver(database_id);
    crate::changelog_v3_roots::WRITE_ROOT_READS.with(|reads| reads.set(0));
    crate::journal::recover_journal_path(
        &shared.database,
        &crate::journal::journal_path(&path),
        database_id,
    )
    .unwrap();
    // Once before the first replayed frame and once after the last.
    assert_eq!(
        crate::changelog_v3_roots::WRITE_ROOT_READS.with(std::cell::Cell::get),
        2
    );
    assert_materialized_as_sequential(&shared.database, history, &sources, &expected);
}

// req: REC-001, REP-003, PERF-007
#[test]
fn published_suffix_drain_validates_roots_once_per_transaction_and_matches_sequential_receipts() {
    let (_scope, path, store, database_id, history, batch, sources, expected) =
        store_with_three_journaled_frames("v3-chained-drain");
    let last = batch.frames.last().unwrap();
    let runtime = crate::store::JournalRuntime {
        changelog_history: Some(
            expected
                .iter()
                .fold(history, |state, receipt| state.advance(receipt).unwrap()),
        ),
        lane: Arc::new(
            crate::journal::JournalLane::open(
                &crate::journal::journal_path(&path),
                &crate::journal::JournalFileHeader::with_frontiers(
                    database_id,
                    None,
                    None,
                    batch.checkpoint_hash,
                ),
            )
            .unwrap(),
        ),
        database_id,
        last_sequence: None,
        last_administration_sequence: last.covered_administration_sequence,
        last_hash: last.frame_hash,
        published_sequence: None,
        published_administration_sequence: last.covered_administration_sequence,
        published_hash: last.frame_hash,
        suffix_transitions: batch.transition_count,
        suffix_commands: 0,
        suffix_audits: batch.audit_count,
        suffix_bytes: batch.encoded_bytes,
        suffix_physical_bytes: batch
            .frames
            .iter()
            .map(|frame| frame.placement.padded_len)
            .sum(),
        suffix_frames: batch.frames.clone(),
        unpublished_transitions: 0,
        unpublished_commands: 0,
        unpublished_audits: 0,
        unpublished_bytes: 0,
        reanchor_required: false,
    };
    let transaction = store.shared.database.begin_write().unwrap();
    crate::changelog_v3_roots::WRITE_ROOT_READS.with(|reads| reads.set(0));
    store
        .shared
        .apply_published_journal_suffix(&transaction, &runtime)
        .unwrap();
    assert_eq!(
        crate::changelog_v3_roots::WRITE_ROOT_READS.with(std::cell::Cell::get),
        2
    );
    transaction.commit().unwrap();
    drop(runtime);
    assert_materialized_as_sequential(&store.shared.database, history, &sources, &expected);
}

fn store_with_journaled_source_cut(
    label: &str,
) -> (
    crate::test_path::ScopedDirectory,
    std::path::PathBuf,
    RedbStore,
    DatabaseId,
    riffdb_storage_api::ChangelogHistoryStateV3,
    JournalCheckpointBatch,
    Vec<JournalFrame>,
    Vec<riffdb_storage_api::AuthoritativeTransactionV3>,
) {
    let scope = crate::test_path::ScopedDirectory::new(label);
    let path = scope.join("db.redb");
    let mut store = RedbStore::open(&path).unwrap();
    let database_id =
        DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x73; 10]).unwrap();
    store.initialize_legacy_fixture(database_id).unwrap();
    let lineage = ChangelogLineageV3::new(database_id, 1, LeadershipEpochV1::initial()).unwrap();
    let history = crate::changelog_v3_activation::activate_validated(
        store.shared.database.begin_write().unwrap(),
        lineage,
        DualFrontier::INITIAL,
    )
    .unwrap();
    let (batch, sources, expected) =
        chained_audit_batch_with_source_cut(&store, &path, database_id, history, 3, true);
    (
        scope,
        path,
        store,
        database_id,
        history,
        batch,
        sources,
        expected,
    )
}

fn assert_source_cut_materialized_as_sequential(
    database: &redb::Database,
    history: riffdb_storage_api::ChangelogHistoryStateV3,
    sources: &[JournalFrame],
    expected: &[riffdb_storage_api::AuthoritativeTransactionV3],
) {
    let pin = database.begin_read().unwrap();
    let mut advanced = history;
    for (source, receipt) in sources.iter().zip(expected) {
        assert_eq!(
            crate::changelog_v3_journal::verify_materialized_frame(&pin, source).unwrap(),
            *receipt
        );
        advanced = advanced.advance(receipt).unwrap();
        if let Some(proof) = source.source_reclamation_proof() {
            advanced = riffdb_storage_api::ChangelogHistoryStateV3::new(
                advanced.lineage(),
                advanced.anchor(),
                advanced.tail(),
                proof.retained().minimum_resume(),
            )
            .unwrap();
        }
    }
    assert_eq!(
        crate::changelog_v3_roots::validate_retained_history(&pin).unwrap(),
        Some(advanced)
    );
}

// req: REC-001, REP-003, STO-030, STO-031, STO-032
#[test]
fn recovery_replay_carries_a_source_cut_floor_into_later_audit_frames() {
    let (_scope, path, store, database_id, history, _batch, sources, expected) =
        store_with_journaled_source_cut("v3-chained-cut-recovery");
    let shared = Arc::clone(&store.shared);
    drop(store);
    // A real open registers this before recovery; stubs resolve through it.
    shared.register_history_resolver(database_id);
    crate::changelog_v3_roots::WRITE_ROOT_READS.with(|reads| reads.set(0));
    crate::journal::recover_journal_path(
        &shared.database,
        &crate::journal::journal_path(&path),
        database_id,
    )
    .unwrap();
    assert_source_cut_materialized_as_sequential(&shared.database, history, &sources, &expected);
}

// req: REC-001, REP-003, STO-030, STO-031, STO-032
#[test]
fn published_suffix_drain_carries_a_source_cut_floor_into_later_audit_frames() {
    let (_scope, path, store, database_id, history, batch, sources, expected) =
        store_with_journaled_source_cut("v3-chained-cut-drain");
    let last = batch.frames.last().unwrap();
    let runtime = crate::store::JournalRuntime {
        changelog_history: Some(expected.iter().zip(&sources).fold(
            history,
            |state, (receipt, source)| {
                let successor = state.advance(receipt).unwrap();
                match source.source_reclamation_proof() {
                    None => successor,
                    Some(proof) => riffdb_storage_api::ChangelogHistoryStateV3::new(
                        successor.lineage(),
                        successor.anchor(),
                        successor.tail(),
                        proof.retained().minimum_resume(),
                    )
                    .unwrap(),
                }
            },
        )),
        lane: Arc::new(
            crate::journal::JournalLane::open(
                &crate::journal::journal_path(&path),
                &crate::journal::JournalFileHeader::with_frontiers(
                    database_id,
                    None,
                    None,
                    batch.checkpoint_hash,
                ),
            )
            .unwrap(),
        ),
        database_id,
        last_sequence: None,
        last_administration_sequence: last.covered_administration_sequence,
        last_hash: last.frame_hash,
        published_sequence: None,
        published_administration_sequence: last.covered_administration_sequence,
        published_hash: last.frame_hash,
        suffix_transitions: batch.transition_count,
        suffix_commands: 0,
        suffix_audits: batch.audit_count,
        suffix_bytes: batch.encoded_bytes,
        suffix_physical_bytes: batch
            .frames
            .iter()
            .map(|frame| frame.placement.padded_len)
            .sum(),
        suffix_frames: batch.frames.clone(),
        unpublished_transitions: 0,
        unpublished_commands: 0,
        unpublished_audits: 0,
        unpublished_bytes: 0,
        reanchor_required: false,
    };
    let transaction = store.shared.database.begin_write().unwrap();
    crate::changelog_v3_roots::WRITE_ROOT_READS.with(|reads| reads.set(0));
    store
        .shared
        .apply_published_journal_suffix(&transaction, &runtime)
        .unwrap();
    transaction.commit().unwrap();
    drop(runtime);
    assert_source_cut_materialized_as_sequential(
        &store.shared.database,
        history,
        &sources,
        &expected,
    );
}
