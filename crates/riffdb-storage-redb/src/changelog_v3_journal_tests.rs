//! Physical source/transaction tests use canonical audit bytes; they do not
//! claim startup service-index validation or application acknowledgement evidence.
// req: REP-003, REC-001, STO-012

use super::*;
use crate::journal::{JournalFrame, JournalMutation, JournalTable};
use riffdb_types::AdministrationSequence;

fn audit_source(history: ChangelogHistoryStateV3) -> JournalFrame {
    let predecessor = history.tail().frontier();
    let assigned = AdministrationSequence::new(
        predecessor
            .administration()
            .map_or(0, AdministrationSequence::get)
            + 1,
    )
    .unwrap();
    let before_admin = AdministrationSequenceAllocator::Next(assigned);
    let after_admin = AdministrationSequenceAllocator::Next(
        AdministrationSequence::new(assigned.get() + 1).unwrap(),
    );
    JournalFrame::service_audit(
        history.lineage().database_id(),
        predecessor.application(),
        predecessor.administration(),
        Some(assigned),
        1,
        [0x42; 32],
        vec![
            JournalMutation::put(
                JournalTable::Audit,
                crate::keys::encode_audit_key(assigned),
                crate::test_audit::canonical_denied_service_audit(assigned),
            )
            .unwrap(),
            JournalMutation::replace(
                JournalTable::Meta,
                META_ADMINISTRATION_SEQUENCE.as_bytes(),
                encode_administration_sequence_allocator_v1(before_admin)
                    .unwrap()
                    .as_bytes(),
                encode_administration_sequence_allocator_v1(after_admin)
                    .unwrap()
                    .into_bytes(),
            )
            .unwrap(),
            JournalMutation::replace(
                JournalTable::Meta,
                key(N::NextChangelogTransaction).unwrap().as_bytes(),
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
    .unwrap()
}

fn publish_recovery_source(path: &Path, source: &JournalFrame) {
    publish_recovery_sources(path, &[source]);
}

fn publish_recovery_sources(path: &Path, sources: &[&JournalFrame]) {
    let source = sources[0];
    let header = crate::journal::JournalFileHeader::with_frontiers(
        source.database_id(),
        source.predecessor_sequence(),
        source.predecessor_administration_sequence(),
        source.previous_hash(),
    );
    let lane = crate::journal::JournalLane::open(path, &header).unwrap();
    for source in sources {
        lane.submit(source.encode().unwrap())
            .unwrap()
            .wait()
            .unwrap();
    }
}

#[test]
fn journal_recovery_preserves_a_materialized_prefix_and_replays_only_its_exact_successor() {
    let scope = crate::test_path::ScopedDirectory::new("v3-recovery-partial-overlap");
    let database = fixture(&scope.join("db.redb"), PRE_V3_REGISTRY);
    let history = activate_validated(
        database.begin_write().unwrap(),
        lineage(),
        DualFrontier::INITIAL,
    )
    .unwrap();
    let first = audit_source(history);
    let transaction = database.begin_write().unwrap();
    let original =
        crate::changelog_v3_journal::materialize_recovered_frame(&transaction, &first).unwrap();
    transaction.commit().unwrap();
    let successor = history.advance(&original).unwrap();
    let second = audit_source(successor);
    let second = JournalFrame::service_audit(
        second.database_id(),
        second.predecessor_sequence(),
        second.predecessor_administration_sequence(),
        second.covered_administration_sequence(),
        1,
        first.encode().unwrap().frame_hash(),
        second.mutations().to_vec(),
    )
    .unwrap();
    let path = scope.join("source.journal");
    publish_recovery_sources(&path, &[&first, &second]);
    let old_pin = database.begin_read().unwrap();
    crate::journal::recover_journal_path(&database, &path, lineage().database_id()).unwrap();
    let pin = database.begin_read().unwrap();
    assert_eq!(
        crate::changelog_v3_journal::verify_materialized_frame(&pin, &first).unwrap(),
        original
    );
    let appended = crate::changelog_v3_journal::verify_materialized_frame(&pin, &second).unwrap();
    assert_eq!(
        crate::changelog_v3_roots::validate_retained_history(&pin).unwrap(),
        Some(successor.advance(&appended).unwrap())
    );
    assert_eq!(
        crate::changelog_v3_roots::validate_retained_history(&old_pin).unwrap(),
        Some(successor)
    );
    assert_eq!(
        pin.open_table(crate::layout::AUDIT).unwrap().len().unwrap(),
        2
    );
}

#[test]
fn journal_recovery_refuses_skipped_or_duplicate_v3_positions_before_replay() {
    use riffdb_storage_api::{ChangelogTransactionAllocator, ChangelogTransactionSequence};
    for assigned in [2, 4] {
        let scope = crate::test_path::ScopedDirectory::new("v3-recovery-position-refusal");
        let database = fixture(&scope.join("db.redb"), PRE_V3_REGISTRY);
        let history = activate_validated(
            database.begin_write().unwrap(),
            lineage(),
            DualFrontier::INITIAL,
        )
        .unwrap();
        let first = audit_source(history);
        let transaction = database.begin_write().unwrap();
        let receipt =
            crate::changelog_v3_journal::materialize_recovered_frame(&transaction, &first).unwrap();
        transaction.abort().unwrap();
        let second = audit_source(history.advance(&receipt).unwrap());
        let allocator = ChangelogTransactionAllocator::Next(
            ChangelogTransactionSequence::new(assigned).unwrap(),
        );
        let mut mutations = second.mutations().to_vec();
        *mutations
            .iter_mut()
            .find(|m| m.key() == key(N::NextChangelogTransaction).unwrap().as_bytes())
            .unwrap() = JournalMutation::replace(
            JournalTable::Meta,
            key(N::NextChangelogTransaction).unwrap().as_bytes(),
            encode_changelog_transaction_allocator_v3(allocator)
                .unwrap()
                .as_bytes(),
            encode_changelog_transaction_allocator_v3(allocator.allocate_one().unwrap().1)
                .unwrap()
                .into_bytes(),
        )
        .unwrap();
        let second = JournalFrame::service_audit(
            second.database_id(),
            second.predecessor_sequence(),
            second.predecessor_administration_sequence(),
            second.covered_administration_sequence(),
            1,
            first.encode().unwrap().frame_hash(),
            mutations,
        )
        .unwrap();
        let path = scope.join("source.journal");
        publish_recovery_sources(&path, &[&first, &second]);
        let before = std::fs::read(&path).unwrap();
        assert_eq!(
            crate::journal::recover_journal_path(&database, &path, lineage().database_id()),
            Err(crate::journal::JournalIoError::Corrupt)
        );
        assert_eq!(std::fs::read(&path).unwrap(), before);
        assert_eq!(
            crate::changelog_v3_roots::validate_retained_history(&database.begin_read().unwrap())
                .unwrap(),
            Some(history)
        );
        assert_eq!(
            database
                .begin_read()
                .unwrap()
                .open_table(crate::layout::AUDIT)
                .unwrap()
                .len()
                .unwrap(),
            0
        );
    }
}

#[test]
fn journal_recovery_materializes_v3_before_reclaim_and_retries_without_another_receipt() {
    let scope = crate::test_path::ScopedDirectory::new("v3-actual-journal-recovery");
    let database = fixture(&scope.join("db.redb"), PRE_V3_REGISTRY);
    let history = activate_validated(
        database.begin_write().unwrap(),
        lineage(),
        DualFrontier::INITIAL,
    )
    .unwrap();
    let source = audit_source(history);
    let path = scope.join("source.journal");
    publish_recovery_source(&path, &source);
    let before = std::fs::read(&path).unwrap();
    crate::journal::recover_journal_path(&database, &path, lineage().database_id()).unwrap();
    let pin = database.begin_read().unwrap();
    let receipt = crate::changelog_v3_journal::verify_materialized_frame(&pin, &source).unwrap();
    let successor = history.advance(&receipt).unwrap();
    assert_eq!(
        crate::changelog_v3_roots::validate_retained_history(&pin).unwrap(),
        Some(successor)
    );
    let (_, tail) = crate::journal::scan_journal(&path, lineage().database_id(), |_| Ok(()))
        .unwrap()
        .unwrap();
    assert_eq!(tail.transition_count, 0);
    assert_ne!(std::fs::read(&path).unwrap(), before);
    let reclaimed = std::fs::read(&path).unwrap();
    crate::journal::recover_journal_path(&database, &path, lineage().database_id()).unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), reclaimed);
    assert_eq!(
        crate::changelog_v3_roots::validate_retained_history(&database.begin_read().unwrap())
            .unwrap(),
        Some(successor)
    );
}

#[test]
fn journal_recovery_verifies_original_v3_overlap_after_a_later_direct_overwrite() {
    let scope = crate::test_path::ScopedDirectory::new("v3-actual-journal-overlap");
    let database = fixture(&scope.join("db.redb"), PRE_V3_REGISTRY);
    let history = activate_validated(
        database.begin_write().unwrap(),
        lineage(),
        DualFrontier::INITIAL,
    )
    .unwrap();
    let source = command_source(history, None, Some(b"original"));
    let path = scope.join("source.journal");
    publish_recovery_source(&path, &source);
    let transaction = database.begin_write().unwrap();
    let original =
        crate::changelog_v3_journal::materialize_recovered_frame(&transaction, &source).unwrap();
    transaction.commit().unwrap();
    let direct = crate::changelog_v3_write::CapturedImmediateWrite::begin(
        &database,
        RedbCommitProfile::Standard,
        riffdb_storage_api::ChangelogAttributionV3::DirectApplicationOrServiceAuditGroup,
    )
    .unwrap();
    direct
        .open_table(crate::layout::ENTITIES)
        .unwrap()
        .insert(b"key".as_slice(), b"later".as_slice())
        .unwrap();
    direct
        .open_table(crate::layout::COMMITS)
        .unwrap()
        .insert(
            2_u64.to_be_bytes().as_slice(),
            b"later-opaque-commit".as_slice(),
        )
        .unwrap();
    direct
        .open_table(META)
        .unwrap()
        .insert(
            META_APPLICATION_SEQUENCE,
            encode_application_sequence_allocator_v1(ApplicationSequenceAllocator::Next(
                riffdb_types::CommitSequence::new(3).unwrap(),
            ))
            .unwrap()
            .as_bytes(),
        )
        .unwrap();
    direct.finish().unwrap().commit_for_test().unwrap();
    let history =
        crate::changelog_v3_roots::validate_retained_history(&database.begin_read().unwrap())
            .unwrap();
    crate::journal::recover_journal_path(&database, &path, lineage().database_id()).unwrap();
    let pin = database.begin_read().unwrap();
    assert_eq!(
        crate::changelog_v3_journal::verify_materialized_frame(&pin, &source).unwrap(),
        original
    );
    assert_eq!(
        crate::changelog_v3_roots::validate_retained_history(&pin).unwrap(),
        history
    );
    assert_eq!(
        pin.open_table(crate::layout::ENTITIES)
            .unwrap()
            .get(b"key".as_slice())
            .unwrap()
            .unwrap()
            .value(),
        b"later"
    );
}

#[test]
fn journal_recovery_refuses_v3_gaps_and_partial_evidence_without_reclaiming_source() {
    for fault in [
        "missing-counter",
        "partial-roots",
        "missing-overlap",
        "wrong-before-image",
    ] {
        let scope = crate::test_path::ScopedDirectory::new("v3-recovery-refusal");
        let database = fixture(&scope.join("db.redb"), PRE_V3_REGISTRY);
        let history = activate_validated(
            database.begin_write().unwrap(),
            lineage(),
            DualFrontier::INITIAL,
        )
        .unwrap();
        let mut source = command_source(history, None, Some(b"original"));
        if fault == "missing-counter" {
            source = JournalFrame::command(
                source.database_id(),
                source.predecessor_sequence(),
                source.covered_sequence(),
                source.predecessor_administration_sequence(),
                source.covered_administration_sequence(),
                1,
                source.previous_hash(),
                source
                    .mutations()
                    .iter()
                    .filter(|m| m.key() != key(N::NextChangelogTransaction).unwrap().as_bytes())
                    .cloned()
                    .collect(),
            )
            .unwrap();
        }
        let transaction = database.begin_write().unwrap();
        match fault {
            "partial-roots" => {
                transaction
                    .open_table(META)
                    .unwrap()
                    .remove(key(N::LeadershipEpoch).unwrap())
                    .unwrap();
            }
            "missing-overlap" => {
                crate::changelog_v3_journal::materialize_recovered_frame(&transaction, &source)
                    .unwrap();
                transaction
                    .open_table(HISTORY)
                    .unwrap()
                    .remove(2_u64.to_be_bytes().as_slice())
                    .unwrap();
            }
            "wrong-before-image" => {
                transaction
                    .open_table(crate::layout::ENTITIES)
                    .unwrap()
                    .insert(b"key".as_slice(), b"unexpected".as_slice())
                    .unwrap();
            }
            _ => {}
        }
        transaction.commit().unwrap();
        let path = scope.join("source.journal");
        publish_recovery_source(&path, &source);
        let journal_before = std::fs::read(&path).unwrap();
        let roots_before = metadata(&database);
        let commits_before = database
            .begin_read()
            .unwrap()
            .open_table(crate::layout::COMMITS)
            .unwrap()
            .len()
            .unwrap();
        for _ in 0..2 {
            assert_eq!(
                crate::journal::recover_journal_path(&database, &path, lineage().database_id()),
                Err(crate::journal::JournalIoError::Corrupt),
                "{fault}"
            );
            assert_eq!(std::fs::read(&path).unwrap(), journal_before, "{fault}");
            assert_eq!(metadata(&database), roots_before, "{fault}");
            assert_eq!(
                database
                    .begin_read()
                    .unwrap()
                    .open_table(crate::layout::COMMITS)
                    .unwrap()
                    .len()
                    .unwrap(),
                commits_before,
                "{fault}"
            );
        }
    }
}

#[test]
fn v3_journal_recovery_process_child() {
    let Some(path) = std::env::var_os("RIFFDB_V3_RECOVERY_DATABASE") else {
        return;
    };
    let database = Database::open(&path).unwrap();
    let journal = crate::journal::journal_path(Path::new(&path));
    crate::journal::recover_journal_path(&database, &journal, lineage().database_id()).unwrap();
    panic!("the requested recovery crash edge was not reached");
}

#[test]
fn v3_journal_recovery_process_crashes_preserve_at_least_one_exact_receipt_source() {
    for edge in ["staged", "committed", "before-reclaim", "reclaimed"] {
        let scope = crate::test_path::ScopedDirectory::new("v3-recovery-process");
        let path = scope.join("db.redb");
        let database = fixture(&path, PRE_V3_REGISTRY);
        let history = activate_validated(
            database.begin_write().unwrap(),
            lineage(),
            DualFrontier::INITIAL,
        )
        .unwrap();
        let source = command_source(history, None, Some(b"original"));
        let journal = crate::journal::journal_path(&path);
        publish_recovery_source(&journal, &source);
        drop(database);
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("changelog_v3_activation::tests::journal_materialization::v3_journal_recovery_process_child")
            .arg("--nocapture")
            .env("RIFFDB_V3_RECOVERY_DATABASE", &path)
            .env("RIFFDB_V3_RECOVERY_EDGE", edge)
            .status().unwrap();
        assert_eq!(status.code(), Some(93), "{edge}");
        let database = Database::open(&path).unwrap();
        let pin = database.begin_read().unwrap();
        let actual = crate::changelog_v3_roots::validate_retained_history(&pin)
            .unwrap()
            .unwrap();
        let (_, tail) = crate::journal::scan_journal(&journal, lineage().database_id(), |frame| {
            assert_eq!(
                frame.encode().unwrap().as_bytes(),
                source.encode().unwrap().as_bytes()
            );
            Ok(())
        })
        .unwrap()
        .unwrap();
        assert_eq!(tail.transition_count, usize::from(edge != "reclaimed"));
        if edge == "staged" {
            assert_eq!(actual, history);
            assert!(
                pin.open_table(crate::layout::ENTITIES)
                    .unwrap()
                    .get(b"key".as_slice())
                    .unwrap()
                    .is_none()
            );
        } else {
            assert_eq!(actual.tail().sequence().get(), 2);
            crate::changelog_v3_journal::verify_materialized_frame(&pin, &source).unwrap();
        }
        drop(pin);
        for _ in 0..2 {
            crate::journal::recover_journal_path(&database, &journal, lineage().database_id())
                .unwrap();
            let pin = database.begin_read().unwrap();
            let receipt =
                crate::changelog_v3_journal::verify_materialized_frame(&pin, &source).unwrap();
            assert_eq!(
                crate::changelog_v3_roots::validate_retained_history(&pin).unwrap(),
                Some(history.advance(&receipt).unwrap())
            );
        }
    }
}

#[test]
fn recovered_journal_receipt_and_allocator_share_the_existing_checkpoint_transaction() {
    let scope = crate::test_path::ScopedDirectory::new("v3-journal-materialization");
    let database = fixture(&scope.join("db.redb"), PRE_V3_REGISTRY);
    let history = activate_validated(
        database.begin_write().unwrap(),
        lineage(),
        DualFrontier::INITIAL,
    )
    .unwrap();
    let source = audit_source(history);
    let source_bytes = source.encode().unwrap();
    let transaction = database.begin_write().unwrap();
    let receipt =
        crate::changelog_v3_journal::materialize_recovered_frame(&transaction, &source).unwrap();
    // No helper commit: the independent pin still sees the predecessor.
    assert_eq!(
        crate::changelog_v3_roots::read_checkpoint_roots(&database.begin_read().unwrap()).unwrap(),
        Some(history)
    );
    transaction.abort().unwrap();
    assert_eq!(
        crate::changelog_v3_roots::read_checkpoint_roots(&database.begin_read().unwrap()).unwrap(),
        Some(history)
    );
    let transaction = database.begin_write().unwrap();
    assert_eq!(
        crate::changelog_v3_journal::materialize_recovered_frame(&transaction, &source).unwrap(),
        receipt
    );
    transaction.commit().unwrap();
    let snapshot = database.begin_read().unwrap();
    let successor = crate::changelog_v3_roots::read_checkpoint_roots(&snapshot)
        .unwrap()
        .unwrap();
    assert_eq!(successor, history.advance(&receipt).unwrap());
    assert_eq!(
        snapshot
            .open_table(HISTORY)
            .unwrap()
            .get(receipt.binding().sequence.get().to_be_bytes().as_slice())
            .unwrap()
            .unwrap()
            .value(),
        receipt.encode().unwrap()
    );
    assert_eq!(source.encode().unwrap().as_bytes(), source_bytes.as_bytes());
    let transaction = database.begin_write().unwrap();
    assert!(
        crate::changelog_v3_journal::materialize_recovered_frame(&transaction, &source).is_err()
    );
    transaction.abort().unwrap();
}

fn command_source(
    history: ChangelogHistoryStateV3,
    before: Option<&[u8]>,
    after: Option<&[u8]>,
) -> JournalFrame {
    use riffdb_types::CommitSequence;
    let predecessor = history.tail().frontier();
    let sequence =
        CommitSequence::new(predecessor.application().map_or(0, CommitSequence::get) + 1).unwrap();
    let entity = match (before, after) {
        (None, Some(value)) => {
            JournalMutation::put(JournalTable::Entities, b"key".to_vec(), value.to_vec())
        }
        (Some(before), Some(value)) => JournalMutation::replace(
            JournalTable::Entities,
            b"key".to_vec(),
            before,
            value.to_vec(),
        ),
        (Some(before), None) => {
            JournalMutation::delete_matching(JournalTable::Entities, b"key".to_vec(), before)
        }
        (None, None) => panic!("fixture must change one entity"),
    }
    .unwrap();
    JournalFrame::command(
        history.lineage().database_id(),
        predecessor.application(),
        Some(sequence),
        predecessor.administration(),
        predecessor.administration(),
        1,
        [0x42; 32],
        vec![
            JournalMutation::put(
                JournalTable::Commits,
                sequence.get().to_be_bytes(),
                b"opaque-commit".to_vec(),
            )
            .unwrap(),
            entity,
            JournalMutation::replace(
                JournalTable::Meta,
                META_APPLICATION_SEQUENCE.as_bytes(),
                encode_application_sequence_allocator_v1(ApplicationSequenceAllocator::Next(
                    sequence,
                ))
                .unwrap()
                .as_bytes(),
                encode_application_sequence_allocator_v1(ApplicationSequenceAllocator::Next(
                    CommitSequence::new(sequence.get() + 1).unwrap(),
                ))
                .unwrap()
                .into_bytes(),
            )
            .unwrap(),
            JournalMutation::replace(
                JournalTable::Meta,
                key(N::NextChangelogTransaction).unwrap().as_bytes(),
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
    .unwrap()
}

#[test]
fn journal_overlap_checks_original_receipt_bytes_after_overwrite_and_delete() {
    let scope = crate::test_path::ScopedDirectory::new("v3-journal-overlap");
    let database = fixture(&scope.join("db.redb"), PRE_V3_REGISTRY);
    let mut history = activate_validated(
        database.begin_write().unwrap(),
        lineage(),
        DualFrontier::INITIAL,
    )
    .unwrap();
    let mut sources = Vec::new();
    let mut receipts = Vec::new();
    for (before, after) in [
        (None, Some(b"first".as_slice())),
        (Some(b"first".as_slice()), Some(b"second".as_slice())),
        (Some(b"second".as_slice()), None),
    ] {
        let source = command_source(history, before, after);
        let transaction = database.begin_write().unwrap();
        let receipt =
            crate::changelog_v3_journal::materialize_recovered_frame(&transaction, &source)
                .unwrap();
        transaction.commit().unwrap();
        history = history.advance(&receipt).unwrap();
        sources.push(source);
        receipts.push(receipt);
    }
    let snapshot = database.begin_read().unwrap();
    assert!(
        snapshot
            .open_table(crate::layout::ENTITIES)
            .unwrap()
            .get(b"key".as_slice())
            .unwrap()
            .is_none()
    );
    for (source, receipt) in sources.iter().zip(&receipts) {
        assert_eq!(
            crate::changelog_v3_journal::verify_materialized_frame(&snapshot, source).unwrap(),
            *receipt
        );
    }
    let transaction = database.begin_write().unwrap();
    transaction
        .open_table(HISTORY)
        .unwrap()
        .remove(2_u64.to_be_bytes().as_slice())
        .unwrap();
    transaction.commit().unwrap();
    // Pinned overlap evidence remains valid; a missing original row in a newer
    // view is corruption, never reconstructed from the surviving entity state.
    assert!(crate::changelog_v3_journal::verify_materialized_frame(&snapshot, &sources[0]).is_ok());
    assert!(
        crate::changelog_v3_journal::verify_materialized_frame(
            &database.begin_read().unwrap(),
            &sources[0]
        )
        .is_err()
    );
}

#[test]
fn journal_materialization_refuses_cancelled_wrong_before_image_and_partial_roots() {
    let scope = crate::test_path::ScopedDirectory::new("v3-journal-refusals");
    let database = fixture(&scope.join("db.redb"), PRE_V3_REGISTRY);
    let history = activate_validated(
        database.begin_write().unwrap(),
        lineage(),
        DualFrontier::INITIAL,
    )
    .unwrap();
    let source = command_source(history, None, Some(b"transient"));
    let mut mutations = source.mutations().to_vec();
    mutations.push(
        JournalMutation::delete_matching(JournalTable::Entities, b"key".to_vec(), b"transient")
            .unwrap(),
    );
    let cancelled = JournalFrame::command(
        source.database_id(),
        source.predecessor_sequence(),
        source.covered_sequence(),
        source.predecessor_administration_sequence(),
        source.covered_administration_sequence(),
        1,
        [0x42; 32],
        mutations,
    )
    .unwrap();
    let transaction = database.begin_write().unwrap();
    transaction
        .open_table(crate::layout::ENTITIES)
        .unwrap()
        .insert(b"key".as_slice(), b"unexpected".as_slice())
        .unwrap();
    transaction.commit().unwrap();
    let transaction = database.begin_write().unwrap();
    assert!(
        crate::changelog_v3_journal::materialize_recovered_frame(&transaction, &cancelled).is_err()
    );
    transaction.abort().unwrap();
    assert_eq!(
        crate::changelog_v3_roots::read_checkpoint_roots(&database.begin_read().unwrap()).unwrap(),
        Some(history)
    );
    let transaction = database.begin_write().unwrap();
    transaction
        .open_table(META)
        .unwrap()
        .remove(key(N::LeadershipEpoch).unwrap())
        .unwrap();
    transaction.commit().unwrap();
    let before = metadata(&database);
    let transaction = database.begin_write().unwrap();
    assert!(
        crate::changelog_v3_journal::materialize_recovered_frame(&transaction, &source).is_err()
    );
    // Root refusal must happen before any write, even if a broken caller commits.
    transaction.commit().unwrap();
    assert_eq!(metadata(&database), before);
}

#[test]
fn complete_changelog_history_validation_rejects_rechecksummed_broken_interiors() {
    use riffdb_storage_api::AuthoritativeMutationV3;
    let scope = crate::test_path::ScopedDirectory::new("v3-complete-history");
    let database = fixture(&scope.join("db.redb"), PRE_V3_REGISTRY);
    assert_eq!(
        crate::changelog_v3_roots::validate_retained_history(&database.begin_read().unwrap())
            .unwrap(),
        None
    );
    let mut history = activate_validated(
        database.begin_write().unwrap(),
        lineage(),
        DualFrontier::INITIAL,
    )
    .unwrap();
    let original_source = audit_source(history);
    let path = scope.join("source.journal");
    publish_recovery_source(&path, &original_source);
    let journal_before = std::fs::read(&path).unwrap();
    let mut receipts = Vec::new();
    for _ in 0..3 {
        let source = audit_source(history);
        let transaction = database.begin_write().unwrap();
        let receipt =
            crate::changelog_v3_journal::materialize_recovered_frame(&transaction, &source)
                .unwrap();
        transaction.commit().unwrap();
        history = history.advance(&receipt).unwrap();
        receipts.push(receipt);
    }
    let pinned = database.begin_read().unwrap();
    assert_eq!(
        crate::changelog_v3_roots::validate_retained_history(&pinned).unwrap(),
        Some(history)
    );
    let original = &receipts[0];
    let mut changes = original.mutations().to_vec();
    let audit = changes
        .iter_mut()
        .find(|mutation| mutation.namespace() == N::Audit)
        .unwrap();
    *audit =
        AuthoritativeMutationV3::put(N::Audit, audit.key(), None, b"substituted-audit").unwrap();
    let replacement =
        AuthoritativeTransactionV3::new(original.binding(), original.attribution(), changes)
            .unwrap()
            .encode()
            .unwrap();
    for bytes in [Some(replacement), None] {
        let transaction = database.begin_write().unwrap();
        {
            let mut table = transaction.open_table(HISTORY).unwrap();
            match bytes {
                Some(bytes) => {
                    table
                        .insert(2_u64.to_be_bytes().as_slice(), bytes.as_slice())
                        .unwrap();
                }
                None => {
                    table.remove(2_u64.to_be_bytes().as_slice()).unwrap();
                }
            }
        }
        transaction.commit().unwrap();
        let fresh = database.begin_read().unwrap();
        // A valid terminal row is deliberately insufficient for the full pass.
        assert_eq!(
            crate::changelog_v3_roots::read_checkpoint_roots(&fresh).unwrap(),
            Some(history)
        );
        assert!(crate::changelog_v3_roots::validate_retained_history(&fresh).is_err());
        assert_eq!(
            crate::journal::recover_journal_path(&database, &path, lineage().database_id()),
            Err(crate::journal::JournalIoError::Corrupt)
        );
        assert_eq!(std::fs::read(&path).unwrap(), journal_before);
        assert_eq!(
            crate::changelog_v3_roots::validate_retained_history(&pinned).unwrap(),
            Some(history)
        );
    }
}
