//! Physical source/pin tests use opaque record payloads. Real service-adapter
//! publication and writer-gate independence are tested in administration.rs.
// req: REP-003, REC-001, PERF-007, STO-012

use super::*;
use crate::journal::{JournalFrame, JournalMutation, JournalTable};
use crate::store::RedbStore;
use redb::ReadableDatabase;
use riffdb_storage_api::{
    ApplicationSequenceAllocator, AuthoritativeNamespaceV1 as N, DatabaseInitializationPort,
    LeadershipEpochV1, proto_codec::*,
};
use riffdb_types::{CommitSequence, DatabaseId, DualFrontier};

fn fixture() -> (
    crate::test_path::ScopedDirectory,
    RedbStore,
    ChangelogHistoryStateV3,
) {
    let scope = crate::test_path::ScopedDirectory::new("v3-receipt-cursor");
    let mut store = RedbStore::open(scope.join("db.redb")).unwrap();
    let database =
        DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x73; 10]).unwrap();
    store.initialize_database(database).unwrap();
    let history = crate::changelog_v3_activation::activate_validated(
        store.shared.database.begin_write().unwrap(),
        ChangelogLineageV3::new(database, 1, LeadershipEpochV1::initial()).unwrap(),
        DualFrontier::INITIAL,
    )
    .unwrap();
    (scope, store, history)
}

fn source(
    history: ChangelogHistoryStateV3,
    prior: Option<&[u8]>,
    after: Option<&[u8]>,
) -> (
    JournalFrame,
    AuthoritativeTransactionV3,
    Arc<[CompositeMutationV1]>,
) {
    let frontier = history.tail().frontier();
    let assigned = CommitSequence::new(frontier.application().map_or(1, |s| s.get() + 1)).unwrap();
    let before = ApplicationSequenceAllocator::Next(assigned);
    let after_allocator = ApplicationSequenceAllocator::Next(assigned.checked_next().unwrap());
    let entity = match (prior, after) {
        (None, Some(value)) => {
            JournalMutation::put(JournalTable::Entities, b"entity".as_slice(), value)
        }
        (Some(prior), Some(value)) => {
            JournalMutation::replace(JournalTable::Entities, b"entity".as_slice(), prior, value)
        }
        (Some(prior), None) => {
            JournalMutation::delete_matching(JournalTable::Entities, b"entity".as_slice(), prior)
        }
        (None, None) => panic!("fixture must change the value"),
    }
    .unwrap();
    let frame = JournalFrame::command(
        history.lineage().database_id(),
        frontier.application(),
        Some(assigned),
        frontier.administration(),
        frontier.administration(),
        1,
        [0x41; 32],
        vec![
            JournalMutation::put(
                JournalTable::Commits,
                assigned.get().to_be_bytes().as_slice(),
                b"opaque-commit".as_slice(),
            )
            .unwrap(),
            entity,
            JournalMutation::replace(
                JournalTable::Meta,
                crate::layout::META_APPLICATION_SEQUENCE.as_bytes(),
                encode_application_sequence_allocator_v1(before)
                    .unwrap()
                    .as_bytes(),
                encode_application_sequence_allocator_v1(after_allocator)
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
    let binding = AuthoritativeTransactionBindingV3 {
        database_id: history.lineage().database_id(),
        history_incarnation: history.lineage().history_incarnation(),
        predecessor: Some(history.tail().sequence()),
        sequence: history.expected_allocator().allocate_one().unwrap().0,
        predecessor_frontier: frontier,
        covered_frontier: DualFrontier::new(Some(assigned), frontier.administration()),
        prior_history_hash: history.tail().history_hash(),
    };
    let receipt = crate::changelog_v3::receipt_from_journal(&frame, binding).unwrap();
    let mutations = Arc::from(
        frame
            .mutations()
            .iter()
            .map(|m| m.composite().unwrap())
            .collect::<Vec<_>>(),
    );
    (frame, receipt, mutations)
}

fn cursor(
    root: Arc<CheckpointRoot>,
    suffix: &ChangelogSuffix,
    current: ChangelogHistoryPointV3,
) -> ReceiptCursor {
    ReceiptCursor {
        root,
        history: suffix.history.unwrap(),
        sources: suffix.sources().unwrap(),
        current,
        failure: None,
    }
}

#[test]
fn cursor_rebase_releases_only_materialized_sources_and_keeps_overwrite_delete_bytes() {
    let (_scope, store, anchor) = fixture();
    let old_root = Arc::new(CheckpointRoot::new(
        store.shared.database.begin_read().unwrap(),
        1,
    ));
    let mut suffix = ChangelogSuffix::from_checkpoint(&old_root).unwrap();
    let mut history = anchor;
    let mut frames = Vec::new();
    let mut receipts = Vec::new();
    let mut covered = None;
    for (prior, after) in [
        (None, Some(b"original".as_slice())),
        (
            Some(b"original".as_slice()),
            Some(b"overwritten".as_slice()),
        ),
        (Some(b"overwritten".as_slice()), None),
    ] {
        let (frame, receipt, mutations) = source(history, prior, after);
        history = history.advance(&receipt).unwrap();
        suffix
            .append(
                Some((receipt.binding(), history)),
                receipt.attribution(),
                Arc::clone(&mutations),
                frame.encode().unwrap().as_bytes().len(),
            )
            .unwrap();
        assert!(Arc::ptr_eq(
            &suffix.head.as_ref().unwrap().source.mutations,
            &mutations
        ));
        if covered.is_none() {
            covered = Some(suffix.clone());
        }
        frames.push(frame);
        receipts.push(receipt);
    }
    let covered = covered.unwrap();
    let first_source = Arc::downgrade(&covered.head.as_ref().unwrap().source);
    let mut old = cursor(Arc::clone(&old_root), &suffix, anchor.tail());
    let write = store.shared.database.begin_write().unwrap();
    crate::changelog_v3_journal::materialize_recovered_frame(&write, &frames[0]).unwrap();
    write.commit().unwrap();
    let checkpoint = Arc::new(CheckpointRoot::new(
        store.shared.database.begin_read().unwrap(),
        2,
    ));
    let rebased = suffix.rebase_after(&covered, &checkpoint).unwrap();
    assert_eq!(rebased.frames, 2);
    assert_eq!(rebased.sources().unwrap()[0].binding.sequence.get(), 3);
    let mut combined = cursor(Arc::clone(&checkpoint), &rebased, anchor.tail());
    drop(suffix);
    drop(covered);
    assert!(
        first_source.upgrade().is_some(),
        "the old pin still needs original bytes"
    );
    let write = store.shared.database.begin_write().unwrap();
    for frame in &frames[1..] {
        crate::changelog_v3_journal::materialize_recovered_frame(&write, frame).unwrap();
    }
    write.commit().unwrap();
    for expected in &receipts {
        assert_eq!(
            old.next_receipt().unwrap().unwrap().encode().unwrap(),
            expected.encode().unwrap()
        );
        assert_eq!(
            combined.next_receipt().unwrap().unwrap().encode().unwrap(),
            expected.encode().unwrap()
        );
    }
    assert!(old.next_receipt().unwrap().is_none());
    assert!(combined.next_receipt().unwrap().is_none());
    drop(old);
    assert!(
        first_source.upgrade().is_none(),
        "new pins must not retain the materialized prefix's payloads"
    );
    let current = Arc::new(CheckpointRoot::new(
        store.shared.database.begin_read().unwrap(),
        3,
    ));
    assert!(
        current
            .open_table(crate::layout::ENTITIES)
            .unwrap()
            .get(b"entity".as_slice())
            .unwrap()
            .is_none()
    );
    let mut materialized = open(
        &RedbReadAccess::Durable(current),
        anchor.lineage(),
        anchor.tail(),
    )
    .unwrap();
    for expected in receipts {
        assert_eq!(
            materialized
                .next_receipt()
                .unwrap()
                .unwrap()
                .encode()
                .unwrap(),
            expected.encode().unwrap()
        );
    }
    assert!(materialized.next_receipt().unwrap().is_none());
}

#[test]
fn receipt_cursor_refuses_missing_rows_and_stays_failed_without_skipping() {
    let (_scope, store, anchor) = fixture();
    let (first, receipt, _) = source(anchor, None, Some(b"first"));
    let next = anchor.advance(&receipt).unwrap();
    let (second, _, _) = source(next, Some(b"first"), None);
    let write = store.shared.database.begin_write().unwrap();
    for frame in [&first, &second] {
        crate::changelog_v3_journal::materialize_recovered_frame(&write, frame).unwrap();
    }
    write
        .open_table(HISTORY)
        .unwrap()
        .remove(receipt.binding().sequence.get().to_be_bytes().as_slice())
        .unwrap();
    write.commit().unwrap();
    let root = Arc::new(CheckpointRoot::new(
        store.shared.database.begin_read().unwrap(),
        1,
    ));
    let mut cursor = open(
        &RedbReadAccess::Durable(root),
        anchor.lineage(),
        anchor.tail(),
    )
    .unwrap();
    let first_error = cursor.next_receipt().unwrap_err();
    assert!(
        matches!(&first_error, ChangelogCursorErrorV3::Storage(error) if error.kind() == StorageErrorKind::CorruptData)
    );
    assert_eq!(cursor.next_receipt().unwrap_err(), first_error);
}

#[test]
fn receipt_cursor_respects_the_exact_pinned_pruning_floor() {
    let (_scope, store, anchor) = fixture();
    let (frame, receipt, _) = source(anchor, None, Some(b"value"));
    let history = anchor.advance(&receipt).unwrap();
    let write = store.shared.database.begin_write().unwrap();
    crate::changelog_v3_journal::materialize_recovered_frame(&write, &frame).unwrap();
    write.commit().unwrap();
    let old = Arc::new(CheckpointRoot::new(
        store.shared.database.begin_read().unwrap(),
        1,
    ));
    // Isolated retained-root fixture, not permission or proof to prune history.
    let pruned = ChangelogHistoryStateV3::new(
        history.lineage(),
        history.anchor(),
        history.tail(),
        history.tail(),
    )
    .unwrap();
    let write = store.shared.database.begin_write().unwrap();
    write
        .open_table(crate::layout::META)
        .unwrap()
        .insert(
            N::ChangelogHistoryState.metadata_key().unwrap(),
            encode_changelog_history_state_v3(pruned)
                .unwrap()
                .as_bytes(),
        )
        .unwrap();
    write.commit().unwrap();
    let root = Arc::new(CheckpointRoot::new(
        store.shared.database.begin_read().unwrap(),
        2,
    ));
    let access = RedbReadAccess::Durable(root);
    assert!(
        matches!(open(&access, anchor.lineage(), anchor.tail()), Err(ChangelogCursorErrorV3::Storage(error)) if error.kind() == StorageErrorKind::HistoryPruned)
    );
    assert!(
        open(&access, anchor.lineage(), history.tail())
            .unwrap()
            .next_receipt()
            .unwrap()
            .is_none()
    );
    assert_eq!(
        open(
            &RedbReadAccess::Durable(old),
            anchor.lineage(),
            anchor.tail()
        )
        .unwrap()
        .next_receipt()
        .unwrap()
        .unwrap()
        .encode()
        .unwrap(),
        receipt.encode().unwrap()
    );
}

#[test]
fn maximum_receipt_source_ancestry_drops_on_the_production_stack() {
    let (_scope, store, anchor) = fixture();
    let (frame, receipt, mutations) = source(anchor, None, Some(b"value"));
    let source = Arc::new(Source {
        binding: receipt.binding(),
        attribution: receipt.attribution(),
        history: anchor.advance(&receipt).unwrap(),
        mutations,
        encoded_bytes: frame.encode().unwrap().as_bytes().len(),
    });
    let weak = Arc::downgrade(&source);
    // Only graph depth matters to Drop. No repeated source is admitted as history.
    std::thread::Builder::new()
        .stack_size(crate::PRODUCTION_THREAD_STACK_BYTES)
        .spawn(move || {
            let mut head = None;
            for _ in 0..MAX_JOURNAL_SUFFIX_TRANSITIONS {
                head = Some(Arc::new(SourceNode {
                    predecessor: head,
                    source: Arc::clone(&source),
                }));
            }
            drop(head);
        })
        .unwrap()
        .join()
        .unwrap();
    assert!(weak.upgrade().is_none());
    drop(store);
}

#[test]
fn receipt_source_bounds_refuse_before_linking_or_advancing_the_private_tail() {
    let (_scope, store, anchor) = fixture();
    let root = CheckpointRoot::new(store.shared.database.begin_read().unwrap(), 1);
    let (frame, receipt, mutations) = source(anchor, None, Some(b"value"));
    let history = anchor.advance(&receipt).unwrap();
    let encoded = frame.encode().unwrap().as_bytes().len();
    for fault in 0..4 {
        let mut suffix = ChangelogSuffix::from_checkpoint(&root).unwrap();
        if fault == 0 {
            suffix.frames = MAX_JOURNAL_SUFFIX_TRANSITIONS;
        }
        if fault == 1 {
            suffix.bytes = MAX_JOURNAL_SUFFIX_BYTES;
        }
        let mut binding = receipt.binding();
        if fault == 3 {
            binding.prior_history_hash[0] ^= 1;
        }
        let size = if fault == 2 {
            MAX_JOURNAL_FRAME_BYTES + 1
        } else {
            encoded
        };
        assert!(
            suffix
                .append(
                    Some((binding, history)),
                    receipt.attribution(),
                    Arc::clone(&mutations),
                    size
                )
                .is_err()
        );
        assert_eq!(suffix.history, Some(anchor));
        assert!(suffix.head.is_none());
    }
}
