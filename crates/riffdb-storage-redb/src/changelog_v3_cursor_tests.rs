//! Physical source/pin tests use opaque record payloads. Real service-adapter
//! publication and writer-gate independence are tested in administration.rs.
// req: REP-003, REC-001, PERF-007, STO-012

use super::*;
use crate::journal::{JournalFrame, JournalMutation, JournalTable};
use crate::store::RedbStore;
use redb::ReadableDatabase;
use riffdb_storage_api::{
    ApplicationSequenceAllocator, AuthoritativeNamespaceV1 as N, LeadershipEpochV1, proto_codec::*,
};
use riffdb_types::{CommitSequence, DatabaseId, DualFrontier};

thread_local! {
    static DROP_DEPTH: std::cell::Cell<(usize, usize)> = const { std::cell::Cell::new((0, 0)) };
    static AFTER_OWNERSHIP_DECISION: std::cell::RefCell<Option<Box<dyn FnMut()>>> =
        const { std::cell::RefCell::new(None) };
}

pub(super) struct SourceNodeDropDepth;

pub(super) fn enter_source_node_drop() -> SourceNodeDropDepth {
    DROP_DEPTH.with(|depth| {
        let (current, maximum) = depth.get();
        depth.set((current + 1, maximum.max(current + 1)));
    });
    SourceNodeDropDepth
}

impl Drop for SourceNodeDropDepth {
    fn drop(&mut self) {
        DROP_DEPTH.with(|depth| {
            let (current, maximum) = depth.get();
            depth.set((current - 1, maximum));
        });
    }
}

pub(super) fn after_predecessor_ownership_decision() {
    AFTER_OWNERSHIP_DECISION.with(|hook| {
        if let Some(hook) = hook.borrow_mut().as_mut() {
            hook();
        }
    });
}

fn fixture() -> (
    crate::test_path::ScopedDirectory,
    RedbStore,
    ChangelogHistoryStateV3,
) {
    let scope = crate::test_path::ScopedDirectory::new("v3-receipt-cursor");
    let mut store = RedbStore::open(scope.join("db.redb")).unwrap();
    let database =
        DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x73; 10]).unwrap();
    store.initialize_legacy_fixture(database).unwrap();
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
        _provider_metadata: None,
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
        reclaimed_resume: None,
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
                    ancestors: Box::new([]),
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
fn raced_last_receipt_source_owners_release_ancestry_without_recursive_drop() {
    use std::sync::mpsc;
    use std::time::Duration;

    let (_scope, store, anchor) = fixture();
    let (frame, receipt, mutations) = source(anchor, None, Some(b"value"));
    let source = Arc::new(Source {
        binding: receipt.binding(),
        attribution: receipt.attribution(),
        history: anchor.advance(&receipt).unwrap(),
        mutations,
        reclaimed_resume: None,
        encoded_bytes: frame.encode().unwrap().as_bytes().len(),
    });
    let source_weak = Arc::downgrade(&source);
    let mut head = None;
    let mut competing_owners = Vec::new();
    let mut weak_nodes = Vec::new();
    // As in the maximum-depth proof, these are ownership graphs, not admitted
    // repeated receipts. Every raced predecessor initially has two real owners.
    for _ in 0..32 {
        let node = Arc::new(SourceNode {
            predecessor: head,
            source: Arc::clone(&source),
            ancestors: Box::new([]),
        });
        weak_nodes.push(Arc::downgrade(&node));
        competing_owners.push(Arc::clone(&node));
        head = Some(node);
    }
    let head = Arc::new(SourceNode {
        predecessor: head,
        source,
        ancestors: Box::new([]),
    });
    weak_nodes.push(Arc::downgrade(&head));
    let (release, requests) = mpsc::sync_channel(0);
    let (released, acknowledgements) = mpsc::sync_channel(0);
    let competitor = std::thread::spawn(move || {
        while requests.recv().is_ok() {
            drop(competing_owners.pop().expect("next raced predecessor"));
            released.send(()).unwrap();
        }
        drop(competing_owners);
    });
    let owner = std::thread::Builder::new()
        .stack_size(crate::PRODUCTION_THREAD_STACK_BYTES)
        .spawn(move || {
            DROP_DEPTH.with(|depth| depth.set((0, 0)));
            AFTER_OWNERSHIP_DECISION.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move || {
                    release.send(()).unwrap();
                    acknowledgements
                        .recv_timeout(Duration::from_secs(5))
                        .unwrap();
                }));
            });
            drop(head);
            AFTER_OWNERSHIP_DECISION.with(|hook| *hook.borrow_mut() = None);
            DROP_DEPTH.with(|depth| depth.get())
        })
        .unwrap();
    let (remaining_depth, maximum_depth) = owner.join().unwrap();
    competitor.join().unwrap();
    assert_eq!(remaining_depth, 0);
    assert!(weak_nodes.iter().all(|node| node.upgrade().is_none()));
    assert!(source_weak.upgrade().is_none());
    assert_eq!(
        maximum_depth, 1,
        "raced ancestry release must stay iterative"
    );
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

// req: OQ-040, PRJ-004
#[test]
fn provider_receipt_admission_refuses_before_progress_and_preserves_final_capture() {
    use riffdb_types::exact_provider_budget_v1::{EXACT_PROVIDER_BUDGET_BYTES, ProviderBudget};
    let (_scope, store, anchor) = fixture();
    let root = Arc::new(CheckpointRoot::new(
        store.shared.database.begin_read().unwrap(),
        1,
    ));
    let mut suffix = ChangelogSuffix::from_checkpoint(&root).unwrap();
    let (frame, receipt, mutations) = source(anchor, None, Some(&vec![b'x'; 262144]));
    let history = anchor.advance(&receipt).unwrap();
    suffix
        .append(
            Some((receipt.binding(), history)),
            receipt.attribution(),
            mutations,
            frame.encode().unwrap().as_bytes().len(),
        )
        .unwrap();
    let budget = ProviderBudget::new();
    let mut denied = cursor(Arc::clone(&root), &suffix, anchor.tail());
    let before = budget.observation().unwrap();
    let pressure = budget
        .reserve(EXACT_PROVIDER_BUDGET_BYTES - before.fixed_owner_bytes)
        .unwrap();
    let full = budget.observation().unwrap();
    assert!(denied.next_receipt_admitted_v1(&budget).is_err());
    assert_eq!(denied.current, anchor.tail());
    assert_eq!(budget.observation().unwrap(), full);
    drop(pressure);
    assert!(
        denied.next_receipt_admitted_v1(&budget).is_err(),
        "admission failure stays fused"
    );
    let mut accepted = cursor(root, &suffix, anchor.tail());
    let admitted = accepted.next_receipt_admitted_v1(&budget).unwrap().unwrap();
    assert_eq!(&**admitted, &receipt);
    assert_eq!(accepted.current, history.tail());
    assert!(
        accepted
            .next_receipt_admitted_v1(&budget)
            .unwrap()
            .is_none()
    );
    let alias = admitted.clone();
    let charged = budget.observation().unwrap().owned_bytes;
    assert!(charged > 262144);
    drop(admitted);
    drop(accepted);
    assert_eq!(budget.observation().unwrap().owned_bytes, charged);
    drop(alias);
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
}

// req: OQ-040, PRJ-004
#[test]
fn provider_archived_receipt_admission_preserves_proven_frame_bytes_and_refuses_pressure() {
    use riffdb_types::exact_provider_budget_v1::{EXACT_PROVIDER_BUDGET_BYTES, ProviderBudget};
    let (scope, _store, anchor) = fixture();
    let (frame, expected, _) = source(anchor, None, Some(&vec![b'q'; 262144]));
    let frame = frame.with_changelog(Some(crate::journal::FrameChangelogBinding::new(
        &expected.binding(),
        anchor.lineage().catalog_digest(),
    )));
    let encoded = frame.encode().unwrap();
    let placement = crate::journal::frame_placement_after(0, &encoded).unwrap();
    let database_path = scope.join("provider-archive-source.redb");
    let path = crate::journal::journal_path(&database_path);
    let header =
        crate::journal::JournalFileHeader::new(anchor.lineage().database_id(), None, [0; 32]);
    let lane = crate::journal::JournalLane::open(&path, &header).unwrap();
    let generation = lane.generation();
    lane.submit(encoded).unwrap().wait().unwrap();
    drop(lane);
    let _resolver = crate::history_stub::HistoryResolver::register(
        anchor.lineage().database_id(),
        &database_path,
        Arc::new(crate::media::RealJournalMedia),
    );
    let stub = crate::history_stub::StubLocator {
        database_id: anchor.lineage().database_id(),
        generation,
        placement,
    }
    .receipt_stub()
    .unwrap();
    let budget = ProviderBudget::new();
    let before = budget.observation().unwrap();
    let pressure = budget
        .reserve(EXACT_PROVIDER_BUDGET_BYTES - before.fixed_owner_bytes)
        .unwrap();
    let full = budget.observation().unwrap();
    assert!(
        matches!(crate::history_stub::provider_receipt_from_value_v1(&stub, &budget), Err(error) if error.kind() == StorageErrorKind::Unavailable)
    );
    assert_eq!(budget.observation().unwrap(), full);
    drop(pressure);
    let candidate = crate::history_stub::provider_receipt_from_value_v1(&stub, &budget).unwrap();
    assert_eq!(
        candidate.receipt.encode().unwrap(),
        crate::history_stub::resolve_history_value(&stub)
            .unwrap()
            .as_ref()
    );
    assert_eq!(candidate.receipt, expected);
    let native = riffdb_types::OwnedCapacityInspectionV1::inspect(&candidate.receipt).unwrap();
    assert!(native.shared_allocations().is_empty());
    assert!(native.unshared_bytes() < budget.observation().unwrap().reserved_bytes);
    drop(native);
    let retained = candidate
        .work
        .settle_owned_value(candidate.receipt)
        .unwrap();
    assert!(budget.observation().unwrap().owned_bytes > 262144);
    drop(retained);
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
    let mut corrupt_stub = crate::history_stub::HistoryStub::decode(&stub)
        .unwrap()
        .unwrap();
    corrupt_stub.placement.padded_len = usize::MAX;
    assert!(crate::history_stub::provider_receipt_from_stub_v1(&corrupt_stub, &budget).is_err());
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
}

// req: OQ-040, PRJ-004
#[test]
fn provider_receipt_framing_bound_checks_hostile_counts_and_overflow_without_copying() {
    let (_scope, _store, anchor) = fixture();
    let (frame, receipt, _) = source(anchor, None, Some(b"receipt"));
    let encoded = frame.encode().unwrap();
    let mut hostile_frame = encoded.as_bytes().to_vec();
    hostile_frame[68..72].copy_from_slice(&u32::MAX.to_be_bytes());
    let footer = hostile_frame.len() - 48;
    let hash = crate::journal::content_digest(&hostile_frame[..footer]);
    hostile_frame[footer + 16..].copy_from_slice(&hash);
    assert!(matches!(
        JournalFrame::decode(&hostile_frame),
        Err(crate::journal::JournalCodecError::InvalidValue)
    ));
    let mut bytes = receipt.encode().unwrap();
    assert!(AuthoritativeTransactionV3::provider_decode_work_bound_v1(&bytes).is_ok());
    bytes[120..124].copy_from_slice(&u32::MAX.to_be_bytes());
    assert!(AuthoritativeTransactionV3::provider_decode_work_bound_v1(&bytes).is_err());
    assert!(AuthoritativeTransactionV3::provider_decode_work_bound_v1(&bytes[..127]).is_err());
    assert!(riffdb_storage_api::provider_receipt_work_bound_v1(usize::MAX, usize::MAX).is_err());
}

// req: OQ-040, PRJ-004
#[test]
fn provider_receipt_open_admission_accounts_cursor_data_and_refuses_unguarded_progress() {
    use riffdb_types::exact_provider_budget_v1::{EXACT_PROVIDER_BUDGET_BYTES, ProviderBudget};
    let (_scope, store, anchor) = fixture();
    let root = Arc::new(CheckpointRoot::new(
        store.shared.database.begin_read().unwrap(),
        1,
    ));
    let access = RedbReadAccess::Current(root);
    let budget = ProviderBudget::new();
    let mut guarded = open_admitted_v1(&access, anchor.lineage(), anchor.tail(), &budget).unwrap();
    assert!(budget.observation().unwrap().owned_bytes >= 1024 * 1024);
    assert!(guarded.next_receipt().is_err());
    assert!(guarded.next_receipt_admitted_v1(&budget).is_err());
    drop(guarded);
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
    let mut foreign_capture =
        open_admitted_v1(&access, anchor.lineage(), anchor.tail(), &budget).unwrap();
    let foreign = ProviderBudget::new();
    let before_foreign = foreign.observation().unwrap();
    assert!(foreign_capture.next_receipt_admitted_v1(&foreign).is_err());
    assert_eq!(foreign.observation().unwrap(), before_foreign);
    assert!(foreign_capture.next_receipt_admitted_v1(&budget).is_err());
    drop(foreign_capture);
    let mut complete = open_admitted_v1(&access, anchor.lineage(), anchor.tail(), &budget).unwrap();
    let before = budget.observation().unwrap();
    let pressure = budget
        .reserve(
            EXACT_PROVIDER_BUDGET_BYTES
                - before.fixed_owner_bytes
                - before.reserved_bytes
                - before.owned_bytes,
        )
        .unwrap();
    let full = budget.observation().unwrap();
    assert!(
        complete
            .next_receipt_admitted_v1(&budget)
            .unwrap()
            .is_none()
    );
    assert_eq!(budget.observation().unwrap(), full);
    assert!(open_admitted_v1(&access, anchor.lineage(), anchor.tail(), &budget).is_err());
    assert_eq!(budget.observation().unwrap(), full);
    drop(pressure);
    drop(complete);
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
}

// req: REC-001, STO-012, STO-032, PERF-007
#[test]
fn composite_source_index_finds_full_bounded_suffix_and_releases_retired_ancestors() {
    let (_scope, store, anchor) = fixture();
    let root = Arc::new(CheckpointRoot::new(
        store.shared.database.begin_read().unwrap(),
        1,
    ));
    let mut suffix = ChangelogSuffix::from_checkpoint(&root).unwrap();
    let mut history = anchor;
    let mut middle_pin = None;
    let mut witnesses = Vec::new();
    for index in 0..MAX_JOURNAL_SUFFIX_TRANSITIONS {
        let (frame, receipt, mutations) = source(
            history,
            if index == 0 {
                None
            } else {
                Some(b"same".as_slice())
            },
            Some(b"same"),
        );
        history = history.advance(&receipt).unwrap();
        suffix
            .append(
                Some((receipt.binding(), history)),
                receipt.attribution(),
                mutations,
                frame.encode().unwrap().as_bytes().len(),
            )
            .unwrap();
        let head = suffix.head.as_ref().unwrap();
        assert!(head.ancestors.len() <= 17);
        // Each stored link names precisely 2^level earlier receipt nodes.
        for (level, ancestor) in head.ancestors.iter().enumerate() {
            assert_eq!(
                ancestor.upgrade().unwrap().source.binding.sequence.get(),
                receipt.binding().sequence.get() - (1_u64 << level)
            );
        }
        if index == 0
            || index + 1 == MAX_JOURNAL_SUFFIX_TRANSITIONS / 2
            || index + 1 == MAX_JOURNAL_SUFFIX_TRANSITIONS
        {
            witnesses.push((Arc::downgrade(head), Arc::downgrade(&head.source)));
        }
        if index + 1 == MAX_JOURNAL_SUFFIX_TRANSITIONS / 2 {
            middle_pin = Some(suffix.clone());
        }
    }
    for sequence in 2..=history.tail().sequence().get() {
        let receipt = suffix
            .read_receipt(&root, ChangelogTransactionSequence::new(sequence).unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(receipt.binding().sequence.get(), sequence);
        assert!(!receipt.mutations().is_empty());
        assert_eq!(
            receipt.attribution(),
            ChangelogAttributionV3::JournaledApplicationGroup
        );
    }
    assert!(
        suffix
            .read_receipt(&root, history.tail().sequence().checked_next().unwrap())
            .unwrap()
            .is_none()
    );
    drop(suffix);
    assert!(witnesses[2].0.upgrade().is_none());
    assert!(witnesses[2].1.upgrade().is_none());
    assert!(witnesses[0].0.upgrade().is_some());
    assert!(witnesses[1].0.upgrade().is_some());
    drop(middle_pin);
    assert!(
        witnesses
            .iter()
            .all(|(node, source)| node.upgrade().is_none() && source.upgrade().is_none())
    );
}

// req: REC-001, REC-005, REC-008
#[test]
fn retained_history_native_receipt_preserves_full_checks_without_serialization() {
    let (scope, _store, anchor) = fixture();
    let (frame, expected, _) = source(anchor, None, Some(&vec![b'q'; 262144]));
    let frame = frame.with_changelog(Some(crate::journal::FrameChangelogBinding::new(
        &expected.binding(),
        anchor.lineage().catalog_digest(),
    )));
    let encoded = frame.encode().unwrap();
    let placement = crate::journal::frame_placement_after(0, &encoded).unwrap();
    let database_path = scope.join("retained-history-source.redb");
    let path = crate::journal::journal_path(&database_path);
    let header =
        crate::journal::JournalFileHeader::new(anchor.lineage().database_id(), None, [0; 32]);
    let lane = crate::journal::JournalLane::open(&path, &header).unwrap();
    let generation = lane.generation();
    lane.submit(encoded).unwrap().wait().unwrap();
    drop(lane);
    let _resolver = crate::history_stub::HistoryResolver::register(
        anchor.lineage().database_id(),
        &database_path,
        Arc::new(crate::media::RealJournalMedia),
    );
    let locator = crate::history_stub::StubLocator {
        database_id: anchor.lineage().database_id(),
        generation,
        placement,
    };
    let stub = locator.receipt_stub().unwrap();
    let legacy = crate::history_stub::resolve_history_value(&stub).unwrap();
    assert_eq!(
        AuthoritativeTransactionV3::decode(&legacy).unwrap(),
        expected
    );
    crate::history_stub::RECEIPT_SERIALIZATIONS.with(|count| count.set(0));
    let native = crate::history_stub::decode_history_value(&stub).unwrap();
    assert_eq!(native, expected);
    assert_eq!(native.encode().unwrap().as_slice(), legacy.as_ref());
    assert_eq!(
        native.history_hash().unwrap(),
        expected.history_hash().unwrap()
    );
    assert_eq!(
        anchor.advance(&native).unwrap(),
        anchor.advance(&expected).unwrap()
    );
    crate::history_stub::RECEIPT_SERIALIZATIONS.with(|count| assert_eq!(count.get(), 0));
    assert_eq!(
        crate::history_stub::decode_history_value(&legacy).unwrap(),
        expected
    );
    let mut corrupt_receipt = legacy.into_owned();
    corrupt_receipt[128] ^= 1;
    assert!(crate::history_stub::decode_history_value(&corrupt_receipt).is_err());
    let segment_stub = locator
        .summary_stub(crate::history_stub::SegmentSummary {
            first_commit_sequence: CommitSequence::new(1),
            last_commit_sequence: CommitSequence::new(1),
            command_count: 1,
            ..Default::default()
        })
        .unwrap();
    assert!(crate::history_stub::decode_history_value(&segment_stub).is_err());
    let mut wrong = crate::history_stub::HistoryStub::decode(&stub)
        .unwrap()
        .unwrap();
    wrong.placement.frame_hash[0] ^= 1;
    assert!(crate::history_stub::decode_history_value(&wrong.encode().unwrap()).is_err());
    // Alter the retained payload without changing its claimed frame hash.
    // The native path must still perform the original complete frame checksum.
    use std::io::{Read, Seek, SeekFrom, Write};
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    file.seek(SeekFrom::Start(placement.position as u64 + 256))
        .unwrap();
    let mut byte = [0_u8];
    file.read_exact(&mut byte).unwrap();
    byte[0] ^= 1;
    file.seek(SeekFrom::Start(placement.position as u64 + 256))
        .unwrap();
    file.write_all(&byte).unwrap();
    file.sync_all().unwrap();
    assert!(crate::history_stub::decode_history_value(&stub).is_err());
}

// req: STO-030, STO-031, REC-001, REC-005, REC-008, END-007, END-008
#[test]
fn native_source_point_receipts_keep_full_frame_checks_without_round_trip_serialization() {
    let (scope, store, anchor) = fixture();
    let (frame, expected, _) = source(anchor, None, Some(&vec![b'q'; 262144]));
    let frame = frame.with_changelog(Some(crate::journal::FrameChangelogBinding::new(
        &expected.binding(),
        anchor.lineage().catalog_digest(),
    )));
    let encoded = frame.encode().unwrap();
    let placement = crate::journal::frame_placement_after(0, &encoded).unwrap();
    let database_path = scope.join("retained-history-source.redb");
    let path = crate::journal::journal_path(&database_path);
    let header =
        crate::journal::JournalFileHeader::new(anchor.lineage().database_id(), None, [0; 32]);
    let lane = crate::journal::JournalLane::open(&path, &header).unwrap();
    let generation = lane.generation();
    lane.submit(encoded).unwrap().wait().unwrap();
    drop(lane);
    let _resolver = crate::history_stub::HistoryResolver::register(
        anchor.lineage().database_id(),
        &database_path,
        Arc::new(crate::media::RealJournalMedia),
    );
    let locator = crate::history_stub::StubLocator {
        database_id: anchor.lineage().database_id(),
        generation,
        placement,
    };
    let stub = locator.receipt_stub().unwrap();

    use crate::source_validation_read::ReceiptRead;
    let sequence = expected.binding().sequence;
    let write = store.shared.database.begin_write().unwrap();
    write
        .open_table(HISTORY)
        .unwrap()
        .insert(sequence.get().to_be_bytes().as_slice(), stub.as_slice())
        .unwrap();
    write.commit().unwrap();
    let root = Arc::new(CheckpointRoot::new(
        store.shared.database.begin_read().unwrap(),
        7,
    ));
    let point = crate::source_validation_pin::ValidationSource::Checkpoint(Arc::clone(&root));
    // These least-authority point readers grant no root or mutation proof. The
    // suffix fixture selects the checkpoint fallback over the same exact row.
    let suffix = ChangelogSuffix {
        checkpoint: Some(anchor),
        history: Some(anchor.advance(&expected).unwrap()),
        head: None,
        frames: 0,
        bytes: 0,
    };
    crate::history_stub::RECEIPT_SERIALIZATIONS.with(|count| count.set(0));
    assert_eq!(&*point.receipt(sequence, None).unwrap().unwrap(), &expected);
    assert_eq!(
        suffix.read_receipt(&root, sequence).unwrap().unwrap(),
        expected
    );
    crate::history_stub::RECEIPT_SERIALIZATIONS.with(|count| {
        assert_eq!(count.get(), 0, "source point reads must not serialize a fully checked native receipt merely to decode it again");
    });
    // A later read must check the actual full frame again, rather than treating
    // an earlier successful resolution or the stub's claimed hash as evidence.
    use std::io::{Read, Seek, SeekFrom, Write};
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    file.seek(SeekFrom::Start(placement.position as u64 + 256))
        .unwrap();
    let mut byte = [0_u8];
    file.read_exact(&mut byte).unwrap();
    byte[0] ^= 1;
    file.seek(SeekFrom::Start(placement.position as u64 + 256))
        .unwrap();
    file.write_all(&byte).unwrap();
    file.sync_all().unwrap();
    assert!(point.receipt(sequence, None).is_err());
    assert!(suffix.read_receipt(&root, sequence).is_err());
}
