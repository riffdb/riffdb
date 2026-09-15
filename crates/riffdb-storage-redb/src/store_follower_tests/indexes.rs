//! Isolated typed-row cache evidence; these fixtures do not grant server readiness.
// req: REP-002, REP-003, PERF-007
use super::*;
use riffdb_storage_api::{
    OutboxDestinationIdV1, StoredDurableEventV1, StoredEventRouteV1, StoredOutboxIntentV1,
    StoredOutboxStatusV1,
};
use riffdb_types::{CanonicalRecord, EventId, EventTypeId, PartitionKeyHash, Timestamp};

fn event() -> StoredDurableEventV1 {
    let id = EventId::new(CommitSequence::first(), 0);
    let payload = CanonicalRecord::new(vec![]).unwrap();
    let hash =
        riffdb_storage_api::derive_event_hash_v1(id, EventTypeId::first(), &payload).unwrap();
    StoredDurableEventV1::new(id, EventTypeId::first(), payload, hash).unwrap()
}

fn indexed_applier(path: &Path) -> RedbFollowerApplier {
    let follower = RedbFollowerStore::open(path).unwrap();
    let read = follower.0.shared.database.begin_read().unwrap();
    let indexes = TransientIndexes::rebuild(&read).unwrap();
    *follower.0.shared.transient_indexes.write().unwrap() = TransientIndexState::Ready(indexes);
    drop(read);
    isolated_applier(follower)
}

fn assert_membership(applier: &RedbFollowerApplier, pending: bool, undelivered: bool, route: bool) {
    let state = applier.shared.transient_indexes.read().unwrap();
    let TransientIndexState::Ready(indexes) = &*state else {
        panic!("cache unavailable after successful apply")
    };
    let expected = |present| {
        if present {
            vec![event().event_id()]
        } else {
            vec![]
        }
    };
    assert_eq!(
        indexes.pending_outbox_page(None, 8),
        Some((expected(pending), false))
    );
    assert_eq!(
        indexes.undelivered_outbox_page(None, 8),
        Some((expected(undelivered), false))
    );
    let (_, rows, more) = indexes
        .partition_event_route_page(PartitionKeyHash::from_bytes([9; 32]), None, None, 8)
        .unwrap()
        .unwrap();
    assert_eq!(rows.len(), usize::from(route));
    assert!(!more);
}

#[test]
fn follower_updates_outbox_and_route_indexes_without_rebuilding_history() {
    let scope = crate::test_path::ScopedDirectory::new("follower-indexes");
    let path = scope.join("db.redb");
    let history = fixture(&path);
    let mut applier = indexed_applier(&path);
    let event = event();
    let key = crate::keys::encode_event_key(event.event_id());
    let route_key = crate::keys::encode_event_route_key(
        PartitionKeyHash::from_bytes([9; 32]),
        event.event_id(),
    );
    let intent =
        crate::codec::encode_outbox_intent_v1(&StoredOutboxIntentV1::new(event.clone())).unwrap();
    let route = crate::codec::encode_event_route_v1(StoredEventRouteV1::new(
        event.event_id(),
        event.event_type_id(),
        event.event_hash(),
    ))
    .unwrap();
    let delivered = crate::codec::encode_outbox_status_v1(&StoredOutboxStatusV1::delivered(
        event.event_id(),
        std::num::NonZeroU32::new(1).unwrap(),
        OutboxDestinationIdV1::new("destination").unwrap(),
        Timestamp::new(5, 0).unwrap(),
    ))
    .unwrap();
    let mut puts = vec![
        Mutation::put(N::Outbox, &key, None, intent.as_bytes()).unwrap(),
        Mutation::put(N::EventRoutes, &route_key, None, route.as_bytes()).unwrap(),
    ];
    puts.sort_by_key(|m| (m.namespace(), m.key().to_vec()));
    let bytes = frame(history, vec![puts]);
    applier.apply_frame(&bytes).unwrap();
    assert_membership(&applier, true, true, true);
    applier.apply_frame(&bytes).unwrap();
    assert_membership(&applier, true, true, true);
    applier.resume_stream().unwrap();
    let bytes = frame(
        applier.durable_history().unwrap(),
        vec![vec![
            Mutation::put(N::OutboxStatus, &key, None, delivered.as_bytes()).unwrap(),
        ]],
    );
    applier.apply_frame(&bytes).unwrap();
    assert_membership(&applier, false, false, true);
    applier.resume_stream().unwrap();
    let mut deletes = vec![
        Mutation::delete_matching(N::Outbox, &key, intent.as_bytes()).unwrap(),
        Mutation::delete_matching(N::OutboxStatus, &key, delivered.as_bytes()).unwrap(),
        Mutation::delete_matching(N::EventRoutes, &route_key, route.as_bytes()).unwrap(),
    ];
    deletes.sort_by_key(|m| (m.namespace(), m.key().to_vec()));
    let bytes = frame(applier.durable_history().unwrap(), vec![deletes]);
    applier.apply_frame(&bytes).unwrap();
    assert_membership(&applier, false, false, false);
    assert_eq!(applier.shared.transient_index_rebuilds(), 0);
}

#[test]
fn follower_index_publication_releases_its_lock_across_durability() {
    let scope = crate::test_path::ScopedDirectory::new("follower-index-publication");
    let path = scope.join("db.redb");
    let history = fixture(&path);
    let mut applier = indexed_applier(&path);
    let event = event();
    let key = crate::keys::encode_event_key(event.event_id());
    let intent = crate::codec::encode_outbox_intent_v1(&StoredOutboxIntentV1::new(event)).unwrap();
    let bytes = frame(
        history,
        vec![vec![
            Mutation::put(N::Outbox, &key, None, intent.as_bytes()).unwrap(),
        ]],
    );
    let (controller, schedule) =
        crate::RedbTestController::pause_root_publication_after_engine_commit();
    Arc::get_mut(&mut applier.shared).unwrap().test_controller = Some(controller);
    let shared = Arc::clone(&applier.shared);
    let old = shared.database.begin_read().unwrap();
    let applier = std::thread::scope(|threads| {
        let writer = threads.spawn(move || {
            applier.apply_frame(&bytes).unwrap();
            applier
        });
        schedule.wait_until_commit_is_visible();
        let unlocked = shared
            .transient_indexes
            .try_read()
            .map(|state| matches!(*state, TransientIndexState::Invalid))
            .unwrap_or(false);
        let remains_old = old
            .open_table(OUTBOX)
            .unwrap()
            .get(key.as_slice())
            .unwrap()
            .is_none();
        let reader_shared = Arc::clone(&shared);
        let reader = threads.spawn(move || reader_shared.capture_checkpoint_root().unwrap());
        schedule.release_after_reader_retries();
        let new_root = reader.join().unwrap();
        let applier = writer.join().unwrap();
        assert!(
            unlocked,
            "no index lock or partial cache may span the flush"
        );
        assert!(
            remains_old,
            "pinned authoritative readers retain their exact snapshot"
        );
        assert!(
            new_root
                .transaction()
                .open_table(OUTBOX)
                .unwrap()
                .get(key.as_slice())
                .unwrap()
                .is_some()
        );
        applier
    });
    assert_membership(&applier, true, true, false);
}

#[test]
fn follower_bad_index_transition_rolls_back_authority_and_fences_the_handle() {
    let scope = crate::test_path::ScopedDirectory::new("follower-index-refusal");
    let path = scope.join("db.redb");
    let history = fixture(&path);
    let mut applier = indexed_applier(&path);
    let key = crate::keys::encode_event_key(event().event_id());
    let intent =
        crate::codec::encode_outbox_intent_v1(&StoredOutboxIntentV1::new(event())).unwrap();
    let bytes = frame(
        history,
        vec![
            vec![Mutation::put(N::Outbox, &key, None, intent.as_bytes()).unwrap()],
            vec![Mutation::put(N::OutboxStatus, &key, None, b"malformed").unwrap()],
        ],
    );
    assert!(applier.apply_frame(&bytes).is_err());
    assert_eq!(
        crate::changelog_v3_roots::read_checkpoint_roots(
            &applier.shared.database.begin_read().unwrap()
        )
        .unwrap(),
        Some(history)
    );
    assert!(
        applier
            .shared
            .database
            .begin_read()
            .unwrap()
            .open_table(OUTBOX_STATUS)
            .unwrap()
            .is_empty()
            .unwrap()
    );
    assert!(matches!(
        *applier.shared.transient_indexes.read().unwrap(),
        TransientIndexState::Invalid
    ));
    assert!(applier.resume_stream().is_err());
    assert!(applier.acknowledge_durable_position().is_err());
    drop(applier);
    let reopened = indexed_applier(&path);
    assert_membership(&reopened, false, false, false);
}

fn pending_frame(history: ChangelogHistoryStateV3) -> Vec<u8> {
    let intent =
        crate::codec::encode_outbox_intent_v1(&StoredOutboxIntentV1::new(event())).unwrap();
    frame(
        history,
        vec![vec![
            Mutation::put(
                N::Outbox,
                &crate::keys::encode_event_key(event().event_id()),
                None,
                intent.as_bytes(),
            )
            .unwrap(),
        ]],
    )
}

#[test]
fn follower_index_process_child() {
    let Ok(path) = std::env::var("RIFFDB_FOLLOWER_INDEX_DATABASE") else {
        return;
    };
    let mut applier = indexed_applier(Path::new(&path));
    let bytes = pending_frame(applier.durable_history().unwrap());
    applier.apply_frame(&bytes).unwrap();
    panic!("requested follower index crash edge was not reached");
}

#[test]
fn follower_index_crashes_rebuild_only_the_durable_prefix_before_retry() {
    for edge in ["receipt", "roots", "committed", "indexes-published"] {
        let scope = crate::test_path::ScopedDirectory::new("follower-index-crash");
        let path = scope.join("db.redb");
        let history = fixture(&path);
        let bytes = pending_frame(history);
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "store::follower_tests::indexes::follower_index_process_child",
                "--nocapture",
            ])
            .env("RIFFDB_FOLLOWER_INDEX_DATABASE", &path)
            .env("RIFFDB_FOLLOWER_APPLY_EDGE", edge)
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(93));
        let mut applier = indexed_applier(&path);
        let committed = matches!(edge, "committed" | "indexes-published");
        assert_membership(&applier, committed, committed, false);
        applier.apply_frame(&bytes).unwrap();
        assert_membership(&applier, true, true, false);
        assert_eq!(
            applier.shared.durable_commit_epoch(),
            u64::from(!committed),
            "exact retry after {edge}"
        );
    }
}
