//! ADR-0240 obligation proofs and crash/restart coverage for derived state.
use super::*;
use riffdb_storage_api::{
    ProjectionApplySnapshotRequest, ProjectionGenerationPosition, ProjectionQuerySelector,
    ProjectionRowUpdateV1,
};

fn catching_up(schema: &riffdb_storage_api::CheckedProjectionSchema) -> StoredProjectionControlV1 {
    StoredProjectionControlV1::new(
        schema.identity().clone(),
        ProjectionGeneration::first(),
        None,
        Some(ProjectionGenerationPosition::new(
            ProjectionGeneration::first(),
            FrontierPosition::BeforeFirst,
        )),
        None,
        ProjectionLifecycleV1::CatchingUp,
        None,
    )
    .unwrap()
}

fn ready_through(
    schema: &riffdb_storage_api::CheckedProjectionSchema,
    sequence: CommitSequence,
) -> StoredProjectionControlV1 {
    StoredProjectionControlV1::new(
        schema.identity().clone(),
        ProjectionGeneration::first(),
        Some(ProjectionGenerationPosition::new(
            ProjectionGeneration::first(),
            FrontierPosition::AppliedThrough(sequence),
        )),
        None,
        Some(riffdb_storage_api::PublishedApplyModeV1::Enabled),
        ProjectionLifecycleV1::Ready,
        None,
    )
    .unwrap()
}

fn query(schema: &riffdb_storage_api::CheckedProjectionSchema) -> ProjectionQueryRequest {
    let selector = ProjectionQuerySelector::new(schema.clone(), Vec::new()).unwrap();
    ProjectionQueryRequest::new(selector, NonZeroU16::MIN, None).unwrap()
}

fn plant_ahead_control(
    ports: &RedbOperationalPorts,
    schema: &riffdb_storage_api::CheckedProjectionSchema,
) {
    let ahead = ready_through(schema, CommitSequence::new(99).unwrap());
    install_control(ports, schema.identity(), &ahead);
}

#[test]
fn derived_state_disagreeing_with_the_primary_is_rebuilt_not_trusted() {
    let (_path, mut ports) = operational("derived-disagreement");
    let schema = recovery_projection_schema();
    seed_command(&ports, CommitSequence::first(), 0);
    install_control(&ports, schema.identity(), &catching_up(&schema));
    let key = schema
        .group_key(
            ProjectionGeneration::first(),
            &[CanonicalValue::string("group-a").unwrap()],
        )
        .unwrap();
    let request = ProjectionApplyRequestV1::new(
        schema.clone(),
        ProjectionGeneration::first(),
        CommitSequence::first(),
        FrontierPosition::BeforeFirst,
        vec![
            ProjectionRowUpdateV1::new(
                &schema,
                key.clone(),
                ProjectionRowPrior::Absent,
                recovery_measures(1),
            )
            .unwrap(),
        ],
    )
    .unwrap();
    assert!(matches!(
        ports.apply_projection(&request).unwrap(),
        ProjectionApplyResult::Applied { .. }
    ));
    plant_ahead_control(&ports, &schema);
    assert!(
        !matches!(
            ports.query_projection(&query(&schema)).unwrap(),
            ProjectionQueryResult::Ready { .. }
        ),
        "ahead-of-primary derived rows must not be served"
    );
    let status = ports.read_projection_status(schema.identity()).unwrap();
    assert!(
        status.published().is_none(),
        "disagreeing derived status must not present a published generation"
    );
    seed_command(&ports, CommitSequence::new(2).unwrap(), 0);
    assert_eq!(
        ports.apply_projection(&request).unwrap(),
        ProjectionApplyResult::StateChanged
    );
    assert!(
        !matches!(
            ports.query_projection(&query(&schema)).unwrap(),
            ProjectionQueryResult::Ready { .. }
        ),
        "discarded derived state must not be served"
    );
    install_control(&ports, schema.identity(), &catching_up(&schema));
    assert!(matches!(
        ports.apply_projection(&request).unwrap(),
        ProjectionApplyResult::Applied { .. }
    ));
}

#[test]
fn derived_state_stale_torn_absent_or_ahead_after_restart_is_rebuilt() {
    for kind in ["stale", "torn", "absent", "ahead"] {
        let (path, mut ports) = operational(&format!("derived-restart-{kind}"));
        let schema = recovery_projection_schema();
        seed_command(&ports, CommitSequence::first(), 0);
        install_control(&ports, schema.identity(), &catching_up(&schema));
        let key = schema
            .group_key(
                ProjectionGeneration::first(),
                &[CanonicalValue::string("group-a").unwrap()],
            )
            .unwrap();
        if kind != "absent" {
            let request = ProjectionApplyRequestV1::new(
                schema.clone(),
                ProjectionGeneration::first(),
                CommitSequence::first(),
                FrontierPosition::BeforeFirst,
                vec![
                    ProjectionRowUpdateV1::new(
                        &schema,
                        key.clone(),
                        ProjectionRowPrior::Absent,
                        recovery_measures(1),
                    )
                    .unwrap(),
                ],
            )
            .unwrap();
            assert!(matches!(
                ports.apply_projection(&request).unwrap(),
                ProjectionApplyResult::Applied { .. }
            ));
        }
        match kind {
            "ahead" => plant_ahead_control(&ports, &schema),
            "torn" => {
                install_control(
                    &ports,
                    schema.identity(),
                    &ready_through(&schema, CommitSequence::first()),
                );
                let access = ports.begin_derived_write().unwrap();
                access
                    .transaction()
                    .unwrap()
                    .open_table(PROJECTION_APPLIED)
                    .unwrap()
                    .retain(|_, _| false)
                    .unwrap();
                access
                    .commit_for(RedbTestOperation::ProjectionMutation)
                    .unwrap();
            }
            "stale" => {
                seed_command(&ports, CommitSequence::new(2).unwrap(), 0);
            }
            "absent" => {}
            _ => unreachable!(),
        }
        drop(ports);
        let store = RedbStore::open(&path.0).unwrap();
        let ports = crate::store::RedbDormantPorts {
            pending_v3_activation: None,
            shared: store.shared,
        }
        .into_operational_after_catalog_validation()
        .unwrap();
        let result = ports.query_projection(&query(&schema)).unwrap();
        if kind == "stale" {
            assert!(
                matches!(result, ProjectionQueryResult::Degraded { .. }),
                "{kind} may lag but must not invent commits"
            );
        } else {
            assert!(
                !matches!(result, ProjectionQueryResult::Ready { .. }),
                "{kind} must not be served as ready after restart"
            );
        }
        let snapshot = ProjectionApplySnapshotRequest::new(
            schema.clone(),
            ProjectionGeneration::first(),
            vec![key],
        )
        .unwrap();
        if kind == "ahead" {
            assert!(
                ports.read_apply_snapshot(&snapshot).is_err()
                    || !matches!(
                        ports.query_projection(&query(&schema)).unwrap(),
                        ProjectionQueryResult::Ready { .. }
                    )
            );
        }
    }
}

#[test]
fn declaring_a_projection_costs_no_write_throughput() {
    let (_path, mut ports) = operational("derived-throughput");
    let schema = recovery_projection_schema();
    install_control(&ports, schema.identity(), &catching_up(&schema));
    for value in 1..=64 {
        seed_command(&ports, CommitSequence::new(value).unwrap(), 0);
    }
    let before = ports.mutation_gate_tickets();
    let mut control = catching_up(&schema);
    for first in (1..=64).step_by(16) {
        let last = first + 15;
        let mut frontier = control.frontier_for(ProjectionGeneration::first()).unwrap();
        let mut members = Vec::new();
        for value in first..=last {
            let sequence = CommitSequence::new(value).unwrap();
            members.push(
                ProjectionApplyRequestV1::new(
                    schema.clone(),
                    ProjectionGeneration::first(),
                    sequence,
                    frontier,
                    vec![],
                )
                .unwrap(),
            );
            frontier = FrontierPosition::AppliedThrough(sequence);
        }
        let request =
            riffdb_storage_api::ProjectionApplyBatchV1::new(control.clone(), members, vec![])
                .unwrap();
        let riffdb_storage_api::ProjectionApplyBatchResult::Applied(next) =
            ports.apply_projection_batch(&request).unwrap()
        else {
            panic!("batch applies without taking the primary gate");
        };
        control = next;
    }
    assert_eq!(
        ports.mutation_gate_tickets(),
        before,
        "declaring and applying a projection must not acquire the primary mutation gate"
    );

    let (done_tx, done_rx) = std::sync::mpsc::channel();
    std::thread::scope(|scope| {
        let held = ports
            .begin_derived_write()
            .expect("hold sidecar write while a command commits");
        scope.spawn(|| {
            seed_command(&ports, CommitSequence::new(97).unwrap(), 0);
            done_tx.send(()).expect("primary write finished");
        });
        done_rx
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("primary write must complete while a sidecar write is held");
        held.abort().expect("release held sidecar write");
    });
}

#[test]
fn restore_leaves_no_derived_state_from_the_replaced_timeline() {
    let (path, mut ports) = operational("derived-restore-sidecar");
    let schema = recovery_projection_schema();
    seed_command(&ports, CommitSequence::first(), 0);
    install_control(&ports, schema.identity(), &catching_up(&schema));
    let key = schema
        .group_key(
            ProjectionGeneration::first(),
            &[CanonicalValue::string("group-a").unwrap()],
        )
        .unwrap();
    let request = ProjectionApplyRequestV1::new(
        schema.clone(),
        ProjectionGeneration::first(),
        CommitSequence::first(),
        FrontierPosition::BeforeFirst,
        vec![
            ProjectionRowUpdateV1::new(
                &schema,
                key,
                ProjectionRowPrior::Absent,
                recovery_measures(1),
            )
            .unwrap(),
        ],
    )
    .unwrap();
    assert!(matches!(
        ports.apply_projection(&request).unwrap(),
        ProjectionApplyResult::Applied { .. }
    ));
    install_control(
        &ports,
        schema.identity(),
        &ready_through(&schema, CommitSequence::first()),
    );
    assert!(matches!(
        ports.query_projection(&query(&schema)).unwrap(),
        ProjectionQueryResult::Ready { .. }
    ));
    drop(ports);
    assert!(
        crate::store::owned_store_files(&path.0)
            .iter()
            .any(|file| file == &crate::store::derived_store_path(&path.0)),
        "owned file set must include the sidecar"
    );
    crate::store::discard_replaced_derived_sidecar(&path.0).unwrap();
    let store = RedbStore::open(&path.0).unwrap();
    let ports = crate::store::RedbDormantPorts {
        pending_v3_activation: None,
        shared: store.shared,
    }
    .into_operational_after_catalog_validation()
    .unwrap();
    assert!(
        !matches!(
            ports.query_projection(&query(&schema)).unwrap(),
            ProjectionQueryResult::Ready { .. }
        ),
        "replaced-timeline derived rows must not survive restore"
    );
}

#[test]
fn concurrent_first_touch_of_the_sidecar_never_reports_unavailable() {
    // redb takes an exclusive file lock, so two threads racing to open the
    // sidecar leave one holding an Unavailable that describes the race rather
    // than the store. Under nextest's parallel profile that surfaced as a
    // projection query failing with storage_unavailable while the same test
    // passed in isolation, which is the shape that gets dismissed as flake.
    //
    // Every thread here touches the sidecar for the first time at once. All of
    // them must succeed: an Unavailable from this path has to mean the store is
    // actually unavailable, or it cannot be acted on.
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let (path, ports) = operational("derived-open-race");
    let shared = Arc::clone(&ports.shared);
    let barrier = Arc::new(std::sync::Barrier::new(8));
    let failures = Arc::new(AtomicUsize::new(0));
    let mut threads = Vec::with_capacity(8);
    for _ in 0..8 {
        let shared = Arc::clone(&shared);
        let barrier = Arc::clone(&barrier);
        let failures = Arc::clone(&failures);
        threads.push(std::thread::spawn(move || {
            barrier.wait();
            if shared.derived_database().is_err() {
                failures.fetch_add(1, Ordering::SeqCst);
            }
        }));
    }
    for thread in threads {
        thread.join().expect("racing opener must not panic");
    }
    assert_eq!(
        failures.load(Ordering::SeqCst),
        0,
        "a concurrent first touch reported the sidecar unavailable"
    );
    drop(path);
}

// req: STO-030
/// An absent sidecar is observed on disk once, not on every read, and a
/// sidecar this handle then creates is read rather than the empty view.
#[test]
fn absent_sidecar_is_probed_once_and_a_later_creation_is_read() {
    use redb::ReadableTableMetadata as _;
    let frontier_rows = |read: &redb::ReadTransaction| {
        read.open_table(crate::layout::PROJECTION_FRONTIER)
            .map_or(0, |table| table.len().unwrap())
    };

    let (path, ports) = operational("derived-absent-probe");
    assert!(!crate::store::derived_store_path(&path.0).exists());
    crate::store::DERIVED_PATH_PROBES.with(|probes| probes.set(0));
    for _ in 0..16 {
        let read = ports.begin_derived_read().unwrap();
        assert_eq!(frontier_rows(&read), 0);
    }
    assert_eq!(
        crate::store::DERIVED_PATH_PROBES.with(std::cell::Cell::get),
        1,
        "an absent sidecar must be probed once per handle"
    );
    assert!(!crate::store::derived_store_path(&path.0).exists());

    let write = ports.begin_derived_write().unwrap();
    write
        .transaction()
        .unwrap()
        .open_table(crate::layout::PROJECTION_FRONTIER)
        .unwrap()
        .insert(b"probe".as_slice(), [1_u8].as_slice())
        .unwrap();
    write
        .commit_for(crate::hooks::RedbTestOperation::ProjectionMutation)
        .unwrap();
    assert!(crate::store::derived_store_path(&path.0).exists());
    let read = ports.begin_derived_read().unwrap();
    assert_eq!(
        frontier_rows(&read),
        1,
        "a sidecar created by this handle must be read, not the empty view"
    );
    assert_eq!(
        crate::store::DERIVED_PATH_PROBES.with(std::cell::Cell::get),
        1
    );
    drop(ports);
}

// req: STO-030
/// ADR-0085 Amendment 2 fence after ADR-0240: document projection frontiers
/// live in the derived sidecar, and retention must read them there. Before
/// this, retention read only the primary's table, so a catching-up projection
/// was invisible and a prune could delete the history it still needs.
#[test]
fn retention_fences_on_the_derived_sidecar_projection_frontier() {
    let (path, ports) = operational("retention-derived-projection-fence");
    let schema = recovery_projection_schema();
    seed_command(&ports, CommitSequence::first(), 0);
    install_control(&ports, schema.identity(), &catching_up(&schema));
    drop(ports);
    let retention = crate::RedbOfflineRetention::bind(&path.0);
    let status = retention.status().expect("retention status");
    assert_eq!(status.max_permissible_watermark, Some(0));
    assert_eq!(
        status.fence_binding,
        riffdb_storage_api::RetentionFenceBinding::ProjectionDurableFrontier
    );
    let refused = retention
        .prune_to(1)
        .expect_err("a catching-up projection fences the prune");
    assert_eq!(refused.kind(), StorageErrorKind::InvariantViolation);
    assert_eq!(
        retention
            .status()
            .expect("status after refusal")
            .watermark_sequence,
        0,
        "the refused prune deleted nothing"
    );
}

// req: STO-030
/// The online lane reads the same sidecar projection fence while it holds the
/// ordered writer gate, and a fenced step deletes nothing.
#[test]
fn online_retention_fences_on_the_derived_sidecar_projection_frontier() {
    let (path, ports) = operational("online-retention-derived-projection-fence");
    let schema = recovery_projection_schema();
    seed_command(&ports, CommitSequence::first(), 0);
    seed_command(&ports, CommitSequence::new(2).unwrap(), 0);
    install_control(&ports, schema.identity(), &catching_up(&schema));
    let step = ports
        .online_retention_step(
            std::num::NonZeroU64::new(1).unwrap(),
            std::num::NonZeroU64::new(256).unwrap(),
        )
        .expect("fenced online step");
    assert_eq!(
        step,
        crate::OnlineRetentionStep::Held(crate::OnlineRetentionHold::Fenced(
            riffdb_storage_api::RetentionFenceBinding::ProjectionDurableFrontier
        ))
    );
    drop(ports);
    assert_eq!(
        crate::RedbOfflineRetention::bind(&path.0)
            .status()
            .expect("status")
            .watermark_sequence,
        0
    );
}

// req: STO-031
/// The online lane honors the durable event-consumer low water: it prunes up
/// to the lowest consumer checkpoint's retention frontier and then holds with
/// the consumer binding, deleting nothing the consumer may still need.
#[test]
fn online_retention_fences_on_the_durable_consumer_low_water() {
    use riffdb_storage_api::{
        AuthoritativePointReader, ConsumerCheckpointV1, EventConsumerIdentityV1,
        StoredEventConsumerV1, proto_codec::encode_event_consumer_v1,
    };
    use riffdb_types::EventConsumerRevision;
    let (_path, ports) = operational("online-retention-consumer-fence");
    for sequence in 1..=3 {
        seed_command(&ports, CommitSequence::new(sequence).unwrap(), 0);
    }
    // A checkpoint after sequence 2 still needs sequence 2: the consumer's
    // retention frontier is sequence 1.
    let identity = EventConsumerIdentityV1::new(
        database_id(),
        riffdb_types::ReactiveModuleHash::from_bytes([0x41; 32]),
        riffdb_types::ReactiveOperationName::new("RowChanges").unwrap(),
        riffdb_types::QueryParameterHash::from_bytes([0x42; 32]),
        riffdb_types::EventConsumerName::new("retention-fence").unwrap(),
    );
    let consumer = StoredEventConsumerV1::checked(
        identity.clone(),
        riffdb_types::PartitionKeyHash::from_bytes([0x43; 32]),
        1,
        EventConsumerRevision::first(),
        ConsumerCheckpointV1::After(riffdb_types::EventId::new(
            CommitSequence::new(2).unwrap(),
            0,
        )),
        Vec::new(),
    )
    .unwrap();
    let encoded = encode_event_consumer_v1(&consumer).unwrap();
    let access = ports.begin_write().expect("begin consumer seed");
    access
        .transaction()
        .expect("consumer seed transaction")
        .open_table(crate::layout::EVENT_CONSUMERS)
        .expect("consumer table")
        .insert(
            crate::keys::encode_event_consumer_key(identity.identity_hash()).as_slice(),
            encoded.as_bytes(),
        )
        .expect("insert consumer");
    access.commit().expect("commit consumer seed");

    let retain = std::num::NonZeroU64::new(1).unwrap();
    let budget = std::num::NonZeroU64::new(256).unwrap();
    assert_eq!(
        ports.online_retention_step(retain, budget).expect("step"),
        crate::OnlineRetentionStep::Advanced {
            through: 1,
            sequences: 1
        }
    );
    assert_eq!(
        ports
            .online_retention_step(retain, budget)
            .expect("fenced step"),
        crate::OnlineRetentionStep::Held(crate::OnlineRetentionHold::Fenced(
            riffdb_storage_api::RetentionFenceBinding::ConsumerLowWater
        ))
    );
    assert!(
        ports
            .read_commit(CommitSequence::new(2).unwrap())
            .expect("read the fenced commit")
            .is_some()
    );
}

// req: STO-030, STO-032
#[test]
fn prepared_prune_revalidation_abandons_a_stale_watermark_without_writes() {
    use riffdb_storage_api::{AuthoritativePointReader, DeferredCommandEpochPort};
    let (_path, ports) = operational("prepared-prune-stale-watermark");
    for sequence in 1..=3 {
        seed_command(&ports, CommitSequence::new(sequence).unwrap(), 0);
    }
    let retain = std::num::NonZeroU64::new(1).unwrap();
    let prepared = crate::retention::prepared::PreparedPrune::prepare(&ports, retain, 1)
        .expect("prepare against old watermark")
        .expect("prunable prefix");
    assert_eq!(
        ports
            .online_retention_step(retain, std::num::NonZeroU64::new(1).unwrap())
            .unwrap(),
        crate::OnlineRetentionStep::Advanced {
            through: 1,
            sequences: 1
        }
    );
    let access = ports
        .begin_deferred_service_audit_write()
        .expect("next ordered turn");
    let before = access
        .read_command_value(
            crate::journal::JournalTable::Meta,
            crate::layout::META_RETENTION_WATERMARK.as_bytes(),
        )
        .unwrap();
    assert!(
        !prepared
            .validate_and_stage(&access)
            .expect("stale attempt abandoned")
    );
    assert_eq!(
        access
            .read_command_value(
                crate::journal::JournalTable::Meta,
                crate::layout::META_RETENTION_WATERMARK.as_bytes()
            )
            .unwrap(),
        before
    );
    access.abort().expect("release private attempt");
    assert_eq!(
        ports
            .read_commit(CommitSequence::first())
            .unwrap_err()
            .kind(),
        StorageErrorKind::HistoryPruned
    );
    assert!(
        ports
            .read_commit(CommitSequence::new(2).unwrap())
            .unwrap()
            .is_some()
    );
    drop(
        ports
            .begin_deferred_command_epoch()
            .expect("abandoning stale preparation does not fence the writer"),
    );
}
