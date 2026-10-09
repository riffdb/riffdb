//! Exact metadata admission only; this does not confer source-cut authority.
// req: STO-030, STO-031, STO-032, REC-001
use super::*;
use crate::journal::{JournalFileHeader, JournalFrame, JournalLane, JournalMutation, JournalTable};
use std::num::NonZeroU64;

// req: STO-030, STO-031, STO-032, REC-001
#[test]
fn published_composite_source_validation_matches_materialized_full_oracle() {
    use crate::changelog_v3_retained_validation::RetainedHistoryValidation;
    use crate::source_validation_pin::ValidationSource;
    let (_scope, mut ports, _) = crate::changelog_v3_control_tests::fixture();
    append_acknowledged_audits(&mut ports, 300);
    let RedbReadAccess::Composite(view) = ports.begin_composite_read().unwrap() else {
        panic!("acknowledged journal receipts retain their composite pin");
    };
    let expected =
        crate::changelog_v3_cursor::history(&RedbReadAccess::Composite(Arc::clone(&view))).unwrap();
    let mut walk = RetainedHistoryValidation::new_source(ValidationSource::Composite(view))
        .unwrap()
        .unwrap();
    let mut pages = 0;
    while !walk
        .advance(riffdb_storage_api::EvidencePageLimit::new(1).unwrap())
        .unwrap()
    {
        assert!(walk.proof().is_err());
        pages += 1;
        assert!(pages < 1000);
    }
    assert!(pages >= 600);
    assert_eq!(walk.proof().unwrap().history(), expected);
    assert_eq!(
        walk.rows_read(),
        [
            expected.tail().sequence().get() - expected.minimum_resume().sequence().get() + 1,
            300
        ]
    );
    let lease = ports.shared.mutation_gate.acquire().unwrap();
    ports
        .shared
        .checkpoint_published_journal_suffix_for_barrier()
        .unwrap();
    let root = ports.shared.capture_checkpoint_root().unwrap();
    assert_eq!(
        crate::changelog_v3_roots::validate_retained_history(&root).unwrap(),
        Some(expected)
    );
    drop(lease);
}

// req: STO-030, STO-031, STO-032, REC-001, END-008
#[test]
fn source_reclamation_progresses_without_draining_oversized_published_tail() {
    use riffdb_storage_api::{ServiceAuditAppendRepository, ServiceAuditGroupAppend};
    let (_scope, mut ports, _) = crate::changelog_v3_control_tests::fixture();
    let mut control = ports.replication_source_control();
    for _ in 0..16 {
        assert_eq!(control.reclaim_history_online().unwrap().0, 0);
        if control.reclamation_yields()[0] == 1 {
            break;
        }
    }
    assert_eq!(control.reclamation_yields()[0], 1);
    append_acknowledged_audits(&mut ports, 300);
    ports
        .shared
        .mutation_gate
        .note_source_publication_request()
        .unwrap();
    let ordinary = ports.begin_deferred_service_audit_write().unwrap();
    ordinary.abort().unwrap();
    let writer = ports.shared.mutation_gate.acquire().unwrap();
    ports.shared.poll_async_checkpoint_locked(true).unwrap();
    drop(writer);
    ports.shared.join_journal_sealer();
    for ordinal in 301..=557 {
        let ServiceAuditGroupAppend::Submitted(fence) = ports
            .submit_service_audit_group(&[source_audit_intent(ordinal)])
            .unwrap()
        else {
            panic!("the distinct audit retains its original completion fence");
        };
        assert_eq!(fence.wait().unwrap().len(), 1);
    }
    assert_eq!(ports.shared.journal_suffix_activity().unwrap().1, 257);
    let publication = ports.shared.capture_checkpoint_root().unwrap().identity();
    let mut removed = 0;
    for _ in 0..16 {
        removed += control.reclaim_history_online().unwrap().0;
        for _ in 0..16 {
            if !control.receipt_validation_pending() {
                break;
            }
            control.advance_validation_off_gate().unwrap();
        }
        if removed > 0 {
            break;
        }
    }
    assert!(
        removed > 0,
        "a fully published tail must not prevent a bounded licensed source cut"
    );
    assert!(removed <= 256);
    assert_eq!(
        ports.shared.capture_checkpoint_root().unwrap().identity(),
        publication,
        "reclamation must not materialize the oversized active tail"
    );
    assert!(!ports.shared.async_checkpoint_in_flight().unwrap());
    assert_eq!(
        ports
            .begin_composite_read()
            .unwrap()
            .read_range_to(JournalTable::Audit, &[], None, 600)
            .unwrap()
            .len(),
        557
    );
}

// req: STO-030, STO-031, STO-032, REC-001, END-007, END-008
#[test]
fn completed_source_proof_catches_published_checkpoint_before_requesting_admission() {
    use riffdb_storage_api::{ServiceAuditAppendRepository, ServiceAuditGroupAppend};
    let (_scope, mut ports, _) = crate::changelog_v3_control_tests::fixture();
    let mut control = ports.replication_source_control();
    assert_eq!(control.reclaim_history_online().unwrap().0, 0);
    for _ in 0..16 {
        control.advance_validation_off_gate().unwrap();
        if !control.receipt_validation_pending() {
            break;
        }
    }
    assert!(!control.receipt_validation_pending());
    let original_root = ports.shared.capture_checkpoint_root().unwrap().identity();
    let qualification = ports
        .shared
        .source_continuation_epoch
        .load(Ordering::Acquire);
    append_acknowledged_audits(&mut ports, 300);
    ports
        .shared
        .mutation_gate
        .note_source_publication_request()
        .unwrap();
    let ordinary = ports.begin_deferred_service_audit_write().unwrap();
    ordinary.abort().unwrap();
    let writer = ports.shared.mutation_gate.acquire().unwrap();
    ports.shared.poll_async_checkpoint_locked(true).unwrap();
    drop(writer);
    ports.shared.join_journal_sealer();
    assert!(ports.shared.capture_checkpoint_root().unwrap().identity() > original_root);
    assert_eq!(
        ports
            .shared
            .source_continuation_epoch
            .load(Ordering::Acquire),
        qualification
    );

    // Both the installed checkpoint and its acknowledged active tail can be
    // continued off-gate. The ordinary writer below prevents source admission.
    for ordinal in 301..=557 {
        let ServiceAuditGroupAppend::Submitted(fence) = ports
            .submit_service_audit_group(&[source_audit_intent(ordinal)])
            .unwrap()
        else {
            panic!("the distinct audit retains its original completion fence");
        };
        assert_eq!(fence.wait().unwrap().len(), 1);
    }
    let census = ports.shared.mutation_gate.source_census().unwrap();
    let epoch = ports.shared.durable_commit_epoch();
    let live_pins = || {
        ports
            .shared
            .extent_roots
            .lock()
            .unwrap()
            .iter()
            .filter(|root| root.strong_count() != 0)
            .count()
    };
    let pins_before = live_pins();
    let published = ports.shared.published_composite_view().unwrap().unwrap();
    let captured = Arc::downgrade(&published);
    drop(published);
    let source_owners_before = captured.strong_count();
    let writer = ports.shared.mutation_gate.acquire().unwrap();
    assert_eq!(control.reclaim_history_online().unwrap().0, 0);
    assert!(
        control.receipt_validation_pending(),
        "installed checkpoint continuation must start before a new source turn"
    );
    assert_eq!(ports.shared.mutation_gate.source_census().unwrap(), census);
    assert_eq!(
        live_pins(),
        pins_before,
        "the candidate shares the actual published root"
    );
    assert_eq!(captured.strong_count(), source_owners_before + 1);
    let [initial, mut continued_rows, invalidated, _] = control.validation_phases();
    assert_eq!(initial, 0);
    assert_eq!(invalidated, 0);
    assert!(continued_rows <= 256);
    for _ in 0..16 {
        let [initial, continued, invalidated] = control.advance_validation_off_gate().unwrap();
        assert_eq!(initial, 0);
        assert!(continued <= 256);
        assert_eq!(invalidated, 0);
        continued_rows += continued;
        if !control.receipt_validation_pending() {
            break;
        }
    }
    assert!(!control.receipt_validation_pending());
    assert_eq!(
        continued_rows, 1114,
        "only the 557 new receipts and their audit rows are validated"
    );
    assert_eq!(
        live_pins(),
        pins_before,
        "exact end releases the replacement pin"
    );
    assert_eq!(captured.strong_count(), source_owners_before);
    assert_eq!(ports.shared.mutation_gate.source_census().unwrap(), census);
    assert_eq!(ports.shared.durable_commit_epoch(), epoch);
    assert!(ports.shared.mutation_gate.is_held());
    assert_eq!(ports.shared.journal_suffix_activity().unwrap().1, 257);
    drop(writer);
    assert_eq!(control.reclaim_history_online().unwrap().0, 0);
    assert_eq!(
        control.reclamation_yields()[0],
        1,
        "a completed proof alone is not a prior independently durable observation"
    );
    assert_eq!(control.admission_yields()[1], 0);
    assert_eq!(ports.shared.durable_commit_epoch(), epoch);
    assert_eq!(ports.shared.journal_suffix_activity().unwrap().1, 257);
    assert!(!ports.shared.async_checkpoint_in_flight().unwrap());
}

#[test]
fn source_validation_pages_finish_without_requesting_a_writer_turn() {
    let (_scope, ports, _) = crate::changelog_v3_control_tests::fixture();
    let mut control = ports.replication_source_control();
    let live_pins = || {
        ports
            .shared
            .extent_roots
            .lock()
            .unwrap()
            .iter()
            .filter(|root| root.strong_count() != 0)
            .count()
    };
    let pins_before = live_pins();
    assert_eq!(control.reclaim_history_online().unwrap().0, 0);
    assert!(control.receipt_validation_pending());
    assert_eq!(live_pins(), pins_before + 1);
    let durable_epoch = ports.shared.durable_commit_epoch();
    let before = ports.shared.mutation_gate.source_census().unwrap();
    let writer = ports.shared.mutation_gate.acquire().unwrap();
    for _ in 0..16 {
        if !control.receipt_validation_pending() {
            break;
        }
        let [initial, continued, invalidated] = control.advance_validation_off_gate().unwrap();
        assert!(initial + continued <= 256);
        assert_eq!(invalidated, 0);
        assert_eq!(
            ports.shared.mutation_gate.source_census().unwrap(),
            before,
            "read-only validation must not request a source turn at exact end"
        );
    }
    assert!(!control.receipt_validation_pending());
    assert_eq!(ports.shared.durable_commit_epoch(), durable_epoch);
    assert_eq!(
        live_pins(),
        pins_before,
        "exact end releases the captured pin"
    );
    assert_eq!(control.advance_validation_off_gate().unwrap(), [0; 3]);
    assert_eq!(ports.shared.mutation_gate.source_census().unwrap(), before);
    assert!(ports.shared.mutation_gate.is_held());
    drop(writer);
}

#[test]
fn off_gate_validation_never_captures_a_missing_or_invalidated_candidate() {
    let (_scope, ports, _) = crate::changelog_v3_control_tests::fixture();
    let mut control = ports.replication_source_control();
    let before = ports.shared.mutation_gate.source_census().unwrap();
    assert_eq!(control.advance_validation_off_gate().unwrap(), [0; 3]);
    assert_eq!(ports.shared.mutation_gate.source_census().unwrap(), before);
    assert!(!control.receipt_validation_pending());
    assert!(ports.shared.journal_runtime.lock().unwrap().is_none());
    assert_eq!(control.reclaim_history_online().unwrap().0, 0);
    assert!(control.receipt_validation_pending());
    let before = ports.shared.mutation_gate.source_census().unwrap();
    ports.shared.invalidate_source_continuation();
    assert_eq!(control.advance_validation_off_gate().unwrap(), [0, 0, 1]);
    assert!(!control.receipt_validation_pending());
    assert_eq!(control.advance_validation_off_gate().unwrap(), [0; 3]);
    assert_eq!(ports.shared.mutation_gate.source_census().unwrap(), before);
    assert!(ports.shared.journal_runtime.lock().unwrap().is_none());
    assert!(
        ports
            .shared
            .extent_roots
            .lock()
            .unwrap()
            .iter()
            .all(|root| root.strong_count() == 0)
    );
}

#[test]
fn off_gate_validation_corruption_releases_proof_and_reader_without_a_writer_turn() {
    let (_scope, ports, states) = crate::changelog_v3_control_tests::fixture();
    let transaction = ports.shared.database.begin_write().unwrap();
    transaction
        .open_table(crate::changelog_v3_activation::HISTORY)
        .unwrap()
        .insert(
            states[2].tail().sequence().get().to_be_bytes().as_slice(),
            b"invalid-receipt".as_slice(),
        )
        .unwrap();
    ports.shared.commit_durable(transaction).unwrap();
    let mut control = ports.replication_source_control();
    assert_eq!(control.reclaim_history_online().unwrap().0, 0);
    assert!(control.receipt_validation_pending());
    let before = ports.shared.mutation_gate.source_census().unwrap();
    let writer = ports.shared.mutation_gate.acquire().unwrap();
    let durable_epoch = ports.shared.durable_commit_epoch();
    let error = control.advance_validation_off_gate().unwrap_err();
    assert_eq!(error.kind(), StorageErrorKind::CorruptData);
    assert!(!control.receipt_validation_pending());
    assert_eq!(control.advance_validation_off_gate().unwrap(), [0; 3]);
    assert_eq!(ports.shared.mutation_gate.source_census().unwrap(), before);
    assert_eq!(ports.shared.durable_commit_epoch(), durable_epoch);
    assert!(
        ports
            .shared
            .extent_roots
            .lock()
            .unwrap()
            .iter()
            .all(|root| root.strong_count() == 0)
    );
    assert!(ports.shared.mutation_gate.is_held());
    drop(writer);
}

#[test]
fn unfinished_extent_publication_yields_before_spending_or_waiting_for_a_root_pin() {
    let (_scope, ports, _) = crate::changelog_v3_control_tests::fixture();
    let mut roots = Vec::new();
    for _ in 0..crate::store::extent_reclamation::MAX_EXTENT_READ_ROOTS {
        roots.push(ports.shared.capture_checkpoint_root().unwrap());
    }
    let (entered, entry) = std::sync::mpsc::channel();
    let (release, released) = std::sync::mpsc::channel();
    let finished = Arc::new(CheckpointCompletion::default());
    let completion = CheckpointPublisher(Arc::clone(&finished));
    *ports.shared.journal_sealer.lock().unwrap() = Some(JournalSealer {
        publication_complete: Arc::new(AtomicBool::new(false)),
        finished,
        handle: std::thread::spawn(move || {
            entered.send(()).unwrap();
            released.recv().unwrap();
            completion.publish(Ok(()));
        }),
    });
    entry.recv().unwrap();
    let lease = ports
        .shared
        .mutation_gate
        .acquire_source()
        .unwrap()
        .unwrap();
    let readiness = ports.shared.prepare_online_source_root();
    drop(lease);
    release.send(()).unwrap();
    ports.shared.join_journal_sealer();
    assert!(
        matches!(readiness, Ok(Readiness::CheckpointPublication)),
        "known unfinished publication must yield before attempting a root capture"
    );
    assert_eq!(ports.shared.extent_roots.lock().unwrap().len(), roots.len());
    assert!(!ports.shared.mutation_gate.is_held());
}

#[test]
fn source_owner_retirement_releases_its_actual_validation_reader_pin() {
    let (_scope, ports, _) = crate::changelog_v3_control_tests::fixture();
    let live_pins = || {
        ports
            .shared
            .extent_roots
            .lock()
            .unwrap()
            .iter()
            .filter(|root| root.strong_count() != 0)
            .count()
    };
    let before = live_pins();
    let mut control = ports.replication_source_control();
    assert_eq!(control.reclaim_history_online().unwrap().0, 0);
    assert!(control.receipt_validation_pending());
    assert_eq!(live_pins(), before + 1);
    ports.shared.retire_online_source_admission();
    control.retire_online_validation();
    assert_eq!(live_pins(), before);
    assert!(!control.receipt_validation_pending());
    assert!(!ports.shared.mutation_gate.is_held());
    assert!(
        ports
            .shared
            .mutation_gate
            .acquire_source()
            .unwrap()
            .is_none()
    );
    drop(ports.shared.mutation_gate.acquire().unwrap());
}

#[test]
fn source_capture_does_not_initialize_an_absent_native_journal_lane() {
    let (_scope, ports, _) = crate::changelog_v3_control_tests::fixture();
    assert!(ports.shared.journal_runtime.lock().unwrap().is_none());
    let mut control = ports.replication_source_control();
    assert_eq!(control.reclaim_history_online().unwrap().0, 0);
    assert!(control.receipt_validation_pending());
    assert!(ports.shared.journal_runtime.lock().unwrap().is_none());
    assert!(!ports.shared.mutation_gate.is_held());
}

fn source_audit_intent(ordinal: u64) -> riffdb_storage_api::ServiceAuditAppendIntentV1 {
    use riffdb_storage_api::{AuditPrincipalV1, ServiceAuditAppendIntentV1};
    use riffdb_types::{
        ActorId, ActorKind, CapabilityId, RequestId, ServiceAuditLinkV1, ServiceAuditPhaseV1,
        ServiceAuditTargetsV1, ServiceIngressKindV1, ServiceOperationV1, Timestamp,
    };
    ServiceAuditAppendIntentV1::new(
        RequestId::from_unix_milliseconds_and_random(19 + ordinal, [0x51; 10]).unwrap(),
        Timestamp::new(1700000000, 0).unwrap(),
        ServiceOperationV1::GetHealth,
        ServiceAuditPhaseV1::Denied,
        AuditPrincipalV1::new(
            ActorId::new("checkpoint-fixture").unwrap(),
            ActorKind::Human,
            CapabilityId::from_unix_milliseconds_and_random(19, [0x52; 10]).unwrap(),
            std::num::NonZeroU64::MIN,
        ),
        ServiceIngressKindV1::Grpc,
        ServiceAuditTargetsV1::empty(),
        None,
        ServiceAuditLinkV1::None,
    )
    .unwrap()
}

fn append_acknowledged_audits(ports: &mut RedbOperationalPorts, count: u64) {
    use riffdb_storage_api::{ServiceAuditAppendRepository, ServiceAuditGroupAppend};
    for ordinal in 1..=count {
        let ServiceAuditGroupAppend::Submitted(fence) = ports
            .submit_service_audit_group(&[source_audit_intent(ordinal)])
            .unwrap()
        else {
            panic!("the standard native writer must acknowledge through its journal");
        };
        assert_eq!(fence.wait().unwrap().len(), 1);
    }
}

// req: OUT-001, REC-001, END-008
#[cfg(feature = "test-fixtures")]
#[test]
fn outbox_admission_checkpoints_large_suffix_without_blocking_audited_writers() {
    use crate::hooks::RedbCheckpointTestPoint as Point;
    use riffdb_storage_api::{ServiceAuditAppendRepository, ServiceAuditGroupAppend};
    let (controller, schedule) = crate::RedbTestController::checkpoint_schedule();
    let (_scope, mut ports, _) =
        crate::changelog_v3_control_tests::fixture_with_controller(controller);
    append_acknowledged_audits(&mut ports, 300);
    schedule.arm(Point::BeforeMaterialization);
    let outbox = RedbOperationalPorts {
        shared: Arc::clone(&ports.shared),
    };
    let writer = std::thread::spawn(move || {
        let access = outbox
            .begin_attributed_write(riffdb_storage_api::ChangelogAttributionV3::OutboxTransition)
            .unwrap();
        access.abort().unwrap();
    });
    schedule.wait(Point::BeforeMaterialization);
    // Acknowledging another audit while the checkpoint is explicitly paused
    // proves neither its receiver nor the writer gate is held by outbox admission.
    let ServiceAuditGroupAppend::Submitted(fence) = ports
        .submit_service_audit_group(&[source_audit_intent(301)])
        .unwrap()
    else {
        panic!("the audit retains its original durable fence");
    };
    assert_eq!(fence.wait().unwrap().len(), 1);
    assert_eq!(ports.shared.journal_suffix_activity().unwrap().1, 1);
    schedule.release(Point::BeforeMaterialization);
    writer.join().unwrap();
    assert_eq!(
        ports
            .begin_composite_read()
            .unwrap()
            .read_range_to(JournalTable::Audit, &[], None, 400)
            .unwrap()
            .len(),
        301
    );
}

// req: OUT-001, REC-001, END-008
#[cfg(feature = "test-fixtures")]
#[test]
fn outbox_admission_waits_for_spare_preparation_outside_the_writer_gate() {
    use crate::hooks::RedbCheckpointTestPoint as Point;
    use riffdb_storage_api::{ServiceAuditAppendRepository, ServiceAuditGroupAppend};
    let (controller, schedule) = crate::RedbTestController::checkpoint_schedule();
    let (_scope, mut ports, states) =
        crate::changelog_v3_control_tests::fixture_with_controller(controller);
    append_acknowledged_audits(&mut ports, 300);
    schedule.arm(Point::BeforeSparePreparation);
    ports
        .shared
        .start_journal_sealer(JournalFileHeader::new(
            states[0].lineage().database_id(),
            None,
            [0; 32],
        ))
        .unwrap();
    schedule.wait(Point::BeforeSparePreparation);
    schedule.arm(Point::BeforeOutboxCheckpointPreparation);
    let outbox = RedbOperationalPorts {
        shared: Arc::clone(&ports.shared),
    };
    let writer = std::thread::spawn(move || {
        outbox
            .begin_attributed_write(riffdb_storage_api::ChangelogAttributionV3::OutboxTransition)
            .and_then(|access| access.abort())
    });
    schedule.wait(Point::BeforeOutboxCheckpointPreparation);
    let (sent, received) = std::sync::mpsc::channel();
    let audit = std::thread::spawn(move || {
        let result = match ports.submit_service_audit_group(&[source_audit_intent(301)]) {
            Ok(ServiceAuditGroupAppend::Submitted(fence)) => fence.wait().map(|rows| rows.len()),
            Ok(_) => panic!("the audit retains its durable completion fence"),
            Err(error) => Err(error),
        };
        sent.send(result).unwrap();
    });
    schedule.release(Point::BeforeOutboxCheckpointPreparation);
    // The timeout is only a deadlock watchdog: the sealer is explicitly held
    // throughout this durable acknowledgement, and released before assertions.
    let progressed = received.recv_timeout(Duration::from_secs(10));
    schedule.release(Point::BeforeSparePreparation);
    audit.join().unwrap();
    let outbox_result = writer.join().unwrap();
    assert_eq!(progressed.unwrap().unwrap(), 1);
    outbox_result.unwrap();
}

#[cfg(feature = "test-fixtures")]
#[test]
fn source_validation_uses_durable_seal_publication_before_spare_preparation() {
    use crate::hooks::RedbCheckpointTestPoint as Point;
    let (controller, schedule) = crate::RedbTestController::checkpoint_schedule();
    let (_scope, mut ports, _) =
        crate::changelog_v3_control_tests::fixture_with_controller(controller);
    append_acknowledged_audits(&mut ports, 1);
    ports.shared.join_journal_sealer();
    let audit_before = ports
        .begin_composite_read()
        .unwrap()
        .read_range_to(JournalTable::Audit, &[], None, 2)
        .unwrap();
    schedule.arm(Point::BeforeSparePreparation);
    schedule.request_checkpoint();
    let ordinary = ports.begin_deferred_service_audit_write().unwrap();
    ordinary.abort().unwrap();
    let writer = ports.shared.mutation_gate.acquire().unwrap();
    ports.shared.poll_async_checkpoint_locked(true).unwrap();
    drop(writer);
    schedule.wait(Point::BeforeSparePreparation);
    // Release and join before assertions: a failed assertion must not strand
    // the store's destructor behind this intentionally paused native worker.
    let epoch = ports.shared.durable_commit_epoch();
    let pending = ports.shared.source_checkpoint_publication_pending();
    let mut control = ports.replication_source_control();
    let capture = control.reclaim_history_online();
    let validation_pending = control.receipt_validation_pending();
    let epoch_after = ports.shared.durable_commit_epoch();
    let audit_after = ports
        .begin_composite_read()
        .and_then(|read| read.read_range_to(JournalTable::Audit, &[], None, 2));
    schedule.release(Point::BeforeSparePreparation);
    ports.shared.join_journal_sealer();
    assert!(
        !pending.unwrap(),
        "future spare preparation is not extent publication"
    );
    assert_eq!(capture.unwrap().0, 0);
    assert!(
        validation_pending,
        "durably published roots must start their real validation"
    );
    assert_eq!(
        epoch_after, epoch,
        "capturing validation cannot advance authority"
    );
    assert_eq!(audit_after.unwrap(), audit_before);
    assert!(!ports.shared.mutation_gate.is_held());
}

#[cfg(feature = "test-fixtures")]
#[test]
fn source_seal_publication_is_bound_to_each_serialized_job() {
    use crate::hooks::RedbCheckpointTestPoint as Point;
    let (controller, schedule) = crate::RedbTestController::checkpoint_schedule();
    let (_scope, ports, states) =
        crate::changelog_v3_control_tests::fixture_with_controller(controller);
    let header = JournalFileHeader::new(states[0].lineage().database_id(), None, [0; 32]);
    schedule.arm(Point::BeforeSealPublication);
    schedule.arm(Point::BeforeSparePreparation);
    ports.shared.start_journal_sealer(header.clone()).unwrap();
    schedule.wait(Point::BeforeSealPublication);
    let first_unpublished = ports.shared.source_checkpoint_publication_pending();
    schedule.release(Point::BeforeSealPublication);
    schedule.wait(Point::BeforeSparePreparation);
    let first_published = ports.shared.source_checkpoint_publication_pending();
    schedule.arm(Point::BeforeSealPublication);
    ports.shared.start_journal_sealer(header).unwrap();
    let waiting_for_previous = ports.shared.source_checkpoint_publication_pending();
    schedule.release(Point::BeforeSparePreparation);
    schedule.wait(Point::BeforeSealPublication);
    let second_unpublished = ports.shared.source_checkpoint_publication_pending();
    schedule.arm(Point::BeforeSparePreparation);
    schedule.release(Point::BeforeSealPublication);
    schedule.wait(Point::BeforeSparePreparation);
    let second_published = ports.shared.source_checkpoint_publication_pending();
    schedule.release(Point::BeforeSparePreparation);
    ports.shared.join_journal_sealer();
    assert!(first_unpublished.unwrap());
    assert!(!first_published.unwrap());
    assert!(waiting_for_previous.unwrap());
    assert!(
        second_unpublished.unwrap(),
        "an old job cannot publish its successor"
    );
    assert!(!second_published.unwrap());
}

#[cfg(feature = "test-fixtures")]
#[test]
fn failed_native_sealing_does_not_report_successful_publication() {
    use crate::hooks::RedbCheckpointTestPoint as Point;
    let (controller, schedule) = crate::RedbTestController::checkpoint_schedule();
    let (_scope, ports, states) =
        crate::changelog_v3_control_tests::fixture_with_controller(controller);
    let sealing = crate::journal::sealing_journal_path(&ports.shared.path);
    std::fs::write(&sealing, b"invalid-sealing-header").unwrap();
    schedule.arm(Point::BeforeSparePreparation);
    ports
        .shared
        .start_journal_sealer(JournalFileHeader::new(
            states[0].lineage().database_id(),
            None,
            [0; 32],
        ))
        .unwrap();
    schedule.wait(Point::BeforeSparePreparation);
    let pending = ports.shared.source_checkpoint_publication_pending();
    let mut control = ports.replication_source_control();
    let capture = control.reclaim_history_online();
    let validation_pending = control.receipt_validation_pending();
    let unchanged = std::fs::read(&sealing);
    schedule.release(Point::BeforeSparePreparation);
    ports.shared.join_journal_sealer();
    assert!(pending.unwrap());
    assert_eq!(capture.unwrap().0, 0);
    assert!(!validation_pending);
    assert_eq!(unchanged.unwrap(), b"invalid-sealing-header");
}

// req: STO-030, STO-031, STO-032, REC-001, END-007, END-008
#[test]
fn oversized_acknowledged_source_suffix_progresses_through_an_ordinary_checkpoint() {
    let (_scope, mut ports, _) = crate::changelog_v3_control_tests::fixture();
    append_acknowledged_audits(&mut ports, 257);
    assert_eq!(ports.shared.journal_suffix_activity().unwrap().1, 257);
    let audit_before = ports
        .begin_composite_read()
        .unwrap()
        .read_range_to(JournalTable::Audit, &[], None, 300)
        .unwrap();
    assert_eq!(audit_before.len(), 257);
    let publication_before = ports.shared.capture_checkpoint_root().unwrap().identity();
    {
        let lease = ports
            .shared
            .mutation_gate
            .acquire_source()
            .unwrap()
            .unwrap();
        assert!(matches!(
            ports.shared.prepare_online_source_root().unwrap(),
            Readiness::PendingJournal
        ));
        assert_eq!(
            ports.shared.capture_checkpoint_root().unwrap().identity(),
            publication_before
        );
        assert_eq!(ports.shared.journal_suffix_activity().unwrap().1, 257);
        assert!(!ports.shared.async_checkpoint_in_flight().unwrap());
        drop(lease);
    }
    // An ordinary subsequent writer performs existing checkpoint preparation;
    // the refused source turn neither drains nor waits under its lease.
    let ordinary = ports.begin_deferred_service_audit_write().unwrap();
    assert!(
        ports.shared.async_checkpoint_in_flight().unwrap(),
        "a known source backlog must not wait for the much larger capacity trigger"
    );
    ordinary.abort().unwrap();
    {
        let writer = ports.shared.mutation_gate.acquire().unwrap();
        ports.shared.poll_async_checkpoint_locked(true).unwrap();
        drop(writer);
    }
    ports.shared.join_journal_sealer();
    assert_eq!(ports.shared.journal_suffix_activity().unwrap().1, 0);
    assert!(ports.shared.capture_checkpoint_root().unwrap().identity() > publication_before);
    assert_eq!(
        ports
            .begin_composite_read()
            .unwrap()
            .read_range_to(JournalTable::Audit, &[], None, 300)
            .unwrap(),
        audit_before
    );
    let lease = ports
        .shared
        .mutation_gate
        .acquire_source()
        .unwrap()
        .unwrap();
    assert!(matches!(
        ports.shared.prepare_online_source_root().unwrap(),
        Readiness::Ready
    ));
    drop(lease);
}

// req: STO-030, STO-031, STO-032, REC-001, END-007, END-008
#[test]
fn published_source_suffix_does_not_force_a_checkpoint_when_logical_retention_is_at_target() {
    let (_scope, mut ports, _) = crate::changelog_v3_control_tests::fixture();
    append_acknowledged_audits(&mut ports, 257);
    let lane = ports.online_retention_lane();
    let (step, phases) = lane
        .step_timed(NonZeroU64::MIN, NonZeroU64::MIN, false)
        .unwrap();
    assert_eq!(
        step,
        crate::OnlineRetentionStep::Held(crate::OnlineRetentionHold::TargetReached)
    );
    assert_eq!(phases.source_admission_pending_journal, 0);
    assert_eq!(phases.source_admission_checkpoint, 0);
    assert!(
        !lane.source_checkpoint_publication_pending().unwrap(),
        "a suggestion is not a started or completed checkpoint"
    );
    assert_eq!(ports.shared.journal_suffix_activity().unwrap().1, 257);
    let ordinary = ports.begin_deferred_service_audit_write().unwrap();
    assert!(!lane.source_checkpoint_publication_pending().unwrap());
    ordinary.abort().unwrap();
    assert_eq!(ports.shared.journal_suffix_activity().unwrap().1, 257);
    assert!(!ports.shared.async_checkpoint_in_flight().unwrap());
}

// req: STO-030, STO-031, STO-032, END-007, END-008
#[test]
fn source_drain_observations_distinguish_unpublished_unknown_and_oversized_work() {
    let (_scope, mut ports, _) = crate::changelog_v3_control_tests::fixture();
    append_acknowledged_audits(&mut ports, 257);
    let lane = ports.online_retention_lane();
    let epoch = ports.shared.durable_commit_epoch();
    let bytes = ports
        .shared
        .journal_runtime
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .suffix_bytes;
    for reason in 0..3 {
        {
            let mut guard = ports.shared.journal_runtime.lock().unwrap();
            let runtime = guard.as_mut().unwrap();
            runtime.unpublished_transitions = usize::from(reason == 0);
            runtime.unpublished_bytes = usize::from(reason == 0);
            runtime.suffix_bytes = bytes + usize::from(reason == 1);
        }
        ports
            .shared
            .mutation_gate
            .clear_source_checkpoint_request()
            .unwrap();
        let lease = ports
            .shared
            .mutation_gate
            .acquire_source()
            .unwrap()
            .unwrap();
        assert!(matches!(
            ports.shared.prepare_online_source_root().unwrap(),
            Readiness::PendingJournal
        ));
        drop(lease);
        let observed = lane.source_drain_observation().unwrap();
        assert_eq!(
            &observed[..3],
            &std::array::from_fn::<_, 3, _>(|index| u64::from(index <= reason))
        );
        assert_eq!(observed[3], 257);
        assert_eq!(
            observed[4],
            u64::try_from(bytes + usize::from(reason == 1)).unwrap()
        );
        assert_eq!(observed[5], u64::from(reason == 0));
        assert_eq!(observed[6], u64::from(reason == 0));
        assert_eq!(
            ports
                .shared
                .mutation_gate
                .source_checkpoint_requested()
                .unwrap(),
            reason == 2
        );
        assert_eq!(
            ports
                .shared
                .mutation_gate
                .source_publication_requested()
                .unwrap(),
            reason == 2
        );
        assert_eq!(ports.shared.durable_commit_epoch(), epoch);
    }
    let mut guard = ports.shared.journal_runtime.lock().unwrap();
    let runtime = guard.as_mut().unwrap();
    runtime.unpublished_transitions = 0;
    runtime.unpublished_bytes = 0;
    runtime.suffix_bytes = bytes;
}

fn runtime(path: &std::path::Path, value_bytes: usize) -> JournalRuntime {
    let database_id =
        DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x82; 10]).unwrap();
    let sequence = CommitSequence::new(1).unwrap();
    let encoded = JournalFrame::new(
        database_id,
        sequence,
        sequence,
        1,
        [0; 32],
        vec![
            JournalMutation::put(
                JournalTable::Commits,
                crate::keys::encode_application_sequence_key(sequence),
                [0x41],
            )
            .unwrap(),
            JournalMutation::put(JournalTable::Entities, [0x10], vec![0x20; value_bytes]).unwrap(),
        ],
    )
    .unwrap()
    .encode()
    .unwrap();
    let lane = Arc::new(
        JournalLane::open(path, &JournalFileHeader::new(database_id, None, [0; 32])).unwrap(),
    );
    let length = encoded.as_bytes().len();
    let physical = crate::journal::extent_frame_bytes(length).unwrap();
    let frame = ValidatedCheckpointFrame {
        source_reclamation: None,
        changelog_binding: None,
        database_id,
        predecessor_sequence: None,
        covered_sequence: Some(sequence),
        predecessor_administration_sequence: None,
        covered_administration_sequence: None,
        previous_hash: [0; 32],
        frame_hash: encoded.frame_hash(),
        transition_count: 1,
        command_count: 1,
        audit_count: 0,
        mutations: Arc::from(Vec::new()),
        extent_generation: lane.generation(),
        placement: crate::journal::frame_placement_after(0, &encoded).unwrap(),
        segment_summaries: Arc::from(Vec::new()),
        encoded,
    };
    JournalRuntime {
        changelog_history: None,
        lane,
        database_id,
        last_sequence: Some(sequence),
        last_administration_sequence: None,
        last_hash: frame.frame_hash,
        published_sequence: Some(sequence),
        published_administration_sequence: None,
        published_hash: frame.frame_hash,
        suffix_transitions: 1,
        suffix_commands: 1,
        suffix_audits: 0,
        suffix_bytes: length,
        suffix_physical_bytes: physical,
        suffix_frames: vec![frame],
        unpublished_transitions: 0,
        unpublished_commands: 0,
        unpublished_audits: 0,
        unpublished_bytes: 0,
        reanchor_required: false,
    }
}

#[test]
fn source_metadata_refuses_every_unknown_or_unpublished_charge_before_materializing() {
    for changed in 0..17 {
        let scope = crate::test_path::ScopedDirectory::new("source-drain-accounting");
        let mut runtime = runtime(&scope.join("journal.riffextent"), 1);
        assert!(suffix_is_bounded(&runtime));
        match changed {
            0 => runtime.suffix_transitions = 257,
            1 => runtime.suffix_bytes = crate::journal::SOURCE_SUFFIX_BYTES + 1,
            2 => runtime.suffix_bytes += 1,
            3 => runtime.suffix_transitions = 2,
            4 => runtime.suffix_commands = 2,
            5 => runtime.suffix_audits = 1,
            6 => runtime.unpublished_transitions = 1,
            7 => runtime.unpublished_commands = 1,
            8 => runtime.unpublished_audits = 1,
            9 => runtime.unpublished_bytes = 1,
            10 => runtime.last_sequence = CommitSequence::new(2),
            11 => runtime.last_administration_sequence = AdministrationSequence::new(1),
            12 => runtime.last_hash = [0; 32],
            13 => runtime.suffix_frames[0].placement.padded_len += 4096,
            14 => runtime.suffix_frames = vec![runtime.suffix_frames[0].clone(); 257],
            15 => runtime.suffix_frames[0].transition_count = 0,
            16 => runtime.suffix_frames.clear(),
            _ => unreachable!(),
        }
        assert!(!suffix_is_bounded(&runtime), "unknown charge {changed}");
        assert!(
            published_suffix_counts(&runtime).is_none(),
            "unknown accounting cannot request ordinary checkpoint work: {changed}"
        );
        // The accounting check has neither submitted nor taken a frame.
        assert_eq!(
            crate::journal::scan_journal(
                &scope.join("journal.riffextent"),
                runtime.database_id,
                |_| Ok(())
            )
            .unwrap()
            .unwrap()
            .1
            .transition_count,
            0
        );
    }
}

#[test]
fn source_metadata_accepts_exact_closed_transition_and_encoded_byte_limits() {
    let scope = crate::test_path::ScopedDirectory::new("source-drain-boundary");
    let mut first = runtime(&scope.join("transitions.riffextent"), 1);
    first.suffix_frames = vec![first.suffix_frames[0].clone(); 256];
    first.suffix_transitions = 256;
    first.suffix_commands = 256;
    first.suffix_bytes *= 256;
    assert!(suffix_is_bounded(&first));
    let base = runtime(&scope.join("base.riffextent"), 1);
    let value_bytes = 1 + crate::journal::SOURCE_SUFFIX_BYTES - base.suffix_bytes;
    let exact = runtime(&scope.join("exact.riffextent"), value_bytes);
    assert_eq!(exact.suffix_bytes, crate::journal::SOURCE_SUFFIX_BYTES);
    assert!(suffix_is_bounded(&exact));
    let excess = runtime(&scope.join("excess.riffextent"), value_bytes + 1);
    assert!(!suffix_is_bounded(&excess));
}

// req: STO-030, STO-031, STO-032, REC-001, END-007, END-008
#[test]
fn source_pending_publication_requests_checkpoint_from_the_next_ordinary_writer() {
    use riffdb_storage_api::{ServiceAuditAppendRepository, ServiceAuditGroupAppend};
    for acknowledged in [0, 257] {
        let (_scope, mut ports, _) = crate::changelog_v3_control_tests::fixture();
        append_acknowledged_audits(&mut ports, acknowledged);
        let ServiceAuditGroupAppend::Submitted(fence) = ports
            .submit_service_audit_group(&[source_audit_intent(acknowledged + 1)])
            .unwrap()
        else {
            panic!("the native writer must retain its original fence");
        };
        ports.shared.join_journal_sealer();
        let root_before = ports.shared.capture_checkpoint_root().unwrap().identity();
        let published_before = ports
            .begin_composite_read()
            .unwrap()
            .read_range_to(JournalTable::Audit, &[], None, 300)
            .unwrap();
        assert_eq!(
            published_before.len(),
            usize::try_from(acknowledged).unwrap()
        );
        let lease = ports
            .shared
            .mutation_gate
            .acquire_source()
            .unwrap()
            .unwrap();
        let result = ports.shared.prepare_online_source_root();
        drop(lease);
        assert!(matches!(result.unwrap(), Readiness::PendingJournal));
        assert_eq!(
            ports.shared.capture_checkpoint_root().unwrap().identity(),
            root_before
        );
        assert_eq!(
            ports
                .shared
                .journal_runtime
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .unpublished_transitions,
            1
        );
        assert_eq!(
            ports
                .begin_composite_read()
                .unwrap()
                .read_range_to(JournalTable::Audit, &[], None, 300)
                .unwrap(),
            published_before
        );
        // Do not complete the original fence before the ordinary writer begins.
        // This deterministic overlap reproduces a pipeline that never becomes empty.
        let ordinary = ports.begin_deferred_service_audit_write().unwrap();
        let checkpoint_started = ports.shared.async_checkpoint_in_flight().unwrap();
        ordinary.abort().unwrap();
        let ServiceAuditGroupAppend::Submitted(second) = ports
            .submit_service_audit_group(&[source_audit_intent(acknowledged + 2)])
            .unwrap()
        else {
            panic!("the second caller also retains its original fence");
        };
        assert_eq!(
            ports
                .shared
                .journal_runtime
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .unpublished_transitions,
            0,
            "an active source handoff publishes the ordinary writer before releasing its lease"
        );
        assert_eq!(second.wait().unwrap().len(), 1);
        let original = fence.wait().unwrap();
        assert_eq!(
            original.len(),
            1,
            "the original completion owner keeps its result"
        );
        assert!(
            checkpoint_started,
            "a source hint must reach the ordinary checkpoint despite an unpublished predecessor"
        );
        let writer = ports.shared.mutation_gate.acquire().unwrap();
        ports.shared.poll_async_checkpoint_locked(true).unwrap();
        drop(writer);
        ports.shared.join_journal_sealer();
        let audits = ports
            .begin_composite_read()
            .unwrap()
            .read_range_to(JournalTable::Audit, &[], None, 300)
            .unwrap();
        assert_eq!(audits.len(), usize::try_from(acknowledged + 2).unwrap());
        assert_eq!(
            &audits[..published_before.len()],
            published_before.as_slice()
        );
        let lease = ports
            .shared
            .mutation_gate
            .acquire_source()
            .unwrap()
            .unwrap();
        assert!(matches!(
            ports.shared.prepare_online_source_root().unwrap(),
            Readiness::Ready
        ));
        drop(lease);
        assert!(
            !ports
                .shared
                .mutation_gate
                .source_publication_requested()
                .unwrap()
        );
        let ServiceAuditGroupAppend::Submitted(third) = ports
            .submit_service_audit_group(&[source_audit_intent(acknowledged + 3)])
            .unwrap()
        else {
            panic!("ordinary pipelining resumes after source capture");
        };
        assert_eq!(
            ports
                .shared
                .journal_runtime
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .unpublished_transitions,
            1
        );
        assert_eq!(third.wait().unwrap().len(), 1);
    }
}

// req: STO-030, STO-031, STO-032, REC-001, END-008
#[test]
fn unknown_pending_tail_never_requests_publication_or_checkpoint() {
    for changed in 0..15 {
        let scope = crate::test_path::ScopedDirectory::new("source-pending-accounting");
        let mut runtime = runtime(&scope.join("journal.riffextent"), 1);
        let first = &runtime.suffix_frames[0];
        runtime.published_sequence = first.predecessor_sequence;
        runtime.published_administration_sequence = first.predecessor_administration_sequence;
        runtime.published_hash = first.previous_hash;
        runtime.unpublished_transitions = runtime.suffix_transitions;
        runtime.unpublished_commands = runtime.suffix_commands;
        runtime.unpublished_audits = runtime.suffix_audits;
        runtime.unpublished_bytes = runtime.suffix_bytes;
        assert!(unpublished_suffix_is_accounted(&runtime));
        assert!(published_suffix_counts(&runtime).is_none());
        match changed {
            0 => runtime.unpublished_transitions += 1,
            1 => runtime.unpublished_commands += 1,
            2 => runtime.unpublished_audits += 1,
            3 => runtime.unpublished_bytes += 1,
            4 => runtime.published_sequence = CommitSequence::new(99),
            5 => runtime.published_hash = [0x67; 32],
            6 => runtime.suffix_transitions += 1,
            7 => runtime.suffix_bytes += 1,
            8 => runtime.suffix_frames[0].frame_hash = [0x67; 32],
            9 => runtime.suffix_frames[0].placement.padded_len += 4096,
            10 => runtime.suffix_frames.clear(),
            11 => runtime.last_hash = [0x67; 32],
            12 => runtime.last_sequence = CommitSequence::new(99),
            13 => runtime.suffix_frames[0].transition_count = 0,
            14 => runtime.suffix_bytes = crate::journal::MAX_JOURNAL_SUFFIX_BYTES + 1,
            _ => unreachable!(),
        }
        assert!(
            !unpublished_suffix_is_accounted(&runtime),
            "unknown tail {changed}"
        );
    }
}

#[path = "source_reclamation_license_tests.rs"]
mod license_tests;
