// req: OQ-040, STO-032, REC-001, REC-004, END-008
#[cfg(feature = "test-fixtures")]
#[test]
fn async_checkpoint_native_refusal_keeps_completed_io_and_descriptor_for_retry() {
    const CHILD: &str = "RIFFDB_CHECKPOINT_REBASE_PRESSURE_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "store::tests::async_checkpoint_native_refusal_keeps_completed_io_and_descriptor_for_retry", "--test-threads=1"])
            .env(CHILD,"1").status().unwrap();
        assert!(
            status.success(),
            "isolated checkpoint capacity regression failed"
        );
        return;
    }
    use crate::hooks::RedbCheckpointTestPoint as Point;
    use riffdb_storage_api::{
        AuditPrincipalV1, ServiceAuditAppendIntentV1, ServiceAuditAppendRepository,
        ServiceAuditGroupAppend,
    };
    use riffdb_types::{
        ActorId, ActorKind, CapabilityId, RequestId, ServiceAuditLinkV1, ServiceAuditPhaseV1,
        ServiceAuditTargetsV1, ServiceIngressKindV1, ServiceOperationV1, Timestamp,
    };
    let intent = |ordinal: u64| {
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
    };
    let (controller, schedule) = crate::RedbTestController::checkpoint_schedule();
    let (_scope, mut ports, _) =
        crate::changelog_v3_control_tests::fixture_with_controller(controller);
    let ServiceAuditGroupAppend::Submitted(first) =
        ports.submit_service_audit_group(&[intent(1)]).unwrap()
    else {
        panic!("original audit must use its durable fence");
    };
    assert_eq!(first.wait().unwrap().len(), 1);
    schedule.arm(Point::BeforeCheckpointCommit);
    schedule.arm(Point::Materialized);
    {
        let lease = ports.shared.mutation_gate.acquire().unwrap();
        ports
            .shared
            .start_async_checkpoint_for_direct_write()
            .unwrap();
        drop(lease);
    }
    schedule.wait(Point::BeforeCheckpointCommit);
    let ServiceAuditGroupAppend::Submitted(second) =
        ports.submit_service_audit_group(&[intent(2)]).unwrap()
    else {
        panic!("newer audit must use its durable fence");
    };
    assert_eq!(second.wait().unwrap().len(), 1);
    let before = ports.shared.capture_or_initialize_composite_view().unwrap();
    let before_access = RedbReadAccess::Composite(Arc::clone(&before));
    let original_rows = before_access
        .read_range_to(crate::journal::JournalTable::Audit, &[], None, 64)
        .unwrap();
    let original_history = crate::changelog_v3_cursor::history(&before_access).unwrap();
    schedule.release(Point::BeforeCheckpointCommit);
    schedule.wait(Point::Materialized);
    schedule.release(Point::Materialized);
    let descriptor_bytes = {
        let mut checkpoint = ports.shared.journal_checkpoint.lock().unwrap();
        let checkpoint = checkpoint.as_mut().unwrap();
        let completion = checkpoint.completion.take().unwrap();
        let result = completion.poll(true).unwrap();
        assert!(
            result.is_ok(),
            "the actual checkpoint I/O must have committed"
        );
        checkpoint.result = Some(result);
        checkpoint.locator_descriptor.as_ref().unwrap().bytes()
    };
    let checkpoint_path = crate::journal::checkpoint_journal_path(&ports.shared.path);
    assert!(checkpoint_path.exists());
    let pool = crate::locator_memory::NativeLocatorPool::global();
    for _ in 0..4096 {
        if !pool.drain_retired_step() {
            break;
        }
    }
    let pressure = pool
        .admit(
            crate::locator_memory::LOCATOR_TOTAL_BYTES - pool.charged_bytes(),
            crate::locator_memory::LocatorAllocationClass::Maintenance,
        )
        .unwrap();
    let lease = ports.shared.mutation_gate.acquire().unwrap();
    assert_eq!(
        ports
            .shared
            .finish_async_checkpoint_if_quiescent()
            .unwrap_err()
            .kind(),
        StorageErrorKind::Unavailable
    );
    {
        let checkpoint = ports.shared.journal_checkpoint.lock().unwrap();
        let checkpoint = checkpoint
            .as_ref()
            .expect("completed checkpoint remains retryable");
        assert!(
            matches!(checkpoint.result, Some(Ok(()))),
            "capacity refusal cannot consume successful checkpoint I/O"
        );
        assert_eq!(
            checkpoint
                .locator_descriptor
                .as_ref()
                .expect("original descriptor remains admitted")
                .bytes(),
            descriptor_bytes
        );
    }
    assert!(!ports.shared.write_fenced.load(Ordering::Acquire));
    assert!(Arc::ptr_eq(
        &before,
        &ports.shared.capture_or_initialize_composite_view().unwrap()
    ));
    assert!(
        checkpoint_path.exists(),
        "unpublished checkpoint extent must not be reclaimed"
    );
    drop(pressure);
    for _ in 0..4096 {
        if !pool.drain_retired_step() {
            break;
        }
    }
    ports.shared.finish_async_checkpoint_if_quiescent().unwrap();
    assert!(ports.shared.journal_checkpoint.lock().unwrap().is_none());
    let after = ports.shared.capture_or_initialize_composite_view().unwrap();
    assert!(!Arc::ptr_eq(&before, &after));
    let after_access = RedbReadAccess::Composite(Arc::clone(&after));
    assert_eq!(
        crate::changelog_v3_cursor::history(&after_access).unwrap(),
        original_history
    );
    assert_eq!(
        after_access
            .read_range_to(crate::journal::JournalTable::Audit, &[], None, 64)
            .unwrap(),
        original_rows
    );
    assert_eq!(
        before_access
            .read_range_to(crate::journal::JournalTable::Audit, &[], None, 64)
            .unwrap(),
        original_rows
    );
    ports.shared.finish_async_checkpoint_if_quiescent().unwrap();
    assert!(
        Arc::ptr_eq(
            &after,
            &ports.shared.capture_or_initialize_composite_view().unwrap()
        ),
        "retry must publish the completion only once"
    );
    drop(lease);
    ports.shared.join_journal_sealer();
    assert!(!ports.shared.write_fenced.load(Ordering::Acquire));
}
