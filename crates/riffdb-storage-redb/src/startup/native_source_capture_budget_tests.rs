//! Diagnostic for the next complete capture owner, beyond retention-only custody.
//! It must validate/admit the terminal source before the evaluator can use it.
use super::*;
use riffdb_storage_api::{
    AuthoritativeScanReader, CommitScanRequest, OwnedSnapshotReader, StorageScanLimit,
};

// req: PRJ-004, STO-012, STO-020, REC-001
#[test]
fn complete_original_capture_admits_terminal_source_work_before_returning() {
    let (_directory, mut ports) = super::native_source_admission::fixture();
    let before_access = ports.begin_composite_read().unwrap();
    let before_history = crate::changelog_v3_cursor::history(&before_access).unwrap();
    drop(before_access);
    // Reuse the existing acknowledged Standard-profile service-audit owner;
    // outbox transitions are immediate and do not create a composite suffix.
    append_genuine_journaled_audit(&mut ports);
    let after_access = ports.begin_composite_read().unwrap();
    assert!(matches!(
        &after_access,
        crate::store::RedbReadAccess::Composite(_)
    ));
    let after_history = crate::changelog_v3_cursor::history(&after_access).unwrap();
    assert!(after_history.tail().sequence() > before_history.tail().sequence());
    drop(after_access);
    let budget = ports.shared.database.source_working_budget();
    let baseline = budget.observation().unwrap();
    let free = riffdb_types::PROJECTION_SOURCE_WORKING_MEMORY_BYTES
        - baseline.fixed_owner_bytes
        - baseline.owned_bytes
        - baseline.reserved_bytes;
    // This still admits the existing retained-metadata-only constructor. Its
    // complete terminal fold/frame proof has its own variable workspace bound.
    let blocked = budget.reserve(free - (512 * 1024 + 8 * 1024)).unwrap();
    let before = budget.observation().unwrap();
    // The ordinary application path remains independent of Source64 admission.
    let snapshot = ports.open_owned_snapshot().unwrap();
    let page = snapshot
        .scan_commits(CommitScanRequest::initial(
            StorageScanLimit::new(2).unwrap(),
        ))
        .unwrap();
    assert_eq!(
        page.records().len(),
        2,
        "ordinary genuine fixture scan must succeed"
    );
    drop(page);
    drop(snapshot);
    let result = ports.capture_original_prefix_retention();
    assert!(
        matches!(result,Err(error) if error.kind()==StorageErrorKind::Unavailable),
        "complete source capture must admit and verify terminal work before return"
    );
    assert_eq!(budget.observation().unwrap(), before);
    drop(blocked);
    assert_eq!(budget.observation().unwrap(), baseline);
    let captured = ports.capture_original_prefix_retention().unwrap();
    assert_eq!(captured.captured_history(), after_history);
    assert!(captured.belongs_to_ports(&ports));
    drop(captured);
    assert_eq!(budget.observation().unwrap(), baseline);
}

fn append_genuine_journaled_audit(ports: &mut crate::RedbOperationalPorts) {
    append_genuine_journaled_audit_number(ports, 0)
}
fn append_genuine_journaled_audit_number(ports: &mut crate::RedbOperationalPorts, ordinal: u64) {
    use riffdb_storage_api::{
        AuditPrincipalV1, ServiceAuditAppendIntentV1, ServiceAuditAppendRepository,
        ServiceAuditGroupAppend,
    };
    use riffdb_types::{
        ActorId, ActorKind, CapabilityId, RequestId, ServiceAuditLinkV1, ServiceAuditPhaseV1,
        ServiceAuditTargetsV1, ServiceIngressKindV1, ServiceOperationV1, Timestamp,
    };
    let intent = ServiceAuditAppendIntentV1::new(
        RequestId::from_unix_milliseconds_and_random(20 + ordinal, [0x51; 10]).unwrap(),
        Timestamp::new(1_700_000_000, 0).unwrap(),
        ServiceOperationV1::GetHealth,
        ServiceAuditPhaseV1::Denied,
        AuditPrincipalV1::new(
            ActorId::new("source-capture-proof").unwrap(),
            ActorKind::Human,
            CapabilityId::from_unix_milliseconds_and_random(19, [0x52; 10]).unwrap(),
            std::num::NonZeroU64::MIN,
        ),
        ServiceIngressKindV1::Grpc,
        ServiceAuditTargetsV1::empty(),
        None,
        ServiceAuditLinkV1::None,
    )
    .unwrap();
    let ServiceAuditGroupAppend::Submitted(fence) =
        ports.submit_service_audit_group(&[intent]).unwrap()
    else {
        panic!("ordinary journal acknowledgement required");
    };
    assert_eq!(fence.wait().unwrap().len(), 1);
}

// req: PRJ-004, STO-012, STO-020, REC-001
#[test]
fn terminal_stub_uses_only_the_originating_resolver_and_releases_source_work() {
    use riffdb_storage_api::{ReplicationSourceHoldKindV1, ReplicationSourceHoldV1};
    let (directory, mut ports) = super::native_source_admission::fixture();
    append_genuine_journaled_audit(&mut ports);
    let access = ports.begin_composite_read().unwrap();
    let captured = crate::changelog_v3_cursor::history(&access).unwrap();
    drop(access);
    // The existing durable hold owner drains/checkpoints the acknowledged suffix.
    let hold = ReplicationSourceHoldV1::new(
        riffdb_types::ReplicationSourceHoldIdV1::new([0x6b; 16]).unwrap(),
        ReplicationSourceHoldKindV1::Bootstrap,
        captured.lineage(),
        captured.tail(),
    );
    assert!(ports.replication_source_control().register(hold).unwrap());
    let read = ports.shared.database.begin_read().unwrap();
    let table = read
        .open_table(crate::changelog_v3_activation::HISTORY)
        .unwrap();
    let row = table
        .get(captured.tail().sequence().get().to_be_bytes().as_slice())
        .unwrap()
        .unwrap();
    assert!(
        crate::history_stub::HistoryStub::decode(row.value())
            .unwrap()
            .is_some()
    );
    let baseline = ports
        .shared
        .database
        .source_working_budget()
        .observation()
        .unwrap();
    {
        let context = crate::store::OriginalSourceReadContext::admit(&ports).unwrap();
        context.validate_terminal(row.value(), captured).unwrap();
        // A valid same-database resolver is globally registered, but this missing
        // owner's extent must not be substituted by that other physical engine.
        let absent = crate::history_stub::HistoryResolver::register(
            captured.lineage().database_id(),
            &directory.join("missing/db.redb"),
            Arc::new(crate::media::RealJournalMedia),
        );
        let error = absent
            .validate_source_terminal_v1(
                row.value(),
                captured,
                ports.shared.database.source_working_budget(),
            )
            .unwrap_err();
        assert_eq!(error.kind(), StorageErrorKind::CorruptData);
    }
    assert_eq!(
        ports
            .shared
            .database
            .source_working_budget()
            .observation()
            .unwrap(),
        baseline
    );
}

// req: PRJ-004, STO-012, STO-020, REC-001
#[test]
fn terminal_fold_releases_primary_gate_while_same_capture_and_workspace_remain_pinned() {
    let (_directory, mut ports) = super::native_source_admission::fixture();
    append_genuine_journaled_audit(&mut ports);
    let access = ports.begin_composite_read().unwrap();
    assert!(matches!(
        &access,
        crate::store::RedbReadAccess::Composite(_)
    ));
    let expected = crate::changelog_v3_cursor::history(&access).unwrap();
    drop(access);
    let before = ports
        .shared
        .database
        .source_working_budget()
        .observation()
        .unwrap();
    let capture_ports = crate::RedbOperationalPorts {
        shared: Arc::clone(&ports.shared),
    };
    let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(0);
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
    let capture = std::thread::spawn(move || {
        assert!(crate::store::install_original_fold_pause_for_test(
            entered_tx, release_rx
        ));
        capture_ports.capture_original_prefix_retention()
    });
    entered_rx
        .recv_timeout(std::time::Duration::from_secs(30))
        .unwrap();
    // Workspace and the captured source are live, but the primary gate is free.
    // try_acquire is deterministic and refuses immediately on the old owner.
    let free = ports
        .original_proof_writer_gate_available_for_test()
        .unwrap();
    if !free {
        release_tx.send(()).unwrap();
        drop(capture.join());
        panic!("terminal fold must release the primary gate before verification");
    }
    append_genuine_journaled_audit_number(&mut ports, 1);
    let current_access = ports.begin_composite_read().unwrap();
    let current = crate::changelog_v3_cursor::history(&current_access).unwrap();
    assert!(current.tail().sequence() > expected.tail().sequence());
    drop(current_access);
    release_tx.send(()).unwrap();
    let captured = capture.join().unwrap().unwrap();
    assert_eq!(captured.captured_history(), expected);
    assert!(captured.belongs_to_ports(&ports));
    drop(captured);
    assert_eq!(
        ports
            .shared
            .database
            .source_working_budget()
            .observation()
            .unwrap(),
        before
    );
}
