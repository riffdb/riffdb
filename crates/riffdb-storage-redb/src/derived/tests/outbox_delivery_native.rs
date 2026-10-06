// req: EFF-002, EFF-003, REC-001, END-008
//! Real dispatcher, archive and process-crash evidence for ADR-0273.
use super::*;
use riffdb_outbox::{
    DeliveryPolicy, DurableLocalArchive, DurableLocalConnector, NoOutboxFailpoints,
    NoOutboxTelemetry, OutboxClock, OutboxClockError, OutboxDispatcher, OutboxRecoveryResult,
    RecoveringOutbox,
};
use riffdb_storage_api::{LocalOutboxArchiveRecordV1, OutboxSucceedGroupV1};
use std::num::NonZeroU64;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::process::ExitStatusExt;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const CHILD_MODE: &str = "RIFFDB_OUTBOX_GROUP_CRASH_MODE";
const CHILD_DATABASE: &str = "RIFFDB_OUTBOX_GROUP_CRASH_DATABASE";
const CHILD_ARCHIVE: &str = "RIFFDB_OUTBOX_GROUP_CRASH_ARCHIVE";

struct Clock(i64);
impl OutboxClock for Clock {
    fn now(&mut self) -> Result<Timestamp, OutboxClockError> {
        Ok(timestamp(self.0))
    }
}

fn policy() -> DeliveryPolicy {
    DeliveryPolicy::new(
        destination(),
        NonZeroU32::new(5).unwrap(),
        NonZeroU32::new(10).unwrap(),
        NonZeroU32::new(3).unwrap(),
        vec![NonZeroU32::MIN; 2],
        OutboxPageLimit::new(NonZeroU16::new(16).unwrap()).unwrap(),
    )
    .unwrap()
}

fn open_ports(
    path: &std::path::Path,
    controller: Option<crate::RedbTestController>,
) -> RedbOperationalPorts {
    let store = match controller {
        Some(controller) => RedbStore::open_with_test_controller(path, controller),
        None => RedbStore::open(path),
    }
    .unwrap();
    crate::store::RedbDormantPorts {
        pending_v3_activation: None,
        shared: store.shared,
    }
    .into_operational_after_catalog_validation()
    .unwrap()
}

fn archive(path: &std::path::Path) -> DurableLocalArchive {
    let mut archive = DurableLocalArchive::open(path, database_id(), NonZeroU64::MAX).unwrap();
    assert!(archive.validate_next_page().unwrap());
    archive
}

fn recovered(
    ports: RedbOperationalPorts,
    now: i64,
) -> riffdb_outbox::RecoveredOutbox<RedbOperationalPorts> {
    match RecoveringOutbox::after_authoritative_readiness(ports).recover(
        &policy(),
        &mut Clock(now),
        &mut NoOutboxFailpoints,
        &mut NoOutboxTelemetry,
    ) {
        OutboxRecoveryResult::Ready { recovered, .. } => recovered,
        OutboxRecoveryResult::Degraded { error, .. } => panic!("native recovery refused: {error}"),
    }
}

#[test]
fn native_dispatch_keeps_sixteen_claims_and_archive_commits_with_one_completion_commit() {
    let (path, ports) = operational("outbox-real-group-dispatch");
    let events = seed_command(&ports, CommitSequence::first(), 16);
    let destination = path.1.join("archive");
    std::fs::create_dir(&destination).unwrap();
    std::fs::set_permissions(&destination, std::fs::Permissions::from_mode(0o700)).unwrap();
    let archive = archive(&destination);
    let archive_census = archive.native_census_v1();
    let mut dispatcher = OutboxDispatcher::new(
        recovered(ports, 10),
        DurableLocalConnector::new(archive, NonZeroU64::MIN),
        Clock(10),
        NoOutboxFailpoints,
        NoOutboxTelemetry,
        policy(),
    );
    let before = dispatcher
        .repository()
        .shared
        .outbox_stage_census
        .snapshot()[7][0];
    let report = dispatcher.dispatch_once().unwrap();
    assert_eq!(report.claims(), 16);
    assert_eq!(report.delivered(), 16);
    assert_eq!(report.retries_scheduled(), 0);
    assert_eq!(
        dispatcher
            .repository()
            .shared
            .outbox_stage_census
            .snapshot()[7][0]
            - before,
        17
    );
    assert_eq!(archive_census.snapshot()[2][0], 16);
    assert_eq!(
        dispatcher
            .connector()
            .archive()
            .observation()
            .unwrap()
            .records(),
        16
    );
    assert!(!dispatcher.repository().has_undelivered_outbox().unwrap());
    for event in events {
        let record = LocalOutboxArchiveRecordV1::new(database_id(), NonZeroU64::MIN, event);
        assert_eq!(
            dispatcher
                .connector()
                .archive()
                .read(&record.key())
                .unwrap(),
            Some(record)
        );
    }
}

#[test]
fn native_success_group_refuses_uncertain_completion_and_fences_following_writes() {
    let (path, mut ports) = operational("outbox-group-unknown");
    let successes = super::outbox_completion::claimed_successes(&mut ports, 16);
    drop(ports);
    let controller =
        crate::RedbTestController::return_unknown_after_commit(RedbTestOperation::OutboxTransition);
    let mut ports = open_ports(&path.0, Some(controller));
    let group = OutboxSucceedGroupV1::new(successes.clone()).unwrap();
    assert_eq!(
        ports.succeed_outbox_group(&group).unwrap_err().kind(),
        StorageErrorKind::CommitStatusUnknown
    );
    assert_eq!(
        ports.succeed_outbox_group(&group).unwrap_err().kind(),
        StorageErrorKind::Unavailable
    );
    drop(ports);
    let ports = open_ports(&path.0, None);
    for success in successes {
        assert_eq!(
            ports.read_outbox_status(success.event_id()).unwrap(),
            OutboxStatusReadResultV1::Status(OutboxStatusObservationV1::Present(
                success.updated().clone()
            ))
        );
    }
    assert!(!ports.has_undelivered_outbox().unwrap());
}

#[test]
fn outbox_group_crash_child() {
    let Ok(mode) = std::env::var(CHILD_MODE) else {
        return;
    };
    let path = PathBuf::from(std::env::var_os(CHILD_DATABASE).unwrap());
    let destination = PathBuf::from(std::env::var_os(CHILD_ARCHIVE).unwrap());
    let ports = open_ports(&path, None);
    let mut archive = archive(&destination);
    let mut successes = Vec::new();
    for ordinal in 0..16 {
        let event_id = EventId::new(CommitSequence::first(), ordinal);
        let event = command_graph_at(CommitSequence::first(), 16).1[ordinal as usize].clone();
        archive
            .accept(&LocalOutboxArchiveRecordV1::new(
                database_id(),
                NonZeroU64::MIN,
                event,
            ))
            .unwrap();
        let OutboxStatusReadResultV1::Status(OutboxStatusObservationV1::Present(delivering)) =
            ports.read_outbox_status(event_id).unwrap()
        else {
            panic!("claim missing");
        };
        successes.push(OutboxSucceedV1::new(delivering, timestamp(11)).unwrap());
    }
    assert_eq!(archive.observation().unwrap().records(), 16);
    drop(archive);
    drop(ports);
    if mode == "accepted" {
        std::process::abort();
    }
    let controller = match mode.as_str() {
        "before" => {
            crate::RedbTestController::abort_before_commit(RedbTestOperation::OutboxTransition)
        }
        "after" => {
            crate::RedbTestController::abort_after_commit(RedbTestOperation::OutboxTransition)
        }
        _ => panic!("unknown closed crash point"),
    };
    let mut ports = open_ports(&path, Some(controller));
    ports
        .succeed_outbox_group(&OutboxSucceedGroupV1::new(successes).unwrap())
        .unwrap();
    panic!("group crash point did not fire");
}

struct OwnedChild(Child);
impl Drop for OwnedChild {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn outbox_completion_group_crash_redelivers_without_losing_archive_records() {
    for mode in ["accepted", "before", "after"] {
        let (path, mut ports) = operational("outbox-group-process-crash");
        let transitions = super::outbox_completion::claimed_successes(&mut ports, 16);
        drop(ports);
        let destination = path.1.join("archive");
        std::fs::create_dir(&destination).unwrap();
        std::fs::set_permissions(&destination, std::fs::Permissions::from_mode(0o700)).unwrap();
        let mut child = OwnedChild(
            Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "derived::tests::outbox_delivery_native::outbox_group_crash_child",
                    "--nocapture",
                ])
                .env(CHILD_MODE, mode)
                .env(CHILD_DATABASE, &path.0)
                .env(CHILD_ARCHIVE, &destination)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap(),
        );
        let deadline = Instant::now() + Duration::from_secs(30);
        let status = loop {
            if let Some(status) = child.0.try_wait().unwrap() {
                break status;
            }
            assert!(Instant::now() < deadline, "bounded crash child deadline");
            std::thread::yield_now();
        };
        assert_eq!(
            status.signal(),
            Some(6),
            "only the exact native abort boundary may terminate this child"
        );
        let ports = open_ports(&path.0, None);
        for transition in &transitions {
            let expected = if mode == "after" {
                transition.updated()
            } else {
                transition.expected()
            };
            assert_eq!(
                ports.read_outbox_status(transition.event_id()).unwrap(),
                OutboxStatusReadResultV1::Status(OutboxStatusObservationV1::Present(
                    expected.clone()
                ))
            );
        }
        assert_eq!(ports.has_undelivered_outbox().unwrap(), mode != "after");
        let archive = archive(&destination);
        let observation = archive.observation().unwrap();
        let census = archive.native_census_v1();
        assert_eq!(observation.records(), 16);
        let mut dispatcher = OutboxDispatcher::new(
            recovered(ports, 30),
            DurableLocalConnector::new(archive, NonZeroU64::MIN),
            Clock(31),
            NoOutboxFailpoints,
            NoOutboxTelemetry,
            policy(),
        );
        let report = dispatcher.dispatch_once().unwrap();
        assert_eq!(report.delivered(), if mode == "after" { 0 } else { 16 });
        assert!(!dispatcher.repository().has_undelivered_outbox().unwrap());
        assert_eq!(
            census.snapshot()[2][0],
            0,
            "equal archive replay must not issue new record commits"
        );
        assert_eq!(
            dispatcher.connector().archive().observation().unwrap(),
            observation
        );
        for event in command_graph_at(CommitSequence::first(), 16).1 {
            let record = LocalOutboxArchiveRecordV1::new(database_id(), NonZeroU64::MIN, event);
            assert_eq!(
                dispatcher
                    .connector()
                    .archive()
                    .read(&record.key())
                    .unwrap(),
                Some(record)
            );
        }
    }
}
