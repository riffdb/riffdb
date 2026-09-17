// Atomic lifecycle writes through the closed transaction port.
// req: REP-006, REC-001, STO-012
use super::*;
use riffdb_storage_api::{
    ChangelogLineageV3, FollowerHoldBudget, LeadershipEpochV1,
    ReplicationAdministrationAwaitingDecision, ReplicationAdministrationCandidateTransaction,
    ReplicationAdministrationCandidateV1, ReplicationAdministrationIntentV1,
    ReplicationAdministrationRequestV1 as Request, ReplicationAdministrationResultV1 as ResultV1,
    ReplicationAdministrationTransactionPort, ReplicationSourceHoldIdV1,
};
use riffdb_types::ReplicationFollowerAuditTargetV1;

fn fixture() -> (
    TestPath,
    RedbOperationalPorts,
    ReplicationFollowerAuditTargetV1,
) {
    let (path, mut ports) = initialized_ports("follower-lifecycle-writes");
    ports
        .bootstrap_capability(&bootstrap_intent(20, capability_id(3), digest(1), 10))
        .unwrap();
    let mut write = ports.shared.database.begin_write().unwrap();
    let lineage = ChangelogLineageV3::new(database_id(), 1, LeadershipEpochV1::initial()).unwrap();
    crate::changelog_v3_activation::stage_validated(
        &mut write,
        lineage,
        riffdb_types::DualFrontier::new(None, AdministrationSequence::new(2)),
    )
    .unwrap();
    ports.shared.commit_durable(write).unwrap();
    let target = ReplicationFollowerAuditTargetV1::new(
        database_id(),
        1,
        LeadershipEpochV1::initial(),
        ReplicationSourceHoldIdV1::new([0x71; 16]).unwrap(),
    )
    .unwrap();
    (path, ports, target)
}

fn execute(ports: &RedbOperationalPorts, request: Request) -> ResultV1 {
    let candidate = ReplicationAdministrationCandidateV1::new(request, principal(capability_id(3)));
    let transaction = ports
        .begin_replication_administration(candidate.clone())
        .unwrap();
    let (awaiting, current) = transaction.read_transaction_current().unwrap();
    assert_eq!(current.unwrap().capability_id(), capability_id(3));
    awaiting
        .commit(ReplicationAdministrationIntentV1::new(
            candidate,
            Timestamp::new(15, 0).unwrap(),
        ))
        .unwrap()
}

#[test]
fn registration_and_retirement_are_atomic_and_replay_the_original_receipt() {
    let (_scope, ports, target) = fixture();
    let register = |request| {
        Request::register(
            request_id(request),
            target,
            FollowerHoldBudget::new(3).unwrap(),
            None,
        )
    };
    let ResultV1::Applied(original) = execute(&ports, register(30)) else {
        panic!("new registration");
    };
    assert_eq!(
        original.administration_sequence(),
        AdministrationSequence::new(3).unwrap()
    );
    assert_eq!(
        original.after().phase(),
        riffdb_storage_api::FollowerRegistrationPhaseV1::AwaitingBootstrap
    );
    let epoch = ports.shared.durable_commit_epoch();
    assert_eq!(
        execute(&ports, register(31)),
        ResultV1::Replayed(original.clone())
    );
    assert_eq!(ports.shared.durable_commit_epoch(), epoch);
    let retire = |request| Request::retire(request_id(request), target, original.generation());
    let ResultV1::Applied(released) = execute(&ports, retire(32)) else {
        panic!("retirement");
    };
    assert_eq!(
        released.administration_sequence(),
        AdministrationSequence::new(4).unwrap()
    );
    assert_eq!(
        released.before(),
        Some(riffdb_storage_api::ReplicationSourceHoldStateV1::Registered(original.after()))
    );
    assert_eq!(
        released.after().phase(),
        riffdb_storage_api::FollowerRegistrationPhaseV1::Retired
    );
    let epoch = ports.shared.durable_commit_epoch();
    assert_eq!(execute(&ports, retire(33)), ResultV1::Replayed(released));
    assert_eq!(execute(&ports, register(34)), ResultV1::Replayed(original));
    assert_eq!(ports.shared.durable_commit_epoch(), epoch);
    assert_eq!(audit_count(&ports), 4);
    crate::changelog_v3_roots::validate_retained_history(
        &ports.shared.database.begin_read().unwrap(),
    )
    .unwrap();
    assert!(!ports.bootstrap_id_is_held(target.hold_id()).unwrap());
}

fn history(ports: &RedbOperationalPorts) -> riffdb_storage_api::ChangelogHistoryStateV3 {
    crate::changelog_v3_roots::validate_retained_history(
        &ports.shared.database.begin_read().unwrap(),
    )
    .unwrap()
    .unwrap()
}
fn register(target: ReplicationFollowerAuditTargetV1) -> Request {
    Request::register(
        request_id(30),
        target,
        FollowerHoldBudget::new(3).unwrap(),
        None,
    )
}

#[test]
fn lifecycle_refusals_and_abandonment_preserve_history_and_original_generation() {
    use riffdb_storage_api::{
        ChangelogTransactionSequence, ReplicationAdministrationRefusalV1 as Refused,
    };
    let (_scope, ports, target) = fixture();
    let before = history(&ports);
    let candidate =
        ReplicationAdministrationCandidateV1::new(register(target), principal(capability_id(3)));
    let transaction = ports
        .begin_replication_administration(candidate.clone())
        .unwrap();
    let (awaiting, _) = transaction.read_transaction_current().unwrap();
    awaiting.abandon();
    assert_eq!(history(&ports), before);
    let transaction = ports
        .begin_replication_administration(candidate.clone())
        .unwrap();
    let (awaiting, _) = transaction.read_transaction_current().unwrap();
    let substitute = ReplicationAdministrationCandidateV1::new(
        Request::register(
            request_id(31),
            target,
            FollowerHoldBudget::new(4).unwrap(),
            None,
        ),
        principal(capability_id(3)),
    );
    assert!(
        awaiting
            .commit(ReplicationAdministrationIntentV1::new(
                substitute,
                Timestamp::new(15, 0).unwrap()
            ))
            .is_err()
    );
    assert_eq!(history(&ports), before);
    let missing = Request::retire(request_id(32), target, before.tail().sequence());
    assert_eq!(
        execute(&ports, missing),
        ResultV1::Refused(Refused::RegistrationMissingOrStale)
    );
    let foreign = ReplicationFollowerAuditTargetV1::new(
        target.database_id(),
        2,
        target.leadership_epoch(),
        target.hold_id(),
    )
    .unwrap();
    assert_eq!(
        execute(&ports, register(foreign)),
        ResultV1::Refused(Refused::LineageMismatch)
    );
    assert_eq!(history(&ports), before);
    let ResultV1::Applied(original) = execute(&ports, register(target)) else {
        panic!("created");
    };
    let before = history(&ports);
    assert_eq!(
        execute(
            &ports,
            Request::retire(
                request_id(33),
                target,
                ChangelogTransactionSequence::new(original.generation().get() + 1).unwrap()
            )
        ),
        ResultV1::Refused(Refused::RegistrationMissingOrStale)
    );
    assert_eq!(
        execute(
            &ports,
            Request::register(
                request_id(34),
                target,
                FollowerHoldBudget::new(4).unwrap(),
                None
            )
        ),
        ResultV1::Refused(Refused::RegistrationConflict)
    );
    assert_eq!(history(&ports), before);
    assert_eq!(audit_count(&ports), 3);
}

#[test]
fn lifecycle_adopts_legacy_fence_and_retirement_releases_only_matching_bootstrap_custody() {
    use riffdb_storage_api::{
        FollowerRegistrationPhaseV1 as Phase, ReplicationSourceHoldKindV1 as Kind,
        ReplicationSourceHoldV1 as Hold,
    };
    let (_scope, ports, target) = fixture();
    let before = history(&ports);
    let legacy = Hold::new(
        target.hold_id(),
        Kind::FollowerAcknowledgement,
        before.lineage(),
        before.tail(),
    );
    let mut source = ports.replication_source_control();
    source.register(legacy).unwrap();
    let ResultV1::Applied(original) = execute(&ports, register(target)) else {
        panic!("adopted");
    };
    assert_eq!(original.after().hold(), legacy);
    assert_eq!(original.after().phase(), Phase::Attached);
    assert_eq!(
        execute(&ports, register(target)),
        ResultV1::Replayed(original)
    );

    let pending = ReplicationFollowerAuditTargetV1::new(
        target.database_id(),
        1,
        target.leadership_epoch(),
        ReplicationSourceHoldIdV1::new([0x72; 16]).unwrap(),
    )
    .unwrap();
    let ResultV1::Applied(original) = execute(&ports, register(pending)) else {
        panic!("pending");
    };
    let before = history(&ports);
    let job = Hold::new(
        pending.hold_id(),
        Kind::Bootstrap,
        before.lineage(),
        before.tail(),
    );
    let archive = Hold::new(
        pending.hold_id(),
        Kind::ArchiveAcknowledgement,
        before.lineage(),
        before.tail(),
    );
    source.register(job).unwrap();
    source.register(archive).unwrap();
    execute(
        &ports,
        Request::retire(request_id(40), pending, original.generation()),
    );
    let pin = ports.shared.database.begin_read().unwrap();
    let holds = pin
        .open_table(crate::changelog_v3_activation::SOURCE_HOLDS)
        .unwrap();
    assert!(holds.get(job.storage_key().as_slice()).unwrap().is_none());
    assert!(
        holds
            .get(archive.storage_key().as_slice())
            .unwrap()
            .is_some()
    );
    assert!(
        ports.bootstrap_id_is_held(pending.hold_id()).unwrap(),
        "independent archive custody"
    );
    assert!(source.attach_bootstrap(job).is_err());
    history(&ports);
}

#[test]
fn lifecycle_drains_published_journal_audit_before_assigning_its_sequence() {
    use riffdb_storage_api::ServiceAuditGroupAppend;
    let (_scope, mut ports, target) = fixture();
    let ServiceAuditGroupAppend::Submitted(fence) = ports
        .submit_service_audit_group(&[denied_audit(50)])
        .unwrap()
    else {
        panic!("journaled audit");
    };
    fence.wait().unwrap();
    let ResultV1::Applied(record) = execute(&ports, register(target)) else {
        panic!("registered");
    };
    assert_eq!(
        record.observed().frontier().administration(),
        AdministrationSequence::new(3)
    );
    assert_eq!(
        record.administration_sequence(),
        AdministrationSequence::new(4).unwrap()
    );
    assert_eq!(audit_count(&ports), 4);
    history(&ports);
}

#[test]
fn registration_capacity_refuses_new_identity_but_preserves_legacy_adoption() {
    use riffdb_storage_api::{
        MAX_REPLICATION_SOURCE_HOLDS_V1, ReplicationAdministrationRefusalV1 as Refused,
        ReplicationSourceHoldKindV1 as Kind, ReplicationSourceHoldV1 as Hold,
        proto_codec::encode_replication_source_hold_v1,
    };
    let (_scope, ports, target) = fixture();
    let before = history(&ports);
    let write = ports.shared.database.begin_write().unwrap();
    {
        let mut holds = write
            .open_table(crate::changelog_v3_activation::SOURCE_HOLDS)
            .unwrap();
        for id in 1..=MAX_REPLICATION_SOURCE_HOLDS_V1 {
            let hold = Hold::new(
                ReplicationSourceHoldIdV1::new(u128::from(id).to_be_bytes()).unwrap(),
                Kind::FollowerAcknowledgement,
                before.lineage(),
                before.tail(),
            );
            let bytes = encode_replication_source_hold_v1(hold).unwrap();
            holds
                .insert(hold.storage_key().as_slice(), bytes.as_bytes())
                .unwrap();
        }
    }
    ports.shared.commit_durable(write).unwrap();
    assert_eq!(
        execute(&ports, register(target)),
        ResultV1::Refused(Refused::CapacityExhausted)
    );
    assert_eq!(history(&ports), before);
    let existing = ReplicationFollowerAuditTargetV1::new(
        target.database_id(),
        1,
        target.leadership_epoch(),
        ReplicationSourceHoldIdV1::new(1_u128.to_be_bytes()).unwrap(),
    )
    .unwrap();
    assert!(matches!(
        execute(&ports, register(existing)),
        ResultV1::Applied(_)
    ));
    history(&ports);
}

#[test]
fn replication_administration_crash_child() {
    let Some(path) = std::env::var_os("RIFFDB_REPLICATION_ADMIN_PATH") else {
        return;
    };
    let store = crate::RedbStore::open(path).unwrap();
    let ports = crate::RedbOperationalPorts {
        shared: store.shared,
    };
    let history = history(&ports);
    let target = ReplicationFollowerAuditTargetV1::new(
        database_id(),
        1,
        LeadershipEpochV1::initial(),
        ReplicationSourceHoldIdV1::new([0x71; 16]).unwrap(),
    )
    .unwrap();
    let request = if std::env::var("RIFFDB_REPLICATION_ADMIN_RETIRE").unwrap() == "1" {
        Request::retire(request_id(32), target, history.tail().sequence())
    } else {
        register(target)
    };
    execute(&ports, request);
    panic!("selected crash edge was not reached");
}

#[test]
fn lifecycle_process_crashes_recover_complete_receipt_hold_and_allocator_or_neither() {
    use std::process::{Command, Stdio};
    for retire in [false, true] {
        for edge in [
            "administration-audit",
            "administration-hold",
            "administration-receipted",
            "administration-committed",
        ] {
            let (scope, ports, target) = fixture();
            let original = if retire {
                let ResultV1::Applied(record) = execute(&ports, register(target)) else {
                    panic!("registered");
                };
                Some(record)
            } else {
                None
            };
            let before = history(&ports);
            let count = audit_count(&ports);
            drop(ports);
            let status = Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "administration::tests::replication_writes::replication_administration_crash_child"])
                .env("RIFFDB_REPLICATION_ADMIN_PATH", &scope.0)
                .env("RIFFDB_REPLICATION_ADMIN_RETIRE", if retire { "1" } else { "0" })
                .env("RIFFDB_V3_SOURCE_CONTROL_EDGE", edge)
                .stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::null()).status().unwrap();
            assert_eq!(status.code(), Some(93), "{edge}, retire={retire}");
            let committed = edge == "administration-committed";
            for _ in 0..2 {
                let store = crate::RedbStore::open(&scope.0).unwrap();
                let ports = crate::RedbOperationalPorts {
                    shared: store.shared,
                };
                let after = history(&ports);
                assert_eq!(
                    after.tail().sequence().get(),
                    before.tail().sequence().get() + u64::from(committed)
                );
                assert_eq!(
                    after.tail().frontier().application(),
                    before.tail().frontier().application()
                );
                assert_eq!(audit_count(&ports), count + usize::from(committed));
                assert_eq!(
                    ports.bootstrap_id_is_held(target.hold_id()).unwrap(),
                    if retire { !committed } else { committed }
                );
            }
            let store = crate::RedbStore::open(&scope.0).unwrap();
            let ports = crate::RedbOperationalPorts {
                shared: store.shared,
            };
            let request = original.map_or_else(
                || register(target),
                |record| Request::retire(request_id(33), target, record.generation()),
            );
            assert_eq!(
                matches!(execute(&ports, request), ResultV1::Replayed(_)),
                committed,
                "recovery replay"
            );
        }
    }
}
