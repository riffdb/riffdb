//! Source custody for a bootstrap of one exact V2 registration generation.
// req: REP-003, REP-006, REC-001, STO-012
use super::{
    acknowledgements::{current, history},
    *,
};
use riffdb_storage_api::{
    ChangelogCursorErrorV3 as Refusal, proto_codec::decode_replication_source_hold_v1,
};

fn bootstrap(policy: Policy, fence: Point) -> Hold {
    Hold::new(
        policy.hold().id(),
        Kind::Bootstrap,
        policy.hold().lineage(),
        fence,
    )
}

fn retained_bootstrap(ports: &crate::RedbOperationalPorts, expected: Hold) -> Option<Hold> {
    let pin = ports.shared.database.begin_read().unwrap();
    let table = pin
        .open_table(crate::changelog_v3_activation::SOURCE_HOLDS)
        .unwrap();
    table
        .get(expected.storage_key().as_slice())
        .unwrap()
        .map(|row| {
            *decode_replication_source_hold_v1(row.value())
                .unwrap()
                .value()
        })
}

fn retire(ports: &crate::RedbOperationalPorts, registration: &Record) {
    let before = history(ports);
    let prior = current(ports, registration.after().hold());
    let retired = Policy::new(
        prior.hold(),
        prior.registered_at(),
        prior.budget(),
        prior.expires_at(),
        Phase::Retired,
        prior.degraded_at(),
    )
    .unwrap();
    let release = Record::new(
        before
            .tail()
            .frontier()
            .administration()
            .unwrap()
            .checked_next()
            .unwrap(),
        registration.timestamp(),
        Action::RetireFollower,
        registration.target(),
        Some(State::Registered(prior)),
        retired,
        before.tail(),
        explicit(),
    )
    .unwrap();
    store_record(ports, &release, before);
}

#[test]
fn registered_bootstrap_preserves_policy_and_attaches_only_its_exact_fence() {
    for predecessor_cut in [true, false] {
        let (_scope, ports, registration) = stored_registration();
        let original = registration.after();
        let source = history(&ports);
        let cut = if predecessor_cut {
            original.registered_at()
        } else {
            source.tail()
        };
        let bootstrap = bootstrap(original, cut);
        let mut control = ports.replication_source_control();
        assert!(ports.bootstrap_id_is_held(bootstrap.id()).unwrap());
        assert!(control.register(bootstrap).unwrap());
        assert_eq!(current(&ports, original.hold()), original);
        let held = history(&ports);
        assert!(!control.register(bootstrap).unwrap());
        assert_eq!(history(&ports), held);
        assert!(control.attach_bootstrap(bootstrap).unwrap());
        let attached = current(&ports, original.hold());
        assert_eq!(attached.phase(), Phase::Attached);
        assert_eq!(attached.hold().fence(), bootstrap.fence());
        assert_eq!(attached.registered_at(), original.registered_at());
        assert_eq!(attached.generation(), original.generation());
        assert_eq!(attached.budget(), original.budget());
        assert_eq!(attached.expires_at(), original.expires_at());
        assert_eq!(retained_bootstrap(&ports, bootstrap), None);
        let after = history(&ports);
        assert_eq!(after.tail().frontier(), source.tail().frontier());
        assert_eq!(
            after.tail().sequence().get(),
            source.tail().sequence().get() + 2
        );
        let epoch = ports.shared.durable_commit_epoch();
        assert!(!control.attach_bootstrap(bootstrap).unwrap());
        assert_eq!(control.register(bootstrap), Err(Refusal::InvalidPosition));
        assert_eq!(ports.shared.durable_commit_epoch(), epoch);
        assert_eq!(history(&ports), after);
    }
}

#[test]
fn registered_bootstrap_refuses_artifacts_before_its_registered_fence_and_missing_custody() {
    let (_scope, ports, registration) = stored_registration();
    let policy = registration.after();
    let before = history(&ports);
    let epoch = ports.shared.durable_commit_epoch();
    let mut control = ports.replication_source_control();
    let pin = ports.shared.database.begin_read().unwrap();
    let receipts = pin
        .open_table(crate::changelog_v3_activation::HISTORY)
        .unwrap();
    let row = receipts
        .get(
            (policy.registered_at().sequence().get() - 1)
                .to_be_bytes()
                .as_slice(),
        )
        .unwrap()
        .unwrap();
    let earlier = riffdb_storage_api::AuthoritativeTransactionV3::decode(row.value()).unwrap();
    let old_artifact = bootstrap(policy, Point::from_receipt(&earlier).unwrap());
    assert_eq!(
        control.register(old_artifact),
        Err(Refusal::InvalidPosition)
    );
    assert_eq!(
        control.attach_bootstrap(old_artifact),
        Err(Refusal::InvalidPosition)
    );
    let unheld = bootstrap(policy, before.tail());
    assert_eq!(
        control.attach_bootstrap(unheld),
        Err(Refusal::InvalidPosition)
    );
    assert_eq!(history(&ports), before);
    assert_eq!(ports.shared.durable_commit_epoch(), epoch);
    assert_eq!(current(&ports, policy.hold()), policy);
}

#[test]
fn retired_registration_refuses_existing_bootstrap_custody_and_attachment() {
    let (_scope, ports, registration) = stored_registration();
    let artifact = bootstrap(registration.after(), history(&ports).tail());
    let mut control = ports.replication_source_control();
    control.register(artifact).unwrap();
    retire(&ports, &registration);
    let before = history(&ports);
    let epoch = ports.shared.durable_commit_epoch();
    assert_eq!(control.register(artifact), Err(Refusal::InvalidPosition));
    assert_eq!(
        control.attach_bootstrap(artifact),
        Err(Refusal::InvalidPosition)
    );
    assert!(
        ports.bootstrap_id_is_held(artifact.id()).unwrap(),
        "separate job still held"
    );
    assert_eq!(history(&ports), before);
    assert_eq!(ports.shared.durable_commit_epoch(), epoch);
}

#[test]
fn retired_artifact_custody_requires_exact_release_evidence() {
    let (_scope, ports, registration) = stored_registration();
    let id = registration.after().hold().id();
    assert!(ports.bootstrap_id_is_held(id).unwrap());
    retire(&ports, &registration);
    assert!(!ports.bootstrap_id_is_held(id).unwrap());
    let write = ports.shared.database.begin_write().unwrap();
    write
        .open_table(crate::layout::AUDIT)
        .unwrap()
        .remove(crate::keys::encode_audit_key(registration.administration_sequence()).as_slice())
        .unwrap();
    ports.shared.commit_durable(write).unwrap();
    assert!(
        ports.bootstrap_id_is_held(id).is_err(),
        "unproven retirement grants no cleanup"
    );
}

#[test]
fn retirement_withholds_manifest_and_pages_from_an_already_held_source() {
    let (scope, ports, registration) = stored_registration();
    let id = registration.after().hold().id();
    let mut build = ports
        .begin_replication_bootstrap_v3(&scope.join("held-source"), id)
        .unwrap();
    let mut complete = false;
    for _ in 0..200 {
        if build.advance().unwrap() {
            complete = true;
            break;
        }
    }
    assert!(
        complete,
        "bounded fixture must finish within its page ceiling"
    );
    let source = build.finish().unwrap();
    source.verify_private_identity().unwrap();
    source.read_page(1).unwrap();
    retire(&ports, &registration);
    assert_eq!(
        [
            source
                .verify_private_identity()
                .err()
                .map(|error| error.kind()),
            source.read_page(1).err().map(|error| error.kind()),
        ],
        [Some(riffdb_storage_api::StorageErrorKind::Unavailable); 2]
    );
}

#[test]
fn adopted_legacy_follower_keeps_its_exact_older_attachment_retry() {
    let (_scope, ports, states) = crate::changelog_v3_control_tests::fixture();
    let oldest = states[2];
    let hold = Hold::new(
        ReplicationSourceHoldIdV1::new([0x41; 16]).unwrap(),
        Kind::FollowerAcknowledgement,
        oldest.lineage(),
        oldest.tail(),
    );
    let mut control = ports.replication_source_control();
    control.register(hold).unwrap();
    let before = history(&ports);
    let policy = Policy::new(
        hold,
        before.tail(),
        FollowerHoldBudget::new(2).unwrap(),
        None,
        Phase::Attached,
        None,
    )
    .unwrap();
    let record = Record::new(
        AdministrationSequence::first(),
        Timestamp::new(1_700_000_000, 0).unwrap(),
        Action::RegisterFollower,
        Target::new(
            hold.lineage().database_id(),
            hold.lineage().history_incarnation(),
            hold.lineage().leadership_epoch(),
            hold.id(),
        )
        .unwrap(),
        Some(State::Legacy(hold)),
        policy,
        before.tail(),
        explicit(),
    )
    .unwrap();
    store_record(&ports, &record, before);
    let before = history(&ports);
    let epoch = ports.shared.durable_commit_epoch();
    let artifact = bootstrap(policy, hold.fence());
    assert!(!control.attach_bootstrap(artifact).unwrap());
    assert_eq!(control.register(artifact), Err(Refusal::InvalidPosition));
    assert_eq!(current(&ports, hold), policy);
    assert_eq!(history(&ports), before);
    assert_eq!(ports.shared.durable_commit_epoch(), epoch);
}

#[test]
fn registered_bootstrap_crash_child() {
    let Some(path) = std::env::var_os("RIFFDB_REGISTERED_BOOTSTRAP_PATH") else {
        return;
    };
    let store = crate::RedbStore::open(path).unwrap();
    let ports = crate::RedbOperationalPorts {
        shared: store.shared,
    };
    let before = history(&ports);
    let expected = Hold::new(
        ReplicationSourceHoldIdV1::new([0x41; 16]).unwrap(),
        Kind::Bootstrap,
        before.lineage(),
        before.tail(),
    );
    let edge = std::env::var("RIFFDB_V3_SOURCE_CONTROL_EDGE").unwrap();
    let mut control = ports.replication_source_control();
    if edge.starts_with("hold-") {
        control.register(expected).unwrap();
    } else {
        let held = retained_bootstrap(&ports, expected).unwrap();
        control.attach_bootstrap(held).unwrap();
    }
    panic!("selected bootstrap custody crash edge was not reached");
}

#[test]
fn registered_bootstrap_crashes_keep_complete_custody_and_policy() {
    use std::process::{Command, Stdio};
    for edge in [
        "hold-staged",
        "hold-receipted",
        "hold-committed",
        "attachment-follower-staged",
        "attachment-bootstrap-removed",
        "attachment-receipted",
        "attachment-committed",
    ] {
        let (scope, ports, registration) = stored_registration();
        let policy = registration.after();
        let artifact = bootstrap(policy, history(&ports).tail());
        let attaching = edge.starts_with("attachment-");
        if attaching {
            ports
                .replication_source_control()
                .register(artifact)
                .unwrap();
        }
        let before = history(&ports);
        let path = scope.join("db.redb");
        drop(ports);
        let status = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "replication_registration_links::tests::bootstrap::registered_bootstrap_crash_child"])
            .env("RIFFDB_REGISTERED_BOOTSTRAP_PATH", &path)
            .env("RIFFDB_V3_SOURCE_CONTROL_EDGE", edge)
            .stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::null())
            .status().unwrap();
        assert_eq!(status.code(), Some(93), "{edge}");
        let committed = edge.ends_with("-committed");
        for _ in 0..2 {
            let store = crate::RedbStore::open(&path).unwrap();
            let ports = crate::RedbOperationalPorts {
                shared: store.shared,
            };
            let recovered = history(&ports);
            let observed = current(&ports, policy.hold());
            assert_eq!(observed.registered_at(), policy.registered_at());
            assert_eq!(observed.generation(), policy.generation());
            assert_eq!(observed.budget(), policy.budget());
            assert_eq!(observed.expires_at(), policy.expires_at());
            assert_eq!(
                observed.phase(),
                if attaching && committed {
                    Phase::Attached
                } else {
                    Phase::AwaitingBootstrap
                }
            );
            assert_eq!(
                observed.hold().fence(),
                if attaching && committed {
                    artifact.fence()
                } else {
                    policy.hold().fence()
                }
            );
            assert_eq!(
                retained_bootstrap(&ports, artifact),
                if attaching != committed {
                    Some(artifact)
                } else {
                    None
                }
            );
            assert_eq!(
                recovered.tail().sequence().get(),
                before.tail().sequence().get() + u64::from(committed)
            );
            assert_eq!(recovered.tail().frontier(), before.tail().frontier());
        }
    }
}
