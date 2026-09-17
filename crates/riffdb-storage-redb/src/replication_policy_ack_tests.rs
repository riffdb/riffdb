//! V2 acknowledgement policy is retained across the existing source-only lane.
// req: REP-003, REP-006, REC-001, STO-012
use super::*;
use riffdb_storage_api::{
    AuthoritativeTransactionV3, ChangelogAttributionV3, ChangelogCursorErrorV3 as Refusal,
    proto_codec::decode_replication_source_hold,
};

pub(super) fn history(ports: &crate::RedbOperationalPorts) -> History {
    crate::changelog_v3_roots::validate_retained_history(
        &ports.shared.database.begin_read().unwrap(),
    )
    .unwrap()
    .unwrap()
}

pub(super) fn current(ports: &crate::RedbOperationalPorts, id: Hold) -> Policy {
    let pin = ports.shared.database.begin_read().unwrap();
    let table = pin
        .open_table(crate::changelog_v3_activation::SOURCE_HOLDS)
        .unwrap();
    let row = table.get(id.storage_key().as_slice()).unwrap().unwrap();
    match *decode_replication_source_hold(row.value()).unwrap().value() {
        State::Registered(policy) => policy,
        State::Legacy(_) => panic!("acknowledgement downgraded the registered policy"),
    }
}

// Isolated physical fixtures have no public registration/attachment writer yet.
fn attached_fixture(ports: &crate::RedbOperationalPorts, original: Policy) -> Policy {
    let attached = Policy::new(
        original.hold(),
        original.registered_at(),
        original.budget(),
        original.expires_at(),
        Phase::Attached,
        original.degraded_at(),
    )
    .unwrap();
    let write = ports.shared.database.begin_write().unwrap();
    let bytes = encode_replication_source_hold_v2(attached).unwrap();
    write
        .open_table(crate::changelog_v3_activation::SOURCE_HOLDS)
        .unwrap()
        .insert(attached.hold().storage_key().as_slice(), bytes.as_bytes())
        .unwrap();
    crate::changelog_v3_roots::validate_retained_history_for_write(&write).unwrap();
    ports.shared.commit_durable(write).unwrap();
    attached
}

#[test]
fn registered_ack_preserves_policy_and_exact_retry_without_admin_advance() {
    let (_scope, ports, registration) = stored_registration();
    let original = attached_fixture(&ports, registration.after());
    let before = history(&ports);
    let next = Hold::new(
        original.hold().id(),
        Kind::FollowerAcknowledgement,
        before.lineage(),
        before.tail(),
    );
    let mut control = ports.replication_source_control();
    assert!(control.advance_acknowledgement(next).unwrap());
    let after = history(&ports);
    let observed = current(&ports, next);
    assert_eq!(observed.hold(), next);
    assert_eq!(observed.generation(), original.generation());
    assert_eq!(observed.registered_at(), original.registered_at());
    assert_eq!(observed.budget(), original.budget());
    assert_eq!(observed.expires_at(), original.expires_at());
    assert_eq!(observed.phase(), Phase::Attached);
    assert_eq!(after.tail().frontier(), before.tail().frontier());
    assert_eq!(
        after.tail().sequence(),
        before.tail().sequence().checked_next().unwrap()
    );
    let pin = ports.shared.database.begin_read().unwrap();
    let receipts = pin
        .open_table(crate::changelog_v3_activation::HISTORY)
        .unwrap();
    let row = receipts
        .get(after.tail().sequence().get().to_be_bytes().as_slice())
        .unwrap()
        .unwrap();
    let receipt = AuthoritativeTransactionV3::decode(row.value()).unwrap();
    assert_eq!(
        receipt.attribution(),
        ChangelogAttributionV3::ReplicationSourceHold
    );
    assert!(receipt.mutations().is_empty());
    let audit = pin.open_table(crate::layout::AUDIT).unwrap();
    assert_eq!(
        audit
            .get(crate::keys::encode_audit_key(registration.administration_sequence()).as_slice())
            .unwrap()
            .unwrap()
            .value(),
        encode_replication_administration_v1(&registration)
            .unwrap()
            .as_bytes()
    );
    drop(pin);
    let epoch = ports.shared.durable_commit_epoch();
    assert!(!control.advance_acknowledgement(next).unwrap());
    assert!(control.advance_acknowledgement(original.hold()).is_err());
    assert!(
        control.register(next).is_err(),
        "legacy registration cannot overwrite V2"
    );
    assert_eq!(ports.shared.durable_commit_epoch(), epoch);
    assert_eq!(history(&ports), after);
    assert_eq!(current(&ports, next), observed);
}

#[test]
fn awaiting_or_retired_registration_cannot_acknowledge_even_an_equal_fence() {
    for retired in [false, true] {
        let (_scope, ports, registration) = stored_registration();
        let original = registration.after();
        if retired {
            let before = history(&ports);
            let policy = Policy::new(
                original.hold(),
                original.registered_at(),
                original.budget(),
                original.expires_at(),
                Phase::Retired,
                None,
            )
            .unwrap();
            let release = Record::new(
                registration
                    .administration_sequence()
                    .checked_next()
                    .unwrap(),
                registration.timestamp(),
                Action::RetireFollower,
                registration.target(),
                Some(State::Registered(original)),
                policy,
                before.tail(),
                explicit(),
            )
            .unwrap();
            store_record(&ports, &release, before);
        }
        let before = history(&ports);
        let epoch = ports.shared.durable_commit_epoch();
        let mut control = ports.replication_source_control();
        for point in [original.hold().fence(), before.tail()] {
            let hold = Hold::new(
                original.hold().id(),
                Kind::FollowerAcknowledgement,
                before.lineage(),
                point,
            );
            assert_eq!(
                control.advance_acknowledgement(hold),
                Err(Refusal::InvalidPosition)
            );
            assert!(control.register(hold).is_err());
        }
        assert_eq!(history(&ports), before);
        assert_eq!(ports.shared.durable_commit_epoch(), epoch);
    }
}

#[test]
fn registered_ack_crash_child() {
    let Some(path) = std::env::var_os("RIFFDB_REGISTERED_ACK_PATH") else {
        return;
    };
    let store = crate::RedbStore::open(path).unwrap();
    let ports = crate::RedbOperationalPorts {
        shared: store.shared,
    };
    let before = history(&ports);
    let hold = Hold::new(
        ReplicationSourceHoldIdV1::new([0x41; 16]).unwrap(),
        Kind::FollowerAcknowledgement,
        before.lineage(),
        before.tail(),
    );
    ports
        .replication_source_control()
        .advance_acknowledgement(hold)
        .unwrap();
    panic!("selected acknowledgement crash edge was not reached");
}

#[test]
fn registered_ack_crashes_preserve_whole_policy_and_original_registration() {
    use std::process::{Command, Stdio};
    for edge in ["hold-staged", "hold-receipted", "hold-committed"] {
        let (scope, ports, registration) = stored_registration();
        let original = attached_fixture(&ports, registration.after());
        let before = history(&ports);
        let path = scope.join("db.redb");
        drop(ports);
        let status = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "replication_registration_links::tests::acknowledgements::registered_ack_crash_child"])
            .env("RIFFDB_REGISTERED_ACK_PATH", &path)
            .env("RIFFDB_V3_SOURCE_CONTROL_EDGE", edge)
            .stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::null())
            .status().unwrap();
        assert_eq!(status.code(), Some(93), "{edge}");
        let committed = edge == "hold-committed";
        for _ in 0..2 {
            let store = crate::RedbStore::open(&path).unwrap();
            let ports = crate::RedbOperationalPorts {
                shared: store.shared,
            };
            let recovered = history(&ports);
            let policy = current(&ports, original.hold());
            assert_eq!(policy.registered_at(), original.registered_at());
            assert_eq!(policy.generation(), original.generation());
            assert_eq!(policy.budget(), original.budget());
            assert_eq!(policy.expires_at(), original.expires_at());
            assert_eq!(policy.phase(), Phase::Attached);
            assert_eq!(
                policy.hold().fence(),
                if committed {
                    before.tail()
                } else {
                    original.hold().fence()
                }
            );
            assert_eq!(
                recovered.tail().sequence().get(),
                before.tail().sequence().get() + u64::from(committed)
            );
            assert_eq!(recovered.tail().frontier(), before.tail().frontier());
            let pin = ports.shared.database.begin_read().unwrap();
            let audit = pin.open_table(crate::layout::AUDIT).unwrap();
            assert_eq!(
                audit
                    .get(
                        crate::keys::encode_audit_key(registration.administration_sequence())
                            .as_slice()
                    )
                    .unwrap()
                    .unwrap()
                    .value(),
                encode_replication_administration_v1(&registration)
                    .unwrap()
                    .as_bytes()
            );
        }
    }
}
