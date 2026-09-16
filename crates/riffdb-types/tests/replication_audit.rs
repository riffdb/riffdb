//! Request-selected follower audit identity and canonical target semantics.
// req: REP-005, REP-006

use riffdb_types::{
    DatabaseId, LeadershipEpochV1, ReplicationFollowerAuditTargetV1, ReplicationSourceHoldIdV1,
    ServiceAuditTargetV1, ServiceAuditTargetsError, ServiceAuditTargetsV1,
};

fn database(fill: u8) -> DatabaseId {
    let mut bytes = [fill; 16];
    bytes[6] = 0x70 | (fill & 0x0f);
    bytes[8] = 0x80 | (fill & 0x3f);
    DatabaseId::from_bytes(bytes).expect("UUIDv7 database")
}

fn target(db: u8, incarnation: u64, epoch: u64, id: u8) -> ServiceAuditTargetV1 {
    ServiceAuditTargetV1::ReplicationFollower(
        ReplicationFollowerAuditTargetV1::new(
            database(db),
            incarnation,
            LeadershipEpochV1::new(epoch).expect("nonzero epoch"),
            ReplicationSourceHoldIdV1::new([id; 16]).expect("nonzero hold"),
        )
        .expect("nonzero incarnation"),
    )
}

#[test]
fn follower_audit_key_binds_every_lineage_component_in_network_order() {
    let value = target(0x21, 0x0102_0304_0506_0708, 0x1112_1314_1516_1718, 0x42);
    let mut expected = vec![0x0b];
    expected.extend_from_slice(database(0x21).as_bytes());
    expected.extend_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8]);
    expected.extend_from_slice(&[17, 18, 19, 20, 21, 22, 23, 24]);
    expected.extend_from_slice(&[0x42; 16]);
    assert_eq!(value.tag(), 0x0b);
    assert_eq!(value.canonical_key(), expected);
    assert_eq!(expected.len(), 49);
    assert_eq!(
        format!("{value:?}"),
        "ServiceAuditTargetV1::ReplicationFollower([REDACTED])"
    );
}

#[test]
fn follower_targets_are_distinct_across_database_incarnation_epoch_and_hold() {
    let ordered = [
        target(1, 1, 1, 1),
        target(1, 1, 1, 2),
        target(1, 1, 2, 1),
        target(1, 2, 1, 1),
        target(2, 1, 1, 1),
    ];
    let values = ServiceAuditTargetsV1::new(ordered.iter().rev().cloned()).expect("distinct");
    assert_eq!(values.as_slice(), ordered);
    assert_eq!(
        ServiceAuditTargetsV1::new([ordered[0].clone(), ordered[0].clone()]),
        Err(ServiceAuditTargetsError::Duplicate)
    );
    assert_eq!(
        ServiceAuditTargetsV1::new((1..=16).map(|id| target(1, 1, 1, id)))
            .expect("sixteen targets")
            .len(),
        16
    );
    assert_eq!(
        ServiceAuditTargetsV1::new((1..=17).map(|id| target(1, 1, 1, id))),
        Err(ServiceAuditTargetsError::TooMany { maximum: 16 })
    );
}

#[test]
fn follower_audit_identity_refuses_unassigned_lineage_and_redacts_payload() {
    assert!(LeadershipEpochV1::new(0).is_none());
    assert!(ReplicationSourceHoldIdV1::new([0; 16]).is_none());
    let hold = ReplicationSourceHoldIdV1::new([0x42; 16]).expect("hold");
    assert!(
        ReplicationFollowerAuditTargetV1::new(database(1), 0, LeadershipEpochV1::initial(), hold)
            .is_none()
    );
    let value = ReplicationFollowerAuditTargetV1::new(
        database(1),
        u64::MAX,
        LeadershipEpochV1::new(u64::MAX).expect("maximum epoch"),
        hold,
    )
    .expect("maximum lineage");
    assert!(value.leadership_epoch().checked_next().is_none());
    assert_eq!(value.history_incarnation(), u64::MAX);
    assert_eq!(
        format!("{value:?}"),
        "ReplicationFollowerAuditTargetV1([REDACTED])"
    );
    assert_eq!(format!("{hold:?}"), "ReplicationSourceHoldIdV1([redacted])");
}
