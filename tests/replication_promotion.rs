//! Production storage portion of WP-748; administrative promotion is separate.
// req: REP-006
use super::*;
use riffdb_storage_api::{
    FollowerHoldBudget, OutboxSucceedV1, ReplicationSourceHoldIdV1, ReplicationSourceHoldKindV1,
    ReplicationSourceHoldV1,
};
use riffdb_storage_redb::RedbOfflineRetention;

fn attach_follower(
    ports: &RedbOperationalPorts,
    root: &Path,
    seed: u8,
) -> riffdb_storage_api::ReplicationBootstrapManifestV1 {
    let id = ReplicationSourceHoldIdV1::new([seed; 16]).unwrap();
    let mut source = ports.begin_replication_bootstrap_v3(root, id).unwrap();
    while !source.advance().unwrap() {}
    let held = source.finish().unwrap();
    let manifest = held.manifest();
    ports
        .attach_replication_bootstrap_v3(manifest, manifest.fence().history().tail())
        .unwrap();
    manifest
}

fn deliver_command_event(ports: &mut RedbOperationalPorts, fixture: &CommandFixture) {
    let claim = OutboxClaimV1::new(
        fixture.records.events()[0].event_id(),
        OutboxStatusObservationV1::AbsentInitialPending,
        OutboxDestinationIdV1::new("follower-retention-proof").unwrap(),
        Timestamp::new(1_700_000_010, 0).unwrap(),
        Timestamp::new(1_700_000_020, 0).unwrap(),
    )
    .unwrap();
    let OutboxTransitionResultV1::Applied(delivering) = ports.claim_outbox(&claim).unwrap() else {
        panic!("fresh event claim must apply");
    };
    let success =
        OutboxSucceedV1::new(delivering, Timestamp::new(1_700_000_011, 0).unwrap()).unwrap();
    assert!(matches!(
        ports.succeed_outbox(&success).unwrap(),
        OutboxTransitionResultV1::Applied(_)
    ));
}

#[test]
fn retention_prune_refuses_to_pass_registered_follower_frontier() {
    let path = TestDatabasePath::new("registered-follower-retention");
    prepare_command_database(&path.0);
    let mut ports = open_operational(RedbStore::open(&path.0).unwrap());
    let first = attach_follower(&ports, &path.0.with_extension("first-bootstrap"), 0x31);
    let second = attach_follower(&ports, &path.0.with_extension("second-bootstrap"), 0x32);
    let fixture = command_fixture_at(1);
    commit_command_fixture(&ports, &fixture);
    deliver_command_event(&mut ports, &fixture);
    let head = ports
        .published_changelog_snapshot_v3()
        .unwrap()
        .authoritative_state_v3()
        .unwrap()
        .history();
    assert_eq!(head.tail().frontier().application(), CommitSequence::new(1));
    // An exhausted observation cannot release the lagging follower's hold.
    // These exact same-lineage positions came from the attached manifest and
    // current source publication; the prune below proves the durable fence.
    let lagging = ReplicationSourceHoldV1::new(
        second.fence().hold_id(),
        ReplicationSourceHoldKindV1::FollowerAcknowledgement,
        head.lineage(),
        second.fence().history().tail(),
    );
    let budget = FollowerHoldBudget::new(1).unwrap();
    let exhausted = budget.observe(lagging, head).unwrap();
    assert_eq!(exhausted.application_lag_sequences(), 1);
    assert!(exhausted.is_exhausted());
    // One current follower cannot release the other's durable lagging fence.
    ports
        .acknowledge_replication_follower_v3(first.fence().hold_id(), head.lineage(), head.tail())
        .unwrap();
    drop(ports);
    let retention = RedbOfflineRetention::bind(&path.0);
    assert_eq!(
        retention.prune_to(1).unwrap_err().kind(),
        riffdb_storage_api::StorageErrorKind::InvariantViolation
    );
    let status = retention.status().unwrap();
    assert_eq!(status.watermark_sequence, 0);
    assert_eq!(status.tombstone_count, 0);
    assert_eq!(status.max_permissible_watermark, Some(0));
    assert_eq!(
        status.fence_binding,
        riffdb_storage_api::RetentionFenceBinding::FollowerLowWater
    );
    let ports = open_operational(RedbStore::open(&path.0).unwrap());
    assert_eq!(
        ports.read_commit(CommitSequence::new(1).unwrap()).unwrap(),
        Some(fixture.records.commit().clone())
    );
    assert_eq!(
        ports.read_entity(&fixture.target).unwrap(),
        Some(fixture.records.entities()[0].post_image().clone())
    );
    ports
        .acknowledge_replication_follower_v3(second.fence().hold_id(), head.lineage(), head.tail())
        .unwrap();
    let caught_up = ReplicationSourceHoldV1::new(
        second.fence().hold_id(),
        ReplicationSourceHoldKindV1::FollowerAcknowledgement,
        head.lineage(),
        head.tail(),
    );
    assert!(!budget.observe(caught_up, head).unwrap().is_exhausted());
    drop(ports);
    let status = retention.prune_to(1).unwrap();
    assert_eq!(status.watermark_sequence, 1);
    assert_eq!(status.tombstone_count, 1);
    let ports = open_operational(RedbStore::open(&path.0).unwrap());
    assert_eq!(
        ports.read_entity(&fixture.target).unwrap(),
        Some(fixture.records.entities()[0].post_image().clone())
    );
    assert_eq!(
        ports
            .read_commit(CommitSequence::new(1).unwrap())
            .unwrap_err()
            .kind(),
        riffdb_storage_api::StorageErrorKind::HistoryPruned
    );
}
