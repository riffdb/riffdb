//! Sequence-only replication telemetry and fail-closed unknown progress.
// req: REP-004
use riffdb_service::{ReplicationRole, ReplicationStatistics};
use riffdb_types::{AdministrationSequence, CommitSequence, DualFrontier};

fn frontier(app: u64, admin: u64) -> DualFrontier {
    DualFrontier::new(CommitSequence::new(app), AdministrationSequence::new(admin))
}
#[test]
fn replication_lag_counts_application_and_administration_sequences_separately() {
    // Existing application history predates V3 activation; the physical frame
    // sequence is deliberately much smaller than either logical frontier.
    let applied = frontier(103, 21);
    let ack = frontier(102, 20);
    let source = frontier(110, 24);
    let follower = ReplicationStatistics::follower(applied, Some(ack), Some(source)).unwrap();
    assert_eq!(follower.role(), ReplicationRole::Follower);
    assert_eq!(follower.applied_frontier(), frontier(103, 21));
    assert_eq!(follower.acknowledged_frontier(), Some(frontier(102, 20)));
    assert_eq!(follower.application_lag_sequences(), Some(7));
    assert_eq!(follower.administration_lag_sequences(), Some(3));
    assert!(!follower.is_caught_up());

    let primary = ReplicationStatistics::primary(frontier(110, 24), 2, Some(ack)).unwrap();
    assert_eq!(primary.role(), ReplicationRole::Primary);
    assert_eq!(primary.registered_followers(), Some(2));
    assert_eq!(primary.applied_frontier(), frontier(110, 24));
    assert_eq!(primary.application_lag_sequences(), Some(8));
    assert_eq!(primary.administration_lag_sequences(), Some(4));
}

#[test]
fn missing_source_or_followers_never_invents_zero_lag() {
    let applied = frontier(103, 21);
    let follower = ReplicationStatistics::follower(applied, Some(applied), None).unwrap();
    assert_eq!(follower.source_frontier(), None);
    assert_eq!(follower.application_lag_sequences(), None);
    assert_eq!(follower.administration_lag_sequences(), None);
    assert!(!follower.is_caught_up());
    let primary = ReplicationStatistics::primary(applied, 0, None).unwrap();
    assert_eq!(primary.acknowledged_frontier(), None);
    assert_eq!(primary.application_lag_sequences(), None);
    assert_eq!(primary.administration_lag_sequences(), None);
    assert_eq!(primary.registered_followers(), Some(0));
}

#[test]
fn recovered_applied_prefix_does_not_invent_a_local_acknowledgement() {
    let applied = frontier(103, 21);
    let report = ReplicationStatistics::follower(applied, None, Some(frontier(105, 22))).unwrap();
    assert_eq!(report.applied_frontier(), applied);
    assert_eq!(report.acknowledged_frontier(), None);
    assert_eq!(report.application_lag_sequences(), Some(2));
    assert_eq!(report.administration_lag_sequences(), Some(1));
}

#[test]
fn operational_replication_is_one_typed_component_and_survives_statistics_assembly() {
    use riffdb_service::{
        ComponentHealth, HealthComponentKind, HealthComponentStatus, OperationalHealthSnapshot,
        OperationalStatisticsSnapshot, StatisticsResult,
    };
    let progress = ReplicationStatistics::follower(frontier(103, 21), None, None).unwrap();
    let health = ComponentHealth::replication(progress);
    assert_eq!(health.component(), HealthComponentKind::Replication);
    assert_eq!(health.status(), HealthComponentStatus::Degraded);
    assert_eq!(health.replication_statistics(), Some(progress));
    assert!(OperationalHealthSnapshot::new(vec![health, health]).is_err());
    let stats = OperationalStatisticsSnapshot::new(CommitSequence::new(103), None, None)
        .with_replication(progress);
    assert_eq!(
        StatisticsResult::new(0, 0, stats).unwrap().replication(),
        Some(progress)
    );
    let primary = ReplicationStatistics::primary(frontier(103, 21), 0, None).unwrap();
    assert_eq!(
        ComponentHealth::replication(primary).status(),
        HealthComponentStatus::Healthy
    );
    assert_eq!(primary.application_lag_sequences(), None);
}

#[test]
// req: REP-006
fn expired_registration_degrades_health_even_when_application_and_audit_heads_match() {
    use riffdb_service::{ComponentHealth, HealthComponentStatus};
    let head = frontier(3, 7);
    let progress = ReplicationStatistics::primary(head, 1, Some(head)).unwrap();
    assert!(progress.is_caught_up());
    let health = ComponentHealth::replication_with_retention_degradation(progress, true).unwrap();
    assert_eq!(health.status(), HealthComponentStatus::Degraded);
    assert_eq!(health.replication_statistics(), Some(progress));
    assert_eq!(
        ComponentHealth::replication(progress).status(),
        HealthComponentStatus::Healthy
    );
    assert!(
        ComponentHealth::replication_with_retention_degradation(
            ReplicationStatistics::primary(head, 0, None).unwrap(),
            true,
        )
        .is_err()
    );
    assert!(
        ComponentHealth::replication_with_retention_degradation(
            ReplicationStatistics::follower(head, Some(head), Some(head)).unwrap(),
            true,
        )
        .is_err()
    );
}

#[test]
fn follower_health_requires_replication_and_catalog_without_a_primary_coordinator() {
    use riffdb_service::{
        BuildInfo, ComponentHealth, HealthComponentKind as Kind, HealthComponentStatus as Status,
        HealthReport, HealthStatus, OperationalHealthSnapshot,
    };
    let head = frontier(103, 21);
    let build = || BuildInfo::new("0.1.0", "test", "rustc-test", vec![], 1, 1, "test").unwrap();
    for (replication, expected) in [
        (
            ComponentHealth::replication(
                ReplicationStatistics::follower(head, Some(head), Some(head)).unwrap(),
            ),
            HealthStatus::Ready,
        ),
        (
            ComponentHealth::replication(
                ReplicationStatistics::follower(head, None, None).unwrap(),
            ),
            HealthStatus::Degraded,
        ),
        (
            ComponentHealth::new(Kind::Replication, Status::Unavailable),
            HealthStatus::NotReady,
        ),
    ] {
        for catalog_ready in [true, false] {
            let operational = OperationalHealthSnapshot::new(vec![
                ComponentHealth::new(Kind::AuthoritativeStorage, Status::Healthy),
                ComponentHealth::new(
                    Kind::Catalog,
                    if catalog_ready {
                        Status::Healthy
                    } else {
                        Status::Unavailable
                    },
                ),
                replication,
            ])
            .unwrap();
            let report = HealthReport::new(
                Some(riffdb_types::ContractVersion::new(1).unwrap()),
                head.application(),
                operational,
                riffdb_types::Timestamp::new(1, 0).unwrap(),
                build(),
            );
            assert_eq!(
                report.status(),
                HealthStatus::NotReady,
                "primary requires its coordinator"
            );
            let report = report.for_role(ReplicationRole::Follower);
            assert_eq!(
                report.status(),
                if catalog_ready {
                    expected
                } else {
                    HealthStatus::NotReady
                }
            );
            assert!(
                report
                    .components()
                    .iter()
                    .all(|component| component.component() != Kind::CommitCoordinator)
            );
        }
    }
}

#[test]
fn impossible_progress_refuses_instead_of_saturating_negative_lag() {
    let applied = frontier(103, 21);
    for source in [frontier(102, 22), frontier(104, 20)] {
        assert!(ReplicationStatistics::follower(applied, Some(applied), Some(source)).is_err());
        assert!(ReplicationStatistics::primary(source, 1, Some(applied)).is_err());
    }
    assert!(ReplicationStatistics::follower(applied, Some(frontier(104, 22)), None).is_err());
    assert!(ReplicationStatistics::primary(applied, 0, Some(applied)).is_err());
    assert!(ReplicationStatistics::primary(applied, 1, None).is_err());
    let caught_up = ReplicationStatistics::follower(applied, Some(applied), Some(applied)).unwrap();
    assert!(
        caught_up.is_caught_up(),
        "control-only receipts create no logical lag"
    );
    assert_eq!(caught_up.application_lag_sequences(), Some(0));
    assert_eq!(caught_up.administration_lag_sequences(), Some(0));
}
