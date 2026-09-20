//! Isolated follower derived-write boundary; these typed fixtures grant no readiness.
// req: REP-002, PRJ-001, REC-001, PERF-007
use super::*;
use redb::ReadableDatabase;
use riffdb_storage_api::{
    AuthoritativeNamespaceV1 as N, ChangelogHistoryStateV3, ChangelogLineageV3, LeadershipEpochV1,
    ProjectionApplyRowObservation, ProjectionApplySnapshotReader, ProjectionGenerationPosition,
    ReplicationFollowerStateV3,
};

fn setup(path: &std::path::Path) -> (ChangelogHistoryStateV3, StoredProjectionControlV1) {
    let mut store =
        RedbStore::open_with_commit_profile(path, crate::RedbCommitProfile::Hardened).unwrap();
    store.initialize_database(database_id()).unwrap();
    let ports = crate::store::RedbDormantPorts {
        pending_v3_activation: None,
        shared: store.shared,
    }
    .into_operational_after_catalog_validation()
    .unwrap();
    for n in 1..=2 {
        seed_command(&ports, CommitSequence::new(n).unwrap(), 0);
    }
    let schema = recovery_projection_schema();
    let control = StoredProjectionControlV1::new(
        schema.identity().clone(),
        ProjectionGeneration::first(),
        None,
        Some(ProjectionGenerationPosition::new(
            ProjectionGeneration::first(),
            FrontierPosition::AppliedThrough(CommitSequence::new(2).unwrap()),
        )),
        None,
        ProjectionLifecycleV1::CatchingUp,
        None,
    )
    .unwrap();
    install_control(&ports, schema.identity(), &control);
    install_control_on_primary(&ports, schema.identity(), &control);
    let history = crate::changelog_v3_activation::activate_validated(
        ports.shared.database.begin_write().unwrap(),
        ChangelogLineageV3::new(database_id(), 1, LeadershipEpochV1::initial()).unwrap(),
        riffdb_types::DualFrontier::new(Some(CommitSequence::new(2).unwrap()), None),
    )
    .unwrap();
    let write = ports.shared.database.begin_write().unwrap();
    write
        .open_table(META)
        .unwrap()
        .insert(
            N::ReplicationFollowerState.metadata_key().unwrap(),
            riffdb_storage_api::proto_codec::encode_replication_follower_state_v3(
                ReplicationFollowerStateV3::attached(history.lineage(), history.tail(), None)
                    .unwrap(),
            )
            .unwrap()
            .as_bytes(),
        )
        .unwrap();
    write
        .open_table(crate::changelog_v3_activation::HISTORY)
        .unwrap()
        .retain(|_, _| false)
        .unwrap();
    ports.shared.commit_durable(write).unwrap();
    (history, control)
}

fn open(path: &std::path::Path) -> crate::RedbFollowerApplier {
    crate::RedbFollowerApplier::from_isolated_test_store(
        crate::RedbFollowerStore::open(path).unwrap(),
    )
    .unwrap()
}

fn authority(path: &std::path::Path) -> std::collections::BTreeMap<(N, Vec<u8>), Vec<u8>> {
    let database = redb::ReadOnlyDatabase::open(path).unwrap();
    let read = database.begin_read().unwrap();
    let mut result = std::collections::BTreeMap::new();
    for namespace in N::ALL {
        if namespace.class() == riffdb_storage_api::ReplicationAuthorityClassV1::RebuildableLocal {
            continue;
        }
        if let Some(key) = namespace.metadata_key() {
            if let Some(value) = read.open_table(META).unwrap().get(key).unwrap() {
                result.insert((namespace, key.as_bytes().to_vec()), value.value().to_vec());
            }
        } else {
            let table = read
                .open_table(redb::TableDefinition::<&[u8], &[u8]>::new(
                    namespace.table(),
                ))
                .unwrap();
            for row in table.iter().unwrap() {
                let (key, value) = row.unwrap();
                result.insert((namespace, key.value().to_vec()), value.value().to_vec());
            }
        }
    }
    result
}

fn request(n: u64, count: u64) -> ProjectionApplyRequestV1 {
    let schema = recovery_projection_schema();
    let generation = ProjectionGeneration::first();
    let key = schema
        .group_key(generation, &[CanonicalValue::string("group-a").unwrap()])
        .unwrap();
    ProjectionApplyRequestV1::new(
        schema.clone(),
        generation,
        CommitSequence::new(n).unwrap(),
        if n == 1 {
            FrontierPosition::BeforeFirst
        } else {
            FrontierPosition::AppliedThrough(CommitSequence::new(n - 1).unwrap())
        },
        vec![
            ProjectionRowUpdateV1::new(
                &schema,
                key,
                if n == 1 {
                    ProjectionRowPrior::Absent
                } else {
                    ProjectionRowPrior::Present(CommitSequence::new(n - 1).unwrap())
                },
                recovery_measures(count),
            )
            .unwrap(),
        ],
    )
    .unwrap()
}

fn observe(applier: &crate::RedbFollowerApplier, n: u64, count: u64) {
    let schema = recovery_projection_schema();
    let key = schema
        .group_key(
            ProjectionGeneration::first(),
            &[CanonicalValue::string("group-a").unwrap()],
        )
        .unwrap();
    let snapshot = applier
        .read_apply_snapshot(
            &ProjectionApplySnapshotRequest::new(schema, ProjectionGeneration::first(), vec![key])
                .unwrap(),
        )
        .unwrap();
    assert_eq!(
        snapshot.expected_frontier(),
        if n == 0 {
            FrontierPosition::BeforeFirst
        } else {
            FrontierPosition::AppliedThrough(CommitSequence::new(n).unwrap())
        }
    );
    if n == 0 {
        assert!(matches!(
            snapshot.rows(),
            [ProjectionApplyRowObservation::Absent(_)]
        ));
    } else {
        let [ProjectionApplyRowObservation::Present(row)] = snapshot.rows() else {
            panic!("missing replayed row")
        };
        assert_eq!(row.measures(), &recovery_measures(count));
    }
}

#[test]
fn follower_projection_replay_is_durable_idempotent_and_preserves_source_authority() {
    let path = TestDatabasePath::new("follower-projection-replay");
    let (history, control) = setup(&path.0);
    let before = authority(&path.0);
    let mut applier = open(&path.0);
    observe(&applier, 0, 0);
    assert_eq!(applier.local_commit_epoch_for_test(), 0);
    let first = applier
        .rebuild_projection_commit(&control, &request(1, 1))
        .unwrap();
    assert_eq!(
        applier
            .rebuild_projection_commit(&control, &request(1, 1))
            .unwrap(),
        first
    );
    assert_eq!(applier.local_commit_epoch_for_test(), 1);
    assert_eq!(applier.durable_history().unwrap(), history);
    drop(applier);
    assert_eq!(authority(&path.0), before);
    let mut applier = open(&path.0);
    observe(&applier, 1, 1);
    applier
        .rebuild_projection_commit(&control, &request(2, 3))
        .unwrap();
    observe(&applier, 2, 3);
    assert_eq!(applier.durable_history().unwrap(), history);
    assert_eq!(applier.local_commit_epoch_for_test(), 1);
    drop(applier);
    assert_eq!(authority(&path.0), before);
}

#[test]
fn follower_projection_refusals_preserve_rows_and_fuse_the_writer() {
    for violation in ["gap", "control", "retry", "frontier", "prior", "generation"] {
        let path = TestDatabasePath::new("follower-projection-refusal");
        let (_, expected) = setup(&path.0);
        let before = authority(&path.0);
        let mut applier = open(&path.0);
        let mut wrong = request(1, 1);
        let mut control = expected.clone();
        let already_applied = matches!(violation, "retry" | "prior");
        if already_applied {
            applier
                .rebuild_projection_commit(&expected, &request(1, 1))
                .unwrap();
        }
        match violation {
            "gap" => wrong = request(2, 3),
            "control" => {
                control = StoredProjectionControlV1::new(
                    expected.identity().clone(),
                    ProjectionGeneration::first(),
                    None,
                    Some(ProjectionGenerationPosition::new(
                        ProjectionGeneration::first(),
                        FrontierPosition::BeforeFirst,
                    )),
                    None,
                    ProjectionLifecycleV1::CatchingUp,
                    None,
                )
                .unwrap();
            }
            "retry" => wrong = request(1, 999),
            "frontier" => wrong = request(3, 4),
            "prior" => {
                wrong = ProjectionApplyRequestV1::new(
                    wrong.schema().clone(),
                    wrong.generation(),
                    CommitSequence::new(2).unwrap(),
                    FrontierPosition::AppliedThrough(CommitSequence::first()),
                    vec![
                        ProjectionRowUpdateV1::new(
                            wrong.schema(),
                            wrong.row_updates()[0].key().clone(),
                            ProjectionRowPrior::Absent,
                            recovery_measures(999),
                        )
                        .unwrap(),
                    ],
                )
                .unwrap();
            }
            "generation" => {
                wrong = ProjectionApplyRequestV1::new(
                    wrong.schema().clone(),
                    ProjectionGeneration::new(2).unwrap(),
                    CommitSequence::first(),
                    FrontierPosition::BeforeFirst,
                    vec![],
                )
                .unwrap();
            }
            _ => unreachable!(),
        }
        let epoch = applier.local_commit_epoch_for_test();
        assert_eq!(
            applier
                .rebuild_projection_commit(&control, &wrong)
                .unwrap_err()
                .kind(),
            StorageErrorKind::CorruptData,
            "{violation}"
        );
        assert_eq!(applier.local_commit_epoch_for_test(), epoch);
        assert!(applier.resume_stream().is_err(), "fused after {violation}");
        assert!(
            applier
                .rebuild_projection_commit(&expected, &request(1, 1))
                .is_err()
        );
        let snapshot_request = ProjectionApplySnapshotRequest::new(
            wrong.schema().clone(),
            ProjectionGeneration::first(),
            vec![],
        )
        .unwrap();
        assert!(applier.read_apply_snapshot(&snapshot_request).is_err());
        drop(applier);
        assert_eq!(authority(&path.0), before, "{violation}");
        let mut applier = open(&path.0);
        observe(
            &applier,
            u64::from(already_applied),
            u64::from(already_applied),
        );
        applier
            .rebuild_projection_commit(&expected, &request(1, 1))
            .unwrap();
        applier
            .rebuild_projection_commit(&expected, &request(2, 3))
            .unwrap();
        observe(&applier, 2, 3);
    }
}

#[test]
fn follower_projection_process_child() {
    let Some(path) = std::env::var_os("RIFFDB_FOLLOWER_PROJECTION_DATABASE") else {
        return;
    };
    let path = std::path::Path::new(&path);
    let database = redb::ReadOnlyDatabase::open(path).unwrap();
    let read = database.begin_read().unwrap();
    let table = read.open_table(PROJECTION_FRONTIER).unwrap();
    let (_, value) = table.iter().unwrap().next().unwrap().unwrap();
    let control = crate::codec::decode_projection_control_v1(value.value())
        .unwrap()
        .into_parts()
        .0;
    drop(value);
    drop(table);
    drop(read);
    drop(database);
    open(path)
        .rebuild_projection_commit(&control, &request(1, 1))
        .unwrap();
    panic!("requested projection crash did not fire");
}

#[test]
fn follower_projection_crashes_keep_rows_and_markers_atomic_without_authoritative_writes() {
    for edge in ["projection-staged", "projection-committed"] {
        let path = TestDatabasePath::new("follower-projection-crash");
        let (_, control) = setup(&path.0);
        let before = authority(&path.0);
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "derived::tests::follower_projection::follower_projection_process_child",
                "--nocapture",
            ])
            .env("RIFFDB_FOLLOWER_PROJECTION_DATABASE", &path.0)
            .env("RIFFDB_FOLLOWER_APPLY_EDGE", edge)
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(93), "{edge}");
        // Opening the writable engine performs redb crash recovery before inspection.
        let mut applier = open(&path.0);
        let committed = edge == "projection-committed";
        observe(&applier, u64::from(committed), u64::from(committed));
        applier
            .rebuild_projection_commit(&control, &request(1, 1))
            .unwrap();
        assert_eq!(applier.local_commit_epoch_for_test(), u64::from(!committed));
        observe(&applier, 1, 1);
        drop(applier);
        assert_eq!(authority(&path.0), before, "{edge}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn follower_projection_preflight_rejects_incomplete_history_without_writing() {
        use riffdb_storage_api::{
            EvidencePageLimit, StructuralEvidenceCursor, StructuralEvidenceSession,
        };
        let path = TestDatabasePath::new("follower-projection-recovery");
        let (_, _) = setup(&path.0);
        let before = authority(&path.0);
        let key =
            riffdb_storage_api::ReadableDigestKey::v1(riffdb_types::DigestKeyId::new(1).unwrap());
        let inputs = riffdb_storage_api::StartupValidationInputs::new(
            Timestamp::new(1000, 0).unwrap(),
            riffdb_storage_api::ReadableCapabilityDigestInventory::new(vec![key]).unwrap(),
            riffdb_storage_api::ReadableIdempotencyDigestInventory::new(vec![key]).unwrap(),
        );
        let mut session = crate::RedbFollowerStore::open(&path.0)
            .unwrap()
            .begin_projection_recovery(
                inputs.clone(),
                std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            )
            .unwrap();
        let id = session.open_session_id();
        assert!(
            session
                .read_structural_evidence(
                    StructuralEvidenceCursor::start(session.database_id(), id),
                    EvidencePageLimit::new(1).unwrap(),
                )
                .is_err(),
            "historical preflight cannot grant structural completion"
        );
        assert!(
            riffdb_catalog::validate_catalog_history(&mut session).is_err(),
            "isolated fixture has no valid historical command bundle"
        );
        assert!(
            crate::RedbFollowerStore::open(&path.0).is_err(),
            "preflight retains engine custody"
        );
        drop(session);
        assert_eq!(authority(&path.0), before);
        let applier = open(&path.0);
        observe(&applier, 0, 0);
    }
}
