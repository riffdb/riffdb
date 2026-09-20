//! ADR-0240 obligation proofs and crash/restart coverage for derived state.
use super::*;
use riffdb_storage_api::{
    ProjectionApplySnapshotRequest, ProjectionGenerationPosition, ProjectionQuerySelector,
    ProjectionRowUpdateV1,
};

fn catching_up(schema: &riffdb_storage_api::CheckedProjectionSchema) -> StoredProjectionControlV1 {
    StoredProjectionControlV1::new(
        schema.identity().clone(),
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
    .unwrap()
}

fn ready_through(
    schema: &riffdb_storage_api::CheckedProjectionSchema,
    sequence: CommitSequence,
) -> StoredProjectionControlV1 {
    StoredProjectionControlV1::new(
        schema.identity().clone(),
        ProjectionGeneration::first(),
        Some(ProjectionGenerationPosition::new(
            ProjectionGeneration::first(),
            FrontierPosition::AppliedThrough(sequence),
        )),
        None,
        Some(riffdb_storage_api::PublishedApplyModeV1::Enabled),
        ProjectionLifecycleV1::Ready,
        None,
    )
    .unwrap()
}

fn query(schema: &riffdb_storage_api::CheckedProjectionSchema) -> ProjectionQueryRequest {
    let selector = ProjectionQuerySelector::new(schema.clone(), Vec::new()).unwrap();
    ProjectionQueryRequest::new(selector, NonZeroU16::MIN, None).unwrap()
}

fn plant_ahead_control(
    ports: &RedbOperationalPorts,
    schema: &riffdb_storage_api::CheckedProjectionSchema,
) {
    let ahead = ready_through(schema, CommitSequence::new(99).unwrap());
    install_control(ports, schema.identity(), &ahead);
}

#[test]
fn derived_state_disagreeing_with_the_primary_is_rebuilt_not_trusted() {
    let (_path, mut ports) = operational("derived-disagreement");
    let schema = recovery_projection_schema();
    seed_command(&ports, CommitSequence::first(), 0);
    install_control(&ports, schema.identity(), &catching_up(&schema));
    let key = schema
        .group_key(
            ProjectionGeneration::first(),
            &[CanonicalValue::string("group-a").unwrap()],
        )
        .unwrap();
    let request = ProjectionApplyRequestV1::new(
        schema.clone(),
        ProjectionGeneration::first(),
        CommitSequence::first(),
        FrontierPosition::BeforeFirst,
        vec![
            ProjectionRowUpdateV1::new(
                &schema,
                key.clone(),
                ProjectionRowPrior::Absent,
                recovery_measures(1),
            )
            .unwrap(),
        ],
    )
    .unwrap();
    assert!(matches!(
        ports.apply_projection(&request).unwrap(),
        ProjectionApplyResult::Applied { .. }
    ));
    plant_ahead_control(&ports, &schema);
    assert!(
        !matches!(
            ports.query_projection(&query(&schema)).unwrap(),
            ProjectionQueryResult::Ready { .. }
        ),
        "ahead-of-primary derived rows must not be served"
    );
    let status = ports.read_projection_status(schema.identity()).unwrap();
    assert!(
        status.published().is_none(),
        "disagreeing derived status must not present a published generation"
    );
    seed_command(&ports, CommitSequence::new(2).unwrap(), 0);
    assert_eq!(
        ports.apply_projection(&request).unwrap(),
        ProjectionApplyResult::StateChanged
    );
    assert!(
        !matches!(
            ports.query_projection(&query(&schema)).unwrap(),
            ProjectionQueryResult::Ready { .. }
        ),
        "discarded derived state must not be served"
    );
    install_control(&ports, schema.identity(), &catching_up(&schema));
    assert!(matches!(
        ports.apply_projection(&request).unwrap(),
        ProjectionApplyResult::Applied { .. }
    ));
}

#[test]
fn derived_state_stale_torn_absent_or_ahead_after_restart_is_rebuilt() {
    for kind in ["stale", "torn", "absent", "ahead"] {
        let (path, mut ports) = operational(&format!("derived-restart-{kind}"));
        let schema = recovery_projection_schema();
        seed_command(&ports, CommitSequence::first(), 0);
        install_control(&ports, schema.identity(), &catching_up(&schema));
        let key = schema
            .group_key(
                ProjectionGeneration::first(),
                &[CanonicalValue::string("group-a").unwrap()],
            )
            .unwrap();
        if kind != "absent" {
            let request = ProjectionApplyRequestV1::new(
                schema.clone(),
                ProjectionGeneration::first(),
                CommitSequence::first(),
                FrontierPosition::BeforeFirst,
                vec![
                    ProjectionRowUpdateV1::new(
                        &schema,
                        key.clone(),
                        ProjectionRowPrior::Absent,
                        recovery_measures(1),
                    )
                    .unwrap(),
                ],
            )
            .unwrap();
            assert!(matches!(
                ports.apply_projection(&request).unwrap(),
                ProjectionApplyResult::Applied { .. }
            ));
        }
        match kind {
            "ahead" => plant_ahead_control(&ports, &schema),
            "torn" => {
                install_control(
                    &ports,
                    schema.identity(),
                    &ready_through(&schema, CommitSequence::first()),
                );
                let access = ports.begin_derived_write().unwrap();
                access
                    .transaction()
                    .unwrap()
                    .open_table(PROJECTION_APPLIED)
                    .unwrap()
                    .retain(|_, _| false)
                    .unwrap();
                access
                    .commit_for(RedbTestOperation::ProjectionMutation)
                    .unwrap();
            }
            "stale" => {
                seed_command(&ports, CommitSequence::new(2).unwrap(), 0);
            }
            "absent" => {}
            _ => unreachable!(),
        }
        drop(ports);
        let store = RedbStore::open(&path.0).unwrap();
        let ports = crate::store::RedbDormantPorts {
            pending_v3_activation: None,
            shared: store.shared,
        }
        .into_operational_after_catalog_validation()
        .unwrap();
        let result = ports.query_projection(&query(&schema)).unwrap();
        if kind == "stale" {
            assert!(
                matches!(result, ProjectionQueryResult::Degraded { .. }),
                "{kind} may lag but must not invent commits"
            );
        } else {
            assert!(
                !matches!(result, ProjectionQueryResult::Ready { .. }),
                "{kind} must not be served as ready after restart"
            );
        }
        let snapshot = ProjectionApplySnapshotRequest::new(
            schema.clone(),
            ProjectionGeneration::first(),
            vec![key],
        )
        .unwrap();
        if kind == "ahead" {
            assert!(
                ports.read_apply_snapshot(&snapshot).is_err()
                    || !matches!(
                        ports.query_projection(&query(&schema)).unwrap(),
                        ProjectionQueryResult::Ready { .. }
                    )
            );
        }
    }
}

#[test]
fn declaring_a_projection_costs_no_write_throughput() {
    let (_path, mut ports) = operational("derived-throughput");
    let schema = recovery_projection_schema();
    install_control(&ports, schema.identity(), &catching_up(&schema));
    for value in 1..=64 {
        seed_command(&ports, CommitSequence::new(value).unwrap(), 0);
    }
    let before = ports.mutation_gate_tickets();
    let mut control = catching_up(&schema);
    for first in (1..=64).step_by(16) {
        let last = first + 15;
        let mut frontier = control.frontier_for(ProjectionGeneration::first()).unwrap();
        let mut members = Vec::new();
        for value in first..=last {
            let sequence = CommitSequence::new(value).unwrap();
            members.push(
                ProjectionApplyRequestV1::new(
                    schema.clone(),
                    ProjectionGeneration::first(),
                    sequence,
                    frontier,
                    vec![],
                )
                .unwrap(),
            );
            frontier = FrontierPosition::AppliedThrough(sequence);
        }
        let request =
            riffdb_storage_api::ProjectionApplyBatchV1::new(control.clone(), members, vec![])
                .unwrap();
        let riffdb_storage_api::ProjectionApplyBatchResult::Applied(next) =
            ports.apply_projection_batch(&request).unwrap()
        else {
            panic!("batch applies without taking the primary gate");
        };
        control = next;
    }
    assert_eq!(
        ports.mutation_gate_tickets(),
        before,
        "declaring and applying a projection must not acquire the primary mutation gate"
    );

    let (done_tx, done_rx) = std::sync::mpsc::channel();
    std::thread::scope(|scope| {
        let held = ports
            .begin_derived_write()
            .expect("hold sidecar write while a command commits");
        scope.spawn(|| {
            seed_command(&ports, CommitSequence::new(97).unwrap(), 0);
            done_tx.send(()).expect("primary write finished");
        });
        done_rx
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("primary write must complete while a sidecar write is held");
        held.abort().expect("release held sidecar write");
    });

    let start = std::time::Instant::now();
    for value in 65..=96 {
        seed_command(&ports, CommitSequence::new(value).unwrap(), 0);
    }
    let with_projection = start.elapsed();

    let (_base_path, base) = operational("derived-throughput-base");
    let start = std::time::Instant::now();
    for value in 1..=32 {
        seed_command(&base, CommitSequence::new(value).unwrap(), 0);
    }
    let without_projection = start.elapsed();
    let slower = with_projection.as_secs_f64() / without_projection.as_secs_f64().max(1e-9);
    assert!(
        slower < 2.0,
        "command writes with an active projection apply must stay within run-to-run spread of baseline, got ratio {slower:.3} ({with_projection:?} vs {without_projection:?})"
    );
}
