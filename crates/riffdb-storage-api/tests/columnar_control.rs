//! Closed schema-bound columnar control semantics.

use riffdb_storage_api::{
    ColumnarProjectionArtifactV1, ColumnarProjectionFailureReasonV1,
    ColumnarProjectionFailureTargetV1, ColumnarProjectionLayoutV1, ColumnarProjectionLifecycleV1,
    ColumnarProjectionReplayLimitsV1, StoredColumnarProjectionControlV1,
    StoredColumnarProjectionFailureV1, StoredColumnarProjectionGenerationV1,
    decode_columnar_projection_control_v1, encode_columnar_projection_control_v1,
};
use riffdb_types::{
    ColumnarProjectionSourceV1, ColumnarProjectionSpecHashV1, CommitSequence, ContractLineage,
    DefinitionFingerprint, FrontierPosition, ProjectionGeneration,
};

// req: PRJ-002, PRJ-004, PRJ-005, PRJ-006, PRJ-007, PRJ-008, PRJ-009, PRJ-010, OQ-020, OQ-024, OQ-053
#[test]
fn columnar_control_lifecycle_shapes_and_retention_inputs_are_closed() {
    let definition = DefinitionFingerprint::from_bytes([0x11; 32]);
    let spec = ColumnarProjectionSpecHashV1::from_bytes([0x22; 32]);
    let limits = limits();
    let initial = StoredColumnarProjectionControlV1::initialize_fresh_v1(
        source(definition),
        definition,
        spec,
        limits,
        1,
    )
    .expect("fresh V1 control");
    assert_shape(
        &initial,
        ColumnarProjectionLifecycleV1::Building,
        Some(FrontierPosition::BeforeFirst),
        false,
    );
    assert_eq!(
        initial.candidate().expect("candidate").layout(),
        ColumnarProjectionLayoutV1::V1
    );
    assert!(initial.candidate().expect("candidate").artifact().is_none());

    let prepared_initial = prepare(initial.clone(), applied(5), applied(7), artifact(700, 0x31));
    assert_shape(
        &prepared_initial,
        ColumnarProjectionLifecycleV1::Building,
        Some(applied(7)),
        false,
    );
    assert!(
        initial
            .clone()
            .retarget_initial_candidate(definition, spec, limits)
            .is_err(),
        "an unchanged target is not drift"
    );
    let changed_initial_spec = ColumnarProjectionSpecHashV1::from_bytes([0x21; 32]);
    let retargeted_initial = prepared_initial
        .clone()
        .retarget_initial_candidate(definition, changed_initial_spec, limits)
        .expect("retarget prepared Initial V1");
    assert_shape(
        &retargeted_initial,
        ColumnarProjectionLifecycleV1::Building,
        Some(FrontierPosition::BeforeFirst),
        false,
    );
    assert_eq!(retargeted_initial.highest_generation().get(), 2);
    assert_eq!(
        retargeted_initial
            .candidate()
            .expect("retargeted Candidate")
            .history_incarnation(),
        1
    );
    assert!(retargeted_initial.published().is_none());
    assert!(retargeted_initial.predecessor().is_none());
    assert!(retargeted_initial.failure().is_none());
    assert!(
        retargeted_initial
            .candidate()
            .expect("retargeted Candidate")
            .artifact()
            .is_none()
    );
    let failed_initial = prepared_initial
        .clone()
        .record_candidate_failure(ColumnarProjectionFailureReasonV1::Storage)
        .expect("failed initial Candidate");
    assert_shape(
        &failed_initial,
        ColumnarProjectionLifecycleV1::Degraded,
        None,
        false,
    );
    let retargeted_failed_initial = failed_initial
        .clone()
        .retarget_initial_candidate(definition, changed_initial_spec, limits)
        .expect("retarget failed Initial V1");
    assert_eq!(
        retargeted_failed_initial.lifecycle(),
        ColumnarProjectionLifecycleV1::Building
    );
    assert_eq!(retargeted_failed_initial.highest_generation().get(), 2);
    assert_eq!(
        retargeted_failed_initial.retention_frontier(),
        Some(FrontierPosition::BeforeFirst)
    );
    assert!(retargeted_failed_initial.failure().is_none());
    assert!(retargeted_failed_initial.published().is_none());
    assert!(retargeted_failed_initial.predecessor().is_none());
    let replacement_initial = failed_initial
        .replace_failed_candidate(ColumnarProjectionLayoutV1::V1, None)
        .expect("replace initial Candidate");
    assert_eq!(
        replacement_initial.lifecycle(),
        ColumnarProjectionLifecycleV1::Building
    );
    assert_eq!(replacement_initial.highest_generation().get(), 2);

    assert!(
        initial
            .publish_prepared_generation(FrontierPosition::BeforeFirst)
            .is_err(),
        "Unprepared cannot publish"
    );
    let ready_v1 = prepared_initial
        .publish_prepared_generation(applied(7))
        .expect("initial publish at current head");
    assert_shape(
        &ready_v1,
        ColumnarProjectionLifecycleV1::Ready,
        Some(applied(7)),
        true,
    );
    assert!(
        ready_v1
            .clone()
            .retarget_initial_candidate(definition, changed_initial_spec, limits)
            .is_err(),
        "published state is not an initial retarget"
    );

    let catching_up = ready_v1
        .clone()
        .begin_v2_candidate([0x41; 32])
        .expect("V1 plus V2 Candidate");
    assert_shape(
        &catching_up,
        ColumnarProjectionLifecycleV1::CatchingUp,
        Some(FrontierPosition::BeforeFirst),
        true,
    );
    let candidate_before = catching_up.candidate().expect("V2 Candidate").clone();
    let advanced_v1 = catching_up
        .advance_published_v1(
            StoredColumnarProjectionGenerationV1::prepared_candidate(
                ProjectionGeneration::first(),
                ColumnarProjectionLayoutV1::V1,
                applied(5),
                applied(8),
                1,
                artifact(800, 0x32),
                definition,
                spec,
                None,
            )
            .expect("V1 witness"),
            applied(9),
        )
        .expect("advance published V1");
    assert_eq!(advanced_v1.candidate(), Some(&candidate_before));
    assert_eq!(
        advanced_v1.published().expect("published").frontier(),
        applied(8)
    );
    let encoded = encode_columnar_projection_control_v1(&advanced_v1).expect("encode tag-67");
    let decoded = decode_columnar_projection_control_v1(encoded.as_bytes()).expect("decode tag-67");
    assert_eq!(decoded.value(), &advanced_v1);
    assert_eq!(
        encode_columnar_projection_control_v1(decoded.value())
            .expect("canonical re-encode")
            .as_bytes(),
        encoded.as_bytes()
    );

    let failed_candidate = advanced_v1
        .clone()
        .record_candidate_failure(ColumnarProjectionFailureReasonV1::ReplayBacklog)
        .expect("Candidate failure with Published");
    assert_shape(
        &failed_candidate,
        ColumnarProjectionLifecycleV1::Degraded,
        Some(applied(8)),
        true,
    );
    let replacement = failed_candidate
        .replace_failed_candidate(ColumnarProjectionLayoutV1::V2, Some([0x42; 32]))
        .expect("replace failed Candidate");
    assert_eq!(
        replacement.lifecycle(),
        ColumnarProjectionLifecycleV1::CatchingUp
    );

    let selected_corruption = replacement
        .record_published_failure()
        .expect("corruption detaches Candidate");
    assert_shape(
        &selected_corruption,
        ColumnarProjectionLifecycleV1::Degraded,
        None,
        false,
    );
    assert!(selected_corruption.published().is_some());
    assert!(selected_corruption.candidate().is_none());
    assert!(
        selected_corruption
            .clone()
            .allocate_same_spec_candidate([0x43; 32])
            .is_err(),
        "selected corruption cannot silently restore serving"
    );
    assert!(
        ready_v1
            .clone()
            .allocate_unservable_rebuild_candidate(
                definition,
                spec,
                limits,
                ColumnarProjectionLayoutV1::V1,
                None,
            )
            .is_err(),
        "healthy same-target state cannot invent an unservable rebuild"
    );

    let corruption_rebuild = selected_corruption
        .allocate_unservable_rebuild_candidate(
            definition,
            spec,
            limits,
            ColumnarProjectionLayoutV1::V1,
            None,
        )
        .expect("same-target corruption rebuild");
    assert_shape(
        &corruption_rebuild,
        ColumnarProjectionLifecycleV1::Rebuilding,
        Some(FrontierPosition::BeforeFirst),
        false,
    );
    assert_eq!(
        corruption_rebuild
            .predecessor()
            .expect("predecessor")
            .spec_hash(),
        spec
    );
    let failed_unservable = corruption_rebuild
        .record_candidate_failure(ColumnarProjectionFailureReasonV1::ResourceLimit)
        .expect("unservable Candidate failure");
    assert_shape(
        &failed_unservable,
        ColumnarProjectionLifecycleV1::Degraded,
        None,
        false,
    );
    assert_eq!(
        failed_unservable
            .replace_failed_candidate(ColumnarProjectionLayoutV1::V1, None)
            .expect("resume unservable rebuild")
            .lifecycle(),
        ColumnarProjectionLifecycleV1::Rebuilding
    );

    let changed_spec = ColumnarProjectionSpecHashV1::from_bytes([0x23; 32]);
    let spec_rebuild = ready_v1
        .clone()
        .allocate_unservable_rebuild_candidate(
            definition,
            changed_spec,
            limits,
            ColumnarProjectionLayoutV1::V1,
            None,
        )
        .expect("spec-change rebuild");
    assert_eq!(spec_rebuild.target_spec_hash(), changed_spec);
    assert_eq!(
        spec_rebuild.predecessor().expect("prior spec").spec_hash(),
        spec
    );
    assert!(spec_rebuild.servable_generation().is_none());

    let ready_v2 = prepare(
        ready_v1
            .begin_v2_candidate([0x51; 32])
            .expect("V2 Candidate"),
        applied(10),
        applied(12),
        artifact(1_200, 0x52),
    )
    .publish_prepared_generation(applied(12))
    .expect("publish V2");
    let rebuilding_v2 = ready_v2
        .clone()
        .allocate_same_spec_candidate([0x53; 32])
        .expect("V2 compaction Candidate");
    assert_shape(
        &rebuilding_v2,
        ColumnarProjectionLifecycleV1::Rebuilding,
        Some(FrontierPosition::BeforeFirst),
        true,
    );
    assert_eq!(
        rebuilding_v2.servable_generation().expect("V2").layout(),
        ColumnarProjectionLayoutV1::V2
    );

    let invalid = ready_v2
        .mark_invalid(ColumnarProjectionFailureReasonV1::Storage)
        .expect("Invalid");
    assert_shape(
        &invalid,
        ColumnarProjectionLifecycleV1::Invalid,
        None,
        false,
    );
    assert!(invalid.published().is_none());

    assert!(
        StoredColumnarProjectionGenerationV1::unprepared_candidate(
            ProjectionGeneration::first(),
            ColumnarProjectionLayoutV1::V1,
            1,
            definition,
            spec,
            Some([0x61; 32]),
        )
        .is_err(),
        "V1 forbids physical fingerprint"
    );
    assert!(
        StoredColumnarProjectionGenerationV1::unprepared_candidate(
            ProjectionGeneration::first(),
            ColumnarProjectionLayoutV1::V2,
            1,
            definition,
            spec,
            None,
        )
        .is_err(),
        "V2 requires physical fingerprint"
    );
    assert!(
        StoredColumnarProjectionFailureV1::new(
            ColumnarProjectionFailureTargetV1::Control,
            ColumnarProjectionFailureReasonV1::Storage,
            Some(ProjectionGeneration::first()),
        )
        .is_err(),
        "Control failure forbids generation"
    );
    assert_eq!(ColumnarProjectionFailureTargetV1::Predecessor.tag(), 3);
}

// req: REC-001, PRJ-004, PRJ-006, PRJ-008, PRJ-009, PRJ-010
#[test]
fn columnar_history_reset_transition_is_exact_and_closed() {
    let definition = DefinitionFingerprint::from_bytes([0x71; 32]);
    let spec = ColumnarProjectionSpecHashV1::from_bytes([0x72; 32]);
    let limits = limits();
    let initial = StoredColumnarProjectionControlV1::initialize_fresh_v1(
        source(definition),
        definition,
        spec,
        limits,
        7,
    )
    .expect("stale initial control");
    assert!(
        initial
            .clone()
            .reset_for_current_history_incarnation(7)
            .is_err(),
        "equal incarnation is not a reset"
    );
    assert!(
        initial
            .clone()
            .reset_for_current_history_incarnation(6)
            .is_err(),
        "future pointers refuse"
    );
    assert!(
        initial
            .clone()
            .reset_for_current_history_incarnation(0)
            .is_err(),
        "zero current incarnation refuses"
    );

    let reset = initial
        .reset_for_current_history_incarnation(8)
        .expect("strictly stale reset");
    assert_eq!(reset.source(), &source(definition));
    assert_eq!(reset.target_definition_fingerprint(), definition);
    assert_eq!(reset.target_spec_hash(), spec);
    assert_eq!(reset.replay_limits(), limits);
    assert_eq!(reset.highest_generation().get(), 2);
    assert_eq!(reset.lifecycle(), ColumnarProjectionLifecycleV1::Building);
    assert!(reset.published().is_none());
    assert!(reset.predecessor().is_none());
    assert!(reset.failure().is_none());
    let candidate = reset.candidate().expect("sole reset candidate");
    assert_eq!(candidate.generation().get(), 2);
    assert_eq!(candidate.layout(), ColumnarProjectionLayoutV1::V1);
    assert_eq!(candidate.history_incarnation(), 8);
    assert_eq!(candidate.frontier(), FrontierPosition::BeforeFirst);
    assert!(candidate.snapshot_frontier().is_none());
    assert!(candidate.artifact().is_none());

    let exhausted_candidate = StoredColumnarProjectionGenerationV1::unprepared_candidate(
        ProjectionGeneration::new(u64::MAX).expect("maximum generation"),
        ColumnarProjectionLayoutV1::V1,
        7,
        definition,
        spec,
        None,
    )
    .expect("exhausted stale candidate");
    let exhausted = StoredColumnarProjectionControlV1::new(
        source(definition),
        definition,
        spec,
        ProjectionGeneration::new(u64::MAX).expect("maximum generation"),
        None,
        Some(exhausted_candidate),
        None,
        ColumnarProjectionLifecycleV1::Building,
        None,
        limits,
    )
    .expect("exhausted control");
    assert!(
        exhausted.reset_for_current_history_incarnation(8).is_err(),
        "generation exhaustion refuses without mutation"
    );
}

fn source(fingerprint: DefinitionFingerprint) -> ColumnarProjectionSourceV1 {
    ColumnarProjectionSourceV1::scalar(
        ContractLineage::new("ColumnarControl").expect("lineage"),
        fingerprint,
    )
}

fn limits() -> ColumnarProjectionReplayLimitsV1 {
    ColumnarProjectionReplayLimitsV1::new(86_400, 1_073_741_824, 100_000).expect("positive limits")
}

fn applied(value: u64) -> FrontierPosition {
    FrontierPosition::AppliedThrough(CommitSequence::new(value).expect("positive sequence"))
}

fn artifact(length: u64, byte: u8) -> ColumnarProjectionArtifactV1 {
    ColumnarProjectionArtifactV1::new(length, [byte; 32]).expect("positive artifact")
}

fn prepare(
    control: StoredColumnarProjectionControlV1,
    snapshot: FrontierPosition,
    frontier: FrontierPosition,
    artifact: ColumnarProjectionArtifactV1,
) -> StoredColumnarProjectionControlV1 {
    let current = control.candidate().expect("Candidate");
    let prepared = StoredColumnarProjectionGenerationV1::prepared_candidate(
        current.generation(),
        current.layout(),
        snapshot,
        frontier,
        current.history_incarnation(),
        artifact,
        current.definition_fingerprint(),
        current.spec_hash(),
        current.physical_generation_fingerprint(),
    )
    .expect("prepared Candidate");
    control
        .record_durable_snapshot(prepared)
        .expect("snapshot CAS")
}

fn assert_shape(
    control: &StoredColumnarProjectionControlV1,
    lifecycle: ColumnarProjectionLifecycleV1,
    retention: Option<FrontierPosition>,
    servable: bool,
) {
    assert_eq!(control.lifecycle(), lifecycle);
    assert_eq!(control.retention_frontier(), retention);
    assert_eq!(control.servable_generation().is_some(), servable);
}
