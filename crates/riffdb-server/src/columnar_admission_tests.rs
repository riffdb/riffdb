//! Bounded readonly admission and source/follower artifact separation.
use super::tests::{ADAPTER_BOARD_CONTRACT, board_runtime, production_vector_runtime};
use super::*;
use riffdb_storage_api::ColumnarProjectionArtifactV1;
use riffdb_types::ContractLineage;

// req: REP-004, PRJ-005, PRJ-006, PRJ-007, PRJ-008, PRJ-009
#[test]
fn follower_columnar_admission_matches_primary_without_source_artifact_authority() {
    use riffdb_storage_api::ColumnarProjectionFailureReasonV1;
    for (runtime, scope) in [
        board_runtime("follower-admission-scalar"),
        production_vector_runtime("follower-admission-vector"),
    ] {
        let configured = if runtime.control_bindings()[0].is_vector {
            Vec::new()
        } else {
            vec![ConfiguredProjection::for_test(
                "ticket_board",
                "Ticket",
                &["status", "title"],
                "organization_id",
            )]
        };
        let pin = runtime.storage().projection_reads().pin().unwrap();
        let active = ActiveCatalogSnapshot::read(&pin).unwrap().unwrap();
        let bindings =
            resolve_columnar_bindings(&configured, Some(active.bundle().bundle())).unwrap();
        assert_eq!(bindings.len(), runtime.control_bindings().len());
        for binding in &bindings {
            let primary = runtime.control_binding(binding.name()).unwrap();
            assert_eq!(binding.spec(), primary.spec());
            assert_eq!(
                binding.definition().fingerprint(),
                primary.definition().fingerprint()
            );
            assert_eq!(binding.is_vector, primary.is_vector);
        }
        let controls = pin.read_columnar_projection_controls().unwrap();
        assert!(controls[0].servable_generation().is_none());
        validate_follower_columnar_controls(&bindings, &controls, 1, FrontierPosition::BeforeFirst)
            .unwrap();
        let failed = controls[0]
            .clone()
            .record_candidate_failure(ColumnarProjectionFailureReasonV1::ArtifactInvalid)
            .unwrap();
        validate_follower_columnar_controls(&bindings, &[failed], 1, FrontierPosition::BeforeFirst)
            .unwrap();
        assert!(
            validate_follower_columnar_controls(&bindings, &[], 1, FrontierPosition::BeforeFirst)
                .is_err()
        );
        assert!(
            validate_follower_columnar_controls(
                &bindings,
                &controls,
                2,
                FrontierPosition::BeforeFirst
            )
            .is_err()
        );
        assert!(
            validate_follower_columnar_controls(
                &bindings,
                &controls,
                0,
                FrontierPosition::BeforeFirst
            )
            .is_err()
        );
        assert_eq!(pin.read_columnar_projection_controls().unwrap(), controls);
        assert_eq!(
            runtime
                .storage()
                .projection_reads()
                .pin()
                .unwrap()
                .read_columnar_projection_controls()
                .unwrap(),
            controls
        );
        assert_eq!(
            std::fs::read_dir(scope.path().join("projections"))
                .unwrap()
                .count(),
            0
        );
    }
}

// req: REP-004, PRJ-005, PRJ-009
#[test]
fn follower_columnar_admission_refuses_foreign_mismatched_and_excessive_controls() {
    let bundle = riffdb_contract_compiler::compile_contract_source(ADAPTER_BOARD_CONTRACT).unwrap();
    let configured = ConfiguredProjection::for_test(
        "ticket_board",
        "Ticket",
        &["status", "title"],
        "organization_id",
    );
    let bindings =
        resolve_columnar_bindings(std::slice::from_ref(&configured), Some(&bundle)).unwrap();
    let spec = bindings[0].spec();
    let make = |source, hash, limits| {
        FreshColumnarProjectionControlV1::new(
            source,
            spec.definition_fingerprint(),
            hash,
            limits,
            1,
        )
        .unwrap()
        .control()
        .clone()
    };
    let good = make(spec.source().clone(), spec.hash(), spec.replay_limits());
    let foreign = make(
        riffdb_types::ColumnarProjectionSourceV1::scalar(
            ContractLineage::new("ForeignBoard").unwrap(),
            spec.definition_fingerprint(),
        ),
        spec.hash(),
        spec.replay_limits(),
    );
    let hash = make(
        spec.source().clone(),
        riffdb_types::ColumnarProjectionSpecHashV1::from_bytes([0x71; 32]),
        spec.replay_limits(),
    );
    let limits = make(
        spec.source().clone(),
        spec.hash(),
        riffdb_types::ColumnarProjectionReplayLimitsV1::new(1, 1, 1).unwrap(),
    );
    for controls in [
        vec![foreign],
        vec![hash],
        vec![limits],
        vec![good.clone(); 2],
        vec![good; 257],
    ] {
        assert!(
            validate_follower_columnar_controls(
                &bindings,
                &controls,
                1,
                FrontierPosition::BeforeFirst
            )
            .is_err()
        );
    }
    let alias =
        ConfiguredProjection::for_test("alias", "Ticket", &["status", "title"], "organization_id");
    assert!(resolve_columnar_bindings(&[configured.clone(), alias], Some(&bundle)).is_err());
    assert!(resolve_columnar_bindings(&vec![configured; 257], Some(&bundle)).is_err());
    assert!(resolve_columnar_bindings(&[], None).unwrap().is_empty());
}

// req: REP-004, PRJ-005, PRJ-008, PRJ-009
#[test]
fn follower_columnar_admission_checks_source_frontier_and_v2_identity_without_selecting_it() {
    use riffdb_storage_api::{ColumnarProjectionGenerationRoleV1, ColumnarProjectionLifecycleV1};
    let bundle = riffdb_contract_compiler::compile_contract_source(ADAPTER_BOARD_CONTRACT).unwrap();
    let projection = ConfiguredProjection::for_test(
        "ticket_board",
        "Ticket",
        &["status", "title"],
        "organization_id",
    );
    let bindings = resolve_columnar_bindings(&[projection], Some(&bundle)).unwrap();
    let spec = bindings[0].spec();
    let head = FrontierPosition::AppliedThrough(CommitSequence::new(10).unwrap());
    let make = |physical| {
        let published = StoredColumnarProjectionGenerationV1::selected(
            ProjectionGeneration::first(),
            ColumnarProjectionLayoutV1::V2,
            head,
            1,
            ColumnarProjectionArtifactV1::new(100, [0x31; 32]).unwrap(),
            spec.definition_fingerprint(),
            spec.hash(),
            Some(physical),
            ColumnarProjectionGenerationRoleV1::Published,
        )
        .unwrap();
        StoredColumnarProjectionControlV1::new(
            spec.source().clone(),
            spec.definition_fingerprint(),
            spec.hash(),
            ProjectionGeneration::first(),
            Some(published),
            None,
            None,
            ColumnarProjectionLifecycleV1::Ready,
            None,
            spec.replay_limits(),
        )
        .unwrap()
    };
    let physical =
        *PhysicalGenerationFingerprintV1::compute(spec.definition_fingerprint()).as_bytes();
    let good = make(physical);
    let selected_failure = good.clone().record_published_failure().unwrap();
    assert!(selected_failure.servable_generation().is_none());
    for control in [good.clone(), selected_failure] {
        validate_follower_columnar_controls(&bindings, std::slice::from_ref(&control), 1, head)
            .unwrap();
        assert!(
            validate_follower_columnar_controls(
                &bindings,
                &[control],
                1,
                FrontierPosition::BeforeFirst
            )
            .is_err()
        );
    }
    assert!(validate_follower_columnar_controls(&bindings, &[make([0x42; 32])], 1, head).is_err());
    // Names resolve to the same source; a local name never changes source bytes.
    let renamed = ConfiguredProjection::for_test(
        "renamed",
        "Ticket",
        &["status", "title"],
        "organization_id",
    );
    let renamed = resolve_columnar_bindings(&[renamed], Some(&bundle)).unwrap();
    validate_follower_columnar_controls(&renamed, &[good], 1, head).unwrap();
}
