//! Canonical migration-bundle compatibility tests.

use riffdb_contract_ir::{
    MigrationBundleV1, MigrationResourceBoundsV1, MigrationStepId, MigrationStepKindV1,
    MigrationStepV1,
};
use riffdb_types::{
    ContractBundleHash, ContractLineage, ContractVersion, MigrationSourceHash, ProjectionId,
};

fn fixture() -> MigrationBundleV1 {
    MigrationBundleV1::new(
        "0.1.0",
        ContractLineage::new("LegalSpend").expect("lineage"),
        ContractVersion::new(1).expect("parent version"),
        ContractBundleHash::from_bytes([1; 32]),
        ContractVersion::new(2).expect("candidate version"),
        ContractBundleHash::from_bytes([2; 32]),
        MigrationSourceHash::from_bytes([3; 32]),
        vec![
            MigrationStepV1::new(
                MigrationStepId::new(1).expect("step ID"),
                vec![],
                MigrationStepKindV1::RebuildProjection {
                    projection: ProjectionId::new(7).expect("projection ID"),
                },
            )
            .expect("step"),
        ],
        MigrationResourceBoundsV1::fixed(),
    )
    .expect("bundle")
}

#[test]
fn canonical_bundle_round_trips_with_exact_identity() {
    let bundle = fixture();
    let decoded = MigrationBundleV1::decode(bundle.canonical_bytes()).expect("decode");

    assert_eq!(decoded, bundle);
    assert_eq!(decoded.bundle_hash(), bundle.bundle_hash());
    assert_eq!(decoded.parent_version().get(), 1);
    assert_eq!(decoded.candidate_version().get(), 2);
}

#[test]
fn decoder_rejects_unknown_step_tags() {
    let bundle = fixture();
    let mut bytes = bundle.canonical_bytes().to_vec();
    let tag = bytes
        .iter()
        .position(|byte| *byte == MigrationStepKindV1::REBUILD_PROJECTION_TAG)
        .expect("encoded step tag");
    bytes[tag] = 0xff;

    let error = MigrationBundleV1::decode(&bytes).expect_err("unknown tag");
    assert!(error.to_string().contains("unknown migration step tag"));
}

#[test]
fn dependencies_must_precede_the_step_and_be_canonical() {
    let error = MigrationStepV1::new(
        MigrationStepId::new(2).expect("step ID"),
        vec![MigrationStepId::new(2).expect("dependency")],
        MigrationStepKindV1::RebuildProjection {
            projection: ProjectionId::new(7).expect("projection ID"),
        },
    )
    .expect_err("forward dependency");

    assert!(error.to_string().contains("migration step dependencies"));
}
