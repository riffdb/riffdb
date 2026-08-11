//! Read-only installation-diff semantic corpus.

use riffdb_application::{
    ApplicationInstallationDiff, ApplicationInstallationPlan, InstallationArtifact,
    InstallationArtifactAction, InstallationContract, InstallationContractAction,
    InstallationCredentialAction, InstallationFeature, InstallationLifecycleState,
    InstallationPlanErrorKind, InstallationRemoteState, ObservedCredentialDestination,
    ObservedInstallationRole, RoleOperationKind,
};
use riffdb_types::{
    ApplicationRoleHash, CapabilityId, ContractBundleHash, ContractLineage, ContractVersion,
    DatabaseAlias, Environment, GeneratedArtifactHash,
};

fn hash32(byte: u8) -> [u8; 32] {
    [byte; 32]
}

fn capability(last: u8) -> CapabilityId {
    CapabilityId::from_bytes([0, 0, 0, 0, 0, 1, 0x70, 0, 0x80, 0, 0, 0, 0, 0, 0, last])
        .expect("valid UUIDv7")
}

fn plan() -> ApplicationInstallationPlan {
    ApplicationInstallationPlan::decode_canonical(include_bytes!(
        "../../../fixtures/installation/application-installation-plan-v1.json"
    ))
    .expect("fixture plan")
}

fn exact_remote(plan: &ApplicationInstallationPlan) -> InstallationRemoteState {
    let input = plan.input();
    InstallationRemoteState::new(
        input.target.clone(),
        InstallationLifecycleState::Ready,
        Some(input.contract),
        input.artifacts.clone(),
        vec![
            ObservedInstallationRole::new(
                input.roles[0].name().clone(),
                input.roles[0].role_hash(),
                input.roles[0].desired_operations().to_vec(),
            )
            .expect("role"),
        ],
        vec![ObservedCredentialDestination::new(
            input.credential_destinations[0].name().clone(),
            input.credential_destinations[0].successor(),
        )],
        input.required_features.clone(),
    )
    .expect("remote state")
}

#[test]
fn exact_remote_state_produces_an_executable_noop_diff() {
    let plan = plan();
    let diff = ApplicationInstallationDiff::compile(&plan, &exact_remote(&plan)).expect("diff");
    assert!(diff.executable());
    assert_eq!(diff.contract(), InstallationContractAction::AlreadyExact);
    assert!(diff.missing_features().is_empty());
    assert!(
        diff.artifacts()
            .iter()
            .all(|action| matches!(action, InstallationArtifactAction::AlreadyExact(_)))
    );
    assert!(
        diff.credentials()
            .iter()
            .all(|action| matches!(action, InstallationCredentialAction::AlreadyExact(_)))
    );
    assert!(diff.roles()[0].additions().is_empty());
    assert!(diff.roles()[0].removals().is_empty());
}

#[test]
fn inexact_artifact_role_credential_and_feature_are_all_visible_and_blocking() {
    let plan = plan();
    let input = plan.input();
    let mut artifacts = input.artifacts.clone();
    artifacts[0] = InstallationArtifact::new(
        artifacts[0].kind(),
        artifacts[0].name().clone(),
        GeneratedArtifactHash::from_bytes(hash32(77)),
    );
    let query_only = input.roles[0]
        .desired_operations()
        .iter()
        .filter(|operation| operation.kind() == RoleOperationKind::Query)
        .cloned()
        .collect();
    let remote = InstallationRemoteState::new(
        input.target.clone(),
        InstallationLifecycleState::Ready,
        Some(input.contract),
        artifacts,
        vec![
            ObservedInstallationRole::new(
                input.roles[0].name().clone(),
                ApplicationRoleHash::from_bytes(hash32(88)),
                query_only,
            )
            .expect("role"),
        ],
        vec![ObservedCredentialDestination::new(
            input.credential_destinations[0].name().clone(),
            capability(2),
        )],
        vec![],
    )
    .expect("remote state");
    let diff = ApplicationInstallationDiff::compile(&plan, &remote).expect("diff");
    assert!(!diff.executable());
    assert!(
        diff.artifacts()
            .iter()
            .any(|action| matches!(action, InstallationArtifactAction::IdentityConflict { .. }))
    );
    assert_eq!(
        diff.credentials(),
        &[InstallationCredentialAction::Occupied(
            input.credential_destinations[0].name().clone()
        )]
    );
    assert_eq!(
        diff.missing_features(),
        &[InstallationFeature::InstallationCampaigns]
    );
    assert!(!diff.roles()[0].additions().is_empty());
    assert!(!diff.roles()[0].widening_approved());
}

#[test]
fn target_or_predecessor_substitution_never_becomes_a_deploy() {
    let plan = plan();
    let input = plan.input();
    let wrong_target = InstallationRemoteState::new(
        riffdb_application::InstallationTarget::new(
            DatabaseAlias::new("other").expect("database"),
            Environment::new("dev").expect("environment"),
            ContractLineage::new("Example").expect("lineage"),
        ),
        InstallationLifecycleState::Ready,
        None,
        vec![],
        vec![],
        vec![],
        input.required_features.clone(),
    )
    .expect("remote state");
    assert_eq!(
        ApplicationInstallationDiff::compile(&plan, &wrong_target)
            .expect_err("target substitution")
            .kind(),
        InstallationPlanErrorKind::IdentityMismatch
    );

    let predecessor = InstallationRemoteState::new(
        input.target.clone(),
        InstallationLifecycleState::Ready,
        Some(InstallationContract::new(
            ContractVersion::new(2).expect("version"),
            ContractBundleHash::from_bytes(hash32(99)),
        )),
        vec![],
        vec![],
        vec![],
        input.required_features.clone(),
    )
    .expect("remote state");
    let diff = ApplicationInstallationDiff::compile(&plan, &predecessor).expect("diff");
    assert_eq!(
        diff.contract(),
        InstallationContractAction::StopForMigration
    );
    assert!(!diff.executable());
}
