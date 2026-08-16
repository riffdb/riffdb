//! Read-only installation-diff semantic corpus.

use riffdb_application::{
    ApplicationInstallationDiff, ApplicationInstallationPlan, InstallationArtifact,
    InstallationArtifactAction, InstallationContract, InstallationContractAction,
    InstallationCredentialAction, InstallationFeature, InstallationLifecycleState,
    InstallationPlanErrorKind, InstallationRemoteState, ObservedCredentialDestination,
    ObservedInstallationRole, QuerySecretOutput, RoleAuthorityAtom, RoleAuthoritySet,
    RoleOperation, RoleOperationKind,
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

fn symbol(value: &str) -> riffdb_application::InstallationSymbol {
    riffdb_application::InstallationSymbol::new(value).expect("symbol")
}

#[test]
fn initial_secret_role_diff_displays_the_complete_confirmed_authority() {
    let mut input = plan().input().clone();
    let query = RoleOperation::new(RoleOperationKind::Query, symbol("GetSession"));
    let output = QuerySecretOutput::new(
        symbol("GetSession"),
        symbol("Session"),
        symbol("token_hash"),
    );
    input.roles = vec![
        riffdb_application::InstallationRole::new_with_authority(
            symbol("SecretRole"),
            ApplicationRoleHash::from_bytes(hash32(44)),
            None,
            RoleAuthoritySet::new(vec![query.clone()], vec![output.clone()]).expect("authority"),
            RoleAuthoritySet::new(vec![], vec![]).expect("empty authority"),
            None,
        )
        .expect("initial role"),
    ];
    input.credential_destinations.clear();
    let plan = ApplicationInstallationPlan::compile(input).expect("plan");
    let remote = InstallationRemoteState::new(
        plan.input().target.clone(),
        InstallationLifecycleState::Ready,
        None,
        vec![],
        vec![],
        vec![],
        plan.input().required_features.clone(),
    )
    .expect("remote");
    let diff = ApplicationInstallationDiff::compile(&plan, &remote).expect("diff");
    assert_eq!(
        diff.roles()[0].additions(),
        &[
            RoleAuthorityAtom::Operation(query),
            RoleAuthorityAtom::QuerySecretOutput(output),
        ]
    );
    assert!(
        diff.executable(),
        "initial authority is bound by plan confirmation"
    );
}

#[test]
fn approved_secret_successor_requires_feature_and_exact_credential_rotation() {
    let plan = ApplicationInstallationPlan::decode_canonical(include_bytes!(
        "../../../fixtures/installation/application-installation-plan-v3.json"
    ))
    .expect("v3 plan");
    let input = plan.input();
    let role = &input.roles[0];
    let observed_role = ObservedInstallationRole::new_with_authority(
        role.name().clone(),
        role.previous_role_hash().expect("predecessor"),
        role.previous_authority().clone(),
    )
    .expect("observed role");
    let remote = InstallationRemoteState::new(
        input.target.clone(),
        InstallationLifecycleState::Ready,
        Some(input.contract),
        input.artifacts.clone(),
        vec![observed_role.clone()],
        vec![ObservedCredentialDestination::new(
            input.credential_destinations[0].name().clone(),
            input.credential_destinations[0]
                .expected_current()
                .expect("credential predecessor"),
        )],
        input.required_features.clone(),
    )
    .expect("remote");
    let diff = ApplicationInstallationDiff::compile(&plan, &remote).expect("diff");
    assert!(diff.executable());
    assert!(diff.roles()[0].widening_approved());
    assert_eq!(
        diff.roles()[0].additions(),
        &[RoleAuthorityAtom::QuerySecretOutput(
            role.desired_secret_outputs()[0].clone()
        )]
    );
    assert!(matches!(
        diff.credentials(),
        [InstallationCredentialAction::Rotate(_)]
    ));

    let unsupported = InstallationRemoteState::new(
        input.target.clone(),
        InstallationLifecycleState::Ready,
        Some(input.contract),
        input.artifacts.clone(),
        vec![observed_role],
        vec![ObservedCredentialDestination::new(
            input.credential_destinations[0].name().clone(),
            input.credential_destinations[0]
                .expected_current()
                .expect("credential predecessor"),
        )],
        vec![InstallationFeature::InstallationCampaigns],
    )
    .expect("unsupported remote");
    let diff = ApplicationInstallationDiff::compile(&plan, &unsupported).expect("diff");
    assert_eq!(
        diff.missing_features(),
        &[InstallationFeature::QuerySecretOutputs]
    );
    assert!(!diff.executable());
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
