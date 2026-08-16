//! Exact installation-plan semantic corpus.

use riffdb_application::{
    APPLICATION_INSTALLATION_PLAN_SCHEMA_V3, ApplicationInstallationPlan,
    ApplicationInstallationPlanInput, CredentialDestination, InstallationArtifact,
    InstallationArtifactKind, InstallationContract, InstallationDriver, InstallationFeature,
    InstallationMigration, InstallationPlanErrorKind, InstallationReimport, InstallationRole,
    InstallationSeed, InstallationSymbol, InstallationTarget, QuerySecretOutput, RoleAuthorityAtom,
    RoleAuthoritySet, RoleOperation, RoleOperationKind, RoleWideningApproval,
};
use riffdb_types::{
    ApplicationExportManifestHash, ApplicationExportReceiptHash, ApplicationLockHash,
    ApplicationManifestHash, ApplicationPortabilityManifestHash, ApplicationRoleHash,
    ApplicationSourceHash, CapabilityId, ContractBundleHash, ContractLineage, ContractVersion,
    DatabaseAlias, Environment, GeneratedArtifactHash, MigrationBundleHash,
};

fn hash32(byte: u8) -> [u8; 32] {
    [byte; 32]
}

fn capability(last: u8) -> CapabilityId {
    CapabilityId::from_bytes([0, 0, 0, 0, 0, 1, 0x70, 0, 0x80, 0, 0, 0, 0, 0, 0, last])
        .expect("valid UUIDv7")
}

fn symbol(value: &str) -> InstallationSymbol {
    InstallationSymbol::new(value).expect("valid symbol")
}

fn operation(kind: RoleOperationKind, name: &str) -> RoleOperation {
    RoleOperation::new(kind, symbol(name))
}

fn secret_output(query: &str, entity: &str, field: &str) -> QuerySecretOutput {
    QuerySecretOutput::new(symbol(query), symbol(entity), symbol(field))
}

fn base_input() -> ApplicationInstallationPlanInput {
    let role = InstallationRole::new(
        symbol("AppRole"),
        ApplicationRoleHash::from_bytes(hash32(8)),
        None,
        vec![
            operation(RoleOperationKind::Command, "CreateThing"),
            operation(RoleOperationKind::Query, "GetThing"),
        ],
        vec![],
        None,
    )
    .expect("initial role");
    ApplicationInstallationPlanInput {
        application: symbol("example"),
        source_hash: ApplicationSourceHash::from_bytes(hash32(1)),
        lock_hash: ApplicationLockHash::from_bytes(hash32(2)),
        manifest_hash: ApplicationManifestHash::from_bytes(hash32(3)),
        target: InstallationTarget::new(
            DatabaseAlias::new("app").expect("database"),
            Environment::new("dev").expect("environment"),
            ContractLineage::new("Example").expect("lineage"),
        ),
        contract: InstallationContract::new(
            ContractVersion::new(1).expect("version"),
            ContractBundleHash::from_bytes(hash32(4)),
        ),
        artifacts: vec![
            InstallationArtifact::new(
                InstallationArtifactKind::ContractBundle,
                symbol("contract"),
                GeneratedArtifactHash::from_bytes(hash32(6)),
            ),
            InstallationArtifact::new(
                InstallationArtifactKind::Manifest,
                symbol("manifest"),
                GeneratedArtifactHash::from_bytes(hash32(5)),
            ),
            InstallationArtifact::new(
                InstallationArtifactKind::QueryModule,
                symbol("queries"),
                GeneratedArtifactHash::from_bytes(hash32(7)),
            ),
        ],
        migration: None,
        roles: vec![role],
        reimport: None,
        credential_destinations: vec![
            CredentialDestination::new(
                symbol("app-runtime"),
                symbol("AppRole"),
                None,
                capability(1),
            )
            .expect("credential destination"),
        ],
        drivers: vec![InstallationDriver::Rust, InstallationDriver::TypeScript],
        seeds: vec![
            InstallationSeed::new(
                symbol("initial-data"),
                GeneratedArtifactHash::from_bytes(hash32(9)),
                7,
            )
            .expect("seed"),
        ],
        required_features: vec![InstallationFeature::InstallationCampaigns],
        adapter_manifest_hash: None,
    }
}

#[test]
fn plan_is_order_independent_content_addressed_and_strictly_decodable() {
    let mut reversed = base_input();
    reversed.artifacts.reverse();
    let first = ApplicationInstallationPlan::compile(base_input()).expect("plan");
    assert_eq!(
        first.canonical_bytes(),
        include_bytes!("../../../fixtures/installation/application-installation-plan-v1.json")
    );
    let second = ApplicationInstallationPlan::compile(reversed).expect("same plan");
    assert_eq!(first.identity(), second.identity());
    assert_eq!(first.canonical_bytes(), second.canonical_bytes());
    assert_eq!(
        ApplicationInstallationPlan::decode_canonical(first.canonical_bytes())
            .expect("canonical plan"),
        first
    );

    let pretty = serde_json::to_string_pretty(
        &serde_json::from_slice::<serde_json::Value>(first.canonical_bytes()).expect("json"),
    )
    .expect("pretty json");
    assert_eq!(
        ApplicationInstallationPlan::decode_canonical(pretty.as_bytes())
            .expect_err("noncanonical bytes")
            .kind(),
        InstallationPlanErrorKind::NonCanonical
    );
}

#[test]
fn reimport_plan_rotates_to_v2_and_binds_all_source_authority() {
    let mut input = base_input();
    input.reimport = Some(InstallationReimport::new(
        ApplicationExportManifestHash::from_bytes(hash32(40)),
        ApplicationExportReceiptHash::from_bytes(hash32(41)),
        ApplicationPortabilityManifestHash::from_bytes(hash32(42)),
    ));
    let plan = ApplicationInstallationPlan::compile(input).expect("reimport plan");
    assert_eq!(
        plan.canonical_bytes(),
        include_bytes!("../../../fixtures/installation/application-installation-plan-v2.json")
    );
    let json = std::str::from_utf8(plan.canonical_bytes()).expect("UTF-8");
    assert!(json.contains("riffdb.application-installation-plan/v2"));
    assert!(json.contains(&"28".repeat(32)));
    assert!(json.contains(&"29".repeat(32)));
    assert!(json.contains(&"2a".repeat(32)));
    assert_eq!(
        ApplicationInstallationPlan::decode_canonical(plan.canonical_bytes())
            .expect("canonical v2 plan"),
        plan
    );

    let mut value =
        serde_json::from_slice::<serde_json::Value>(plan.canonical_bytes()).expect("plan JSON");
    value["schema"] =
        serde_json::Value::String("riffdb.application-installation-plan/v1".to_owned());
    assert_eq!(
        ApplicationInstallationPlan::decode_canonical(
            &serde_json::to_vec(&value).expect("tampered plan")
        )
        .expect_err("v1 cannot carry reimport authority")
        .kind(),
        InstallationPlanErrorKind::InvalidShape
    );
}

#[test]
fn successor_role_widening_requires_the_exact_symbolic_diff() {
    let previous_hash = ApplicationRoleHash::from_bytes(hash32(10));
    let desired_hash = ApplicationRoleHash::from_bytes(hash32(11));
    let existing = vec![operation(RoleOperationKind::Query, "GetThing")];
    let desired = vec![
        operation(RoleOperationKind::Query, "GetThing"),
        operation(RoleOperationKind::Command, "CreateThing"),
    ];
    assert_eq!(
        InstallationRole::new(
            symbol("AppRole"),
            desired_hash,
            Some(previous_hash),
            desired.clone(),
            existing.clone(),
            None,
        )
        .expect_err("widening is not implicit")
        .kind(),
        InstallationPlanErrorKind::ApprovalRequired
    );
    let wrong = RoleWideningApproval::new(
        previous_hash,
        vec![operation(RoleOperationKind::Command, "DeleteThing")],
    )
    .expect("syntactically valid approval");
    assert_eq!(
        InstallationRole::new(
            symbol("AppRole"),
            desired_hash,
            Some(previous_hash),
            desired.clone(),
            existing.clone(),
            Some(wrong),
        )
        .expect_err("approval must be exact")
        .kind(),
        InstallationPlanErrorKind::InvalidApproval
    );
    let exact = RoleWideningApproval::new(
        previous_hash,
        vec![operation(RoleOperationKind::Command, "CreateThing")],
    )
    .expect("exact approval");
    InstallationRole::new(
        symbol("AppRole"),
        desired_hash,
        Some(previous_hash),
        desired,
        existing,
        Some(exact),
    )
    .expect("explicit exact widening");
}

#[test]
fn secret_output_is_an_independent_exact_widening_atom_and_rotates_plan_to_v3() {
    let previous_hash = ApplicationRoleHash::from_bytes(hash32(10));
    let desired_hash = ApplicationRoleHash::from_bytes(hash32(11));
    let operation = operation(RoleOperationKind::Query, "GetSession");
    let output = secret_output("GetSession", "Session", "token_hash");
    let previous = RoleAuthoritySet::new(vec![operation.clone()], vec![]).expect("authority");
    let desired = RoleAuthoritySet::new(vec![operation], vec![output.clone()]).expect("authority");

    assert_eq!(
        InstallationRole::new_with_authority(
            symbol("AppRole"),
            desired_hash,
            Some(previous_hash),
            desired.clone(),
            previous.clone(),
            None,
        )
        .expect_err("a new secret-output atom is widening")
        .kind(),
        InstallationPlanErrorKind::ApprovalRequired
    );

    let approval = RoleWideningApproval::new_authority(
        previous_hash,
        vec![RoleAuthorityAtom::QuerySecretOutput(output)],
    )
    .expect("exact approval");
    let role = InstallationRole::new_with_authority(
        symbol("AppRole"),
        desired_hash,
        Some(previous_hash),
        desired,
        previous,
        Some(approval),
    )
    .expect("approved role");
    let mut input = base_input();
    input.roles = vec![role];
    input.credential_destinations = vec![
        CredentialDestination::new(
            symbol("app-runtime"),
            symbol("AppRole"),
            Some(capability(2)),
            capability(1),
        )
        .expect("credential rotation"),
    ];
    let plan = ApplicationInstallationPlan::compile(input).expect("v3 plan");
    assert_eq!(
        plan.canonical_bytes(),
        include_bytes!("../../../fixtures/installation/application-installation-plan-v3.json")
    );
    assert_eq!(plan.schema(), APPLICATION_INSTALLATION_PLAN_SCHEMA_V3);
    assert!(
        plan.input()
            .required_features
            .contains(&InstallationFeature::QuerySecretOutputs)
    );
    let value = serde_json::from_slice::<serde_json::Value>(plan.canonical_bytes()).expect("json");
    assert!(value["roles"][0].get("desired_operations").is_none());
    assert_eq!(
        value["roles"][0]["desired_authority"][1],
        serde_json::json!({
            "query": "GetSession",
            "entity": "Session",
            "field": "token_hash"
        })
    );
    assert_eq!(
        ApplicationInstallationPlan::decode_canonical(plan.canonical_bytes()).expect("v3 decode"),
        plan
    );
}

#[test]
fn secret_output_query_identity_is_distinct_and_approval_order_is_not_normalized() {
    let first =
        RoleAuthorityAtom::QuerySecretOutput(secret_output("FindSession", "Session", "token_hash"));
    let second =
        RoleAuthorityAtom::QuerySecretOutput(secret_output("GetSession", "Session", "token_hash"));
    assert_ne!(first, second);
    assert_eq!(
        RoleWideningApproval::new_authority(
            ApplicationRoleHash::from_bytes(hash32(10)),
            vec![second, first],
        )
        .expect_err("approval order is security-significant")
        .kind(),
        InstallationPlanErrorKind::InvalidApproval
    );
}

#[test]
fn secret_authority_narrowing_identity_rotation_and_approval_failures_are_exact() {
    let previous_hash = ApplicationRoleHash::from_bytes(hash32(10));
    let desired_hash = ApplicationRoleHash::from_bytes(hash32(11));
    let query = operation(RoleOperationKind::Query, "GetSession");
    let secret = secret_output("GetSession", "Session", "token_hash");
    let previous = RoleAuthoritySet::new(vec![query.clone()], vec![secret.clone()])
        .expect("previous authority");
    let narrowed = RoleAuthoritySet::new(vec![query.clone()], vec![]).expect("narrowed");
    InstallationRole::new_with_authority(
        symbol("AppRole"),
        desired_hash,
        Some(previous_hash),
        narrowed,
        previous.clone(),
        None,
    )
    .expect("removal narrows without approval");
    InstallationRole::new_with_authority(
        symbol("AppRole"),
        desired_hash,
        Some(previous_hash),
        previous.clone(),
        previous.clone(),
        None,
    )
    .expect("same atoms rotate identity without widening");

    let desired = RoleAuthoritySet::new(
        vec![query],
        vec![
            secret.clone(),
            secret_output("LookupSession", "Session", "token_hash"),
        ],
    )
    .expect("desired authority");
    let stale = RoleWideningApproval::new_authority(
        ApplicationRoleHash::from_bytes(hash32(9)),
        vec![RoleAuthorityAtom::QuerySecretOutput(secret_output(
            "LookupSession",
            "Session",
            "token_hash",
        ))],
    )
    .expect("stale approval shape");
    assert_eq!(
        InstallationRole::new_with_authority(
            symbol("AppRole"),
            desired_hash,
            Some(previous_hash),
            desired.clone(),
            previous.clone(),
            Some(stale),
        )
        .expect_err("stale predecessor")
        .kind(),
        InstallationPlanErrorKind::InvalidApproval
    );
    let excess = RoleWideningApproval::new_authority(
        previous_hash,
        vec![
            RoleAuthorityAtom::Operation(operation(RoleOperationKind::Command, "DeleteSession")),
            RoleAuthorityAtom::QuerySecretOutput(secret_output(
                "LookupSession",
                "Session",
                "token_hash",
            )),
        ],
    )
    .expect("canonical excess approval");
    assert_eq!(
        InstallationRole::new_with_authority(
            symbol("AppRole"),
            desired_hash,
            Some(previous_hash),
            desired,
            previous,
            Some(excess),
        )
        .expect_err("excess atom")
        .kind(),
        InstallationPlanErrorKind::InvalidApproval
    );
}

#[test]
fn authority_bounds_and_v3_old_reader_refusal_are_closed() {
    let operations = (0..=riffdb_application::MAX_ROLE_OPERATIONS)
        .map(|index| operation(RoleOperationKind::Query, &format!("Q{index:04}")))
        .collect();
    assert_eq!(
        RoleAuthoritySet::new(operations, vec![])
            .expect_err("operation ceiling")
            .kind(),
        InstallationPlanErrorKind::LimitExceeded
    );

    let plan = ApplicationInstallationPlan::decode_canonical(include_bytes!(
        "../../../fixtures/installation/application-installation-plan-v3.json"
    ))
    .expect("v3 fixture");
    let mut value =
        serde_json::from_slice::<serde_json::Value>(plan.canonical_bytes()).expect("json");
    value["schema"] =
        serde_json::Value::String("riffdb.application-installation-plan/v2".to_owned());
    let mut bytes = serde_json::to_vec(&value).expect("old schema bytes");
    bytes.push(b'\n');
    assert_eq!(
        ApplicationInstallationPlan::decode_canonical(&bytes)
            .expect_err("old reader identity cannot ignore authority atoms")
            .kind(),
        InstallationPlanErrorKind::InvalidShape
    );
}

#[test]
fn migration_and_credential_safety_fail_closed_before_planning() {
    assert_eq!(
        InstallationMigration::new(
            ContractVersion::new(1).expect("version"),
            ContractBundleHash::from_bytes(hash32(20)),
            ContractVersion::new(2).expect("version"),
            ContractBundleHash::from_bytes(hash32(21)),
            MigrationBundleHash::from_bytes(hash32(22)),
            MigrationBundleHash::from_bytes(hash32(23)),
        )
        .expect_err("confirmation mismatch")
        .kind(),
        InstallationPlanErrorKind::UnsafeMigration
    );
    assert_eq!(
        CredentialDestination::new(
            symbol("runtime"),
            symbol("AppRole"),
            Some(capability(1)),
            capability(1),
        )
        .expect_err("overwrite is not rotation")
        .kind(),
        InstallationPlanErrorKind::CredentialConflict
    );
}

#[test]
fn plan_debug_and_errors_do_not_expose_credentials_or_seed_values() {
    let plan = ApplicationInstallationPlan::compile(base_input()).expect("plan");
    let debug = format!("{plan:?}");
    assert!(!debug.contains(&capability(1).to_string()));
    assert!(!debug.contains("seed value"));
    assert_eq!(
        InstallationSymbol::new("/home/operator/credential")
            .expect_err("paths are forbidden")
            .to_string(),
        "application installation symbol is invalid"
    );
}
