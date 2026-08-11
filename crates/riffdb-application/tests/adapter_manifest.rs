//! Canonical adapter-conformance manifest and fail-closed binding corpus.

use riffdb_application::{
    AdapterConformanceErrorKind, AdapterConformanceManifest, AdapterConformanceManifestInput,
    AdapterConformanceProbe, AdapterDriverRequirement, AdapterEvolutionClass,
    AdapterEvolutionRequirement, AdapterFeatureClaim, AdapterFeatureDisposition, AdapterLimitation,
    AdapterPlatform, AdapterRoleRequirement, ApplicationInstallationPlan,
    ApplicationInstallationPlanInput, CredentialDestination, InstallationArtifact,
    InstallationArtifactKind, InstallationContract, InstallationDriver, InstallationFeature,
    InstallationRole, InstallationSymbol, InstallationTarget, RoleOperation, RoleOperationKind,
};
use riffdb_types::{
    AdapterConformanceManifestHash, ApplicationLockHash, ApplicationManifestHash,
    ApplicationRoleHash, ApplicationSourceHash, CapabilityId, ContractBundleHash, ContractLineage,
    ContractVersion, DatabaseAlias, Environment, GeneratedArtifactHash,
};

fn hash32(byte: u8) -> [u8; 32] {
    [byte; 32]
}

fn symbol(value: &str) -> InstallationSymbol {
    InstallationSymbol::new(value).expect("valid symbol")
}

fn operation(kind: RoleOperationKind, name: &str) -> RoleOperation {
    RoleOperation::new(kind, symbol(name))
}

fn manifest() -> AdapterConformanceManifest {
    AdapterConformanceManifest::compile(AdapterConformanceManifestInput {
        adapter: symbol("openfga"),
        adapter_version: symbol("v0.1.0"),
        application_manifest_hash: ApplicationManifestHash::from_bytes(hash32(1)),
        application_lock_hash: ApplicationLockHash::from_bytes(hash32(2)),
        contract_lineage: ContractLineage::new("OpenFgaAdapter").expect("lineage"),
        feature_claims: vec![
            AdapterFeatureClaim::required(InstallationFeature::RemoteTls),
            AdapterFeatureClaim::required(InstallationFeature::BulkCommands),
            AdapterFeatureClaim::required(InstallationFeature::InstallationCampaigns),
            AdapterFeatureClaim::unavailable(
                InstallationFeature::DataLifecycle,
                AdapterLimitation::ProductUnavailable,
            ),
        ],
        artifacts: vec![InstallationArtifact::new(
            InstallationArtifactKind::ContractBundle,
            symbol("contract"),
            GeneratedArtifactHash::from_bytes(hash32(3)),
        )],
        roles: vec![
            AdapterRoleRequirement::new(
                symbol("OpenFgaApplication"),
                ApplicationRoleHash::from_bytes(hash32(4)),
                vec![
                    operation(RoleOperationKind::Command, "WriteTuples"),
                    operation(RoleOperationKind::Query, "ListTuples"),
                ],
            )
            .expect("role"),
        ],
        drivers: vec![
            AdapterDriverRequirement::new(
                InstallationDriver::Go,
                symbol("go1.24"),
                vec![AdapterPlatform::LinuxX86_64Gnu],
                GeneratedArtifactHash::from_bytes(hash32(5)),
            )
            .expect("driver"),
        ],
        conformance: vec![
            AdapterConformanceProbe::new(
                symbol("tuple-page"),
                symbol("OpenFgaApplication"),
                operation(RoleOperationKind::Query, "ListTuples"),
                GeneratedArtifactHash::from_bytes(hash32(6)),
                100,
            )
            .expect("probe"),
        ],
        evolution: vec![
            AdapterEvolutionRequirement::new(
                AdapterEvolutionClass::EmptyInstall,
                None,
                InstallationContract::new(
                    ContractVersion::new(2).expect("version"),
                    ContractBundleHash::from_bytes(hash32(7)),
                ),
                None,
                GeneratedArtifactHash::from_bytes(hash32(8)),
            )
            .expect("empty install"),
            AdapterEvolutionRequirement::new(
                AdapterEvolutionClass::CompatibleUpgrade,
                Some(InstallationContract::new(
                    ContractVersion::new(1).expect("version"),
                    ContractBundleHash::from_bytes(hash32(9)),
                )),
                InstallationContract::new(
                    ContractVersion::new(2).expect("version"),
                    ContractBundleHash::from_bytes(hash32(7)),
                ),
                None,
                GeneratedArtifactHash::from_bytes(hash32(10)),
            )
            .expect("upgrade"),
        ],
    })
    .expect("manifest")
}

fn current_manifest() -> AdapterConformanceManifest {
    let mut input = manifest().input().clone();
    input.feature_claims.extend([
        AdapterFeatureClaim::new(
            InstallationFeature::OperationalQueries,
            AdapterFeatureDisposition::Optional,
            None,
        )
        .expect("optional operational queries"),
        AdapterFeatureClaim::new(
            InstallationFeature::WorkflowConcurrency,
            AdapterFeatureDisposition::Optional,
            None,
        )
        .expect("optional workflows"),
        AdapterFeatureClaim::unavailable(
            InstallationFeature::RowPolicies,
            AdapterLimitation::ProductUnavailable,
        ),
    ]);
    AdapterConformanceManifest::compile_v2(input).expect("current exhaustive manifest")
}

#[test]
fn manifest_is_canonical_content_addressed_and_strict() {
    let first = manifest();
    let decoded = AdapterConformanceManifest::decode_canonical(first.canonical_bytes())
        .expect("canonical manifest");
    assert_eq!(decoded, first);
    assert_eq!(decoded.identity(), first.identity());
    assert_eq!(
        String::from_utf8_lossy(decoded.canonical_bytes()),
        String::from_utf8_lossy(include_bytes!(
            "../../../fixtures/installation/adapter-conformance-manifest-v1.json"
        ))
    );

    let current = AdapterConformanceManifest::decode_canonical(include_bytes!(
        "../../../fixtures/adapters/conformance/openfga/adapter.conformance.json"
    ))
    .expect("current v2 manifest");
    assert_eq!(
        current.schema(),
        riffdb_application::ADAPTER_CONFORMANCE_MANIFEST_SCHEMA_V2
    );

    let mut value: serde_json::Value =
        serde_json::from_slice(first.canonical_bytes()).expect("manifest JSON");
    value["raw_permissions"] = serde_json::json!(["ReadEntity"]);
    let mut bytes = serde_json::to_vec(&value).expect("hostile JSON");
    bytes.push(b'\n');
    assert_eq!(
        AdapterConformanceManifest::decode_canonical(&bytes)
            .expect_err("unknown escape surface is rejected")
            .kind(),
        AdapterConformanceErrorKind::InvalidEncoding
    );
}

#[test]
fn manifest_binds_the_exact_installation_plan_and_receipt_identity() {
    let manifest = manifest();
    let role = InstallationRole::new(
        symbol("OpenFgaApplication"),
        ApplicationRoleHash::from_bytes(hash32(4)),
        None,
        vec![
            operation(RoleOperationKind::Command, "WriteTuples"),
            operation(RoleOperationKind::Query, "ListTuples"),
        ],
        vec![],
        None,
    )
    .expect("role");
    let plan = ApplicationInstallationPlan::compile(ApplicationInstallationPlanInput {
        application: symbol("openfga"),
        source_hash: ApplicationSourceHash::from_bytes(hash32(11)),
        lock_hash: ApplicationLockHash::from_bytes(hash32(2)),
        manifest_hash: ApplicationManifestHash::from_bytes(hash32(1)),
        target: InstallationTarget::new(
            DatabaseAlias::new("openfga").expect("database"),
            Environment::new("dev").expect("environment"),
            ContractLineage::new("OpenFgaAdapter").expect("lineage"),
        ),
        contract: InstallationContract::new(
            ContractVersion::new(2).expect("version"),
            ContractBundleHash::from_bytes(hash32(7)),
        ),
        artifacts: vec![
            InstallationArtifact::new(
                InstallationArtifactKind::Manifest,
                symbol("manifest"),
                GeneratedArtifactHash::from_bytes(hash32(12)),
            ),
            InstallationArtifact::new(
                InstallationArtifactKind::ContractBundle,
                symbol("contract"),
                GeneratedArtifactHash::from_bytes(hash32(3)),
            ),
            InstallationArtifact::new(
                InstallationArtifactKind::AdapterManifest,
                symbol("openfga"),
                GeneratedArtifactHash::from_bytes(*manifest.identity().as_bytes()),
            ),
        ],
        migration: None,
        roles: vec![role],
        credential_destinations: vec![
            CredentialDestination::new(
                symbol("runtime"),
                symbol("OpenFgaApplication"),
                None,
                CapabilityId::from_bytes([0, 0, 0, 0, 0, 1, 0x70, 0, 0x80, 0, 0, 0, 0, 0, 0, 1])
                    .expect("capability"),
            )
            .expect("destination"),
        ],
        drivers: vec![InstallationDriver::Go],
        seeds: vec![],
        required_features: vec![
            InstallationFeature::RemoteTls,
            InstallationFeature::BulkCommands,
            InstallationFeature::InstallationCampaigns,
        ],
        adapter_manifest_hash: Some(AdapterConformanceManifestHash::from_bytes(
            *manifest.identity().as_bytes(),
        )),
    })
    .expect("plan");

    manifest
        .validate_installation_plan(&plan)
        .expect("exact bind");

    let mut wrong = plan.input().clone();
    wrong.contract = InstallationContract::new(
        ContractVersion::new(3).expect("version"),
        ContractBundleHash::from_bytes(hash32(13)),
    );
    let wrong = ApplicationInstallationPlan::compile(wrong).expect("wrong plan");
    assert_eq!(
        manifest
            .validate_installation_plan(&wrong)
            .expect_err("successor drift is rejected")
            .kind(),
        AdapterConformanceErrorKind::IdentityMismatch
    );
}

#[test]
fn feature_disposition_cannot_hide_a_fallback() {
    assert_eq!(
        AdapterFeatureClaim::new(
            InstallationFeature::OperationalQueries,
            AdapterFeatureDisposition::Required,
            Some(AdapterLimitation::ProductUnavailable),
        )
        .expect_err("required feature cannot carry fallback-like limitation")
        .kind(),
        AdapterConformanceErrorKind::InvalidShape
    );
}

#[test]
fn every_closed_feature_requires_an_explicit_support_disposition() {
    let mut incomplete = current_manifest().input().clone();
    incomplete.feature_claims.pop();
    assert_eq!(
        AdapterConformanceManifest::compile_v2(incomplete)
            .expect_err("omitted product feature cannot become an implicit fallback")
            .kind(),
        AdapterConformanceErrorKind::InvalidShape
    );
}

#[test]
fn compatible_application_evolution_can_rotate_authority_without_contract_drift() {
    let current = InstallationContract::new(
        ContractVersion::new(2).expect("version"),
        ContractBundleHash::from_bytes(hash32(7)),
    );
    AdapterEvolutionRequirement::new(
        AdapterEvolutionClass::CompatibleUpgrade,
        Some(current),
        current,
        None,
        GeneratedArtifactHash::from_bytes(hash32(14)),
    )
    .expect("same-contract generated artifact and authority rotation is compatible evolution");
}

#[test]
fn driver_runtime_claims_are_exact_and_never_ranges_or_floating_channels() {
    for invalid in ["latest", "stable", "go1.24..1.26", "current"] {
        assert_eq!(
            AdapterDriverRequirement::new(
                InstallationDriver::Go,
                symbol(invalid),
                vec![AdapterPlatform::LinuxX86_64Gnu],
                GeneratedArtifactHash::from_bytes(hash32(15)),
            )
            .expect_err("floating or ranged runtime identity is rejected")
            .kind(),
            AdapterConformanceErrorKind::InvalidShape
        );
    }
}
