//! Resumable installation-campaign semantic corpus.

use riffdb_application::{
    ApplicationInstallationCampaign, ApplicationInstallationCampaignState,
    ApplicationInstallationPlan, ApplicationInstallationPlanInput, ApplicationInstallationReceipt,
    CredentialDestination, InstallationArtifact, InstallationArtifactKind,
    InstallationCampaignErrorKind, InstallationCampaignPhase, InstallationContract,
    InstallationDriver, InstallationFailureCode, InstallationFeature, InstallationNextAction,
    InstallationRole, InstallationSeed, InstallationStage, InstallationStageEvidence,
    InstallationSymbol, InstallationTarget, InstalledCredentialEvidence, InstalledRoleEvidence,
    InstalledSeedEvidence, RoleOperation, RoleOperationKind,
};
use riffdb_types::{
    ApplicationInstallationCampaignId, ApplicationLockHash, ApplicationManifestHash,
    ApplicationRoleHash, ApplicationSourceHash, CapabilityId, ContractBundleHash, ContractLineage,
    ContractVersion, DatabaseAlias, Environment, GeneratedArtifactHash,
};

fn hash32(byte: u8) -> [u8; 32] {
    [byte; 32]
}

fn campaign_id(last: u8) -> ApplicationInstallationCampaignId {
    ApplicationInstallationCampaignId::from_bytes([
        0, 0, 0, 0, 0, 2, 0x70, 0, 0x80, 0, 0, 0, 0, 0, 0, last,
    ])
    .expect("valid UUIDv7")
}

fn capability(last: u8) -> CapabilityId {
    CapabilityId::from_bytes([0, 0, 0, 0, 0, 1, 0x70, 0, 0x80, 0, 0, 0, 0, 0, 0, last])
        .expect("valid UUIDv7")
}

fn symbol(value: &str) -> InstallationSymbol {
    InstallationSymbol::new(value).expect("valid symbol")
}

fn role_hash() -> ApplicationRoleHash {
    ApplicationRoleHash::from_bytes(hash32(8))
}

fn plan(source_byte: u8) -> ApplicationInstallationPlan {
    ApplicationInstallationPlan::compile(ApplicationInstallationPlanInput {
        application: symbol("example"),
        source_hash: ApplicationSourceHash::from_bytes(hash32(source_byte)),
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
                InstallationArtifactKind::Manifest,
                symbol("manifest"),
                GeneratedArtifactHash::from_bytes(hash32(5)),
            ),
            InstallationArtifact::new(
                InstallationArtifactKind::ContractBundle,
                symbol("contract"),
                GeneratedArtifactHash::from_bytes(hash32(6)),
            ),
            InstallationArtifact::new(
                InstallationArtifactKind::QueryModule,
                symbol("queries"),
                GeneratedArtifactHash::from_bytes(hash32(7)),
            ),
        ],
        migration: None,
        roles: vec![
            InstallationRole::new(
                symbol("AppRole"),
                role_hash(),
                None,
                vec![
                    RoleOperation::new(RoleOperationKind::Query, symbol("GetThing")),
                    RoleOperation::new(RoleOperationKind::Command, symbol("CreateThing")),
                ],
                vec![],
                None,
            )
            .expect("role"),
        ],
        credential_destinations: vec![
            CredentialDestination::new(
                symbol("app-runtime"),
                symbol("AppRole"),
                None,
                capability(1),
            )
            .expect("credential"),
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
    })
    .expect("plan")
}

fn evidence(stage: InstallationStage) -> InstallationStageEvidence {
    match stage {
        InstallationStage::Preflight => InstallationStageEvidence::Preflight {
            source_hash: ApplicationSourceHash::from_bytes(hash32(1)),
            lock_hash: ApplicationLockHash::from_bytes(hash32(2)),
            manifest_hash: ApplicationManifestHash::from_bytes(hash32(3)),
        },
        InstallationStage::Contract => InstallationStageEvidence::Contract {
            version: ContractVersion::new(1).expect("version"),
            bundle_hash: ContractBundleHash::from_bytes(hash32(4)),
        },
        InstallationStage::Migration => InstallationStageEvidence::Migration {
            migration_hash: None,
        },
        InstallationStage::QueryModules => {
            InstallationStageEvidence::QueryModules(vec![InstallationArtifact::new(
                InstallationArtifactKind::QueryModule,
                symbol("queries"),
                GeneratedArtifactHash::from_bytes(hash32(7)),
            )])
        }
        InstallationStage::ReactiveModules => InstallationStageEvidence::ReactiveModules(vec![]),
        InstallationStage::Roles => {
            InstallationStageEvidence::Roles(vec![InstalledRoleEvidence::new(
                symbol("AppRole"),
                role_hash(),
            )])
        }
        InstallationStage::Credentials => {
            InstallationStageEvidence::Credentials(vec![InstalledCredentialEvidence::new(
                symbol("app-runtime"),
                capability(1),
            )])
        }
        InstallationStage::DriverProof => InstallationStageEvidence::DriverProof(vec![
            InstallationDriver::Rust,
            InstallationDriver::TypeScript,
        ]),
        InstallationStage::Seeds => {
            InstallationStageEvidence::Seeds(vec![InstalledSeedEvidence::new(
                symbol("initial-data"),
                GeneratedArtifactHash::from_bytes(hash32(9)),
                5,
                2,
            )])
        }
        InstallationStage::Receipt => panic!("receipt is sealed by the state machine"),
    }
}

#[test]
fn every_interruption_resumes_after_revalidating_completed_identities() {
    let plan = plan(1);
    let id = campaign_id(1);
    let mut campaign = ApplicationInstallationCampaign::start(id, plan.identity());
    for stage in &InstallationStage::ALL[..9] {
        let exact = evidence(*stage);
        campaign
            .complete_stage(&plan, exact.clone())
            .expect("stage completion");
        let completed_count = campaign.completed_evidence().len();
        campaign
            .complete_stage(&plan, exact)
            .expect("same evidence is idempotent");
        assert_eq!(campaign.completed_evidence().len(), completed_count);

        let mut recovered = campaign.clone();
        let observation = recovered.resume(id, &plan).expect("verified resume");
        assert_ne!(observation.phase(), InstallationCampaignPhase::Installed);
        campaign = recovered;
    }

    let receipt = campaign.seal_receipt(&plan).expect("terminal receipt");
    assert_eq!(
        receipt.canonical_bytes(),
        include_bytes!("../../../fixtures/installation/application-installation-receipt-v1.json")
    );
    assert_eq!(
        campaign.seal_receipt(&plan).expect("idempotent seal"),
        receipt
    );
    let observation = campaign.observe();
    assert_eq!(observation.phase(), InstallationCampaignPhase::Installed);
    assert_eq!(observation.next_stage(), None);
    assert_eq!(observation.next_action(), InstallationNextAction::None);
    assert_eq!(observation.receipt_hash(), Some(receipt.identity()));
    assert_eq!(
        ApplicationInstallationReceipt::decode_canonical(receipt.canonical_bytes())
            .expect("canonical receipt"),
        receipt
    );

    let receipt_text = std::str::from_utf8(receipt.canonical_bytes()).expect("utf8 json");
    assert!(!receipt_text.contains("capability_id"));
    assert!(!receipt_text.contains("credential"));
    assert!(!receipt_text.contains(&capability(1).to_string()));
    assert!(!receipt_text.contains("/home/"));
}

#[test]
fn partial_failure_is_typed_and_can_never_report_installed() {
    let plan = plan(1);
    let id = campaign_id(2);
    let mut campaign = ApplicationInstallationCampaign::start(id, plan.identity());
    campaign
        .complete_stage(&plan, evidence(InstallationStage::Preflight))
        .expect("preflight");
    let partial = campaign
        .record_failure(&plan, InstallationFailureCode::RemoteIdentityMismatch)
        .expect("partial");
    assert_eq!(partial.phase(), InstallationCampaignPhase::Partial);
    assert_eq!(partial.next_stage(), Some(InstallationStage::Contract));
    assert_eq!(
        partial.next_action(),
        InstallationNextAction::DeployContract
    );
    assert_eq!(
        partial.failure().expect("failure").code(),
        InstallationFailureCode::RemoteIdentityMismatch
    );
    assert_eq!(partial.receipt_hash(), None);
    assert!(!campaign.is_installed());

    let resumed = campaign.resume(id, &plan).expect("resume");
    assert_eq!(resumed.phase(), InstallationCampaignPhase::Running);
    assert_eq!(resumed.failure(), None);
}

#[test]
fn campaign_identity_plan_identity_stage_order_and_evidence_fail_closed() {
    let exact_plan = plan(1);
    let other_plan = plan(42);
    let id = campaign_id(3);
    let mut campaign = ApplicationInstallationCampaign::start(id, exact_plan.identity());
    assert_eq!(
        campaign
            .resume(id, &other_plan)
            .expect_err("same campaign cannot mean another plan")
            .kind(),
        InstallationCampaignErrorKind::PlanIdentityMismatch
    );
    assert_eq!(
        campaign
            .resume(campaign_id(4), &exact_plan)
            .expect_err("identity is caller-stable")
            .kind(),
        InstallationCampaignErrorKind::CampaignIdentityMismatch
    );
    assert_eq!(
        campaign
            .complete_stage(&exact_plan, evidence(InstallationStage::Contract))
            .expect_err("stage skip")
            .kind(),
        InstallationCampaignErrorKind::StageOutOfOrder
    );
    assert_eq!(
        campaign
            .complete_stage(
                &exact_plan,
                InstallationStageEvidence::Preflight {
                    source_hash: ApplicationSourceHash::from_bytes(hash32(99)),
                    lock_hash: ApplicationLockHash::from_bytes(hash32(2)),
                    manifest_hash: ApplicationManifestHash::from_bytes(hash32(3)),
                },
            )
            .expect_err("completed identity mismatch")
            .kind(),
        InstallationCampaignErrorKind::EvidenceMismatch
    );
    assert!(campaign.completed_evidence().is_empty());
}

#[test]
fn canonical_campaign_state_recovers_every_partial_and_terminal_boundary() {
    let plan = plan(1);
    // The checked plan, terminal campaign state, and receipt fixtures form one
    // exact compatibility chain rather than unrelated individually valid samples.
    let id = campaign_id(1);
    let mut campaign = ApplicationInstallationCampaign::start(id, plan.identity());

    for stage in &InstallationStage::ALL[..9] {
        campaign
            .complete_stage(&plan, evidence(*stage))
            .expect("stage completion");
        let state = ApplicationInstallationCampaignState::capture(&campaign, &plan)
            .expect("canonical durable state");
        let recovered =
            ApplicationInstallationCampaignState::decode_canonical(state.canonical_bytes())
                .expect("strict durable decode");
        assert_eq!(recovered.plan(), &plan);
        assert_eq!(recovered.campaign(), &campaign);
        assert_eq!(recovered.canonical_bytes(), state.canonical_bytes());
    }

    campaign
        .record_failure(&plan, InstallationFailureCode::ServiceUnavailable)
        .expect("typed partial");
    let partial = ApplicationInstallationCampaignState::capture(&campaign, &plan)
        .expect("partial durable state");
    let recovered =
        ApplicationInstallationCampaignState::decode_canonical(partial.canonical_bytes())
            .expect("partial recovery");
    assert_eq!(
        recovered.campaign().observe().phase(),
        InstallationCampaignPhase::Partial
    );

    campaign.resume(id, &plan).expect("clear partial failure");
    campaign.seal_receipt(&plan).expect("terminal receipt");
    let terminal = ApplicationInstallationCampaignState::capture(&campaign, &plan)
        .expect("terminal durable state");
    assert_eq!(
        terminal.canonical_bytes(),
        include_bytes!(
            "../../../fixtures/installation/application-installation-campaign-state-v1.json"
        )
    );
    let recovered =
        ApplicationInstallationCampaignState::decode_canonical(terminal.canonical_bytes())
            .expect("terminal recovery");
    assert!(recovered.campaign().is_installed());
    assert_eq!(recovered.campaign(), &campaign);
}

#[test]
fn campaign_state_rejects_noncanonical_tampered_and_cross_plan_bytes() {
    let plan = plan(1);
    let mut campaign = ApplicationInstallationCampaign::start(campaign_id(6), plan.identity());
    campaign
        .complete_stage(&plan, evidence(InstallationStage::Preflight))
        .expect("preflight");
    let state =
        ApplicationInstallationCampaignState::capture(&campaign, &plan).expect("canonical state");

    let mut noncanonical = state.canonical_bytes().to_vec();
    noncanonical.extend_from_slice(b"\n");
    assert_eq!(
        ApplicationInstallationCampaignState::decode_canonical(&noncanonical)
            .expect_err("extra bytes are not canonical")
            .kind(),
        InstallationCampaignErrorKind::NonCanonical
    );

    let text = std::str::from_utf8(state.canonical_bytes()).expect("utf8 state");
    let plan_hash = plan
        .identity()
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let tampered = text.replacen(&plan_hash, &"ff".repeat(32), 1);
    assert_eq!(
        ApplicationInstallationCampaignState::decode_canonical(tampered.as_bytes())
            .expect_err("plan identity substitution")
            .kind(),
        InstallationCampaignErrorKind::PlanIdentityMismatch
    );
}
