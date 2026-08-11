#![forbid(unsafe_code)]

//! Public semantic acceptance for exact resumable application installation.

use riffdb_application::{
    ApplicationInstallationCampaign, ApplicationInstallationCampaignState,
    ApplicationInstallationPlan, ApplicationInstallationReceipt, InstallationArtifactKind,
    InstallationCampaignErrorKind, InstallationCampaignPhase, InstallationDriver,
    InstallationFailureCode, InstallationNextAction, InstallationStage, InstallationStageEvidence,
    InstalledCredentialEvidence, InstalledRoleEvidence, InstalledSeedEvidence,
};
use riffdb_types::{ApplicationInstallationCampaignId, RequestId};

const PLAN: &[u8] =
    include_bytes!("../fixtures/installation/application-installation-plan-v1.json");
const TERMINAL_STATE: &[u8] =
    include_bytes!("../fixtures/installation/application-installation-campaign-state-v1.json");
const RECEIPT: &[u8] =
    include_bytes!("../fixtures/installation/application-installation-receipt-v1.json");

#[test]
fn canonical_plan_campaign_and_receipt_form_one_exact_restart_chain() {
    let plan = ApplicationInstallationPlan::decode_canonical(PLAN).expect("canonical plan");
    let state = ApplicationInstallationCampaignState::decode_canonical(TERMINAL_STATE)
        .expect("canonical terminal state");
    let receipt =
        ApplicationInstallationReceipt::decode_canonical(RECEIPT).expect("canonical receipt");

    assert_eq!(state.plan(), &plan);
    assert!(state.campaign().is_installed());
    assert_eq!(
        state.campaign().observe().receipt_hash(),
        Some(receipt.identity())
    );
    assert_eq!(receipt.plan_hash(), plan.identity());
    assert_eq!(
        receipt.campaign_id(),
        state.campaign().observe().campaign_id()
    );
    assert_eq!(state.canonical_bytes(), TERMINAL_STATE);
    assert_eq!(receipt.canonical_bytes(), RECEIPT);
}

#[test]
fn same_id_different_plan_and_partial_completion_never_become_success() {
    let plan = ApplicationInstallationPlan::decode_canonical(PLAN).expect("canonical plan");
    let campaign_id = campaign_id(3);
    let mut campaign = ApplicationInstallationCampaign::start(campaign_id, plan.identity());
    campaign
        .complete_stage(
            &plan,
            InstallationStageEvidence::Preflight {
                source_hash: plan.input().source_hash,
                lock_hash: plan.input().lock_hash,
                manifest_hash: plan.input().manifest_hash,
            },
        )
        .expect("preflight completes");
    let durable =
        ApplicationInstallationCampaignState::capture(&campaign, &plan).expect("durable partial");
    let recovered =
        ApplicationInstallationCampaignState::decode_canonical(durable.canonical_bytes())
            .expect("restart decode");
    assert_eq!(
        recovered.campaign().observe().next_stage(),
        Some(InstallationStage::Contract)
    );

    let mut changed = String::from_utf8(PLAN.to_vec()).expect("plan JSON");
    changed = changed.replacen(&"01".repeat(32), &"0a".repeat(32), 1);
    let changed = ApplicationInstallationPlan::decode_canonical(changed.as_bytes())
        .expect("canonical changed plan");
    let (_, mut recovered_campaign) = recovered.into_parts();
    assert_eq!(
        recovered_campaign
            .resume(campaign_id, &changed)
            .expect_err("campaign identity cannot be rebound")
            .kind(),
        InstallationCampaignErrorKind::PlanIdentityMismatch
    );

    let partial = recovered_campaign
        .record_failure(&plan, InstallationFailureCode::RemoteIdentityMismatch)
        .expect("typed partial failure");
    assert_eq!(partial.phase(), InstallationCampaignPhase::Partial);
    assert_eq!(
        partial.next_action(),
        InstallationNextAction::DeployContract
    );
    assert_eq!(partial.receipt_hash(), None);
    assert!(!recovered_campaign.is_installed());
}

#[test]
fn campaign_identifiers_are_caller_stable_uuidv7_values() {
    let campaign = campaign_id(7);
    let request = RequestId::from_bytes(campaign.into_bytes()).expect("same UUID shape");
    assert_eq!(campaign.into_bytes(), request.into_bytes());
    assert_eq!(campaign.to_string().chars().nth(14), Some('7'));
}

#[test]
fn every_controller_stage_survives_a_durable_resume_before_the_next_action() {
    let plan = ApplicationInstallationPlan::decode_canonical(PLAN).expect("canonical plan");
    let input = plan.input();
    let campaign_id = campaign_id(9);
    let stages = vec![
        InstallationStageEvidence::Preflight {
            source_hash: input.source_hash,
            lock_hash: input.lock_hash,
            manifest_hash: input.manifest_hash,
        },
        InstallationStageEvidence::Contract {
            version: input.contract.version(),
            bundle_hash: input.contract.bundle_hash(),
        },
        InstallationStageEvidence::Migration {
            migration_hash: input.migration.map(|migration| migration.migration_hash()),
        },
        InstallationStageEvidence::QueryModules(
            input
                .artifacts
                .iter()
                .filter(|artifact| artifact.kind() == InstallationArtifactKind::QueryModule)
                .cloned()
                .collect(),
        ),
        InstallationStageEvidence::ReactiveModules(
            input
                .artifacts
                .iter()
                .filter(|artifact| artifact.kind() == InstallationArtifactKind::ReactiveModule)
                .cloned()
                .collect(),
        ),
        InstallationStageEvidence::Roles(
            input
                .roles
                .iter()
                .map(|role| InstalledRoleEvidence::new(role.name().clone(), role.role_hash()))
                .collect(),
        ),
        InstallationStageEvidence::Credentials(
            input
                .credential_destinations
                .iter()
                .map(|destination| {
                    InstalledCredentialEvidence::new(
                        destination.name().clone(),
                        destination.successor(),
                    )
                })
                .collect(),
        ),
        InstallationStageEvidence::DriverProof(vec![
            InstallationDriver::Rust,
            InstallationDriver::TypeScript,
        ]),
        InstallationStageEvidence::Seeds(
            input
                .seeds
                .iter()
                .map(|seed| {
                    InstalledSeedEvidence::new(
                        seed.name().clone(),
                        seed.content_hash(),
                        seed.item_count(),
                        0,
                    )
                })
                .collect(),
        ),
    ];
    let expected_next = [
        Some(InstallationStage::Contract),
        Some(InstallationStage::Migration),
        Some(InstallationStage::QueryModules),
        Some(InstallationStage::ReactiveModules),
        Some(InstallationStage::Roles),
        Some(InstallationStage::Credentials),
        Some(InstallationStage::DriverProof),
        Some(InstallationStage::Seeds),
        Some(InstallationStage::Receipt),
    ];
    let mut campaign = ApplicationInstallationCampaign::start(campaign_id, plan.identity());

    for (evidence, expected_next) in stages.into_iter().zip(expected_next) {
        campaign
            .complete_stage(&plan, evidence)
            .expect("exact stage completes");
        let durable = ApplicationInstallationCampaignState::capture(&campaign, &plan)
            .expect("capture stage boundary");
        let recovered =
            ApplicationInstallationCampaignState::decode_canonical(durable.canonical_bytes())
                .expect("recover stage boundary");
        let (recovered_plan, mut recovered_campaign) = recovered.into_parts();
        assert_eq!(recovered_plan, plan);
        recovered_campaign
            .resume(campaign_id, &plan)
            .expect("same campaign and plan resume");
        assert_eq!(recovered_campaign.observe().next_stage(), expected_next);
        assert_ne!(
            recovered_campaign.observe().phase(),
            InstallationCampaignPhase::Installed,
            "partial controller progress cannot masquerade as installed"
        );
        campaign = recovered_campaign;
    }

    let receipt = campaign
        .seal_receipt(&plan)
        .expect("only the server-owned terminal stage seals a receipt");
    assert!(campaign.is_installed());
    assert_eq!(receipt.plan_hash(), plan.identity());
    assert_eq!(receipt.campaign_id(), campaign_id);
}

fn campaign_id(last: u8) -> ApplicationInstallationCampaignId {
    ApplicationInstallationCampaignId::from_bytes([
        0, 0, 0, 0, 0, 2, 0x70, 0, 0x80, 0, 0, 0, 0, 0, 0, last,
    ])
    .expect("valid campaign UUIDv7")
}
