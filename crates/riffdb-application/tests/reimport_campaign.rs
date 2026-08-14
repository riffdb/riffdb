//! Fail-closed durable reimport campaign semantics.

use std::{fs, num::NonZeroU64, path::Path};

use riffdb_application::{
    ApplicationPortabilityManifest, ApplicationReimportCampaignErrorKind,
    ApplicationReimportCampaignPhaseV1, ApplicationReimportCampaignV1,
    ApplicationReimportFailureV1, ApplicationReimportReceipt, ApplicationReimportSourceV1,
    InstallationSymbol, PortableRecordClass, ReimportPageMappingOutcomeV1,
    ReimportWorkflowQuiescenceV1,
};
use riffdb_types::{
    ApplicationExportPageHash, ApplicationExportReceiptHash, ApplicationReimportAuthorityV1,
    CapabilityApplicationReimportScopeV1, CapabilityId, DatabaseId, GeneratedArtifactHash,
};

fn hash32(byte: u8) -> [u8; 32] {
    [byte; 32]
}

fn database(last: u8) -> DatabaseId {
    DatabaseId::from_unix_milliseconds_and_random(42, [last; 10]).expect("database UUIDv7")
}

fn capability(last: u8) -> CapabilityId {
    CapabilityId::from_unix_milliseconds_and_random(43, [last; 10]).expect("capability UUIDv7")
}

fn fixtures() -> (ApplicationPortabilityManifest, ApplicationReimportReceipt) {
    let manifest = ApplicationPortabilityManifest::decode_canonical(include_bytes!(
        "../../../fixtures/export/openfga/portability-manifest-v3.json"
    ))
    .expect("manifest");
    let receipt = ApplicationReimportReceipt::decode_canonical(
        include_bytes!("../../../fixtures/export/openfga/reimport-receipt-v3.json"),
        &manifest,
    )
    .expect("receipt");
    (manifest, receipt)
}

fn source(
    manifest: &ApplicationPortabilityManifest,
    receipt: &ApplicationReimportReceipt,
    page_hashes: Vec<ApplicationExportPageHash>,
) -> ApplicationReimportSourceV1 {
    ApplicationReimportSourceV1::new(
        receipt.input().export_manifest_hash,
        ApplicationExportReceiptHash::from_bytes(hash32(11)),
        manifest.identity(),
        database(1),
        receipt.input().target_database_id,
        2,
        page_hashes,
        vec![],
    )
    .expect("source")
}

fn authority(revision: u64) -> ApplicationReimportAuthorityV1 {
    ApplicationReimportAuthorityV1::new(capability(7), NonZeroU64::new(revision).expect("revision"))
}

fn domain_fixtures(domain: &str) -> (ApplicationPortabilityManifest, ApplicationReimportReceipt) {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures/export")
        .join(domain);
    let manifest = ApplicationPortabilityManifest::decode_canonical(
        &fs::read(root.join("portability-manifest-v3.json")).expect("portability manifest"),
    )
    .expect("canonical V3 manifest");
    let receipt = ApplicationReimportReceipt::decode_canonical(
        &fs::read(root.join("reimport-receipt-v3.json")).expect("reimport receipt"),
        &manifest,
    )
    .expect("receipt reconciles the V3 manifest");
    (manifest, receipt)
}

#[test]
fn workflow_quiescence_is_proven_before_a_source_can_exist() {
    let workflow = InstallationSymbol::new("RunWorkflow").expect("symbol");
    assert_eq!(
        ReimportWorkflowQuiescenceV1::new(workflow.clone(), 2, 1, 1)
            .expect_err("active lease must fail")
            .kind(),
        ApplicationReimportCampaignErrorKind::WorkflowNotQuiescent
    );
    assert_eq!(
        ReimportWorkflowQuiescenceV1::new(workflow, 2, 2, 0)
            .expect("quiescent")
            .checked_rows(),
        2
    );
}

#[test]
fn page_checkpoint_is_atomic_canonical_and_resumes_at_the_exact_next_page() {
    let (manifest, receipt) = fixtures();
    let first = ApplicationExportPageHash::from_bytes(hash32(31));
    let second = ApplicationExportPageHash::from_bytes(hash32(32));
    let expected = &receipt.input().mappings[0];
    let mut campaign = ApplicationReimportCampaignV1::start(
        source(&manifest, &receipt, vec![first, second]),
        authority(1),
        CapabilityApplicationReimportScopeV1::WholeApplication,
        &manifest,
    )
    .expect("campaign");

    let wrong = ReimportPageMappingOutcomeV1::new(
        PortableRecordClass::Entity,
        InstallationSymbol::new("NotInSchedule").expect("symbol"),
        1,
        false,
        GeneratedArtifactHash::from_bytes(hash32(50)),
    )
    .expect("outcome");
    assert_eq!(
        campaign
            .complete_page(NonZeroU64::MIN, first, vec![wrong])
            .expect_err("mapping substitution")
            .kind(),
        ApplicationReimportCampaignErrorKind::MappingMismatch
    );
    assert_eq!(campaign.mappings()[0].records(), 0);

    campaign
        .complete_page(
            NonZeroU64::MIN,
            first,
            vec![
                ReimportPageMappingOutcomeV1::new(
                    expected.class(),
                    expected.symbol().clone(),
                    1,
                    false,
                    GeneratedArtifactHash::from_bytes(hash32(51)),
                )
                .expect("first outcome"),
            ],
        )
        .expect("first page");
    let bytes = campaign.encode_canonical().expect("canonical checkpoint");
    let mut recovered =
        ApplicationReimportCampaignV1::decode_canonical(&bytes).expect("recover checkpoint");
    assert_eq!(recovered.next_page().get(), 2);
    assert_eq!(
        recovered
            .complete_page(NonZeroU64::MIN, first, vec![])
            .expect_err("committed page cannot be selected twice")
            .kind(),
        ApplicationReimportCampaignErrorKind::PageOutOfOrder
    );
    recovered
        .complete_page(
            NonZeroU64::new(2).expect("page"),
            second,
            vec![
                ReimportPageMappingOutcomeV1::new(
                    expected.class(),
                    expected.symbol().clone(),
                    1,
                    false,
                    expected.outcome_hash(),
                )
                .expect("second outcome"),
            ],
        )
        .expect("second page");
    assert_eq!(
        recovered.phase(),
        ApplicationReimportCampaignPhaseV1::Reconciling
    );
    let reconciled = recovered
        .reconcile(&manifest, receipt.input().observations.clone())
        .expect("reconcile");
    assert_eq!(reconciled.identity(), receipt.identity());
    let terminal = recovered.encode_canonical().expect("terminal checkpoint");
    let terminal = ApplicationReimportCampaignV1::decode_canonical(&terminal)
        .expect("terminal checkpoint recovery");
    assert_eq!(terminal.receipt_document(), Some(receipt.canonical_bytes()));

    let mut noncanonical = bytes;
    noncanonical.insert(0, b' ');
    assert_eq!(
        ApplicationReimportCampaignV1::decode_canonical(&noncanonical)
            .expect_err("noncanonical JSON")
            .kind(),
        ApplicationReimportCampaignErrorKind::NonCanonical
    );
}

#[test]
fn an_exact_empty_source_page_advances_without_fabricating_a_command_outcome() {
    let (manifest, receipt) = fixtures();
    let hashes = vec![ApplicationExportPageHash::from_bytes([21; 32])];
    let empty_source = ApplicationReimportSourceV1::new(
        receipt.input().export_manifest_hash,
        ApplicationExportReceiptHash::from_bytes(hash32(11)),
        manifest.identity(),
        database(1),
        receipt.input().target_database_id,
        0,
        hashes.clone(),
        vec![],
    )
    .expect("empty source");
    let mut campaign = ApplicationReimportCampaignV1::start(
        empty_source,
        authority(1),
        CapabilityApplicationReimportScopeV1::WholeApplication,
        &manifest,
    )
    .expect("campaign");
    campaign
        .complete_page(NonZeroU64::MIN, hashes[0], Vec::new())
        .expect("exact empty page");
    assert_eq!(
        campaign.phase(),
        ApplicationReimportCampaignPhaseV1::Reconciling
    );
}

#[test]
fn capability_revision_change_closes_the_campaign_without_progress() {
    let (manifest, receipt) = fixtures();
    let page = ApplicationExportPageHash::from_bytes(hash32(41));
    let mut campaign = ApplicationReimportCampaignV1::start(
        source(&manifest, &receipt, vec![page]),
        authority(1),
        CapabilityApplicationReimportScopeV1::PrincipalFiltered,
        &manifest,
    )
    .expect("campaign");
    assert_eq!(
        campaign
            .verify_authority(authority(2))
            .expect_err("authority rotation closes campaign")
            .kind(),
        ApplicationReimportCampaignErrorKind::AuthorityChanged
    );
    assert_eq!(campaign.phase(), ApplicationReimportCampaignPhaseV1::Failed);
    assert_eq!(
        campaign.failure(),
        Some(ApplicationReimportFailureV1::AuthorityChanged)
    );
    assert_eq!(campaign.mappings()[0].records(), 0);
}

#[test]
fn four_alpha_domains_resume_and_reconcile_from_canonical_v3_evidence() {
    for (ordinal, domain) in ["openfga", "mlflow", "better-auth", "woodpecker"]
        .into_iter()
        .enumerate()
    {
        let (manifest, expected_receipt) = domain_fixtures(domain);
        let page_hash = ApplicationExportPageHash::from_bytes([0x60 + ordinal as u8; 32]);
        let rows = expected_receipt
            .input()
            .mappings
            .iter()
            .map(|mapping| mapping.records())
            .sum();
        let workflow_quiescence = match domain {
            "mlflow" => vec![
                ReimportWorkflowQuiescenceV1::new(
                    InstallationSymbol::new("RunLifecycle").expect("workflow"),
                    2,
                    2,
                    0,
                )
                .expect("quiescent workflow"),
            ],
            "woodpecker" => vec![
                ReimportWorkflowQuiescenceV1::new(
                    InstallationSymbol::new("PipelineLifecycle").expect("workflow"),
                    2,
                    2,
                    0,
                )
                .expect("quiescent workflow"),
            ],
            _ => Vec::new(),
        };
        let source = ApplicationReimportSourceV1::new(
            expected_receipt.input().export_manifest_hash,
            ApplicationExportReceiptHash::from_bytes([0x70 + ordinal as u8; 32]),
            manifest.identity(),
            database(0x40 + ordinal as u8),
            expected_receipt.input().target_database_id,
            rows,
            vec![page_hash],
            workflow_quiescence,
        )
        .expect("exact source evidence");
        let campaign = ApplicationReimportCampaignV1::start(
            source,
            authority(1),
            CapabilityApplicationReimportScopeV1::WholeApplication,
            &manifest,
        )
        .expect("start campaign");

        // Simulate a process loss immediately after the durable start checkpoint.
        let mut recovered = ApplicationReimportCampaignV1::decode_canonical(
            &campaign.encode_canonical().expect("start checkpoint"),
        )
        .expect("recover start checkpoint");
        let outcomes = expected_receipt
            .input()
            .mappings
            .iter()
            .map(|mapping| {
                ReimportPageMappingOutcomeV1::new(
                    mapping.class(),
                    mapping.symbol().clone(),
                    mapping.records(),
                    false,
                    mapping.outcome_hash(),
                )
                .expect("compiler-owned mapping result")
            })
            .collect();
        recovered
            .complete_page(NonZeroU64::MIN, page_hash, outcomes)
            .expect("durable page");
        assert_eq!(
            recovered.phase(),
            ApplicationReimportCampaignPhaseV1::Reconciling
        );

        // A second process loss must resume at reconciliation, never replay page mutation.
        let mut recovered = ApplicationReimportCampaignV1::decode_canonical(
            &recovered.encode_canonical().expect("page checkpoint"),
        )
        .expect("recover page checkpoint");
        assert_eq!(recovered.rows_applied(), rows);
        let actual = recovered
            .reconcile(&manifest, expected_receipt.input().observations.clone())
            .expect("exact named-query reconciliation");
        assert_eq!(actual.identity(), expected_receipt.identity(), "{domain}");
        assert_eq!(
            ApplicationReimportCampaignV1::decode_canonical(
                &recovered.encode_canonical().expect("terminal checkpoint")
            )
            .expect("recover terminal checkpoint")
            .phase(),
            ApplicationReimportCampaignPhaseV1::Reconciled,
            "{domain}"
        );
    }
}
