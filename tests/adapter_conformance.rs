#![forbid(unsafe_code)]

//! Four-domain semantic acceptance for adapter-owned installation evidence.

use std::fs;
use std::path::{Path, PathBuf};

use riffdb_application::{
    ADAPTER_CONFORMANCE_MANIFEST_SCHEMA_V2, AdapterConformanceErrorKind,
    AdapterConformanceManifest, ApplicationInstallationCampaignState, ApplicationInstallationPlan,
    ApplicationInstallationReceipt, InstallationCampaignPhase, InstallationFailureCode,
    InstallationFeature,
};
use riffdb_types::hash_generated_artifact;

const DOMAINS: &[&str] = &["openfga", "mlflow", "better-auth", "woodpecker"];

#[test]
fn four_domain_manifests_bind_empty_and_populated_evolution_receipts() {
    for domain in DOMAINS {
        let root = fixture_root().join(domain);
        let manifest = decode_manifest(&root);
        let empty = decode_plan(&root, "empty-install-plan.json");
        let upgrade = decode_plan(&root, "upgrade-plan.json");
        manifest
            .validate_installation_plan(&empty)
            .expect("empty installation plan is exactly manifest-bound");
        manifest
            .validate_installation_plan(&upgrade)
            .expect("populated evolution plan is exactly manifest-bound");

        assert_eq!(manifest.schema(), ADAPTER_CONFORMANCE_MANIFEST_SCHEMA_V2);
        assert_eq!(
            manifest.input().feature_claims.len(),
            InstallationFeature::ALL.len()
        );
        assert!(empty.input().roles.iter().all(|role| {
            role.previous_role_hash().is_none() && role.widening_approval().is_none()
        }));
        assert!(
            empty
                .input()
                .credential_destinations
                .iter()
                .all(|credential| { credential.expected_current().is_none() })
        );
        assert!(upgrade.input().roles.iter().all(|role| {
            role.previous_role_hash().is_some() && role.widening_approval().is_some()
        }));
        assert!(
            upgrade
                .input()
                .credential_destinations
                .iter()
                .all(|credential| {
                    credential.expected_current().is_some()
                        && credential.expected_current() != Some(credential.successor())
                })
        );

        verify_receipt(&root, "empty-install-receipt.json", &empty, &manifest);
        verify_receipt(&root, "upgrade-receipt.json", &upgrade, &manifest);
    }
}

#[test]
fn every_claim_has_exact_public_observation_and_closed_catalog_support() {
    let supported_features = [
        InstallationFeature::RemoteTls,
        InstallationFeature::BulkCommands,
        InstallationFeature::OperationalQueries,
        InstallationFeature::WorkflowConcurrency,
        InstallationFeature::InstallationCampaigns,
        InstallationFeature::RowPolicies,
    ];
    for domain in DOMAINS {
        let root = fixture_root().join(domain);
        let manifest = decode_manifest(&root);
        let operation_hash = hash_generated_artifact(
            &fs::read(root.join("operation-observation.json")).expect("operation observation"),
        );
        let evolution_hash = hash_generated_artifact(
            &fs::read(root.join("evolution-observation.json")).expect("evolution observation"),
        );
        assert!(manifest.input().conformance.iter().all(|probe| {
            probe.expected_observation_hash() == operation_hash
                && probe.maximum_items() > 0
                && probe.maximum_items() <= riffdb_application::MAX_ADAPTER_PROBE_ITEMS
        }));
        assert!(manifest.input().evolution.iter().all(|evolution| {
            evolution.expected_observation_hash() == evolution_hash
                && evolution.migration_hash().is_none()
        }));
        manifest
            .validate_catalog(&supported_features, &manifest.input().drivers)
            .expect("required released features and exact drivers are available");

        let missing_required = supported_features
            .iter()
            .copied()
            .filter(|feature| {
                !manifest.input().feature_claims.iter().any(|claim| {
                    claim.feature() == *feature
                        && claim.disposition()
                            == riffdb_application::AdapterFeatureDisposition::Required
                })
            })
            .collect::<Vec<_>>();
        assert_eq!(
            manifest
                .validate_catalog(&missing_required, &manifest.input().drivers)
                .expect_err("required feature absence is a typed product gap")
                .kind(),
            AdapterConformanceErrorKind::UnsupportedFeature
        );
        assert_eq!(
            manifest
                .validate_catalog(&supported_features, &[])
                .expect_err("driver/platform absence is a typed platform gap")
                .kind(),
            AdapterConformanceErrorKind::UnsupportedDriver
        );
    }
}

#[test]
fn failed_campaign_evidence_is_immutable_partial_and_actionable() {
    for domain in DOMAINS {
        let root = fixture_root().join(domain);
        let state_bytes = fs::read(root.join("failed-campaign.json")).expect("failed campaign");
        let state = ApplicationInstallationCampaignState::decode_canonical(&state_bytes)
            .expect("canonical failed campaign");
        let observation = state.campaign().observe();
        assert_eq!(observation.phase(), InstallationCampaignPhase::Partial);
        assert_eq!(
            observation.failure().expect("typed failure").code(),
            InstallationFailureCode::RemoteIdentityMismatch
        );
        assert_eq!(observation.receipt_hash(), None);

        let classification_bytes =
            fs::read(root.join("failure-classification.json")).expect("classification");
        let classification: serde_json::Value =
            serde_json::from_slice(&classification_bytes).expect("classification JSON");
        assert_eq!(
            classification["schema"],
            "riffdb.adapter-failure-classification/v1"
        );
        assert_eq!(classification["adapter"], *domain);
        assert_eq!(classification["immutable"], true);
        assert_eq!(classification["classification"], "riffdb_product_defect");
        assert_eq!(
            classification["action"],
            "inspect_remote_identity_and_resume_same_campaign"
        );
        assert_eq!(
            classification["campaign_state_hash"],
            hex(hash_generated_artifact(&state_bytes).as_bytes())
        );
    }
}

#[test]
fn conformance_data_has_no_kernel_permission_hook_or_fallback_channel() {
    let forbidden = [
        "entity_type_id",
        "field_id",
        "index_id",
        "raw_permissions",
        "storage_access",
        "kernel_access",
        "server_hook",
        "shell",
        "method_path",
        "version_range",
        "fallback",
    ];
    for domain in DOMAINS {
        let bytes = fs::read(fixture_root().join(domain).join("adapter.conformance.json"))
            .expect("manifest bytes");
        let source = std::str::from_utf8(&bytes).expect("manifest UTF-8");
        for token in forbidden {
            assert!(
                !source.contains(token),
                "{domain} manifest exposed forbidden channel {token}"
            );
        }
    }
}

fn verify_receipt(
    root: &Path,
    file: &str,
    plan: &ApplicationInstallationPlan,
    manifest: &AdapterConformanceManifest,
) {
    let bytes = fs::read(root.join(file)).expect("installation receipt");
    let receipt = ApplicationInstallationReceipt::decode_canonical(&bytes)
        .expect("canonical installation receipt");
    assert_eq!(receipt.plan_hash(), plan.identity());
    let value: serde_json::Value = serde_json::from_slice(&bytes).expect("receipt JSON");
    assert_eq!(
        value["adapter_manifest_hash"],
        hex(manifest.identity().as_bytes())
    );
    assert_eq!(value["terminal_state"], "installed");
    assert!(value["credentials"].as_array().is_some_and(|credentials| {
        !credentials.is_empty()
            && credentials.iter().all(|credential| {
                credential.get("destination").is_some()
                    && credential.get("capability_id").is_some()
                    && credential.get("bearer").is_none()
                    && credential.get("token").is_none()
            })
    }));
}

fn decode_manifest(root: &Path) -> AdapterConformanceManifest {
    AdapterConformanceManifest::decode_canonical(
        &fs::read(root.join("adapter.conformance.json")).expect("adapter manifest"),
    )
    .expect("canonical current adapter manifest")
}

fn decode_plan(root: &Path, name: &str) -> ApplicationInstallationPlan {
    ApplicationInstallationPlan::decode_canonical(
        &fs::read(root.join(name)).expect("installation plan"),
    )
    .expect("canonical installation plan")
}

fn fixture_root() -> PathBuf {
    if let Some(configured) = std::env::var_os("RIFFDB_ADAPTER_CONFORMANCE_FIXTURE_ROOT") {
        let root = PathBuf::from(configured);
        assert!(
            root.is_absolute() && root.is_dir() && !root.is_symlink(),
            "configured adapter conformance fixture root must be an absolute non-symlink directory"
        );
        return root;
    }
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("fixtures/adapters/conformance")
}

fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}
