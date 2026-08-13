#![forbid(unsafe_code)]

//! Deterministic generator for the four adapter-owned WP-569 conformance fixtures.

use std::collections::BTreeSet;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use riffdb_application::{
    AdapterConformanceManifest, AdapterConformanceManifestInput, AdapterConformanceProbe,
    AdapterDriverRequirement, AdapterEvolutionClass, AdapterEvolutionRequirement,
    AdapterFeatureClaim, AdapterFeatureDisposition, AdapterLimitation, AdapterPlatform,
    AdapterRoleRequirement, ApplicationInstallationCampaign, ApplicationInstallationCampaignState,
    ApplicationInstallationPlan, ApplicationInstallationPlanInput, CredentialDestination,
    InstallationArtifact, InstallationArtifactKind, InstallationContract, InstallationDriver,
    InstallationFeature, InstallationRole, InstallationStageEvidence, InstallationSymbol,
    InstallationTarget, InstalledCredentialEvidence, InstalledReimportEvidence,
    InstalledRoleEvidence, RoleOperation, RoleOperationKind, RoleWideningApproval,
};
use riffdb_query_module::{
    ApplicationLock, ApplicationManifest, GeneratedApplicationArtifactKind, ManifestRole,
};
use riffdb_types::{
    AdapterConformanceManifestHash, ApplicationRoleHash, CapabilityId, ContractLineage,
    ContractVersion, DatabaseAlias, Environment, GeneratedArtifactHash, hash_generated_artifact,
};
use serde_json::{Value, json};

const SCHEMA_OBSERVATION: &str = "riffdb.adapter-conformance-observation/v1";
const SCHEMA_EVOLUTION: &str = "riffdb.adapter-evolution-observation/v1";
const SCHEMA_FAILURE: &str = "riffdb.adapter-failure-classification/v1";

#[derive(Clone, Copy)]
struct ProbeSpec {
    name: &'static str,
    kind: RoleOperationKind,
    operation: &'static str,
    maximum_items: u32,
}

#[derive(Clone, Copy)]
struct AdapterSpec {
    name: &'static str,
    application_root: &'static str,
    primary_role: &'static str,
    required: &'static [InstallationFeature],
    probes: &'static [ProbeSpec],
}

const OPENFGA_REQUIRED: &[InstallationFeature] = &[
    InstallationFeature::RemoteTls,
    InstallationFeature::BulkCommands,
    InstallationFeature::OperationalQueries,
    InstallationFeature::InstallationCampaigns,
];
const BETTER_AUTH_REQUIRED: &[InstallationFeature] = &[
    InstallationFeature::RemoteTls,
    InstallationFeature::BulkCommands,
    InstallationFeature::OperationalQueries,
    InstallationFeature::WorkflowConcurrency,
    InstallationFeature::InstallationCampaigns,
    InstallationFeature::RowPolicies,
];
const PAYLOAD_REQUIRED: &[InstallationFeature] = OPENFGA_REQUIRED;
const MLFLOW_REQUIRED: &[InstallationFeature] = &[
    InstallationFeature::RemoteTls,
    InstallationFeature::OperationalQueries,
    InstallationFeature::WorkflowConcurrency,
    InstallationFeature::InstallationCampaigns,
];
const WOODPECKER_REQUIRED: &[InstallationFeature] = &[
    InstallationFeature::RemoteTls,
    InstallationFeature::WorkflowConcurrency,
    InstallationFeature::InstallationCampaigns,
];

const OPENFGA_PROBES: &[ProbeSpec] = &[
    ProbeSpec {
        name: "write-tuples",
        kind: RoleOperationKind::Command,
        operation: "WriteTuples",
        maximum_items: 64,
    },
    ProbeSpec {
        name: "list-tuples",
        kind: RoleOperationKind::Query,
        operation: "ListFgaTuples",
        maximum_items: 25,
    },
];
const BETTER_AUTH_PROBES: &[ProbeSpec] = &[
    ProbeSpec {
        name: "create-user-account-sessions",
        kind: RoleOperationKind::Command,
        operation: "CreateUserAccountSessions",
        maximum_items: 8,
    },
    ProbeSpec {
        name: "get-session",
        kind: RoleOperationKind::Query,
        operation: "GetSession",
        maximum_items: 1,
    },
];
const PAYLOAD_PROBES: &[ProbeSpec] = &[
    ProbeSpec {
        name: "create-documents",
        kind: RoleOperationKind::Command,
        operation: "CreateDocuments",
        maximum_items: 64,
    },
    ProbeSpec {
        name: "search-documents",
        kind: RoleOperationKind::Query,
        operation: "SearchDocuments",
        maximum_items: 25,
    },
];
const MLFLOW_PROBES: &[ProbeSpec] = &[
    ProbeSpec {
        name: "claim-run",
        kind: RoleOperationKind::Command,
        operation: "ClaimRun",
        maximum_items: 1,
    },
    ProbeSpec {
        name: "due-run",
        kind: RoleOperationKind::Query,
        operation: "DueRun",
        maximum_items: 1,
    },
];
const WOODPECKER_PROBES: &[ProbeSpec] = &[
    ProbeSpec {
        name: "claim-pipeline",
        kind: RoleOperationKind::Command,
        operation: "ClaimPipeline",
        maximum_items: 1,
    },
    ProbeSpec {
        name: "pipeline-transitions",
        kind: RoleOperationKind::EventStream,
        operation: "PipelineTransitions",
        maximum_items: 32,
    },
];

const ADAPTERS: &[AdapterSpec] = &[
    AdapterSpec {
        name: "openfga",
        application_root: "fixtures/adapters/operational-conformance",
        primary_role: "AdapterOperationalApplication",
        required: OPENFGA_REQUIRED,
        probes: OPENFGA_PROBES,
    },
    AdapterSpec {
        name: "mlflow",
        application_root: "fixtures/adapters/mlflow",
        primary_role: "MlflowSchedulerWorker",
        required: MLFLOW_REQUIRED,
        probes: MLFLOW_PROBES,
    },
    AdapterSpec {
        name: "better-auth",
        application_root: "fixtures/adapters/better-auth",
        primary_role: "BetterAuthApplication",
        required: BETTER_AUTH_REQUIRED,
        probes: BETTER_AUTH_PROBES,
    },
    AdapterSpec {
        name: "woodpecker",
        application_root: "fixtures/adapters/woodpecker",
        primary_role: "WoodpeckerSchedulerWorker",
        required: WOODPECKER_REQUIRED,
        probes: WOODPECKER_PROBES,
    },
    // Retained post-alpha regression after Better Auth replaced Payload in the gate.
    AdapterSpec {
        name: "payload",
        application_root: "fixtures/adapters/operational-conformance",
        primary_role: "AdapterOperationalApplication",
        required: PAYLOAD_REQUIRED,
        probes: PAYLOAD_PROBES,
    },
];

fn main() {
    let mut arguments = env::args_os().skip(1);
    let repository = arguments
        .next()
        .map(PathBuf::from)
        .unwrap_or_else(|| panic!("usage: generate_adapter_conformance REPOSITORY OUTPUT_ROOT"));
    let output = arguments
        .next()
        .map(PathBuf::from)
        .unwrap_or_else(|| panic!("usage: generate_adapter_conformance REPOSITORY OUTPUT_ROOT"));
    assert!(arguments.next().is_none(), "unexpected generator argument");

    for (index, spec) in ADAPTERS.iter().enumerate() {
        generate_adapter(&repository, &output, *spec, index as u8 + 1);
    }
}

fn generate_adapter(repository: &Path, output: &Path, spec: AdapterSpec, ordinal: u8) {
    let application_root = repository.join(spec.application_root);
    let manifest = ApplicationManifest::decode_canonical(
        &fs::read(application_root.join("generated/riffdb.application.exact.json"))
            .expect("read exact application manifest"),
    )
    .expect("decode exact application manifest");
    let lock_bytes = fs::read(application_root.join("riffdb.application.lock.json"))
        .expect("read application lock");
    let lock = ApplicationLock::decode_canonical(&lock_bytes).expect("decode application lock");
    assert_eq!(manifest.identity(), lock.manifest_hash());
    let lock_json: Value = serde_json::from_slice(&lock_bytes).expect("lock JSON");

    let application_artifacts = installation_artifacts(&manifest, &lock);
    let role_requirements = manifest
        .roles()
        .iter()
        .map(|role| {
            AdapterRoleRequirement::new(
                symbol(role.name()),
                role_hash(&lock_json, role.name()),
                role_operations(role),
            )
            .expect("adapter role requirement")
        })
        .collect::<Vec<_>>();
    let observation = canonical_json(json!({
        "adapter": spec.name,
        "languages": ["go", "python", "rust", "typescript"],
        "operations": spec.probes.iter().map(|probe| json!({
            "maximum_items": probe.maximum_items,
            "name": probe.name,
            "operation": probe.operation,
            "result": "passed",
        })).collect::<Vec<_>>(),
        "schema": SCHEMA_OBSERVATION,
        "transport": "public_tls",
    }));
    let observation_hash = hash_generated_artifact(&observation);
    let evolution_observation = canonical_json(json!({
        "adapter": spec.name,
        "credential_rotation": "successor_proved_before_predecessor_revocation",
        "empty_install": "installed",
        "populated_compatible_upgrade": "installed",
        "role_widening": "explicit_exact_diff",
        "schema": SCHEMA_EVOLUTION,
    }));
    let evolution_hash = hash_generated_artifact(&evolution_observation);
    let drivers = driver_requirements(observation_hash);
    let contract = InstallationContract::new(
        ContractVersion::new(manifest.contract().version()).expect("contract version"),
        manifest.contract().bundle_hash(),
    );
    let feature_claims = InstallationFeature::ALL
        .into_iter()
        .map(|feature| {
            if spec.required.contains(&feature) {
                AdapterFeatureClaim::required(feature)
            } else if matches!(
                feature,
                InstallationFeature::RowPolicies | InstallationFeature::DataLifecycle
            ) {
                AdapterFeatureClaim::unavailable(feature, AdapterLimitation::ProductUnavailable)
            } else {
                AdapterFeatureClaim::new(feature, AdapterFeatureDisposition::Optional, None)
                    .expect("optional feature")
            }
        })
        .collect::<Vec<_>>();
    let adapter_manifest =
        AdapterConformanceManifest::compile_v2(AdapterConformanceManifestInput {
            adapter: symbol(spec.name),
            adapter_version: symbol("v0.1.0"),
            application_manifest_hash: manifest.identity(),
            application_lock_hash: lock.identity(),
            contract_lineage: ContractLineage::new(manifest.contract().lineage()).expect("lineage"),
            feature_claims,
            artifacts: application_artifacts.clone(),
            roles: role_requirements,
            drivers: drivers.clone(),
            conformance: spec
                .probes
                .iter()
                .map(|probe| {
                    AdapterConformanceProbe::new(
                        symbol(probe.name),
                        symbol(spec.primary_role),
                        operation(probe.kind, probe.operation),
                        observation_hash,
                        probe.maximum_items,
                    )
                    .expect("bounded conformance probe")
                })
                .collect(),
            evolution: vec![
                AdapterEvolutionRequirement::new(
                    AdapterEvolutionClass::EmptyInstall,
                    None,
                    contract,
                    None,
                    evolution_hash,
                )
                .expect("empty installation case"),
                AdapterEvolutionRequirement::new(
                    AdapterEvolutionClass::CompatibleUpgrade,
                    Some(contract),
                    contract,
                    None,
                    evolution_hash,
                )
                .expect("same-contract application evolution"),
            ],
        })
        .expect("compile adapter conformance manifest");

    let empty_plan = installation_plan(
        spec,
        ordinal,
        &manifest,
        &lock,
        application_artifacts.clone(),
        adapter_manifest.identity(),
        false,
    );
    let upgrade_plan = installation_plan(
        spec,
        ordinal,
        &manifest,
        &lock,
        application_artifacts,
        adapter_manifest.identity(),
        true,
    );
    adapter_manifest
        .validate_installation_plan(&empty_plan)
        .expect("empty plan is manifest-bound");
    adapter_manifest
        .validate_installation_plan(&upgrade_plan)
        .expect("upgrade plan is manifest-bound");

    let empty_receipt = seal_campaign(&empty_plan, capability(ordinal, 0x40));
    let upgrade_receipt = seal_campaign(&upgrade_plan, capability(ordinal, 0x50));
    let failed_state = failed_campaign(&upgrade_plan, capability(ordinal, 0x60));
    let failure_hash = hash_generated_artifact(failed_state.canonical_bytes());
    let failure_classification = canonical_json(json!({
        "action": "inspect_remote_identity_and_resume_same_campaign",
        "adapter": spec.name,
        "campaign_state_hash": hex(failure_hash.as_bytes()),
        "classification": "riffdb_product_defect",
        "code": "remote_identity_mismatch",
        "immutable": true,
        "schema": SCHEMA_FAILURE,
    }));

    let root = output.join("fixtures/adapters/conformance").join(spec.name);
    write(
        &root.join("adapter.conformance.json"),
        adapter_manifest.canonical_bytes(),
    );
    write(
        &root.join("empty-install-plan.json"),
        empty_plan.canonical_bytes(),
    );
    write(
        &root.join("upgrade-plan.json"),
        upgrade_plan.canonical_bytes(),
    );
    write(
        &root.join("empty-install-receipt.json"),
        empty_receipt.canonical_bytes(),
    );
    write(
        &root.join("upgrade-receipt.json"),
        upgrade_receipt.canonical_bytes(),
    );
    write(&root.join("operation-observation.json"), &observation);
    write(
        &root.join("evolution-observation.json"),
        &evolution_observation,
    );
    write(
        &root.join("failed-campaign.json"),
        failed_state.canonical_bytes(),
    );
    write(
        &root.join("failure-classification.json"),
        &failure_classification,
    );
}

fn installation_plan(
    spec: AdapterSpec,
    ordinal: u8,
    manifest: &ApplicationManifest,
    lock: &ApplicationLock,
    mut artifacts: Vec<InstallationArtifact>,
    adapter_hash: AdapterConformanceManifestHash,
    upgrade: bool,
) -> ApplicationInstallationPlan {
    artifacts.push(InstallationArtifact::new(
        InstallationArtifactKind::AdapterManifest,
        symbol(spec.name),
        GeneratedArtifactHash::from_bytes(*adapter_hash.as_bytes()),
    ));
    let lock_json: Value = serde_json::from_slice(lock.canonical_bytes()).expect("lock JSON");
    let roles = manifest
        .roles()
        .iter()
        .map(|role| installation_role(spec.name, role, &lock_json, upgrade))
        .collect::<Vec<_>>();
    let credentials = manifest
        .roles()
        .iter()
        .enumerate()
        .map(|(index, role)| {
            CredentialDestination::new(
                symbol(&format!("{}-runtime-{}", spec.name, index + 1)),
                symbol(role.name()),
                upgrade.then(|| capability(ordinal, index as u8 + 1)),
                capability(ordinal, index as u8 + 0x20),
            )
            .expect("credential destination")
        })
        .collect();
    ApplicationInstallationPlan::compile(ApplicationInstallationPlanInput {
        application: symbol(manifest.application_name()),
        source_hash: lock.source_hash(),
        lock_hash: lock.identity(),
        manifest_hash: manifest.identity(),
        target: InstallationTarget::new(
            DatabaseAlias::new(spec.name).expect("database alias"),
            Environment::new("development").expect("environment"),
            ContractLineage::new(manifest.contract().lineage()).expect("lineage"),
        ),
        contract: InstallationContract::new(
            ContractVersion::new(manifest.contract().version()).expect("contract version"),
            manifest.contract().bundle_hash(),
        ),
        artifacts,
        migration: None,
        roles,
        reimport: None,
        credential_destinations: credentials,
        drivers: vec![
            InstallationDriver::Rust,
            InstallationDriver::Go,
            InstallationDriver::TypeScript,
            InstallationDriver::Python,
        ],
        seeds: vec![],
        required_features: spec.required.to_vec(),
        adapter_manifest_hash: Some(adapter_hash),
    })
    .expect("compile exact adapter installation plan")
}

fn installation_role(
    adapter: &str,
    role: &ManifestRole,
    lock_json: &Value,
    upgrade: bool,
) -> InstallationRole {
    let desired = role_operations(role);
    let previous = if upgrade {
        desired[..desired.len().saturating_sub(1)].to_vec()
    } else {
        Vec::new()
    };
    let previous_hash = upgrade.then(|| {
        ApplicationRoleHash::from_bytes(
            *hash_generated_artifact(
                format!(
                    "riffdb.adapter-role-predecessor/v1\n{adapter}\n{}\n",
                    role.name()
                )
                .as_bytes(),
            )
            .as_bytes(),
        )
    });
    let approval = previous_hash.map(|hash| {
        let previous_set = previous.iter().cloned().collect::<BTreeSet<_>>();
        let additions = desired
            .iter()
            .filter(|operation| !previous_set.contains(*operation))
            .cloned()
            .collect();
        RoleWideningApproval::new(hash, additions).expect("exact role widening")
    });
    InstallationRole::new(
        symbol(role.name()),
        role_hash(lock_json, role.name()),
        previous_hash,
        desired,
        previous,
        approval,
    )
    .expect("installation role")
}

fn installation_artifacts(
    manifest: &ApplicationManifest,
    lock: &ApplicationLock,
) -> Vec<InstallationArtifact> {
    let mut artifacts = lock
        .artifacts()
        .iter()
        .filter_map(|artifact| {
            let (kind, name) = match artifact.kind() {
                GeneratedApplicationArtifactKind::Manifest => {
                    (InstallationArtifactKind::Manifest, "manifest")
                }
                GeneratedApplicationArtifactKind::ContractBundle => {
                    (InstallationArtifactKind::ContractBundle, "contract")
                }
                GeneratedApplicationArtifactKind::Rust => (InstallationArtifactKind::Rust, "rust"),
                GeneratedApplicationArtifactKind::TypeScript => {
                    (InstallationArtifactKind::TypeScript, "typescript")
                }
                GeneratedApplicationArtifactKind::Go => (InstallationArtifactKind::Go, "go"),
                GeneratedApplicationArtifactKind::Python => {
                    (InstallationArtifactKind::Python, "python")
                }
                GeneratedApplicationArtifactKind::Mcp => (InstallationArtifactKind::Mcp, "mcp"),
                GeneratedApplicationArtifactKind::ReactiveModule => return None,
            };
            Some(InstallationArtifact::new(
                kind,
                symbol(name),
                artifact.content_hash(),
            ))
        })
        .collect::<Vec<_>>();
    artifacts.extend(manifest.query_modules().iter().map(|module| {
        InstallationArtifact::new(
            InstallationArtifactKind::QueryModule,
            symbol(module.name()),
            GeneratedArtifactHash::from_bytes(*module.module_hash().as_bytes()),
        )
    }));
    artifacts.extend(manifest.reactive_modules().iter().map(|module| {
        InstallationArtifact::new(
            InstallationArtifactKind::ReactiveModule,
            symbol(module.name()),
            GeneratedArtifactHash::from_bytes(*module.module_hash().as_bytes()),
        )
    }));
    artifacts
}

fn role_operations(role: &ManifestRole) -> Vec<RoleOperation> {
    let mut operations = Vec::new();
    operations.extend(
        role.queries()
            .iter()
            .map(|name| operation(RoleOperationKind::Query, name)),
    );
    operations.extend(
        role.commands()
            .iter()
            .map(|name| operation(RoleOperationKind::Command, name)),
    );
    operations.extend(
        role.event_streams()
            .iter()
            .map(|name| operation(RoleOperationKind::EventStream, name)),
    );
    operations.extend(
        role.watch_queries()
            .iter()
            .map(|name| operation(RoleOperationKind::QueryWatch, name)),
    );
    operations.extend(
        role.agent_subscriptions()
            .iter()
            .map(|name| operation(RoleOperationKind::AgentSubscription, name)),
    );
    operations
}

fn role_hash(lock: &Value, role: &str) -> ApplicationRoleHash {
    let value = lock["roles"]
        .as_array()
        .expect("lock roles")
        .iter()
        .find(|value| value["definition"]["name"] == role)
        .and_then(|value| value["definition_hash"].as_str())
        .expect("role definition hash");
    ApplicationRoleHash::from_bytes(parse_hash(value))
}

fn driver_requirements(hash: GeneratedArtifactHash) -> Vec<AdapterDriverRequirement> {
    [
        (InstallationDriver::Rust, "rust1.97.0"),
        (InstallationDriver::Go, "go1.24"),
        (InstallationDriver::TypeScript, "node22"),
        (InstallationDriver::Python, "python3.13"),
    ]
    .into_iter()
    .map(|(driver, runtime)| {
        AdapterDriverRequirement::new(
            driver,
            symbol(runtime),
            vec![AdapterPlatform::LinuxX86_64Gnu],
            hash,
        )
        .expect("driver requirement")
    })
    .collect()
}

fn seal_campaign(
    plan: &ApplicationInstallationPlan,
    campaign_id: CapabilityId,
) -> riffdb_application::ApplicationInstallationReceipt {
    let campaign_id =
        riffdb_types::ApplicationInstallationCampaignId::from_bytes(campaign_id.into_bytes())
            .expect("campaign UUIDv7");
    let input = plan.input();
    let mut campaign = ApplicationInstallationCampaign::start(campaign_id, plan.identity());
    let stages = [
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
            migration_hash: None,
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
        InstallationStageEvidence::Reimport(InstalledReimportEvidence::NotRequired),
        InstallationStageEvidence::Credentials(
            input
                .credential_destinations
                .iter()
                .map(|credential| {
                    InstalledCredentialEvidence::new(
                        credential.name().clone(),
                        credential.successor(),
                    )
                })
                .collect(),
        ),
        InstallationStageEvidence::DriverProof(input.drivers.clone()),
        InstallationStageEvidence::Seeds(vec![]),
    ];
    for stage in stages {
        campaign
            .complete_stage(plan, stage)
            .expect("complete exact campaign stage");
    }
    campaign.seal_receipt(plan).expect("seal terminal receipt")
}

fn failed_campaign(
    plan: &ApplicationInstallationPlan,
    campaign_id: CapabilityId,
) -> ApplicationInstallationCampaignState {
    let campaign_id =
        riffdb_types::ApplicationInstallationCampaignId::from_bytes(campaign_id.into_bytes())
            .expect("campaign UUIDv7");
    let mut campaign = ApplicationInstallationCampaign::start(campaign_id, plan.identity());
    campaign
        .complete_stage(
            plan,
            InstallationStageEvidence::Preflight {
                source_hash: plan.input().source_hash,
                lock_hash: plan.input().lock_hash,
                manifest_hash: plan.input().manifest_hash,
            },
        )
        .expect("preflight");
    campaign
        .record_failure(
            plan,
            riffdb_application::InstallationFailureCode::RemoteIdentityMismatch,
        )
        .expect("typed partial failure");
    ApplicationInstallationCampaignState::capture(&campaign, plan).expect("capture failed campaign")
}

fn capability(adapter: u8, purpose: u8) -> CapabilityId {
    CapabilityId::from_bytes([
        0, 0, 0, 0, adapter, purpose, 0x70, 0, 0x80, 0, 0, 0, 0, 0, adapter, purpose,
    ])
    .expect("stable UUIDv7")
}

fn operation(kind: RoleOperationKind, name: &str) -> RoleOperation {
    RoleOperation::new(kind, symbol(name))
}

fn symbol(value: &str) -> InstallationSymbol {
    InstallationSymbol::new(value).expect("bounded symbolic name")
}

fn canonical_json(value: Value) -> Vec<u8> {
    let mut bytes = serde_json::to_vec(&value).expect("canonical JSON");
    bytes.push(b'\n');
    bytes
}

fn write(path: &Path, bytes: &[u8]) {
    fs::create_dir_all(path.parent().expect("fixture parent")).expect("create fixture parent");
    fs::write(path, bytes).expect("write fixture");
}

fn parse_hash(value: &str) -> [u8; 32] {
    assert_eq!(value.len(), 64, "hash length");
    let mut bytes = [0_u8; 32];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16).expect("lower hash");
    }
    assert_eq!(hex(&bytes), value, "hash must be canonical lowercase");
    bytes
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
