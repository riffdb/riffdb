#![forbid(unsafe_code)]
//! Deterministic generator for the adapter portability manifests and receipts.

use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use riffdb_application::{
    AdapterConformanceManifest, ApplicationPortabilityManifest,
    ApplicationPortabilityManifestInput, ApplicationReimportReceipt, InstallationArtifactKind,
    InstallationSymbol, PortableOmission, PortableOmissionClass, PortableOmissionReason,
    PortableRecordClass, PortableRecordMapping, PortableReimportStrategy, ReimportMappingResult,
    ReimportObservation, ReimportObservationParameter, ReimportObservationResult,
};
use riffdb_contract_ir::ContractBundle;
use riffdb_types::{
    ApplicationExportManifestHash, CanonicalValue, DatabaseId, QueryModuleHash,
    hash_generated_artifact,
};

struct Domain<'a> {
    name: &'a str,
    application_root: &'a str,
    mappings: &'a [(&'a str, &'a str)],
    query: &'a str,
    parameters: &'a [(&'a str, u16)],
    empty_collection: Option<&'a str>,
    seed: u8,
}

const DOMAINS: [Domain<'static>; 5] = [
    Domain {
        name: "openfga",
        application_root: "fixtures/adapters/operational-conformance",
        mappings: &[("FgaTuple", "ReconstituteFgaTuples")],
        query: "ListFgaTuples",
        parameters: &[("store_id", 10)],
        empty_collection: Some("tuples"),
        seed: 1,
    },
    Domain {
        name: "better-auth",
        application_root: "fixtures/adapters/better-auth",
        mappings: &[
            ("User", "ReconstituteUsers"),
            ("Account", "ReconstituteAccounts"),
            ("Session", "ReconstituteSessions"),
            ("VerificationToken", "ReconstituteVerificationTokens"),
        ],
        query: "GetSession",
        parameters: &[("organization_id", 30), ("user_id", 31), ("session_id", 32)],
        empty_collection: None,
        seed: 2,
    },
    Domain {
        name: "mlflow",
        application_root: "fixtures/adapters/mlflow",
        mappings: &[("ScheduledRun", "ReconstituteScheduledRuns")],
        query: "DueRun",
        parameters: &[("organization_id", 50), ("run_id", 51)],
        empty_collection: None,
        seed: 3,
    },
    Domain {
        name: "woodpecker",
        application_root: "fixtures/adapters/woodpecker",
        mappings: &[("ScheduledPipeline", "ReconstituteScheduledPipelines")],
        query: "GetPipeline",
        parameters: &[("organization_id", 60), ("pipeline_id", 61)],
        empty_collection: None,
        seed: 4,
    },
    Domain {
        name: "payload",
        application_root: "fixtures/adapters/operational-conformance",
        mappings: &[("Document", "ReconstituteDocuments")],
        query: "ListDraftDocuments",
        parameters: &[("site_id", 40)],
        empty_collection: Some("documents"),
        seed: 5,
    },
];

fn main() {
    let write = env::args().nth(1).as_deref() == Some("--write");
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    for domain in DOMAINS {
        generate_domain(&root, &domain, write);
    }
}

fn generate_domain(root: &Path, domain: &Domain<'_>, write: bool) {
    let bundle = ContractBundle::decode(
        &fs::read(
            root.join(domain.application_root)
                .join("generated/riffdb.contract.bundle"),
        )
        .expect("read adapter contract bundle"),
    )
    .expect("decode adapter contract bundle");
    let adapter = AdapterConformanceManifest::decode_canonical(
        &fs::read(
            root.join("fixtures/adapters/conformance")
                .join(domain.name)
                .join("adapter.conformance.json"),
        )
        .expect("read adapter manifest"),
    )
    .expect("decode adapter manifest");
    let observation_hash = empty_observation_hash(domain.empty_collection);
    let module_hash = adapter
        .input()
        .artifacts
        .iter()
        .find(|artifact| artifact.kind() == InstallationArtifactKind::QueryModule)
        .map(|artifact| QueryModuleHash::from_bytes(*artifact.content_hash().as_bytes()))
        .expect("adapter query module");
    let manifest = ApplicationPortabilityManifest::compile(ApplicationPortabilityManifestInput {
        adapter_manifest_hash: adapter.identity(),
        contract_lineage: bundle.lineage().clone(),
        contract_version: bundle.contract_version(),
        contract_bundle_hash: bundle.bundle_hash(),
        mappings: domain
            .mappings
            .iter()
            .map(|(entity, command)| {
                PortableRecordMapping::new(
                    PortableRecordClass::Entity,
                    symbol(entity),
                    PortableReimportStrategy::reimport_command(symbol(command)),
                )
            })
            .collect(),
        omissions: vec![
            PortableOmission::new(
                PortableOmissionClass::Provenance,
                None,
                PortableOmissionReason::HistoricalRecordsNotRegenerable,
            )
            .expect("provenance omission"),
            PortableOmission::new(
                PortableOmissionClass::PublicAudit,
                None,
                PortableOmissionReason::HistoricalRecordsNotRegenerable,
            )
            .expect("audit omission"),
        ],
        observations: vec![
            ReimportObservation::new_with_parameters(
                symbol("application_observation"),
                symbol(domain.query),
                module_hash,
                domain
                    .parameters
                    .iter()
                    .map(|(name, suffix)| {
                        ReimportObservationParameter::new(
                            symbol(name),
                            CanonicalValue::Uuid(application_uuid(*suffix)),
                        )
                        .expect("observation parameter")
                    })
                    .collect(),
                observation_hash,
                500,
            )
            .expect("observation"),
        ],
    })
    .expect("compile portability manifest");
    manifest
        .validate_compiled_contract(&bundle)
        .expect("compiled contract mapping");
    assert_eq!(manifest.input().adapter_manifest_hash, adapter.identity());
    assert_eq!(
        manifest.input().contract_lineage,
        adapter.input().contract_lineage
    );
    assert!(adapter.input().evolution.iter().any(|evolution| {
        evolution.successor().version() == manifest.input().contract_version
            && evolution.successor().bundle_hash() == manifest.input().contract_bundle_hash
    }));
    manifest
        .validate_adapter_conformance(&adapter)
        .unwrap_or_else(|error| panic!("{} adapter mapping: {error:?}", domain.name));

    let outcome_hash = hash_generated_artifact(format!("{}-outcomes-v1", domain.name).as_bytes());
    let receipt = ApplicationReimportReceipt::reconcile(
        &manifest,
        ApplicationExportManifestHash::from_bytes(
            *hash_generated_artifact(format!("{}-export-v1", domain.name).as_bytes()).as_bytes(),
        ),
        database_id(domain.seed),
        domain
            .mappings
            .iter()
            .map(|(entity, _)| {
                ReimportMappingResult::new(
                    PortableRecordClass::Entity,
                    symbol(entity),
                    2,
                    2,
                    0,
                    outcome_hash,
                )
                .expect("mapping result")
            })
            .collect(),
        vec![ReimportObservationResult::new(
            symbol("application_observation"),
            observation_hash,
        )],
    )
    .expect("reimport receipt");

    let destination = root.join("fixtures/export").join(domain.name);
    update(
        destination.join("portability-manifest-v3.json"),
        manifest.canonical_bytes(),
        write,
    );
    update(
        destination.join("reimport-receipt-v2.json"),
        receipt.canonical_bytes(),
        write,
    );
}

fn symbol(value: &str) -> InstallationSymbol {
    InstallationSymbol::new(value).expect("fixture symbol")
}

fn database_id(seed: u8) -> DatabaseId {
    DatabaseId::from_bytes([0, 0, 0, 0, 0, seed, 0x70, 0, 0x80, 0, 0, 0, 0, 0, 0, seed])
        .expect("fixture database ID")
}

fn application_uuid(suffix: u16) -> [u8; 16] {
    let mut value = [
        0x01, 0x8f, 0x0f, 0x8b, 0x7c, 0x6d, 0x7e, 0x31, 0x8a, 0x4f, 0, 0, 0, 0, 0, 0,
    ];
    value[14..].copy_from_slice(&suffix.to_be_bytes());
    value
}

fn empty_observation_hash(collection: Option<&str>) -> riffdb_types::GeneratedArtifactHash {
    let mut bytes = b"riffdb.reimport-observation/v1\0".to_vec();
    let outcome = if collection.is_some() {
        "Found"
    } else {
        "Missing"
    };
    push_bytes(&mut bytes, outcome.as_bytes());
    match collection {
        Some(name) => {
            bytes.extend_from_slice(&1_u32.to_be_bytes());
            push_bytes(&mut bytes, name.as_bytes());
            bytes.push(4); // SymbolicResultField::Many.
            bytes.extend_from_slice(&0_u32.to_be_bytes());
        }
        None => bytes.extend_from_slice(&0_u32.to_be_bytes()),
    }
    hash_generated_artifact(&bytes)
}

fn push_bytes(target: &mut Vec<u8>, value: &[u8]) {
    target.extend_from_slice(
        &u32::try_from(value.len())
            .expect("fixture value length")
            .to_be_bytes(),
    );
    target.extend_from_slice(value);
}

fn update(path: PathBuf, expected: &[u8], write: bool) {
    if write {
        fs::create_dir_all(path.parent().expect("fixture parent")).expect("create fixture parent");
        fs::write(&path, expected).expect("write fixture");
        return;
    }
    let actual = fs::read(&path).unwrap_or_default();
    assert_eq!(actual, expected, "stale fixture: {}", path.display());
}
