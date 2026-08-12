#![forbid(unsafe_code)]
//! Deterministic generator for the adapter portability manifests and receipts.

use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use riffdb_application::{
    AdapterConformanceManifest, ApplicationPortabilityManifest,
    ApplicationPortabilityManifestInput, ApplicationReimportReceipt, InstallationSymbol,
    PortableOmission, PortableOmissionClass, PortableOmissionReason, PortableRecordClass,
    PortableRecordMapping, PortableReimportStrategy, ReimportMappingResult, ReimportObservation,
    ReimportObservationResult,
};
use riffdb_contract_ir::ContractBundle;
use riffdb_types::{ApplicationExportManifestHash, DatabaseId, hash_generated_artifact};

struct Domain<'a> {
    name: &'a str,
    application_root: &'a str,
    entity: &'a str,
    command: &'a str,
    record_input: &'a str,
    query: &'a str,
    seed: u8,
}

// The two workflow adapters are intentionally absent until RiffDB has an
// accepted compiler-owned way to reconstruct workflow state without admitting
// an ordinary state-field write. The all-domain acceptance gate remains red.
const DOMAINS: [Domain<'static>; 2] = [
    Domain {
        name: "openfga",
        application_root: "fixtures/adapters/operational-conformance",
        entity: "FgaTuple",
        command: "WriteTuples",
        record_input: "tuples",
        query: "ListFgaTuples",
        seed: 1,
    },
    Domain {
        name: "payload",
        application_root: "fixtures/adapters/operational-conformance",
        entity: "Document",
        command: "CreateDocuments",
        record_input: "documents",
        query: "ListDraftDocuments",
        seed: 2,
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
    let observation_bytes = fs::read(
        root.join("fixtures/adapters/conformance")
            .join(domain.name)
            .join("operation-observation.json"),
    )
    .expect("read observation");
    let observation_hash = hash_generated_artifact(&observation_bytes);
    let manifest = ApplicationPortabilityManifest::compile(ApplicationPortabilityManifestInput {
        adapter_manifest_hash: adapter.identity(),
        contract_lineage: bundle.lineage().clone(),
        contract_version: bundle.contract_version(),
        contract_bundle_hash: bundle.bundle_hash(),
        mappings: vec![PortableRecordMapping::new(
            PortableRecordClass::Entity,
            symbol(domain.entity),
            PortableReimportStrategy::bounded_collection_command(
                symbol(domain.command),
                symbol("request_id"),
                symbol(domain.record_input),
            ),
        )],
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
            ReimportObservation::new(
                symbol("application_observation"),
                symbol(domain.query),
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
        .expect("adapter mapping");

    let outcome_hash = hash_generated_artifact(format!("{}-outcomes-v1", domain.name).as_bytes());
    let receipt = ApplicationReimportReceipt::reconcile(
        &manifest,
        ApplicationExportManifestHash::from_bytes(
            *hash_generated_artifact(format!("{}-export-v1", domain.name).as_bytes()).as_bytes(),
        ),
        database_id(domain.seed),
        vec![
            ReimportMappingResult::new(
                PortableRecordClass::Entity,
                symbol(domain.entity),
                2,
                2,
                0,
                outcome_hash,
            )
            .expect("mapping result"),
        ],
        vec![ReimportObservationResult::new(
            symbol("application_observation"),
            observation_hash,
        )],
    )
    .expect("reimport receipt");

    let destination = root.join("fixtures/export").join(domain.name);
    update(
        destination.join("portability-manifest-v1.json"),
        manifest.canonical_bytes(),
        write,
    );
    update(
        destination.join("reimport-receipt-v1.json"),
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

fn update(path: PathBuf, expected: &[u8], write: bool) {
    if write {
        fs::create_dir_all(path.parent().expect("fixture parent")).expect("create fixture parent");
        fs::write(&path, expected).expect("write fixture");
        return;
    }
    let actual = fs::read(&path).unwrap_or_default();
    assert_eq!(actual, expected, "stale fixture: {}", path.display());
}
