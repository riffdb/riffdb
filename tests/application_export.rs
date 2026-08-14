#![forbid(unsafe_code)]

//! Semantic acceptance for compiler-owned export portability artifacts.

use std::fs;
use std::path::{Path, PathBuf};

use riffdb_application::{
    AdapterConformanceManifest, ApplicationPortabilityManifest, ApplicationReimportReceipt,
    PortableReimportStrategy,
};
use riffdb_contract_compiler::{CompilerDiagnosticCode, validate_contract_source};
use riffdb_contract_ir::ContractBundle;

const PORTABLE_DOMAINS: &[(&str, &str)] = &[
    ("openfga", "fixtures/adapters/operational-conformance"),
    ("mlflow", "fixtures/adapters/mlflow"),
    ("better-auth", "fixtures/adapters/better-auth"),
    ("woodpecker", "fixtures/adapters/woodpecker"),
    // Retained post-alpha portability regression after the accepted adapter substitution.
    ("payload", "fixtures/adapters/operational-conformance"),
];

#[test]
fn portable_adapter_manifests_are_exact_compiled_and_reconciled() {
    for (domain, application_root) in PORTABLE_DOMAINS {
        let export_root = repository_root().join("fixtures/export").join(domain);
        let manifest = ApplicationPortabilityManifest::decode_canonical(
            &fs::read(export_root.join("portability-manifest-v3.json"))
                .expect("portability manifest"),
        )
        .expect("canonical portability manifest");
        let bundle = ContractBundle::decode(
            &fs::read(
                repository_root()
                    .join(application_root)
                    .join("generated/riffdb.contract.bundle"),
            )
            .expect("contract bundle"),
        )
        .expect("exact contract bundle");
        let adapter = AdapterConformanceManifest::decode_canonical(
            &fs::read(
                repository_root()
                    .join("fixtures/adapters/conformance")
                    .join(domain)
                    .join("adapter.conformance.json"),
            )
            .expect("adapter conformance manifest"),
        )
        .expect("canonical adapter conformance manifest");

        manifest
            .validate_compiled_contract(&bundle)
            .expect("mapping resolves against exact compiler IR");
        manifest
            .validate_adapter_conformance(&adapter)
            .expect("mapping is present in adapter-owned public role");
        assert!(manifest.input().mappings.iter().all(|mapping| matches!(
            mapping.strategy(),
            PortableReimportStrategy::ReimportCommand { .. }
        )));
        assert!(manifest.input().observations.iter().all(|observation| {
            observation.module_hash().is_some() && !observation.parameters().is_empty()
        }));
        let receipt = ApplicationReimportReceipt::decode_canonical(
            &fs::read(export_root.join("reimport-receipt-v3.json")).expect("reimport receipt"),
            &manifest,
        )
        .expect("terminal receipt reconciles exact observations");
        assert_eq!(
            receipt.input().portability_manifest_hash,
            manifest.identity()
        );

        let source = fs::read_to_string(export_root.join("portability-manifest-v3.json"))
            .expect("manifest text");
        for forbidden in [
            "entity_type_id",
            "field_id",
            "index_id",
            "storage_key",
            "method_path",
            "callback",
            "raw_transaction",
            "capability_mask",
        ] {
            assert!(
                !source.contains(forbidden),
                "{domain} manifest exposed forbidden reimport channel {forbidden}"
            );
        }
    }
}

#[test]
fn workflow_state_cannot_be_reconstituted_by_an_ordinary_field_write() {
    let source = r#"
contract UnsafeWorkflowImport version 1 {
  enum State { Queued, Running }
  entity Work {
    key (organization_id: uuid, work_id: uuid)
    field state: State
  }
  aggregate Works {
    root Work
    partition_by organization_id
    conflict_key (organization_id, work_id)
  }
  workflow WorkLifecycle {
    entity Work
    state state
    transition Start from (Queued) to Running
  }
  command ImportWork {
    input request_id: uuid
    input organization_id: uuid
    input work_id: uuid
    input state: State
    idempotency_key request_id
    create Work(organization_id, work_id) as work else AlreadyExists {}
    set work.state = state
    return Imported {}
  }
}
"#;
    let error =
        validate_contract_source(source).expect_err("workflow state write must fail closed");
    let diagnostics = error.semantic().expect("semantic refusal");
    assert!(diagnostics.as_slice().iter().any(|diagnostic| {
        diagnostic.code() == CompilerDiagnosticCode::InvalidWorkflowTransition
    }));
}

fn repository_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}
