#![forbid(unsafe_code)]

//! Compiler-owned source/lock acceptance tests.

use riffdb_contract_compiler::compile_contract_source;
use riffdb_query_module::{
    ApplicationLock, ApplicationLockErrorKind, ApplicationSourceManifest,
    GeneratedApplicationArtifact, GeneratedApplicationArtifactKind, NamedQuerySource, QueryModule,
    QueryModuleCandidate, QueryModuleName, QueryModuleVersion,
};

const CONTRACT: &str = include_str!("../../../examples/app-baseline/contracts/ticketdesk.riff");
const LIST_TICKETS: &str = include_str!("../../../queries/ticketdesk/list_tickets.riffq");
const SOURCE: &str = r#"{
  "schema": "riffdb.application-source/v1",
  "application": "ticketdesk",
  "contract": {
    "source": "riffdb/contract.riff",
    "lineage": "TicketDesk",
    "version": 1
  },
  "generation": {
    "rust": "generated/rust/client.rs",
    "typescript": "generated/typescript/client.ts",
    "mcp": "generated/mcp/tools.json"
  },
  "query_modules": [{
    "name": "ticketdesk",
    "version": 1,
    "queries": [{
      "name": "ListTickets",
      "source": "riffdb/queries/list_tickets.riffq"
    }]
  }],
  "roles": [{
    "name": "TicketDeskApplication",
    "environment": "development",
    "tenant_scope": "global",
    "queries": ["ListTickets"],
    "commands": ["CreateTicket"]
  }],
  "seed_inputs": []
}"#;

fn compiled_application() -> (
    ApplicationSourceManifest,
    riffdb_query_module::ApplicationManifest,
    riffdb_contract_ir::ContractBundle,
    QueryModule,
) {
    let source = ApplicationSourceManifest::parse(SOURCE).expect("source");
    let contract = compile_contract_source(CONTRACT).expect("contract");
    let module = QueryModule::compile(
        QueryModuleCandidate::new(
            QueryModuleName::new("ticketdesk").expect("module name"),
            QueryModuleVersion::new(1).expect("module version"),
            vec![NamedQuerySource::new("ListTickets", LIST_TICKETS).expect("query")],
        )
        .expect("candidate"),
        &contract,
    )
    .expect("module");
    let manifest = source
        .exact_manifest(&contract, std::slice::from_ref(&module))
        .expect("exact manifest");
    (source, manifest, contract, module)
}

#[test]
fn lock_covers_every_compiler_owned_identity_and_generated_artifact() {
    let (source, manifest, contract, module) = compiled_application();
    let artifacts = [
        GeneratedApplicationArtifact::new(
            GeneratedApplicationArtifactKind::Rust,
            source.generation().rust(),
            b"generated rust",
        )
        .expect("rust"),
        GeneratedApplicationArtifact::new(
            GeneratedApplicationArtifactKind::TypeScript,
            source.generation().typescript(),
            b"generated typescript",
        )
        .expect("typescript"),
        GeneratedApplicationArtifact::new(
            GeneratedApplicationArtifactKind::Mcp,
            source.generation().mcp(),
            b"generated mcp",
        )
        .expect("mcp"),
    ];
    let lock = ApplicationLock::compile(
        &source,
        &manifest,
        &contract,
        std::slice::from_ref(&module),
        &artifacts,
    )
    .expect("lock");
    let text = std::str::from_utf8(lock.canonical_bytes()).expect("UTF-8");

    assert_eq!(lock.source_hash(), source.identity());
    assert_eq!(lock.manifest_hash(), manifest.identity());
    for expected in [
        "bundle_hash",
        "plan_root_hash",
        "module_hash",
        "plan_hash",
        "source_hash",
        "definition_hash",
        "content_hash",
    ] {
        assert!(text.contains(expected), "missing {expected}");
    }
    assert!(!text.contains("tenant_id"));
    assert!(!text.contains("credential"));
    assert_eq!(
        ApplicationLock::decode_canonical(lock.canonical_bytes())
            .expect("strict round trip")
            .identity(),
        lock.identity()
    );
}

#[test]
fn lock_rejects_stale_manifest_duplicate_outputs_and_noncanonical_bytes() {
    let (source, manifest, contract, module) = compiled_application();
    let stale_source =
        ApplicationSourceManifest::parse(&SOURCE.replace("\"version\": 1", "\"version\": 2"))
            .expect("stale source parses");
    let error = ApplicationLock::compile(
        &stale_source,
        &manifest,
        &contract,
        std::slice::from_ref(&module),
        &[],
    )
    .expect_err("stale source");
    assert_eq!(error.kind(), ApplicationLockErrorKind::IdentityMismatch);

    let duplicate = GeneratedApplicationArtifact::new(
        GeneratedApplicationArtifactKind::Rust,
        "generated/client.rs",
        b"same",
    )
    .expect("artifact");
    let error = ApplicationLock::compile(
        &source,
        &manifest,
        &contract,
        std::slice::from_ref(&module),
        &[duplicate.clone(), duplicate],
    )
    .expect_err("duplicate");
    assert_eq!(error.kind(), ApplicationLockErrorKind::Duplicate);

    let lock = ApplicationLock::compile(
        &source,
        &manifest,
        &contract,
        std::slice::from_ref(&module),
        &[],
    )
    .expect("lock");
    let mut noncanonical = b" ".to_vec();
    noncanonical.extend_from_slice(lock.canonical_bytes());
    assert_eq!(
        ApplicationLock::decode_canonical(&noncanonical)
            .expect_err("noncanonical")
            .kind(),
        ApplicationLockErrorKind::NonCanonical
    );
}

#[test]
fn v3_lock_pins_the_exact_canonical_contract_bundle_artifact() {
    let (source, manifest, contract, module) = compiled_application();
    let bundle = GeneratedApplicationArtifact::new(
        GeneratedApplicationArtifactKind::ContractBundle,
        riffdb_query_module::CONTRACT_BUNDLE_ARTIFACT_PATH,
        contract.canonical_bytes(),
    )
    .expect("bundle artifact");
    let lock = ApplicationLock::compile_v3(
        &source,
        &manifest,
        &contract,
        std::slice::from_ref(&module),
        std::slice::from_ref(&bundle),
    )
    .expect("v3 lock");

    assert_eq!(
        lock.schema(),
        riffdb_query_module::APPLICATION_LOCK_SCHEMA_V3
    );
    assert_eq!(
        lock.contract_bundle_artifact().expect("pinned bundle"),
        &bundle
    );
    assert_eq!(
        ApplicationLock::decode_canonical(lock.canonical_bytes())
            .expect("strict v3 round trip")
            .contract_bundle_artifact(),
        Some(&bundle)
    );

    let substituted = GeneratedApplicationArtifact::new(
        GeneratedApplicationArtifactKind::ContractBundle,
        riffdb_query_module::CONTRACT_BUNDLE_ARTIFACT_PATH,
        b"substituted bundle",
    )
    .expect("substituted artifact");
    assert_eq!(
        ApplicationLock::compile_v3(
            &source,
            &manifest,
            &contract,
            std::slice::from_ref(&module),
            std::slice::from_ref(&substituted),
        )
        .expect_err("substitution rejected")
        .kind(),
        ApplicationLockErrorKind::IdentityMismatch
    );
}

#[test]
fn v2_lock_requires_exact_python_artifact_and_v1_rejects_it() {
    let v2_source_text = SOURCE
        .replace("application-source/v1", "application-source/v2")
        .replace(
            "\"mcp\": \"generated/mcp/tools.json\"",
            "\"mcp\": \"generated/mcp/tools.json\",\n    \"python\": \"generated/python/client.py\"",
        );
    let source = ApplicationSourceManifest::parse(&v2_source_text).expect("v2 source");
    let contract = compile_contract_source(CONTRACT).expect("contract");
    let module = QueryModule::compile(
        QueryModuleCandidate::new(
            QueryModuleName::new("ticketdesk").expect("module name"),
            QueryModuleVersion::new(1).expect("module version"),
            vec![NamedQuerySource::new("ListTickets", LIST_TICKETS).expect("query")],
        )
        .expect("candidate"),
        &contract,
    )
    .expect("module");
    let manifest = source
        .exact_manifest(&contract, std::slice::from_ref(&module))
        .expect("manifest");
    assert_eq!(
        ApplicationLock::compile(
            &source,
            &manifest,
            &contract,
            std::slice::from_ref(&module),
            &[],
        )
        .expect_err("missing Python")
        .kind(),
        ApplicationLockErrorKind::IdentityMismatch
    );
    let python = GeneratedApplicationArtifact::new(
        GeneratedApplicationArtifactKind::Python,
        "generated/python/client.py",
        b"generated python",
    )
    .expect("Python artifact");
    let lock = ApplicationLock::compile(
        &source,
        &manifest,
        &contract,
        std::slice::from_ref(&module),
        std::slice::from_ref(&python),
    )
    .expect("v2 lock");
    assert_eq!(
        lock.schema(),
        riffdb_query_module::APPLICATION_LOCK_SCHEMA_V2
    );
    assert_eq!(
        ApplicationLock::decode_canonical(lock.canonical_bytes())
            .expect("v2 decode")
            .canonical_bytes(),
        lock.canonical_bytes()
    );

    let (v1_source, v1_manifest, v1_contract, v1_module) = compiled_application();
    assert_eq!(
        ApplicationLock::compile(
            &v1_source,
            &v1_manifest,
            &v1_contract,
            std::slice::from_ref(&v1_module),
            &[python],
        )
        .expect_err("v1 rejects Python")
        .kind(),
        ApplicationLockErrorKind::InvalidShape
    );
}
