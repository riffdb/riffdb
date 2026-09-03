#![forbid(unsafe_code)]

//! Compiler-owned source/lock acceptance tests.

use riffdb_contract_compiler::{compile_contract_source, compile_contract_successor};
use riffdb_contract_ir::{
    MigrationBundleV1, MigrationResourceBoundsV1, MigrationStepId, MigrationStepKindV1,
    MigrationStepV1, StableIdNamespaceTag,
};
use riffdb_query_module::{
    ApplicationLock, ApplicationLockErrorKind, ApplicationMigrationLockInput,
    ApplicationSourceManifest, GeneratedApplicationArtifact, GeneratedApplicationArtifactKind,
    NamedQuerySource, QueryModule, QueryModuleCandidate, QueryModuleName, QueryModuleVersion,
};
use riffdb_types::hash_migration_source;

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
            source.generation().rust().expect("rust"),
            b"generated rust",
        )
        .expect("rust"),
        GeneratedApplicationArtifact::new(
            GeneratedApplicationArtifactKind::TypeScript,
            source.generation().typescript().expect("typescript"),
            b"generated typescript",
        )
        .expect("typescript"),
        GeneratedApplicationArtifact::new(
            GeneratedApplicationArtifactKind::Mcp,
            source.generation().mcp().expect("mcp"),
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
    let decoded =
        ApplicationLock::decode_canonical(lock.canonical_bytes()).expect("strict round trip");
    assert_eq!(decoded.identity(), lock.identity());
    let mut expected_artifacts = artifacts.to_vec();
    expected_artifacts.sort_by(|left, right| left.path().cmp(right.path()));
    assert_eq!(decoded.artifacts(), expected_artifacts);
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
fn v7_lock_pins_symbolic_row_policy_receipts_and_rejects_omission() {
    let contract = compile_contract_source(include_str!(
        "../../../fixtures/compiler/row-policy/valid/document-access.riff"
    ))
    .expect("policy contract");
    let query = r#"
query GetDocument(
    $organization_id: Document.organization_id,
    $document_id: Document.document_id,
) {
    one document from Document
        where organization_id == $organization_id
          && document_id == $document_id
        else NotFound
    return Found { document: document { document_id owner_id team_id visibility } }
    outcomes Found | NotFound
}
"#;
    let module = QueryModule::compile(
        QueryModuleCandidate::new(
            QueryModuleName::new("policy_surface").expect("module name"),
            QueryModuleVersion::new(1).expect("module version"),
            vec![NamedQuerySource::new("GetDocument", query).expect("query")],
        )
        .expect("candidate"),
        &contract,
    )
    .expect("module");
    let source_text = |policies: &str| {
        format!(
            r#"{{
  "application": "policy-surface",
  "contract": {{"lineage": "PolicySurface", "source": "contract.riff", "version": 1}},
  "generation": {{"go": "generated/go/client.go", "mcp": "generated/mcp/tools.json", "python": "generated/python/client.py", "rust": "generated/rust/client.rs", "typescript": "generated/typescript/client.ts"}},
  "migrations": [],
  "query_modules": [{{"name": "policy_surface", "queries": [{{"name": "GetDocument", "source": "queries/get_document.riffq"}}], "version": 1}}],
  "reactive_modules": [],
  "roles": [{{"agent_subscriptions": [], "commands": [], "environment": "development", "event_streams": [], "name": "DocumentReader", "queries": ["GetDocument"], "row_policies": {policies}, "tenant_scope": "global", "watch_queries": []}}],
  "schema": "riffdb.application-source/v6",
  "seed_inputs": []
}}"#
        )
    };
    let source =
        ApplicationSourceManifest::parse(&source_text("[\"DocumentAccess\"]")).expect("source");
    let manifest = source
        .exact_manifest_v2(&contract, std::slice::from_ref(&module), &[])
        .expect("manifest");
    let artifacts = [
        GeneratedApplicationArtifact::new(
            GeneratedApplicationArtifactKind::ContractBundle,
            riffdb_query_module::CONTRACT_BUNDLE_ARTIFACT_PATH,
            contract.canonical_bytes(),
        )
        .expect("bundle"),
        GeneratedApplicationArtifact::new(
            GeneratedApplicationArtifactKind::Rust,
            source.generation().rust().expect("rust"),
            b"rust",
        )
        .expect("rust"),
        GeneratedApplicationArtifact::new(
            GeneratedApplicationArtifactKind::TypeScript,
            source.generation().typescript().expect("typescript"),
            b"typescript",
        )
        .expect("typescript"),
        GeneratedApplicationArtifact::new(
            GeneratedApplicationArtifactKind::Python,
            source.generation().python().expect("Python path"),
            b"python",
        )
        .expect("python"),
        GeneratedApplicationArtifact::new(
            GeneratedApplicationArtifactKind::Go,
            source.generation().go().expect("Go path"),
            b"go",
        )
        .expect("go"),
        GeneratedApplicationArtifact::new(
            GeneratedApplicationArtifactKind::Mcp,
            source.generation().mcp().expect("mcp"),
            b"mcp",
        )
        .expect("mcp"),
    ];
    let lock = ApplicationLock::compile_v7(
        &source,
        &manifest,
        &contract,
        std::slice::from_ref(&module),
        &[],
        &artifacts,
        &[],
    )
    .expect("V7 lock");
    let text = std::str::from_utf8(lock.canonical_bytes()).expect("UTF-8");

    assert_eq!(
        lock.schema(),
        riffdb_query_module::APPLICATION_LOCK_SCHEMA_V7
    );
    assert!(text.contains(r#""application_role_definition":3"#));
    assert!(text.contains(r#""name":"DocumentAccess""#));
    assert!(text.contains(r#""entity":"Document""#));
    assert!(text.contains(r#""operations":["read","create","update","delete"]"#));
    assert!(!text.contains("team_ids"));
    assert_eq!(
        ApplicationLock::decode_canonical(lock.canonical_bytes()).expect("round trip"),
        lock
    );

    let omitted_source = ApplicationSourceManifest::parse(&source_text("[]")).expect("source");
    let omitted_manifest = omitted_source
        .exact_manifest_v2(&contract, std::slice::from_ref(&module), &[])
        .expect("manifest");
    assert_eq!(
        ApplicationLock::compile_v7(
            &omitted_source,
            &omitted_manifest,
            &contract,
            std::slice::from_ref(&module),
            &[],
            &artifacts,
            &[],
        )
        .expect_err("protected query cannot omit the policy")
        .kind(),
        ApplicationLockErrorKind::IdentityMismatch
    );
}

#[test]
fn checked_row_policy_rotation_fixtures_decode_as_one_current_identity_family() {
    let source = ApplicationSourceManifest::decode_canonical(include_bytes!(
        "../../../fixtures/application-manifests/policy-surface-v6.json"
    ))
    .expect("source V6 fixture");
    let manifest = riffdb_query_module::ApplicationManifest::decode_canonical(include_bytes!(
        "../../../fixtures/application-manifests/policy-surface-exact-v4.json"
    ))
    .expect("manifest V4 fixture");
    let lock = ApplicationLock::decode_canonical(include_bytes!(
        "../../../fixtures/application-locks/policy-surface-v7.json"
    ))
    .expect("lock V7 fixture");

    assert_eq!(
        source.schema(),
        riffdb_query_module::APPLICATION_SOURCE_SCHEMA_V6
    );
    assert_eq!(
        manifest.schema(),
        riffdb_query_module::APPLICATION_MANIFEST_SCHEMA_V4
    );
    assert_eq!(
        lock.schema(),
        riffdb_query_module::APPLICATION_LOCK_SCHEMA_V7
    );
    assert_eq!(lock.source_hash(), source.identity());
    assert_eq!(lock.manifest_hash(), manifest.identity());
    let text = std::str::from_utf8(lock.canonical_bytes()).expect("UTF-8");
    assert!(text.contains(r#""name":"DocumentAccess""#));
    assert!(text.contains(r#""application_role_definition":3"#));
    assert!(!text.contains("team_ids"));
}

// req: DX-006, DX-042, DX-043, DX-044, DX-045, DX-047, DX-049
#[test]
fn v8_lock_contains_exactly_the_v7_declared_surface_set() {
    let source = ApplicationSourceManifest::decode_canonical(include_bytes!(
        "../../../fixtures/application-manifests/go-only-v7.json"
    ))
    .expect("Go-only V7 source");
    let contract =
        compile_contract_source("contract Inventory version 1 {\n}\n").expect("empty contract");
    let module = QueryModule::compile(
        QueryModuleCandidate::new(
            QueryModuleName::new("inventory").expect("module name"),
            QueryModuleVersion::new(1).expect("module version"),
            Vec::new(),
        )
        .expect("candidate"),
        &contract,
    )
    .expect("module");
    let manifest = source
        .exact_manifest_v2(&contract, std::slice::from_ref(&module), &[])
        .expect("V5 manifest");
    assert_eq!(
        manifest.schema(),
        riffdb_query_module::APPLICATION_MANIFEST_SCHEMA_V5
    );
    let mandatory = [
        GeneratedApplicationArtifact::new(
            GeneratedApplicationArtifactKind::Manifest,
            riffdb_query_module::EXACT_MANIFEST_ARTIFACT_PATH,
            manifest.canonical_bytes(),
        )
        .expect("manifest artifact"),
        GeneratedApplicationArtifact::new(
            GeneratedApplicationArtifactKind::Go,
            source.generation().go().expect("Go path"),
            b"generated Go",
        )
        .expect("Go artifact"),
        GeneratedApplicationArtifact::new(
            GeneratedApplicationArtifactKind::ContractBundle,
            riffdb_query_module::CONTRACT_BUNDLE_ARTIFACT_PATH,
            contract.canonical_bytes(),
        )
        .expect("contract artifact"),
    ];
    assert_eq!(
        ApplicationLock::compile_v7(
            &source,
            &manifest,
            &contract,
            std::slice::from_ref(&module),
            &[],
            &mandatory,
            &[],
        )
        .expect_err("the predecessor writer must reject V7/V5 identities")
        .kind(),
        ApplicationLockErrorKind::UnsupportedVersion
    );
    let lock = ApplicationLock::compile_v8(
        &source,
        &manifest,
        &contract,
        std::slice::from_ref(&module),
        &[],
        &mandatory,
        &[],
    )
    .expect("V8 lock");
    assert_eq!(
        manifest.canonical_bytes(),
        include_bytes!("../../../fixtures/application-manifests/go-only-exact-v5.json")
    );
    assert_eq!(
        lock.canonical_bytes(),
        include_bytes!("../../../fixtures/application-locks/go-only-v8.json")
    );
    assert_eq!(
        lock.schema(),
        riffdb_query_module::APPLICATION_LOCK_SCHEMA_V8
    );
    assert_eq!(
        ApplicationLock::decode_canonical(lock.canonical_bytes()).expect("round trip"),
        lock
    );
    assert_eq!(
        lock.artifacts()
            .iter()
            .filter(|artifact| matches!(
                artifact.kind(),
                GeneratedApplicationArtifactKind::Rust
                    | GeneratedApplicationArtifactKind::Go
                    | GeneratedApplicationArtifactKind::TypeScript
                    | GeneratedApplicationArtifactKind::Python
                    | GeneratedApplicationArtifactKind::Mcp
            ))
            .map(GeneratedApplicationArtifact::kind)
            .collect::<Vec<_>>(),
        vec![GeneratedApplicationArtifactKind::Go]
    );

    for invalid in [
        mandatory[..2].to_vec(),
        mandatory
            .iter()
            .cloned()
            .chain([GeneratedApplicationArtifact::new(
                GeneratedApplicationArtifactKind::Rust,
                "generated/rust/client.rs",
                b"extra Rust",
            )
            .expect("extra artifact")])
            .collect(),
    ] {
        assert_eq!(
            ApplicationLock::compile_v8(
                &source,
                &manifest,
                &contract,
                std::slice::from_ref(&module),
                &[],
                &invalid,
                &[],
            )
            .expect_err("missing or extra artifacts fail closed")
            .kind(),
            ApplicationLockErrorKind::IdentityMismatch
        );
    }
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

#[test]
fn v4_lock_pins_and_round_trips_one_canonical_successor_with_exact_parent_entries() {
    let parent = compile_contract_source(CONTRACT).expect("parent");
    let candidate_source = CONTRACT.replace("version 1", "version 2");
    let candidate = compile_contract_successor(&candidate_source, &parent).expect("candidate");
    let source_text = SOURCE
        .replace("application-source/v1", "application-source/v3")
        .replacen("\"version\": 1", "\"version\": 2", 1)
        .replace(
            "\"mcp\": \"generated/mcp/tools.json\"",
            "\"mcp\": \"generated/mcp/tools.json\",\n    \"python\": \"generated/python/client.py\"",
        )
        .replace(
            "\"query_modules\":",
            concat!(
                "\"migrations\": [{",
                "\"parent_bundle\":\"retained/ticketdesk-v1.bundle\",",
                "\"source\":\"riffdb/migrations/v1-to-v2.riffm\"}],\n  ",
                "\"query_modules\":"
            ),
        );
    let source = ApplicationSourceManifest::parse(&source_text).expect("v3 source");
    let module = QueryModule::compile(
        QueryModuleCandidate::new(
            QueryModuleName::new("ticketdesk").expect("module name"),
            QueryModuleVersion::new(1).expect("module version"),
            vec![NamedQuerySource::new("ListTickets", LIST_TICKETS).expect("query")],
        )
        .expect("module candidate"),
        &candidate,
    )
    .expect("module");
    let manifest = source
        .exact_manifest(&candidate, std::slice::from_ref(&module))
        .expect("manifest");
    let migration_source = "migration TicketDesk from 1 to 2 {}";
    let step = MigrationStepV1::new(
        MigrationStepId::new(1).expect("step ID"),
        Vec::new(),
        MigrationStepKindV1::RetireIdentity {
            namespace: StableIdNamespaceTag::Entity,
            owner_kind: 0,
            owner_ids: Vec::new(),
            stable_id: 1,
        },
    )
    .expect("structural migration step");
    let migration = MigrationBundleV1::new(
        env!("CARGO_PKG_VERSION"),
        candidate.lineage().clone(),
        parent.contract_version(),
        parent.bundle_hash(),
        candidate.contract_version(),
        candidate.bundle_hash(),
        hash_migration_source(migration_source.as_bytes()),
        vec![step],
        MigrationResourceBoundsV1::fixed(),
    )
    .expect("migration bundle");
    let input = ApplicationMigrationLockInput::new(
        "riffdb/migrations/v1-to-v2.riffm",
        migration_source.as_bytes(),
        "retained/ticketdesk-v1.bundle",
        &parent,
        "generated/migrations/v1-to-v2.bundle",
        &migration,
    )
    .expect("lock input");
    let artifacts = [
        GeneratedApplicationArtifact::new(
            GeneratedApplicationArtifactKind::ContractBundle,
            riffdb_query_module::CONTRACT_BUNDLE_ARTIFACT_PATH,
            candidate.canonical_bytes(),
        )
        .expect("candidate artifact"),
        GeneratedApplicationArtifact::new(
            GeneratedApplicationArtifactKind::Python,
            source.generation().python().expect("Python path"),
            b"generated python",
        )
        .expect("Python artifact"),
    ];
    let lock = ApplicationLock::compile_v4(
        &source,
        &manifest,
        &candidate,
        std::slice::from_ref(&module),
        &artifacts,
        std::slice::from_ref(&input),
    )
    .expect("v4 lock");
    assert_eq!(
        lock.schema(),
        riffdb_query_module::APPLICATION_LOCK_SCHEMA_V4
    );
    assert_eq!(lock.migrations().len(), 1);
    assert_eq!(
        lock.migrations()[0].parent_bundle_hash(),
        parent.bundle_hash()
    );
    assert_eq!(
        ApplicationLock::decode_canonical(lock.canonical_bytes()).expect("strict V4 round trip"),
        lock
    );

    assert_eq!(
        ApplicationLock::compile_v4(
            &source,
            &manifest,
            &candidate,
            std::slice::from_ref(&module),
            &artifacts,
            &[],
        )
        .expect_err("missing retained parent")
        .kind(),
        ApplicationLockErrorKind::IdentityMismatch
    );
}
