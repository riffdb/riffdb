#![forbid(unsafe_code)]

//! Final P7 compatibility and dogfood acceptance matrix.

use riffdb_catalog::{ValidatedContractBundle, ValidatedMigrationPlan};
use riffdb_contract_compiler::{
    compile_contract_migration_successor, compile_contract_source, compile_contract_successor,
    compile_migration_source,
};
use riffdb_contract_ir::{ContractBundle, MigrationBundleV1};
use riffdb_query_module::{
    ApplicationLock, ApplicationSourceManifest, GeneratedApplicationArtifact,
    GeneratedApplicationArtifactKind, NamedQuerySource, QueryModule, QueryModuleCandidate,
    QueryModuleName, QueryModuleVersion,
};

const GATE_A_PARENT: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/migrations/gate-a/ticketdesk/parent.contract.bundle"
));
const GATE_A_SUCCESSOR: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/migrations/gate-a/ticketdesk/successor.contract.bundle"
));
const GATE_A_MIGRATION: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/migrations/gate-a/ticketdesk/v1-to-v2.migration.bundle"
));
const GATE_B_PARENT: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/migrations/gate-b/structural-rows/parent.contract.bundle"
));
const GATE_B_SUCCESSOR: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/migrations/gate-b/structural-rows/successor.contract.bundle"
));
const GATE_B_MIGRATION: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/migrations/gate-b/structural-rows/v1-to-v2.migration.bundle"
));
const GATE_C_PARENT: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/migrations/gate-c/key-ownership/parent.contract.bundle"
));
const GATE_C_SUCCESSOR: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/migrations/gate-c/key-ownership/successor.contract.bundle"
));
const GATE_C_MIGRATION: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/migrations/gate-c/key-ownership/v1-to-v2.migration.bundle"
));

const EA_PARENT: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../examples/migration-evolution/ea/parent.riff"
));
const EA_SUCCESSOR: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../examples/migration-evolution/ea/successor.riff"
));
const EA_MIGRATION: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../examples/migration-evolution/ea/v1-to-v2.riffm"
));

const TICKETDESK: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../examples/app-baseline/contracts/ticketdesk.riff"
));
const LIST_TICKETS: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../queries/ticketdesk/list_tickets.riffq"
));
const APPLICATION_SOURCE_V1: &str = r#"{
  "schema": "riffdb.application-source/v1",
  "application": "ticketdesk-compatibility",
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

#[test]
fn every_supported_gate_is_bound_to_one_exact_direct_parent() {
    let cases = [
        ("A", GATE_A_PARENT, GATE_A_SUCCESSOR, GATE_A_MIGRATION),
        ("B", GATE_B_PARENT, GATE_B_SUCCESSOR, GATE_B_MIGRATION),
        ("C", GATE_C_PARENT, GATE_C_SUCCESSOR, GATE_C_MIGRATION),
    ];

    for (gate, parent_bytes, successor_bytes, migration_bytes) in cases {
        let parent = ContractBundle::decode(parent_bytes).expect("canonical parent bundle");
        let successor =
            ContractBundle::decode(successor_bytes).expect("canonical successor bundle");
        let migration =
            MigrationBundleV1::decode(migration_bytes).expect("canonical migration bundle");
        assert_eq!(
            migration.parent_bundle_hash(),
            parent.bundle_hash(),
            "Gate {gate}"
        );
        assert_eq!(
            migration.candidate_bundle_hash(),
            successor.bundle_hash(),
            "Gate {gate}"
        );
        assert_eq!(
            migration.parent_version(),
            parent.contract_version(),
            "Gate {gate}"
        );
        assert_eq!(
            migration.candidate_version(),
            successor.contract_version(),
            "Gate {gate}"
        );
        ValidatedMigrationPlan::from_artifacts(
            ValidatedContractBundle::from_compiler_bundle(parent).expect("validated parent"),
            ValidatedContractBundle::from_compiler_bundle(successor).expect("validated successor"),
            migration,
        )
        .unwrap_or_else(|error| panic!("Gate {gate} plan failed closed unexpectedly: {error:?}"));
    }
}

#[test]
fn multiple_supported_parents_target_one_canonical_successor_without_chaining() {
    const V1: &str = r#"
contract Evolution version 1 {
  entity Row { key (id: uuid) field value: i64 }
  aggregate Rows { root Row partition_by id conflict_key (id) }
}
"#;
    const V3: &str = r#"
contract Evolution version 3 {
  entity Row { key (id: uuid) field value: i64 field doubled: i64 }
  aggregate Rows { root Row partition_by id conflict_key (id) }
}
"#;
    let v1 = compile_contract_source(V1).expect("v1");
    let v2 = compile_contract_successor(&V1.replace("version 1", "version 2"), &v1).expect("v2");
    let v3 = compile_contract_successor(V3, &v2).expect("canonical v3");
    let from_v1 = compile_migration_source(
        "migration Evolution from 1 to 3 { transform Row { set doubled = old.value + 1 } }",
        &v1,
        &v3,
    )
    .expect("direct v1 to v3");
    let from_v2 = compile_migration_source(
        "migration Evolution from 2 to 3 { transform Row { set doubled = old.value + 1 } }",
        &v2,
        &v3,
    )
    .expect("direct v2 to v3");

    assert_eq!(from_v1.candidate_bundle_hash(), v3.bundle_hash());
    assert_eq!(from_v2.candidate_bundle_hash(), v3.bundle_hash());
    assert_ne!(from_v1.parent_bundle_hash(), from_v2.parent_bundle_hash());
}

#[test]
fn prior_application_sources_and_locks_remain_strictly_readable() {
    let contract = compile_contract_source(TICKETDESK).expect("TicketDesk contract");
    let module = QueryModule::compile(
        QueryModuleCandidate::new(
            QueryModuleName::new("ticketdesk").expect("module name"),
            QueryModuleVersion::new(1).expect("module version"),
            vec![NamedQuerySource::new("ListTickets", LIST_TICKETS).expect("query")],
        )
        .expect("module candidate"),
        &contract,
    )
    .expect("query module");

    let v1_source = ApplicationSourceManifest::parse(APPLICATION_SOURCE_V1).expect("source V1");
    let v1_manifest = v1_source
        .exact_manifest(&contract, std::slice::from_ref(&module))
        .expect("manifest V1");
    let v1_lock = ApplicationLock::compile(
        &v1_source,
        &v1_manifest,
        &contract,
        std::slice::from_ref(&module),
        &[],
    )
    .expect("lock V1");
    assert_lock_round_trip(&v1_lock, riffdb_query_module::APPLICATION_LOCK_SCHEMA_V1);

    let source_v2_text = APPLICATION_SOURCE_V1
        .replace("application-source/v1", "application-source/v2")
        .replace(
            "\"mcp\": \"generated/mcp/tools.json\"",
            "\"mcp\": \"generated/mcp/tools.json\",\n    \"python\": \"generated/python/client.py\"",
        );
    let v2_source = ApplicationSourceManifest::parse(&source_v2_text).expect("source V2");
    let v2_manifest = v2_source
        .exact_manifest(&contract, std::slice::from_ref(&module))
        .expect("manifest V2");
    let python = GeneratedApplicationArtifact::new(
        GeneratedApplicationArtifactKind::Python,
        "generated/python/client.py",
        b"compatibility Python fixture",
    )
    .expect("Python artifact");
    let v2_lock = ApplicationLock::compile(
        &v2_source,
        &v2_manifest,
        &contract,
        std::slice::from_ref(&module),
        std::slice::from_ref(&python),
    )
    .expect("lock V2");
    assert_lock_round_trip(&v2_lock, riffdb_query_module::APPLICATION_LOCK_SCHEMA_V2);

    let contract_artifact = GeneratedApplicationArtifact::new(
        GeneratedApplicationArtifactKind::ContractBundle,
        riffdb_query_module::CONTRACT_BUNDLE_ARTIFACT_PATH,
        contract.canonical_bytes(),
    )
    .expect("contract artifact");
    let v3_lock = ApplicationLock::compile_v3(
        &v1_source,
        &v1_manifest,
        &contract,
        std::slice::from_ref(&module),
        std::slice::from_ref(&contract_artifact),
    )
    .expect("lock V3");
    assert_lock_round_trip(&v3_lock, riffdb_query_module::APPLICATION_LOCK_SCHEMA_V3);

    let source_v3 = ApplicationSourceManifest::decode_canonical(include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../fixtures/application-manifests/ticketdesk-migration-v3.json"
    )))
    .expect("source V3 fixture");
    assert_eq!(source_v3.migrations().len(), 1);
    let v4_lock = ApplicationLock::decode_canonical(include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../fixtures/application-locks/ticketdesk-migration-v4.json"
    )))
    .expect("lock V4 fixture");
    assert_lock_round_trip(&v4_lock, riffdb_query_module::APPLICATION_LOCK_SCHEMA_V4);
    assert_eq!(v4_lock.migrations().len(), 1);
}

#[test]
fn ea_dogfood_evolution_requires_and_compiles_an_exact_bounded_migration() {
    let parent = compile_contract_source(EA_PARENT).expect("EA predecessor");
    let (successor, migration) =
        compile_contract_migration_successor(EA_SUCCESSOR, EA_MIGRATION, &parent)
            .expect("EA migration");
    assert_eq!(migration.parent_bundle_hash(), parent.bundle_hash());
    assert_eq!(migration.candidate_bundle_hash(), successor.bundle_hash());
    assert!(migration.steps().len() <= 4_096);
    assert!(successor.schema().enums().iter().any(|enumeration| {
        enumeration
            .variants()
            .iter()
            .any(|variant| variant.name() == "Email")
    }));
    assert!(
        successor
            .schema()
            .entities()
            .iter()
            .any(|entity| entity.name() == "EmailDraft")
    );
    ValidatedMigrationPlan::from_artifacts(
        ValidatedContractBundle::from_compiler_bundle(parent).expect("validated EA parent"),
        ValidatedContractBundle::from_compiler_bundle(successor).expect("validated EA successor"),
        migration,
    )
    .expect("sealed EA plan");
}

fn assert_lock_round_trip(lock: &ApplicationLock, expected_schema: &str) {
    assert_eq!(lock.schema(), expected_schema);
    assert_eq!(
        ApplicationLock::decode_canonical(lock.canonical_bytes()).expect("strict lock round trip"),
        *lock
    );
}
