//! Dependency and authority checks for the semantic storage boundary.

const MODULES: &[(&str, &str)] = &[
    ("admission.rs", include_str!("../src/admission.rs")),
    ("audit.rs", include_str!("../src/audit.rs")),
    ("authoritative.rs", include_str!("../src/authoritative.rs")),
    ("backup.rs", include_str!("../src/backup.rs")),
    ("bounds.rs", include_str!("../src/bounds.rs")),
    ("capability.rs", include_str!("../src/capability.rs")),
    ("catalog.rs", include_str!("../src/catalog.rs")),
    ("command.rs", include_str!("../src/command.rs")),
    ("command_txn.rs", include_str!("../src/command_txn.rs")),
    ("error.rs", include_str!("../src/error.rs")),
    ("identity.rs", include_str!("../src/identity.rs")),
    ("integrity.rs", include_str!("../src/integrity.rs")),
    ("lib.rs", include_str!("../src/lib.rs")),
    ("outbox.rs", include_str!("../src/outbox.rs")),
    ("plan.rs", include_str!("../src/plan.rs")),
    ("projection.rs", include_str!("../src/projection.rs")),
    (
        "projection_schema.rs",
        include_str!("../src/projection_schema.rs"),
    ),
    ("records.rs", include_str!("../src/records.rs")),
    (
        "schema_binding.rs",
        include_str!("../src/schema_binding.rs"),
    ),
    ("sequence.rs", include_str!("../src/sequence.rs")),
    ("snapshot.rs", include_str!("../src/snapshot.rs")),
    ("startup.rs", include_str!("../src/startup.rs")),
];

#[test]
fn only_projection_schema_names_contract_ir() {
    for (name, source) in MODULES {
        if *name == "projection_schema.rs" {
            assert!(source.contains("riffdb_contract_ir::BoundProjectionGroupSchema"));
        } else {
            assert!(
                !source.contains("riffdb_contract_ir"),
                "{name} must remain IR-blind"
            );
        }
    }
}

#[test]
fn live_range_prefix_has_no_raw_byte_constructor() {
    let source = include_str!("../src/snapshot.rs");
    assert!(!source.contains("IndexRangePrefix::new"));
    assert!(!source.contains("pub fn from_bytes"));
    assert!(source.contains("pub struct IndexRangePrefixBuilder"));
}

#[test]
fn startup_key_evidence_can_only_derive_schema_binding_from_durable_postimages() {
    let source = include_str!("../src/startup.rs");
    assert!(!source.contains("HistoricalKeySchemaRefV1"));
    assert!(source.contains("pub fn from_entity(record: &StoredEntityRecordV1)"));
    assert!(source.contains("pub fn from_index_entry(record: &StoredIndexEntryV1)"));
    assert!(source.contains("pub fn from_index_epoch(record: &StoredIndexEpochV1)"));
    assert!(!source.contains("pub fn new(schema: DurableKeySchemaBindingV1"));
}

#[test]
fn command_staging_retains_and_checks_the_exact_candidate_graph() {
    let transaction = include_str!("../src/command_txn.rs");
    assert!(transaction.contains("fn intent(&self) -> &CommitIntent;"));
    assert!(transaction.contains("fn affected_current(&self) -> &AffectedEpochCurrentState;"));
    assert!(transaction.contains("fn write_plan(&self) -> &CommandWriteSetPlanV1;"));
    assert!(transaction.contains("CommandWriteSetPlanV1::matches_retained_candidate"));
    assert!(transaction.contains("AtomicCommandRecordSet::matches_reserved_candidate"));
    assert!(transaction.contains("AtomicCommandRecordSet::matches_durability_mode"));

    let records = include_str!("../src/records.rs");
    assert!(records.contains("sequence != assignment.assigned()"));
    assert!(records.contains("validate_intent_entity_derivation(evaluated, &entities)?"));
    assert!(records.contains("validate_intent_event_derivation(evaluated, sequence, &events)?"));
    assert!(records.contains("pub fn matches_retained_candidate("));
    assert!(records.contains("pub fn matches_reserved_candidate("));
    assert!(records.contains("pub fn matches_durability_mode("));
}

#[test]
fn semantic_api_excludes_engines_transports_sources_and_generic_transactions() {
    let forbidden = [
        "CommandPlan",
        "ValidatedCatalogHistory",
        "IncidentIdSource",
        "KeyedDigest",
        "NodeId",
        "SystemTime",
        "getrandom",
        "rand::",
        "redb::",
        "prost::",
        "tonic::",
        "rmcp::",
        "async fn",
        "trait StorageEngine",
        "dyn Fn",
        "impl Fn",
    ];
    for (name, source) in MODULES {
        for token in forbidden {
            assert!(
                !source.contains(token),
                "{name} contains forbidden `{token}`"
            );
        }
    }
}

#[test]
fn manifest_has_only_the_reviewed_semantic_dependencies() {
    let manifest = include_str!("../Cargo.toml");
    assert!(manifest.contains("riffdb-types"));
    assert!(manifest.contains("riffdb-contract-ir"));
    for forbidden in [
        "riffdb-proto",
        "riffdb-runtime",
        "riffdb-commit",
        "redb",
        "prost",
        "tokio",
        "rand",
        "getrandom",
    ] {
        assert!(
            !manifest.contains(forbidden),
            "manifest contains forbidden dependency `{forbidden}`"
        );
    }
}
