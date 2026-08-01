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
    ("migration.rs", include_str!("../src/migration.rs")),
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

const PROTO_CODEC_BOUNDS: &str = include_str!("../src/proto_codec/bounds.rs");

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
    assert!(source.contains("pub fn from_index_epoch(record: &StoredIndexEpochV1)"));
    assert!(!source.contains("pub fn new(schema: DurableKeySchemaBindingV1"));
    assert!(!source.contains("pub fn from_index_entry"));
    assert!(source.contains("pub(crate) fn from_codec_checked_parts("));
    assert!(!source.contains("pub fn from_codec_checked_parts("));
}

#[test]
fn startup_capability_evidence_can_only_derive_entries_from_durable_records() {
    let source = include_str!("../src/startup.rs");
    assert!(source.contains(
        "pub fn from_capability_entry(\n        capability: &crate::StoredCapabilityRecordV1,"
    ));
    assert!(!source.contains("pub fn new(\n        capability_id: CapabilityId"));
}

#[test]
fn completed_startup_handoff_owns_the_same_session_retained_metadata() {
    let source = include_str!("../src/startup.rs");
    assert!(source.contains("retained_metadata: RetainedMetadataV1"));
    assert!(source.contains("pub const fn retained_metadata(&self) -> &RetainedMetadataV1"));
    assert!(
        source.contains(
            "pub fn into_parts(self) -> (DatabaseId, OpenSessionId, RetainedMetadataV1, P)"
        )
    );
    assert!(source.contains("_authority: P::CompletionAuthority"));
}

#[test]
fn startup_migration_port_exposes_identity_but_no_catalog_authority() {
    let source = include_str!("../src/startup.rs");
    for required in [
        "pub trait StartupIndexMigrationPort: Sized {",
        "fn database_id(&self) -> DatabaseId;",
        "fn open_session_id(&self) -> OpenSessionId;",
    ] {
        assert!(
            source.contains(required),
            "missing identity-only port surface `{required}`"
        );
    }

    for forbidden in [
        "IndexMigrationCatalogAuthority",
        "IndexMigrationCatalogTracker",
        "IndexMigrationCatalogAdvance",
        "IndexMigrationCatalogCompletion",
        "IndexMigrationPageWork",
        "IndexMigrationPageStep",
        "IndexMigrationInstruction",
        "IndexMigrationInstructionBatch",
        "StartupIndexMigrationRead",
        "StartupIndexMigrationEnd",
        "fn read_index_migration_page",
        "fn apply_index_migration",
        "fn finish_index_migration",
    ] {
        assert!(
            !source.contains(forbidden),
            "storage API retained migration authority through `{forbidden}`"
        );
    }
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
    assert!(records.contains("expected_pending.partition_key() != stored_outcome.partition_key()"));
    assert!(records.contains("validate_intent_entity_derivation(evaluated, &entities)?"));
    assert!(records.contains("validate_intent_event_derivation(evaluated, sequence, &events)?"));
    assert!(records.contains("pub fn matches_retained_candidate("));
    assert!(records.contains("pub fn matches_reserved_candidate("));
    assert!(records.contains("pub fn matches_durability_mode("));
}

#[test]
fn aggregate_cap_exceedance_requires_a_codec_minted_origin() {
    assert!(
        PROTO_CODEC_BOUNDS.contains(
            "#[derive(Debug, Eq, PartialEq)]\n#[non_exhaustive]\npub struct AggregateCapExceededOriginV1 {\n    _codec_origin: (),\n}"
        )
    );
    assert!(PROTO_CODEC_BOUNDS.contains("const fn from_final_comparison() -> Self"));
    assert!(!PROTO_CODEC_BOUNDS.contains("pub fn from_final_comparison"));
    assert!(!PROTO_CODEC_BOUNDS.contains("pub const fn from_final_comparison"));
    assert!(
        PROTO_CODEC_BOUNDS.contains("ExceedsAcceptedAggregateCap(AggregateCapExceededOriginV1),")
    );
    assert!(PROTO_CODEC_BOUNDS.contains("AggregateCapExceededOriginV1::from_final_comparison(),"));
    assert!(!PROTO_CODEC_BOUNDS.contains("ExceedsAcceptedAggregateCap,"));
}

#[test]
fn sequence_free_semantic_shape_is_the_single_plan_validation_authority() {
    let records = include_str!("../src/records.rs");
    for required in [
        "#[derive(Eq, PartialEq)]\npub struct ValidatedCommandWriteSetShapeV1 {",
        "pub fn new(\n        intent: &CommitIntent,",
        "validate_index_entries(&index_entries)?;",
        "validate_index_epochs(&index_epochs)?;",
        "validate_affected_epoch_coverage(",
        "validate_post_image_bindings(",
        "projected_atomic_semantic_breakdown(intent, &index_entries, &index_epochs)?",
        "let shape = ValidatedCommandWriteSetShapeV1::new(",
        "Ok(Self::from_validated_shape(shape, encoded_upper_bound))",
        "pub fn from_validated_shape(",
        "CommandWriteSetChargeV1::from_validated_shape(&shape, encoded_upper_bound)",
    ] {
        assert!(
            records.contains(required),
            "write-shape authority is missing {required}"
        );
    }
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
        "riffdb_proto",
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
    assert!(manifest.contains("riffdb-proto"));
    assert!(manifest.contains("prost"));
    for forbidden in [
        "riffdb-runtime",
        "riffdb-commit",
        "redb",
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
