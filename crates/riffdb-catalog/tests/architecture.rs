//! Compile-time source boundary checks for the catalog/storage split.

const CATALOG_MANIFEST: &str = include_str!("../Cargo.toml");
const CATALOG_BUNDLE: &str = include_str!("../src/bundle.rs");
const CATALOG_CAPABILITY_PARTITION: &str = include_str!("../src/capability_partition.rs");
const CATALOG_DEPLOYMENT: &str = include_str!("../src/deployment.rs");
const CATALOG_HISTORY: &str = include_str!("../src/history.rs");
const CATALOG_LINEAGE: &str = include_str!("../src/lineage.rs");
const CATALOG_LIB: &str = include_str!("../src/lib.rs");
const CATALOG_MATERIALIZATION: &str = include_str!("../src/materialization.rs");
const CATALOG_PROJECTION_MATERIALIZATION: &str =
    include_str!("../src/projection_materialization.rs");
const STORAGE_MANIFEST: &str = include_str!("../../riffdb-storage-api/Cargo.toml");
const STORAGE_CATALOG: &str = include_str!("../../riffdb-storage-api/src/catalog.rs");
const STORAGE_LIB: &str = include_str!("../../riffdb-storage-api/src/lib.rs");
const STORAGE_STARTUP: &str = include_str!("../../riffdb-storage-api/src/startup.rs");
const INVARIANT_MANIFEST: &str = include_str!("../../riffdb-invariant/Cargo.toml");

#[test]
fn catalog_prepares_but_cannot_submit_storage_mutations() {
    let catalog_sources = [
        CATALOG_BUNDLE,
        CATALOG_CAPABILITY_PARTITION,
        CATALOG_DEPLOYMENT,
        CATALOG_HISTORY,
        CATALOG_LINEAGE,
        CATALOG_MATERIALIZATION,
        CATALOG_LIB,
    ]
    .join("\n");

    for forbidden in ["CatalogAdministrationRepository", "activate_catalog("] {
        assert!(
            !catalog_sources.contains(forbidden),
            "catalog source acquired mutation authority through {forbidden}"
        );
    }
    assert!(catalog_sources.contains("CatalogActivationIntentV1"));
}

#[test]
fn command_materialization_authority_has_no_serializable_or_cloneable_escape() {
    let production = CATALOG_MATERIALIZATION
        .split("\n#[cfg(test)]\nmod tests")
        .next()
        .expect("materialization production source");
    for wrapper in [
        "MaterializedCommandSnapshot",
        "MaterializedTransactionCurrentState",
        "CommandSnapshotResourceLimitEvidence",
    ] {
        assert!(production.contains(&format!("pub struct {wrapper} {{")));
        assert!(!production.contains(&format!("#[derive(Clone)]\npub struct {wrapper}")));
    }
    for forbidden in [
        "Serialize",
        "Deserialize",
        "pub fn new(",
        "pub fn masks(",
        "pub fn into_snapshot(",
        "pub fn into_state(",
    ] {
        assert!(
            !production.contains(forbidden),
            "process-local materialization authority escaped through {forbidden}"
        );
    }
    assert!(production.contains("resolved_plan: ResolvedExecutablePlan,"));
    assert!(production.contains("raw_snapshot: ReadSnapshot,"));
    assert!(!production.contains("raw_snapshot: ReadSnapshot,\n    masks:"));
    assert!(production.contains("state.bindings() != self.snapshot.bindings()"));
    assert!(production.contains("state.root_validations() != self.snapshot.root_validations()"));
}

#[test]
fn projection_materialization_view_is_move_only_and_hides_the_durable_event() {
    assert!(
        CATALOG_PROJECTION_MATERIALIZATION
            .contains("pub struct ProjectionEventMaterializationView<'plan, 'event> {")
    );
    assert!(
        !CATALOG_PROJECTION_MATERIALIZATION
            .contains("#[derive(Clone)]\npub struct ProjectionEventMaterializationView")
    );
    for forbidden in [
        "Serialize",
        "Deserialize",
        "pub fn source_event(",
        "pub fn raw_payload(",
        "pub fn event_hash(",
        "pub fn event_id(",
        "pub fn into_payload(",
    ] {
        assert!(
            !CATALOG_PROJECTION_MATERIALIZATION.contains(forbidden),
            "projection materialization authority escaped through {forbidden}"
        );
    }
    for required in [
        "_source_event: &'event StoredDurableEventV1,",
        "pub const fn known_payload(&self) -> &CanonicalRecord",
        "pub const fn projection_plan(&self) -> &ProjectionPlan",
        "Instruction::EmitEvent(construction)",
        "DurableKeySchemaBindingV1::from_plan(writer)",
    ] {
        assert!(
            CATALOG_PROJECTION_MATERIALIZATION.contains(required),
            "projection materialization omitted `{required}`"
        );
    }
    assert!(!STORAGE_LIB.contains("ProjectionEventMaterializationView"));
    assert!(!STORAGE_CATALOG.contains("ProjectionEventMaterializationView"));
}

#[test]
fn symbolic_event_materialization_never_exposes_the_raw_event_or_private_actor_identity() {
    const SOURCE: &str = include_str!("../src/event_materialization.rs");
    const REPLAY: &str = include_str!("../src/event_replay.rs");

    assert!(SOURCE.contains("pub struct SymbolicEventView"));
    assert!(SOURCE.contains("formatter.write_str(\"SymbolicEventView([REDACTED])\")"));
    assert!(!SOURCE.contains("pub fn source_event("));
    assert!(!SOURCE.contains("pub fn payload("));
    assert!(!SOURCE.contains("pub fn principal_id("));
    assert!(!SOURCE.contains("pub fn agent_session_id("));
    assert!(!SOURCE.contains("pub fn partition_hash("));
    assert!(!SOURCE.contains("pub fn conflict_hashes("));

    let envelope = REPLAY
        .split_once("pub struct SymbolicEventEnvelope")
        .expect("symbolic event envelope")
        .1
        .split_once("impl fmt::Debug for SymbolicEventEnvelope")
        .expect("bounded envelope implementation")
        .0;
    for forbidden in [
        "pub fn principal_id(",
        "pub fn agent_session_id(",
        "pub fn partition_hash(",
        "pub fn conflict_hashes(",
        "pub fn payload(",
    ] {
        assert!(
            !envelope.contains(forbidden),
            "symbolic event envelope escaped through {forbidden}"
        );
    }
    for required in [
        "pub const fn actor_kind(",
        "pub const fn provenance_id(",
        "pub const fn history_incarnation(",
        "pub const fn root_request_id(",
        "pub fn fields(",
    ] {
        assert!(
            envelope.contains(required),
            "symbolic event envelope omitted {required}"
        );
    }
}

#[test]
fn catalog_storage_module_remains_ir_opaque_and_history_proof_remains_catalog_owned() {
    let storage_sources = [STORAGE_CATALOG, STORAGE_LIB].join("\n");

    assert!(!STORAGE_CATALOG.contains("riffdb_contract_ir"));
    assert!(!storage_sources.contains("ValidatedCatalogHistory"));
    assert!(!storage_sources.contains("LineageMaterializationProof"));
    assert!(!STORAGE_CATALOG.contains("KeySchema"));

    assert!(CATALOG_HISTORY.contains("pub struct ValidatedCatalogHistory"));
    assert!(!CATALOG_HISTORY.contains("Serialize"));
    assert!(!CATALOG_HISTORY.contains("Deserialize"));
    let validated_history_impl = CATALOG_HISTORY
        .split_once("impl ValidatedCatalogHistory {")
        .expect("validated history implementation")
        .1
        .split_once("impl fmt::Debug for ValidatedCatalogHistory")
        .expect("validated history implementation boundary")
        .0;
    assert!(!validated_history_impl.contains("pub fn new("));
    assert!(CATALOG_LINEAGE.contains("pub(crate) struct LineageMaterializationProof"));
    assert!(!CATALOG_LINEAGE.contains("Serialize"));
    assert!(!CATALOG_LINEAGE.contains("Deserialize"));
    assert!(!CATALOG_LIB.contains("pub use lineage"));
    assert!(CATALOG_BUNDLE.contains("lineage_proof: Arc<LineageMaterializationProof>"));
    assert!(CATALOG_BUNDLE.contains("executing_ordinal: u16"));
    assert!(!CATALOG_BUNDLE.contains("is_structural_genesis(self)"));
    assert_eq!(
        CATALOG_BUNDLE
            .matches("pub(crate) fn resolve_plan(")
            .count(),
        1,
        "only ActiveCatalogSnapshot may expose the internal resolver"
    );
    assert!(CATALOG_BUNDLE.contains("let active = ActiveCatalogSnapshot::read(repository)?"));
    assert!(CATALOG_BUNDLE.contains("active.resolve_plan(reference)"));
}

#[test]
fn catalog_storage_boundary_has_no_generic_semantic_validation_callback() {
    for source in [CATALOG_HISTORY, CATALOG_LINEAGE, STORAGE_CATALOG] {
        for forbidden in ["impl Fn", "dyn Fn", "FnMut", "FnOnce"] {
            assert!(
                !source.contains(forbidden),
                "catalog/storage validation boundary accepts {forbidden}"
            );
        }
    }

    assert!(CATALOG_MANIFEST.contains("riffdb-storage-api"));
    assert!(!CATALOG_MANIFEST.contains("riffdb-service"));
    assert!(!STORAGE_MANIFEST.contains("riffdb-catalog"));
}

#[test]
fn historical_partition_derivation_uses_only_the_pure_invariant_evaluator() {
    assert!(
        CATALOG_MANIFEST
            .contains("riffdb-invariant = { version = \"0.1.0\", path = \"../riffdb-invariant\" }")
    );
    assert!(!INVARIANT_MANIFEST.contains("riffdb-catalog"));
    assert!(CATALOG_HISTORY.contains(
        "use riffdb_invariant::{EvaluationError, ExpressionValueSource, evaluate_expression};"
    ));

    let evaluator = CATALOG_HISTORY
        .split_once("fn derive_historical_partition(")
        .expect("historical partition evaluator")
        .1
        .split_once("fn validate_historical_bundle_link(")
        .expect("historical evaluator boundary")
        .0;
    assert_eq!(evaluator.matches("evaluate_expression(").count(), 1);
    assert!(evaluator.contains("aggregate.keys().expressions()"));
    assert!(evaluator.contains("aggregate.keys().partition_expression()"));
    assert!(evaluator.contains("impl ExpressionValueSource for HistoricalRootKeyValues<'_>"));
    assert!(evaluator.contains("fn schema_field("));

    for forbidden in [
        "StorageError",
        "StructuralEvidenceSession",
        "Repository",
        "StorageEngine",
        "Redb",
        "MemoryStore",
        "impl Fn",
        "dyn Fn",
        "FnOnce",
        "SystemTime",
        "Instant",
        "thread_rng",
        "getrandom",
        "async fn",
        ".await",
        "CommandPlan",
        "EvaluatedCommand",
        "CommandRuntime",
        "ExecutablePlan",
        "transaction_time(",
        "transaction_date(",
    ] {
        assert!(
            !evaluator.contains(forbidden),
            "historical partition evaluator acquired forbidden authority through `{forbidden}`"
        );
    }
}

#[test]
fn catalog_exclusively_owns_consuming_migration_tracking_and_completion() {
    for required in [
        "type BackendBrand<B> = PhantomData<fn(B) -> B>;",
        "pub trait CatalogIndexMigrationBackend: StartupIndexMigrationPort + Sized",
        "request: CatalogIndexMigrationScanRequest<Self>",
        "request: CatalogIndexMigrationBundleRequest<Self>",
        "pending: CatalogIndexMigrationPendingBatch<Self>",
        "completion: CatalogIndexMigrationCompletion<Self>",
        "pub struct CatalogIndexMigrationBatch<B> {",
        "pub struct CatalogIndexMigrationPendingBatch<B> {",
        "pub struct CatalogIndexMigrationCompletion<B> {",
        "pub fn run(self) -> Result<B::Output, CatalogIndexMigrationDriveError>",
        "while let Some(row) = rows.pop_front()",
        "derive_index_migration_instruction(&context, row, bundle)",
        ".finish_index_migration(completion)",
    ] {
        assert!(
            CATALOG_HISTORY.contains(required),
            "catalog migration state omitted `{required}`"
        );
    }

    for forbidden in [
        "IndexMigrationCatalogAuthority",
        "IndexMigrationCatalogTracker",
        "IndexMigrationCatalogAdvance",
        "IndexMigrationCatalogCompletion",
        "IndexMigrationPageWork",
        "IndexMigrationInstructionBatch",
        "pub fn into_v1_rewrite",
        "pub fn into_v2_confirm",
    ] {
        assert!(
            !STORAGE_STARTUP.contains(forbidden),
            "storage API retained forgeable migration authority through `{forbidden}`"
        );
    }

    for proof in [
        "CatalogIndexMigrationBatch<B>",
        "CatalogIndexMigrationPendingBatch<B>",
        "CatalogIndexMigrationApplied<B>",
        "CatalogIndexMigrationCompletion<B>",
    ] {
        let declaration = CATALOG_HISTORY
            .split_once(&format!("pub struct {proof}"))
            .unwrap_or_else(|| panic!("missing proof declaration {proof}"))
            .1
            .split_once('}')
            .expect("closed proof body")
            .0;
        assert!(
            declaration.contains("_backend: BackendBrand<B>"),
            "{proof} is not invariantly backend-branded"
        );
    }

    assert!(CATALOG_HISTORY.contains("```compile_fail"));
    assert!(CATALOG_HISTORY.contains("fn substitute<A, B>("));
    assert!(!CATALOG_HISTORY.contains("pub fn derive_index_migration_instruction"));
    assert!(!CATALOG_HISTORY.contains("pub fn new(\n        start: IndexMigrationCursor"));
}

#[test]
fn catalog_owns_one_pure_capability_partition_schema_validator() {
    for required in [
        "pub fn validate_capability_partition(",
        "pub fn validate_capability_partition_scope(",
        "partition.lineage() != contract.lineage()",
        ".aggregate(partition.partition_key().aggregate_type_id())",
        ".decode_partition(partition.partition_key())",
    ] {
        assert!(
            CATALOG_CAPABILITY_PARTITION.contains(required),
            "catalog capability validator omitted `{required}`"
        );
    }
    for forbidden in ["riffdb_service", "riffdb_storage_api", "dyn Fn", "impl Fn"] {
        assert!(
            !CATALOG_CAPABILITY_PARTITION.contains(forbidden),
            "catalog capability validator acquired forbidden authority through `{forbidden}`"
        );
    }
    assert!(
        CATALOG_HISTORY
            .contains("validate_capability_partition(active, evidence.scoped_partition())"),
        "startup history bypassed the shared catalog-owned capability validator"
    );
}

#[test]
fn every_active_lineage_ingress_uses_the_exact_canonical_byte_budget() {
    let production_lineage = CATALOG_LINEAGE
        .split("\n#[cfg(test)]\nmod tests")
        .next()
        .expect("lineage production source");
    for required in [
        "budget.push_bundle(bundle.bundle().canonical_bytes().len())?;",
        "budget.push_bundle(stored.canonical_bytes().len())?;",
        "projected.push_bundle(candidate.bundle().canonical_bytes().len())?;",
    ] {
        assert!(
            production_lineage.contains(required),
            "active-lineage path bypassed the canonical-byte budget: {required}"
        );
    }
    assert!(
        CATALOG_HISTORY.contains(".push_bundle(evidence.bytes().as_bytes().len())"),
        "same-session startup evidence bypassed the canonical-byte budget"
    );
}

#[test]
fn catalog_revalidates_the_complete_compiler_owned_command_registry() {
    for required in [
        "MCP_COMMAND_NAME_REGISTRY_VERSION_V2",
        "registry.lineage() != bundle.lineage()",
        "registry.entries().len() != bundle.commands().len()",
        "registry.entries().iter().zip(bundle.commands())",
        "entry.command_id() != command.command_id()",
        "entry.source_command_name() != command.name()",
        "McpCommandToolNameV2::new_checked",
        "!names.insert(entry.tool_name().as_str())",
    ] {
        assert!(
            CATALOG_BUNDLE.contains(required),
            "catalog omitted registry check `{required}`"
        );
    }
}
