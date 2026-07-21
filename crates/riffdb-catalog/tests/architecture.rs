//! Compile-time source boundary checks for the catalog/storage split.

const CATALOG_MANIFEST: &str = include_str!("../Cargo.toml");
const CATALOG_BUNDLE: &str = include_str!("../src/bundle.rs");
const CATALOG_DEPLOYMENT: &str = include_str!("../src/deployment.rs");
const CATALOG_HISTORY: &str = include_str!("../src/history.rs");
const CATALOG_LINEAGE: &str = include_str!("../src/lineage.rs");
const CATALOG_LIB: &str = include_str!("../src/lib.rs");
const STORAGE_MANIFEST: &str = include_str!("../../riffdb-storage-api/Cargo.toml");
const STORAGE_CATALOG: &str = include_str!("../../riffdb-storage-api/src/catalog.rs");
const STORAGE_LIB: &str = include_str!("../../riffdb-storage-api/src/lib.rs");

#[test]
fn catalog_prepares_but_cannot_submit_storage_mutations() {
    let catalog_sources = [
        CATALOG_BUNDLE,
        CATALOG_DEPLOYMENT,
        CATALOG_HISTORY,
        CATALOG_LINEAGE,
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
fn catalog_storage_module_remains_ir_opaque_and_history_proof_remains_catalog_owned() {
    let storage_sources = [STORAGE_CATALOG, STORAGE_LIB].join("\n");

    assert!(!STORAGE_CATALOG.contains("riffdb_contract_ir"));
    assert!(!storage_sources.contains("ValidatedCatalogHistory"));
    assert!(!storage_sources.contains("LineageMaterializationProof"));
    assert!(!STORAGE_CATALOG.contains("KeySchema"));

    assert!(CATALOG_HISTORY.contains("pub struct ValidatedCatalogHistory"));
    assert!(!CATALOG_HISTORY.contains("Serialize"));
    assert!(!CATALOG_HISTORY.contains("Deserialize"));
    assert!(!CATALOG_HISTORY.contains("pub fn new("));
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
    assert!(!STORAGE_MANIFEST.contains("riffdb-catalog"));
}

#[test]
fn catalog_revalidates_the_complete_compiler_owned_command_registry() {
    for required in [
        "MCP_COMMAND_NAME_REGISTRY_VERSION_V1",
        "registry.lineage() != bundle.lineage()",
        "registry.entries().len() != bundle.commands().len()",
        "registry.entries().iter().zip(bundle.commands())",
        "entry.command_id() != command.command_id()",
        "entry.source_command_name() != command.name()",
        "McpCommandToolNameV1::new_checked",
        "!names.insert(entry.tool_name().as_str())",
    ] {
        assert!(
            CATALOG_BUNDLE.contains(required),
            "catalog omitted registry check `{required}`"
        );
    }
}
