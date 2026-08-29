#![forbid(unsafe_code)]

//! Covering-index migration evidence.
//!
//! SPEC.md forbids an existing index identity from gaining or changing a
//! cover: "evolution declares a new index and uses the ordinary receipted
//! rebuild/migration path". That rebuild path is exercised here. SPEC.md also
//! requires that, for a V14 covering index, the exact canonical covered record
//! is derived from the entity post-image — the same post-image the index key
//! is derived from. A rebuilt entry that carries no cover is indistinguishable
//! from durable corruption to the covered read, which fails closed on a
//! cover-field-set mismatch rather than hydrating from the entity.

use riffdb_catalog::{ValidatedContractBundle, ValidatedMigrationPlan};
use riffdb_commit::MigrationCoordinator;
use riffdb_contract_compiler::{
    compile_contract_source, compile_contract_successor, compile_migration_source,
};
use riffdb_contract_ir::MigrationStepKindV1;
use riffdb_query_compiler::compile_query;
use riffdb_query_ir::SymbolicCatalog;
use riffdb_riffql_syntax::parse_query;
use riffdb_storage_api::{
    ApplicationSequenceAllocator, DurableKeySchemaBindingV1, EntityTarget, StoredEntityRecordV1,
};
use riffdb_storage_memory::{MemoryMigrationHistoryWitness, MemoryMigrationStage};
use riffdb_types::{CanonicalRecord, CanonicalValue, EntityVersion, Timestamp};

const CONTRACT: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../examples/app-baseline/contracts/ticketdesk.riff"
));
const BOARD_QUERY: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../queries/ticketdesk/board_page_450.riffq"
));

/// The exact declaration the predecessor fixture must not carry. Stripping it
/// yields a lawful predecessor in which `by_board_project_status` does not yet
/// exist, so the successor introduces it as a new index identity.
const COVERED_INDEX_LINE: &str = "    index by_board_project_status (organization_id, project_id, status, ticket_id) cover (title, reporter_id, assignee_id)\n";

/// Migrating a database that gains a covering index must write the complete
/// canonical cover, because the covered read hard-checks the stored cover
/// field-ID set and fails closed when it does not match.
#[test]
fn adding_a_covering_index_rebuilds_entries_with_their_complete_cover() {
    let (parent, candidate, plan) = covering_index_plan();

    // What the covered read demands of every stored entry it scans.
    let catalog = SymbolicCatalog::from_bundle(candidate.bundle()).expect("successor catalog");
    let program =
        compile_query(&parse_query(BOARD_QUERY).expect("board parse"), &catalog).expect("program");
    let layout = program.steps()[0]
        .covered_result_layout()
        .expect("board page must compile to a covered plan");
    let mut expected_cover = layout.internal_cover_field_ids().to_vec();
    expected_cover.sort_unstable();
    assert_eq!(
        expected_cover.len(),
        3,
        "board page covers title, reporter_id and assignee_id"
    );

    let source = ticket_row(&parent);
    let mut stage = memory_stage(&parent, vec![source.clone()]);
    MigrationCoordinator::apply(&plan, &mut stage).expect("covering-index migration applies");

    // Only the newly declared index is rebuilt: no existing index identity
    // changed, so the entity is not rebound.
    let entries = stage.index_entries();
    assert_eq!(
        entries.len(),
        1,
        "exactly the new covering index is rebuilt for the one migrated ticket"
    );
    let entry = &entries[0];

    let stored_cover = entry
        .covered_values()
        .fields()
        .iter()
        .map(|(field, _)| *field)
        .collect::<Vec<_>>();
    assert_eq!(
        stored_cover, expected_cover,
        "a rebuilt covering-index entry must carry the exact cover field set the \
         covered read requires; an empty or partial cover is read as corruption \
         and fails the query closed"
    );

    // The cover must come from the same post-image the index key came from.
    for (field, value) in entry.covered_values().fields() {
        let expected = source
            .fields()
            .fields()
            .iter()
            .find_map(|(candidate, value)| (candidate == field).then_some(value))
            .expect("covered field is a direct field of the migrated entity");
        assert_eq!(
            value, expected,
            "covered value must be derived from the migrated post-image"
        );
    }
}

/// A migration that changes an unrelated index rebinds the whole entity, so
/// every index it owns is rebuilt — including a covering index that was
/// already correct. Such a migration must not strip a working cover.
#[test]
fn an_unrelated_index_change_does_not_strip_an_existing_cover() {
    // Predecessor already declares the covering index, so its entries are
    // sound before this migration runs.
    let parent_bundle = compile_contract_source(CONTRACT).expect("predecessor contract");
    let successor_source = CONTRACT.replace("version 1", "version 2").replace(
        "index by_assignee_status (organization_id, assignee_id, status, ticket_id)",
        "index by_assignee_status (organization_id, assignee_id, ticket_id)",
    );
    assert!(
        successor_source
            .contains("index by_assignee_status (organization_id, assignee_id, ticket_id)"),
        "the unrelated index declaration this fixture rewrites has changed"
    );
    let candidate_bundle = compile_contract_successor(&successor_source, &parent_bundle)
        .expect("successor changes an unrelated index");
    let migration = compile_migration_source(
        "migration TicketDesk from 1 to 2 {}",
        &parent_bundle,
        &candidate_bundle,
    )
    .expect("derived rebuild proof");

    let parent = ValidatedContractBundle::from_compiler_bundle(parent_bundle).expect("parent");
    let candidate =
        ValidatedContractBundle::from_compiler_bundle(candidate_bundle).expect("candidate");
    let (covering_id, mut expected_cover) = {
        let covering = candidate
            .bundle()
            .schema()
            .entities()
            .iter()
            .find(|entity| entity.name() == "Ticket")
            .expect("Ticket")
            .indexes()
            .iter()
            .find(|index| index.name() == "by_board_project_status")
            .expect("covering index");
        (covering.id(), covering.cover_fields().to_vec())
    };
    assert_eq!(
        expected_cover.len(),
        3,
        "the covering index still covers three fields"
    );
    expected_cover.sort_unstable();

    let plan = ValidatedMigrationPlan::from_artifacts(parent.clone(), candidate, migration)
        .expect("sealed plan");
    let mut stage = memory_stage(&parent, vec![ticket_row(&parent)]);
    MigrationCoordinator::apply(&plan, &mut stage).expect("migration applies");

    let rebuilt = stage
        .index_entries()
        .iter()
        .find(|entry| entry.key().index_id() == covering_id)
        .expect("the rebound entity rebuilds every index it owns");
    let stored_cover = rebuilt
        .covered_values()
        .fields()
        .iter()
        .map(|(field, _)| *field)
        .collect::<Vec<_>>();
    assert_eq!(
        stored_cover, expected_cover,
        "rebuilding an entity for an unrelated index change must preserve the \
         covering index's complete cover, not silently empty it"
    );
}

/// The rebuilt entry must bind to the successor contract, so a covered read
/// compiled against that contract accepts it rather than rejecting the binding.
#[test]
fn rebuilt_covering_entries_bind_to_the_successor_contract() {
    let (parent, candidate, plan) = covering_index_plan();
    let mut stage = memory_stage(&parent, vec![ticket_row(&parent)]);
    MigrationCoordinator::apply(&plan, &mut stage).expect("covering-index migration applies");

    let entry = &stage.index_entries()[0];
    assert_eq!(entry.schema_binding().lineage(), candidate.lineage());
    assert_eq!(
        entry.schema_binding().contract_version(),
        candidate.contract_version()
    );
    assert_eq!(
        entry.schema_binding().bundle_hash(),
        candidate.bundle_hash()
    );
}

/// Predecessor without the covering index, successor that declares it, and the
/// derived rebuild proof that carries them apart.
fn covering_index_plan() -> (
    ValidatedContractBundle,
    ValidatedContractBundle,
    ValidatedMigrationPlan,
) {
    let parent_source = CONTRACT.replace(COVERED_INDEX_LINE, "");
    assert!(
        !parent_source.contains("by_board_project_status"),
        "the covering-index declaration this fixture strips has changed in \
         examples/app-baseline/contracts/ticketdesk.riff; update COVERED_INDEX_LINE"
    );
    let parent_bundle = compile_contract_source(&parent_source).expect("predecessor contract");
    let candidate_bundle =
        compile_contract_successor(&CONTRACT.replace("version 1", "version 2"), &parent_bundle)
            .expect("successor declares the covering index");
    let migration = compile_migration_source(
        "migration TicketDesk from 1 to 2 {}",
        &parent_bundle,
        &candidate_bundle,
    )
    .expect("derived rebuild proof");
    assert!(
        migration
            .steps()
            .iter()
            .any(|step| matches!(step.kind(), MigrationStepKindV1::RebuildIndex { .. })),
        "declaring a new covering index must derive a RebuildIndex step"
    );

    let parent = ValidatedContractBundle::from_compiler_bundle(parent_bundle).expect("parent");
    let candidate =
        ValidatedContractBundle::from_compiler_bundle(candidate_bundle).expect("candidate");
    let plan = ValidatedMigrationPlan::from_artifacts(parent.clone(), candidate.clone(), migration)
        .expect("sealed covering-index plan");
    (parent, candidate, plan)
}

fn memory_stage(
    parent: &ValidatedContractBundle,
    rows: Vec<StoredEntityRecordV1>,
) -> MemoryMigrationStage {
    let history = MemoryMigrationHistoryWitness::new(
        ApplicationSequenceAllocator::initial(),
        b"immutable-covering-index-history".to_vec(),
    )
    .expect("history witness");
    MemoryMigrationStage::new(parent.to_stored().expect("stored parent"), rows, history)
        .expect("memory stage")
}

fn ticket_row(parent: &ValidatedContractBundle) -> StoredEntityRecordV1 {
    named_row(
        parent,
        "Ticket",
        vec![
            ("organization_id", CanonicalValue::Uuid(uuid_for(1))),
            ("ticket_id", CanonicalValue::Uuid(uuid_for(2))),
            ("project_id", CanonicalValue::Uuid(uuid_for(3))),
            ("reporter_id", CanonicalValue::Uuid(uuid_for(4))),
            ("assignee_id", CanonicalValue::Uuid(uuid_for(5))),
            ("status", enum_value(parent, "TicketStatus", "Open")),
            (
                "title",
                CanonicalValue::string("covered ticket").expect("title"),
            ),
            ("created_at", timestamp(1_700_000_000)),
            ("updated_at", timestamp(1_700_000_100)),
        ],
    )
}

fn named_row(
    parent: &ValidatedContractBundle,
    entity_name: &str,
    values: Vec<(&str, CanonicalValue)>,
) -> StoredEntityRecordV1 {
    let entity = parent
        .bundle()
        .schema()
        .entities()
        .iter()
        .find(|entity| entity.name() == entity_name)
        .expect("fixture entity");
    let fields = entity
        .record()
        .fields()
        .iter()
        .map(|field| {
            let value = values
                .iter()
                .find_map(|(name, value)| (*name == field.name()).then(|| value.clone()))
                .expect("fixture field value");
            (field.id(), value)
        })
        .collect::<Vec<_>>();
    let record = CanonicalRecord::new(fields).expect("canonical named row");
    let key_values = entity
        .primary_key_fields()
        .iter()
        .map(|field_id| {
            record
                .fields()
                .iter()
                .find_map(|(candidate, value)| (candidate == field_id).then(|| value.clone()))
                .expect("primary key value")
        })
        .collect::<Vec<_>>();
    let key = entity
        .primary_key()
        .encode_entity(&key_values)
        .expect("canonical entity key");
    let target = EntityTarget::new(entity.id(), key).expect("named target");
    StoredEntityRecordV1::new(
        target,
        EntityVersion::first(),
        parent.contract_version(),
        DurableKeySchemaBindingV1::new(
            parent.lineage().clone(),
            parent.contract_version(),
            parent.bundle_hash(),
        ),
        record,
    )
    .expect("stored named row")
}

fn enum_value(parent: &ValidatedContractBundle, type_name: &str, variant: &str) -> CanonicalValue {
    let enumeration = parent
        .bundle()
        .schema()
        .enums()
        .iter()
        .find(|enumeration| enumeration.name() == type_name)
        .expect("fixture enum");
    CanonicalValue::Enum {
        type_id: enumeration.id(),
        variant_id: enumeration
            .variants()
            .iter()
            .find(|candidate| candidate.name() == variant)
            .expect("fixture variant")
            .id(),
    }
}

fn timestamp(seconds: i64) -> CanonicalValue {
    CanonicalValue::Timestamp(Timestamp::new(seconds, 0).expect("fixture timestamp"))
}

fn uuid_for(value: i64) -> [u8; 16] {
    let mut bytes = [0u8; 16];
    bytes[0..8].copy_from_slice(&value.to_be_bytes());
    bytes[6] = 0x70 | (bytes[6] & 0x0f);
    bytes[8] = 0x80 | (bytes[8] & 0x3f);
    bytes
}
