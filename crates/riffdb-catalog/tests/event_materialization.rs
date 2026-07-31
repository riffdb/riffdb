//! Symbolic historical event materialization and redaction semantics.

use riffdb_catalog::{
    ActiveCatalogSnapshot, EventMaterializationErrorKind, ValidatedContractBundle,
};
use riffdb_contract_compiler::{compile_contract_source, compile_contract_successor};
use riffdb_contract_ir::{ContractBundle, EventSchema};
use riffdb_storage_api::{
    ActiveCatalogPointerV1, CatalogRepository, ExecutablePlanRef, StorageError,
    StoredContractBundleV1, StoredDurableEventV1, derive_event_hash_v1,
};
use riffdb_types::{
    CanonicalRecord, CanonicalValue, CommitSequence, ContractLineage, EventId, FieldId,
};

const EVENT_V1: &str = include_str!("fixtures/projection_event_v1.riff");
const EVENT_V2: &str = include_str!("fixtures/projection_event_v2.riff");
const EVENT_V3: &str = include_str!("fixtures/projection_event_v3.riff");

struct ReadRepository {
    active: ActiveCatalogPointerV1,
    bundles: Vec<StoredContractBundleV1>,
}

impl CatalogRepository for ReadRepository {
    fn read_active_catalog(&self) -> Result<Option<ActiveCatalogPointerV1>, StorageError> {
        Ok(Some(self.active.clone()))
    }

    fn read_contract_bundle(
        &self,
        lineage: &ContractLineage,
        contract_version: riffdb_types::ContractVersion,
    ) -> Result<Option<StoredContractBundleV1>, StorageError> {
        Ok(self
            .bundles
            .iter()
            .find(|bundle| {
                bundle.lineage() == lineage && bundle.contract_version() == contract_version
            })
            .cloned())
    }
}

fn compile_chain() -> (Vec<ValidatedContractBundle>, ReadRepository) {
    let mut compiled: Vec<ContractBundle> = Vec::new();
    for source in [EVENT_V1, EVENT_V2, EVENT_V3] {
        let next = compiled.last().map_or_else(
            || compile_contract_source(source),
            |parent| compile_contract_successor(source, parent),
        );
        compiled.push(next.expect("event evolution fixture compiles"));
    }
    let bundles = compiled
        .into_iter()
        .map(ValidatedContractBundle::from_compiler_bundle)
        .collect::<Result<Vec<_>, _>>()
        .expect("event evolution fixture validates");
    let stored = bundles
        .iter()
        .map(|bundle| bundle.to_stored().expect("stored bundle"))
        .collect::<Vec<_>>();
    let active = ActiveCatalogPointerV1::from_bundle(stored.last().expect("active bundle"));
    (
        bundles,
        ReadRepository {
            active,
            bundles: stored,
        },
    )
}

fn event_schema(bundle: &ValidatedContractBundle) -> &EventSchema {
    bundle
        .bundle()
        .schema()
        .events()
        .first()
        .expect("fixture event")
}

fn field_id(event: &EventSchema, name: &str) -> FieldId {
    event
        .payload()
        .fields()
        .iter()
        .find(|field| field.name() == name)
        .map(riffdb_contract_ir::FieldSchema::id)
        .expect("fixture field")
}

fn plan_reference(bundle: &ValidatedContractBundle) -> ExecutablePlanRef {
    let command = bundle.bundle().commands().first().expect("fixture command");
    ExecutablePlanRef::new(
        bundle.lineage().clone(),
        bundle.contract_version(),
        bundle.bundle_hash(),
        command.command_id(),
        command.plan_hash(),
    )
}

fn event(
    bundle: &ValidatedContractBundle,
    fields: Vec<(&str, CanonicalValue)>,
) -> StoredDurableEventV1 {
    let schema = event_schema(bundle);
    let payload = CanonicalRecord::new(
        fields
            .into_iter()
            .map(|(name, value)| (field_id(schema, name), value))
            .collect(),
    )
    .expect("canonical event payload");
    let event_id = EventId::new(CommitSequence::first(), 0);
    let event_hash = derive_event_hash_v1(event_id, schema.id(), &payload).expect("event hash");
    StoredDurableEventV1::new(event_id, schema.id(), payload, event_hash).expect("stored event")
}

#[test]
fn symbolic_selection_normalizes_ancestors_and_hides_unselected_descendant_fields() {
    let (bundles, repository) = compile_chain();
    let active = ActiveCatalogSnapshot::read(&repository)
        .expect("catalog read")
        .expect("active catalog");
    let materializer = active
        .resolve_event_materializer("Changed", ["note", "id"])
        .expect("symbolic selection");
    assert_eq!(materializer.event_name(), "Changed");
    assert_eq!(
        materializer.selected_field_names().collect::<Vec<_>>(),
        ["note", "id"]
    );

    let ancestor = event(&bundles[0], vec![("id", CanonicalValue::Uuid([0x11; 16]))]);
    let view = materializer
        .materialize_event(&plan_reference(&bundles[0]), &ancestor)
        .expect("compatible ancestor event");
    assert_eq!(view.event_id(), ancestor.event_id());
    assert_eq!(view.event_name(), "Changed");
    assert_eq!(view.command_name(), "Change");
    assert_eq!(
        view.writer_contract_version(),
        bundles[0].contract_version()
    );
    assert_eq!(view.fields().len(), 2);
    assert_eq!(view.fields()[0].name(), "note");
    assert_eq!(view.fields()[0].value(), &CanonicalValue::Null);
    assert_eq!(view.fields()[1].name(), "id");
    assert_eq!(view.fields()[1].value(), &CanonicalValue::Uuid([0x11; 16]));
    assert_eq!(format!("{view:?}"), "SymbolicEventView([REDACTED])");
    assert!(format!("{:?}", view.fields()[1]).contains("[REDACTED]"));
    assert!(!format!("{:?}", view.fields()[1]).contains("11"));

    let descendant = event(
        &bundles[2],
        vec![
            ("id", CanonicalValue::Uuid([0x22; 16])),
            ("note", CanonicalValue::Null),
            ("tag", CanonicalValue::I64(99)),
        ],
    );
    let view = materializer
        .materialize_event(&plan_reference(&bundles[2]), &descendant)
        .expect("complete descendant event");
    assert_eq!(view.fields().len(), 2);
    assert!(view.fields().iter().all(|field| field.name() != "tag"));
}

#[test]
fn symbolic_resolution_and_historical_integrity_fail_closed() {
    let (bundles, repository) = compile_chain();
    let active = ActiveCatalogSnapshot::read(&repository)
        .expect("catalog read")
        .expect("active catalog");

    for result in [
        active.resolve_event_materializer("Missing", ["id"]),
        active.resolve_event_materializer("Changed", ["missing"]),
        active.resolve_event_materializer("Changed", ["id", "id"]),
    ] {
        assert_eq!(
            result.expect_err("invalid symbolic selection").kind(),
            EventMaterializationErrorKind::UnknownSymbol
        );
    }
    assert_eq!(
        active
            .resolve_event_materializer("Changed", std::iter::empty())
            .expect_err("empty selection")
            .kind(),
        EventMaterializationErrorKind::HardLimit
    );

    let materializer = active
        .resolve_event_materializer("Changed", ["id"])
        .expect("selection");
    let malformed = event(&bundles[1], vec![("id", CanonicalValue::Uuid([0x33; 16]))]);
    assert_eq!(
        materializer
            .materialize_event(&plan_reference(&bundles[1]), &malformed)
            .expect_err("schema-incomplete event")
            .kind(),
        EventMaterializationErrorKind::Integrity
    );
}
