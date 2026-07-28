//! Catalog-owned projection plan resolution and opaque event normalization.

use riffdb_catalog::{
    ActiveCatalogSnapshot, CatalogErrorKind, ProjectionEventMaterializationErrorKind,
    ValidatedContractBundle,
};
use riffdb_contract_compiler::{compile_contract_source, compile_contract_successor};
use riffdb_contract_ir::{ContractBundle, EventSchema};
use riffdb_storage_api::{
    ActiveCatalogPointerV1, CatalogRepository, ExecutablePlanRef, StorageError,
    StoredContractBundleV1, StoredDurableEventV1, derive_event_hash_v1,
};
use riffdb_types::{
    CanonicalRecord, CanonicalValue, CommitSequence, ContractLineage, EventId, EventTypeId,
    FieldId, MAX_CANONICAL_DOCUMENT_BYTES, PlanHash, ProjectionId, ProjectionIdentity,
    ProjectionPlanHash, encode_canonical_record,
};

const EVENT_V1: &str = include_str!("fixtures/projection_event_v1.riff");
const EVENT_V2: &str = include_str!("fixtures/projection_event_v2.riff");
const EVENT_V3: &str = include_str!("fixtures/projection_event_v3.riff");
const LIMIT_V1: &str = include_str!("fixtures/projection_event_limit_v1.riff");
const LIMIT_V2: &str = include_str!("fixtures/projection_event_limit_v2.riff");

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

fn compile_chain(
    sources: &[&str],
) -> (
    Vec<ContractBundle>,
    Vec<ValidatedContractBundle>,
    ReadRepository,
) {
    let mut compiled = Vec::with_capacity(sources.len());
    for source in sources {
        let bundle = compiled.last().map_or_else(
            || compile_contract_source(source),
            |parent| compile_contract_successor(source, parent),
        );
        compiled.push(bundle.expect("fixture compiles"));
    }
    let validated = compiled
        .iter()
        .cloned()
        .map(ValidatedContractBundle::from_compiler_bundle)
        .collect::<Result<Vec<_>, _>>()
        .expect("fixtures pass catalog validation");
    let stored = validated
        .iter()
        .map(|bundle| bundle.to_stored().expect("stored fixture"))
        .collect::<Vec<_>>();
    let active = ActiveCatalogPointerV1::from_bundle(stored.last().expect("nonempty chain"));
    (
        compiled,
        validated,
        ReadRepository {
            active,
            bundles: stored,
        },
    )
}

fn projection_identity(bundle: &ValidatedContractBundle) -> ProjectionIdentity {
    let projection = bundle
        .bundle()
        .projections()
        .first()
        .expect("fixture projection");
    ProjectionIdentity::new(
        bundle.lineage().clone(),
        projection.projection_id(),
        projection.plan_hash(),
    )
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

fn record(event: &EventSchema, fields: Vec<(&str, CanonicalValue)>) -> CanonicalRecord {
    CanonicalRecord::new(
        fields
            .into_iter()
            .map(|(name, value)| (field_id(event, name), value))
            .collect(),
    )
    .expect("canonical fixture payload")
}

fn stored_event(event_type: EventTypeId, payload: CanonicalRecord) -> StoredDurableEventV1 {
    let event_id = EventId::new(CommitSequence::first(), 0);
    let event_hash =
        derive_event_hash_v1(event_id, event_type, &payload).expect("bounded event hash");
    StoredDurableEventV1::new(event_id, event_type, payload, event_hash)
        .expect("bounded stored event")
}

#[test]
fn strict_ancestor_null_fill_and_descendant_hiding_preserve_the_source_event() {
    let (_, bundles, repository) = compile_chain(&[EVENT_V1, EVENT_V2, EVENT_V3]);
    let active = ActiveCatalogSnapshot::read(&repository)
        .expect("catalog read")
        .expect("active catalog");
    let identity = projection_identity(&bundles[1]);
    let resolved = active
        .resolve_projection(&identity)
        .expect("historical projection identity");
    assert_eq!(resolved.identity(), &identity);
    assert_eq!(
        resolved.projection_plan(),
        bundles[1]
            .bundle()
            .projections()
            .first()
            .expect("projection")
    );

    let event_v1_schema = event_schema(&bundles[0]);
    let ancestor = stored_event(
        event_v1_schema.id(),
        record(
            event_v1_schema,
            vec![("id", CanonicalValue::Uuid([0x11; 16]))],
        ),
    );
    let ancestor_before = ancestor.clone();
    let ancestor_bytes_before =
        encode_canonical_record(ancestor.payload()).expect("canonical source bytes");
    let ancestor_view = resolved
        .materialize_event(&plan_reference(&bundles[0]), &ancestor)
        .expect("strict ancestor materializes");
    let projection_schema = event_schema(&bundles[1]);
    assert_eq!(
        ancestor_view.known_payload(),
        &record(
            projection_schema,
            vec![
                ("id", CanonicalValue::Uuid([0x11; 16])),
                ("note", CanonicalValue::Null),
            ],
        )
    );
    assert_eq!(ancestor_view.projection_plan(), resolved.projection_plan());
    assert_eq!(
        format!("{ancestor_view:?}"),
        "ProjectionEventMaterializationView([REDACTED])"
    );
    drop(ancestor_view);
    assert_eq!(ancestor, ancestor_before);
    assert_eq!(
        encode_canonical_record(ancestor.payload()).expect("canonical source bytes"),
        ancestor_bytes_before
    );
    assert_eq!(ancestor.event_hash(), ancestor_before.event_hash());

    let event_v3_schema = event_schema(&bundles[2]);
    let descendant = stored_event(
        event_v3_schema.id(),
        record(
            event_v3_schema,
            vec![
                ("id", CanonicalValue::Uuid([0x22; 16])),
                ("note", CanonicalValue::Null),
                ("tag", CanonicalValue::I64(9)),
            ],
        ),
    );
    let descendant_before = descendant.clone();
    let descendant_view = resolved
        .materialize_event(&plan_reference(&bundles[2]), &descendant)
        .expect("complete descendant materializes");
    assert_eq!(
        descendant_view.known_payload(),
        &record(
            projection_schema,
            vec![
                ("id", CanonicalValue::Uuid([0x22; 16])),
                ("note", CanonicalValue::Null),
            ],
        )
    );
    assert!(
        descendant_view
            .known_payload()
            .fields()
            .binary_search_by_key(&field_id(event_v3_schema, "tag"), |(id, _)| *id)
            .is_err(),
        "descendant-only field must be invisible"
    );
    drop(descendant_view);
    assert_eq!(descendant, descendant_before);

    let exact = stored_event(
        projection_schema.id(),
        record(
            projection_schema,
            vec![
                ("id", CanonicalValue::Uuid([0x33; 16])),
                ("note", CanonicalValue::Null),
            ],
        ),
    );
    let first = resolved
        .materialize_event(&plan_reference(&bundles[1]), &exact)
        .expect("exact event");
    let first_payload = first.known_payload().clone();
    drop(first);
    let second = resolved
        .materialize_event(&plan_reference(&bundles[1]), &exact)
        .expect("identical rebuild normalization");
    assert_eq!(second.known_payload(), &first_payload);
}

#[test]
fn resolution_requires_the_exact_active_lineage_projection_identity() {
    let (_, bundles, repository) = compile_chain(&[EVENT_V1, EVENT_V2, EVENT_V3]);
    let active = ActiveCatalogSnapshot::read(&repository)
        .expect("catalog read")
        .expect("active catalog");
    let exact = projection_identity(&bundles[1]);
    active
        .resolve_projection(&exact)
        .expect("exact historical projection");

    let foreign_lineage = ProjectionIdentity::new(
        ContractLineage::new("ForeignProjectionEventEvolution").expect("lineage"),
        exact.projection_id(),
        exact.plan_hash(),
    );
    let wrong_id = ProjectionIdentity::new(
        exact.contract_lineage().clone(),
        ProjectionId::try_from(exact.projection_id().get() + 1).expect("projection ID"),
        exact.plan_hash(),
    );
    let wrong_hash = ProjectionIdentity::new(
        exact.contract_lineage().clone(),
        exact.projection_id(),
        ProjectionPlanHash::from_bytes([0x55; 32]),
    );
    for (case, identity) in [
        ("foreign lineage", foreign_lineage),
        ("wrong projection ID", wrong_id),
        ("wrong projection hash", wrong_hash),
    ] {
        assert_eq!(
            active.resolve_projection(&identity).expect_err(case).kind(),
            CatalogErrorKind::UnknownExecutablePlan
        );
    }

    let debug = format!("{:?}", active.resolve_projection(&exact).expect("resolved"));
    assert!(debug.contains("[REDACTED]"));
    assert!(!debug.contains(exact.contract_lineage().as_str()));
    assert!(!debug.contains(&format!("{:?}", exact.plan_hash())));
}

#[test]
fn foreign_writers_omissions_types_bounds_and_plan_substitution_fail_closed() {
    let (_, bundles, repository) = compile_chain(&[EVENT_V1, EVENT_V2, EVENT_V3]);
    let active = ActiveCatalogSnapshot::read(&repository)
        .expect("catalog read")
        .expect("active catalog");
    let resolved_v1 = active
        .resolve_projection(&projection_identity(&bundles[0]))
        .expect("v1 projection");
    let resolved_v2 = active
        .resolve_projection(&projection_identity(&bundles[1]))
        .expect("v2 projection");
    let schema_v1 = event_schema(&bundles[0]);
    let schema_v2 = event_schema(&bundles[1]);
    let schema_v3 = event_schema(&bundles[2]);

    let exact_omission = stored_event(
        schema_v2.id(),
        record(schema_v2, vec![("id", CanonicalValue::Uuid([0x41; 16]))]),
    );
    let descendant_omission = stored_event(
        schema_v3.id(),
        record(
            schema_v3,
            vec![
                ("id", CanonicalValue::Uuid([0x42; 16])),
                ("note", CanonicalValue::Null),
            ],
        ),
    );
    let genesis_required_omission = stored_event(
        schema_v1.id(),
        CanonicalRecord::new(vec![]).expect("empty record"),
    );
    let wrong_type = stored_event(
        schema_v2.id(),
        record(
            schema_v2,
            vec![
                ("id", CanonicalValue::I64(1)),
                ("note", CanonicalValue::Null),
            ],
        ),
    );
    let over_bound = stored_event(
        schema_v2.id(),
        record(
            schema_v2,
            vec![
                ("id", CanonicalValue::Uuid([0x43; 16])),
                (
                    "note",
                    CanonicalValue::string("123456789").expect("global string bound"),
                ),
            ],
        ),
    );

    for (case, result) in [
        (
            "exact omission",
            resolved_v2.materialize_event(&plan_reference(&bundles[1]), &exact_omission),
        ),
        (
            "descendant omission",
            resolved_v2.materialize_event(&plan_reference(&bundles[2]), &descendant_omission),
        ),
        (
            "genesis required omission",
            resolved_v1.materialize_event(&plan_reference(&bundles[0]), &genesis_required_omission),
        ),
        (
            "wrong static type",
            resolved_v2.materialize_event(&plan_reference(&bundles[1]), &wrong_type),
        ),
        (
            "declared bound exceeded",
            resolved_v2.materialize_event(&plan_reference(&bundles[1]), &over_bound),
        ),
    ] {
        assert_eq!(
            result.expect_err(case).kind(),
            ProjectionEventMaterializationErrorKind::Integrity
        );
    }

    let foreign_source = EVENT_V1.replace(
        "ProjectionEventEvolution",
        "ForeignProjectionEventEvolution",
    );
    let foreign = compile_contract_source(&foreign_source).expect("foreign fixture");
    let foreign =
        ValidatedContractBundle::from_compiler_bundle(foreign).expect("foreign catalog bundle");
    let valid_v1 = stored_event(
        schema_v1.id(),
        record(schema_v1, vec![("id", CanonicalValue::Uuid([0x44; 16]))]),
    );
    assert_eq!(
        resolved_v1
            .materialize_event(&plan_reference(&foreign), &valid_v1)
            .expect_err("foreign writer")
            .kind(),
        ProjectionEventMaterializationErrorKind::Integrity
    );

    let exact_plan = plan_reference(&bundles[0]);
    let substituted = ExecutablePlanRef::new(
        exact_plan.contract_lineage().clone(),
        exact_plan.contract_version(),
        exact_plan.contract_bundle_hash(),
        exact_plan.command_id(),
        PlanHash::from_bytes([0x77; 32]),
    );
    assert_eq!(
        resolved_v1
            .materialize_event(&substituted, &valid_v1)
            .expect_err("command plan substitution")
            .kind(),
        ProjectionEventMaterializationErrorKind::Integrity
    );

    let wrong_event_type = EventTypeId::try_from(schema_v1.id().get() + 1).expect("event ID");
    let wrong_event = stored_event(
        wrong_event_type,
        CanonicalRecord::new(vec![]).expect("empty record"),
    );
    assert_eq!(
        resolved_v1
            .materialize_event(&plan_reference(&bundles[0]), &wrong_event)
            .expect_err("wrong event type")
            .kind(),
        ProjectionEventMaterializationErrorKind::Integrity
    );
}

#[test]
fn proof_authorized_null_expansion_rejects_one_byte_over_the_document_limit() {
    let (_, bundles, repository) = compile_chain(&[LIMIT_V1, LIMIT_V2]);
    let active = ActiveCatalogSnapshot::read(&repository)
        .expect("catalog read")
        .expect("active catalog");
    let resolved = active
        .resolve_projection(&projection_identity(&bundles[1]))
        .expect("v2 projection");
    let schema_v1 = event_schema(&bundles[0]);
    let payload = record(
        schema_v1,
        vec![
            ("bucket", CanonicalValue::I64(7)),
            (
                "data",
                CanonicalValue::bytes(vec![0x5a; 1_048_546]).expect("global bytes bound"),
            ),
        ],
    );
    assert_eq!(
        encode_canonical_record(&payload)
            .expect("exact-limit payload")
            .len(),
        MAX_CANONICAL_DOCUMENT_BYTES
    );
    let event = stored_event(schema_v1.id(), payload);
    assert_eq!(
        resolved
            .materialize_event(&plan_reference(&bundles[0]), &event)
            .expect_err("inserted null exceeds the canonical ceiling")
            .kind(),
        ProjectionEventMaterializationErrorKind::HardLimit
    );
}
