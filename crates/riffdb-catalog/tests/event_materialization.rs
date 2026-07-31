//! Symbolic historical event materialization and redaction semantics.

use riffdb_catalog::{
    ActiveCatalogSnapshot, EventMaterializationErrorKind, EventReplayErrorKind,
    ValidatedContractBundle,
};
use riffdb_contract_compiler::{compile_contract_source, compile_contract_successor};
use riffdb_contract_ir::{ContractBundle, EventSchema};
use riffdb_storage_api::{
    ActiveCatalogPointerV1, AuthoritativePointReader, CatalogRepository, EncodedContentCharge,
    EncodedPageItem, EntityTarget, EventRoutePageLimit, EventRouteScanRequestV1, EventRouteScanV1,
    EventRouteUpperFenceV1, ExecutablePlanRef, IdempotencyIdentity, PartitionEventRouteReader,
    StorageError, StoredCommitRecordV1, StoredContractBundleV1, StoredDurableEventV1,
    StoredEntityRecordV1, StoredEventRouteV1, StoredOutcomeV1, StoredProvenanceRecordV1,
    derive_event_hash_v1,
};
use riffdb_types::{
    CanonicalRecord, CanonicalValue, CommitSequence, ContractLineage, EventId, FieldId,
    PartitionKeyHash, ProvenanceId, hash_partition_key,
};
use std::num::NonZeroU16;

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

fn route_for(
    bundle: &ValidatedContractBundle,
    event: &StoredDurableEventV1,
) -> (PartitionKeyHash, StoredEventRouteV1) {
    let schema = event_schema(bundle);
    let partition = schema.partition().expect("streamable fixture event");
    let values = partition
        .fields()
        .iter()
        .map(|field_id| {
            event
                .payload()
                .fields()
                .iter()
                .find(|(candidate, _)| candidate == field_id)
                .map(|(_, value)| value.clone())
                .expect("partition payload field")
        })
        .collect::<Vec<_>>();
    let key = partition
        .key_schema()
        .encode_partition(&values)
        .expect("partition key");
    (
        hash_partition_key(key.as_bytes()),
        StoredEventRouteV1::new(event.event_id(), event.event_type_id(), event.event_hash()),
    )
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
    let replay = active
        .resolve_event_replay(
            "Changed",
            [("id", CanonicalValue::Uuid([0x11; 16]))],
            ["id"],
        )
        .expect("symbolic event replay");
    assert_eq!(replay.event_name(), "Changed");
    assert_eq!(format!("{replay:?}"), "ResolvedEventReplay([CHECKED])");
    assert_eq!(materializer.event_name(), "Changed");
    assert_eq!(
        materializer.selected_field_names().collect::<Vec<_>>(),
        ["note", "id"]
    );

    let ancestor = event(&bundles[0], vec![("id", CanonicalValue::Uuid([0x11; 16]))]);
    let (partition_hash, route) = route_for(&bundles[0], &ancestor);
    let view = materializer
        .materialize_routed_event(
            &plan_reference(&bundles[0]),
            partition_hash,
            route,
            &ancestor,
        )
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
    let (partition_hash, route) = route_for(&bundles[2], &descendant);
    let view = materializer
        .materialize_routed_event(
            &plan_reference(&bundles[2]),
            partition_hash,
            route,
            &descendant,
        )
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
    let valid = event(&bundles[0], vec![("id", CanonicalValue::Uuid([0x44; 16]))]);
    let (partition_hash, route) = route_for(&bundles[0], &valid);
    assert_eq!(
        materializer
            .materialize_routed_event(
                &plan_reference(&bundles[0]),
                PartitionKeyHash::from_bytes([0xff; 32]),
                route,
                &valid,
            )
            .expect_err("route partition does not match the symbolic payload")
            .kind(),
        EventMaterializationErrorKind::Integrity
    );
    let wrong_route = StoredEventRouteV1::new(
        EventId::new(CommitSequence::first(), 1),
        valid.event_type_id(),
        valid.event_hash(),
    );
    assert_eq!(
        materializer
            .materialize_routed_event(
                &plan_reference(&bundles[0]),
                partition_hash,
                wrong_route,
                &valid,
            )
            .expect_err("route identity does not match the authoritative event")
            .kind(),
        EventMaterializationErrorKind::Integrity
    );
    let malformed = event(&bundles[1], vec![("id", CanonicalValue::Uuid([0x33; 16]))]);
    let (partition_hash, route) = route_for(&bundles[1], &malformed);
    assert_eq!(
        materializer
            .materialize_routed_event(
                &plan_reference(&bundles[1]),
                partition_hash,
                route,
                &malformed,
            )
            .expect_err("schema-incomplete event")
            .kind(),
        EventMaterializationErrorKind::Integrity
    );
}

#[test]
fn an_unpartitioned_event_is_not_application_streamable() {
    let source = EVENT_V1.replace("    partition_by (id)\n", "");
    let bundle = ValidatedContractBundle::from_compiler_bundle(
        compile_contract_source(&source).expect("legacy unpartitioned event compiles"),
    )
    .expect("compiler bundle validates");
    let stored = bundle.to_stored().expect("stored bundle");
    let repository = ReadRepository {
        active: ActiveCatalogPointerV1::from_bundle(&stored),
        bundles: vec![stored],
    };
    let active = ActiveCatalogSnapshot::read(&repository)
        .expect("catalog read")
        .expect("active catalog");

    assert_eq!(
        active
            .resolve_event_materializer("Changed", ["id"])
            .expect_err("unpartitioned events remain internal")
            .kind(),
        EventMaterializationErrorKind::NotStreamable
    );
    assert_eq!(
        active
            .resolve_event_replay(
                "Changed",
                [("id", CanonicalValue::Uuid([0x11; 16]))],
                ["id"],
            )
            .expect_err("unpartitioned replay remains internal")
            .kind(),
        EventReplayErrorKind::Materialization(EventMaterializationErrorKind::NotStreamable)
    );
}

struct EventWithoutCommitReader {
    event: StoredDurableEventV1,
}

impl AuthoritativePointReader for EventWithoutCommitReader {
    fn read_entity(
        &self,
        _target: &EntityTarget,
    ) -> Result<Option<StoredEntityRecordV1>, StorageError> {
        Ok(None)
    }

    fn read_stored_outcome(
        &self,
        _identity: &IdempotencyIdentity,
    ) -> Result<Option<StoredOutcomeV1>, StorageError> {
        Ok(None)
    }

    fn read_commit(
        &self,
        _sequence: CommitSequence,
    ) -> Result<Option<StoredCommitRecordV1>, StorageError> {
        Ok(None)
    }

    fn read_provenance(
        &self,
        _provenance_id: ProvenanceId,
    ) -> Result<Option<StoredProvenanceRecordV1>, StorageError> {
        Ok(None)
    }

    fn read_durable_event(
        &self,
        event_id: EventId,
    ) -> Result<Option<StoredDurableEventV1>, StorageError> {
        Ok((event_id == self.event.event_id()).then(|| self.event.clone()))
    }
}

impl PartitionEventRouteReader for EventWithoutCommitReader {
    fn scan_partition_event_routes(
        &self,
        request: EventRouteScanRequestV1,
    ) -> Result<EventRouteScanV1, StorageError> {
        let route = StoredEventRouteV1::new(
            self.event.event_id(),
            self.event.event_type_id(),
            self.event.event_hash(),
        );
        Ok(EventRouteScanV1::exact_end(
            request,
            EventRouteUpperFenceV1::Inclusive(self.event.event_id()),
            vec![EncodedPageItem::new(
                route,
                EncodedContentCharge::new(64).expect("bounded route charge"),
            )],
        )
        .expect("canonical one-route page"))
    }
}

#[test]
fn replay_requires_the_enclosing_commit_before_any_event_escapes() {
    let (bundles, repository) = compile_chain();
    let active = ActiveCatalogSnapshot::read(&repository)
        .expect("catalog read")
        .expect("active catalog");
    let replay = active
        .resolve_event_replay(
            "Changed",
            [("id", CanonicalValue::Uuid([0x55; 16]))],
            ["id"],
        )
        .expect("resolved replay");
    let event = event(&bundles[0], vec![("id", CanonicalValue::Uuid([0x55; 16]))]);
    let reader = EventWithoutCommitReader { event };
    let limit =
        EventRoutePageLimit::new(NonZeroU16::new(1).expect("nonzero")).expect("bounded limit");

    assert_eq!(
        replay
            .replay_page(
                &reader,
                riffdb_catalog::EventReplayPosition::Initial { after: None },
                limit,
                1,
            )
            .expect_err("orphan route/event cannot escape")
            .kind(),
        EventReplayErrorKind::Integrity
    );
}
