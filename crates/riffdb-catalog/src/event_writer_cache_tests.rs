use super::*;
use riffdb_storage_api::{
    ActiveCatalogPointerV1, CatalogRepository, StorageError, StoredContractBundleV1,
    derive_event_hash_v1,
};
use riffdb_types::{CanonicalRecord, CommitSequence, ContractLineage};

struct Repository(StoredContractBundleV1);
impl CatalogRepository for Repository {
    fn read_active_catalog(&self) -> Result<Option<ActiveCatalogPointerV1>, StorageError> {
        Ok(Some(ActiveCatalogPointerV1::from_bundle(&self.0)))
    }
    fn read_contract_bundle(
        &self,
        _: &ContractLineage,
        _: ContractVersion,
    ) -> Result<Option<StoredContractBundleV1>, StorageError> {
        Ok(Some(self.0.clone()))
    }
}

#[test]
fn cached_writer_plan_keeps_per_event_route_and_payload_validation() {
    let bundle = ValidatedContractBundle::from_compiler_bundle(
        riffdb_contract_compiler::compile_contract_source(include_str!(
            "../tests/fixtures/projection_event_v1.riff"
        ))
        .unwrap(),
    )
    .unwrap();
    let active = ActiveCatalogSnapshot::read(&Repository(bundle.to_stored().unwrap()))
        .unwrap()
        .unwrap();
    let materializer = active
        .resolve_event_materializer("Changed", ["id"])
        .unwrap();
    let command = bundle.bundle().commands().first().unwrap();
    let writer = ExecutablePlanRef::new(
        bundle.lineage().clone(),
        bundle.contract_version(),
        bundle.bundle_hash(),
        command.command_id(),
        command.plan_hash(),
    );
    let schema = bundle.bundle().schema().events().first().unwrap();
    let field = schema.payload().fields().first().unwrap().id();
    let value = CanonicalValue::Uuid([7; 16]);
    let partition = schema
        .partition()
        .unwrap()
        .key_schema()
        .encode_partition(std::slice::from_ref(&value))
        .unwrap();
    let partition_hash = hash_partition_key(partition.as_bytes());
    let event = |ordinal, present| {
        let payload = CanonicalRecord::new(if present {
            vec![(field, value.clone())]
        } else {
            vec![]
        })
        .unwrap();
        let id = EventId::new(CommitSequence::first(), ordinal);
        let hash = derive_event_hash_v1(id, schema.id(), &payload).unwrap();
        StoredDurableEventV1::new(id, schema.id(), payload, hash).unwrap()
    };
    let route = |event: &StoredDurableEventV1| {
        StoredEventRouteV1::new(event.event_id(), event.event_type_id(), event.event_hash())
    };
    let first = event(0, true);
    let mut cache = materializer.page_materializer();
    cache
        .materialize(&writer, partition_hash, route(&first), &first)
        .unwrap();
    // Keep the old Arc alive: a newly resolved/cloned plan cannot reuse its address.
    let first_plan = cache.writer_plan.as_ref().unwrap().clone();
    for ordinal in 1..20 {
        let sibling = event(ordinal, true);
        let view = cache
            .materialize(&writer, partition_hash, route(&sibling), &sibling)
            .unwrap();
        assert_eq!(view.event_id(), sibling.event_id());
        assert!(std::ptr::eq(
            first_plan.plan(),
            cache.writer_plan.as_ref().unwrap().plan()
        ));
    }
    let malformed = event(20, false);
    assert_eq!(
        cache
            .materialize(&writer, partition_hash, route(&malformed), &malformed)
            .unwrap_err()
            .kind(),
        EventMaterializationErrorKind::Integrity
    );
    assert_eq!(
        cache
            .materialize(&writer, partition_hash, route(&first), &event(1, true))
            .unwrap_err()
            .kind(),
        EventMaterializationErrorKind::Integrity
    );
    let substituted = ExecutablePlanRef::new(
        bundle.lineage().clone(),
        bundle.contract_version(),
        bundle.bundle_hash(),
        command.command_id(),
        PlanHash::from_bytes([0; 32]),
    );
    assert_eq!(
        cache
            .materialize(&substituted, partition_hash, route(&first), &first)
            .unwrap_err()
            .kind(),
        EventMaterializationErrorKind::Integrity
    );
    assert!(cache.writer_plan.is_none());
    cache
        .materialize(&writer, partition_hash, route(&first), &first)
        .unwrap();
}
