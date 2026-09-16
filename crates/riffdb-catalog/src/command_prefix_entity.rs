//! Complete entity-image validation for exact command-prefix reconstruction.

use crate::command_prefix::corrupt;
use crate::{CatalogError, ValidatedContractBundle};
use riffdb_storage_api::{
    AuthoritativeNamespaceV1 as N, StoredCommandCapsuleV2, StoredEntityRecordV1,
};

pub(super) fn validate_images(
    bundle: &ValidatedContractBundle,
    command: &StoredCommandCapsuleV2,
) -> Result<(), CatalogError> {
    let Some(prefix) = command.prefix_evidence() else {
        return Ok(());
    };
    let plan = command.base().commit().plan();
    if bundle
        .bundle()
        .command(plan.command_id())
        .is_none_or(|p| p.plan_hash() != plan.command_plan_hash())
    {
        return Err(corrupt());
    }
    for row in prefix
        .mutations()
        .iter()
        .filter(|row| row.namespace() == N::Entities)
    {
        let key = riffdb_types::EntityKey::from_bytes(row.key().to_vec()).map_err(|_| corrupt())?;
        let schema = bundle.bundle().schema();
        let entity = schema.entity(key.entity_type_id()).ok_or_else(corrupt)?;
        let partition = crate::history::derive_historical_partition(schema, entity, &key)?;
        // ADR-0170 permits different aggregate namespaces on one route.
        // Decode both with their owning schema; compare typed route components.
        let route = |partition: &riffdb_types::PartitionKey| {
            schema
                .aggregate(partition.aggregate_type_id())
                .ok_or_else(corrupt)?
                .keys()
                .partition_schema()
                .decode_partition(partition)
                .map_err(|_| corrupt())
        };
        if route(&partition)? != route(command.base().outcome().partition_key())?
            || !command
                .entity_transitions()
                .iter()
                .any(|t| t.target().key() == &key)
        {
            return Err(corrupt());
        }
        if let Some(bytes) = row.value() {
            let decoded =
                riffdb_storage_api::decode_entity_record_v1(bytes).map_err(|_| corrupt())?;
            let record = decoded.value();
            if record.target().key() != &key || !record.schema_binding().matches_plan(plan) {
                return Err(corrupt());
            }
            validate_record(bundle, record)?;
        }
    }
    Ok(())
}

pub(super) fn validate_record(
    bundle: &ValidatedContractBundle,
    record: &StoredEntityRecordV1,
) -> Result<(), CatalogError> {
    let schema = bundle.bundle().schema();
    let entity = schema
        .entity(record.target().entity_type_id())
        .ok_or_else(corrupt)?;
    validate_fields(schema, entity.record(), record.fields())?;
    crate::materialization::validate_entity_record_key(entity, record).map_err(|_| corrupt())
}

fn validate_fields(
    schema: &riffdb_contract_ir::SchemaIr,
    declared: &riffdb_contract_ir::RecordSchema,
    fields: &riffdb_types::CanonicalRecord,
) -> Result<(), CatalogError> {
    // Newly written images have every field, including explicit optional nulls.
    // Historical null materialization belongs to the separately proven prior.
    if declared.fields().len() != fields.fields().len() {
        return Err(corrupt());
    }
    for (declared, (id, value)) in declared.fields().iter().zip(fields.fields()) {
        if declared.id() != *id {
            return Err(corrupt());
        }
        validate_value(schema, declared.value_type(), value)?;
    }
    Ok(())
}

fn validate_value(
    schema: &riffdb_contract_ir::SchemaIr,
    kind: &riffdb_contract_ir::ValueType,
    value: &riffdb_types::CanonicalValue,
) -> Result<(), CatalogError> {
    use riffdb_contract_ir::RecordTypeRef;
    use riffdb_types::CanonicalValue;
    crate::materialization::validate_static_value(schema, kind, value).map_err(|_| corrupt())?;
    if matches!(value, CanonicalValue::Null) {
        return Ok(());
    }
    if let Some(inner) = kind.optional_inner() {
        return validate_value(schema, inner, value);
    }
    if let (Some((inner, _)), CanonicalValue::List(values)) = (kind.list_parts(), value) {
        for value in values.values() {
            validate_value(schema, inner, value)?;
        }
    }
    // The shared scalar validator checks enum membership and bounded primitive
    // containers. Record references additionally need their complete field shape.
    // Canonical stored-value decoding bounds both depth and aggregate size.
    if let (Some(owner), CanonicalValue::Record(fields)) = (kind.record_ref(), value) {
        let declared = match owner {
            RecordTypeRef::Entity(id) => schema.entity(*id).map(|e| e.record()),
            RecordTypeRef::Event(id) => schema.event(*id).map(|e| e.payload()),
            _ => None,
        }
        .ok_or_else(corrupt)?;
        validate_fields(schema, declared, fields)?;
    }
    Ok(())
}
