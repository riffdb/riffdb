//! Intrinsic vector row identities and command-local reciprocal inventory.
//! Catalog derivation and predecessor counter arithmetic are separate proofs.

use std::collections::{BTreeMap, BTreeSet};

use riffdb_storage_api::{
    AuthoritativeNamespaceV1 as N, DurableCodecError, DurableCodecErrorKind, StoredCommandCapsuleV2,
};
use riffdb_types::CanonicalValue;

use super::rows::required;
use crate::keys;

fn corrupt() -> DurableCodecError {
    DurableCodecError::new(DurableCodecErrorKind::CorruptData)
}

pub(super) fn validate_post_images(
    command: &StoredCommandCapsuleV2,
) -> Result<(), DurableCodecError> {
    let Some(prefix) = command.prefix_evidence() else {
        return Ok(());
    };
    let rows = prefix.mutations();
    let commit = command.base().commit();
    let mut primary_count = 0;
    for row in rows.iter().filter(|m| m.namespace() == N::VectorEvidence) {
        primary_count += 1;
        let (key, field) = keys::decode_vector_evidence_key(row.key()).map_err(|_| corrupt())?;
        let entity = required(rows, N::Entities, key.as_bytes())?;
        match (row.value(), entity.value()) {
            (Some(bytes), Some(entity_bytes)) => {
                let decoded = riffdb_storage_api::decode_vector_evidence_v1(bytes)?;
                let evidence = decoded.value();
                let entity = riffdb_storage_api::decode_entity_record_v1(entity_bytes)?;
                let entity = entity.value();
                if evidence.target() != entity.target()
                    || evidence.target().key() != &key
                    || evidence.vector_field() != field
                    || evidence.entity_version() != entity.entity_version()
                    || evidence.evidence_sequence() != command.commit_sequence()
                    || evidence.provenance_id() != commit.provenance_id()
                    || evidence.plan() != commit.plan()
                    || evidence.schema_binding() != entity.schema_binding()
                    || evidence.partition_key() != command.base().outcome().partition_key()
                {
                    return Err(corrupt());
                }
                let value = entity
                    .fields()
                    .fields()
                    .binary_search_by_key(&field, |(id, _)| *id)
                    .ok()
                    .map(|offset| &entity.fields().fields()[offset].1);
                if !matches!(
                    (value, evidence.embedding_write()),
                    (Some(CanonicalValue::Vector(_)), Some(_)) | (Some(CanonicalValue::Null), None)
                ) {
                    return Err(corrupt());
                }
            }
            (None, None) => {}
            _ => return Err(corrupt()),
        }
    }

    // Borrow matched primary keys, and keep only canonical observation keys.
    // Memory is bounded by the already checked prefix item/byte ceilings.
    let mut matched = BTreeSet::new();
    let mut observations = BTreeSet::new();
    let mut health_fields = BTreeMap::<_, BTreeSet<_>>::new();
    for row in rows
        .iter()
        .filter(|m| m.namespace() == N::VectorEvidenceIndex)
    {
        let (target, entity_key) =
            keys::decode_vector_evidence_index_key(row.key()).map_err(|_| corrupt())?;
        let primary_key = keys::encode_vector_evidence_key(&entity_key, target.vector_field())
            .map_err(|_| corrupt())?;
        let primary = required(rows, N::VectorEvidence, &primary_key)?;
        if !matched.insert(primary.key())
            || row.expected_hash().is_some() != primary.expected_hash().is_some()
        {
            return Err(corrupt());
        }
        match (row.value(), primary.value()) {
            (Some(bytes), Some(primary_bytes)) => {
                let index = riffdb_storage_api::decode_vector_evidence_index_v1(bytes)?;
                let evidence = riffdb_storage_api::decode_vector_evidence_v1(primary_bytes)?;
                if index.value().target() != &target
                    || index.value().entity_key() != &entity_key
                    || !index.value().matches_evidence(evidence.value())
                {
                    return Err(corrupt());
                }
            }
            (None, None) => {}
            _ => return Err(corrupt()),
        }
        let key = keys::encode_vector_observation_key(&target).map_err(|_| corrupt())?;
        required(rows, N::VectorObservations, &key)?;
        observations.insert(key);
        let key =
            keys::encode_vector_health_observation_key(target.lineage()).map_err(|_| corrupt())?;
        required(rows, N::VectorObservations, &key)?;
        health_fields
            .entry(key.clone())
            .or_default()
            .insert((target.entity_type(), target.vector_field()));
        observations.insert(key);
    }
    if matched.len() != primary_count {
        return Err(corrupt());
    }
    for row in rows
        .iter()
        .filter(|m| m.namespace() == N::VectorObservations)
    {
        if !observations.contains(row.key()) {
            return Err(corrupt());
        }
        if let Ok(lineage) = keys::decode_vector_health_observation_key(row.key()) {
            let health = riffdb_storage_api::decode_vector_health_observation_v1(
                row.value().ok_or_else(corrupt)?,
            )?;
            let mut required_fields = health_fields.remove(row.key()).ok_or_else(corrupt)?;
            for field in health.value().fields() {
                required_fields.remove(&(field.entity_type(), field.vector_field()));
            }
            if !required_fields.is_empty()
                || health.value().lineage() != &lineage
                || health.value().revision() != command.commit_sequence()
            {
                return Err(corrupt());
            }
        } else {
            let target = keys::decode_vector_observation_key(row.key()).map_err(|_| corrupt())?;
            if let Some(bytes) = row.value() {
                let observation = riffdb_storage_api::decode_vector_observation_v1(bytes)?;
                if observation.value().target() != &target
                    || observation.value().total_entities() == 0
                    || observation.value().revision() != command.commit_sequence()
                {
                    return Err(corrupt());
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "vector_tests.rs"]
mod tests;
