//! Read-only counter proofs over catalog-validated evidence transitions.
//! Keep one partition's model map at a time; unknown retained predecessors
//! cannot establish arithmetic and remain for the complete history proof.

use std::collections::BTreeMap;

use riffdb_storage_api::{
    AuthoritativeMutationV3 as Mutation, AuthoritativeNamespaceV1 as N, StorageError,
    StorageErrorKind, StoredCommandCapsuleV2, VectorEvidenceClassificationTransitionV1 as Change,
    VectorHealthObservationV1 as Health, VectorObservationCountsV1 as Counts,
    VectorObservationTargetV1 as Target,
};

use super::predecessor::{PhysicalPrior, PriorImages, with_prior};
use super::rows::required;
use crate::error::{codec_error, storage_error};
use crate::keys;

/// Borrow only keys, actual predecessor presence, and compiler-owned thresholds.
/// Complete plans and model metadata are never retained across entity checks.
pub(super) type CheckedVectors<'a> = BTreeMap<&'a [u8], (bool, u64)>;
type Groups<'a> = BTreeMap<&'a [u8], Vec<&'a Mutation>>;

fn corrupt() -> StorageError {
    storage_error(StorageErrorKind::CorruptData)
}

pub(super) fn validate(
    command: &StoredCommandCapsuleV2,
    checked: &CheckedVectors<'_>,
    physical: PhysicalPrior<'_>,
    prior: &PriorImages<'_>,
) -> Result<(), StorageError> {
    let prefix = command.prefix_evidence().ok_or_else(corrupt)?;
    let rows = prefix.mutations();
    let mut groups = Groups::new();
    for row in rows
        .iter()
        .filter(|row| row.namespace() == N::VectorEvidenceIndex)
    {
        let (target, entity) =
            keys::decode_vector_evidence_index_key(row.key()).map_err(|_| corrupt())?;
        let key = keys::encode_vector_evidence_key(&entity, target.vector_field())
            .map_err(|_| corrupt())?;
        let primary = required(rows, N::VectorEvidence, &key).map_err(codec_error)?;
        let key = keys::encode_vector_observation_key(&target).map_err(|_| corrupt())?;
        let observation = required(rows, N::VectorObservations, &key).map_err(codec_error)?;
        groups.entry(observation.key()).or_default().push(primary);
    }
    if groups.is_empty() {
        return Ok(());
    }
    let mut complete = true;
    for (key, primary) in &mut groups {
        // Source maintenance orders transitions by entity target and field.
        primary.sort_by_key(|row| row.key());
        let target = keys::decode_vector_observation_key(key).map_err(|_| corrupt())?;
        let row = required(rows, N::VectorObservations, key).map_err(codec_error)?;
        if primary.iter().any(|row| !checked.contains_key(row.key())) {
            complete = false;
            continue;
        }
        let mut known = false;
        partition_prior(command, physical, prior, row, &target, |before| {
            let mut expected =
                before.unwrap_or_else(|| Counts::empty(target.clone(), command.commit_sequence()));
            for primary in primary.iter() {
                let (present, _) = checked.get(primary.key()).ok_or_else(corrupt)?;
                let change = classification(physical, prior, primary, *present)?;
                expected
                    .apply(&change, command.commit_sequence())
                    .map_err(|_| corrupt())?;
            }
            let supplied = decode_counts(row.value())?;
            let expected = (expected.total_entities() != 0).then_some(expected);
            if supplied != expected {
                return Err(corrupt());
            }
            known = true;
            Ok(())
        })?;
        complete &= known;
    }
    if !complete {
        return Ok(());
    }
    validate_health(command, checked, &groups, physical, prior)
}

fn decode_counts(bytes: Option<&[u8]>) -> Result<Option<Counts>, StorageError> {
    bytes
        .map(riffdb_storage_api::decode_vector_observation_v1)
        .transpose()
        .map_err(codec_error)
        .map(|value| value.map(|value| value.into_parts().0))
}

fn partition_prior(
    command: &StoredCommandCapsuleV2,
    physical: PhysicalPrior<'_>,
    prior: &PriorImages<'_>,
    row: &Mutation,
    target: &Target,
    check: impl FnOnce(Option<Counts>) -> Result<(), StorageError>,
) -> Result<(), StorageError> {
    with_prior(physical, prior, N::VectorObservations, row.key(), |bytes| {
        if !row.matches_prior(bytes) {
            return Err(corrupt());
        }
        let counts = decode_counts(bytes)?;
        if counts.as_ref().is_some_and(|counts| {
            counts.target() != target
                || counts.total_entities() == 0
                || counts.revision() >= command.commit_sequence()
        }) {
            return Err(corrupt());
        }
        check(counts)
    })
}

fn classification(
    physical: PhysicalPrior<'_>,
    prior: &PriorImages<'_>,
    row: &Mutation,
    present: bool,
) -> Result<Change, StorageError> {
    let successor = row
        .value()
        .map(riffdb_storage_api::decode_vector_evidence_v1)
        .transpose()
        .map_err(codec_error)?;
    let mut result = None;
    with_prior(physical, prior, N::VectorEvidence, row.key(), |bytes| {
        if bytes.is_some() != present || !row.matches_prior(bytes) {
            return Err(corrupt());
        }
        let before = bytes
            .map(riffdb_storage_api::decode_vector_evidence_v1)
            .transpose()
            .map_err(codec_error)?;
        result = Some(
            Change::from_evidence(
                before.as_ref().map(|v| v.value()),
                successor.as_ref().map(|v| v.value()),
            )
            .map_err(|_| corrupt())?,
        );
        Ok(())
    })?;
    match result {
        Some(result) => Ok(result),
        // Catalog checks explicitly proved absence for a retained create.
        None if !present => Change::from_evidence(None, successor.as_ref().map(|v| v.value()))
            .map_err(|_| corrupt()),
        None => Err(corrupt()),
    }
}

fn validate_health(
    command: &StoredCommandCapsuleV2,
    checked: &CheckedVectors<'_>,
    groups: &Groups<'_>,
    physical: PhysicalPrior<'_>,
    prior: &PriorImages<'_>,
) -> Result<(), StorageError> {
    let rows = command.prefix_evidence().ok_or_else(corrupt)?.mutations();
    let lineage = command.base().commit().plan().contract_lineage();
    let key = keys::encode_vector_health_observation_key(lineage).map_err(|_| corrupt())?;
    let row = required(rows, N::VectorObservations, &key).map_err(codec_error)?;
    let mut known = None;
    with_prior(physical, prior, N::VectorObservations, &key, |bytes| {
        if !row.matches_prior(bytes) {
            return Err(corrupt());
        }
        let health = bytes
            .map(riffdb_storage_api::decode_vector_health_observation_v1)
            .transpose()
            .map_err(codec_error)?
            .map(|v| v.into_parts().0);
        if health.as_ref().is_some_and(|health| {
            health.lineage() != lineage || health.revision() >= command.commit_sequence()
        }) {
            return Err(corrupt());
        }
        known = Some(health);
        Ok(())
    })?;
    let Some(health) = known else {
        return Ok(());
    };
    // Release the health table guard before reading partition rows in the same table.
    let mut health =
        health.unwrap_or_else(|| Health::empty(lineage.clone(), command.commit_sequence()));
    // Remove all affected contributions before adding successors. This
    // read-only fold avoids transient overflow from a different partition
    // order, while retaining the shared checked threshold/count arithmetic.
    for add in [false, true] {
        for (key, primary) in groups {
            let target = keys::decode_vector_observation_key(key).map_err(|_| corrupt())?;
            let threshold = checked
                .get(primary.first().ok_or_else(corrupt)?.key())
                .ok_or_else(corrupt)?
                .1;
            if primary
                .iter()
                .any(|row| checked.get(row.key()).is_none_or(|(_, t)| *t != threshold))
            {
                return Err(corrupt());
            }
            let row = required(rows, N::VectorObservations, key).map_err(codec_error)?;
            let mut apply = |before: Option<&Counts>, next: Option<&Counts>| {
                health
                    .apply_partition(
                        target.entity_type(),
                        target.vector_field(),
                        threshold,
                        before,
                        next,
                        command.commit_sequence(),
                    )
                    .map_err(|_| corrupt())
            };
            if add {
                apply(None, decode_counts(row.value())?.as_ref())?;
            } else {
                partition_prior(command, physical, prior, row, &target, |counts| {
                    apply(counts.as_ref(), None)
                })?;
            }
        }
    }
    let supplied =
        riffdb_storage_api::decode_vector_health_observation_v1(row.value().ok_or_else(corrupt)?)
            .map_err(codec_error)?;
    if supplied.value() != &health {
        return Err(corrupt());
    }
    Ok(())
}
