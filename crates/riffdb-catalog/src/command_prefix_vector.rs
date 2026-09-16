//! Catalog-derived vector source/embedding transitions for restoration evidence.

use crate::command_prefix::{checked_prior, corrupt};
use crate::{CatalogError, ResolvedExecutablePlan, ValidatedContractBundle};
use riffdb_storage_api::{
    AuthoritativeNamespaceV1 as N, CommittedEntityTransitionV1, DurableKeySchemaBindingV1,
    EntityTarget, ExecutablePlanRef, StoredCommandCapsuleV2, StoredEntityRecordV1,
    StoredVectorEvidenceV1, VectorEvidenceMutationV1, VectorEvidenceTransitionPlanV1,
};
use riffdb_types::{CanonicalValue, CommitSequence, FieldId, PartitionKey, ProvenanceId};
use std::borrow::Cow;

fn require_bundle(
    bundle: &ValidatedContractBundle,
    plan: &ExecutablePlanRef,
) -> Result<(), CatalogError> {
    if bundle.lineage() != plan.contract_lineage()
        || bundle.contract_version() != plan.contract_version()
        || bundle.bundle_hash() != plan.contract_bundle_hash()
    {
        return Err(corrupt());
    }
    Ok(())
}

/// Enumerates production fields for an exact command-owned entity. Physical keys
/// stay with the storage owner; undeclared supplied field identities must refuse.
pub fn command_prefix_vector_fields_v1<'a>(
    bundle: &'a ValidatedContractBundle,
    command: &StoredCommandCapsuleV2,
    target: &EntityTarget,
) -> Result<impl Iterator<Item = FieldId> + 'a, CatalogError> {
    require_bundle(bundle, command.base().commit().plan())?;
    if !command
        .entity_transitions()
        .iter()
        .any(|t| t.target() == target)
    {
        return Err(corrupt());
    }
    let entity = target.entity_type_id();
    if bundle.bundle().schema().entity(entity).is_none() {
        return Err(corrupt());
    }
    Ok(bundle
        .bundle()
        .schema()
        .vector_production_specs()
        .iter()
        .filter(move |spec| spec.entity() == entity)
        .map(|spec| spec.field()))
}

pub(super) fn validate_supplied_images(
    bundle: &ValidatedContractBundle,
    command: &StoredCommandCapsuleV2,
) -> Result<(), CatalogError> {
    let Some(prefix) = command.prefix_evidence() else {
        return Ok(());
    };
    for row in prefix
        .mutations()
        .iter()
        .filter(|row| row.namespace() == N::VectorEvidence)
    {
        let Some(bytes) = row.value() else {
            continue;
        };
        let decoded =
            riffdb_storage_api::decode_vector_evidence_v1(bytes).map_err(|_| corrupt())?;
        let evidence = decoded.value();
        if !command_prefix_vector_fields_v1(bundle, command, evidence.target())?
            .any(|field| field == evidence.vector_field())
        {
            return Err(corrupt());
        }
        let production = bundle
            .bundle()
            .schema()
            .vector_production_spec(evidence.target().entity_type_id(), evidence.vector_field())
            .ok_or_else(corrupt)?;
        if evidence.embedding_write().is_some_and(|write| {
            write.sequence() == command.commit_sequence()
                && write.metadata() != production.metadata()
        }) {
            return Err(corrupt());
        }
    }
    Ok(())
}

/// Prepares bounded, read-only checks for every production vector field of one
/// entity with a known predecessor. Unknown history is not known absence.
/// Raw writer identity is checked before eligible historical null expansion.
pub fn command_prefix_entity_vectors_v1<'a>(
    bundle: &'a ValidatedContractBundle,
    command: &'a StoredCommandCapsuleV2,
    transition: &'a CommittedEntityTransitionV1,
    prior: Option<&'a StoredEntityRecordV1>,
    resolved: Option<&ResolvedExecutablePlan>,
) -> Result<CommandPrefixVectorChecksV1<'a>, CatalogError> {
    require_bundle(bundle, command.base().commit().plan())?;
    if !command.entity_transitions().contains(transition) {
        return Err(corrupt());
    }
    let prior = checked_prior(command, transition, prior, resolved)?;
    let prefix = command.prefix_evidence().ok_or_else(corrupt)?;
    let offset = prefix
        .mutations()
        .binary_search_by(|row| {
            (row.namespace(), row.key()).cmp(&(N::Entities, transition.target().key().as_bytes()))
        })
        .map_err(|_| corrupt())?;
    let next = prefix.mutations()[offset]
        .value()
        .map(riffdb_storage_api::decode_entity_record_v1)
        .transpose()
        .map_err(|_| corrupt())?
        .map(|value| value.into_parts().0);
    if next.as_ref().is_some_and(|row| {
        row.target() != transition.target()
            || !row
                .schema_binding()
                .matches_plan(command.base().commit().plan())
    }) {
        return Err(corrupt());
    }
    Ok(CommandPrefixVectorChecksV1 {
        bundle,
        plan: command.base().commit().plan(),
        target: transition.target(),
        sequence: command.commit_sequence(),
        provenance: command.base().commit().provenance_id(),
        partition: command.base().outcome().partition_key(),
        prior,
        next,
    })
}

/// Catalog context only; no readiness, reconstruction or publication authority.
/// Validate every declared field against its actual evidence predecessor before
/// accepting a complete transition inventory.
pub struct CommandPrefixVectorChecksV1<'a> {
    bundle: &'a ValidatedContractBundle,
    plan: &'a ExecutablePlanRef,
    target: &'a EntityTarget,
    sequence: CommitSequence,
    provenance: ProvenanceId,
    partition: &'a PartitionKey,
    prior: Option<Cow<'a, StoredEntityRecordV1>>,
    next: Option<StoredEntityRecordV1>,
}

impl CommandPrefixVectorChecksV1<'_> {
    /// Proves mutation presence from known entity values even if an older
    /// evidence row is unavailable in a retained segment. This is not a proof
    /// of that unknown row's contents or counter predecessor.
    pub fn validate_inventory(
        &self,
        field: FieldId,
        supplied: Option<&VectorEvidenceMutationV1>,
    ) -> Result<(), CatalogError> {
        let schema = self.bundle.bundle().schema();
        let production = schema
            .vector_production_spec(self.target.entity_type_id(), field)
            .ok_or_else(corrupt)?;
        let spec = schema
            .vector_field_spec(self.target.entity_type_id(), field)
            .ok_or_else(corrupt)?;
        for row in [self.prior.as_deref(), self.next.as_ref()]
            .into_iter()
            .flatten()
        {
            for field in spec.source_fields() {
                self.validated_field(row, *field)?;
            }
        }
        let old = self.vector_value(self.prior.as_deref(), field)?;
        let next_value = self.vector_value(self.next.as_ref(), field)?;
        if let Some(next) = &self.next {
            let rewrite = match supplied {
                Some(VectorEvidenceMutationV1::Put(evidence)) => evidence
                    .embedding_write()
                    .filter(|write| write.sequence() == self.sequence),
                _ => None,
            };
            let source_changed = self.prior.is_none()
                || spec.source_fields().iter().any(|field| {
                    self.prior
                        .as_deref()
                        .and_then(|row| field_value(row, *field))
                        != field_value(next, *field)
                });
            if rewrite.is_some_and(|write| write.metadata() != production.metadata())
                || (old != next_value && rewrite.is_none())
                || (rewrite.is_some() && next_value.is_none())
                || ((source_changed || rewrite.is_some())
                    != matches!(supplied, Some(VectorEvidenceMutationV1::Put(_))))
                || matches!(supplied, Some(VectorEvidenceMutationV1::Delete { .. }))
            {
                return Err(corrupt());
            }
        } else if matches!(supplied, Some(VectorEvidenceMutationV1::Put(_)))
            || ((old.is_some()
                || self
                    .prior
                    .as_ref()
                    .is_some_and(|row| row.schema_binding().matches_plan(self.plan)))
                && supplied.is_none())
        {
            return Err(corrupt());
        }
        Ok(())
    }

    /// Derives the exact transition or proves that no evidence write is needed.
    /// A supplied embedding rewrite may retain identical vector components; its
    /// metadata must still match the compiler-owned production declaration.
    pub fn validate(
        &self,
        field: FieldId,
        prior: Option<&StoredVectorEvidenceV1>,
        supplied: Option<&VectorEvidenceMutationV1>,
    ) -> Result<Option<VectorEvidenceTransitionPlanV1>, CatalogError> {
        self.validate_inventory(field, supplied)?;
        let schema = self.bundle.bundle().schema();
        let production = schema
            .vector_production_spec(self.target.entity_type_id(), field)
            .ok_or_else(corrupt)?;
        let spec = schema
            .vector_field_spec(self.target.entity_type_id(), field)
            .ok_or_else(corrupt)?;
        let old_value = self.vector_value(self.prior.as_deref(), field)?;
        let next_value = self.vector_value(self.next.as_ref(), field)?;
        if let Some(evidence) = prior {
            let entity = self.prior.as_deref().ok_or_else(corrupt)?;
            if evidence.target() != self.target
                || evidence.vector_field() != field
                || evidence.entity_version() > entity.entity_version()
                || evidence.evidence_sequence() >= self.sequence
                || evidence.schema_binding().lineage() != self.plan.contract_lineage()
                || evidence.embedding_write().is_some() != old_value.is_some()
            {
                return Err(corrupt());
            }
        } else if old_value.is_some()
            || self
                .prior
                .as_ref()
                .is_some_and(|row| row.schema_binding().matches_plan(self.plan))
        {
            // Every create under this exact production declaration emitted
            // evidence, including an explicitly absent embedding.
            return Err(corrupt());
        }
        let derived = if let Some(next) = &self.next {
            let rewrite = match supplied {
                Some(VectorEvidenceMutationV1::Put(evidence)) => evidence
                    .embedding_write()
                    .filter(|write| write.sequence() == self.sequence),
                _ => None,
            };
            if rewrite.is_some_and(|write| write.metadata() != production.metadata())
                || (old_value != next_value && rewrite.is_none())
                || (rewrite.is_some() && next_value.is_none())
            {
                return Err(corrupt());
            }
            let source_changed = self.prior.is_none()
                || spec.source_fields().iter().any(|field| {
                    self.prior
                        .as_deref()
                        .and_then(|row| field_value(row, *field))
                        != field_value(next, *field)
                });
            if source_changed || rewrite.is_some() {
                Some(
                    VectorEvidenceTransitionPlanV1::live(
                        self.target.clone(),
                        self.partition.clone(),
                        field,
                        spec.stale_entity_count_threshold(),
                        next.entity_version(),
                        prior,
                        source_changed,
                        rewrite.map(|write| write.metadata().clone()),
                        DurableKeySchemaBindingV1::from_plan(self.plan),
                        self.provenance,
                        self.plan.clone(),
                    )
                    .map_err(|_| corrupt())?,
                )
            } else {
                None
            }
        } else {
            prior
                .map(|evidence| {
                    VectorEvidenceTransitionPlanV1::delete(
                        evidence,
                        spec.stale_entity_count_threshold(),
                        self.provenance,
                        self.plan.clone(),
                    )
                })
                .transpose()
                .map_err(|_| corrupt())?
        };
        let expected = derived
            .as_ref()
            .map(|plan| plan.materialize(self.sequence))
            .transpose()
            .map_err(|_| corrupt())?;
        if expected.as_ref() != supplied {
            return Err(corrupt());
        }
        Ok(derived)
    }

    fn vector_value<'a>(
        &self,
        row: Option<&'a StoredEntityRecordV1>,
        field: FieldId,
    ) -> Result<Option<&'a CanonicalValue>, CatalogError> {
        let Some(row) = row else {
            return Ok(None);
        };
        let value = self.validated_field(row, field)?;
        match value {
            CanonicalValue::Null => Ok(None),
            value @ CanonicalValue::Vector(_) => Ok(Some(value)),
            _ => Err(corrupt()),
        }
    }
    fn validated_field<'a>(
        &self,
        row: &'a StoredEntityRecordV1,
        field: FieldId,
    ) -> Result<&'a CanonicalValue, CatalogError> {
        let value = field_value(row, field).ok_or_else(corrupt)?;
        let entity = self
            .bundle
            .bundle()
            .schema()
            .entity(self.target.entity_type_id())
            .ok_or_else(corrupt)?;
        let declaration = entity
            .record()
            .fields()
            .iter()
            .find(|candidate| candidate.id() == field)
            .ok_or_else(corrupt)?;
        crate::materialization::validate_static_value(
            self.bundle.bundle().schema(),
            declaration.value_type(),
            value,
        )
        .map_err(|_| corrupt())?;
        Ok(value)
    }
}

fn field_value(row: &StoredEntityRecordV1, field: FieldId) -> Option<&CanonicalValue> {
    row.fields()
        .fields()
        .binary_search_by_key(&field, |(id, _)| *id)
        .ok()
        .map(|offset| &row.fields().fields()[offset].1)
}

impl std::fmt::Debug for CommandPrefixVectorChecksV1<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("CommandPrefixVectorChecksV1([REDACTED])")
    }
}

#[cfg(test)]
#[path = "command_prefix_vector_tests.rs"]
mod tests;
