//! Catalog-resolved, process-local projection event materialization.

use std::error::Error;
use std::fmt;
use std::sync::Arc;

use riffdb_contract_ir::{Instruction, ProjectionPlan, RecordSchema, SchemaIr};
use riffdb_storage_api::{DurableKeySchemaBindingV1, ExecutablePlanRef, StoredDurableEventV1};
use riffdb_types::{CanonicalRecord, CanonicalValue, ProjectionIdentity, encode_canonical_record};

use crate::lineage::{LineageMaterializationProof, RecordOwnerV1, WriterRelation};
use crate::materialization::validate_static_value;
use crate::{
    ActiveCatalogSnapshot, CatalogError, CatalogErrorKind, MigrationFinding,
    ValidatedContractBundle, ValidatedMigrationPlan,
};

/// Stable classification for projection event-materialization failures.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ProjectionEventMaterializationErrorKind {
    /// Event, writer, lineage, schema, or proof evidence was inconsistent.
    Integrity,
    /// Proof-authorized normalization exceeded the canonical document ceiling.
    HardLimit,
}

impl ProjectionEventMaterializationErrorKind {
    /// Returns fixed safe text without event, plan, field, or payload details.
    #[must_use]
    pub const fn safe_message(self) -> &'static str {
        match self {
            Self::Integrity => "projection event materialization integrity failure",
            Self::HardLimit => "projection event materialization exceeds the hard limit",
        }
    }
}

/// A typed, redaction-safe projection event-materialization failure.
#[derive(Clone, Eq, PartialEq)]
pub struct ProjectionEventMaterializationError {
    kind: ProjectionEventMaterializationErrorKind,
}

impl ProjectionEventMaterializationError {
    const fn integrity() -> Self {
        Self {
            kind: ProjectionEventMaterializationErrorKind::Integrity,
        }
    }

    const fn hard_limit() -> Self {
        Self {
            kind: ProjectionEventMaterializationErrorKind::HardLimit,
        }
    }

    /// Returns the stable failure classification.
    #[must_use]
    pub const fn kind(&self) -> ProjectionEventMaterializationErrorKind {
        self.kind
    }
}

impl fmt::Debug for ProjectionEventMaterializationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProjectionEventMaterializationError")
            .field("kind", &self.kind)
            .finish()
    }
}

impl fmt::Display for ProjectionEventMaterializationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.kind.safe_message())
    }
}

impl Error for ProjectionEventMaterializationError {}

/// One exact checked projection plan resolved through the active lineage.
#[derive(Clone)]
pub struct ResolvedProjectionPlan {
    identity: ProjectionIdentity,
    plan: ProjectionPlan,
    bundle: ValidatedContractBundle,
    lineage_proof: Arc<LineageMaterializationProof>,
    projection_ordinal: u16,
}

impl ResolvedProjectionPlan {
    /// Returns the exact lineage, projection ID, and semantic plan hash.
    #[must_use]
    pub const fn identity(&self) -> &ProjectionIdentity {
        &self.identity
    }

    /// Borrows the exact checked projection plan selected by the identity.
    #[must_use]
    pub const fn projection_plan(&self) -> &ProjectionPlan {
        &self.plan
    }

    /// Validates one immutable event and exposes only its normalized known fields.
    pub fn materialize_event<'plan, 'event>(
        &'plan self,
        writer: &ExecutablePlanRef,
        event: &'event StoredDurableEventV1,
    ) -> Result<
        ProjectionEventMaterializationView<'plan, 'event>,
        ProjectionEventMaterializationError,
    > {
        let (writer_ordinal, writer_bundle) = self
            .lineage_proof
            .exact_member(writer.contract_version(), writer.contract_bundle_hash())
            .filter(|(_, bundle)| bundle.lineage() == writer.contract_lineage())
            .ok_or_else(ProjectionEventMaterializationError::integrity)?;
        let writer_plan = writer_bundle
            .resolve_plan_with_proof(writer, Arc::clone(&self.lineage_proof), writer_ordinal)
            .map_err(|_| ProjectionEventMaterializationError::integrity())?;

        if event.event_type_id() != self.plan.source_event()
            || !writer_plan.plan().instructions().iter().any(|instruction| {
                matches!(
                    instruction,
                    Instruction::EmitEvent(construction)
                        if construction.event_type() == event.event_type_id()
                )
            })
        {
            return Err(ProjectionEventMaterializationError::integrity());
        }

        let writer_schema = writer_bundle
            .bundle()
            .schema()
            .event(event.event_type_id())
            .ok_or_else(ProjectionEventMaterializationError::integrity)?;
        validate_complete_payload(
            writer_bundle.bundle().schema(),
            writer_schema.payload(),
            event.payload(),
        )?;

        let projection_schema = self
            .bundle
            .bundle()
            .schema()
            .event(self.plan.source_event())
            .ok_or_else(ProjectionEventMaterializationError::integrity)?;
        let writer_binding = DurableKeySchemaBindingV1::from_plan(writer);
        let (relation, null_fill) = self
            .lineage_proof
            .writer_materialization(
                RecordOwnerV1::Event(event.event_type_id()),
                &writer_binding,
                self.projection_ordinal,
            )
            .map_err(|_| ProjectionEventMaterializationError::integrity())?;
        if null_fill.as_ref().is_some_and(|mask| {
            !mask.has_canonical_shape(projection_schema.payload().fields().len())
        }) {
            return Err(ProjectionEventMaterializationError::integrity());
        }

        let mut known_fields = Vec::with_capacity(projection_schema.payload().fields().len());
        for (position, field) in projection_schema.payload().fields().iter().enumerate() {
            let source_value = event
                .payload()
                .fields()
                .binary_search_by_key(&field.id(), |(field_id, _)| *field_id)
                .ok()
                .map(|index| &event.payload().fields()[index].1);
            let eligible = null_fill.as_ref().is_some_and(|mask| mask.allows(position));
            let value = match source_value {
                Some(value) => {
                    validate_static_value(self.bundle.bundle().schema(), field.value_type(), value)
                        .map_err(|_| ProjectionEventMaterializationError::integrity())?;
                    value.clone()
                }
                None if relation == WriterRelation::Ancestor
                    && eligible
                    && field.value_type().is_optional() =>
                {
                    CanonicalValue::Null
                }
                None => return Err(ProjectionEventMaterializationError::integrity()),
            };
            known_fields.push((field.id(), value));
        }

        let known_payload = CanonicalRecord::new(known_fields)
            .map_err(|_| ProjectionEventMaterializationError::integrity())?;
        encode_canonical_record(&known_payload)
            .map_err(|_| ProjectionEventMaterializationError::hard_limit())?;
        Ok(ProjectionEventMaterializationView {
            resolved: self,
            _source_event: event,
            known_payload,
        })
    }
}

impl fmt::Debug for ResolvedProjectionPlan {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResolvedProjectionPlan")
            .field("identity", &"[REDACTED]")
            .field("plan", &"[CHECKED]")
            .field("bundle", &"[CHECKED]")
            .field("lineage_proof", &"[CHECKED]")
            .field("lineage_bundle_count", &self.lineage_proof.bundle_count())
            .field("projection_ordinal", &self.projection_ordinal)
            .finish()
    }
}

/// Move-only process-local view presented to projection expression evaluation.
///
/// The source event borrow privately retains every unknown field unchanged. The
/// only payload accessor returns a distinct record containing fields known to
/// the exact resolved plan plus proof-authorized canonical nulls.
///
/// ```compile_fail
/// use riffdb_catalog::ProjectionEventMaterializationView;
///
/// fn require_clone<T: Clone>() {}
///
/// require_clone::<ProjectionEventMaterializationView<'static, 'static>>();
/// ```
pub struct ProjectionEventMaterializationView<'plan, 'event> {
    resolved: &'plan ResolvedProjectionPlan,
    _source_event: &'event StoredDurableEventV1,
    known_payload: CanonicalRecord,
}

impl ProjectionEventMaterializationView<'_, '_> {
    /// Borrows the exact checked projection plan for this view.
    #[must_use]
    pub const fn projection_plan(&self) -> &ProjectionPlan {
        self.resolved.projection_plan()
    }

    /// Borrows only the normalized fields visible to the resolved plan.
    #[must_use]
    pub const fn known_payload(&self) -> &CanonicalRecord {
        &self.known_payload
    }
}

impl fmt::Debug for ProjectionEventMaterializationView<'_, '_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ProjectionEventMaterializationView([REDACTED])")
    }
}

impl ActiveCatalogSnapshot {
    /// Resolves one exact active-lineage projection identity.
    ///
    /// Projection identity intentionally excludes application version. When an
    /// unchanged plan appears in multiple compatible bundles, the latest exact
    /// active-lineage member is selected.
    pub fn resolve_projection(
        &self,
        identity: &ProjectionIdentity,
    ) -> Result<ResolvedProjectionPlan, CatalogError> {
        if identity.contract_lineage() != self.pointer().lineage() {
            return Err(CatalogError::new(CatalogErrorKind::UnknownExecutablePlan));
        }
        let (index, bundle, plan) = self
            .lineage_proof()
            .bundles()
            .iter()
            .enumerate()
            .rev()
            .find_map(|(index, bundle)| {
                bundle
                    .bundle()
                    .projection(identity.projection_id())
                    .filter(|plan| plan.plan_hash() == identity.plan_hash())
                    .map(|plan| (index, bundle, plan))
            })
            .ok_or_else(|| CatalogError::new(CatalogErrorKind::UnknownExecutablePlan))?;
        let projection_ordinal = u16::try_from(index)
            .map_err(|_| CatalogError::new(CatalogErrorKind::UnknownExecutablePlan))?;
        Ok(ResolvedProjectionPlan {
            identity: identity.clone(),
            plan: plan.clone(),
            bundle: bundle.clone(),
            lineage_proof: Arc::clone(self.lineage_proof()),
            projection_ordinal,
        })
    }
}

impl ValidatedMigrationPlan {
    /// Resolves one required successor projection without activating its catalog.
    pub fn resolve_candidate_projection(
        &self,
        projection_id: riffdb_types::ProjectionId,
    ) -> Result<ResolvedProjectionPlan, MigrationFinding> {
        if !self.rebuilt_projections().contains(&projection_id) {
            return Err(MigrationFinding::from_stage_error(
                riffdb_storage_api::MigrationStageError::Integrity,
            ));
        }
        let proof = self.candidate_lineage_proof()?;
        let ordinal = u16::try_from(proof.bundle_count().saturating_sub(1)).map_err(|_| {
            MigrationFinding::from_stage_error(riffdb_storage_api::MigrationStageError::Integrity)
        })?;
        let bundle = proof.terminal().clone();
        let plan = bundle
            .bundle()
            .projection(projection_id)
            .ok_or_else(|| {
                MigrationFinding::from_stage_error(
                    riffdb_storage_api::MigrationStageError::Integrity,
                )
            })?
            .clone();
        Ok(ResolvedProjectionPlan {
            identity: ProjectionIdentity::new(
                bundle.lineage().clone(),
                projection_id,
                plan.plan_hash(),
            ),
            plan,
            bundle,
            lineage_proof: proof,
            projection_ordinal: ordinal,
        })
    }
}

fn validate_complete_payload(
    schema: &SchemaIr,
    record_schema: &RecordSchema,
    payload: &CanonicalRecord,
) -> Result<(), ProjectionEventMaterializationError> {
    if record_schema.fields().len() != payload.fields().len() {
        return Err(ProjectionEventMaterializationError::integrity());
    }
    for (schema_field, (field_id, value)) in record_schema.fields().iter().zip(payload.fields()) {
        if schema_field.id() != *field_id {
            return Err(ProjectionEventMaterializationError::integrity());
        }
        validate_static_value(schema, schema_field.value_type(), value)
            .map_err(|_| ProjectionEventMaterializationError::integrity())?;
    }
    Ok(())
}
