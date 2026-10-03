//! Account for all retained artifacts and the largest generation workspace.
// req: OQ-040, PRJ-004
use super::*;
use riffdb_types::{
    OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1,
    exact_provider_budget_v1::{BudgetUnavailable, ProviderAllocation, ProviderBudget},
};
use std::sync::Arc;
use workflow_admission::allocation;

pub(super) struct SchemaRegistrySectionBound {
    pub(super) retained: u64,
    pub(super) generation: u64,
}
impl SchemaRegistrySectionBound {
    pub(super) fn inspect(
        bytes: &[u8],
        schema: &SchemaIr,
        commands: &[CommandPlan],
        projections: &[ProjectionPlan],
    ) -> Result<Self, ProviderBundleEncodingErrorV1> {
        let mut reader = Reader::new(bytes);
        let count = decode_len_with_minimum(&mut reader, "generated schema artifacts", 20_480, 41)?;
        if count != expected_schema_artifact_count(schema, commands, projections)? {
            return Err(IrValidationError::HashMismatch {
                kind: "generated schema artifact registry",
            }
            .into());
        }
        // Inspect framing without constructing JSON or copying stored payloads.
        // Ordinary regeneration below still checks each exact key, JSON and hash.
        for _ in 0..count {
            reader.array::<5>()?;
            reader.bytes(crate::MAX_JSON_SCHEMA_ARTIFACT_BYTES)?;
            reader.array::<32>()?;
        }
        reader.finish()?;
        let mut retained = allocation::<GeneratedSchemaArtifact>(count)?;
        let mut generation = 0;
        visit_schema_artifact_sources(schema, commands, projections, |source| {
            let size = source.expected_size(schema)?;
            checked_len(
                "generated JSON Schema",
                size,
                crate::MAX_JSON_SCHEMA_ARTIFACT_BYTES,
            )?;
            retained = retained
                .checked_add(allocation::<u8>(size)?)
                .ok_or(BudgetUnavailable)?;
            generation = generation.max(crate::json_schema::json_workspace_bound(size)?);
            Ok::<_, ProviderBundleEncodingErrorV1>(())
        })?;
        Ok(Self {
            retained,
            generation,
        })
    }
    pub(super) fn maximum(&self) -> Result<u64, BudgetUnavailable> {
        // Earlier artifact strings survive the next JSON construction/render.
        // The established per-artifact bound includes the current output string;
        // including it in retained capacity too is conservative. The vector is
        // preallocated to the exact count and never grows during generation.
        self.retained
            .checked_add(self.generation)
            .and_then(|value| value.checked_add(32 * 1024))
            .and_then(|value| value.checked_add(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1))
            .ok_or(BudgetUnavailable)
    }
}

/// Decode the existing schema registry against already constructed first-party
/// IR. Input ownership, IR construction and complete bundle identity are separate.
#[doc(hidden)]
pub fn decode_provider_schema_registry_section_admitted_v1(
    bytes: &[u8],
    schema: &SchemaIr,
    commands: &[CommandPlan],
    projections: &[ProjectionPlan],
    budget: &ProviderBudget,
) -> Result<Arc<ProviderAllocation<Vec<GeneratedSchemaArtifact>>>, ProviderBundleEncodingErrorV1> {
    checked_len("canonical contract bundle", bytes.len(), MAX_BUNDLE_BYTES)?;
    let sizing = budget.reserve(32 * 1024)?;
    let bound = SchemaRegistrySectionBound::inspect(bytes, schema, commands, projections)?;
    let work = budget.reserve(bound.maximum()?)?;
    drop(sizing);
    let mut reader = Reader::new(bytes);
    let artifacts = decode_schema_artifacts(&mut reader, schema, commands, projections)?;
    reader.finish()?;
    work.settle_owned_value(artifacts).map_err(Into::into)
}
