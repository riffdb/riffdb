//! Conversion of checked source deltas into the existing provider mutations.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use riffdb_projection::{
    ExactPredicateIndexMutationV1, ExactPredicatePartitionIndexV4, ExactPredicatePartitionIndexV5,
    ExactPredicateProviderErrorV1, ExactPredicateProviderRowV1, ExactTextIndexMutationV2,
    ExactTextIndexMutationV3, ExactTextProviderErrorV1,
};
use riffdb_query_ir::QueryAccessProgramV1;
use riffdb_types::{
    CanonicalRecord, CanonicalValue, CommitSequence, FieldId, ProjectionGeneration,
};

use super::super::{
    ExactPredicateRegistrationView, ExactTextProviderState, ExactTextRegistration, RebuildFailure,
    exact_reference_cell,
};
use super::{CapturedSource, Delta, DeltaPlan, Selected};

fn output_fields(program: &QueryAccessProgramV1) -> Result<BTreeSet<FieldId>, RebuildFailure> {
    let [step] = program.steps() else {
        return Err(RebuildFailure::Integrity);
    };
    let access = program
        .internal_entity_access(step.entity())
        .ok_or(RebuildFailure::Integrity)?;
    step.selected_fields()
        .iter()
        .map(|name| {
            access
                .internal_field_id(name)
                .ok_or(RebuildFailure::Integrity)
        })
        .collect()
}

fn output(
    record: &CanonicalRecord,
    fields: &BTreeSet<FieldId>,
) -> Result<CanonicalRecord, RebuildFailure> {
    let projected = CanonicalRecord::new(
        record
            .fields()
            .iter()
            .filter(|(field, _)| fields.contains(field))
            .cloned()
            .collect(),
    )
    .map_err(|_| RebuildFailure::Integrity)?;
    if projected.len() != fields.len() {
        return Err(RebuildFailure::Integrity);
    }
    Ok(projected)
}

pub(in crate::exact_text_adapter) fn prepare_text(
    captured: &CapturedSource,
    previous: &Selected<ExactTextProviderState>,
    registration: &ExactTextRegistration,
) -> Result<Option<Selected<ExactTextProviderState>>, RebuildFailure> {
    let generation = previous.generation();
    if previous.partition()
        != riffdb_types::hash_partition_key(registration.partition_key.as_bytes())
    {
        return Err(RebuildFailure::Integrity);
    }
    match (previous.provider.as_ref(), registration.query.filter()) {
        (ExactTextProviderState::V2(_), None) => {}
        (ExactTextProviderState::V3(provider), Some(filter))
            if provider.filter_field() == filter.internal_field() => {}
        _ => return Err(RebuildFailure::Integrity),
    }
    if !previous.validates_frontier(previous.frontier().ok_or(RebuildFailure::Integrity)?) {
        return Err(RebuildFailure::Integrity);
    }
    let program = registration.query.representative_program();
    let Some(delta) = Delta::read(
        captured,
        &previous.source,
        DeltaPlan {
            program,
            partition_value: &registration.partition_value,
            max_candidates: registration.query.binding().family().max_candidates(),
            policy: registration.row_policy.as_deref(),
            generation,
        },
    )?
    else {
        return Ok(None);
    };
    let mut provider = previous.provider.clone();
    if delta.source.head > previous.source.head {
        let fields = output_fields(program)?;
        let text_field = registration.query.binding().family().field();
        let filter_field = registration
            .query
            .filter()
            .map(|filter| filter.internal_field());
        let mut unfiltered = Vec::new();
        let mut filtered = Vec::new();
        for (key, record) in delta.changes {
            let row = record.as_ref().map(|record| record.fields());
            let text = row.and_then(|row| {
                row.fields()
                    .iter()
                    .find(|(field, _)| *field == text_field)
                    .map(|(_, value)| value)
            });
            let text = match text {
                None | Some(CanonicalValue::Null) => None,
                Some(CanonicalValue::String(text)) => Some(text.as_str().to_owned()),
                Some(_) => return Err(RebuildFailure::Integrity),
            };
            match (text, row, filter_field) {
                (Some(value), Some(row), None) => {
                    unfiltered.push(ExactTextIndexMutationV2::Upsert {
                        row: key,
                        value,
                        output: output(row, &fields)?,
                    })
                }
                (Some(value), Some(row), Some(filter)) => {
                    filtered.push(ExactTextIndexMutationV3::Upsert {
                        row: key,
                        value,
                        filter: row
                            .fields()
                            .iter()
                            .find(|(field, _)| *field == filter)
                            .map_or(CanonicalValue::Null, |(_, value)| value.clone()),
                        output: output(row, &fields)?,
                    })
                }
                (_, _, None) => unfiltered.push(ExactTextIndexMutationV2::Delete { row: key }),
                (_, _, Some(_)) => filtered.push(ExactTextIndexMutationV3::Delete(key)),
            }
        }
        let result = match Arc::make_mut(&mut provider) {
            ExactTextProviderState::V2(provider) if filter_field.is_none() => {
                provider.apply(delta.source.head, &unfiltered)
            }
            ExactTextProviderState::V3(provider) if filter_field.is_some() => {
                provider.apply(delta.source.head, &filtered)
            }
            _ => return Err(RebuildFailure::Integrity),
        };
        result.map_err(|error| match error {
            ExactTextProviderErrorV1::PartitionRowLimit
            | ExactTextProviderErrorV1::CheckpointTooLarge
            | ExactTextProviderErrorV1::ValueTooLong
            | ExactTextProviderErrorV1::OutputTooLarge => RebuildFailure::Capacity(generation),
            _ => RebuildFailure::Integrity,
        })?;
    } else if !delta.changes.is_empty() {
        return Err(RebuildFailure::Integrity);
    }
    Ok(Some(Selected {
        provider,
        source: delta.source,
    }))
}

pub(in crate::exact_text_adapter) trait PredicateProvider:
    Clone
{
    fn binding_matches(
        &self,
        expected: (
            riffdb_types::QueryPlanHash,
            riffdb_types::ApplicationRoleHash,
            riffdb_types::PartitionKeyHash,
            u64,
        ),
    ) -> bool;
    fn generation(&self) -> ProjectionGeneration;
    fn frontier(&self) -> CommitSequence;
    fn apply(
        &mut self,
        head: CommitSequence,
        changes: &[ExactPredicateIndexMutationV1],
    ) -> Result<(), ExactPredicateProviderErrorV1>;
}

macro_rules! predicate_provider {
    ($ty:ty) => {
        impl PredicateProvider for $ty {
            fn binding_matches(
                &self,
                expected: (
                    riffdb_types::QueryPlanHash,
                    riffdb_types::ApplicationRoleHash,
                    riffdb_types::PartitionKeyHash,
                    u64,
                ),
            ) -> bool {
                let binding = self.binding();
                (
                    binding.plan(),
                    binding.policy_shape(),
                    binding.partition(),
                    binding.history_incarnation(),
                ) == expected
            }
            fn generation(&self) -> ProjectionGeneration {
                self.binding().generation()
            }
            fn frontier(&self) -> CommitSequence {
                self.binding().frontier()
            }
            fn apply(
                &mut self,
                head: CommitSequence,
                changes: &[ExactPredicateIndexMutationV1],
            ) -> Result<(), ExactPredicateProviderErrorV1> {
                Self::apply(self, head, changes)
            }
        }
    };
}
predicate_provider!(ExactPredicatePartitionIndexV4);
predicate_provider!(ExactPredicatePartitionIndexV5);

pub(in crate::exact_text_adapter) fn prepare_predicate<
    P: PredicateProvider,
    R: ExactPredicateRegistrationView,
>(
    captured: &CapturedSource,
    previous: &Selected<P>,
    registration: &R,
) -> Result<Option<Selected<P>>, RebuildFailure> {
    let generation = previous.generation();
    if !previous.binding_matches((
        registration.identity(),
        registration.policy_shape(),
        riffdb_types::hash_partition_key(registration.partition_key().as_bytes()),
        captured.history_incarnation,
    )) {
        return Err(RebuildFailure::Integrity);
    }
    if !previous.validates_frontier(previous.frontier()) {
        return Err(RebuildFailure::Integrity);
    }
    let program = registration.access_program();
    let Some(delta) = Delta::read(
        captured,
        &previous.source,
        DeltaPlan {
            program,
            partition_value: registration.partition_value(),
            max_candidates: registration.max_candidates(),
            policy: registration.row_policy(),
            generation,
        },
    )?
    else {
        return Ok(None);
    };
    let mut provider = previous.provider.clone();
    if delta.source.head > previous.source.head {
        let output_fields = output_fields(program)?;
        let profiles = registration.referenced_profiles()?;
        let changes = delta
            .changes
            .into_iter()
            .map(|(key, record)| {
                let Some(record) = record else {
                    return Ok(ExactPredicateIndexMutationV1::Delete(key));
                };
                let values = record
                    .fields()
                    .fields()
                    .iter()
                    .cloned()
                    .collect::<BTreeMap<_, _>>();
                let fields = profiles
                    .iter()
                    .map(|(field, profile)| {
                        exact_reference_cell(values.get(field), *profile).map(|cell| (*field, cell))
                    })
                    .collect::<Result<_, _>>()?;
                Ok(ExactPredicateIndexMutationV1::Upsert(
                    ExactPredicateProviderRowV1::new(
                        key,
                        fields,
                        output(record.fields(), &output_fields)?,
                    ),
                ))
            })
            .collect::<Result<Vec<_>, RebuildFailure>>()?;
        Arc::make_mut(&mut provider)
            .apply(delta.source.head, &changes)
            .map_err(|error| match error {
                ExactPredicateProviderErrorV1::BoundExceeded
                | ExactPredicateProviderErrorV1::StateAmplification
                | ExactPredicateProviderErrorV1::FuelExhausted => {
                    RebuildFailure::Capacity(generation)
                }
                _ => RebuildFailure::Integrity,
            })?;
    } else if !delta.changes.is_empty() {
        return Err(RebuildFailure::Integrity);
    }
    Ok(Some(Selected {
        provider,
        source: delta.source,
    }))
}
