//! Pure projection-plan evaluation and checked atomic-apply preparation.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;

use riffdb_catalog::{
    ProjectionEventMaterializationErrorKind, ProjectionEventMaterializationView,
    ResolvedProjectionPlan,
};
use riffdb_contract_ir::{ProjectionAggregation, ProjectionMeasurePlan};
use riffdb_invariant::{EvaluationError, ExpressionEvaluator, ExpressionValueSource};
use riffdb_storage_api::{
    CheckedProjectionSchema, ProjectionApplyRequestV1, ProjectionApplyRowObservation,
    ProjectionApplySnapshotReader, ProjectionApplySnapshotRequest, ProjectionRowPrior,
    ProjectionRowUpdateV1, StorageError, StorageErrorKind, StorageValueError, StoredCommitRecordV1,
};
use riffdb_types::{
    CanonicalRecord, CanonicalValue, CommitSequence, Date, FieldId, LogicalTime,
    MAX_PROJECTION_ROW_UPDATES, ProjectionGeneration, ProjectionGroupKey, ProjectionIdentity,
};

const SECONDS_PER_DAY: i64 = 86_400;

/// Closed failure returned while deriving one projection apply request.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ProjectionEvaluationErrorKind {
    /// A checked expression or aggregate addition overflowed.
    ArithmeticOverflow,
    /// Catalog rejected a source event as malformed or incompatible.
    MalformedDurableEvent,
    /// A required authoritative commit was absent from the contiguous replay.
    MissingCommit,
    /// The exact checked projection plan and schema did not agree.
    PlanOrSchemaUnavailable,
    /// Existing derived state or a storage observation was inconsistent.
    ProjectionStateIntegrity,
    /// A fixed projection bound was exceeded.
    HardLimitExceeded,
    /// The specialized storage port was temporarily unavailable.
    StorageUnavailable,
    /// Storage could not establish whether a derived write completed.
    CommitStatusUnknown,
}

impl ProjectionEvaluationErrorKind {
    /// Returns the durable closed generation-failure code, when applicable.
    #[must_use]
    pub const fn failure_code(self) -> Option<riffdb_storage_api::ProjectionFailureCodeV1> {
        use riffdb_storage_api::ProjectionFailureCodeV1;
        match self {
            Self::ArithmeticOverflow => Some(ProjectionFailureCodeV1::ArithmeticOverflow),
            Self::MalformedDurableEvent => Some(ProjectionFailureCodeV1::MalformedDurableEvent),
            Self::MissingCommit => Some(ProjectionFailureCodeV1::MissingCommit),
            Self::PlanOrSchemaUnavailable => Some(ProjectionFailureCodeV1::PlanOrSchemaUnavailable),
            Self::ProjectionStateIntegrity => {
                Some(ProjectionFailureCodeV1::ProjectionStateIntegrity)
            }
            Self::HardLimitExceeded => Some(ProjectionFailureCodeV1::HardLimitExceeded),
            Self::StorageUnavailable | Self::CommitStatusUnknown => None,
        }
    }

    const fn safe_message(self) -> &'static str {
        match self {
            Self::ArithmeticOverflow => "projection arithmetic overflow",
            Self::MalformedDurableEvent => "projection source event is malformed",
            Self::MissingCommit => "projection source commit is missing",
            Self::PlanOrSchemaUnavailable => "projection plan or schema is unavailable",
            Self::ProjectionStateIntegrity => "projection state integrity failure",
            Self::HardLimitExceeded => "projection hard limit exceeded",
            Self::StorageUnavailable => "projection storage is unavailable",
            Self::CommitStatusUnknown => "projection commit status is unknown",
        }
    }
}

/// Redaction-safe projection evaluation failure.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct ProjectionEvaluationError {
    kind: ProjectionEvaluationErrorKind,
}

impl ProjectionEvaluationError {
    pub(crate) const fn new(kind: ProjectionEvaluationErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the closed failure classification.
    #[must_use]
    pub const fn kind(self) -> ProjectionEvaluationErrorKind {
        self.kind
    }
}

impl fmt::Debug for ProjectionEvaluationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProjectionEvaluationError")
            .field("kind", &self.kind)
            .finish()
    }
}

impl fmt::Display for ProjectionEvaluationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.kind.safe_message())
    }
}

impl Error for ProjectionEvaluationError {}

/// Move-only, process-local aggregate deltas for one authoritative commit.
///
/// Values remain private so callers cannot substitute rows between evaluation
/// and apply preparation. Debug output never exposes group keys or measures.
pub struct EvaluatedProjectionCommit {
    schema: CheckedProjectionSchema,
    generation: ProjectionGeneration,
    sequence: CommitSequence,
    measures: Vec<ProjectionMeasurePlan>,
    grouped_deltas: BTreeMap<ProjectionGroupKey, CanonicalRecord>,
    grouped_event_counts: BTreeMap<ProjectionGroupKey, u64>,
    grouped_mean_totals: BTreeMap<(ProjectionGroupKey, FieldId), i128>,
}

impl EvaluatedProjectionCommit {
    /// Returns the exact projection identity bound during evaluation.
    #[must_use]
    pub const fn identity(&self) -> &ProjectionIdentity {
        self.schema.identity()
    }

    /// Returns the target retained generation.
    #[must_use]
    pub const fn generation(&self) -> ProjectionGeneration {
        self.generation
    }

    /// Returns the exact authoritative sequence, including irrelevant commits.
    #[must_use]
    pub const fn sequence(&self) -> CommitSequence {
        self.sequence
    }

    /// Returns the bounded number of changed groups.
    #[must_use]
    pub fn changed_group_count(&self) -> usize {
        self.grouped_deltas.len()
    }

    pub(crate) const fn schema(&self) -> &CheckedProjectionSchema {
        &self.schema
    }

    pub(crate) fn grouped_deltas(&self) -> &BTreeMap<ProjectionGroupKey, CanonicalRecord> {
        &self.grouped_deltas
    }

    pub(crate) fn combine_with_prior(
        &self,
        key: &ProjectionGroupKey,
        prior_measures: &CanonicalRecord,
    ) -> Result<CanonicalRecord, ProjectionEvaluationError> {
        let Some(delta) = self.grouped_deltas.get(key) else {
            return Err(ProjectionEvaluationError::new(
                ProjectionEvaluationErrorKind::ProjectionStateIntegrity,
            ));
        };
        if prior_measures.fields().len() != delta.fields().len()
            || prior_measures.fields().len() != self.measures.len()
        {
            return Err(ProjectionEvaluationError::new(
                ProjectionEvaluationErrorKind::ProjectionStateIntegrity,
            ));
        }
        let prior_count = self.prior_event_count(prior_measures)?;
        let delta_count = self.grouped_event_counts.get(key).copied().unwrap_or(1);
        let mut fields = Vec::with_capacity(self.measures.len());
        for ((measure, (current_id, current)), (delta_id, delta_val)) in self
            .measures
            .iter()
            .zip(prior_measures.fields())
            .zip(delta.fields())
        {
            if current_id != delta_id || *current_id != measure.field().id() {
                return Err(ProjectionEvaluationError::new(
                    ProjectionEvaluationErrorKind::ProjectionStateIntegrity,
                ));
            }
            let combined = match measure.aggregation() {
                ProjectionAggregation::Count | ProjectionAggregation::Sum => {
                    checked_add(current, delta_val)?
                }
                ProjectionAggregation::Min => checked_min(current, delta_val)?,
                ProjectionAggregation::Max => checked_max(current, delta_val)?,
                ProjectionAggregation::Mean => {
                    if prior_count == 0 {
                        delta_val.clone()
                    } else {
                        let prior_total = value_to_i128(current)?
                            .checked_mul(i128::from(prior_count))
                            .ok_or_else(|| {
                                ProjectionEvaluationError::new(
                                    ProjectionEvaluationErrorKind::ArithmeticOverflow,
                                )
                            })?;
                        let delta_total = self
                            .grouped_mean_totals
                            .get(&(key.clone(), *current_id))
                            .copied()
                            .ok_or_else(|| {
                                ProjectionEvaluationError::new(
                                    ProjectionEvaluationErrorKind::ProjectionStateIntegrity,
                                )
                            })?;
                        let total = prior_total.checked_add(delta_total).ok_or_else(|| {
                            ProjectionEvaluationError::new(
                                ProjectionEvaluationErrorKind::ArithmeticOverflow,
                            )
                        })?;
                        let total_count =
                            prior_count.checked_add(delta_count).ok_or_else(|| {
                                ProjectionEvaluationError::new(
                                    ProjectionEvaluationErrorKind::ArithmeticOverflow,
                                )
                            })?;
                        i128_to_mean_value(total, total_count, current)?
                    }
                }
            };
            fields.push((*current_id, combined));
        }
        CanonicalRecord::new(fields).map_err(|_| {
            ProjectionEvaluationError::new(ProjectionEvaluationErrorKind::ProjectionStateIntegrity)
        })
    }

    fn prior_event_count(
        &self,
        prior_measures: &CanonicalRecord,
    ) -> Result<u64, ProjectionEvaluationError> {
        for (measure, (_, value)) in self.measures.iter().zip(prior_measures.fields()) {
            if measure.aggregation() == ProjectionAggregation::Count {
                return match value {
                    CanonicalValue::U64(count) => Ok(*count),
                    _ => Err(ProjectionEvaluationError::new(
                        ProjectionEvaluationErrorKind::ProjectionStateIntegrity,
                    )),
                };
            }
        }
        Ok(1)
    }
}

impl fmt::Debug for EvaluatedProjectionCommit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EvaluatedProjectionCommit")
            .field("identity", &"[REDACTED]")
            .field("generation", &self.generation)
            .field("sequence", &self.sequence)
            .field("changed_group_count", &self.grouped_deltas.len())
            .finish()
    }
}

/// Evaluates one commit through the catalog-owned normalized event view.
///
/// Irrelevant events and empty commits return an empty delta set. Every source
/// event is normalized by catalog before any filter, key, or measure expression
/// runs. This function performs no storage access and is shared by live catch-up
/// and recovery replay.
pub fn evaluate_projection_commit(
    resolved: &ResolvedProjectionPlan,
    schema: CheckedProjectionSchema,
    generation: ProjectionGeneration,
    commit: &StoredCommitRecordV1,
) -> Result<EvaluatedProjectionCommit, ProjectionEvaluationError> {
    let plan = resolved.projection_plan();
    if resolved.identity() != schema.identity()
        || resolved.identity().projection_id() != plan.projection_id()
        || resolved.identity().plan_hash() != plan.plan_hash()
    {
        return Err(ProjectionEvaluationError::new(
            ProjectionEvaluationErrorKind::PlanOrSchemaUnavailable,
        ));
    }

    let transaction_date = transaction_date(commit.logical_time())?;
    let mut grouped_deltas: BTreeMap<ProjectionGroupKey, CanonicalRecord> = BTreeMap::new();
    let mut grouped_event_counts: BTreeMap<ProjectionGroupKey, u64> = BTreeMap::new();
    let mut grouped_mean_totals: BTreeMap<(ProjectionGroupKey, FieldId), i128> = BTreeMap::new();
    let measures = plan.measures().to_vec();
    // Every event materialized below reports this same resolved plan, so the
    // expression arena is identical for the whole pass and the memo cache is
    // allocated once rather than once per event. Each batch clears the entries
    // it recorded, on both start and drop, so no value crosses between events.
    let mut evaluator = ExpressionEvaluator::new(plan.expressions());
    for event in commit
        .events()
        .iter()
        .filter(|event| event.event_type_id() == plan.source_event())
    {
        let view = resolved
            .materialize_event(commit.plan(), event)
            .map_err(|error| {
                ProjectionEvaluationError::new(match error.kind() {
                    ProjectionEventMaterializationErrorKind::Integrity => {
                        ProjectionEvaluationErrorKind::MalformedDurableEvent
                    }
                    ProjectionEventMaterializationErrorKind::HardLimit => {
                        ProjectionEvaluationErrorKind::HardLimitExceeded
                    }
                })
            })?;
        let Some((key, delta)) = evaluate_materialized_event(
            &view,
            &schema,
            generation,
            commit.logical_time(),
            transaction_date,
            &mut evaluator,
        )?
        else {
            continue;
        };

        if let Some(current) = grouped_deltas.get_mut(&key) {
            let count = grouped_event_counts.get_mut(&key).ok_or_else(|| {
                ProjectionEvaluationError::new(
                    ProjectionEvaluationErrorKind::ProjectionStateIntegrity,
                )
            })?;
            *count = count.checked_add(1).ok_or_else(|| {
                ProjectionEvaluationError::new(ProjectionEvaluationErrorKind::ArithmeticOverflow)
            })?;
            *current = combine_commit_measure_records(
                &key,
                &measures,
                current,
                &delta,
                *count,
                &mut grouped_mean_totals,
            )?;
        } else {
            if grouped_deltas.len() == MAX_PROJECTION_ROW_UPDATES {
                return Err(ProjectionEvaluationError::new(
                    ProjectionEvaluationErrorKind::HardLimitExceeded,
                ));
            }
            for (measure, (field_id, value)) in measures.iter().zip(delta.fields()) {
                if measure.aggregation() == ProjectionAggregation::Mean {
                    grouped_mean_totals.insert((key.clone(), *field_id), value_to_i128(value)?);
                }
            }
            grouped_event_counts.insert(key.clone(), 1);
            grouped_deltas.insert(key, delta);
        }
    }

    Ok(EvaluatedProjectionCommit {
        schema,
        generation,
        sequence: commit.commit_sequence(),
        measures,
        grouped_deltas,
        grouped_event_counts,
        grouped_mean_totals,
    })
}

/// Combines evaluated deltas with one storage-atomic row/frontier snapshot.
pub fn prepare_projection_apply(
    evaluated: &EvaluatedProjectionCommit,
    reader: &impl ProjectionApplySnapshotReader,
) -> Result<ProjectionApplyRequestV1, ProjectionEvaluationError> {
    let snapshot_request = ProjectionApplySnapshotRequest::new(
        evaluated.schema.clone(),
        evaluated.generation,
        evaluated.grouped_deltas.keys().cloned().collect(),
    )
    .map_err(map_storage_value_error)?;
    let snapshot = reader
        .read_apply_snapshot(&snapshot_request)
        .map_err(map_storage_error)?;
    let mut updates = Vec::with_capacity(evaluated.grouped_deltas.len());
    for (observation, (key, delta)) in snapshot.rows().iter().zip(&evaluated.grouped_deltas) {
        if observation.key() != key {
            return Err(ProjectionEvaluationError::new(
                ProjectionEvaluationErrorKind::ProjectionStateIntegrity,
            ));
        }
        let (prior, measures) = match observation {
            ProjectionApplyRowObservation::Absent(_) => (ProjectionRowPrior::Absent, delta.clone()),
            ProjectionApplyRowObservation::Present(row) => (
                ProjectionRowPrior::Present(row.last_changed_sequence()),
                evaluated.combine_with_prior(key, row.measures())?,
            ),
        };
        updates.push(
            ProjectionRowUpdateV1::new(evaluated.schema(), key.clone(), prior, measures)
                .map_err(map_storage_value_error)?,
        );
    }
    ProjectionApplyRequestV1::new(
        evaluated.schema.clone(),
        evaluated.generation,
        evaluated.sequence,
        snapshot.expected_frontier(),
        updates,
    )
    .map_err(map_storage_value_error)
}

/// Runs the shared pure evaluator and prepares one atomic request.
pub fn evaluate_and_prepare_projection_commit(
    resolved: &ResolvedProjectionPlan,
    schema: CheckedProjectionSchema,
    generation: ProjectionGeneration,
    commit: &StoredCommitRecordV1,
    reader: &impl ProjectionApplySnapshotReader,
) -> Result<ProjectionApplyRequestV1, ProjectionEvaluationError> {
    let evaluated = evaluate_projection_commit(resolved, schema, generation, commit)?;
    prepare_projection_apply(&evaluated, reader)
}

fn evaluate_materialized_event(
    view: &ProjectionEventMaterializationView<'_, '_>,
    schema: &CheckedProjectionSchema,
    generation: ProjectionGeneration,
    logical_time: LogicalTime,
    transaction_date: Date,
    evaluator: &mut ExpressionEvaluator<'_>,
) -> Result<Option<(ProjectionGroupKey, CanonicalRecord)>, ProjectionEvaluationError> {
    let plan = view.projection_plan();
    let values = ProjectionExpressionValues {
        payload: view.known_payload(),
        logical_time,
        transaction_date,
    };
    let mut batch = evaluator.batch(&values);
    if let Some(filter) = plan.filter()
        && !batch
            .evaluate_predicate(filter)
            .map_err(map_evaluation_error)?
    {
        return Ok(None);
    }

    let group_values = plan
        .key_expressions()
        .iter()
        .map(|expression| batch.evaluate(*expression).map_err(map_evaluation_error))
        .collect::<Result<Vec<_>, _>>()?;
    let key = schema
        .group_key(generation, &group_values)
        .map_err(map_storage_value_error)?;

    let fields = plan
        .measures()
        .iter()
        .map(|measure| {
            let value = match measure.aggregation() {
                ProjectionAggregation::Count => CanonicalValue::U64(1),
                ProjectionAggregation::Sum
                | ProjectionAggregation::Min
                | ProjectionAggregation::Max
                | ProjectionAggregation::Mean => batch
                    .evaluate(measure.expression().ok_or_else(|| {
                        ProjectionEvaluationError::new(
                            ProjectionEvaluationErrorKind::PlanOrSchemaUnavailable,
                        )
                    })?)
                    .map_err(map_evaluation_error)?,
            };
            Ok((measure.field().id(), value))
        })
        .collect::<Result<Vec<_>, ProjectionEvaluationError>>()?;
    let measures = CanonicalRecord::new(fields).map_err(|_| {
        ProjectionEvaluationError::new(ProjectionEvaluationErrorKind::PlanOrSchemaUnavailable)
    })?;
    schema
        .validate_measure_record(&measures)
        .map_err(map_storage_value_error)?;
    Ok(Some((key, measures)))
}

fn combine_commit_measure_records(
    key: &ProjectionGroupKey,
    measures: &[ProjectionMeasurePlan],
    current: &CanonicalRecord,
    delta: &CanonicalRecord,
    event_count: u64,
    mean_totals: &mut BTreeMap<(ProjectionGroupKey, FieldId), i128>,
) -> Result<CanonicalRecord, ProjectionEvaluationError> {
    if current.fields().len() != delta.fields().len() || current.fields().len() != measures.len() {
        return Err(ProjectionEvaluationError::new(
            ProjectionEvaluationErrorKind::ProjectionStateIntegrity,
        ));
    }
    let mut fields = Vec::with_capacity(measures.len());
    for ((measure, (current_id, current_val)), (delta_id, delta_val)) in
        measures.iter().zip(current.fields()).zip(delta.fields())
    {
        if current_id != delta_id || *current_id != measure.field().id() {
            return Err(ProjectionEvaluationError::new(
                ProjectionEvaluationErrorKind::ProjectionStateIntegrity,
            ));
        }
        let next = match measure.aggregation() {
            ProjectionAggregation::Count | ProjectionAggregation::Sum => {
                checked_add(current_val, delta_val)?
            }
            ProjectionAggregation::Min => checked_min(current_val, delta_val)?,
            ProjectionAggregation::Max => checked_max(current_val, delta_val)?,
            ProjectionAggregation::Mean => {
                let total = mean_totals
                    .get_mut(&(key.clone(), *current_id))
                    .ok_or_else(|| {
                        ProjectionEvaluationError::new(
                            ProjectionEvaluationErrorKind::ProjectionStateIntegrity,
                        )
                    })?;
                *total = total
                    .checked_add(value_to_i128(delta_val)?)
                    .ok_or_else(|| {
                        ProjectionEvaluationError::new(
                            ProjectionEvaluationErrorKind::ArithmeticOverflow,
                        )
                    })?;
                i128_to_mean_value(*total, event_count, current_val)?
            }
        };
        fields.push((*current_id, next));
    }
    CanonicalRecord::new(fields).map_err(|_| {
        ProjectionEvaluationError::new(ProjectionEvaluationErrorKind::ProjectionStateIntegrity)
    })
}

fn checked_add(
    current: &CanonicalValue,
    delta: &CanonicalValue,
) -> Result<CanonicalValue, ProjectionEvaluationError> {
    let value = match (current, delta) {
        (CanonicalValue::I64(current), CanonicalValue::I64(delta)) => {
            current.checked_add(*delta).map(CanonicalValue::I64)
        }
        (CanonicalValue::U64(current), CanonicalValue::U64(delta)) => {
            current.checked_add(*delta).map(CanonicalValue::U64)
        }
        (CanonicalValue::Decimal(current), CanonicalValue::Decimal(delta)) => current
            .checked_add(*delta)
            .ok()
            .map(CanonicalValue::Decimal),
        (CanonicalValue::Money(current), CanonicalValue::Money(delta)) => {
            current.checked_add(*delta).ok().map(CanonicalValue::Money)
        }
        _ => {
            return Err(ProjectionEvaluationError::new(
                ProjectionEvaluationErrorKind::ProjectionStateIntegrity,
            ));
        }
    };
    value.ok_or_else(|| {
        ProjectionEvaluationError::new(ProjectionEvaluationErrorKind::ArithmeticOverflow)
    })
}

fn checked_min(
    current: &CanonicalValue,
    delta: &CanonicalValue,
) -> Result<CanonicalValue, ProjectionEvaluationError> {
    match (current, delta) {
        (CanonicalValue::I64(current), CanonicalValue::I64(delta)) => {
            Ok(CanonicalValue::I64((*current).min(*delta)))
        }
        (CanonicalValue::U64(current), CanonicalValue::U64(delta)) => {
            Ok(CanonicalValue::U64((*current).min(*delta)))
        }
        (CanonicalValue::Decimal(current), CanonicalValue::Decimal(delta))
            if current.spec() == delta.spec() =>
        {
            riffdb_types::Decimal::new(
                current.spec(),
                current.coefficient().min(delta.coefficient()),
            )
            .map(CanonicalValue::Decimal)
            .map_err(|_| {
                ProjectionEvaluationError::new(
                    ProjectionEvaluationErrorKind::ProjectionStateIntegrity,
                )
            })
        }
        (CanonicalValue::Money(current), CanonicalValue::Money(delta))
            if current.currency() == delta.currency()
                && current.amount().spec() == delta.amount().spec() =>
        {
            let amount = riffdb_types::Decimal::new(
                current.amount().spec(),
                current
                    .amount()
                    .coefficient()
                    .min(delta.amount().coefficient()),
            )
            .map_err(|_| {
                ProjectionEvaluationError::new(
                    ProjectionEvaluationErrorKind::ProjectionStateIntegrity,
                )
            })?;
            Ok(CanonicalValue::Money(riffdb_types::Money::new(
                current.currency(),
                amount,
            )))
        }
        _ => Err(ProjectionEvaluationError::new(
            ProjectionEvaluationErrorKind::ProjectionStateIntegrity,
        )),
    }
}

fn checked_max(
    current: &CanonicalValue,
    delta: &CanonicalValue,
) -> Result<CanonicalValue, ProjectionEvaluationError> {
    match (current, delta) {
        (CanonicalValue::I64(current), CanonicalValue::I64(delta)) => {
            Ok(CanonicalValue::I64((*current).max(*delta)))
        }
        (CanonicalValue::U64(current), CanonicalValue::U64(delta)) => {
            Ok(CanonicalValue::U64((*current).max(*delta)))
        }
        (CanonicalValue::Decimal(current), CanonicalValue::Decimal(delta))
            if current.spec() == delta.spec() =>
        {
            riffdb_types::Decimal::new(
                current.spec(),
                current.coefficient().max(delta.coefficient()),
            )
            .map(CanonicalValue::Decimal)
            .map_err(|_| {
                ProjectionEvaluationError::new(
                    ProjectionEvaluationErrorKind::ProjectionStateIntegrity,
                )
            })
        }
        (CanonicalValue::Money(current), CanonicalValue::Money(delta))
            if current.currency() == delta.currency()
                && current.amount().spec() == delta.amount().spec() =>
        {
            let amount = riffdb_types::Decimal::new(
                current.amount().spec(),
                current
                    .amount()
                    .coefficient()
                    .max(delta.amount().coefficient()),
            )
            .map_err(|_| {
                ProjectionEvaluationError::new(
                    ProjectionEvaluationErrorKind::ProjectionStateIntegrity,
                )
            })?;
            Ok(CanonicalValue::Money(riffdb_types::Money::new(
                current.currency(),
                amount,
            )))
        }
        _ => Err(ProjectionEvaluationError::new(
            ProjectionEvaluationErrorKind::ProjectionStateIntegrity,
        )),
    }
}

fn value_to_i128(value: &CanonicalValue) -> Result<i128, ProjectionEvaluationError> {
    match value {
        CanonicalValue::I64(v) => Ok(i128::from(*v)),
        CanonicalValue::U64(v) => Ok(i128::from(*v)),
        CanonicalValue::Decimal(v) => Ok(v.coefficient()),
        CanonicalValue::Money(v) => Ok(v.amount().coefficient()),
        _ => Err(ProjectionEvaluationError::new(
            ProjectionEvaluationErrorKind::ProjectionStateIntegrity,
        )),
    }
}

fn i128_to_mean_value(
    total: i128,
    count: u64,
    prototype: &CanonicalValue,
) -> Result<CanonicalValue, ProjectionEvaluationError> {
    let divisor = i128::from(count);
    if divisor <= 0 {
        return Err(ProjectionEvaluationError::new(
            ProjectionEvaluationErrorKind::ProjectionStateIntegrity,
        ));
    }
    let mean = total.checked_div(divisor).ok_or_else(|| {
        ProjectionEvaluationError::new(ProjectionEvaluationErrorKind::ArithmeticOverflow)
    })?;
    match prototype {
        CanonicalValue::I64(_) => i64::try_from(mean).map(CanonicalValue::I64).map_err(|_| {
            ProjectionEvaluationError::new(ProjectionEvaluationErrorKind::ArithmeticOverflow)
        }),
        CanonicalValue::U64(_) => u64::try_from(mean).map(CanonicalValue::U64).map_err(|_| {
            ProjectionEvaluationError::new(ProjectionEvaluationErrorKind::ArithmeticOverflow)
        }),
        CanonicalValue::Decimal(d) => riffdb_types::Decimal::new(d.spec(), mean)
            .map(CanonicalValue::Decimal)
            .map_err(|_| {
                ProjectionEvaluationError::new(ProjectionEvaluationErrorKind::ArithmeticOverflow)
            }),
        CanonicalValue::Money(m) => riffdb_types::Decimal::new(m.amount().spec(), mean)
            .map(|amount| CanonicalValue::Money(riffdb_types::Money::new(m.currency(), amount)))
            .map_err(|_| {
                ProjectionEvaluationError::new(ProjectionEvaluationErrorKind::ArithmeticOverflow)
            }),
        _ => Err(ProjectionEvaluationError::new(
            ProjectionEvaluationErrorKind::ProjectionStateIntegrity,
        )),
    }
}

fn transaction_date(logical_time: LogicalTime) -> Result<Date, ProjectionEvaluationError> {
    let days = logical_time
        .timestamp()
        .seconds()
        .div_euclid(SECONDS_PER_DAY);
    i32::try_from(days)
        .map(Date::from_days_since_unix_epoch)
        .map_err(|_| {
            ProjectionEvaluationError::new(ProjectionEvaluationErrorKind::ArithmeticOverflow)
        })
}

fn map_evaluation_error(error: EvaluationError) -> ProjectionEvaluationError {
    ProjectionEvaluationError::new(match error {
        EvaluationError::Arithmetic => ProjectionEvaluationErrorKind::ArithmeticOverflow,
        EvaluationError::Integrity => ProjectionEvaluationErrorKind::PlanOrSchemaUnavailable,
    })
}

pub(crate) fn map_storage_error(error: StorageError) -> ProjectionEvaluationError {
    ProjectionEvaluationError::new(match error.kind() {
        StorageErrorKind::Unavailable => ProjectionEvaluationErrorKind::StorageUnavailable,
        StorageErrorKind::CommitStatusUnknown => ProjectionEvaluationErrorKind::CommitStatusUnknown,
        StorageErrorKind::LimitExceeded => ProjectionEvaluationErrorKind::HardLimitExceeded,
        StorageErrorKind::CorruptData
        | StorageErrorKind::IncompatibleFormat
        | StorageErrorKind::InvariantViolation
        | StorageErrorKind::SequenceExhausted
        | StorageErrorKind::HistoryPruned => {
            ProjectionEvaluationErrorKind::ProjectionStateIntegrity
        }
    })
}

pub(crate) fn map_storage_value_error(error: StorageValueError) -> ProjectionEvaluationError {
    ProjectionEvaluationError::new(match error {
        StorageValueError::LimitExceeded | StorageValueError::SizeOverflow => {
            ProjectionEvaluationErrorKind::HardLimitExceeded
        }
        StorageValueError::Empty
        | StorageValueError::InvalidShape
        | StorageValueError::Duplicate
        | StorageValueError::NonCanonicalOrder
        | StorageValueError::IdentityMismatch => {
            ProjectionEvaluationErrorKind::ProjectionStateIntegrity
        }
    })
}

struct ProjectionExpressionValues<'payload> {
    payload: &'payload CanonicalRecord,
    logical_time: LogicalTime,
    transaction_date: Date,
}

impl ExpressionValueSource for ProjectionExpressionValues<'_> {
    fn source_event_field(&self, field: FieldId) -> Option<CanonicalValue> {
        self.payload
            .fields()
            .binary_search_by_key(&field, |(candidate, _)| *candidate)
            .ok()
            .map(|index| self.payload.fields()[index].1.clone())
    }

    fn transaction_time(&self) -> Option<LogicalTime> {
        Some(self.logical_time)
    }

    fn transaction_date(&self) -> Option<Date> {
        Some(self.transaction_date)
    }
}
