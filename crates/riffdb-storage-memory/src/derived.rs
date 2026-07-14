//! In-memory outbox and projection storage ports.

use riffdb_storage_api::{
    EncodedPageItem, OutboxClaimV1, OutboxDeadLetterV1, OutboxPageLimit, OutboxRenewV1,
    OutboxRepository, OutboxRetryV1, OutboxStatusObservationV1, OutboxStatusReadResultV1,
    OutboxSucceedV1, OutboxTransitionResultV1, PendingOutboxItemV1, PendingOutboxScanV1,
    ProjectionApplyRequestV1, ProjectionApplyResult, ProjectionApplyRowObservation,
    ProjectionApplySnapshot, ProjectionApplySnapshotBuilder, ProjectionApplySnapshotReader,
    ProjectionApplySnapshotRequest, ProjectionControlOperation, ProjectionControlResult,
    ProjectionLifecycleV1, ProjectionLowerContinuation, ProjectionMutationRepository,
    ProjectionQueryReader, ProjectionQueryRequest, ProjectionQueryResult, ProjectionRowPrior,
    ProjectionStatus, ProjectionUnavailableReason, StorageError, StorageErrorKind,
    StorageValueError, StoredOutboxStatusV1, StoredProjectionApplyV1, StoredProjectionControlV1,
    StoredProjectionStateV1, evaluate_projection_control_operation,
};
use riffdb_types::{
    CommitSequence, EventId, FrontierPosition, ProjectionApplyKey, ProjectionIdentity,
};

use crate::state::{
    MemoryState, OutboxStatusCasPreparation, PreparedMemoryDelta, PreparedOutboxStatusCas,
    PreparedProjectionControlCas, ProjectionControlCasPreparation, memory_composite_charge,
    memory_record_charge, unique_binary_search_by,
};
use crate::store::{MemoryOperationalPorts, storage_error};

impl OutboxRepository for MemoryOperationalPorts {
    fn read_outbox_status(
        &self,
        event_id: EventId,
    ) -> Result<OutboxStatusReadResultV1, StorageError> {
        self.read(|state| read_outbox_observation(state, event_id))
    }

    fn scan_pending_outbox(
        &self,
        after: Option<EventId>,
        limit: OutboxPageLimit,
    ) -> Result<PendingOutboxScanV1, StorageError> {
        self.read(|state| {
            let start = after.map_or(Ok(0), |after| {
                let equal_start = state
                    .pending_outbox_events
                    .partition_point(|event_id| *event_id < after);
                let after_equal = state
                    .pending_outbox_events
                    .partition_point(|event_id| *event_id <= after);
                if after_equal.saturating_sub(equal_start) > 1 {
                    return Err(storage_error(StorageErrorKind::CorruptData));
                }
                Ok(after_equal)
            })?;
            let requested = usize::from(limit.get().get());
            let end = start
                .checked_add(requested)
                .map_or(state.pending_outbox_events.len(), |end| {
                    end.min(state.pending_outbox_events.len())
                });
            let checked_start = start.saturating_sub(1);
            let checked_end = end.saturating_add(1).min(state.pending_outbox_events.len());
            if state.pending_outbox_events[checked_start..checked_end]
                .windows(2)
                .any(|pair| pair[0] >= pair[1])
            {
                return Err(storage_error(StorageErrorKind::CorruptData));
            }
            let mut items = Vec::with_capacity(end.saturating_sub(start));
            for event_id in &state.pending_outbox_events[start..end] {
                let (event, intent, status) = reciprocal_outbox_item(state, *event_id)?
                    .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
                if !status.is_pending() {
                    return Err(storage_error(StorageErrorKind::CorruptData));
                }
                let record_count = if matches!(status, OutboxStatusObservationV1::Present(_)) {
                    3
                } else {
                    2
                };
                let item =
                    PendingOutboxItemV1::new(event, intent, status).map_err(stored_value_error)?;
                items.push(EncodedPageItem::new(
                    item,
                    memory_composite_charge(record_count)?,
                ));
            }
            PendingOutboxScanV1::page(items, end < state.pending_outbox_events.len())
                .map_err(stored_value_error)
        })
    }

    fn claim_outbox(
        &mut self,
        transition: &OutboxClaimV1,
    ) -> Result<OutboxTransitionResultV1, StorageError> {
        self.transition_outbox(
            transition.event_id(),
            transition.expected(),
            transition.updated(),
        )
    }

    fn renew_outbox(
        &mut self,
        transition: &OutboxRenewV1,
    ) -> Result<OutboxTransitionResultV1, StorageError> {
        self.transition_outbox(
            transition.event_id(),
            &OutboxStatusObservationV1::Present(transition.expected().clone()),
            transition.updated(),
        )
    }

    fn succeed_outbox(
        &mut self,
        transition: &OutboxSucceedV1,
    ) -> Result<OutboxTransitionResultV1, StorageError> {
        self.transition_outbox(
            transition.event_id(),
            &OutboxStatusObservationV1::Present(transition.expected().clone()),
            transition.updated(),
        )
    }

    fn retry_outbox(
        &mut self,
        transition: &OutboxRetryV1,
    ) -> Result<OutboxTransitionResultV1, StorageError> {
        self.transition_outbox(
            transition.event_id(),
            &OutboxStatusObservationV1::Present(transition.expected().clone()),
            transition.updated(),
        )
    }

    fn dead_letter_outbox(
        &mut self,
        transition: &OutboxDeadLetterV1,
    ) -> Result<OutboxTransitionResultV1, StorageError> {
        self.transition_outbox(
            transition.event_id(),
            transition.expected(),
            transition.updated(),
        )
    }
}

impl MemoryOperationalPorts {
    fn transition_outbox(
        &self,
        event_id: EventId,
        expected: &OutboxStatusObservationV1,
        updated: &StoredOutboxStatusV1,
    ) -> Result<OutboxTransitionResultV1, StorageError> {
        self.apply_prepared(|state| {
            // The status CAS helper protects event/intent reciprocity. This
            // read additionally proves the enclosing authoritative commit and
            // all derived sidecars before any mutation is prepared.
            reciprocal_outbox_item(state, event_id)?;
            let prepared = state.prepare_outbox_status_cas(
                event_id,
                expected,
                updated.clone(),
                memory_record_charge(),
            )?;
            let accelerator = unique_binary_search_by(&state.pending_outbox_events, |candidate| {
                candidate.cmp(&event_id)
            })?;
            match prepared {
                OutboxStatusCasPreparation::Apply(status) => {
                    let current_pending = expected.is_pending();
                    if current_pending != accelerator.is_ok() {
                        return Err(storage_error(StorageErrorKind::CorruptData));
                    }
                    let next_pending = updated.state().is_pending();
                    let pending = match (current_pending, next_pending, accelerator) {
                        (true, false, Ok(index)) => PendingOutboxDelta::Remove(index),
                        (false, true, Err(index)) => PendingOutboxDelta::Insert(index, event_id),
                        (true, true, Ok(_)) | (false, false, Err(_)) => PendingOutboxDelta::Keep,
                        _ => return Err(storage_error(StorageErrorKind::CorruptData)),
                    };
                    Ok(PreparedOutboxDelta::Apply { status, pending })
                }
                OutboxStatusCasPreparation::NoChange(result) => {
                    let effective_pending = match &result {
                        OutboxTransitionResultV1::StateChanged(observed) => observed.is_pending(),
                        OutboxTransitionResultV1::AuthoritativeIntentMissing => false,
                        OutboxTransitionResultV1::Applied(_) => {
                            return Err(storage_error(StorageErrorKind::InvariantViolation));
                        }
                    };
                    if effective_pending != accelerator.is_ok() {
                        return Err(storage_error(StorageErrorKind::CorruptData));
                    }
                    Ok(PreparedOutboxDelta::NoChange(result))
                }
            }
        })
    }
}

enum PendingOutboxDelta {
    Keep,
    Insert(usize, EventId),
    Remove(usize),
}

enum PreparedOutboxDelta {
    Apply {
        status: PreparedOutboxStatusCas,
        pending: PendingOutboxDelta,
    },
    NoChange(OutboxTransitionResultV1),
}

impl PreparedMemoryDelta for PreparedOutboxDelta {
    type Output = OutboxTransitionResultV1;

    fn apply(self, state: &mut MemoryState) -> Self::Output {
        match self {
            Self::Apply { status, pending } => {
                let result = status.apply(state);
                match pending {
                    PendingOutboxDelta::Keep => {}
                    PendingOutboxDelta::Insert(index, event_id) => {
                        state.pending_outbox_events.insert(index, event_id);
                    }
                    PendingOutboxDelta::Remove(index) => {
                        state.pending_outbox_events.remove(index);
                    }
                }
                result
            }
            Self::NoChange(result) => result,
        }
    }
}

impl ProjectionApplySnapshotReader for MemoryOperationalPorts {
    fn read_apply_snapshot(
        &self,
        request: &ProjectionApplySnapshotRequest,
    ) -> Result<ProjectionApplySnapshot, StorageError> {
        self.read(|state| {
            let control = find_projection_control(state, request.schema().identity())?
                .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
            if !control.permits_application(request.generation()) {
                return Err(storage_error(StorageErrorKind::InvariantViolation));
            }
            let frontier = control
                .frontier_for(request.generation())
                .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
            let mut snapshot = ProjectionApplySnapshotBuilder::new(request, frontier)
                .map_err(stored_value_error)?;
            for key in request.group_keys() {
                let observation = match unique_binary_search_by(&state.projection_states, |row| {
                    row.key().cmp(key)
                })? {
                    Ok(index) => ProjectionApplyRowObservation::Present(
                        state.projection_states[index].clone(),
                    ),
                    Err(_) => ProjectionApplyRowObservation::Absent(key.clone()),
                };
                snapshot.push_row(observation).map_err(stored_value_error)?;
            }
            snapshot.finish().map_err(stored_value_error)
        })
    }
}

impl ProjectionMutationRepository for MemoryOperationalPorts {
    fn apply_projection(
        &mut self,
        request: &ProjectionApplyRequestV1,
    ) -> Result<ProjectionApplyResult, StorageError> {
        self.apply_prepared(|state| prepare_projection_apply(state, request))
    }

    fn transition_projection_control(
        &mut self,
        operation: ProjectionControlOperation,
    ) -> Result<ProjectionControlResult, StorageError> {
        self.apply_prepared(|state| {
            let identity = projection_operation_identity(&operation);
            let current = find_projection_control(state, identity)?;
            let result = evaluate_projection_control_operation(
                current,
                &operation,
                authoritative_head(state),
            )
            .map_err(request_value_error)?;
            let ProjectionControlResult::Updated(updated) = result else {
                return Ok(PreparedProjectionControlDelta::NoChange(result));
            };
            let expected = match &operation {
                ProjectionControlOperation::CreateInitial { .. } => None,
                ProjectionControlOperation::StartInitialScan { expected }
                | ProjectionControlOperation::AllocateRebuild { expected }
                | ProjectionControlOperation::PublishCandidate { expected }
                | ProjectionControlOperation::RecordFailure { expected, .. }
                | ProjectionControlOperation::RecoverDegraded { expected }
                | ProjectionControlOperation::MarkInvalid { expected } => Some(expected),
            };
            match state.prepare_projection_control_cas(
                identity,
                expected,
                updated,
                memory_record_charge(),
            )? {
                ProjectionControlCasPreparation::Apply(update) => {
                    Ok(PreparedProjectionControlDelta::Apply(update))
                }
                ProjectionControlCasPreparation::Existing(existing) => {
                    Ok(PreparedProjectionControlDelta::NoChange(
                        ProjectionControlResult::AlreadyInitialized(existing),
                    ))
                }
                ProjectionControlCasPreparation::StateChanged => Ok(
                    PreparedProjectionControlDelta::NoChange(ProjectionControlResult::StateChanged),
                ),
            }
        })
    }
}

enum PreparedProjectionControlDelta {
    Apply(PreparedProjectionControlCas),
    NoChange(ProjectionControlResult),
}

impl PreparedMemoryDelta for PreparedProjectionControlDelta {
    type Output = ProjectionControlResult;

    fn apply(self, state: &mut MemoryState) -> Self::Output {
        match self {
            Self::Apply(update) => ProjectionControlResult::Updated(update.apply(state)),
            Self::NoChange(result) => result,
        }
    }
}

enum PreparedRowPosition {
    Replace(usize),
    Insert(usize),
}

struct PreparedProjectionApplyDelta {
    rows: Vec<(PreparedRowPosition, StoredProjectionStateV1)>,
    marker_position: usize,
    marker: StoredProjectionApplyV1,
    control_index: usize,
    control: StoredProjectionControlV1,
}

enum PreparedProjectionApply {
    Apply(PreparedProjectionApplyDelta),
    NoChange(ProjectionApplyResult),
}

impl PreparedMemoryDelta for PreparedProjectionApply {
    type Output = ProjectionApplyResult;

    fn apply(self, state: &mut MemoryState) -> Self::Output {
        match self {
            Self::Apply(delta) => {
                for (position, row) in delta.rows.into_iter().rev() {
                    match position {
                        PreparedRowPosition::Replace(index) => {
                            state.projection_states[index] = row;
                        }
                        PreparedRowPosition::Insert(index) => {
                            state.projection_states.insert(index, row);
                        }
                    }
                }
                state
                    .projection_applies
                    .insert(delta.marker_position, delta.marker.clone());
                state.projection_controls[delta.control_index] = delta.control.clone();
                ProjectionApplyResult::Applied {
                    marker: delta.marker,
                    control: delta.control,
                }
            }
            Self::NoChange(result) => result,
        }
    }
}

fn prepare_projection_apply(
    state: &MemoryState,
    request: &ProjectionApplyRequestV1,
) -> Result<PreparedProjectionApply, StorageError> {
    require_authoritative_commit(state, request.sequence())?;
    let canonical = ProjectionApplyRequestV1::new(
        request.schema().clone(),
        request.generation(),
        request.sequence(),
        request.expected_frontier(),
        request.row_updates().to_vec(),
    )
    .map_err(request_value_error)?;
    if canonical.apply_hash() != request.apply_hash() {
        return Err(storage_error(StorageErrorKind::InvariantViolation));
    }

    if find_projection_control(state, request.identity())?.is_none() {
        return Ok(PreparedProjectionApply::NoChange(
            ProjectionApplyResult::StateChanged,
        ));
    }
    let control_index = unique_binary_search_by(&state.projection_controls, |control| {
        control.identity().cmp(request.identity())
    })?
    .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
    let control = &state.projection_controls[control_index];
    let retained = control
        .frontier_for(request.generation())
        .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;

    if sequence_is_at_or_before(request.sequence(), retained) {
        let marker_key = ProjectionApplyKey::new(
            request.identity().clone(),
            request.generation(),
            request.sequence(),
        );
        let marker = find_projection_marker(state, &marker_key)?
            .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
        if marker.canonical_hash() != request.apply_hash() {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        return Ok(PreparedProjectionApply::NoChange(
            ProjectionApplyResult::AlreadyApplied(marker.clone()),
        ));
    }
    if !control.permits_application(request.generation()) || retained != request.expected_frontier()
    {
        return Ok(PreparedProjectionApply::NoChange(
            ProjectionApplyResult::StateChanged,
        ));
    }

    let marker_key = ProjectionApplyKey::new(
        request.identity().clone(),
        request.generation(),
        request.sequence(),
    );
    let marker_position = unique_binary_search_by(&state.projection_applies, |marker| {
        marker.key().cmp(&marker_key)
    })?;
    let Err(marker_position) = marker_position else {
        return Err(storage_error(StorageErrorKind::CorruptData));
    };

    let mut rows = Vec::with_capacity(request.row_updates().len());
    for update in request.row_updates() {
        let position =
            unique_binary_search_by(&state.projection_states, |row| row.key().cmp(update.key()))?;
        match (update.prior(), position) {
            (ProjectionRowPrior::Absent, Err(index)) => {
                rows.push((
                    PreparedRowPosition::Insert(index),
                    projection_post_image(request, update)?,
                ));
            }
            (ProjectionRowPrior::Present(expected), Ok(index))
                if state.projection_states[index].last_changed_sequence() == expected =>
            {
                rows.push((
                    PreparedRowPosition::Replace(index),
                    projection_post_image(request, update)?,
                ));
            }
            (ProjectionRowPrior::Absent | ProjectionRowPrior::Present(_), Ok(_) | Err(_)) => {
                return Ok(PreparedProjectionApply::NoChange(
                    ProjectionApplyResult::StateChanged,
                ));
            }
        }
    }
    let control = control
        .after_apply(
            request.generation(),
            request.expected_frontier(),
            request.sequence(),
        )
        .map_err(request_value_error)?;
    let marker = StoredProjectionApplyV1::new(marker_key, request.apply_hash());
    Ok(PreparedProjectionApply::Apply(
        PreparedProjectionApplyDelta {
            rows,
            marker_position,
            marker,
            control_index,
            control,
        },
    ))
}

fn projection_post_image(
    request: &ProjectionApplyRequestV1,
    update: &riffdb_storage_api::ProjectionRowUpdateV1,
) -> Result<StoredProjectionStateV1, StorageError> {
    StoredProjectionStateV1::new(
        request.schema(),
        update.key().clone(),
        update.measures().clone(),
        request.sequence(),
    )
    .map_err(request_value_error)
}

impl ProjectionQueryReader for MemoryOperationalPorts {
    fn query_projection(
        &self,
        request: &ProjectionQueryRequest,
    ) -> Result<ProjectionQueryResult, StorageError> {
        self.read(|state| query_projection(state, request))
    }

    fn read_projection_status(
        &self,
        identity: &ProjectionIdentity,
    ) -> Result<ProjectionStatus, StorageError> {
        self.read(|state| {
            let head = authoritative_head(state);
            Ok(find_projection_control(state, identity)?.map_or_else(
                || ProjectionStatus::uninitialized(identity.clone(), head),
                |control| ProjectionStatus::from_control(control, head),
            ))
        })
    }
}

fn query_projection(
    state: &MemoryState,
    request: &ProjectionQueryRequest,
) -> Result<ProjectionQueryResult, StorageError> {
    let Some(control) = find_projection_control(state, request.selector().identity())? else {
        return Ok(ProjectionQueryResult::Degraded {
            current: FrontierPosition::BeforeFirst,
            reason: ProjectionUnavailableReason::Building,
        });
    };
    match control.lifecycle() {
        ProjectionLifecycleV1::Building | ProjectionLifecycleV1::CatchingUp => {
            Ok(ProjectionQueryResult::Degraded {
                current: control
                    .candidate()
                    .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?
                    .frontier(),
                reason: ProjectionUnavailableReason::Building,
            })
        }
        ProjectionLifecycleV1::Rebuilding => Ok(ProjectionQueryResult::Degraded {
            current: control
                .published()
                .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?
                .frontier(),
            reason: ProjectionUnavailableReason::Rebuilding,
        }),
        ProjectionLifecycleV1::Degraded => {
            let failure = control
                .failure()
                .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
            let current = control
                .frontier_for(failure.generation())
                .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
            Ok(ProjectionQueryResult::Degraded {
                current,
                reason: ProjectionUnavailableReason::Failure(failure.code()),
            })
        }
        ProjectionLifecycleV1::Invalid => {
            let failure = control
                .failure()
                .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
            Ok(ProjectionQueryResult::Invalid {
                reason: failure.code(),
            })
        }
        ProjectionLifecycleV1::Ready => query_ready_projection(state, request, control),
    }
}

fn query_ready_projection(
    state: &MemoryState,
    request: &ProjectionQueryRequest,
    control: &StoredProjectionControlV1,
) -> Result<ProjectionQueryResult, StorageError> {
    let published = control
        .published()
        .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
    if request.continuation().is_some_and(|continuation| {
        continuation.generation() != published.generation()
            || continuation.observed_frontier() != published.frontier()
    }) {
        return Ok(ProjectionQueryResult::ContinuationInvalidated);
    }
    let prefix = request
        .selector()
        .schema()
        .group_prefix(
            published.generation(),
            request.selector().leading_components(),
        )
        .map_err(request_value_error)?;
    let mut index = state
        .projection_states
        .partition_point(|row| row.key().as_bytes() < prefix.as_bytes());
    if let Some(continuation) = request.continuation() {
        index = state
            .projection_states
            .partition_point(|row| row.key() <= continuation.exclusive_last_key());
    }

    let maximum = usize::from(request.limit().get());
    let mut rows = Vec::with_capacity(maximum);
    while index < state.projection_states.len() && rows.len() < maximum {
        let row = &state.projection_states[index];
        if !row.key().as_bytes().starts_with(prefix.as_bytes()) {
            break;
        }
        if row.identity() != request.selector().identity()
            || row.generation() != published.generation()
            || !sequence_is_at_or_before(row.last_changed_sequence(), published.frontier())
        {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        rows.push(EncodedPageItem::new(row.clone(), memory_record_charge()));
        index += 1;
    }
    let has_more = state
        .projection_states
        .get(index)
        .is_some_and(|row| row.key().as_bytes().starts_with(prefix.as_bytes()));
    let next = if has_more {
        let last = rows
            .last()
            .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
        Some(
            ProjectionLowerContinuation::new(
                request.selector(),
                published.generation(),
                last.value().key().clone(),
                published.frontier(),
            )
            .map_err(stored_value_error)?,
        )
    } else {
        None
    };
    ProjectionQueryResult::ready(
        request,
        published.generation(),
        published.frontier(),
        rows,
        next,
    )
    .map_err(stored_value_error)
}

fn reciprocal_outbox_item(
    state: &MemoryState,
    event_id: EventId,
) -> Result<
    Option<(
        riffdb_storage_api::StoredDurableEventV1,
        riffdb_storage_api::StoredOutboxIntentV1,
        OutboxStatusObservationV1,
    )>,
    StorageError,
> {
    let event = unique_binary_search_by(&state.events, |event| event.event_id().cmp(&event_id))?;
    let intent = unique_binary_search_by(&state.outbox_intents, |intent| {
        intent.event_id().cmp(&event_id)
    })?;
    let status = unique_binary_search_by(&state.outbox_statuses, |status| {
        status.event_id().cmp(&event_id)
    })?;
    let status_charge = unique_binary_search_by(&state.synthetic_charges.outbox_statuses, |row| {
        row.key.cmp(&event_id)
    })?;
    if status.is_ok() != status_charge.is_ok() {
        return Err(storage_error(StorageErrorKind::CorruptData));
    }
    if status.is_ok() {
        require_status_charge(state, event_id)?;
    }

    let commit = unique_binary_search_by(&state.commits, |commit| {
        commit.commit_sequence().cmp(&event_id.commit_sequence())
    })?;
    let event_ordinal = usize::try_from(event_id.event_ordinal())
        .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
    match (event, intent) {
        (Err(_), Err(_)) => {
            if status.is_ok()
                || commit.is_ok_and(|index| {
                    state.commits[index].events().get(event_ordinal).is_some()
                        || state.commits[index]
                            .outbox_event_ids()
                            .get(event_ordinal)
                            .is_some()
                })
            {
                return Err(storage_error(StorageErrorKind::CorruptData));
            }
            Ok(None)
        }
        (Ok(event), Ok(intent)) if state.outbox_intents[intent].event() == &state.events[event] => {
            let Ok(commit) = commit else {
                return Err(storage_error(StorageErrorKind::CorruptData));
            };
            if state.commits[commit].events().get(event_ordinal) != Some(&state.events[event])
                || state.commits[commit].outbox_event_ids().get(event_ordinal) != Some(&event_id)
            {
                return Err(storage_error(StorageErrorKind::CorruptData));
            }
            let status = match status {
                Ok(index) => {
                    OutboxStatusObservationV1::Present(state.outbox_statuses[index].clone())
                }
                Err(_) => OutboxStatusObservationV1::AbsentInitialPending,
            };
            Ok(Some((
                state.events[event].clone(),
                state.outbox_intents[intent].clone(),
                status,
            )))
        }
        (Ok(_) | Err(_), Ok(_) | Err(_)) => Err(storage_error(StorageErrorKind::CorruptData)),
    }
}

fn read_outbox_observation(
    state: &MemoryState,
    event_id: EventId,
) -> Result<OutboxStatusReadResultV1, StorageError> {
    let Some((_, _, status)) = reciprocal_outbox_item(state, event_id)? else {
        if state.pending_outbox_events.binary_search(&event_id).is_ok() {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        return Ok(OutboxStatusReadResultV1::AuthoritativeIntentMissing);
    };
    let pending = unique_binary_search_by(&state.pending_outbox_events, |candidate| {
        candidate.cmp(&event_id)
    })?
    .is_ok();
    if pending != status.is_pending() {
        return Err(storage_error(StorageErrorKind::CorruptData));
    }
    Ok(OutboxStatusReadResultV1::Status(status))
}

fn require_status_charge(state: &MemoryState, event_id: EventId) -> Result<(), StorageError> {
    let position = unique_binary_search_by(&state.synthetic_charges.outbox_statuses, |row| {
        row.key.cmp(&event_id)
    })?;
    let Ok(index) = position else {
        return Err(storage_error(StorageErrorKind::CorruptData));
    };
    if state.synthetic_charges.outbox_statuses[index]
        .charge
        .encoded_content_charge()
        != memory_record_charge()
    {
        return Err(storage_error(StorageErrorKind::CorruptData));
    }
    Ok(())
}

fn find_projection_control<'a>(
    state: &'a MemoryState,
    identity: &ProjectionIdentity,
) -> Result<Option<&'a StoredProjectionControlV1>, StorageError> {
    let control = unique_binary_search_by(&state.projection_controls, |control| {
        control.identity().cmp(identity)
    })?;
    let charge = unique_binary_search_by(&state.synthetic_charges.projection_controls, |row| {
        row.key.cmp(identity)
    })?;
    if control.is_ok() != charge.is_ok() {
        return Err(storage_error(StorageErrorKind::CorruptData));
    }
    match control {
        Ok(index) => {
            let Ok(charge) = charge else {
                return Err(storage_error(StorageErrorKind::CorruptData));
            };
            if state.synthetic_charges.projection_controls[charge]
                .charge
                .encoded_content_charge()
                != memory_record_charge()
            {
                return Err(storage_error(StorageErrorKind::CorruptData));
            }
            Ok(Some(&state.projection_controls[index]))
        }
        Err(_) => Ok(None),
    }
}

fn find_projection_marker<'a>(
    state: &'a MemoryState,
    key: &ProjectionApplyKey,
) -> Result<Option<&'a StoredProjectionApplyV1>, StorageError> {
    unique_binary_search_by(&state.projection_applies, |marker| marker.key().cmp(key))
        .map(|position| position.ok().map(|index| &state.projection_applies[index]))
}

fn projection_operation_identity(operation: &ProjectionControlOperation) -> &ProjectionIdentity {
    match operation {
        ProjectionControlOperation::CreateInitial { schema } => schema.identity(),
        ProjectionControlOperation::StartInitialScan { expected }
        | ProjectionControlOperation::AllocateRebuild { expected }
        | ProjectionControlOperation::PublishCandidate { expected }
        | ProjectionControlOperation::RecordFailure { expected, .. }
        | ProjectionControlOperation::RecoverDegraded { expected }
        | ProjectionControlOperation::MarkInvalid { expected } => expected.identity(),
    }
}

fn authoritative_head(state: &MemoryState) -> FrontierPosition {
    state
        .commits
        .last()
        .map_or(FrontierPosition::BeforeFirst, |commit| {
            FrontierPosition::AppliedThrough(commit.commit_sequence())
        })
}

fn require_authoritative_commit(
    state: &MemoryState,
    sequence: CommitSequence,
) -> Result<(), StorageError> {
    if unique_binary_search_by(&state.commits, |commit| {
        commit.commit_sequence().cmp(&sequence)
    })?
    .is_err()
    {
        return Err(storage_error(StorageErrorKind::CorruptData));
    }
    Ok(())
}

fn sequence_is_at_or_before(sequence: CommitSequence, frontier: FrontierPosition) -> bool {
    matches!(frontier, FrontierPosition::AppliedThrough(applied) if sequence <= applied)
}

fn request_value_error(error: StorageValueError) -> StorageError {
    let kind = match error {
        StorageValueError::LimitExceeded | StorageValueError::SizeOverflow => {
            StorageErrorKind::LimitExceeded
        }
        StorageValueError::Empty
        | StorageValueError::NonCanonicalOrder
        | StorageValueError::Duplicate
        | StorageValueError::IdentityMismatch
        | StorageValueError::InvalidShape => StorageErrorKind::InvariantViolation,
    };
    storage_error(kind)
}

fn stored_value_error(error: StorageValueError) -> StorageError {
    let kind = match error {
        StorageValueError::LimitExceeded | StorageValueError::SizeOverflow => {
            StorageErrorKind::LimitExceeded
        }
        StorageValueError::Empty
        | StorageValueError::NonCanonicalOrder
        | StorageValueError::Duplicate
        | StorageValueError::IdentityMismatch
        | StorageValueError::InvalidShape => StorageErrorKind::CorruptData,
    };
    storage_error(kind)
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU16, NonZeroU32};

    use riffdb_storage_api::{
        DeclaredOutcome, DurabilityMode, ExecutablePlanRef, OutboxDeliveryStateV1,
        OutboxDestinationIdV1, ProjectionFailureCodeV1, ProjectionFailureV1,
        ProjectionGenerationPosition, ProjectionQuerySelector, ProjectionRowUpdateV1,
        PublishedApplyModeV1, ReadDependencies, StoredCommitRecordV1, StoredDurableEventV1,
        StoredOutboxIntentV1, StoredReadDependenciesV1, derive_event_hash_v1,
    };
    use riffdb_types::{
        ActorId, ActorKind, AdmittedActorContext, AggregateTypeId, CanonicalInputHash,
        CanonicalRecord, CanonicalValue, CommandId, ContractBundleHash, ContractLineage,
        ContractVersion, Date, Decimal, DecimalSpec, EventTypeId, FieldId, LogicalTime, OutcomeId,
        PartitionKeyBuilder, PlanHash, ProjectionGeneration, ProjectionId, ProjectionPlanHash,
        ProvenanceId, RequestId, TenantScope, Timestamp, hash_partition_key,
    };

    use super::*;
    use crate::startup::MemoryDormantPorts;
    use crate::state::{KeyedSyntheticCharge, SyntheticRecordCharge};
    use crate::store::MemoryStore;

    fn uuid_bytes(fill: u8) -> [u8; 16] {
        let mut bytes = [fill; 16];
        bytes[6] = 0x70 | (fill & 0x0f);
        bytes[8] = 0x80 | (fill & 0x3f);
        bytes
    }

    fn operational_ports() -> MemoryOperationalPorts {
        MemoryDormantPorts {
            store: MemoryStore::new(),
        }
        .into_operational()
    }

    fn with_state(ports: &MemoryOperationalPorts, operation: impl FnOnce(&mut MemoryState)) {
        let access = ports.acquire().expect("acquire memory state");
        access
            .write(|state| {
                operation(state);
                Ok(())
            })
            .expect("mutate test state");
    }

    fn projection_identity() -> ProjectionIdentity {
        ProjectionIdentity::new(
            ContractLineage::new("memory-derived").expect("lineage"),
            ProjectionId::first(),
            ProjectionPlanHash::from_bytes([0x31; 32]),
        )
    }

    fn generation(value: u64) -> ProjectionGeneration {
        ProjectionGeneration::new(value).expect("nonzero generation")
    }

    fn sequence(value: u64) -> CommitSequence {
        CommitSequence::new(value).expect("nonzero commit sequence")
    }

    fn timestamp(seconds: i64) -> Timestamp {
        Timestamp::new(seconds, 0).expect("timestamp")
    }

    fn destination() -> OutboxDestinationIdV1 {
        OutboxDestinationIdV1::new("primary").expect("destination")
    }

    fn command_graph_at(
        sequence: CommitSequence,
        event_count: u32,
    ) -> (StoredCommitRecordV1, Vec<StoredDurableEventV1>) {
        let plan = ExecutablePlanRef::new(
            ContractLineage::new("memory-derived").expect("lineage"),
            ContractVersion::new(1).expect("contract version"),
            ContractBundleHash::from_bytes([0x21; 32]),
            CommandId::first(),
            PlanHash::from_bytes([0x22; 32]),
        );
        let actor = AdmittedActorContext::new(
            ActorId::new("maintainer").expect("actor"),
            ActorKind::Human,
            TenantScope::Global,
            None,
        );
        let sequence_byte = u8::try_from(sequence.get()).unwrap_or(0x40);
        let request_id =
            RequestId::from_bytes(uuid_bytes(sequence_byte.wrapping_add(0x11))).expect("request");
        let provenance_id = ProvenanceId::from_bytes(uuid_bytes(sequence_byte.wrapping_add(0x21)))
            .expect("provenance");
        let logical_time = LogicalTime::new(timestamp(42));
        let mut partition = PartitionKeyBuilder::new(AggregateTypeId::first());
        partition.push_u64(1).expect("partition component");
        let partition_hash = hash_partition_key(partition.finish().expect("partition").as_bytes());
        let events = (0..event_count)
            .map(|ordinal| {
                let event_id = EventId::new(sequence, ordinal);
                let event_type_id = EventTypeId::first();
                let payload = CanonicalRecord::new(Vec::new()).expect("event payload");
                let event_hash =
                    derive_event_hash_v1(event_id, event_type_id, &payload).expect("event hash");
                StoredDurableEventV1::new(event_id, event_type_id, payload, event_hash)
                    .expect("event")
            })
            .collect::<Vec<_>>();
        let event_ids = events
            .iter()
            .map(StoredDurableEventV1::event_id)
            .collect::<Vec<_>>();
        let outcome = DeclaredOutcome::new(
            OutcomeId::first(),
            CanonicalRecord::new(Vec::new()).expect("outcome record"),
        )
        .expect("declared outcome");
        let read_dependencies = StoredReadDependenciesV1::from_live(
            &ReadDependencies::new(Vec::new()).expect("empty read dependencies"),
        )
        .expect("stored read dependencies");
        let commit = StoredCommitRecordV1::new(
            sequence,
            request_id,
            plan,
            CanonicalInputHash::from_bytes([0x32; 32]),
            actor,
            logical_time,
            partition_hash,
            Vec::new(),
            read_dependencies,
            Vec::new(),
            events.clone(),
            outcome,
            provenance_id,
            event_ids,
            DurabilityMode::Memory,
        )
        .expect("commit");
        (commit, events)
    }

    fn seed_outbox(ports: &MemoryOperationalPorts, event_count: u32) -> Vec<EventId> {
        let (commit, events) = command_graph_at(CommitSequence::first(), event_count);
        let event_ids = events
            .iter()
            .map(StoredDurableEventV1::event_id)
            .collect::<Vec<_>>();
        with_state(ports, |state| {
            state.commits.push(commit);
            state
                .outbox_intents
                .extend(events.iter().cloned().map(StoredOutboxIntentV1::new));
            state.events.extend(events);
            state
                .pending_outbox_events
                .extend(event_ids.iter().copied());
        });
        event_ids
    }

    fn seed_empty_commit(ports: &MemoryOperationalPorts, sequence: CommitSequence) {
        let (commit, events) = command_graph_at(sequence, 0);
        assert!(events.is_empty());
        with_state(ports, |state| state.commits.push(commit));
    }

    fn budget_group_values(organization: u8, day: i32) -> Vec<CanonicalValue> {
        vec![
            CanonicalValue::Uuid([organization; 16]),
            CanonicalValue::I64(2026),
            CanonicalValue::Date(Date::new(day)),
        ]
    }

    fn budget_measures(coefficient: i128) -> CanonicalRecord {
        let spec = DecimalSpec::new(28, 2).expect("budget decimal spec");
        CanonicalRecord::new(vec![(
            FieldId::first(),
            CanonicalValue::Decimal(
                Decimal::new(spec, coefficient).expect("bounded projection measure"),
            ),
        )])
        .expect("projection measures")
    }

    fn install_control(ports: &MemoryOperationalPorts, control: StoredProjectionControlV1) {
        let identity = control.identity().clone();
        with_state(ports, |state| {
            state.projection_controls.push(control);
            state
                .synthetic_charges
                .projection_controls
                .push(KeyedSyntheticCharge {
                    key: identity,
                    charge: SyntheticRecordCharge::new(memory_record_charge()),
                });
        });
    }

    #[test]
    fn implicit_pending_scan_is_bounded_ordered_and_exact_end() {
        let ports = operational_ports();
        let event_ids = seed_outbox(&ports, 3);
        assert_eq!(
            ports
                .read_outbox_status(event_ids[0])
                .expect("read implicit pending"),
            OutboxStatusReadResultV1::Status(OutboxStatusObservationV1::AbsentInitialPending)
        );

        let limit =
            OutboxPageLimit::new(NonZeroU16::new(2).expect("nonzero")).expect("bounded limit");
        let first = ports
            .scan_pending_outbox(None, limit)
            .expect("first pending page");
        let PendingOutboxScanV1::Page { items, next_after } = first else {
            panic!("three rows at limit two must produce a page");
        };
        assert_eq!(next_after, event_ids[1]);
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].value().event_id(), event_ids[0]);
        assert_eq!(items[1].value().event_id(), event_ids[1]);
        assert!(items.iter().all(|item| {
            item.encoded_content_charge().get() == 2
                && matches!(
                    item.value().status(),
                    OutboxStatusObservationV1::AbsentInitialPending
                )
        }));

        let final_page = ports
            .scan_pending_outbox(Some(next_after), limit)
            .expect("final pending page");
        let PendingOutboxScanV1::ExactEnd { items } = final_page else {
            panic!("last row must reach exact end");
        };
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].value().event_id(), event_ids[2]);

        assert_eq!(
            ports
                .scan_pending_outbox(Some(event_ids[2]), limit)
                .expect("empty exact end"),
            PendingOutboxScanV1::ExactEnd { items: Vec::new() }
        );
    }

    #[test]
    fn outbox_cas_updates_status_charge_and_pending_accelerator_atomically() {
        let mut ports = operational_ports();
        let event_id = seed_outbox(&ports, 1)[0];
        let claim = OutboxClaimV1::new(
            event_id,
            OutboxStatusObservationV1::AbsentInitialPending,
            destination(),
            timestamp(10),
            timestamp(20),
        )
        .expect("claim");
        let OutboxTransitionResultV1::Applied(delivering) =
            ports.claim_outbox(&claim).expect("apply claim")
        else {
            panic!("claim must apply");
        };
        assert!(matches!(
            delivering.state(),
            OutboxDeliveryStateV1::Delivering { attempt, .. } if attempt.get() == 1
        ));
        with_state(&ports, |state| {
            assert_eq!(state.outbox_statuses, vec![delivering.clone()]);
            assert!(state.pending_outbox_events.is_empty());
            assert_eq!(state.synthetic_charges.outbox_statuses.len(), 1);
            assert_eq!(
                state.synthetic_charges.outbox_statuses[0]
                    .charge
                    .encoded_content_charge(),
                memory_record_charge()
            );
        });

        assert_eq!(
            ports.claim_outbox(&claim).expect("stale claim"),
            OutboxTransitionResultV1::StateChanged(OutboxStatusObservationV1::Present(
                delivering.clone()
            ))
        );
        let retry = OutboxRetryV1::new(delivering.clone(), Some(timestamp(30)), None)
            .expect("retry transition");
        let OutboxTransitionResultV1::Applied(pending) =
            ports.retry_outbox(&retry).expect("apply retry")
        else {
            panic!("retry must apply");
        };
        let page = ports
            .scan_pending_outbox(
                None,
                OutboxPageLimit::new(NonZeroU16::new(1).expect("nonzero")).expect("limit"),
            )
            .expect("explicit pending page");
        let PendingOutboxScanV1::ExactEnd { items } = page else {
            panic!("one pending item must reach exact end");
        };
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].encoded_content_charge().get(), 3);
        assert_eq!(
            items[0].value().status(),
            &OutboxStatusObservationV1::Present(pending.clone())
        );
        with_state(&ports, |state| {
            assert_eq!(state.pending_outbox_events, vec![event_id]);
            assert_eq!(state.outbox_statuses, vec![pending]);
            assert_eq!(state.synthetic_charges.outbox_statuses.len(), 1);
        });
    }

    #[test]
    fn renew_success_and_dead_letter_preserve_exact_cas_semantics() {
        let mut ports = operational_ports();
        let event_id = seed_outbox(&ports, 1)[0];
        let claim = OutboxClaimV1::new(
            event_id,
            OutboxStatusObservationV1::AbsentInitialPending,
            destination(),
            timestamp(10),
            timestamp(20),
        )
        .expect("claim");
        let OutboxTransitionResultV1::Applied(delivering) =
            ports.claim_outbox(&claim).expect("claim")
        else {
            panic!("claim must apply");
        };
        let renew = OutboxRenewV1::new(delivering.clone(), timestamp(25)).expect("renew");
        let OutboxTransitionResultV1::Applied(renewed) =
            ports.renew_outbox(&renew).expect("apply renewal")
        else {
            panic!("renewal must apply");
        };
        let stale_success = riffdb_storage_api::OutboxSucceedV1::new(delivering, timestamp(26))
            .expect("stale success request");
        assert_eq!(
            ports.succeed_outbox(&stale_success).expect("stale success"),
            OutboxTransitionResultV1::StateChanged(OutboxStatusObservationV1::Present(
                renewed.clone()
            ))
        );
        let success =
            riffdb_storage_api::OutboxSucceedV1::new(renewed, timestamp(27)).expect("success");
        let OutboxTransitionResultV1::Applied(delivered) =
            ports.succeed_outbox(&success).expect("apply success")
        else {
            panic!("success must apply");
        };
        assert!(matches!(
            delivered.state(),
            OutboxDeliveryStateV1::Delivered { attempts, .. } if attempts.get() == 1
        ));
        with_state(&ports, |state| {
            assert!(state.pending_outbox_events.is_empty());
            assert_eq!(state.outbox_statuses, vec![delivered]);
        });

        let mut dead_letter_ports = operational_ports();
        let dead_letter_event = seed_outbox(&dead_letter_ports, 1)[0];
        let dead_letter = OutboxDeadLetterV1::new(
            dead_letter_event,
            OutboxStatusObservationV1::AbsentInitialPending,
            Some(destination()),
            timestamp(50),
            None,
        )
        .expect("dead letter");
        assert!(matches!(
            dead_letter_ports
                .dead_letter_outbox(&dead_letter)
                .expect("apply dead letter"),
            OutboxTransitionResultV1::Applied(status)
                if matches!(status.state(), OutboxDeliveryStateV1::DeadLetter { attempts: 0, .. })
        ));
        with_state(&dead_letter_ports, |state| {
            assert!(state.pending_outbox_events.is_empty());
            assert_eq!(state.outbox_statuses.len(), 1);
            assert_eq!(state.synthetic_charges.outbox_statuses.len(), 1);
        });
    }

    #[test]
    fn missing_or_nonreciprocal_outbox_authority_never_creates_status() {
        let mut missing_ports = operational_ports();
        let missing_event = EventId::new(CommitSequence::first(), 0);
        let claim = OutboxClaimV1::new(
            missing_event,
            OutboxStatusObservationV1::AbsentInitialPending,
            destination(),
            timestamp(10),
            timestamp(20),
        )
        .expect("claim");
        assert_eq!(
            missing_ports
                .claim_outbox(&claim)
                .expect("missing authority result"),
            OutboxTransitionResultV1::AuthoritativeIntentMissing
        );
        with_state(&missing_ports, |state| {
            assert!(state.outbox_statuses.is_empty());
            assert!(state.synthetic_charges.outbox_statuses.is_empty());
            assert!(state.pending_outbox_events.is_empty());
        });

        let corrupt_ports = operational_ports();
        let event_id = seed_outbox(&corrupt_ports, 1)[0];
        with_state(&corrupt_ports, |state| {
            state.commits.clear();
        });
        assert_eq!(
            corrupt_ports
                .read_outbox_status(event_id)
                .expect_err("orphan event and intent must fail")
                .kind(),
            StorageErrorKind::CorruptData
        );
    }

    #[test]
    fn outbox_sidecar_or_accelerator_corruption_fails_closed() {
        let mut ports = operational_ports();
        let event_id = seed_outbox(&ports, 1)[0];
        let claim = OutboxClaimV1::new(
            event_id,
            OutboxStatusObservationV1::AbsentInitialPending,
            destination(),
            timestamp(10),
            timestamp(20),
        )
        .expect("claim");
        ports.claim_outbox(&claim).expect("apply claim");
        with_state(&ports, |state| {
            state.synthetic_charges.outbox_statuses.clear();
        });
        assert_eq!(
            ports
                .read_outbox_status(event_id)
                .expect_err("missing status charge must fail")
                .kind(),
            StorageErrorKind::CorruptData
        );

        let duplicate_ports = operational_ports();
        let duplicate_event = seed_outbox(&duplicate_ports, 1)[0];
        with_state(&duplicate_ports, |state| {
            state.pending_outbox_events.push(duplicate_event);
        });
        assert_eq!(
            duplicate_ports
                .scan_pending_outbox(
                    None,
                    OutboxPageLimit::new(NonZeroU16::new(1).expect("nonzero")).expect("limit"),
                )
                .expect_err("duplicate accelerator must fail")
                .kind(),
            StorageErrorKind::CorruptData
        );
    }

    #[test]
    fn projection_control_cas_and_status_share_authoritative_state() {
        let mut ports = operational_ports();
        seed_outbox(&ports, 1);
        let identity = projection_identity();
        let initial = StoredProjectionControlV1::initial(identity.clone());
        install_control(&ports, initial.clone());

        let status = ports
            .read_projection_status(&identity)
            .expect("projection status");
        assert_eq!(status.lifecycle(), ProjectionLifecycleV1::Building);
        assert_eq!(status.candidate(), initial.candidate());
        assert_eq!(status.authoritative_head(), applied_frontier(1));

        let result = ports
            .transition_projection_control(ProjectionControlOperation::StartInitialScan {
                expected: initial.clone(),
            })
            .expect("start initial scan");
        let ProjectionControlResult::Updated(catching_up) = result else {
            panic!("initial scan transition must update");
        };
        assert_eq!(catching_up.lifecycle(), ProjectionLifecycleV1::CatchingUp);
        assert_eq!(
            ports
                .transition_projection_control(ProjectionControlOperation::StartInitialScan {
                    expected: initial,
                })
                .expect("stale control CAS"),
            ProjectionControlResult::StateChanged
        );
        with_state(&ports, |state| {
            assert_eq!(state.projection_controls, vec![catching_up.clone()]);
            assert_eq!(state.synthetic_charges.projection_controls.len(), 1);
            assert_eq!(
                state.synthetic_charges.projection_controls[0]
                    .charge
                    .encoded_content_charge(),
                memory_record_charge()
            );
        });

        assert_eq!(
            ports
                .transition_projection_control(ProjectionControlOperation::PublishCandidate {
                    expected: catching_up,
                })
                .expect_err("before-first candidate is behind authoritative head")
                .kind(),
            StorageErrorKind::InvariantViolation
        );
    }

    #[test]
    fn projection_failure_and_invalid_transitions_are_atomic() {
        let mut ports = operational_ports();
        let identity = projection_identity();
        let initial = StoredProjectionControlV1::initial(identity.clone());
        install_control(&ports, initial.clone());
        let failure = ProjectionFailureV1::new(
            ProjectionGeneration::first(),
            ProjectionFailureCodeV1::PlanOrSchemaUnavailable,
            None,
        );
        let ProjectionControlResult::Updated(degraded) = ports
            .transition_projection_control(ProjectionControlOperation::RecordFailure {
                expected: initial,
                failure: failure.clone(),
            })
            .expect("record failure")
        else {
            panic!("failure must update control");
        };
        assert_eq!(degraded.lifecycle(), ProjectionLifecycleV1::Degraded);
        assert_eq!(degraded.failure(), Some(&failure));

        let ProjectionControlResult::Updated(invalid) = ports
            .transition_projection_control(ProjectionControlOperation::MarkInvalid {
                expected: degraded,
            })
            .expect("mark invalid")
        else {
            panic!("invalid transition must update control");
        };
        assert_eq!(invalid.lifecycle(), ProjectionLifecycleV1::Invalid);
        assert_eq!(invalid.failure(), Some(&failure));
        assert_eq!(
            ports
                .transition_projection_control(ProjectionControlOperation::RecoverDegraded {
                    expected: invalid,
                })
                .expect_err("invalid has no recovery edge")
                .kind(),
            StorageErrorKind::InvariantViolation
        );
        with_state(&ports, |state| {
            assert_eq!(state.projection_controls.len(), 1);
            assert_eq!(
                state.projection_controls[0].lifecycle(),
                ProjectionLifecycleV1::Invalid
            );
            assert_eq!(state.synthetic_charges.projection_controls.len(), 1);
        });
    }

    #[test]
    fn projection_status_maps_absence_and_rejects_orphan_charge() {
        let ports = operational_ports();
        let identity = projection_identity();
        let status = ports
            .read_projection_status(&identity)
            .expect("uninitialized status");
        assert_eq!(status.lifecycle(), ProjectionLifecycleV1::Building);
        assert_eq!(status.published(), None);
        assert_eq!(status.candidate(), None);
        assert_eq!(status.authoritative_head(), FrontierPosition::BeforeFirst);

        with_state(&ports, |state| {
            state
                .synthetic_charges
                .projection_controls
                .push(KeyedSyntheticCharge {
                    key: identity.clone(),
                    charge: SyntheticRecordCharge::new(memory_record_charge()),
                });
        });
        assert_eq!(
            ports
                .read_projection_status(&identity)
                .expect_err("orphan projection charge must fail")
                .kind(),
            StorageErrorKind::CorruptData
        );
    }

    #[test]
    fn projection_apply_query_and_continuation_are_one_atomic_semantic_path() {
        let schema = riffdb_testkit::model::budget_projection_schema();
        let identity = schema.identity().clone();
        let mut ports = operational_ports();
        seed_empty_commit(&ports, sequence(1));
        let catching_up = StoredProjectionControlV1::new(
            identity,
            generation(1),
            None,
            Some(ProjectionGenerationPosition::new(
                generation(1),
                FrontierPosition::BeforeFirst,
            )),
            None,
            ProjectionLifecycleV1::CatchingUp,
            None,
        )
        .expect("catching-up control");
        install_control(&ports, catching_up.clone());

        let mut keys = [
            schema
                .group_key(generation(1), &budget_group_values(1, 20_001))
                .expect("second projection key"),
            schema
                .group_key(generation(1), &budget_group_values(1, 20_000))
                .expect("first projection key"),
        ];
        keys.sort_unstable();
        let snapshot_request =
            ProjectionApplySnapshotRequest::new(schema.clone(), generation(1), keys.to_vec())
                .expect("snapshot request");
        let snapshot = ports
            .read_apply_snapshot(&snapshot_request)
            .expect("before-first apply snapshot");
        assert_eq!(snapshot.expected_frontier(), FrontierPosition::BeforeFirst);
        assert!(
            snapshot
                .rows()
                .iter()
                .all(|row| matches!(row, ProjectionApplyRowObservation::Absent(_)))
        );

        let first_updates = keys
            .iter()
            .enumerate()
            .map(|(index, key)| {
                ProjectionRowUpdateV1::new(
                    &schema,
                    key.clone(),
                    ProjectionRowPrior::Absent,
                    budget_measures(i128::try_from(index + 1).expect("small coefficient") * 100),
                )
                .expect("projection row update")
            })
            .collect::<Vec<_>>();
        let first_apply = ProjectionApplyRequestV1::new(
            schema.clone(),
            generation(1),
            sequence(1),
            FrontierPosition::BeforeFirst,
            first_updates,
        )
        .expect("first apply request");
        let mut missing_commit_ports = operational_ports();
        install_control(&missing_commit_ports, catching_up.clone());
        assert_eq!(
            missing_commit_ports
                .apply_projection(&first_apply)
                .expect_err("missing authoritative commit must reject apply")
                .kind(),
            StorageErrorKind::CorruptData
        );
        with_state(&missing_commit_ports, |state| {
            assert!(state.projection_states.is_empty());
            assert!(state.projection_applies.is_empty());
            assert_eq!(
                state.projection_controls[0].frontier_for(generation(1)),
                Some(FrontierPosition::BeforeFirst)
            );
        });
        let ProjectionApplyResult::Applied {
            marker,
            control: applied_control,
        } = ports
            .apply_projection(&first_apply)
            .expect("apply first sequence")
        else {
            panic!("first sequence must apply");
        };
        assert_eq!(marker.canonical_hash(), first_apply.apply_hash());
        assert_eq!(
            applied_control.frontier_for(generation(1)),
            Some(applied_frontier(1))
        );
        assert_eq!(
            ports
                .apply_projection(&first_apply)
                .expect("equal duplicate apply"),
            ProjectionApplyResult::AlreadyApplied(marker)
        );

        let mismatched_duplicate = ProjectionApplyRequestV1::new(
            schema.clone(),
            generation(1),
            sequence(1),
            FrontierPosition::BeforeFirst,
            keys.iter()
                .map(|key| {
                    ProjectionRowUpdateV1::new(
                        &schema,
                        key.clone(),
                        ProjectionRowPrior::Absent,
                        budget_measures(999),
                    )
                    .expect("mismatched duplicate row")
                })
                .collect(),
        )
        .expect("mismatched duplicate request");
        assert_eq!(
            ports
                .apply_projection(&mismatched_duplicate)
                .expect_err("unequal duplicate hash must fail")
                .kind(),
            StorageErrorKind::CorruptData
        );

        let ProjectionControlResult::Updated(ready) = ports
            .transition_projection_control(ProjectionControlOperation::PublishCandidate {
                expected: applied_control,
            })
            .expect("publish caught-up candidate")
        else {
            panic!("publication must update control");
        };
        assert_eq!(ready.lifecycle(), ProjectionLifecycleV1::Ready);

        let selector =
            ProjectionQuerySelector::new(schema.clone(), vec![CanonicalValue::Uuid([1; 16])])
                .expect("organization prefix selector");
        assert!(
            ProjectionQueryRequest::new(
                selector.clone(),
                NonZeroU16::new(500).expect("nonzero"),
                None,
            )
            .is_ok()
        );
        assert_eq!(
            ProjectionQueryRequest::new(
                selector.clone(),
                NonZeroU16::new(501).expect("nonzero"),
                None,
            ),
            Err(StorageValueError::LimitExceeded)
        );
        let first_query = ProjectionQueryRequest::new(
            selector.clone(),
            NonZeroU16::new(1).expect("nonzero"),
            None,
        )
        .expect("first query");
        let ProjectionQueryResult::Ready {
            generation: observed_generation,
            frontier,
            rows,
            next,
        } = ports
            .query_projection(&first_query)
            .expect("first ready query page")
        else {
            panic!("published projection must be ready");
        };
        assert_eq!(observed_generation, generation(1));
        assert_eq!(frontier, applied_frontier(1));
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].value().key(), &keys[0]);
        assert_eq!(rows[0].encoded_content_charge(), memory_record_charge());
        let continuation = *next.expect("second row requires continuation");

        let second_query = ProjectionQueryRequest::new(
            selector.clone(),
            NonZeroU16::new(1).expect("nonzero"),
            Some(continuation.clone()),
        )
        .expect("continuation query");
        let ProjectionQueryResult::Ready { rows, next, .. } = ports
            .query_projection(&second_query)
            .expect("second ready query page")
        else {
            panic!("second page must remain ready");
        };
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].value().key(), &keys[1]);
        assert_eq!(next, None);

        let exact_selector =
            ProjectionQuerySelector::new(schema.clone(), budget_group_values(1, 20_000))
                .expect("complete-key selector");
        let exact_request = ProjectionQueryRequest::new(
            exact_selector,
            NonZeroU16::new(10).expect("nonzero"),
            None,
        )
        .expect("exact projection request");
        let ProjectionQueryResult::Ready { rows, next, .. } = ports
            .query_projection(&exact_request)
            .expect("exact projection query")
        else {
            panic!("exact query must be ready");
        };
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].value().key(), &keys[0]);
        assert_eq!(next, None);

        seed_empty_commit(&ports, sequence(2));
        let stale_prior = ProjectionApplyRequestV1::new(
            schema.clone(),
            generation(1),
            sequence(2),
            applied_frontier(1),
            vec![
                ProjectionRowUpdateV1::new(
                    &schema,
                    keys[0].clone(),
                    ProjectionRowPrior::Absent,
                    budget_measures(300),
                )
                .expect("stale row prior"),
            ],
        )
        .expect("stale-prior request");
        assert_eq!(
            ports
                .apply_projection(&stale_prior)
                .expect("stale prior result"),
            ProjectionApplyResult::StateChanged
        );
        with_state(&ports, |state| {
            assert_eq!(state.projection_applies.len(), 1);
            assert_eq!(
                state.projection_controls[0].frontier_for(generation(1)),
                Some(applied_frontier(1))
            );
            assert_eq!(
                state.projection_states[0].last_changed_sequence(),
                sequence(1)
            );
        });

        let second_apply = ProjectionApplyRequestV1::new(
            schema.clone(),
            generation(1),
            sequence(2),
            applied_frontier(1),
            vec![
                ProjectionRowUpdateV1::new(
                    &schema,
                    keys[0].clone(),
                    ProjectionRowPrior::Present(sequence(1)),
                    budget_measures(300),
                )
                .expect("matching row prior"),
            ],
        )
        .expect("second apply request");
        assert!(matches!(
            ports
                .apply_projection(&second_apply)
                .expect("apply second sequence"),
            ProjectionApplyResult::Applied { .. }
        ));
        assert_eq!(
            ports
                .query_projection(&second_query)
                .expect("stale continuation result"),
            ProjectionQueryResult::ContinuationInvalidated
        );

        let post_apply_snapshot = ports
            .read_apply_snapshot(&snapshot_request)
            .expect("post-apply snapshot");
        assert_eq!(post_apply_snapshot.expected_frontier(), applied_frontier(2));
        assert_eq!(post_apply_snapshot.rows().len(), 2);
        assert!(matches!(
            &post_apply_snapshot.rows()[0],
            ProjectionApplyRowObservation::Present(row)
                if row.last_changed_sequence() == sequence(2)
        ));

        with_state(&ports, |state| {
            state.projection_controls[0] = StoredProjectionControlV1::new(
                schema.identity().clone(),
                generation(2),
                Some(ProjectionGenerationPosition::new(
                    generation(2),
                    applied_frontier(2),
                )),
                None,
                Some(PublishedApplyModeV1::Enabled),
                ProjectionLifecycleV1::Ready,
                None,
            )
            .expect("replacement generation control");
        });
        assert_eq!(
            ports
                .apply_projection(&first_apply)
                .expect_err("retired generation apply must fail")
                .kind(),
            StorageErrorKind::InvariantViolation
        );
    }

    #[test]
    fn projection_query_maps_every_unavailable_lifecycle_without_rows() {
        let schema = riffdb_testkit::model::budget_projection_schema();
        let selector =
            ProjectionQuerySelector::new(schema.clone(), Vec::new()).expect("all-groups selector");
        let request =
            ProjectionQueryRequest::new(selector, NonZeroU16::new(10).expect("nonzero"), None)
                .expect("projection query");

        let absent = operational_ports();
        assert_eq!(
            absent.query_projection(&request).expect("absent control"),
            ProjectionQueryResult::Degraded {
                current: FrontierPosition::BeforeFirst,
                reason: ProjectionUnavailableReason::Building,
            }
        );

        let building = operational_ports();
        install_control(
            &building,
            StoredProjectionControlV1::initial(schema.identity().clone()),
        );
        assert_eq!(
            building
                .query_projection(&request)
                .expect("building control"),
            ProjectionQueryResult::Degraded {
                current: FrontierPosition::BeforeFirst,
                reason: ProjectionUnavailableReason::Building,
            }
        );

        let rebuilding = operational_ports();
        install_control(
            &rebuilding,
            StoredProjectionControlV1::new(
                schema.identity().clone(),
                generation(2),
                Some(ProjectionGenerationPosition::new(
                    generation(1),
                    applied_frontier(1),
                )),
                Some(ProjectionGenerationPosition::new(
                    generation(2),
                    FrontierPosition::BeforeFirst,
                )),
                Some(PublishedApplyModeV1::Enabled),
                ProjectionLifecycleV1::Rebuilding,
                None,
            )
            .expect("rebuilding control"),
        );
        assert_eq!(
            rebuilding
                .query_projection(&request)
                .expect("rebuilding control"),
            ProjectionQueryResult::Degraded {
                current: applied_frontier(1),
                reason: ProjectionUnavailableReason::Rebuilding,
            }
        );

        let failure = ProjectionFailureV1::new(
            generation(1),
            ProjectionFailureCodeV1::PlanOrSchemaUnavailable,
            None,
        );
        let degraded = operational_ports();
        install_control(
            &degraded,
            StoredProjectionControlV1::new(
                schema.identity().clone(),
                generation(1),
                None,
                Some(ProjectionGenerationPosition::new(
                    generation(1),
                    FrontierPosition::BeforeFirst,
                )),
                None,
                ProjectionLifecycleV1::Degraded,
                Some(failure.clone()),
            )
            .expect("degraded control"),
        );
        assert_eq!(
            degraded
                .query_projection(&request)
                .expect("degraded control"),
            ProjectionQueryResult::Degraded {
                current: FrontierPosition::BeforeFirst,
                reason: ProjectionUnavailableReason::Failure(failure.code()),
            }
        );

        let invalid = operational_ports();
        install_control(
            &invalid,
            StoredProjectionControlV1::new(
                schema.identity().clone(),
                generation(1),
                None,
                Some(ProjectionGenerationPosition::new(
                    generation(1),
                    FrontierPosition::BeforeFirst,
                )),
                None,
                ProjectionLifecycleV1::Invalid,
                Some(failure.clone()),
            )
            .expect("invalid control"),
        );
        assert_eq!(
            invalid.query_projection(&request).expect("invalid control"),
            ProjectionQueryResult::Invalid {
                reason: failure.code(),
            }
        );
    }

    fn applied_frontier(value: u64) -> FrontierPosition {
        FrontierPosition::AppliedThrough(sequence(value))
    }

    #[test]
    fn retained_published_failure_suspends_application_in_memory() {
        let mut ports = operational_ports();
        let identity = projection_identity();
        let ready = StoredProjectionControlV1::new(
            identity,
            generation(1),
            Some(ProjectionGenerationPosition::new(
                generation(1),
                applied_frontier(1),
            )),
            None,
            Some(PublishedApplyModeV1::Enabled),
            ProjectionLifecycleV1::Ready,
            None,
        )
        .expect("ready control");
        install_control(&ports, ready.clone());
        let failure = ProjectionFailureV1::new(
            generation(1),
            ProjectionFailureCodeV1::ProjectionStateIntegrity,
            Some(sequence(1)),
        );
        let ProjectionControlResult::Updated(degraded) = ports
            .transition_projection_control(ProjectionControlOperation::RecordFailure {
                expected: ready,
                failure,
            })
            .expect("record published failure")
        else {
            panic!("published failure must update control");
        };
        assert_eq!(
            degraded.published_apply_mode(),
            Some(PublishedApplyModeV1::Suspended)
        );
        assert!(!degraded.permits_application(generation(1)));
    }

    #[test]
    fn outbox_attempt_numbers_never_use_zero() {
        let ports = operational_ports();
        let event_id = seed_outbox(&ports, 1)[0];
        let claim = OutboxClaimV1::new(
            event_id,
            OutboxStatusObservationV1::AbsentInitialPending,
            destination(),
            timestamp(1),
            timestamp(2),
        )
        .expect("claim");
        assert_eq!(
            claim.updated().state().attempts(),
            NonZeroU32::new(1).expect("nonzero").get()
        );
    }
}
