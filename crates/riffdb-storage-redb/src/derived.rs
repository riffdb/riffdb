//! Durable outbox and projection ports over the frozen redb layout.

use std::ops::Bound::{Excluded, Unbounded};

use redb::ReadableTable;
use riffdb_storage_api::{
    CanonicalStoredEnvelopeV1, EncodedContentCharge, EncodedPageItem, OutboxClaimV1,
    OutboxDeadLetterV1, OutboxPageLimit, OutboxRenewV1, OutboxRepository, OutboxRetryV1,
    OutboxStatusObservationV1, OutboxStatusReadResultV1, OutboxSucceedV1, OutboxTransitionResultV1,
    PendingOutboxItemV1, PendingOutboxScanV1, ProjectionApplyRequestV1, ProjectionApplyResult,
    ProjectionApplyRowObservation, ProjectionApplySnapshot, ProjectionApplySnapshotBuilder,
    ProjectionApplySnapshotReader, ProjectionApplySnapshotRequest, ProjectionControlOperation,
    ProjectionControlResult, ProjectionLifecycleV1, ProjectionLowerContinuation,
    ProjectionMutationRepository, ProjectionQueryReader, ProjectionQueryRequest,
    ProjectionQueryResult, ProjectionRowPrior, ProjectionStatus, ProjectionUnavailableReason,
    StorageError, StorageErrorKind, StorageValueError, StoredCommitRecordV1, StoredDurableEventV1,
    StoredOutboxIntentV1, StoredOutboxStatusV1, StoredProjectionApplyV1, StoredProjectionControlV1,
    StoredProjectionStateV1, evaluate_projection_control_operation,
};
use riffdb_types::{
    CommitSequence, EventId, FrontierPosition, MAX_PROJECTION_WRITE_SET_BYTES, ProjectionApplyKey,
    ProjectionFrontierKey, ProjectionGroupKey, ProjectionIdentity,
};

use crate::codec::{
    decode_commit_record_v1, decode_durable_event_v1, decode_outbox_intent_v1,
    decode_outbox_status_v1, decode_projection_apply_v1, decode_projection_control_v1,
    decode_projection_state_v1, encode_outbox_status_v1, encode_projection_apply_v1,
    encode_projection_control_v1, encode_projection_state_v1,
};
use crate::error::{precommit_storage_error, storage_error, table_error};
use crate::keys::{
    decode_application_sequence_key, decode_event_key, decode_projection_group_key,
    encode_application_sequence_key, encode_event_key, encode_projection_apply_key,
    encode_projection_frontier_key, encode_projection_group_key,
};
use crate::layout::{
    COMMITS, EVENTS, OUTBOX, OUTBOX_STATUS, PROJECTION_APPLIED, PROJECTION_FRONTIER,
    PROJECTION_STATE,
};
use crate::store::RedbOperationalPorts;

impl OutboxRepository for RedbOperationalPorts {
    fn read_outbox_status(
        &self,
        event_id: EventId,
    ) -> Result<OutboxStatusReadResultV1, StorageError> {
        let transaction = self.begin_read()?;
        let events = transaction.open_table(EVENTS).map_err(table_error)?;
        let intents = transaction.open_table(OUTBOX).map_err(table_error)?;
        let statuses = transaction.open_table(OUTBOX_STATUS).map_err(table_error)?;
        let commits = transaction.open_table(COMMITS).map_err(table_error)?;
        Ok(
            reciprocal_outbox_item(&events, &intents, &statuses, &commits, event_id)?.map_or(
                OutboxStatusReadResultV1::AuthoritativeIntentMissing,
                |item| OutboxStatusReadResultV1::Status(item.status),
            ),
        )
    }

    fn scan_pending_outbox(
        &self,
        after: Option<EventId>,
        limit: OutboxPageLimit,
    ) -> Result<PendingOutboxScanV1, StorageError> {
        let transaction = self.begin_read()?;
        let events = transaction.open_table(EVENTS).map_err(table_error)?;
        let intents = transaction.open_table(OUTBOX).map_err(table_error)?;
        let statuses = transaction.open_table(OUTBOX_STATUS).map_err(table_error)?;
        let commits = transaction.open_table(COMMITS).map_err(table_error)?;
        let after_key = after.map(encode_event_key);
        let mut scan = match after_key.as_ref() {
            Some(key) => intents
                .range::<&[u8]>((Excluded(key.as_slice()), Unbounded))
                .map_err(precommit_storage_error)?,
            None => intents.iter().map_err(precommit_storage_error)?,
        };
        let wanted = usize::from(limit.get().get());
        let mut items = Vec::with_capacity(wanted);
        let mut encoded_bytes = 0usize;
        let mut has_more = false;

        for entry in &mut scan {
            let (physical_key, _) = entry.map_err(precommit_storage_error)?;
            let event_id = decode_event_key(physical_key.value()).map_err(|_| corrupt())?;
            let item = reciprocal_outbox_item(&events, &intents, &statuses, &commits, event_id)?
                .ok_or_else(corrupt)?;
            if !item.status.is_pending() {
                continue;
            }
            if items.len() == wanted {
                has_more = true;
                break;
            }
            let next_bytes = encoded_bytes
                .checked_add(item.encoded_bytes)
                .ok_or_else(corrupt)?;
            if next_bytes > riffdb_storage_api::MAX_SCAN_PAGE_BYTES {
                if items.is_empty() {
                    return Err(storage_error(StorageErrorKind::LimitExceeded));
                }
                has_more = true;
                break;
            }
            let charge = EncodedContentCharge::new(item.encoded_bytes).ok_or_else(corrupt)?;
            let pending = PendingOutboxItemV1::new(item.event, item.intent, item.status)
                .map_err(stored_value_error)?;
            items.push(EncodedPageItem::new(pending, charge));
            encoded_bytes = next_bytes;
        }

        PendingOutboxScanV1::page(items, has_more).map_err(stored_value_error)
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

impl RedbOperationalPorts {
    fn transition_outbox(
        &self,
        event_id: EventId,
        expected: &OutboxStatusObservationV1,
        updated: &StoredOutboxStatusV1,
    ) -> Result<OutboxTransitionResultV1, StorageError> {
        if updated.event_id() != event_id {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        let access = self.begin_write()?;
        let current = {
            let transaction = access.transaction()?;
            let events = transaction.open_table(EVENTS).map_err(table_error)?;
            let intents = transaction.open_table(OUTBOX).map_err(table_error)?;
            let statuses = transaction.open_table(OUTBOX_STATUS).map_err(table_error)?;
            let commits = transaction.open_table(COMMITS).map_err(table_error)?;
            reciprocal_outbox_item(&events, &intents, &statuses, &commits, event_id)?
        };
        let Some(current) = current else {
            access.abort()?;
            return Ok(OutboxTransitionResultV1::AuthoritativeIntentMissing);
        };
        if &current.status != expected {
            let observed = current.status;
            access.abort()?;
            return Ok(OutboxTransitionResultV1::StateChanged(observed));
        }

        let encoded = encode_outbox_status_v1(updated)?;
        {
            let transaction = access.transaction()?;
            let mut statuses = transaction.open_table(OUTBOX_STATUS).map_err(table_error)?;
            let key = encode_event_key(event_id);
            let previous = statuses
                .insert(key.as_slice(), encoded.as_bytes())
                .map_err(precommit_storage_error)?;
            match (expected, previous.as_ref()) {
                (OutboxStatusObservationV1::AbsentInitialPending, None) => {}
                (OutboxStatusObservationV1::Present(expected), Some(previous)) => {
                    let prior = decode_outbox_status_v1(previous.value())?.into_parts().0;
                    if &prior != expected || prior.event_id() != event_id {
                        return Err(corrupt());
                    }
                }
                (
                    OutboxStatusObservationV1::AbsentInitialPending
                    | OutboxStatusObservationV1::Present(_),
                    None | Some(_),
                ) => return Err(storage_error(StorageErrorKind::InvariantViolation)),
            }
        }
        access.commit()?;
        Ok(OutboxTransitionResultV1::Applied(updated.clone()))
    }
}

struct ReciprocalOutboxItem {
    event: StoredDurableEventV1,
    intent: StoredOutboxIntentV1,
    status: OutboxStatusObservationV1,
    encoded_bytes: usize,
}

fn reciprocal_outbox_item<E, I, S, C>(
    events: &E,
    intents: &I,
    statuses: &S,
    commits: &C,
    event_id: EventId,
) -> Result<Option<ReciprocalOutboxItem>, StorageError>
where
    E: ReadableTable<&'static [u8], &'static [u8]>,
    I: ReadableTable<&'static [u8], &'static [u8]>,
    S: ReadableTable<&'static [u8], &'static [u8]>,
    C: ReadableTable<&'static [u8], &'static [u8]>,
{
    let key = encode_event_key(event_id);
    let event = events
        .get(key.as_slice())
        .map_err(precommit_storage_error)?
        .map(|encoded| decode_durable_event_v1(encoded.value()))
        .transpose()?;
    let intent = intents
        .get(key.as_slice())
        .map_err(precommit_storage_error)?
        .map(|encoded| decode_outbox_intent_v1(encoded.value()))
        .transpose()?;
    let status = statuses
        .get(key.as_slice())
        .map_err(precommit_storage_error)?
        .map(|encoded| decode_outbox_status_v1(encoded.value()))
        .transpose()?;
    if status
        .as_ref()
        .is_some_and(|status| status.value().event_id() != event_id)
    {
        return Err(corrupt());
    }

    let commit = read_commit(commits, event_id.commit_sequence())?;
    let ordinal = usize::try_from(event_id.event_ordinal()).map_err(|_| corrupt())?;
    match (event, intent) {
        (None, None) => {
            if status.is_some()
                || commit.is_some_and(|commit| {
                    commit.events().get(ordinal).is_some()
                        || commit.outbox_event_ids().get(ordinal).is_some()
                })
            {
                return Err(corrupt());
            }
            Ok(None)
        }
        (Some(event), Some(intent)) => {
            if event.value().event_id() != event_id
                || intent.value().event_id() != event_id
                || intent.value().event() != event.value()
            {
                return Err(corrupt());
            }
            let commit = commit.ok_or_else(corrupt)?;
            if commit.events().get(ordinal) != Some(event.value())
                || commit.outbox_event_ids().get(ordinal) != Some(&event_id)
            {
                return Err(corrupt());
            }
            let encoded_bytes = event
                .encoded_content_charge()
                .get()
                .checked_add(intent.encoded_content_charge().get())
                .and_then(|bytes| {
                    status.as_ref().map_or(Some(bytes), |status| {
                        bytes.checked_add(status.encoded_content_charge().get())
                    })
                })
                .ok_or_else(corrupt)?;
            let status = status.map_or(OutboxStatusObservationV1::AbsentInitialPending, |status| {
                OutboxStatusObservationV1::Present(status.into_parts().0)
            });
            Ok(Some(ReciprocalOutboxItem {
                event: event.into_parts().0,
                intent: intent.into_parts().0,
                status,
                encoded_bytes,
            }))
        }
        (None | Some(_), None | Some(_)) => Err(corrupt()),
    }
}

impl ProjectionApplySnapshotReader for RedbOperationalPorts {
    fn read_apply_snapshot(
        &self,
        request: &ProjectionApplySnapshotRequest,
    ) -> Result<ProjectionApplySnapshot, StorageError> {
        let transaction = self.begin_read()?;
        let controls = transaction
            .open_table(PROJECTION_FRONTIER)
            .map_err(table_error)?;
        let rows = transaction
            .open_table(PROJECTION_STATE)
            .map_err(table_error)?;
        let control = read_projection_control(&controls, request.schema().identity())?
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
        if !control.permits_application(request.generation()) {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        let frontier = control
            .frontier_for(request.generation())
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
        let mut snapshot =
            ProjectionApplySnapshotBuilder::new(request, frontier).map_err(stored_value_error)?;
        for key in request.group_keys() {
            let observation = read_projection_state(&rows, request.schema(), key)?.map_or_else(
                || ProjectionApplyRowObservation::Absent(key.clone()),
                ProjectionApplyRowObservation::Present,
            );
            snapshot.push_row(observation).map_err(stored_value_error)?;
        }
        snapshot.finish().map_err(stored_value_error)
    }
}

impl ProjectionMutationRepository for RedbOperationalPorts {
    fn apply_projection(
        &mut self,
        request: &ProjectionApplyRequestV1,
    ) -> Result<ProjectionApplyResult, StorageError> {
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

        let access = self.begin_write()?;
        let transaction = access.transaction()?;
        {
            let commits = transaction.open_table(COMMITS).map_err(table_error)?;
            if read_commit(&commits, request.sequence())?.is_none() {
                return Err(corrupt());
            }
        }
        let current_control = {
            let controls = transaction
                .open_table(PROJECTION_FRONTIER)
                .map_err(table_error)?;
            read_projection_control(&controls, request.identity())?
        };
        let Some(current_control) = current_control else {
            access.abort()?;
            return Ok(ProjectionApplyResult::StateChanged);
        };
        let retained = current_control
            .frontier_for(request.generation())
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
        let marker_key = ProjectionApplyKey::new(
            request.identity().clone(),
            request.generation(),
            request.sequence(),
        );

        if sequence_is_at_or_before(request.sequence(), retained) {
            let marker = {
                let markers = transaction
                    .open_table(PROJECTION_APPLIED)
                    .map_err(table_error)?;
                read_projection_marker(&markers, &marker_key)?.ok_or_else(corrupt)?
            };
            if marker.canonical_hash() != request.apply_hash() {
                return Err(corrupt());
            }
            access.abort()?;
            return Ok(ProjectionApplyResult::AlreadyApplied(marker));
        }
        if !current_control.permits_application(request.generation())
            || retained != request.expected_frontier()
        {
            access.abort()?;
            return Ok(ProjectionApplyResult::StateChanged);
        }
        {
            let markers = transaction
                .open_table(PROJECTION_APPLIED)
                .map_err(table_error)?;
            if read_projection_marker(&markers, &marker_key)?.is_some() {
                return Err(corrupt());
            }
        }

        let mut prepared_rows = Vec::with_capacity(request.row_updates().len());
        {
            let rows = transaction
                .open_table(PROJECTION_STATE)
                .map_err(table_error)?;
            for update in request.row_updates() {
                let current = read_projection_state(&rows, request.schema(), update.key())?;
                let prior_matches = match (update.prior(), current.as_ref()) {
                    (ProjectionRowPrior::Absent, None) => true,
                    (ProjectionRowPrior::Present(expected), Some(current)) => {
                        current.last_changed_sequence() == expected
                    }
                    (
                        ProjectionRowPrior::Absent | ProjectionRowPrior::Present(_),
                        None | Some(_),
                    ) => false,
                };
                if !prior_matches {
                    drop(rows);
                    access.abort()?;
                    return Ok(ProjectionApplyResult::StateChanged);
                }
                let post_image = projection_post_image(request, update)?;
                let encoded = encode_projection_state_v1(&post_image)?;
                prepared_rows.push(PreparedProjectionRow {
                    key: update.key().clone(),
                    current,
                    encoded,
                });
            }
        }
        let control = current_control
            .after_apply(
                request.generation(),
                request.expected_frontier(),
                request.sequence(),
            )
            .map_err(request_value_error)?;
        let marker = StoredProjectionApplyV1::new(marker_key.clone(), request.apply_hash());
        let encoded_marker = encode_projection_apply_v1(&marker)?;
        let encoded_control = encode_projection_control_v1(&control)?;
        let encoded_bytes = prepared_rows.iter().try_fold(
            encoded_marker
                .encoded_content_charge()
                .get()
                .checked_add(encoded_control.encoded_content_charge().get())
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?,
            |bytes, row| {
                bytes
                    .checked_add(row.encoded.encoded_content_charge().get())
                    .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))
            },
        )?;
        if encoded_bytes > MAX_PROJECTION_WRITE_SET_BYTES {
            return Err(storage_error(StorageErrorKind::LimitExceeded));
        }

        {
            let mut rows = transaction
                .open_table(PROJECTION_STATE)
                .map_err(table_error)?;
            for row in &prepared_rows {
                let previous = rows
                    .insert(
                        encode_projection_group_key(&row.key),
                        row.encoded.as_bytes(),
                    )
                    .map_err(precommit_storage_error)?;
                match (&row.current, previous.as_ref()) {
                    (None, None) => {}
                    (Some(expected), Some(previous)) => {
                        let observed =
                            decode_projection_state_v1(previous.value(), request.schema())?
                                .into_parts()
                                .0;
                        if &observed != expected || observed.key() != &row.key {
                            return Err(corrupt());
                        }
                    }
                    (None | Some(_), None | Some(_)) => {
                        return Err(storage_error(StorageErrorKind::InvariantViolation));
                    }
                }
            }
        }
        {
            let mut markers = transaction
                .open_table(PROJECTION_APPLIED)
                .map_err(table_error)?;
            if markers
                .insert(
                    encode_projection_apply_key(&marker_key),
                    encoded_marker.as_bytes(),
                )
                .map_err(precommit_storage_error)?
                .is_some()
            {
                return Err(storage_error(StorageErrorKind::InvariantViolation));
            }
        }
        {
            let key = ProjectionFrontierKey::new(request.identity().clone());
            let mut controls = transaction
                .open_table(PROJECTION_FRONTIER)
                .map_err(table_error)?;
            let previous = controls
                .insert(
                    encode_projection_frontier_key(&key),
                    encoded_control.as_bytes(),
                )
                .map_err(precommit_storage_error)?
                .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
            let observed = decode_projection_control_v1(previous.value())?
                .into_parts()
                .0;
            if observed != current_control {
                return Err(corrupt());
            }
        }
        access.commit()?;
        Ok(ProjectionApplyResult::Applied { marker, control })
    }

    fn transition_projection_control(
        &mut self,
        operation: ProjectionControlOperation,
    ) -> Result<ProjectionControlResult, StorageError> {
        let identity = projection_operation_identity(&operation).clone();
        let access = self.begin_write()?;
        let transaction = access.transaction()?;
        let current = {
            let controls = transaction
                .open_table(PROJECTION_FRONTIER)
                .map_err(table_error)?;
            read_projection_control(&controls, &identity)?
        };
        let head = {
            let commits = transaction.open_table(COMMITS).map_err(table_error)?;
            authoritative_head(&commits)?
        };
        let result = evaluate_projection_control_operation(current.as_ref(), &operation, head)
            .map_err(request_value_error)?;
        let ProjectionControlResult::Updated(updated) = result else {
            access.abort()?;
            return Ok(result);
        };
        let encoded = encode_projection_control_v1(&updated)?;
        {
            let key = ProjectionFrontierKey::new(identity);
            let mut controls = transaction
                .open_table(PROJECTION_FRONTIER)
                .map_err(table_error)?;
            let previous = controls
                .insert(encode_projection_frontier_key(&key), encoded.as_bytes())
                .map_err(precommit_storage_error)?;
            match (current.as_ref(), previous.as_ref()) {
                (None, None) => {}
                (Some(expected), Some(previous)) => {
                    let observed = decode_projection_control_v1(previous.value())?
                        .into_parts()
                        .0;
                    if &observed != expected {
                        return Err(corrupt());
                    }
                }
                (None | Some(_), None | Some(_)) => {
                    return Err(storage_error(StorageErrorKind::InvariantViolation));
                }
            }
        }
        access.commit()?;
        Ok(ProjectionControlResult::Updated(updated))
    }
}

struct PreparedProjectionRow {
    key: ProjectionGroupKey,
    current: Option<StoredProjectionStateV1>,
    encoded: CanonicalStoredEnvelopeV1,
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

impl ProjectionQueryReader for RedbOperationalPorts {
    fn query_projection(
        &self,
        request: &ProjectionQueryRequest,
    ) -> Result<ProjectionQueryResult, StorageError> {
        let transaction = self.begin_read()?;
        let controls = transaction
            .open_table(PROJECTION_FRONTIER)
            .map_err(table_error)?;
        let rows = transaction
            .open_table(PROJECTION_STATE)
            .map_err(table_error)?;
        let Some(control) = read_projection_control(&controls, request.selector().identity())?
        else {
            return Ok(ProjectionQueryResult::Degraded {
                current: FrontierPosition::BeforeFirst,
                reason: ProjectionUnavailableReason::Building,
            });
        };
        match control.lifecycle() {
            ProjectionLifecycleV1::Building | ProjectionLifecycleV1::CatchingUp => {
                Ok(ProjectionQueryResult::Degraded {
                    current: control.candidate().ok_or_else(corrupt)?.frontier(),
                    reason: ProjectionUnavailableReason::Building,
                })
            }
            ProjectionLifecycleV1::Rebuilding => Ok(ProjectionQueryResult::Degraded {
                current: control.published().ok_or_else(corrupt)?.frontier(),
                reason: ProjectionUnavailableReason::Rebuilding,
            }),
            ProjectionLifecycleV1::Degraded => {
                let failure = control.failure().ok_or_else(corrupt)?;
                let current = control
                    .frontier_for(failure.generation())
                    .ok_or_else(corrupt)?;
                Ok(ProjectionQueryResult::Degraded {
                    current,
                    reason: ProjectionUnavailableReason::Failure(failure.code()),
                })
            }
            ProjectionLifecycleV1::Invalid => {
                let failure = control.failure().ok_or_else(corrupt)?;
                Ok(ProjectionQueryResult::Invalid {
                    reason: failure.code(),
                })
            }
            ProjectionLifecycleV1::Ready => query_ready_projection(&rows, request, &control),
        }
    }

    fn read_projection_status(
        &self,
        identity: &ProjectionIdentity,
    ) -> Result<ProjectionStatus, StorageError> {
        let transaction = self.begin_read()?;
        let controls = transaction
            .open_table(PROJECTION_FRONTIER)
            .map_err(table_error)?;
        let commits = transaction.open_table(COMMITS).map_err(table_error)?;
        let head = authoritative_head(&commits)?;
        Ok(read_projection_control(&controls, identity)?.map_or_else(
            || ProjectionStatus::uninitialized(identity.clone(), head),
            |control| ProjectionStatus::from_control(&control, head),
        ))
    }
}

fn query_ready_projection<T>(
    rows: &T,
    request: &ProjectionQueryRequest,
    control: &StoredProjectionControlV1,
) -> Result<ProjectionQueryResult, StorageError>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
{
    let published = control.published().ok_or_else(corrupt)?;
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
    let mut scan = match request.continuation() {
        Some(continuation) => rows
            .range::<&[u8]>((
                Excluded(continuation.exclusive_last_key().as_bytes()),
                Unbounded,
            ))
            .map_err(precommit_storage_error)?,
        None => rows
            .range(prefix.as_bytes()..)
            .map_err(precommit_storage_error)?,
    };
    let wanted = usize::from(request.limit().get());
    let mut result_rows = Vec::with_capacity(wanted);
    let mut encoded_bytes = 0usize;
    let mut has_more = false;

    for entry in &mut scan {
        let (physical_key, encoded) = entry.map_err(precommit_storage_error)?;
        if !physical_key.value().starts_with(prefix.as_bytes()) {
            break;
        }
        let key = decode_projection_group_key(physical_key.value()).map_err(|_| corrupt())?;
        if key.identity() != request.selector().identity()
            || key.generation() != published.generation()
        {
            return Err(corrupt());
        }
        if result_rows.len() == wanted {
            has_more = true;
            break;
        }
        let decoded = decode_projection_state_v1(encoded.value(), request.selector().schema())?;
        if decoded.value().key() != &key
            || !sequence_is_at_or_before(
                decoded.value().last_changed_sequence(),
                published.frontier(),
            )
        {
            return Err(corrupt());
        }
        let next_bytes = encoded_bytes
            .checked_add(decoded.encoded_content_charge().get())
            .ok_or_else(corrupt)?;
        if next_bytes > riffdb_types::MAX_PROJECTION_QUERY_CONTENT_BYTES {
            if result_rows.is_empty() {
                return Err(storage_error(StorageErrorKind::LimitExceeded));
            }
            has_more = true;
            break;
        }
        encoded_bytes = next_bytes;
        result_rows.push(decoded);
    }
    let next = if has_more {
        let last = result_rows.last().ok_or_else(corrupt)?;
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
        result_rows,
        next,
    )
    .map_err(stored_value_error)
}

fn read_projection_control<T>(
    table: &T,
    identity: &ProjectionIdentity,
) -> Result<Option<StoredProjectionControlV1>, StorageError>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
{
    let key = ProjectionFrontierKey::new(identity.clone());
    let Some(encoded) = table
        .get(encode_projection_frontier_key(&key))
        .map_err(precommit_storage_error)?
    else {
        return Ok(None);
    };
    let control = decode_projection_control_v1(encoded.value())?
        .into_parts()
        .0;
    if control.identity() != identity {
        return Err(corrupt());
    }
    Ok(Some(control))
}

fn read_projection_state<T>(
    table: &T,
    schema: &riffdb_storage_api::CheckedProjectionSchema,
    key: &ProjectionGroupKey,
) -> Result<Option<StoredProjectionStateV1>, StorageError>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
{
    let Some(encoded) = table
        .get(encode_projection_group_key(key))
        .map_err(precommit_storage_error)?
    else {
        return Ok(None);
    };
    let row = decode_projection_state_v1(encoded.value(), schema)?
        .into_parts()
        .0;
    if row.key() != key {
        return Err(corrupt());
    }
    Ok(Some(row))
}

fn read_projection_marker<T>(
    table: &T,
    key: &ProjectionApplyKey,
) -> Result<Option<StoredProjectionApplyV1>, StorageError>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
{
    let Some(encoded) = table
        .get(encode_projection_apply_key(key))
        .map_err(precommit_storage_error)?
    else {
        return Ok(None);
    };
    let marker = decode_projection_apply_v1(encoded.value())?.into_parts().0;
    if marker.key() != key {
        return Err(corrupt());
    }
    Ok(Some(marker))
}

fn read_commit<T>(
    table: &T,
    sequence: CommitSequence,
) -> Result<Option<StoredCommitRecordV1>, StorageError>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
{
    let key = encode_application_sequence_key(sequence);
    let Some(encoded) = table.get(key.as_slice()).map_err(precommit_storage_error)? else {
        return Ok(None);
    };
    let commit = decode_commit_record_v1(encoded.value())?.into_parts().0;
    if commit.commit_sequence() != sequence {
        return Err(corrupt());
    }
    Ok(Some(commit))
}

fn authoritative_head<T>(table: &T) -> Result<FrontierPosition, StorageError>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
{
    let Some((physical_key, encoded)) = table.last().map_err(precommit_storage_error)? else {
        return Ok(FrontierPosition::BeforeFirst);
    };
    let sequence = decode_application_sequence_key(physical_key.value()).map_err(|_| corrupt())?;
    let commit = decode_commit_record_v1(encoded.value())?.into_parts().0;
    if commit.commit_sequence() != sequence {
        return Err(corrupt());
    }
    Ok(FrontierPosition::AppliedThrough(sequence))
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

fn sequence_is_at_or_before(sequence: CommitSequence, frontier: FrontierPosition) -> bool {
    matches!(frontier, FrontierPosition::AppliedThrough(applied) if sequence <= applied)
}

fn request_value_error(error: StorageValueError) -> StorageError {
    match error {
        StorageValueError::LimitExceeded | StorageValueError::SizeOverflow => {
            storage_error(StorageErrorKind::LimitExceeded)
        }
        StorageValueError::Empty
        | StorageValueError::NonCanonicalOrder
        | StorageValueError::Duplicate
        | StorageValueError::IdentityMismatch
        | StorageValueError::InvalidShape => storage_error(StorageErrorKind::InvariantViolation),
    }
}

fn stored_value_error(error: StorageValueError) -> StorageError {
    match error {
        StorageValueError::LimitExceeded | StorageValueError::SizeOverflow => {
            storage_error(StorageErrorKind::LimitExceeded)
        }
        StorageValueError::Empty
        | StorageValueError::NonCanonicalOrder
        | StorageValueError::Duplicate
        | StorageValueError::IdentityMismatch
        | StorageValueError::InvalidShape => corrupt(),
    }
}

const fn corrupt() -> StorageError {
    storage_error(StorageErrorKind::CorruptData)
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU16, NonZeroU32};
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    use redb::ReadableTableMetadata;
    use riffdb_storage_api::{
        DatabaseInitializationPort, DeclaredOutcome, DurabilityMode, ExecutablePlanRef,
        OutboxDeliveryStateV1, OutboxDestinationIdV1, OutboxRetryV1, ReadDependencies,
        StoredReadDependenciesV1, derive_event_hash_v1,
    };
    use riffdb_types::{
        ActorId, ActorKind, AdmittedActorContext, AggregateTypeId, CanonicalInputHash,
        CanonicalRecord, CommandId, ContractBundleHash, ContractLineage, ContractVersion,
        DatabaseId, EventTypeId, LogicalTime, OutcomeId, PartitionKeyBuilder, PlanHash,
        ProjectionId, ProjectionPlanHash, ProvenanceId, RequestId, TenantScope, Timestamp,
        hash_partition_key,
    };

    use super::*;
    use crate::codec::{
        encode_commit_record_v1, encode_durable_event_v1, encode_outbox_intent_v1,
        encode_projection_control_v1,
    };
    use crate::layout::{COMMITS, EVENTS, OUTBOX, OUTBOX_STATUS, PROJECTION_FRONTIER};
    use crate::store::RedbStore;

    static NEXT_TEST_PATH: AtomicU64 = AtomicU64::new(1);

    struct TestDatabasePath(PathBuf);

    impl TestDatabasePath {
        fn new(label: &str) -> Self {
            let ordinal = NEXT_TEST_PATH.fetch_add(1, Ordering::Relaxed);
            Self(std::env::temp_dir().join(format!(
                "riffdb-redb-derived-{label}-{}-{ordinal}.redb",
                std::process::id()
            )))
        }
    }

    impl Drop for TestDatabasePath {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    fn uuid_bytes(fill: u8) -> [u8; 16] {
        let mut bytes = [fill; 16];
        bytes[6] = 0x70 | (fill & 0x0f);
        bytes[8] = 0x80 | (fill & 0x3f);
        bytes
    }

    fn database_id() -> DatabaseId {
        DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x11; 10])
            .expect("database ID")
    }

    fn operational(label: &str) -> (TestDatabasePath, RedbOperationalPorts) {
        let path = TestDatabasePath::new(label);
        let mut store = RedbStore::open(&path.0).expect("open store");
        store
            .initialize_database(database_id())
            .expect("initialize store");
        let ports = RedbOperationalPorts {
            shared: Arc::clone(&store.shared),
        };
        (path, ports)
    }

    fn timestamp(seconds: i64) -> Timestamp {
        Timestamp::new(seconds, 0).expect("timestamp")
    }

    fn destination() -> OutboxDestinationIdV1 {
        OutboxDestinationIdV1::new("primary").expect("destination")
    }

    fn projection_identity(seed: u8) -> ProjectionIdentity {
        ProjectionIdentity::new(
            ContractLineage::new(format!("redb-derived-{seed}")).expect("lineage"),
            ProjectionId::first(),
            ProjectionPlanHash::from_bytes([seed; 32]),
        )
    }

    fn command_graph_at(
        sequence: CommitSequence,
        event_count: u32,
    ) -> (StoredCommitRecordV1, Vec<StoredDurableEventV1>) {
        let plan = ExecutablePlanRef::new(
            ContractLineage::new("redb-derived").expect("lineage"),
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
            &ReadDependencies::new(Vec::new()).expect("empty dependencies"),
        )
        .expect("stored dependencies");
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
            DurabilityMode::Sync,
        )
        .expect("commit");
        (commit, events)
    }

    fn seed_command(
        ports: &RedbOperationalPorts,
        sequence: CommitSequence,
        event_count: u32,
    ) -> Vec<StoredDurableEventV1> {
        let (commit, events) = command_graph_at(sequence, event_count);
        let encoded_commit = encode_commit_record_v1(&commit).expect("encode commit");
        let encoded_events = events
            .iter()
            .map(|event| {
                let intent = StoredOutboxIntentV1::new(event.clone());
                (
                    encode_durable_event_v1(event).expect("encode event"),
                    encode_outbox_intent_v1(&intent).expect("encode intent"),
                )
            })
            .collect::<Vec<_>>();
        let access = ports.begin_write().expect("begin seed transaction");
        let transaction = access.transaction().expect("seed transaction");
        {
            let mut commits = transaction.open_table(COMMITS).expect("commit table");
            let key = encode_application_sequence_key(sequence);
            assert!(
                commits
                    .insert(key.as_slice(), encoded_commit.as_bytes())
                    .expect("insert commit")
                    .is_none()
            );
        }
        {
            let mut event_table = transaction.open_table(EVENTS).expect("event table");
            let mut outbox_table = transaction.open_table(OUTBOX).expect("outbox table");
            for (event, (encoded_event, encoded_intent)) in events.iter().zip(encoded_events.iter())
            {
                let key = encode_event_key(event.event_id());
                assert!(
                    event_table
                        .insert(key.as_slice(), encoded_event.as_bytes())
                        .expect("insert event")
                        .is_none()
                );
                assert!(
                    outbox_table
                        .insert(key.as_slice(), encoded_intent.as_bytes())
                        .expect("insert intent")
                        .is_none()
                );
            }
        }
        access.commit().expect("commit seed transaction");
        events
    }

    fn install_control(
        ports: &RedbOperationalPorts,
        physical_identity: &ProjectionIdentity,
        control: &StoredProjectionControlV1,
    ) {
        let encoded = encode_projection_control_v1(control).expect("encode control");
        let key = ProjectionFrontierKey::new(physical_identity.clone());
        let access = ports.begin_write().expect("begin control transaction");
        {
            let mut controls = access
                .transaction()
                .expect("control transaction")
                .open_table(PROJECTION_FRONTIER)
                .expect("control table");
            controls
                .insert(encode_projection_frontier_key(&key), encoded.as_bytes())
                .expect("insert control");
        }
        access.commit().expect("commit control transaction");
    }

    #[test]
    fn pending_scan_and_status_cas_use_real_envelope_charges() {
        let (_path, mut ports) = operational("outbox-cas");
        let events = seed_command(&ports, CommitSequence::first(), 2);
        let event_ids = events
            .iter()
            .map(StoredDurableEventV1::event_id)
            .collect::<Vec<_>>();
        let limit = OutboxPageLimit::new(NonZeroU16::new(1).expect("nonzero")).expect("limit");

        let PendingOutboxScanV1::Page { items, next_after } = ports
            .scan_pending_outbox(None, limit)
            .expect("first pending page")
        else {
            panic!("two pending rows at limit one must page");
        };
        assert_eq!(next_after, event_ids[0]);
        assert_eq!(items.len(), 1);
        let expected_initial_charge = encode_durable_event_v1(items[0].value().event())
            .expect("encode scanned event")
            .encoded_content_charge()
            .get()
            + encode_outbox_intent_v1(items[0].value().intent())
                .expect("encode scanned intent")
                .encoded_content_charge()
                .get();
        assert_eq!(
            items[0].encoded_content_charge().get(),
            expected_initial_charge
        );

        let claim = OutboxClaimV1::new(
            event_ids[0],
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
        assert_eq!(
            ports.claim_outbox(&claim).expect("stale claim"),
            OutboxTransitionResultV1::StateChanged(OutboxStatusObservationV1::Present(
                delivering.clone()
            ))
        );

        let PendingOutboxScanV1::ExactEnd { items } = ports
            .scan_pending_outbox(None, limit)
            .expect("only second event remains pending")
        else {
            panic!("one remaining pending row must reach exact end");
        };
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].value().event_id(), event_ids[1]);

        let retry = OutboxRetryV1::new(delivering, Some(timestamp(30)), None).expect("retry");
        let OutboxTransitionResultV1::Applied(pending) =
            ports.retry_outbox(&retry).expect("apply retry")
        else {
            panic!("retry must apply");
        };
        let two = OutboxPageLimit::new(NonZeroU16::new(2).expect("nonzero")).expect("limit");
        let PendingOutboxScanV1::ExactEnd { items } = ports
            .scan_pending_outbox(None, two)
            .expect("explicit and implicit pending scan")
        else {
            panic!("both pending rows must reach exact end");
        };
        assert_eq!(items.len(), 2);
        assert_eq!(
            items[0].value().status(),
            &OutboxStatusObservationV1::Present(pending.clone())
        );
        let status_charge = encode_outbox_status_v1(&pending)
            .expect("encode pending status")
            .encoded_content_charge()
            .get();
        assert_eq!(
            items[0].encoded_content_charge().get(),
            expected_initial_charge + status_charge
        );
    }

    #[test]
    fn missing_or_nonreciprocal_outbox_authority_never_repairs_state() {
        let (_path, mut ports) = operational("outbox-corruption");
        let missing = EventId::new(CommitSequence::first(), 0);
        let claim = OutboxClaimV1::new(
            missing,
            OutboxStatusObservationV1::AbsentInitialPending,
            destination(),
            timestamp(10),
            timestamp(20),
        )
        .expect("claim");
        assert_eq!(
            ports.claim_outbox(&claim).expect("missing authority"),
            OutboxTransitionResultV1::AuthoritativeIntentMissing
        );
        {
            let transaction = ports.begin_read().expect("read transaction");
            let statuses = transaction.open_table(OUTBOX_STATUS).expect("status table");
            assert!(statuses.is_empty().expect("empty status table"));
        }

        let event_id = seed_command(&ports, CommitSequence::first(), 1)[0].event_id();
        let access = ports.begin_write().expect("begin corruption transaction");
        {
            let mut commits = access
                .transaction()
                .expect("transaction")
                .open_table(COMMITS)
                .expect("commit table");
            let key = encode_application_sequence_key(CommitSequence::first());
            assert!(
                commits
                    .remove(key.as_slice())
                    .expect("remove authoritative commit")
                    .is_some()
            );
        }
        access.commit().expect("commit test corruption");
        assert_eq!(
            ports
                .read_outbox_status(event_id)
                .expect_err("orphan event and intent must fail")
                .kind(),
            StorageErrorKind::CorruptData
        );
        let transaction = ports.begin_read().expect("post-error read");
        let events = transaction.open_table(EVENTS).expect("event table");
        let intents = transaction.open_table(OUTBOX).expect("outbox table");
        let key = encode_event_key(event_id);
        assert!(events.get(key.as_slice()).expect("read event").is_some());
        assert!(intents.get(key.as_slice()).expect("read intent").is_some());
    }

    #[test]
    fn projection_control_cas_status_and_invalid_transition_are_atomic() {
        let (_path, mut ports) = operational("projection-control");
        seed_command(&ports, CommitSequence::first(), 0);
        let identity = projection_identity(0x31);
        let initial = StoredProjectionControlV1::initial(identity.clone());
        install_control(&ports, &identity, &initial);

        let status = ports
            .read_projection_status(&identity)
            .expect("projection status");
        assert_eq!(status.lifecycle(), ProjectionLifecycleV1::Building);
        assert_eq!(status.candidate(), initial.candidate());
        assert_eq!(
            status.authoritative_head(),
            FrontierPosition::AppliedThrough(CommitSequence::first())
        );

        let ProjectionControlResult::Updated(catching_up) = ports
            .transition_projection_control(ProjectionControlOperation::StartInitialScan {
                expected: initial.clone(),
            })
            .expect("start initial scan")
        else {
            panic!("start transition must update");
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
        assert_eq!(
            ports
                .transition_projection_control(ProjectionControlOperation::PublishCandidate {
                    expected: catching_up.clone(),
                })
                .expect_err("before-first candidate is behind head")
                .kind(),
            StorageErrorKind::InvariantViolation
        );
        let retained = ports
            .read_projection_status(&identity)
            .expect("retained status after rejected publication");
        assert_eq!(retained.lifecycle(), ProjectionLifecycleV1::CatchingUp);
        assert_eq!(retained.candidate(), catching_up.candidate());
    }

    #[test]
    fn projection_control_key_payload_mismatch_is_corruption() {
        let (_path, ports) = operational("projection-key-mismatch");
        let physical_identity = projection_identity(0x41);
        let payload_identity = projection_identity(0x42);
        let payload = StoredProjectionControlV1::initial(payload_identity);
        install_control(&ports, &physical_identity, &payload);

        assert_eq!(
            ports
                .read_projection_status(&physical_identity)
                .expect_err("physical and repeated identities must agree")
                .kind(),
            StorageErrorKind::CorruptData
        );
    }

    #[test]
    fn outbox_attempts_remain_nonzero_after_durable_round_trip() {
        let (_path, mut ports) = operational("outbox-attempt");
        let event_id = seed_command(&ports, CommitSequence::first(), 1)[0].event_id();
        let claim = OutboxClaimV1::new(
            event_id,
            OutboxStatusObservationV1::AbsentInitialPending,
            destination(),
            timestamp(10),
            timestamp(20),
        )
        .expect("claim");
        let OutboxTransitionResultV1::Applied(status) =
            ports.claim_outbox(&claim).expect("claim outbox")
        else {
            panic!("claim must apply");
        };
        assert!(matches!(
            status.state(),
            OutboxDeliveryStateV1::Delivering { attempt, .. }
                if *attempt == NonZeroU32::new(1).expect("nonzero")
        ));
        assert_eq!(
            ports.read_outbox_status(event_id).expect("read status"),
            OutboxStatusReadResultV1::Status(OutboxStatusObservationV1::Present(status))
        );
    }
}
