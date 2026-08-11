//! Durable outbox and projection ports over the frozen redb layout.

use std::ops::Bound::{Excluded, Unbounded};

use redb::ReadableTable;
use riffdb_storage_api::{
    CanonicalStoredEnvelopeV1, CleanProjectionGenerationV1, EncodedContentCharge, EncodedPageItem,
    OutboxClaimV1, OutboxDeadLetterV1, OutboxPageLimit, OutboxRecoveryUpperFenceV1, OutboxRenewV1,
    OutboxRepository, OutboxRetryV1, OutboxStatusObservationV1, OutboxStatusReadResultV1,
    OutboxSucceedV1, OutboxTransitionResultV1, PendingOutboxItemV1, PendingOutboxScanV1,
    ProjectionApplyRequestV1, ProjectionApplyResult, ProjectionApplyRowObservation,
    ProjectionApplySnapshot, ProjectionApplySnapshotBuilder, ProjectionApplySnapshotReader,
    ProjectionApplySnapshotRequest, ProjectionControlOperation, ProjectionControlResult,
    ProjectionControlScanV1, ProjectionLifecycleV1, ProjectionLowerContinuation,
    ProjectionMutationRepository, ProjectionQueryReader, ProjectionQueryRequest,
    ProjectionQueryResult, ProjectionRecoveryContinuationV1, ProjectionRecoveryExpectedPageV1,
    ProjectionRecoveryFindingCodeV1, ProjectionRecoveryFindingV1, ProjectionRecoveryPageLimit,
    ProjectionRecoveryRepository, ProjectionRecoveryValidationRequestV1,
    ProjectionRecoveryValidationResultV1, ProjectionRowPrior, ProjectionStatus,
    ProjectionUnavailableReason, StorageError, StorageErrorKind, StorageValueError,
    StoredCommitRecordV1, StoredDurableEventV1, StoredOutboxIntentV1, StoredOutboxStatusV1,
    StoredProjectionApplyV1, StoredProjectionControlV1, StoredProjectionStateV1,
    UndeliveredOutboxStatusScanRequestV1, UndeliveredOutboxStatusScanV1, UndeliveredOutboxStatusV1,
    evaluate_projection_control_operation,
};
use riffdb_types::{
    CommitSequence, EventId, FrontierPosition, MAX_PROJECTION_WRITE_SET_BYTES, ProjectionApplyKey,
    ProjectionFrontierKey, ProjectionGeneration, ProjectionGroupKey, ProjectionIdentity,
};

use crate::codec::{
    decode_durable_event_v1, decode_outbox_intent_with_event, decode_outbox_status_v1,
    decode_projection_apply_v1, decode_projection_control_v1, decode_projection_state_v1,
    encode_durable_event_v1, encode_outbox_intent_v1, encode_outbox_status_v1,
    encode_projection_apply_v1, encode_projection_control_v1, encode_projection_state_v1,
};
use crate::command_authority::{
    command_authority_head, command_member_at_access, commit_at, commit_at_access,
};
use crate::error::{precommit_storage_error, storage_error, table_error};
use crate::hooks::RedbTestOperation;
use crate::journal::JournalTable;
#[cfg(test)]
use crate::keys::encode_application_sequence_key;
use crate::keys::{
    decode_projection_frontier_key, decode_projection_group_key, encode_event_key,
    encode_projection_apply_key, encode_projection_frontier_key, encode_projection_group_key,
};
use crate::layout::{
    COMMITS, EVENTS, OUTBOX, OUTBOX_STATUS, PROJECTION_APPLIED, PROJECTION_FRONTIER,
    PROJECTION_STATE,
};
use crate::store::{RedbOperationalPorts, RedbReadAccess};
use crate::transient::TransientIndexDelta;

impl OutboxRepository for RedbOperationalPorts {
    fn has_undelivered_outbox(&self) -> Result<bool, StorageError> {
        let _lease = self.acquire_indexed_read_lease()?;
        self.undelivered_outbox_page(None, 1)
            .map(|(event_ids, _)| !event_ids.is_empty())
    }

    fn read_outbox_status(
        &self,
        event_id: EventId,
    ) -> Result<OutboxStatusReadResultV1, StorageError> {
        let transaction = self.begin_composite_read()?;
        let statuses = transaction.open_table(OUTBOX_STATUS).map_err(table_error)?;
        Ok(
            reciprocal_outbox_item_access(&transaction, &statuses, event_id)?.map_or(
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
        let _lease = self.acquire_indexed_read_lease()?;
        let wanted = usize::from(limit.get().get());
        let (event_ids, mut has_more) = self.pending_outbox_page(after, wanted)?;
        let transaction = self.begin_composite_read()?;
        let statuses = transaction.open_table(OUTBOX_STATUS).map_err(table_error)?;
        let mut items = Vec::with_capacity(wanted);
        let mut encoded_bytes = 0usize;

        for event_id in event_ids {
            let item = reciprocal_outbox_item_access(&transaction, &statuses, event_id)?
                .ok_or_else(corrupt)?;
            if !item.status.is_pending() {
                return Err(corrupt());
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

    fn scan_undelivered_outbox_statuses(
        &self,
        request: UndeliveredOutboxStatusScanRequestV1,
    ) -> Result<UndeliveredOutboxStatusScanV1, StorageError> {
        let _lease = self.acquire_indexed_read_lease()?;
        // The transient index gates derived-component availability only. The
        // recovery page itself is always read and proven from durable tables.
        let _ = self.undelivered_outbox_page(None, 0)?;
        let transaction = self.begin_composite_read()?;
        let statuses = transaction.open_table(OUTBOX_STATUS).map_err(table_error)?;
        let inclusive_upper = request.inclusive_upper().or(self.outbox_intent_last()?);
        let Some(inclusive_upper) = inclusive_upper else {
            if statuses.last().map_err(precommit_storage_error)?.is_some() {
                return Err(corrupt());
            }
            return UndeliveredOutboxStatusScanV1::exact_end(
                request,
                OutboxRecoveryUpperFenceV1::BeforeFirst,
                Vec::new(),
            )
            .map_err(stored_value_error);
        };
        if request.after().is_some_and(|after| after > inclusive_upper) {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }

        let wanted = usize::from(request.limit().get().get());
        let (source, indexed_has_more) =
            self.undelivered_outbox_page(request.after(), wanted.saturating_add(1))?;
        let mut items = Vec::with_capacity(wanted);
        let mut encoded_bytes = 0usize;
        let mut has_more = indexed_has_more;

        for event_id in source {
            if event_id > inclusive_upper {
                break;
            }
            let item = reciprocal_outbox_item_access(&transaction, &statuses, event_id)?
                .ok_or_else(corrupt)?;
            if !status_is_undelivered(&item.status) {
                continue;
            }
            let next_bytes = encoded_bytes
                .checked_add(item.encoded_bytes)
                .ok_or_else(corrupt)?;
            if items.len() == wanted || next_bytes > riffdb_storage_api::MAX_SCAN_PAGE_BYTES {
                if items.is_empty() {
                    return Err(storage_error(StorageErrorKind::LimitExceeded));
                }
                has_more = true;
                break;
            }
            let charge = EncodedContentCharge::new(item.encoded_bytes).ok_or_else(corrupt)?;
            let status = UndeliveredOutboxStatusV1::new(event_id, item.status)
                .map_err(stored_value_error)?;
            items.push(EncodedPageItem::new(status, charge));
            encoded_bytes = next_bytes;
        }

        if has_more {
            UndeliveredOutboxStatusScanV1::page(request, inclusive_upper, items)
                .map_err(stored_value_error)
        } else {
            UndeliveredOutboxStatusScanV1::exact_end(
                request,
                OutboxRecoveryUpperFenceV1::Inclusive(inclusive_upper),
                items,
            )
            .map_err(stored_value_error)
        }
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
        access.ensure_outbox_indexes_available()?;
        let current = {
            let transaction = access.transaction()?;
            let events = transaction.open_table(EVENTS).map_err(table_error)?;
            let intents = transaction.open_table(OUTBOX).map_err(table_error)?;
            let statuses = transaction.open_table(OUTBOX_STATUS).map_err(table_error)?;
            let commits = transaction.open_table(COMMITS).map_err(table_error)?;
            reciprocal_outbox_item(self, &events, &intents, &statuses, &commits, event_id)?
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
        access.commit_for_with_delta(
            RedbTestOperation::OutboxTransition,
            Some(TransientIndexDelta::PendingOutboxMembership {
                event_id,
                was_pending: expected.is_pending(),
                pending: updated.state().is_pending(),
                was_undelivered: status_is_undelivered(expected),
                undelivered: !matches!(
                    updated.state(),
                    riffdb_storage_api::OutboxDeliveryStateV1::Delivered { .. }
                ),
            }),
        )?;
        Ok(OutboxTransitionResultV1::Applied(updated.clone()))
    }
}

fn status_is_undelivered(status: &OutboxStatusObservationV1) -> bool {
    !matches!(
        status,
        OutboxStatusObservationV1::Present(stored)
            if matches!(
                stored.state(),
                riffdb_storage_api::OutboxDeliveryStateV1::Delivered { .. }
            )
    )
}

struct ReciprocalOutboxItem {
    event: StoredDurableEventV1,
    intent: StoredOutboxIntentV1,
    status: OutboxStatusObservationV1,
    encoded_bytes: usize,
}

fn reciprocal_outbox_item_access<S>(
    access: &RedbReadAccess,
    statuses: &S,
    event_id: EventId,
) -> Result<Option<ReciprocalOutboxItem>, StorageError>
where
    S: ReadableTable<&'static [u8], &'static [u8]>,
{
    let key = encode_event_key(event_id);
    if let Some(command) = command_member_at_access(access, event_id.commit_sequence())? {
        let command = command.base();
        let commit = command.commit();
        let ordinal = usize::try_from(event_id.event_ordinal()).map_err(|_| corrupt())?;
        let event = commit.events().get(ordinal).ok_or_else(corrupt)?.clone();
        if event.event_id() != event_id || commit.outbox_event_ids().get(ordinal) != Some(&event_id)
        {
            return Err(corrupt());
        }
        let intent = StoredOutboxIntentV1::new(event.clone());
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
        let event_bytes = encode_durable_event_v1(&event)?.as_bytes().len();
        let intent_bytes = encode_outbox_intent_v1(&intent)?.as_bytes().len();
        let encoded_bytes = event_bytes
            .checked_add(intent_bytes)
            .and_then(|bytes| {
                status.as_ref().map_or(Some(bytes), |status| {
                    bytes.checked_add(status.encoded_content_charge().get())
                })
            })
            .ok_or_else(corrupt)?;
        let status = status.map_or(OutboxStatusObservationV1::AbsentInitialPending, |status| {
            OutboxStatusObservationV1::Present(status.into_parts().0)
        });
        return Ok(Some(ReciprocalOutboxItem {
            event,
            intent,
            status,
            encoded_bytes,
        }));
    }
    let event = access
        .read_value(JournalTable::Events, &key)?
        .map(|encoded| decode_durable_event_v1(&encoded))
        .transpose()?;
    let encoded_intent = access.read_value(JournalTable::Outbox, &key)?;
    let intent = match (encoded_intent, event.as_ref()) {
        (Some(encoded), Some(event)) => Some(decode_outbox_intent_with_event(
            &encoded,
            event.value().clone(),
        )?),
        (Some(_), None) => return Err(corrupt()),
        (None, _) => None,
    };
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
    let commit = commit_at_access(access, event_id.commit_sequence())?;
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

fn reciprocal_outbox_item<E, I, S, C>(
    ports: &RedbOperationalPorts,
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
    if let Some(command) = ports.indexed_command_at(event_id.commit_sequence())? {
        let ordinal = usize::try_from(event_id.event_ordinal()).map_err(|_| corrupt())?;
        let event = command.events().get(ordinal).ok_or_else(corrupt)?.clone();
        if event.event_id() != event_id {
            return Err(corrupt());
        }
        let intent = StoredOutboxIntentV1::new(event.clone());
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
        let event_bytes = encode_durable_event_v1(&event)?.as_bytes().len();
        let intent_bytes = encode_outbox_intent_v1(&intent)?.as_bytes().len();
        let encoded_bytes = event_bytes
            .checked_add(intent_bytes)
            .and_then(|bytes| {
                status.as_ref().map_or(Some(bytes), |status| {
                    bytes.checked_add(status.encoded_content_charge().get())
                })
            })
            .ok_or_else(corrupt)?;
        let status = status.map_or(OutboxStatusObservationV1::AbsentInitialPending, |status| {
            OutboxStatusObservationV1::Present(status.into_parts().0)
        });
        return Ok(Some(ReciprocalOutboxItem {
            event,
            intent,
            status,
            encoded_bytes,
        }));
    }
    let event = events
        .get(key.as_slice())
        .map_err(precommit_storage_error)?
        .map(|encoded| decode_durable_event_v1(encoded.value()))
        .transpose()?;
    let encoded_intent = intents
        .get(key.as_slice())
        .map_err(precommit_storage_error)?
        .map(|encoded| encoded.value().to_vec());
    let intent = match (encoded_intent, event.as_ref()) {
        (Some(encoded), Some(event)) => Some(decode_outbox_intent_with_event(
            &encoded,
            event.value().clone(),
        )?),
        (Some(_), None) => return Err(corrupt()),
        (None, _) => None,
    };
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

    let commit = read_commit(commits, events, event_id.commit_sequence())?;
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
        let transaction = self.begin_composite_read()?;
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
            let events = transaction.open_table(EVENTS).map_err(table_error)?;
            if read_commit(&commits, &events, request.sequence())?.is_none() {
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
        access.commit_for(RedbTestOperation::ProjectionMutation)?;
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
            let events = transaction.open_table(EVENTS).map_err(table_error)?;
            authoritative_head(&commits, &events)?
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
        access.commit_for(RedbTestOperation::ProjectionMutation)?;
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
                generation: None,
                current: FrontierPosition::BeforeFirst,
                reason: ProjectionUnavailableReason::Building,
            });
        };
        match control.lifecycle() {
            ProjectionLifecycleV1::Building | ProjectionLifecycleV1::CatchingUp => {
                let candidate = control.candidate().ok_or_else(corrupt)?;
                Ok(ProjectionQueryResult::Degraded {
                    generation: Some(candidate.generation()),
                    current: candidate.frontier(),
                    reason: ProjectionUnavailableReason::Building,
                })
            }
            ProjectionLifecycleV1::Rebuilding => {
                let published = control.published().ok_or_else(corrupt)?;
                Ok(ProjectionQueryResult::Degraded {
                    generation: Some(published.generation()),
                    current: published.frontier(),
                    reason: ProjectionUnavailableReason::Rebuilding,
                })
            }
            ProjectionLifecycleV1::Degraded => {
                let failure = control.failure().ok_or_else(corrupt)?;
                let current = control
                    .frontier_for(failure.generation())
                    .ok_or_else(corrupt)?;
                Ok(ProjectionQueryResult::Degraded {
                    generation: Some(failure.generation()),
                    current,
                    reason: ProjectionUnavailableReason::Failure(failure.code()),
                })
            }
            ProjectionLifecycleV1::Invalid => {
                let failure = control.failure().ok_or_else(corrupt)?;
                let current = control
                    .frontier_for(failure.generation())
                    .ok_or_else(corrupt)?;
                Ok(ProjectionQueryResult::Invalid {
                    generation: failure.generation(),
                    current,
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
        let transaction = self.begin_composite_read()?;
        let controls = transaction
            .open_table(PROJECTION_FRONTIER)
            .map_err(table_error)?;
        let head = transaction.application_frontier()?.map_or(
            FrontierPosition::BeforeFirst,
            FrontierPosition::AppliedThrough,
        );
        Ok(read_projection_control(&controls, identity)?.map_or_else(
            || ProjectionStatus::uninitialized(identity.clone(), head),
            |control| ProjectionStatus::from_control(&control, head),
        ))
    }
}

impl ProjectionRecoveryRepository for RedbOperationalPorts {
    fn scan_projection_controls(
        &self,
        after: Option<&ProjectionIdentity>,
        limit: ProjectionRecoveryPageLimit,
    ) -> Result<ProjectionControlScanV1, StorageError> {
        let transaction = self.begin_read()?;
        let controls = transaction
            .open_table(PROJECTION_FRONTIER)
            .map_err(table_error)?;
        let lower = after.map(|identity| {
            ProjectionFrontierKey::new(identity.clone())
                .as_bytes()
                .to_vec()
        });
        let bounds = lower
            .as_deref()
            .map_or((Unbounded, Unbounded), |lower| (Excluded(lower), Unbounded));
        let mut scan = controls
            .range::<&[u8]>(bounds)
            .map_err(precommit_storage_error)?;
        let maximum = usize::from(limit.get().get());
        let mut result = Vec::with_capacity(maximum);
        let mut bytes = 0usize;
        let mut has_more = false;
        for entry in &mut scan {
            let (physical_key, encoded) = entry.map_err(precommit_storage_error)?;
            if result.len() == maximum {
                has_more = true;
                break;
            }
            let key =
                decode_projection_frontier_key(physical_key.value()).map_err(|_| corrupt())?;
            let decoded = decode_projection_control_v1(encoded.value())?;
            if decoded.value().identity() != key.identity() {
                return Err(corrupt());
            }
            let next = bytes
                .checked_add(decoded.encoded_content_charge().get())
                .ok_or_else(corrupt)?;
            if next > riffdb_storage_api::MAX_SCAN_PAGE_BYTES {
                if result.is_empty() {
                    return Err(storage_error(StorageErrorKind::LimitExceeded));
                }
                has_more = true;
                break;
            }
            bytes = next;
            result.push(decoded);
        }
        ProjectionControlScanV1::page(result, has_more).map_err(stored_value_error)
    }

    fn validate_projection_recovery_page(
        &self,
        request: &ProjectionRecoveryValidationRequestV1,
    ) -> Result<ProjectionRecoveryValidationResultV1, StorageError> {
        let transaction = self.begin_read()?;
        let controls = transaction
            .open_table(PROJECTION_FRONTIER)
            .map_err(table_error)?;
        let commits = transaction.open_table(COMMITS).map_err(table_error)?;
        let events = transaction.open_table(EVENTS).map_err(table_error)?;
        let markers = transaction
            .open_table(PROJECTION_APPLIED)
            .map_err(table_error)?;
        let rows = transaction
            .open_table(PROJECTION_STATE)
            .map_err(table_error)?;
        let control = match read_projection_control(&controls, request.schema().identity()) {
            Ok(control) => control,
            Err(error) if error.kind() == StorageErrorKind::CorruptData => {
                return Ok(redb_projection_recovery_finding(
                    request,
                    ProjectionRecoveryFindingCodeV1::MalformedDerivedRecord,
                ));
            }
            Err(error) => return Err(error),
        };
        if control.as_ref() != Some(request.expected_control())
            || authoritative_head(&commits, &events)? != request.expected_authoritative_head()
        {
            return Ok(ProjectionRecoveryValidationResultV1::FenceChanged);
        }

        if request.expected_position().frontier() == FrontierPosition::BeforeFirst {
            if projection_marker_namespace_has_any(
                &markers,
                request.schema().identity(),
                request.generation(),
            )? || projection_state_namespace_has_any(
                &rows,
                request.schema().identity(),
                request.generation(),
            )? {
                return Ok(redb_projection_recovery_finding(
                    request,
                    ProjectionRecoveryFindingCodeV1::BeforeFirstNotEmpty,
                ));
            }
            return Ok(ProjectionRecoveryValidationResultV1::ExactEnd(
                CleanProjectionGenerationV1::new(
                    request.schema().identity().clone(),
                    request.expected_position(),
                    0,
                ),
            ));
        }

        match request.expected_page() {
            ProjectionRecoveryExpectedPageV1::Markers(expected) => {
                validate_redb_projection_marker_page(&markers, &commits, &events, request, expected)
            }
            ProjectionRecoveryExpectedPageV1::Rows(expected) => {
                validate_redb_projection_state_page(
                    &rows, &markers, &commits, &events, request, expected,
                )
            }
        }
    }
}

fn validate_redb_projection_marker_page<M, C, E>(
    markers: &M,
    commits: &C,
    events: &E,
    request: &ProjectionRecoveryValidationRequestV1,
    expected: &[StoredProjectionApplyV1],
) -> Result<ProjectionRecoveryValidationResultV1, StorageError>
where
    M: ReadableTable<&'static [u8], &'static [u8]>,
    C: ReadableTable<&'static [u8], &'static [u8]>,
    E: ReadableTable<&'static [u8], &'static [u8]>,
{
    let after = match request.continuation() {
        None => None,
        Some(ProjectionRecoveryContinuationV1::Markers { after }) => Some(*after),
        Some(ProjectionRecoveryContinuationV1::Rows { .. }) => {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
    };
    let prefix =
        projection_marker_namespace_prefix(request.schema().identity(), request.generation());
    let lower = after.map(|sequence| {
        ProjectionApplyKey::new(
            request.schema().identity().clone(),
            request.generation(),
            sequence,
        )
        .as_bytes()
        .to_vec()
    });
    let bounds = lower.as_deref().map_or(
        (std::ops::Bound::Included(prefix.as_slice()), Unbounded),
        |lower| (Excluded(lower), Unbounded),
    );
    let mut scan = markers
        .range::<&[u8]>(bounds)
        .map_err(precommit_storage_error)?;
    let maximum = usize::from(request.limit().get().get());
    let mut actual = Vec::with_capacity(maximum);
    let mut bytes = 0usize;
    let mut has_more = false;
    let mut first_extra = None;
    for entry in &mut scan {
        let (physical_key, encoded) = entry.map_err(precommit_storage_error)?;
        if !physical_key.value().starts_with(&prefix) {
            break;
        }
        let key = match riffdb_types::ProjectionApplyKey::from_bytes(physical_key.value().to_vec())
        {
            Ok(key) => key,
            Err(_) => {
                return Ok(redb_projection_recovery_finding(
                    request,
                    ProjectionRecoveryFindingCodeV1::MalformedDerivedRecord,
                ));
            }
        };
        if actual.len() == maximum {
            has_more = true;
            first_extra = Some(key.commit_sequence());
            break;
        }
        let decoded = match decode_projection_apply_v1(encoded.value()) {
            Ok(decoded) => decoded,
            Err(_) => {
                return Ok(redb_projection_recovery_finding(
                    request,
                    ProjectionRecoveryFindingCodeV1::MalformedDerivedRecord,
                ));
            }
        };
        if decoded.value().key() != &key {
            return Ok(redb_projection_recovery_finding(
                request,
                ProjectionRecoveryFindingCodeV1::MalformedDerivedRecord,
            ));
        }
        let next = bytes
            .checked_add(decoded.encoded_content_charge().get())
            .ok_or_else(corrupt)?;
        if next > riffdb_storage_api::MAX_SCAN_PAGE_BYTES {
            if actual.is_empty() {
                return Err(storage_error(StorageErrorKind::LimitExceeded));
            }
            has_more = true;
            first_extra = Some(key.commit_sequence());
            break;
        }
        bytes = next;
        actual.push(decoded.into_parts().0);
    }
    if actual.len() != expected.len() {
        return Ok(redb_projection_recovery_finding(
            request,
            ProjectionRecoveryFindingCodeV1::MarkerMismatch,
        ));
    }
    let mut next_sequence =
        after.map_or(Some(CommitSequence::first()), CommitSequence::checked_next);
    let frontier = request.expected_position().frontier();
    for (actual, expected) in actual.iter().zip(expected) {
        let sequence = actual.key().commit_sequence();
        if actual.key().identity() != request.schema().identity()
            || actual.key().generation() != request.generation()
            || Some(sequence) != next_sequence
        {
            return Ok(redb_projection_recovery_finding(
                request,
                ProjectionRecoveryFindingCodeV1::MarkerSequenceMismatch,
            ));
        }
        if !sequence_is_at_or_before(sequence, frontier) {
            return Ok(redb_projection_recovery_finding(
                request,
                ProjectionRecoveryFindingCodeV1::MarkerAboveFrontier,
            ));
        }
        if read_commit(commits, events, sequence)?.is_none() {
            return Err(corrupt());
        }
        if actual != expected {
            return Ok(redb_projection_recovery_finding(
                request,
                ProjectionRecoveryFindingCodeV1::MarkerMismatch,
            ));
        }
        next_sequence = sequence.checked_next();
    }
    if has_more {
        if first_extra.is_some_and(|sequence| !sequence_is_at_or_before(sequence, frontier)) {
            return Ok(redb_projection_recovery_finding(
                request,
                ProjectionRecoveryFindingCodeV1::MarkerAboveFrontier,
            ));
        }
        let Some(last) = actual.last() else {
            return Ok(redb_projection_recovery_finding(
                request,
                ProjectionRecoveryFindingCodeV1::MarkerMismatch,
            ));
        };
        return Ok(ProjectionRecoveryValidationResultV1::Page {
            continuation: ProjectionRecoveryContinuationV1::markers(last.key().commit_sequence()),
        });
    }
    let validated_through = actual
        .last()
        .map(|marker| marker.key().commit_sequence())
        .or(after);
    if !matches!(
        frontier,
        FrontierPosition::AppliedThrough(frontier) if validated_through == Some(frontier)
    ) {
        return Ok(redb_projection_recovery_finding(
            request,
            ProjectionRecoveryFindingCodeV1::MarkerSequenceMismatch,
        ));
    }
    Ok(ProjectionRecoveryValidationResultV1::Page {
        continuation: ProjectionRecoveryContinuationV1::rows(None, 0),
    })
}

fn validate_redb_projection_state_page<S, M, C, E>(
    rows: &S,
    markers: &M,
    commits: &C,
    events: &E,
    request: &ProjectionRecoveryValidationRequestV1,
    expected: &[StoredProjectionStateV1],
) -> Result<ProjectionRecoveryValidationResultV1, StorageError>
where
    S: ReadableTable<&'static [u8], &'static [u8]>,
    M: ReadableTable<&'static [u8], &'static [u8]>,
    C: ReadableTable<&'static [u8], &'static [u8]>,
    E: ReadableTable<&'static [u8], &'static [u8]>,
{
    let (after, validated_rows) = match request.continuation() {
        Some(ProjectionRecoveryContinuationV1::Rows {
            after,
            validated_rows,
        }) => (after.as_ref(), *validated_rows),
        None | Some(ProjectionRecoveryContinuationV1::Markers { .. }) => {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
    };
    let prefix = riffdb_types::ProjectionGroupPrefixBuilder::new(
        request.schema().identity().clone(),
        request.generation(),
    )
    .finish();
    let bounds = after.map_or(
        (std::ops::Bound::Included(prefix.as_bytes()), Unbounded),
        |after| (Excluded(after.as_bytes()), Unbounded),
    );
    let mut scan = rows
        .range::<&[u8]>(bounds)
        .map_err(precommit_storage_error)?;
    let maximum = usize::from(request.limit().get().get());
    let mut actual = Vec::with_capacity(maximum);
    let mut bytes = 0usize;
    let mut has_more = false;
    for entry in &mut scan {
        let (physical_key, encoded) = entry.map_err(precommit_storage_error)?;
        if !physical_key.value().starts_with(prefix.as_bytes()) {
            break;
        }
        if actual.len() == maximum {
            has_more = true;
            break;
        }
        let key = match decode_projection_group_key(physical_key.value()) {
            Ok(key) => key,
            Err(_) => {
                return Ok(redb_projection_recovery_finding(
                    request,
                    ProjectionRecoveryFindingCodeV1::MalformedDerivedRecord,
                ));
            }
        };
        let decoded = match decode_projection_state_v1(encoded.value(), request.schema()) {
            Ok(decoded) => decoded,
            Err(_) => {
                return Ok(redb_projection_recovery_finding(
                    request,
                    ProjectionRecoveryFindingCodeV1::MalformedDerivedRecord,
                ));
            }
        };
        if decoded.value().key() != &key {
            return Ok(redb_projection_recovery_finding(
                request,
                ProjectionRecoveryFindingCodeV1::MalformedDerivedRecord,
            ));
        }
        let next = bytes
            .checked_add(decoded.encoded_content_charge().get())
            .ok_or_else(corrupt)?;
        if next > riffdb_storage_api::MAX_SCAN_PAGE_BYTES {
            if actual.is_empty() {
                return Err(storage_error(StorageErrorKind::LimitExceeded));
            }
            has_more = true;
            break;
        }
        bytes = next;
        actual.push(decoded.into_parts().0);
    }
    if actual.len() != expected.len() {
        return Ok(redb_projection_recovery_finding(
            request,
            ProjectionRecoveryFindingCodeV1::StateMismatch,
        ));
    }
    let frontier = request.expected_position().frontier();
    for (actual, expected) in actual.iter().zip(expected) {
        if actual.identity() != request.schema().identity()
            || actual.generation() != request.generation()
            || !sequence_is_at_or_before(actual.last_changed_sequence(), frontier)
        {
            return Ok(redb_projection_recovery_finding(
                request,
                ProjectionRecoveryFindingCodeV1::StateLinkMismatch,
            ));
        }
        let marker_key = ProjectionApplyKey::new(
            request.schema().identity().clone(),
            request.generation(),
            actual.last_changed_sequence(),
        );
        match read_projection_marker(markers, &marker_key) {
            Ok(Some(_)) => {}
            Ok(None) => {
                return Ok(redb_projection_recovery_finding(
                    request,
                    ProjectionRecoveryFindingCodeV1::StateLinkMismatch,
                ));
            }
            Err(error) if error.kind() == StorageErrorKind::CorruptData => {
                return Ok(redb_projection_recovery_finding(
                    request,
                    ProjectionRecoveryFindingCodeV1::MalformedDerivedRecord,
                ));
            }
            Err(error) => return Err(error),
        }
        if read_commit(commits, events, actual.last_changed_sequence())?.is_none() {
            return Err(corrupt());
        }
        if actual != expected {
            return Ok(redb_projection_recovery_finding(
                request,
                ProjectionRecoveryFindingCodeV1::StateMismatch,
            ));
        }
    }
    let observed =
        u64::try_from(actual.len()).map_err(|_| storage_error(StorageErrorKind::LimitExceeded))?;
    let validated_rows = validated_rows
        .checked_add(observed)
        .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
    if has_more {
        let Some(last) = actual.last() else {
            return Ok(redb_projection_recovery_finding(
                request,
                ProjectionRecoveryFindingCodeV1::StateMismatch,
            ));
        };
        return Ok(ProjectionRecoveryValidationResultV1::Page {
            continuation: ProjectionRecoveryContinuationV1::rows(
                Some(last.key().clone()),
                validated_rows,
            ),
        });
    }
    Ok(ProjectionRecoveryValidationResultV1::ExactEnd(
        CleanProjectionGenerationV1::new(
            request.schema().identity().clone(),
            request.expected_position(),
            validated_rows,
        ),
    ))
}

fn projection_marker_namespace_prefix(
    identity: &ProjectionIdentity,
    generation: ProjectionGeneration,
) -> Vec<u8> {
    let first = ProjectionApplyKey::new(identity.clone(), generation, CommitSequence::first());
    first.as_bytes()[..first.as_bytes().len() - std::mem::size_of::<u64>()].to_vec()
}

fn projection_marker_namespace_has_any<T>(
    table: &T,
    identity: &ProjectionIdentity,
    generation: ProjectionGeneration,
) -> Result<bool, StorageError>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
{
    let prefix = projection_marker_namespace_prefix(identity, generation);
    let mut scan = table
        .range::<&[u8]>(prefix.as_slice()..)
        .map_err(precommit_storage_error)?;
    let Some(entry) = scan.next() else {
        return Ok(false);
    };
    let (key, _) = entry.map_err(precommit_storage_error)?;
    Ok(key.value().starts_with(&prefix))
}

fn projection_state_namespace_has_any<T>(
    table: &T,
    identity: &ProjectionIdentity,
    generation: ProjectionGeneration,
) -> Result<bool, StorageError>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
{
    let prefix =
        riffdb_types::ProjectionGroupPrefixBuilder::new(identity.clone(), generation).finish();
    let mut scan = table
        .range::<&[u8]>(prefix.as_bytes()..)
        .map_err(precommit_storage_error)?;
    let Some(entry) = scan.next() else {
        return Ok(false);
    };
    let (key, _) = entry.map_err(precommit_storage_error)?;
    Ok(key.value().starts_with(prefix.as_bytes()))
}

fn redb_projection_recovery_finding(
    request: &ProjectionRecoveryValidationRequestV1,
    code: ProjectionRecoveryFindingCodeV1,
) -> ProjectionRecoveryValidationResultV1 {
    ProjectionRecoveryValidationResultV1::Finding(ProjectionRecoveryFindingV1::new(
        request.schema().identity().clone(),
        request.generation(),
        code,
    ))
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
    events: &impl ReadableTable<&'static [u8], &'static [u8]>,
    sequence: CommitSequence,
) -> Result<Option<StoredCommitRecordV1>, StorageError>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
{
    commit_at(table, events, sequence)
}

fn authoritative_head<T>(
    table: &T,
    events: &impl ReadableTable<&'static [u8], &'static [u8]>,
) -> Result<FrontierPosition, StorageError>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
{
    Ok(command_authority_head(table, events)?.map_or(
        FrontierPosition::BeforeFirst,
        FrontierPosition::AppliedThrough,
    ))
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

    use redb::ReadableTableMetadata;
    use riffdb_contract_compiler::compile_contract_source;
    use riffdb_storage_api::{
        DatabaseInitializationPort, DeclaredOutcome, DurabilityMode, ExecutablePlanRef,
        OutboxDeliveryStateV1, OutboxDestinationIdV1, OutboxRetryV1, ProjectionQuerySelector,
        ProjectionRowUpdateV1, ReadDependencies, StoredReadDependenciesV1, derive_event_hash_v1,
    };
    use riffdb_types::{
        ActorId, ActorKind, AdmittedActorContext, AggregateTypeId, CanonicalInputHash,
        CanonicalRecord, CanonicalValue, CommandId, ContractBundleHash, ContractLineage,
        ContractVersion, DatabaseId, EventTypeId, FieldId, LogicalTime, OutcomeId,
        PartitionKeyBuilder, PlanHash, ProjectionId, ProjectionPlanHash, ProvenanceId, RequestId,
        TenantScope, Timestamp, hash_partition_key,
    };

    use super::*;
    use crate::codec::{
        encode_commit_record_v1, encode_durable_event_v1, encode_outbox_intent_v1,
        encode_projection_control_v1,
    };
    use crate::layout::{COMMITS, EVENTS, OUTBOX, OUTBOX_STATUS, PROJECTION_FRONTIER};
    use crate::store::RedbStore;

    /// Whole-directory scope: the database and every side file it grows live
    /// in one [`crate::test_path::ScopedDirectory`] removed on drop — pass,
    /// fail, or panic.
    struct TestDatabasePath(
        PathBuf,
        // Held only so `Drop` removes the whole scope.
        #[allow(dead_code)] crate::test_path::ScopedDirectory,
    );

    impl TestDatabasePath {
        fn new(label: &str) -> Self {
            let scope = crate::test_path::ScopedDirectory::new(label);
            Self(scope.join("db.redb"), scope)
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
        let dormant = crate::store::RedbDormantPorts {
            shared: store.shared,
        };
        let ports = dormant
            .into_operational_after_catalog_validation()
            .expect("activate test ports");
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

    fn recovery_projection_schema() -> riffdb_storage_api::CheckedProjectionSchema {
        let source = "\
contract Recovery version 1 {
  event Source { group: string<16> }
  projection Totals {
    source event Source
    key (group)
    measure total = count()
    frontier transactionally_ordered
  }
}
";
        riffdb_storage_api::CheckedProjectionSchema::new(
            compile_contract_source(source)
                .expect("recovery contract")
                .bound_projection_group_schema(ProjectionId::first())
                .expect("recovery projection"),
        )
    }

    fn recovery_measures(count: u64) -> CanonicalRecord {
        CanonicalRecord::new(vec![(FieldId::first(), CanonicalValue::U64(count))])
            .expect("recovery measures")
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
        access
            .commit_for_with_delta(
                RedbTestOperation::CommandBatch,
                Some(TransientIndexDelta::PendingOutboxInserted(
                    events.iter().map(StoredDurableEventV1::event_id).collect(),
                )),
            )
            .expect("commit seed transaction");
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
    fn outbox_accelerators_rebuild_every_undelivered_state_after_reopen() {
        let (path, mut ports) = operational("outbox-index-rebuild");
        let events = seed_command(&ports, CommitSequence::first(), 5);
        let event_ids = events
            .iter()
            .map(StoredDurableEventV1::event_id)
            .collect::<Vec<_>>();
        let claim = |event_id, started, deadline| {
            OutboxClaimV1::new(
                event_id,
                OutboxStatusObservationV1::AbsentInitialPending,
                destination(),
                timestamp(started),
                timestamp(deadline),
            )
            .expect("claim")
        };
        let delivered = match ports
            .claim_outbox(&claim(event_ids[0], 10, 20))
            .expect("claim")
        {
            OutboxTransitionResultV1::Applied(status) => status,
            result => panic!("unexpected claim result: {result:?}"),
        };
        assert!(matches!(
            ports
                .succeed_outbox(&OutboxSucceedV1::new(delivered, timestamp(21)).expect("success"))
                .expect("success result"),
            OutboxTransitionResultV1::Applied(_)
        ));
        assert!(matches!(
            ports
                .claim_outbox(&claim(event_ids[1], 30, 40))
                .expect("delivering claim"),
            OutboxTransitionResultV1::Applied(_)
        ));
        assert!(matches!(
            ports
                .dead_letter_outbox(
                    &OutboxDeadLetterV1::new(
                        event_ids[2],
                        OutboxStatusObservationV1::AbsentInitialPending,
                        Some(destination()),
                        timestamp(50),
                        None,
                    )
                    .expect("dead letter")
                )
                .expect("dead-letter result"),
            OutboxTransitionResultV1::Applied(_)
        ));
        let explicit_pending = match ports
            .claim_outbox(&claim(event_ids[3], 60, 70))
            .expect("claim")
        {
            OutboxTransitionResultV1::Applied(status) => status,
            result => panic!("unexpected claim result: {result:?}"),
        };
        assert!(matches!(
            ports
                .retry_outbox(
                    &OutboxRetryV1::new(explicit_pending, Some(timestamp(80)), None)
                        .expect("retry")
                )
                .expect("retry result"),
            OutboxTransitionResultV1::Applied(_)
        ));
        drop(ports);

        let store = RedbStore::open(&path.0).expect("reopen store");
        let dormant = crate::store::RedbDormantPorts {
            shared: store.shared,
        };
        let reopened = dormant
            .into_operational_after_catalog_validation()
            .expect("rebuild transient indexes");
        let limit = OutboxPageLimit::new(NonZeroU16::new(5).expect("nonzero")).expect("limit");
        let PendingOutboxScanV1::ExactEnd { items } = reopened
            .scan_pending_outbox(None, limit)
            .expect("scan rebuilt pending index")
        else {
            panic!("explicit and absent pending rows reach exact end");
        };
        assert_eq!(
            items
                .iter()
                .map(|item| item.value().event_id())
                .collect::<Vec<_>>(),
            vec![event_ids[3], event_ids[4]]
        );
        let undelivered = reopened
            .scan_undelivered_outbox_statuses(UndeliveredOutboxStatusScanRequestV1::initial(
                None, limit,
            ))
            .expect("scan rebuilt undelivered index");
        assert_eq!(
            undelivered
                .items()
                .iter()
                .map(|item| item.value().event_id())
                .collect::<Vec<_>>(),
            event_ids[1..].to_vec()
        );
        assert_eq!(undelivered.continuation(), None);
    }

    #[test]
    fn malformed_delivery_status_degrades_outbox_without_blocking_core_activation() {
        let (path, ports) = operational("outbox-index-degraded");
        let event_id = seed_command(&ports, CommitSequence::first(), 1)[0].event_id();
        let access = ports.begin_write().expect("begin corruption transaction");
        {
            let mut statuses = access
                .transaction()
                .expect("corruption transaction")
                .open_table(OUTBOX_STATUS)
                .expect("status table");
            let key = encode_event_key(event_id);
            statuses
                .insert(key.as_slice(), b"not-a-stored-envelope".as_slice())
                .expect("insert malformed derived status");
        }
        access.commit().expect("commit derived corruption");
        drop(ports);

        let store = RedbStore::open(&path.0).expect("reopen store");
        let dormant = crate::store::RedbDormantPorts {
            shared: store.shared,
        };
        let reopened = dormant
            .into_operational_after_catalog_validation()
            .expect("derived corruption does not block core activation");
        let transaction = reopened.begin_read().expect("core read remains available");
        let commits = transaction.open_table(COMMITS).expect("commit table");
        let key = encode_application_sequence_key(CommitSequence::first());
        assert!(commits.get(key.as_slice()).expect("read commit").is_some());
        drop(commits);
        drop(transaction);

        let limit = OutboxPageLimit::new(NonZeroU16::MIN).expect("limit");
        assert_eq!(
            reopened
                .scan_pending_outbox(None, limit)
                .expect_err("degraded outbox index is unavailable")
                .kind(),
            StorageErrorKind::Unavailable
        );
        assert_eq!(
            reopened
                .scan_undelivered_outbox_statuses(UndeliveredOutboxStatusScanRequestV1::initial(
                    None, limit
                ),)
                .expect_err("degraded undelivered index is unavailable")
                .kind(),
            StorageErrorKind::Unavailable
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
    fn projection_recovery_survives_reopen_and_fails_closed_on_corruption_or_fence_change() {
        let (path, mut ports) = operational("projection-recovery");
        let sequence = CommitSequence::first();
        seed_command(&ports, sequence, 0);
        let schema = recovery_projection_schema();
        let generation = ProjectionGeneration::first();
        let initial = StoredProjectionControlV1::new(
            schema.identity().clone(),
            generation,
            None,
            Some(riffdb_storage_api::ProjectionGenerationPosition::new(
                generation,
                FrontierPosition::BeforeFirst,
            )),
            None,
            ProjectionLifecycleV1::CatchingUp,
            None,
        )
        .expect("catching-up control");
        install_control(&ports, schema.identity(), &initial);
        let key = schema
            .group_key(
                generation,
                &[CanonicalValue::string("group-a").expect("group value")],
            )
            .expect("projection group key");
        let measures = recovery_measures(1);
        let apply = ProjectionApplyRequestV1::new(
            schema.clone(),
            generation,
            sequence,
            FrontierPosition::BeforeFirst,
            vec![
                ProjectionRowUpdateV1::new(
                    &schema,
                    key.clone(),
                    ProjectionRowPrior::Absent,
                    measures.clone(),
                )
                .expect("projection update"),
            ],
        )
        .expect("projection apply");
        let ProjectionApplyResult::Applied { marker, control } =
            ports.apply_projection(&apply).expect("apply projection")
        else {
            panic!("projection apply must advance the retained candidate");
        };
        let expected_row =
            StoredProjectionStateV1::new(&schema, key, measures, sequence).expect("expected row");
        let other_control = StoredProjectionControlV1::initial(projection_identity(0x52));
        install_control(&ports, other_control.identity(), &other_control);
        drop(ports);

        let store = RedbStore::open(&path.0).expect("reopen store");
        let dormant = crate::store::RedbDormantPorts {
            shared: store.shared,
        };
        let reopened = dormant
            .into_operational_after_catalog_validation()
            .expect("activate reopened projection store");
        let one = ProjectionRecoveryPageLimit::new(NonZeroU16::MIN).expect("one-row limit");
        let mut ordered = [control.clone(), other_control];
        ordered.sort_by(|left, right| {
            ProjectionFrontierKey::new(left.identity().clone())
                .as_bytes()
                .cmp(ProjectionFrontierKey::new(right.identity().clone()).as_bytes())
        });
        let ProjectionControlScanV1::Page {
            controls,
            next_after,
        } = reopened
            .scan_projection_controls(None, one)
            .expect("first control page")
        else {
            panic!("two controls at limit one must page");
        };
        assert_eq!(controls[0].value(), &ordered[0]);
        assert_eq!(&next_after, ordered[0].identity());
        let ProjectionControlScanV1::ExactEnd { controls } = reopened
            .scan_projection_controls(Some(&next_after), one)
            .expect("final control page")
        else {
            panic!("second control must reach exact end");
        };
        assert_eq!(controls[0].value(), &ordered[1]);

        let head = FrontierPosition::AppliedThrough(sequence);
        let marker_request = ProjectionRecoveryValidationRequestV1::new(
            schema.clone(),
            control.clone(),
            head,
            generation,
            one,
            None,
            ProjectionRecoveryExpectedPageV1::markers(vec![marker.clone()]),
        )
        .expect("marker request");
        let ProjectionRecoveryValidationResultV1::Page { continuation } = reopened
            .validate_projection_recovery_page(&marker_request)
            .expect("validate persisted marker")
        else {
            panic!("marker exact end must advance to state validation");
        };
        let row_request = ProjectionRecoveryValidationRequestV1::new(
            schema.clone(),
            control.clone(),
            head,
            generation,
            one,
            Some(continuation),
            ProjectionRecoveryExpectedPageV1::rows(vec![expected_row]),
        )
        .expect("row request");
        let ProjectionRecoveryValidationResultV1::ExactEnd(clean) = reopened
            .validate_projection_recovery_page(&row_request)
            .expect("validate persisted row")
        else {
            panic!("matching replay evidence must reach exact end");
        };
        assert_eq!(clean.position(), control.candidate().expect("candidate"));
        assert_eq!(clean.state_rows(), 1);

        let selector =
            ProjectionQuerySelector::new(schema.clone(), Vec::new()).expect("query selector");
        let query =
            ProjectionQueryRequest::new(selector, NonZeroU16::MIN, None).expect("projection query");
        assert_eq!(
            reopened
                .query_projection(&query)
                .expect("query retained catching-up projection"),
            ProjectionQueryResult::Degraded {
                generation: Some(generation),
                current: head,
                reason: ProjectionUnavailableReason::Building,
            }
        );

        let access = reopened
            .begin_write()
            .expect("begin derived corruption transaction");
        {
            let mut markers = access
                .transaction()
                .expect("corruption transaction")
                .open_table(PROJECTION_APPLIED)
                .expect("marker table");
            markers
                .insert(
                    encode_projection_apply_key(marker.key()),
                    b"malformed-projection-marker".as_slice(),
                )
                .expect("corrupt marker");
        }
        access.commit().expect("commit derived corruption");
        assert!(matches!(
            reopened
                .validate_projection_recovery_page(&marker_request)
                .expect("malformed marker is a derived finding"),
            ProjectionRecoveryValidationResultV1::Finding(finding)
                if finding.code() == ProjectionRecoveryFindingCodeV1::MalformedDerivedRecord
        ));

        seed_command(
            &reopened,
            CommitSequence::new(2).expect("second sequence"),
            0,
        );
        assert_eq!(
            reopened
                .validate_projection_recovery_page(&marker_request)
                .expect("stale head fence"),
            ProjectionRecoveryValidationResultV1::FenceChanged
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
