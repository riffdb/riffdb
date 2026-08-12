//! Durable redb persistence for backend-neutral event-consumer transitions.

use redb::ReadableTable;
use riffdb_catalog::{
    EventReplayErrorKind, EventReplayPosition, ResolvedEventReplay, SymbolicEventEnvelope,
    SymbolicEventReplayPage,
};
use riffdb_policy::{AuthorizedQueryRowPolicyContextV1, EventPolicyCandidateV1};
use riffdb_query_executor::MAX_QUERY_SCANNED_ROWS;
use riffdb_storage_api::{
    AuthoritativePointReader, CapabilityLifecycleV1, ConsumerDeliveryStateV1,
    ConsumerLeaseCandidateV1, ConsumerStateError, CoordinateConsumerAcknowledgementV1,
    CoordinateConsumerLeaseResultV1, CoordinatedConsumerLeaseV1, EncodedPageItem, EntityTarget,
    EvaluatedEventConsumerTransitionV1, EventConsumerIdentityV1, EventConsumerRepository,
    EventConsumerSnapshotV1, EventConsumerTransitionResultV1, EventConsumerTransitionV1,
    EventRoutePageLimit, EventRouteScanRequestV1, EventRouteScanV1, EventRouteUpperFenceV1,
    ExpectedConsumerDeliveryV1, IdempotencyIdentity, MAX_CONSUMER_BATCH_ITEMS,
    MAX_CONSUMER_DELIVERY_RECORDS, MAX_CONSUMER_IN_FLIGHT, MAX_CONSUMER_SPARSE_RESOLUTIONS,
    MAX_EVENT_CONSUMERS, MAX_SCAN_PAGE_BYTES, PartitionEventRouteReader,
    PreparedConsumerResolutionV1, StorageError, StorageErrorKind, StoredCommitRecordV1,
    StoredDurableEventV1, StoredEntityRecordV1, StoredEventConsumerDeliveryV1,
    StoredEventConsumerV1, StoredOutcomeV1, StoredProvenanceRecordV1,
    evaluate_consumer_lease_validation, evaluate_event_consumer_transition,
    normalize_consumer_recovery, prepare_consumer_resolution, resolve_policy_hidden_state,
    status_from_snapshot,
};
use riffdb_types::{
    CommitSequence, DatabaseId, EventConsumerIdentityHash, EventDeliveryAttempt, EventId,
    EventLeaseToken, PartitionKeyHash, ProvenanceId, Timestamp,
};

use crate::codec::{
    decode_command_locator_v1, decode_database_identity_v1, decode_durable_event_v1,
    decode_entity_record_v1, decode_event_consumer_delivery_v1, decode_event_consumer_v1,
    decode_event_route_v1, decode_index_entry_v2, decode_provenance_record_v1,
    encode_event_consumer_delivery_v1, encode_event_consumer_v1,
};
use crate::command_authority::{command_member_at, commit_at};
use crate::error::{precommit_storage_error, storage_error, table_error};
use crate::hooks::RedbTestOperation;
use crate::keys::{
    decode_event_consumer_delivery_key, decode_event_consumer_key, decode_event_route_key,
    decode_index_entry_key, encode_entity_key, encode_event_consumer_delivery_key,
    encode_event_consumer_key, encode_event_key, encode_event_route_key, encode_provenance_key,
};
use crate::layout::{
    CAPABILITIES, CAPABILITY_TOKENS, COMMITS, ENTITIES, EVENT_CONSUMER_DELIVERIES, EVENT_CONSUMERS,
    EVENT_ROUTES, EVENTS, META, META_DATABASE_ID, PROVENANCE, SECONDARY_INDEXES,
};
use crate::store::RedbOperationalPorts;

/// Maximum raw partition-route candidates examined by one protected replay.
pub const MAX_PROTECTED_EVENT_REPLAY_CANDIDATES: u16 = 1024;

/// Move-only protected replay request evaluated at one redb safe point.
pub struct ProtectedEventReplayV1 {
    /// Catalog-resolved event symbol, selected fields, and partition route.
    pub replay: ResolvedEventReplay,
    /// Exact frozen route position.
    pub position: EventReplayPosition,
    /// Maximum visible events returned.
    pub return_limit: EventRoutePageLimit,
    /// Maximum raw route candidates examined.
    pub candidate_limit: EventRoutePageLimit,
    /// Current durable history incarnation.
    pub history_incarnation: u64,
    /// Service-owned authorization instant.
    pub observed_at: Timestamp,
    /// Move-only current row-policy authority.
    pub policy: AuthorizedQueryRowPolicyContextV1,
}

/// Closed result class for one protected replay scan.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProtectedEventReplayDispositionV1 {
    /// Visible limit or the frozen route end was reached normally.
    Page,
    /// The raw candidate ceiling was reached before a visible result or end.
    BoundedProgress,
}

/// Policy-filtered symbolic page and its inference-safe work disposition.
pub struct ProtectedEventReplayPageV1 {
    page: SymbolicEventReplayPage,
    disposition: ProtectedEventReplayDispositionV1,
}

/// Closed transaction-current protected replay outcome.
pub enum ProtectedEventReplayResultV1 {
    /// Policy remained current and produced one checked page.
    Page(ProtectedEventReplayPageV1),
    /// Capability revision, lifecycle, validity, or role binding changed.
    AuthorizationChanged,
}

impl ProtectedEventReplayPageV1 {
    /// Consumes the checked protected result.
    #[must_use]
    pub fn into_parts(self) -> (SymbolicEventReplayPage, ProtectedEventReplayDispositionV1) {
        (self.page, self.disposition)
    }
}

impl std::fmt::Debug for ProtectedEventReplayV1 {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ProtectedEventReplayV1([REDACTED])")
    }
}

/// One protected consumer selection whose policy context was reconstructed by
/// the shared service from a fresh current authorization proof.
pub struct ProtectedEventConsumerLeaseV1 {
    /// Complete exact durable consumer identity.
    pub identity: EventConsumerIdentityV1,
    /// Catalog-proven selected partition.
    pub partition_hash: PartitionKeyHash,
    /// Current durable history incarnation.
    pub history_incarnation: u64,
    /// Coordinator-observed current instant.
    pub observed_at: Timestamp,
    /// Exclusive expiry for newly leased visible events.
    pub expires_at: Timestamp,
    /// Complete catalog-selected event window in exact order.
    pub selected_events: Vec<EventId>,
    /// Fresh opaque tokens for every possible visible lease.
    pub tokens: Vec<EventLeaseToken>,
    /// Maximum events returned by this call.
    pub batch_limit: u8,
    /// Maximum concurrent live leases.
    pub in_flight_limit: u8,
    /// Move-only current row-policy authority.
    pub policy: AuthorizedQueryRowPolicyContextV1,
}

/// One exact reaction lease validated together with current event authority.
pub struct ProtectedEventConsumerLeaseValidationV1 {
    /// Complete exact durable consumer identity.
    pub identity: EventConsumerIdentityV1,
    /// Catalog-resolved stream partition.
    pub partition_hash: PartitionKeyHash,
    /// Leased trigger event.
    pub event_id: EventId,
    /// Exact delivery attempt.
    pub attempt: EventDeliveryAttempt,
    /// Attempt-specific opaque token.
    pub token: EventLeaseToken,
    /// Restore fence.
    pub history_incarnation: u64,
    /// Service-owned current instant.
    pub observed_at: Timestamp,
    /// Move-only current capability and compiled row-policy authority.
    pub policy: AuthorizedQueryRowPolicyContextV1,
}

/// Inference-safe protected reaction-lease validation result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProtectedEventConsumerLeaseValidationResultV1 {
    /// Current event authority passed and the exact lease state was evaluated.
    Validation(riffdb_storage_api::CoordinatedConsumerLeaseValidationV1),
    /// Capability or current-row authority no longer permits the event.
    Denied,
}

/// One protected acknowledgement or negative acknowledgement whose policy is
/// re-evaluated inside the same redb mutation fence as consumer resolution.
pub struct ProtectedEventConsumerResolutionV1 {
    /// Ordinary checked acknowledgement intent.
    pub acknowledgement: CoordinateConsumerAcknowledgementV1,
    /// Retry/dead-letter eligibility for a negative acknowledgement.
    pub retry_at: Option<Timestamp>,
    /// Move-only current row-policy authority.
    pub policy: AuthorizedQueryRowPolicyContextV1,
}

impl std::fmt::Debug for ProtectedEventConsumerResolutionV1 {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ProtectedEventConsumerResolutionV1([REDACTED])")
    }
}

impl std::fmt::Debug for ProtectedEventConsumerLeaseV1 {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ProtectedEventConsumerLeaseV1([REDACTED])")
    }
}

impl std::fmt::Debug for ProtectedEventConsumerLeaseValidationV1 {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ProtectedEventConsumerLeaseValidationV1([REDACTED])")
    }
}

impl RedbOperationalPorts {
    /// Filters one symbolic replay before visible limiting and cursor release.
    ///
    /// The read-only operation deliberately enters the mutation fence: doing
    /// so gives capability revision, current entity rows, relationship indexes,
    /// event routes, commits, and provenance one transaction-current redb view.
    /// The transaction is always aborted and never advances durable state.
    pub fn replay_protected_events(
        &mut self,
        request: ProtectedEventReplayV1,
    ) -> Result<ProtectedEventReplayResultV1, StorageError> {
        if request.history_incarnation == 0
            || request.candidate_limit.get().get() > MAX_PROTECTED_EVENT_REPLAY_CANDIDATES
        {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        let access = self.begin_write()?;
        let transaction = access.transaction()?;
        let database_id = read_database_id_from_write(transaction)?;
        if !current_capability_matches_policy(
            transaction,
            database_id,
            request.observed_at,
            &request.policy,
        )? {
            access.abort()?;
            return Ok(ProtectedEventReplayResultV1::AuthorizationChanged);
        }

        let reader = ProtectedEventReplayReader { transaction };
        let one = EventRoutePageLimit::new(std::num::NonZeroU16::new(1).ok_or_else(corrupt)?)
            .map_err(|_| corrupt())?;
        let maximum_visible = usize::from(request.return_limit.get().get());
        let maximum_candidates = usize::from(request.candidate_limit.get().get());
        let mut position = request.position;
        let mut visible = Vec::with_capacity(maximum_visible);
        let mut continuation = None;
        let mut inclusive_upper = None;
        let mut disposition = ProtectedEventReplayDispositionV1::Page;

        for examined in 0..maximum_candidates {
            let page = request
                .replay
                .replay_page(&reader, position, one, request.history_incarnation)
                .map_err(map_event_replay_storage_error)?;
            if inclusive_upper
                .replace(page.inclusive_upper())
                .is_some_and(|prior| prior != page.inclusive_upper())
            {
                return Err(corrupt());
            }
            continuation = page.continuation();
            for event in page.into_items() {
                if authorize_symbolic_event(transaction, &event, &request.policy)? {
                    visible.push(event);
                }
            }
            if visible.len() == maximum_visible || continuation.is_none() {
                break;
            }
            position = EventReplayPosition::Continue(continuation.ok_or_else(corrupt)?);
            if examined.saturating_add(1) == maximum_candidates {
                disposition = ProtectedEventReplayDispositionV1::BoundedProgress;
            }
        }

        let inclusive_upper = inclusive_upper.unwrap_or(EventRouteUpperFenceV1::BeforeFirst);
        let page = SymbolicEventReplayPage::from_policy_filtered_parts(
            visible,
            continuation,
            inclusive_upper,
        );
        access.abort()?;
        Ok(ProtectedEventReplayResultV1::Page(
            ProtectedEventReplayPageV1 { page, disposition },
        ))
    }

    /// Validates one reaction lease and its trigger event at one safe point.
    pub fn validate_protected_event_consumer_lease(
        &mut self,
        request: ProtectedEventConsumerLeaseValidationV1,
    ) -> Result<ProtectedEventConsumerLeaseValidationResultV1, StorageError> {
        if request.history_incarnation == 0 {
            return Err(corrupt());
        }
        let access = self.begin_write()?;
        let transaction = access.transaction()?;
        let database_id = read_database_id_from_write(transaction)?;
        if request.identity.database_id() != database_id {
            return Err(corrupt());
        }
        let snapshot = {
            let consumers = transaction
                .open_table(EVENT_CONSUMERS)
                .map_err(table_error)?;
            let deliveries = transaction
                .open_table(EVENT_CONSUMER_DELIVERIES)
                .map_err(table_error)?;
            read_snapshot_from_tables(
                &consumers,
                &deliveries,
                database_id,
                request.identity.identity_hash(),
            )?
        };
        if !current_capability_matches_policy(
            transaction,
            database_id,
            request.observed_at,
            &request.policy,
        )? || authorize_selected_events(transaction, &[request.event_id], &request.policy)?
            .as_slice()
            != [request.event_id]
        {
            access.abort()?;
            return Ok(ProtectedEventConsumerLeaseValidationResultV1::Denied);
        }
        let validation = evaluate_consumer_lease_validation(
            snapshot.as_ref(),
            &request.identity,
            request.partition_hash,
            request.event_id,
            request.attempt,
            request.token,
            request.history_incarnation,
            request.observed_at,
        )?;
        access.abort()?;
        Ok(ProtectedEventConsumerLeaseValidationResultV1::Validation(
            validation,
        ))
    }

    /// Evaluates current event authority and publishes hidden resolutions plus
    /// visible leases in one redb mutation fence.
    pub fn coordinate_protected_event_consumer_lease(
        &mut self,
        request: ProtectedEventConsumerLeaseV1,
    ) -> Result<CoordinateConsumerLeaseResultV1, StorageError> {
        validate_protected_request(&request)?;
        let access = self.begin_write()?;
        let transaction = access.transaction()?;
        let database_id = read_database_id_from_write(transaction)?;
        if request.identity.database_id() != database_id {
            return Err(corrupt());
        }
        let identity_hash = request.identity.identity_hash();
        let current = {
            let consumers = transaction
                .open_table(EVENT_CONSUMERS)
                .map_err(table_error)?;
            let deliveries = transaction
                .open_table(EVENT_CONSUMER_DELIVERIES)
                .map_err(table_error)?;
            read_snapshot_from_tables(&consumers, &deliveries, database_id, identity_hash)?
        };
        if let Some(snapshot) = current.as_ref()
            && (snapshot.consumer().identity() != &request.identity
                || snapshot.consumer().partition_hash() != request.partition_hash
                || snapshot.consumer().history_incarnation() != request.history_incarnation)
        {
            return Err(corrupt());
        }

        if current.as_ref().is_some_and(|snapshot| {
            snapshot.deliveries().iter().any(|delivery| {
                matches!(
                    delivery.state(),
                    ConsumerDeliveryStateV1::Leased { expires_at, .. }
                        if request.observed_at >= expires_at
                )
            })
        }) {
            let snapshot = current.as_ref().ok_or_else(corrupt)?;
            let (replacement_consumer, replacement_deliveries) = normalize_consumer_recovery(
                snapshot,
                request.observed_at,
                request.history_incarnation,
                false,
            )?;
            let transition = EventConsumerTransitionV1::Recover {
                expected_revision: snapshot.consumer().revision(),
                observed_at: request.observed_at,
                replacement_consumer,
                replacement_deliveries,
            };
            let evaluated = evaluate_event_consumer_transition(current.clone(), transition)
                .map_err(state_error)?;
            let EvaluatedEventConsumerTransitionV1::Replace(replacement) = evaluated else {
                access.abort()?;
                return Ok(CoordinateConsumerLeaseResultV1 {
                    transition: EventConsumerTransitionResultV1::StateChanged,
                    leases: Vec::new(),
                    status: current.as_ref().map(status_from_snapshot),
                });
            };
            replace_snapshot(transaction, current.as_ref(), replacement.as_ref())?;
            access.commit_for(RedbTestOperation::EventConsumerTransition)?;
            return Ok(CoordinateConsumerLeaseResultV1 {
                transition: EventConsumerTransitionResultV1::StateChanged,
                leases: Vec::new(),
                status: Some(status_from_snapshot(replacement.as_ref())),
            });
        }

        if !current_capability_matches_policy(
            transaction,
            database_id,
            request.observed_at,
            &request.policy,
        )? {
            access.abort()?;
            return Ok(CoordinateConsumerLeaseResultV1 {
                transition: EventConsumerTransitionResultV1::StateChanged,
                leases: Vec::new(),
                status: current.as_ref().map(status_from_snapshot),
            });
        }

        let visible_events =
            authorize_selected_events(transaction, &request.selected_events, &request.policy)?;
        let live = current.as_ref().map_or(0, |snapshot| {
            snapshot
                .deliveries()
                .iter()
                .filter(|row| matches!(row.state(), ConsumerDeliveryStateV1::Leased { .. }))
                .count()
        });
        let available = usize::from(request.batch_limit)
            .min(usize::from(request.in_flight_limit).saturating_sub(live));
        let mut candidates = Vec::with_capacity(available);
        let mut leases = Vec::with_capacity(available);
        for event_id in &visible_events {
            if candidates.len() == available {
                break;
            }
            let expected = current.as_ref().and_then(|snapshot| {
                snapshot
                    .deliveries()
                    .iter()
                    .find(|row| row.event_id() == *event_id)
                    .map(StoredEventConsumerDeliveryV1::state)
            });
            let attempt = match expected {
                None => EventDeliveryAttempt::first(),
                Some(ConsumerDeliveryStateV1::Retry {
                    failed_attempts,
                    eligible_at,
                }) if eligible_at <= request.observed_at => {
                    failed_attempts.checked_next().ok_or_else(corrupt)?
                }
                Some(_) => continue,
            };
            if current.as_ref().is_some_and(|snapshot| {
                !snapshot.consumer().checkpoint().precedes(*event_id)
                    || snapshot
                        .consumer()
                        .sparse_resolutions()
                        .iter()
                        .any(|resolution| resolution.event_id() == *event_id)
            }) {
                continue;
            }
            let token = request.tokens[candidates.len()];
            let replacement = StoredEventConsumerDeliveryV1::new(
                identity_hash,
                *event_id,
                request.history_incarnation,
                ConsumerDeliveryStateV1::Leased {
                    attempt,
                    token,
                    expires_at: request.expires_at,
                },
            )
            .map_err(state_error)?;
            let expected = match expected {
                None => ExpectedConsumerDeliveryV1::Absent,
                Some(ConsumerDeliveryStateV1::Retry {
                    failed_attempts,
                    eligible_at,
                }) => ExpectedConsumerDeliveryV1::Retry {
                    failed_attempts,
                    eligible_at,
                },
                Some(_) => continue,
            };
            candidates
                .push(ConsumerLeaseCandidateV1::new(expected, replacement).map_err(state_error)?);
            leases.push(CoordinatedConsumerLeaseV1 {
                event_id: *event_id,
                attempt,
                token,
                expires_at: request.expires_at,
            });
        }

        let base = match current.as_ref() {
            Some(snapshot) => snapshot.consumer().clone(),
            None => StoredEventConsumerV1::initial(
                request.identity.clone(),
                request.partition_hash,
                request.history_incarnation,
            )
            .map_err(state_error)?,
        };
        let (checkpoint, sparse) =
            resolve_policy_hidden_state(&base, &request.selected_events, &visible_events)
                .map_err(state_error)?;
        if current.is_some()
            && candidates.is_empty()
            && checkpoint == base.checkpoint()
            && sparse == base.sparse_resolutions()
        {
            access.abort()?;
            return Ok(CoordinateConsumerLeaseResultV1 {
                transition: EventConsumerTransitionResultV1::Applied,
                leases,
                status: current.as_ref().map(status_from_snapshot),
            });
        }
        let replacement_consumer = match current.as_ref() {
            Some(snapshot) => snapshot
                .consumer()
                .advance(checkpoint, sparse)
                .map_err(state_error)?,
            None => StoredEventConsumerV1::checked(
                request.identity,
                request.partition_hash,
                request.history_incarnation,
                riffdb_types::EventConsumerRevision::first(),
                checkpoint,
                sparse,
            )
            .map_err(state_error)?,
        };
        let transition = EventConsumerTransitionV1::PolicySelect {
            consumer_identity_hash: identity_hash,
            expected_revision: current
                .as_ref()
                .map(|snapshot| snapshot.consumer().revision()),
            replacement_consumer,
            candidates,
            examined_events: request.selected_events,
            visible_events,
        };
        let evaluated =
            evaluate_event_consumer_transition(current.clone(), transition).map_err(state_error)?;
        let replacement = match evaluated {
            EvaluatedEventConsumerTransitionV1::Replace(replacement) => replacement,
            EvaluatedEventConsumerTransitionV1::NoChange(result) => {
                access.abort()?;
                leases.clear();
                return Ok(CoordinateConsumerLeaseResultV1 {
                    transition: result,
                    leases,
                    status: current.as_ref().map(status_from_snapshot),
                });
            }
            EvaluatedEventConsumerTransitionV1::Retire(_) => return Err(corrupt()),
        };
        replace_snapshot(transaction, current.as_ref(), replacement.as_ref())?;
        access.commit_for(RedbTestOperation::EventConsumerTransition)?;
        Ok(CoordinateConsumerLeaseResultV1 {
            transition: EventConsumerTransitionResultV1::Applied,
            leases,
            status: Some(status_from_snapshot(replacement.as_ref())),
        })
    }

    /// Revalidates current event authority and resolves one lease atomically.
    pub fn coordinate_protected_event_consumer_resolution(
        &mut self,
        request: ProtectedEventConsumerResolutionV1,
    ) -> Result<EventConsumerTransitionResultV1, StorageError> {
        let access = self.begin_write()?;
        let transaction = access.transaction()?;
        let database_id = read_database_id_from_write(transaction)?;
        if request.acknowledgement.identity.database_id() != database_id {
            return Err(corrupt());
        }
        let identity = request.acknowledgement.identity.identity_hash();
        let current = {
            let consumers = transaction
                .open_table(EVENT_CONSUMERS)
                .map_err(table_error)?;
            let deliveries = transaction
                .open_table(EVENT_CONSUMER_DELIVERIES)
                .map_err(table_error)?;
            read_snapshot_from_tables(&consumers, &deliveries, database_id, identity)?
        };
        let Some(snapshot) = current.as_ref() else {
            access.abort()?;
            return Ok(EventConsumerTransitionResultV1::NotFound);
        };
        if !current_capability_matches_policy(
            transaction,
            database_id,
            request.acknowledgement.observed_at,
            &request.policy,
        )? || authorize_selected_events(
            transaction,
            &[request.acknowledgement.event_id],
            &request.policy,
        )?
        .as_slice()
            != [request.acknowledgement.event_id]
        {
            access.abort()?;
            return Ok(EventConsumerTransitionResultV1::StateChanged);
        }
        let transition = match prepare_consumer_resolution(
            snapshot,
            &request.acknowledgement,
            request.retry_at,
        )? {
            PreparedConsumerResolutionV1::NoChange(result) => {
                access.abort()?;
                return Ok(result);
            }
            PreparedConsumerResolutionV1::Transition(transition) => transition,
        };
        let evaluated =
            evaluate_event_consumer_transition(current.clone(), transition).map_err(state_error)?;
        let replacement = match evaluated {
            EvaluatedEventConsumerTransitionV1::Replace(replacement) => replacement,
            EvaluatedEventConsumerTransitionV1::NoChange(result) => {
                access.abort()?;
                return Ok(result);
            }
            EvaluatedEventConsumerTransitionV1::Retire(_) => return Err(corrupt()),
        };
        replace_snapshot(transaction, current.as_ref(), replacement.as_ref())?;
        access.commit_for(RedbTestOperation::EventConsumerTransition)?;
        Ok(EventConsumerTransitionResultV1::Applied)
    }
}

fn validate_protected_request(request: &ProtectedEventConsumerLeaseV1) -> Result<(), StorageError> {
    if request.history_incarnation == 0
        || request.observed_at >= request.expires_at
        || request.batch_limit == 0
        || usize::from(request.batch_limit) > MAX_CONSUMER_BATCH_ITEMS
        || request.in_flight_limit == 0
        || usize::from(request.in_flight_limit) > MAX_CONSUMER_IN_FLIGHT
        || request.selected_events.len() > MAX_CONSUMER_SPARSE_RESOLUTIONS + 1
        || request.tokens.len() < usize::from(request.batch_limit)
        || !strict_event_order(&request.selected_events)
    {
        return Err(storage_error(StorageErrorKind::InvariantViolation));
    }
    Ok(())
}

struct ProtectedEventReplayReader<'a> {
    transaction: &'a redb::WriteTransaction,
}

impl PartitionEventRouteReader for ProtectedEventReplayReader<'_> {
    fn scan_partition_event_routes(
        &self,
        request: EventRouteScanRequestV1,
    ) -> Result<EventRouteScanV1, StorageError> {
        let table = self
            .transaction
            .open_table(EVENT_ROUTES)
            .map_err(table_error)?;
        let mut partition_lower = [0_u8; 44];
        partition_lower[..32].copy_from_slice(request.partition_hash().as_bytes());
        let mut partition_upper = [0xff_u8; 44];
        partition_upper[..32].copy_from_slice(request.partition_hash().as_bytes());
        let inclusive_upper = match request.inclusive_upper() {
            Some(event_id) => {
                let key = encode_event_route_key(request.partition_hash(), event_id);
                let row = table
                    .get(key.as_slice())
                    .map_err(precommit_storage_error)?
                    .ok_or_else(corrupt)?;
                let route = decode_event_route_v1(row.value())?.into_parts().0;
                if route.event_id() != event_id {
                    return Err(corrupt());
                }
                EventRouteUpperFenceV1::Inclusive(event_id)
            }
            None => match table
                .range(partition_lower.as_slice()..=partition_upper.as_slice())
                .map_err(precommit_storage_error)?
                .next_back()
            {
                Some(row) => {
                    let (key, value) = row.map_err(precommit_storage_error)?;
                    let (partition, event_id) =
                        decode_event_route_key(key.value()).map_err(|_| corrupt())?;
                    let route = decode_event_route_v1(value.value())?.into_parts().0;
                    if partition != request.partition_hash() || route.event_id() != event_id {
                        return Err(corrupt());
                    }
                    EventRouteUpperFenceV1::Inclusive(event_id)
                }
                None => EventRouteUpperFenceV1::BeforeFirst,
            },
        };
        let EventRouteUpperFenceV1::Inclusive(upper) = inclusive_upper else {
            return EventRouteScanV1::exact_end(request, inclusive_upper, Vec::new())
                .map_err(|_| corrupt());
        };
        if request.after().is_some_and(|after| after >= upper) {
            return EventRouteScanV1::exact_end(request, inclusive_upper, Vec::new())
                .map_err(|_| corrupt());
        }
        let start = request.after().map_or(partition_lower.to_vec(), |after| {
            encode_event_route_key(request.partition_hash(), after).to_vec()
        });
        let end = encode_event_route_key(request.partition_hash(), upper);
        let wanted = usize::from(request.limit().get().get());
        let mut items = Vec::with_capacity(wanted);
        let mut encoded_bytes = 0usize;
        let mut has_more = false;
        for row in table
            .range(start.as_slice()..=end.as_slice())
            .map_err(precommit_storage_error)?
        {
            let (key, value) = row.map_err(precommit_storage_error)?;
            let (partition, event_id) =
                decode_event_route_key(key.value()).map_err(|_| corrupt())?;
            if request.after().is_some_and(|after| event_id <= after) {
                continue;
            }
            if items.len() == wanted {
                has_more = true;
                break;
            }
            let decoded = decode_event_route_v1(value.value())?;
            let (route, charge) = decoded.into_parts();
            if partition != request.partition_hash() || route.event_id() != event_id {
                return Err(corrupt());
            }
            let next_bytes = encoded_bytes
                .checked_add(charge.get())
                .ok_or_else(corrupt)?;
            if next_bytes > MAX_SCAN_PAGE_BYTES {
                has_more = true;
                break;
            }
            encoded_bytes = next_bytes;
            items.push(EncodedPageItem::new(route, charge));
        }
        if has_more {
            EventRouteScanV1::page(request, upper, items).map_err(|_| corrupt())
        } else {
            EventRouteScanV1::exact_end(request, inclusive_upper, items).map_err(|_| corrupt())
        }
    }
}

impl AuthoritativePointReader for ProtectedEventReplayReader<'_> {
    fn read_entity(
        &self,
        target: &EntityTarget,
    ) -> Result<Option<StoredEntityRecordV1>, StorageError> {
        let entities = self.transaction.open_table(ENTITIES).map_err(table_error)?;
        let Some(row) = entities
            .get(encode_entity_key(target.key()))
            .map_err(precommit_storage_error)?
        else {
            return Ok(None);
        };
        let record = decode_entity_record_v1(row.value())?.into_parts().0;
        if record.target() != target {
            return Err(corrupt());
        }
        Ok(Some(record))
    }

    fn read_stored_outcome(
        &self,
        _identity: &IdempotencyIdentity,
    ) -> Result<Option<StoredOutcomeV1>, StorageError> {
        Err(storage_error(StorageErrorKind::InvariantViolation))
    }

    fn read_commit(
        &self,
        sequence: CommitSequence,
    ) -> Result<Option<StoredCommitRecordV1>, StorageError> {
        let commits = self.transaction.open_table(COMMITS).map_err(table_error)?;
        let events = self.transaction.open_table(EVENTS).map_err(table_error)?;
        commit_at(&commits, &events, sequence)
    }

    fn read_provenance(
        &self,
        provenance_id: ProvenanceId,
    ) -> Result<Option<StoredProvenanceRecordV1>, StorageError> {
        let table = self
            .transaction
            .open_table(PROVENANCE)
            .map_err(table_error)?;
        let key = encode_provenance_key(provenance_id);
        let Some(row) = table.get(key.as_slice()).map_err(precommit_storage_error)? else {
            return Ok(None);
        };
        let record = match decode_command_locator_v1(row.value()) {
            Ok(locator) => {
                let locator = locator.into_parts().0;
                let commits = self.transaction.open_table(COMMITS).map_err(table_error)?;
                let events = self.transaction.open_table(EVENTS).map_err(table_error)?;
                let command = command_member_at(&commits, &events, locator.commit_sequence())?
                    .ok_or_else(corrupt)?;
                if command.base().commit_sequence() != locator.commit_sequence() {
                    return Err(corrupt());
                }
                command.base().provenance().clone()
            }
            Err(_) => decode_provenance_record_v1(row.value())?.into_parts().0,
        };
        if record.provenance_id() != provenance_id {
            return Err(corrupt());
        }
        Ok(Some(record))
    }

    fn read_durable_event(
        &self,
        event_id: EventId,
    ) -> Result<Option<StoredDurableEventV1>, StorageError> {
        let events = self.transaction.open_table(EVENTS).map_err(table_error)?;
        let Some(row) = events
            .get(encode_event_key(event_id).as_slice())
            .map_err(precommit_storage_error)?
        else {
            return Ok(None);
        };
        let event = decode_durable_event_v1(row.value())?.into_parts().0;
        if event.event_id() != event_id {
            return Err(corrupt());
        }
        Ok(Some(event))
    }
}

fn authorize_symbolic_event(
    transaction: &redb::WriteTransaction,
    event: &SymbolicEventEnvelope,
    policy: &AuthorizedQueryRowPolicyContextV1,
) -> Result<bool, StorageError> {
    let Some(anchor) = event.policy_anchor() else {
        return Ok(false);
    };
    let target = anchor.source();
    if !policy.protects(target.entity_type_id()) {
        return Err(storage_error(StorageErrorKind::InvariantViolation));
    }
    let current = {
        let entities = transaction.open_table(ENTITIES).map_err(table_error)?;
        let Some(row) = entities
            .get(encode_entity_key(target.key()))
            .map_err(precommit_storage_error)?
        else {
            return Ok(false);
        };
        let record = decode_entity_record_v1(row.value())?.into_parts().0;
        if record.target() != target {
            return Err(corrupt());
        }
        record
    };
    let lookups = policy
        .relationship_lookups(target.entity_type_id(), current.fields())
        .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
    let evidence = lookups
        .iter()
        .map(|lookup| indexed_relationship_exists_write(transaction, lookup))
        .collect::<Result<Vec<_>, _>>()?;
    let candidate = EventPolicyCandidateV1::new(
        event.event_id(),
        target.key().clone(),
        anchor.read_policy().clone(),
    );
    Ok(policy
        .authorize_event_release(&candidate, current.fields(), &evidence)
        .is_allowed())
}

fn map_event_replay_storage_error(error: riffdb_catalog::EventReplayError) -> StorageError {
    match error.kind() {
        EventReplayErrorKind::Storage(kind) => storage_error(kind),
        EventReplayErrorKind::Materialization(_) | EventReplayErrorKind::Integrity => corrupt(),
    }
}

fn current_capability_matches_policy(
    transaction: &redb::WriteTransaction,
    database_id: DatabaseId,
    observed_at: Timestamp,
    policy: &AuthorizedQueryRowPolicyContextV1,
) -> Result<bool, StorageError> {
    let Some((capability_id, revision)) = policy.internal_capability_identity() else {
        return Err(storage_error(StorageErrorKind::InvariantViolation));
    };
    let expected_grant = policy
        .internal_row_policy_grant()
        .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
    let capabilities = transaction.open_table(CAPABILITIES).map_err(table_error)?;
    let lookups = transaction
        .open_table(CAPABILITY_TOKENS)
        .map_err(table_error)?;
    let Some(current) = crate::administration::capability_from_tables(
        &capabilities,
        &lookups,
        database_id,
        capability_id,
    )?
    else {
        return Ok(false);
    };
    Ok(current.revision() == revision
        && matches!(current.lifecycle(), CapabilityLifecycleV1::Active)
        && current.issued_at() <= observed_at
        && observed_at < current.expires_at()
        && current.grant().internal_row_policy() == Some(expected_grant))
}

fn authorize_selected_events(
    transaction: &redb::WriteTransaction,
    selected_events: &[EventId],
    policy: &AuthorizedQueryRowPolicyContextV1,
) -> Result<Vec<EventId>, StorageError> {
    let mut visible = Vec::with_capacity(selected_events.len());
    for event_id in selected_events {
        let event = {
            let events = transaction.open_table(EVENTS).map_err(table_error)?;
            let row = events
                .get(encode_event_key(*event_id).as_slice())
                .map_err(precommit_storage_error)?
                .ok_or_else(corrupt)?;
            decode_durable_event_v1(row.value())?.into_parts().0
        };
        if event.event_id() != *event_id {
            return Err(corrupt());
        }
        let Some(anchor) = event.policy_anchor() else {
            // V1 events have no compiler-owned current-row identity. They are
            // indistinguishable policy-hidden history for an ordinary
            // protected consumer, never an integrity failure or payload-based
            // authority fallback.
            continue;
        };
        let target = anchor.source();
        if !policy.protects(target.entity_type_id()) {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        let current = {
            let entities = transaction.open_table(ENTITIES).map_err(table_error)?;
            let Some(row) = entities
                .get(encode_entity_key(target.key()))
                .map_err(precommit_storage_error)?
            else {
                continue;
            };
            let record = decode_entity_record_v1(row.value())?.into_parts().0;
            if record.target() != target {
                return Err(corrupt());
            }
            record
        };
        let lookups = policy
            .relationship_lookups(target.entity_type_id(), current.fields())
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        let evidence = lookups
            .iter()
            .map(|lookup| indexed_relationship_exists_write(transaction, lookup))
            .collect::<Result<Vec<_>, _>>()?;
        let candidate = EventPolicyCandidateV1::new(
            *event_id,
            target.key().clone(),
            anchor.read_policy().clone(),
        );
        if policy
            .authorize_event_release(&candidate, current.fields(), &evidence)
            .is_allowed()
        {
            visible.push(*event_id);
        }
    }
    Ok(visible)
}

fn indexed_relationship_exists_write(
    transaction: &redb::WriteTransaction,
    lookup: &riffdb_policy::AuthorizedIndexedRelationshipLookupV1,
) -> Result<bool, StorageError> {
    let upper = exclusive_prefix_end(lookup.index_prefix())
        .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
    let indexes = transaction
        .open_table(SECONDARY_INDEXES)
        .map_err(table_error)?;
    let mut inspected = 0usize;
    for row in indexes
        .range(lookup.index_prefix()..upper.as_slice())
        .map_err(precommit_storage_error)?
    {
        if inspected == usize::try_from(MAX_QUERY_SCANNED_ROWS).unwrap_or(usize::MAX) {
            return Err(storage_error(StorageErrorKind::LimitExceeded));
        }
        inspected = inspected.saturating_add(1);
        let (key, value) = row.map_err(precommit_storage_error)?;
        let key = decode_index_entry_key(key.value()).map_err(|_| corrupt())?;
        let entry = decode_index_entry_v2(value.value())?.into_parts().0;
        if entry.key() != &key {
            return Err(corrupt());
        }
        if entry.partition_key() == lookup.partition() {
            return Ok(true);
        }
    }
    Ok(false)
}

fn exclusive_prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut upper = prefix.to_vec();
    let position = upper.iter().rposition(|byte| *byte != u8::MAX)?;
    upper[position] = upper[position].saturating_add(1);
    upper.truncate(position + 1);
    Some(upper)
}

fn strict_event_order(events: &[EventId]) -> bool {
    events.windows(2).all(|pair| pair[0] < pair[1])
}

impl EventConsumerRepository for RedbOperationalPorts {
    fn inspect_event_consumer(
        &self,
        identity: EventConsumerIdentityHash,
    ) -> Result<Option<EventConsumerSnapshotV1>, StorageError> {
        let transaction = self.begin_read()?;
        let database_id = read_database_id_from_read(&transaction)?;
        let consumers = transaction
            .open_table(EVENT_CONSUMERS)
            .map_err(table_error)?;
        let deliveries = transaction
            .open_table(EVENT_CONSUMER_DELIVERIES)
            .map_err(table_error)?;
        read_snapshot_from_tables(&consumers, &deliveries, database_id, identity)
    }

    fn transition_event_consumer(
        &mut self,
        transition: EventConsumerTransitionV1,
    ) -> Result<EventConsumerTransitionResultV1, StorageError> {
        let access = self.begin_write()?;
        let transaction = access.transaction()?;
        let database_id = read_database_id_from_write(transaction)?;
        let identity = transition.consumer_identity_hash();
        let current = {
            let consumers = transaction
                .open_table(EVENT_CONSUMERS)
                .map_err(table_error)?;
            let deliveries = transaction
                .open_table(EVENT_CONSUMER_DELIVERIES)
                .map_err(table_error)?;
            read_snapshot_from_tables(&consumers, &deliveries, database_id, identity)?
        };
        let evaluated =
            evaluate_event_consumer_transition(current.clone(), transition).map_err(state_error)?;
        match evaluated {
            EvaluatedEventConsumerTransitionV1::NoChange(result) => {
                access.abort()?;
                Ok(result)
            }
            EvaluatedEventConsumerTransitionV1::Replace(replacement) => {
                replace_snapshot(transaction, current.as_ref(), replacement.as_ref())?;
                access.commit_for(RedbTestOperation::EventConsumerTransition)?;
                Ok(EventConsumerTransitionResultV1::Applied)
            }
            EvaluatedEventConsumerTransitionV1::Retire(identity) => {
                remove_snapshot(transaction, current.as_ref(), identity)?;
                access.commit_for(RedbTestOperation::EventConsumerTransition)?;
                Ok(EventConsumerTransitionResultV1::Applied)
            }
        }
    }

    fn inspect_event_consumer_inventory(
        &self,
    ) -> Result<Vec<EventConsumerSnapshotV1>, StorageError> {
        let transaction = self.begin_read()?;
        let database_id = read_database_id_from_read(&transaction)?;
        let consumers = transaction
            .open_table(EVENT_CONSUMERS)
            .map_err(table_error)?;
        let deliveries = transaction
            .open_table(EVENT_CONSUMER_DELIVERIES)
            .map_err(table_error)?;
        let mut inventory = Vec::new();
        for entry in consumers.iter().map_err(precommit_storage_error)? {
            if inventory.len() == MAX_EVENT_CONSUMERS {
                return Err(corrupt());
            }
            let (key, value) = entry.map_err(precommit_storage_error)?;
            let consumer = decode_event_consumer_v1(value.value())?.into_parts().0;
            validate_consumer_key(database_id, key.value(), &consumer)?;
            let rows = read_deliveries(&deliveries, consumer.identity().identity_hash())?;
            inventory.push(EventConsumerSnapshotV1::new(consumer, rows).map_err(state_error)?);
        }
        Ok(inventory)
    }

    fn event_consumer_retention_low_water(&self) -> Result<Option<u64>, StorageError> {
        let transaction = self.begin_read()?;
        let database_id = read_database_id_from_read(&transaction)?;
        let consumers = transaction
            .open_table(EVENT_CONSUMERS)
            .map_err(table_error)?;
        retention_low_water_from_table(&consumers, database_id)
    }
}

fn read_database_id_from_read(
    transaction: &redb::ReadTransaction,
) -> Result<DatabaseId, StorageError> {
    let meta = transaction.open_table(META).map_err(table_error)?;
    let row = meta
        .get(META_DATABASE_ID)
        .map_err(precommit_storage_error)?
        .ok_or_else(corrupt)?;
    Ok(*decode_database_identity_v1(row.value())?.value())
}

fn read_database_id_from_write(
    transaction: &redb::WriteTransaction,
) -> Result<DatabaseId, StorageError> {
    let meta = transaction.open_table(META).map_err(table_error)?;
    let row = meta
        .get(META_DATABASE_ID)
        .map_err(precommit_storage_error)?
        .ok_or_else(corrupt)?;
    Ok(*decode_database_identity_v1(row.value())?.value())
}

pub(crate) fn read_snapshot_from_tables<C, D>(
    consumers: &C,
    deliveries: &D,
    database_id: DatabaseId,
    identity: EventConsumerIdentityHash,
) -> Result<Option<EventConsumerSnapshotV1>, StorageError>
where
    C: ReadableTable<&'static [u8], &'static [u8]>,
    D: ReadableTable<&'static [u8], &'static [u8]>,
{
    let key = encode_event_consumer_key(identity);
    let Some(row) = consumers
        .get(key.as_slice())
        .map_err(precommit_storage_error)?
    else {
        return Ok(None);
    };
    let consumer = decode_event_consumer_v1(row.value())?.into_parts().0;
    validate_consumer_key(database_id, key.as_slice(), &consumer)?;
    let delivery_rows = read_deliveries(deliveries, identity)?;
    EventConsumerSnapshotV1::new(consumer, delivery_rows)
        .map(Some)
        .map_err(state_error)
}

fn read_deliveries<D>(
    table: &D,
    identity: EventConsumerIdentityHash,
) -> Result<Vec<StoredEventConsumerDeliveryV1>, StorageError>
where
    D: ReadableTable<&'static [u8], &'static [u8]>,
{
    let (lower, upper) = delivery_bounds(identity);
    let mut rows = Vec::new();
    for entry in table
        .range(lower.as_slice()..=upper.as_slice())
        .map_err(precommit_storage_error)?
    {
        if rows.len() == MAX_CONSUMER_DELIVERY_RECORDS {
            return Err(corrupt());
        }
        let (key, value) = entry.map_err(precommit_storage_error)?;
        let (key_identity, event_id) =
            decode_event_consumer_delivery_key(key.value()).map_err(|_| corrupt())?;
        let delivery = decode_event_consumer_delivery_v1(value.value())?
            .into_parts()
            .0;
        if key_identity != identity
            || delivery.consumer_identity_hash() != identity
            || delivery.event_id() != event_id
        {
            return Err(corrupt());
        }
        rows.push(delivery);
    }
    Ok(rows)
}

fn replace_snapshot(
    transaction: &redb::WriteTransaction,
    current: Option<&EventConsumerSnapshotV1>,
    replacement: &EventConsumerSnapshotV1,
) -> Result<(), StorageError> {
    let identity = replacement.consumer().identity().identity_hash();
    remove_snapshot(transaction, current, identity)?;
    let consumer_key = encode_event_consumer_key(identity);
    let consumer_value = encode_event_consumer_v1(replacement.consumer())?;
    let mut consumers = transaction
        .open_table(EVENT_CONSUMERS)
        .map_err(table_error)?;
    if consumers
        .insert(consumer_key.as_slice(), consumer_value.as_bytes())
        .map_err(precommit_storage_error)?
        .is_some()
    {
        return Err(corrupt());
    }
    drop(consumers);
    let mut deliveries = transaction
        .open_table(EVENT_CONSUMER_DELIVERIES)
        .map_err(table_error)?;
    for delivery in replacement.deliveries() {
        let key = encode_event_consumer_delivery_key(identity, delivery.event_id());
        let value = encode_event_consumer_delivery_v1(delivery)?;
        if deliveries
            .insert(key.as_slice(), value.as_bytes())
            .map_err(precommit_storage_error)?
            .is_some()
        {
            return Err(corrupt());
        }
    }
    Ok(())
}

fn remove_snapshot(
    transaction: &redb::WriteTransaction,
    current: Option<&EventConsumerSnapshotV1>,
    identity: EventConsumerIdentityHash,
) -> Result<(), StorageError> {
    let consumer_key = encode_event_consumer_key(identity);
    let mut consumers = transaction
        .open_table(EVENT_CONSUMERS)
        .map_err(table_error)?;
    let removed = consumers
        .remove(consumer_key.as_slice())
        .map_err(precommit_storage_error)?;
    if removed.is_some() != current.is_some() {
        return Err(corrupt());
    }
    drop(removed);
    drop(consumers);
    let mut deliveries = transaction
        .open_table(EVENT_CONSUMER_DELIVERIES)
        .map_err(table_error)?;
    if let Some(current) = current {
        for delivery in current.deliveries() {
            let key = encode_event_consumer_delivery_key(identity, delivery.event_id());
            if deliveries
                .remove(key.as_slice())
                .map_err(precommit_storage_error)?
                .is_none()
            {
                return Err(corrupt());
            }
        }
    }
    Ok(())
}

pub(crate) fn retention_low_water_from_table<T>(
    consumers: &T,
    database_id: DatabaseId,
) -> Result<Option<u64>, StorageError>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
{
    let mut minimum = None;
    for (count, entry) in consumers
        .iter()
        .map_err(precommit_storage_error)?
        .enumerate()
    {
        if count == MAX_EVENT_CONSUMERS {
            return Err(corrupt());
        }
        let (key, value) = entry.map_err(precommit_storage_error)?;
        let consumer = decode_event_consumer_v1(value.value())?.into_parts().0;
        validate_consumer_key(database_id, key.value(), &consumer)?;
        minimum = Some(minimum.map_or_else(
            || consumer.checkpoint().retention_frontier(),
            |current: u64| current.min(consumer.checkpoint().retention_frontier()),
        ));
    }
    Ok(minimum)
}

fn validate_consumer_key(
    database_id: DatabaseId,
    key: &[u8],
    consumer: &StoredEventConsumerV1,
) -> Result<(), StorageError> {
    let identity = decode_event_consumer_key(key).map_err(|_| corrupt())?;
    if consumer.identity().database_id() != database_id
        || consumer.identity().identity_hash() != identity
    {
        return Err(corrupt());
    }
    Ok(())
}

fn delivery_bounds(identity: EventConsumerIdentityHash) -> ([u8; 44], [u8; 44]) {
    let mut lower = [0_u8; 44];
    lower[..32].copy_from_slice(identity.as_bytes());
    let mut upper = lower;
    upper[32..].fill(u8::MAX);
    (lower, upper)
}

fn state_error(error: ConsumerStateError) -> StorageError {
    let kind = match error {
        ConsumerStateError::LimitExceeded => StorageErrorKind::LimitExceeded,
        ConsumerStateError::InvalidShape | ConsumerStateError::RevisionExhausted => {
            StorageErrorKind::CorruptData
        }
    };
    storage_error(kind)
}

fn corrupt() -> StorageError {
    storage_error(StorageErrorKind::CorruptData)
}
