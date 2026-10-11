#![forbid(unsafe_code)]

//! WP-160 deterministic crash, retry, and duplicate-delivery evidence.

use std::collections::{BTreeMap, VecDeque};
use std::num::{NonZeroU16, NonZeroU32};
use std::sync::{Arc, Mutex, MutexGuard};

use riffdb_outbox::{
    ConnectorDeclaration, ConnectorDestinationKind, ConnectorDisposition, ConnectorIdempotency,
    DeliveryAttempt, DeliveryPolicy, NoOutboxFailpoints, NoOutboxTelemetry, OutboxClock,
    OutboxClockError, OutboxConnector, OutboxDispatcher, OutboxFailpoint, OutboxFailpoints,
    OutboxRecoveryResult, OutboxStatusPageRequest, OutboxStatusSource, OutboxStatusState,
    OutboxTelemetry, OutboxTelemetryEvent, OutboxWorkerError, RecoveringOutbox,
};
use riffdb_storage_api::{
    EncodedContentCharge, EncodedPageItem, OutboxClaimV1, OutboxDeadLetterV1,
    OutboxDeliveryStateV1, OutboxPageLimit, OutboxRenewV1, OutboxRepository, OutboxRetryMetadataV1,
    OutboxRetryV1, OutboxSafeErrorV1, OutboxStatusObservationV1, OutboxStatusReadResultV1,
    OutboxSucceedV1, OutboxTransitionResultV1, PendingOutboxItemV1, PendingOutboxScanV1,
    StorageError, StorageErrorKind, StoredDurableEventV1, StoredOutboxIntentV1,
    StoredOutboxStatusV1, UndeliveredOutboxStatusScanRequestV1, UndeliveredOutboxStatusScanV1,
    UndeliveredOutboxStatusV1, derive_event_hash_v1,
};
use riffdb_types::{CanonicalRecord, CommitSequence, EventHash, EventId, EventTypeId, Timestamp};

#[derive(Clone)]
struct FakeRepository {
    state: Arc<Mutex<FakeState>>,
}

#[derive(Default)]
struct FakeState {
    authoritative: BTreeMap<EventId, (StoredDurableEventV1, StoredOutboxIntentV1)>,
    statuses: BTreeMap<EventId, StoredOutboxStatusV1>,
    change_during_next_retry: Option<StoredOutboxStatusV1>,
    atomic_success_groups: bool,
    success_group_sizes: Vec<usize>,
    groups_seen_by_retry: Vec<usize>,
    fail_next_group: Option<StorageErrorKind>,
    atomic_claim_groups: bool,
    claim_group_sizes: Vec<usize>,
    individual_claims: usize,
    renewals: usize,
    renewal_state_change: bool,
}

impl FakeRepository {
    fn with_events(count: u32) -> Self {
        let mut state = FakeState::default();
        for ordinal in 0..count {
            let event = make_event(ordinal);
            state.authoritative.insert(
                event.event_id(),
                (event.clone(), StoredOutboxIntentV1::new(event)),
            );
        }
        Self {
            state: Arc::new(Mutex::new(state)),
        }
    }

    fn lock(&self) -> Result<MutexGuard<'_, FakeState>, StorageError> {
        self.state
            .lock()
            .map_err(|_| storage_error(StorageErrorKind::Unavailable))
    }

    fn status(&self, event_id: EventId) -> Option<StoredOutboxStatusV1> {
        self.lock()
            .expect("test repository lock")
            .statuses
            .get(&event_id)
            .cloned()
    }

    fn set_status(&self, status: StoredOutboxStatusV1) {
        self.lock()
            .expect("test repository lock")
            .statuses
            .insert(status.event_id(), status);
    }

    fn insert_event(&self, ordinal: u32) {
        let event = make_event(ordinal);
        let prior = self
            .lock()
            .expect("test repository lock")
            .authoritative
            .insert(
                event.event_id(),
                (event.clone(), StoredOutboxIntentV1::new(event)),
            );
        assert!(prior.is_none(), "test event must be new");
    }

    fn insert_orphan_status(&self, status: StoredOutboxStatusV1) {
        self.set_status(status);
    }

    fn change_during_next_retry(&self, status: StoredOutboxStatusV1) {
        self.lock()
            .expect("test repository lock")
            .change_during_next_retry = Some(status);
    }

    fn authoritative_counts(&self) -> (usize, usize) {
        let state = self.lock().expect("test repository lock");
        let events = state.authoritative.len();
        let intents = state
            .authoritative
            .values()
            .filter(|(event, intent)| intent.event() == event)
            .count();
        (events, intents)
    }

    fn validate_source(state: &FakeState) -> Result<(), StorageError> {
        for (event_id, status) in &state.statuses {
            if status.event_id() != *event_id || !state.authoritative.contains_key(event_id) {
                return Err(storage_error(StorageErrorKind::CorruptData));
            }
        }
        for (event_id, (event, intent)) in &state.authoritative {
            if event.event_id() != *event_id || intent.event() != event {
                return Err(storage_error(StorageErrorKind::CorruptData));
            }
        }
        Ok(())
    }

    fn transition(
        &self,
        event_id: EventId,
        expected: &OutboxStatusObservationV1,
        updated: &StoredOutboxStatusV1,
    ) -> Result<OutboxTransitionResultV1, StorageError> {
        let mut state = self.lock()?;
        Self::validate_source(&state)?;
        if !state.authoritative.contains_key(&event_id) {
            return Ok(OutboxTransitionResultV1::AuthoritativeIntentMissing);
        }
        let current = state.statuses.get(&event_id).cloned().map_or(
            OutboxStatusObservationV1::AbsentInitialPending,
            OutboxStatusObservationV1::Present,
        );
        if &current != expected {
            return Ok(OutboxTransitionResultV1::StateChanged(current));
        }
        if updated.event_id() != event_id {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        state.statuses.insert(event_id, updated.clone());
        Ok(OutboxTransitionResultV1::Applied(updated.clone()))
    }
}

impl OutboxRepository for FakeRepository {
    fn supports_outbox_succeed_groups(&self) -> bool {
        self.lock().unwrap().atomic_success_groups
    }

    fn supports_outbox_claim_groups(&self) -> bool {
        self.lock().unwrap().atomic_claim_groups
    }

    fn claim_outbox_group(
        &mut self,
        group: &riffdb_storage_api::OutboxClaimGroupV1,
    ) -> Result<riffdb_storage_api::OutboxClaimGroupResultV1, StorageError> {
        use riffdb_storage_api::{OutboxClaimGroupCompletionV1, OutboxClaimGroupResultV1};
        let mut state = self.lock()?;
        assert!(state.atomic_claim_groups);
        Self::validate_source(&state)?;
        state.claim_group_sizes.push(group.claims().len());
        let mut results = Vec::new();
        for claim in group.claims() {
            if !state.authoritative.contains_key(&claim.event_id()) {
                return Ok(OutboxClaimGroupResultV1::AuthoritativeIntentMissing);
            }
            let current = state.statuses.get(&claim.event_id()).cloned().map_or(
                OutboxStatusObservationV1::AbsentInitialPending,
                OutboxStatusObservationV1::Present,
            );
            results.push(if &current == claim.expected() {
                OutboxTransitionResultV1::Applied(claim.updated().clone())
            } else {
                OutboxTransitionResultV1::StateChanged(current)
            });
        }
        for result in &results {
            if let OutboxTransitionResultV1::Applied(status) = result {
                state.statuses.insert(status.event_id(), status.clone());
            }
        }
        Ok(OutboxClaimGroupResultV1::Complete(
            OutboxClaimGroupCompletionV1::new(group, results).unwrap(),
        ))
    }

    fn succeed_outbox_group(
        &mut self,
        group: &riffdb_storage_api::OutboxSucceedGroupV1,
    ) -> Result<riffdb_storage_api::OutboxSucceedGroupResultV1, StorageError> {
        use riffdb_storage_api::{OutboxSucceedGroupCompletionV1, OutboxSucceedGroupResultV1};
        let mut state = self.lock()?;
        assert!(state.atomic_success_groups);
        Self::validate_source(&state)?;
        state.success_group_sizes.push(group.transitions().len());
        if let Some(kind) = state.fail_next_group.take() {
            return Err(storage_error(kind));
        }
        let mut results = Vec::new();
        for transition in group.transitions() {
            if !state.authoritative.contains_key(&transition.event_id()) {
                return Ok(OutboxSucceedGroupResultV1::AuthoritativeIntentMissing);
            }
            let current = state.statuses.get(&transition.event_id()).cloned().map_or(
                OutboxStatusObservationV1::AbsentInitialPending,
                OutboxStatusObservationV1::Present,
            );
            results.push(
                if current == OutboxStatusObservationV1::Present(transition.expected().clone()) {
                    OutboxTransitionResultV1::Applied(transition.updated().clone())
                } else {
                    OutboxTransitionResultV1::StateChanged(current)
                },
            );
        }
        for result in &results {
            if let OutboxTransitionResultV1::Applied(status) = result {
                state.statuses.insert(status.event_id(), status.clone());
            }
        }
        Ok(OutboxSucceedGroupResultV1::Complete(
            OutboxSucceedGroupCompletionV1::new(group, results).unwrap(),
        ))
    }

    fn read_outbox_status(
        &self,
        event_id: EventId,
    ) -> Result<OutboxStatusReadResultV1, StorageError> {
        let state = self.lock()?;
        Self::validate_source(&state)?;
        if !state.authoritative.contains_key(&event_id) {
            return Ok(OutboxStatusReadResultV1::AuthoritativeIntentMissing);
        }
        Ok(OutboxStatusReadResultV1::Status(
            state.statuses.get(&event_id).cloned().map_or(
                OutboxStatusObservationV1::AbsentInitialPending,
                OutboxStatusObservationV1::Present,
            ),
        ))
    }

    fn scan_pending_outbox(
        &self,
        after: Option<EventId>,
        limit: OutboxPageLimit,
    ) -> Result<PendingOutboxScanV1, StorageError> {
        let state = self.lock()?;
        Self::validate_source(&state)?;
        let wanted = usize::from(limit.get().get());
        let mut pending = Vec::new();
        for (event_id, (event, intent)) in &state.authoritative {
            if after.is_some_and(|after| *event_id <= after) {
                continue;
            }
            let status = state.statuses.get(event_id).cloned().map_or(
                OutboxStatusObservationV1::AbsentInitialPending,
                OutboxStatusObservationV1::Present,
            );
            if !status.is_pending() {
                continue;
            }
            pending.push(
                PendingOutboxItemV1::new(event.clone(), intent.clone(), status)
                    .map_err(|_| storage_error(StorageErrorKind::CorruptData))?,
            );
            if pending.len() > wanted {
                break;
            }
        }
        let has_more = pending.len() > wanted;
        pending.truncate(wanted);
        let charge = EncodedContentCharge::new(1).expect("nonzero test charge");
        let items = pending
            .into_iter()
            .map(|item| EncodedPageItem::new(item, charge))
            .collect();
        PendingOutboxScanV1::page(items, has_more)
            .map_err(|_| storage_error(StorageErrorKind::CorruptData))
    }

    fn scan_undelivered_outbox_statuses(
        &self,
        request: UndeliveredOutboxStatusScanRequestV1,
    ) -> Result<UndeliveredOutboxStatusScanV1, StorageError> {
        let state = self.lock()?;
        Self::validate_source(&state)?;
        let Some(inclusive_upper) = state.authoritative.keys().next_back().copied() else {
            return UndeliveredOutboxStatusScanV1::exact_end(
                request,
                riffdb_storage_api::OutboxRecoveryUpperFenceV1::BeforeFirst,
                Vec::new(),
            )
            .map_err(|_| storage_error(StorageErrorKind::CorruptData));
        };
        let inclusive_upper = request.inclusive_upper().unwrap_or(inclusive_upper);
        let wanted = usize::from(request.limit().get().get());
        let mut items = Vec::new();
        for event_id in state.authoritative.keys() {
            if request.after().is_some_and(|after| *event_id <= after)
                || *event_id > inclusive_upper
            {
                continue;
            }
            let status = state.statuses.get(event_id).cloned().map_or(
                OutboxStatusObservationV1::AbsentInitialPending,
                OutboxStatusObservationV1::Present,
            );
            if matches!(
                &status,
                OutboxStatusObservationV1::Present(stored)
                    if matches!(stored.state(), OutboxDeliveryStateV1::Delivered { .. })
            ) {
                continue;
            }
            let status = UndeliveredOutboxStatusV1::new(*event_id, status)
                .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
            items.push(EncodedPageItem::new(
                status,
                EncodedContentCharge::new(1).expect("nonzero test charge"),
            ));
            if items.len() > wanted {
                break;
            }
        }
        let has_more = items.len() > wanted;
        items.truncate(wanted);
        let scan = if has_more {
            UndeliveredOutboxStatusScanV1::page(request, inclusive_upper, items)
        } else {
            UndeliveredOutboxStatusScanV1::exact_end(
                request,
                riffdb_storage_api::OutboxRecoveryUpperFenceV1::Inclusive(inclusive_upper),
                items,
            )
        };
        scan.map_err(|_| storage_error(StorageErrorKind::CorruptData))
    }

    fn claim_outbox(
        &mut self,
        transition: &OutboxClaimV1,
    ) -> Result<OutboxTransitionResultV1, StorageError> {
        self.lock()?.individual_claims += 1;
        self.transition(
            transition.event_id(),
            transition.expected(),
            transition.updated(),
        )
    }

    fn renew_outbox(
        &mut self,
        transition: &OutboxRenewV1,
    ) -> Result<OutboxTransitionResultV1, StorageError> {
        {
            let mut state = self.lock()?;
            state.renewals += 1;
            if state.renewal_state_change {
                let current = state.statuses.get(&transition.event_id()).cloned().map_or(
                    OutboxStatusObservationV1::AbsentInitialPending,
                    OutboxStatusObservationV1::Present,
                );
                return Ok(OutboxTransitionResultV1::StateChanged(current));
            }
        }
        self.transition(
            transition.event_id(),
            &OutboxStatusObservationV1::Present(transition.expected().clone()),
            transition.updated(),
        )
    }

    fn succeed_outbox(
        &mut self,
        transition: &OutboxSucceedV1,
    ) -> Result<OutboxTransitionResultV1, StorageError> {
        self.transition(
            transition.event_id(),
            &OutboxStatusObservationV1::Present(transition.expected().clone()),
            transition.updated(),
        )
    }

    fn retry_outbox(
        &mut self,
        transition: &OutboxRetryV1,
    ) -> Result<OutboxTransitionResultV1, StorageError> {
        let changed = {
            let mut state = self.lock()?;
            let groups = state.success_group_sizes.len();
            state.groups_seen_by_retry.push(groups);
            state.change_during_next_retry.take()
        };
        if let Some(changed) = changed {
            let event_id = changed.event_id();
            self.lock()?.statuses.insert(event_id, changed.clone());
            return Ok(OutboxTransitionResultV1::StateChanged(
                OutboxStatusObservationV1::Present(changed),
            ));
        }
        self.transition(
            transition.event_id(),
            &OutboxStatusObservationV1::Present(transition.expected().clone()),
            transition.updated(),
        )
    }

    fn dead_letter_outbox(
        &mut self,
        transition: &OutboxDeadLetterV1,
    ) -> Result<OutboxTransitionResultV1, StorageError> {
        self.transition(
            transition.event_id(),
            transition.expected(),
            transition.updated(),
        )
    }
}

// req: EFF-002, EFF-003, REC-001, END-008
#[test]
fn outbox_completion_group_flushes_at_bounds_and_before_retry() {
    let repository = FakeRepository::with_events(35);
    repository.lock().unwrap().atomic_success_groups = true;
    let mut script = vec![ConnectorDisposition::Accepted; 19];
    script.push(ConnectorDisposition::Retryable { safe_error: None });
    let connector = SharedConnector::new(script, ConnectorDisposition::Accepted);
    let clock = SharedClock::new(10);
    let policy = policy(3);
    let mut dispatcher = OutboxDispatcher::new(
        recovered(repository.clone(), &policy, &clock),
        connector.clone(),
        clock,
        NoOutboxFailpoints,
        NoOutboxTelemetry,
        policy,
    );
    let report = dispatcher.dispatch_once().unwrap();
    assert_eq!(report.claims(), 35);
    assert_eq!(report.delivered(), 34);
    assert_eq!(report.retries_scheduled(), 1);
    assert_eq!(repository.lock().unwrap().success_group_sizes, [16, 3, 15]);
    assert_eq!(repository.lock().unwrap().groups_seen_by_retry, [2]);
    assert_eq!(connector.observations().len(), 35);
    for ordinal in 0..35 {
        let status = repository.status(event_id(ordinal)).unwrap();
        assert_eq!(status.state().attempts(), 1);
        assert_eq!(
            matches!(status.state(), OutboxDeliveryStateV1::Delivered { .. }),
            ordinal != 19
        );
    }
}

fn grouped_dispatcher(
    repository: &FakeRepository,
    connector: SharedConnector,
    clock: SharedClock,
    policy: DeliveryPolicy,
) -> OutboxDispatcher<
    FakeRepository,
    SharedConnector,
    SharedClock,
    NoOutboxFailpoints,
    NoOutboxTelemetry,
> {
    {
        let mut state = repository.lock().unwrap();
        state.atomic_success_groups = true;
        state.atomic_claim_groups = true;
    }
    OutboxDispatcher::new(
        recovered(repository.clone(), &policy, &clock),
        connector,
        clock,
        NoOutboxFailpoints,
        NoOutboxTelemetry,
        policy,
    )
}

// req: EFF-002, EFF-003, REC-001, END-008
#[test]
fn outbox_claim_groups_take_one_claim_commit_for_a_full_page() {
    // The same sixteen deliveries with individual claims, then with groups.
    let individual = FakeRepository::with_events(16);
    individual.lock().unwrap().atomic_success_groups = true;
    let clock = SharedClock::new(10);
    let policy = policy_with_limit(3, 16);
    let mut dispatcher = OutboxDispatcher::new(
        recovered(individual.clone(), &policy, &clock),
        SharedConnector::new(vec![], ConnectorDisposition::Accepted),
        clock,
        NoOutboxFailpoints,
        NoOutboxTelemetry,
        policy,
    );
    let individual_report = dispatcher.dispatch_once().unwrap();
    assert_eq!(individual.lock().unwrap().individual_claims, 16);

    let grouped = FakeRepository::with_events(16);
    let connector = SharedConnector::new(vec![], ConnectorDisposition::Accepted);
    let mut dispatcher = grouped_dispatcher(
        &grouped,
        connector.clone(),
        SharedClock::new(10),
        policy_with_limit(3, 16),
    );
    let grouped_report = dispatcher.dispatch_once().unwrap();
    let state = grouped.lock().unwrap();
    assert_eq!(state.individual_claims, 0);
    assert_eq!(state.claim_group_sizes, [16]);
    assert_eq!(state.success_group_sizes, [16]);
    assert_eq!(grouped_report, individual_report);
    assert_eq!(grouped_report.claims(), 16);
    assert_eq!(grouped_report.delivered(), 16);
    // Every event ends in exactly the state individual claims produce.
    for ordinal in 0..16 {
        assert_eq!(
            state.statuses.get(&event_id(ordinal)),
            individual.status(event_id(ordinal)).as_ref()
        );
    }
    let delivered = connector.observations();
    assert_eq!(delivered.len(), 16);
    assert!(
        delivered
            .windows(2)
            .all(|pair| pair[0].event_id < pair[1].event_id)
    );
}

// req: EFF-002, EFF-003, REC-001, END-008
#[test]
fn outbox_claim_run_ends_at_the_first_ineligible_or_exhausted_item() {
    let repository = FakeRepository::with_events(10);
    let destination =
        riffdb_storage_api::OutboxDestinationIdV1::new("test/deterministic").expect("destination");
    // Event 4 waits for a later retry; event 7 has used every attempt.
    repository.set_status(StoredOutboxStatusV1::pending(
        event_id(4),
        OutboxRetryMetadataV1::new(
            NonZeroU32::new(1).expect("attempt"),
            timestamp(5),
            Some(timestamp(100)),
            destination.clone(),
            None,
        ),
    ));
    repository.set_status(StoredOutboxStatusV1::pending(
        event_id(7),
        OutboxRetryMetadataV1::new(
            NonZeroU32::new(3).expect("attempt"),
            timestamp(5),
            None,
            destination,
            None,
        ),
    ));
    let connector = SharedConnector::new(vec![], ConnectorDisposition::Accepted);
    let mut dispatcher = grouped_dispatcher(
        &repository,
        connector.clone(),
        SharedClock::new(10),
        policy(3),
    );
    let report = dispatcher.dispatch_once().unwrap();
    let state = repository.lock().unwrap();
    assert_eq!(state.claim_group_sizes, [4, 2, 2]);
    assert_eq!(state.individual_claims, 0);
    assert_eq!(report.deferred(), 1);
    assert_eq!(report.dead_lettered(), 1);
    assert_eq!(report.delivered(), 8);
    assert!(matches!(
        state.statuses[&event_id(4)].state(),
        OutboxDeliveryStateV1::Pending(_)
    ));
    assert!(matches!(
        state.statuses[&event_id(7)].state(),
        OutboxDeliveryStateV1::DeadLetter { .. }
    ));
}

// req: EFF-002, EFF-003, REC-001, END-008
#[test]
fn outbox_claim_group_admits_only_what_the_destination_accepts_together() {
    let repository = FakeRepository::with_events(8);
    let connector = SharedConnector::new(vec![], ConnectorDisposition::Accepted).with_capacity(3);
    let mut dispatcher = grouped_dispatcher(
        &repository,
        connector.clone(),
        SharedClock::new(10),
        policy(3),
    );
    let report = dispatcher.dispatch_once().unwrap();
    let state = repository.lock().unwrap();
    // Only the three the destination can hold together are claimed; the rest
    // defer without a claim, so none of them spends an attempt.
    assert_eq!(state.claim_group_sizes, [3]);
    assert_eq!(state.individual_claims, 0);
    assert_eq!(report.delivered(), 3);
    assert_eq!(report.retries_scheduled(), 0);
    assert_eq!(report.deferred(), 5);
    for ordinal in 3..8 {
        assert!(!state.statuses.contains_key(&event_id(ordinal)));
    }
    assert_eq!(connector.observations().len(), 3);
}

// req: EFF-002, EFF-003, REC-001, END-008
#[test]
fn outbox_claimed_lease_past_its_deadline_is_renewed_before_delivery() {
    // Each delivery takes six seconds of a ten-second lease, so the third
    // member and later are reached after the group's shared deadline.
    let repository = FakeRepository::with_events(5);
    let clock = SharedClock::new(10);
    let connector =
        SharedConnector::new(vec![], ConnectorDisposition::Accepted).advancing(&clock, 6);
    let mut dispatcher = grouped_dispatcher(&repository, connector.clone(), clock, policy(3));
    let report = dispatcher.dispatch_once().unwrap();
    let state = repository.lock().unwrap();
    assert_eq!(state.claim_group_sizes, [5]);
    assert_eq!(report.delivered(), 5);
    // Deliveries start at 10, 16, 22, 28 and 34 against the shared deadline
    // of 20: each of the last three members renews its own lease first.
    assert_eq!(state.renewals, 3);

    // A renewal that does not apply ends the run before connector I/O.
    let repository = FakeRepository::with_events(5);
    repository.lock().unwrap().renewal_state_change = true;
    let clock = SharedClock::new(10);
    let connector =
        SharedConnector::new(vec![], ConnectorDisposition::Accepted).advancing(&clock, 6);
    let mut dispatcher = grouped_dispatcher(&repository, connector.clone(), clock, policy(3));
    assert!(matches!(
        dispatcher.dispatch_once(),
        Err(OutboxWorkerError::StateChanged {
            phase: riffdb_outbox::OutboxTransitionPhase::Renew
        })
    ));
    assert_eq!(connector.observations().len(), 2);
    let state = repository.lock().unwrap();
    assert_eq!(
        state.success_group_sizes,
        [2],
        "earlier successes flush first"
    );
}

// req: EFF-002, EFF-003, REC-001, END-008
#[test]
fn uncertain_group_retains_all_delivering_fences_and_publishes_no_success() {
    let repository = FakeRepository::with_events(16);
    {
        let mut state = repository.lock().unwrap();
        state.atomic_success_groups = true;
        state.fail_next_group = Some(StorageErrorKind::CommitStatusUnknown);
    }
    let connector = SharedConnector::new(vec![], ConnectorDisposition::Accepted);
    let clock = SharedClock::new(10);
    let policy = policy(3);
    let telemetry = SharedTelemetry::default();
    let mut dispatcher = OutboxDispatcher::new(
        recovered(repository.clone(), &policy, &clock),
        connector.clone(),
        clock,
        NoOutboxFailpoints,
        telemetry.clone(),
        policy,
    );
    assert!(
        matches!(dispatcher.dispatch_once(), Err(OutboxWorkerError::Storage(error)) if error.kind() == StorageErrorKind::CommitStatusUnknown)
    );
    assert_eq!(connector.observations().len(), 16);
    for ordinal in 0..16 {
        assert!(matches!(
            repository.status(event_id(ordinal)).unwrap().state(),
            OutboxDeliveryStateV1::Delivering { .. }
        ));
    }
    assert!(
        !telemetry
            .events()
            .iter()
            .any(|event| matches!(event, OutboxTelemetryEvent::Delivered { .. }))
    );
}

#[derive(Clone)]
struct SharedClock {
    now: Arc<Mutex<Timestamp>>,
}

#[derive(Clone, Default)]
struct SharedTelemetry(Arc<Mutex<Vec<OutboxTelemetryEvent>>>);

impl SharedTelemetry {
    fn events(&self) -> Vec<OutboxTelemetryEvent> {
        self.0.lock().unwrap().clone()
    }
}

impl OutboxTelemetry for SharedTelemetry {
    fn record(&mut self, event: OutboxTelemetryEvent) {
        self.0.lock().unwrap().push(event);
    }
}

impl SharedClock {
    fn new(seconds: i64) -> Self {
        Self {
            now: Arc::new(Mutex::new(timestamp(seconds))),
        }
    }

    fn set(&self, seconds: i64) {
        *self.now.lock().expect("test clock lock") = timestamp(seconds);
    }
}

impl OutboxClock for SharedClock {
    fn now(&mut self) -> Result<Timestamp, OutboxClockError> {
        self.now
            .lock()
            .map(|value| *value)
            .map_err(|_| OutboxClockError::Unavailable)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ConnectorObservation {
    event_id: EventId,
    event_hash: EventHash,
    attempt: u32,
}

#[derive(Clone)]
struct SharedConnector {
    state: Arc<Mutex<SharedConnectorState>>,
}

struct SharedConnectorState {
    script: VecDeque<ConnectorDisposition>,
    fallback: ConnectorDisposition,
    observations: Vec<ConnectorObservation>,
    /// Events the destination can still accept, like a quota; `None` is unbounded.
    capacity: Option<usize>,
    /// Seconds each delivery advances this clock by.
    advances: Option<(SharedClock, i64)>,
}

impl SharedConnector {
    fn new(script: Vec<ConnectorDisposition>, fallback: ConnectorDisposition) -> Self {
        Self {
            state: Arc::new(Mutex::new(SharedConnectorState {
                script: script.into(),
                fallback,
                observations: Vec::new(),
                capacity: None,
                advances: None,
            })),
        }
    }

    fn with_capacity(self, capacity: usize) -> Self {
        self.state.lock().expect("test connector lock").capacity = Some(capacity);
        self
    }

    fn advancing(self, clock: &SharedClock, seconds: i64) -> Self {
        self.state.lock().expect("test connector lock").advances = Some((clock.clone(), seconds));
        self
    }

    fn observations(&self) -> Vec<ConnectorObservation> {
        self.state
            .lock()
            .expect("test connector lock")
            .observations
            .clone()
    }
}

impl OutboxConnector for SharedConnector {
    fn declaration(&self) -> ConnectorDeclaration {
        ConnectorDeclaration::new(
            ConnectorDestinationKind::Deterministic,
            ConnectorIdempotency::EventId,
        )
    }

    fn ready_to_deliver(&mut self, _event: &StoredDurableEventV1) -> bool {
        self.state
            .lock()
            .expect("test connector lock")
            .capacity
            .is_none_or(|capacity| capacity > 0)
    }

    fn admissible_run(&mut self, events: &[&StoredDurableEventV1]) -> usize {
        let capacity = self.state.lock().expect("test connector lock").capacity;
        capacity.map_or(events.len(), |capacity| capacity.min(events.len()))
    }

    fn deliver(&mut self, attempt: DeliveryAttempt<'_>) -> ConnectorDisposition {
        let mut state = self.state.lock().expect("test connector lock");
        if let Some(capacity) = state.capacity.as_mut() {
            assert!(*capacity > 0, "delivered past the destination's capacity");
            *capacity -= 1;
        }
        if let Some((clock, seconds)) = &state.advances {
            let now = clock.now.lock().expect("test clock lock").seconds();
            clock.set(now + seconds);
        }
        state.observations.push(ConnectorObservation {
            event_id: attempt.event_id(),
            event_hash: attempt.event().event_hash(),
            attempt: attempt.attempt().get(),
        });
        let fallback = state.fallback.clone();
        state.script.pop_front().unwrap_or(fallback)
    }
}

struct OneShotFailpoint {
    selected: OutboxFailpoint,
    fired: bool,
}

impl OneShotFailpoint {
    fn new(selected: OutboxFailpoint) -> Self {
        Self {
            selected,
            fired: false,
        }
    }
}

impl OutboxFailpoints for OneShotFailpoint {
    fn should_interrupt(&mut self, failpoint: OutboxFailpoint, _event_id: Option<EventId>) -> bool {
        if !self.fired && failpoint == self.selected {
            self.fired = true;
            true
        } else {
            false
        }
    }
}

struct InsertAfterFirstNormalization {
    repository: FakeRepository,
    inserted: bool,
}

impl InsertAfterFirstNormalization {
    fn new(repository: FakeRepository) -> Self {
        Self {
            repository,
            inserted: false,
        }
    }
}

impl OutboxTelemetry for InsertAfterFirstNormalization {
    fn record(&mut self, event: OutboxTelemetryEvent) {
        if !self.inserted && matches!(event, OutboxTelemetryEvent::RecoveryNormalized { .. }) {
            self.repository.insert_event(2);
            self.inserted = true;
        }
    }
}

fn recovered(
    repository: FakeRepository,
    policy: &DeliveryPolicy,
    clock: &SharedClock,
) -> riffdb_outbox::RecoveredOutbox<FakeRepository> {
    match RecoveringOutbox::after_authoritative_readiness(repository).recover(
        policy,
        &mut clock.clone(),
        &mut NoOutboxFailpoints,
        &mut NoOutboxTelemetry,
    ) {
        OutboxRecoveryResult::Ready { recovered, .. } => recovered,
        OutboxRecoveryResult::Degraded { error, .. } => {
            panic!("unexpected recovery failure: {error}")
        }
    }
}

fn policy(max_attempts: u32) -> DeliveryPolicy {
    policy_with_limit(max_attempts, 50)
}

fn policy_with_limit(max_attempts: u32, scan_limit: u16) -> DeliveryPolicy {
    let max_attempts = NonZeroU32::new(max_attempts).expect("nonzero attempts");
    let retry_delays = (1..max_attempts.get())
        .map(|attempt| NonZeroU32::new(attempt).expect("nonzero delay"))
        .collect();
    DeliveryPolicy::new(
        riffdb_storage_api::OutboxDestinationIdV1::new("test/deterministic").expect("destination"),
        NonZeroU32::new(5).expect("timeout"),
        NonZeroU32::new(10).expect("lease"),
        max_attempts,
        retry_delays,
        OutboxPageLimit::new(NonZeroU16::new(scan_limit).expect("limit")).expect("bounded limit"),
    )
    .expect("valid policy")
}

fn make_event(ordinal: u32) -> StoredDurableEventV1 {
    let event_id = event_id(ordinal);
    let event_type_id = EventTypeId::try_from(1).expect("event type");
    let payload = CanonicalRecord::new(Vec::new()).expect("payload");
    let event_hash = derive_event_hash_v1(event_id, event_type_id, &payload).expect("event hash");
    StoredDurableEventV1::new(event_id, event_type_id, payload, event_hash).expect("event")
}

fn event_id(ordinal: u32) -> EventId {
    EventId::new(CommitSequence::first(), ordinal)
}

fn timestamp(seconds: i64) -> Timestamp {
    Timestamp::new(seconds, 0).expect("timestamp")
}

fn storage_error(kind: StorageErrorKind) -> StorageError {
    StorageError::new(kind, None)
}

fn assert_delivering(repository: &FakeRepository, expected_attempt: u32) {
    let status = repository.status(event_id(0)).expect("explicit status");
    assert!(matches!(
        status.state(),
        OutboxDeliveryStateV1::Delivering { attempt, .. }
            if attempt.get() == expected_attempt
    ));
}

fn assert_delivered(repository: &FakeRepository, expected_attempts: u32) {
    let status = repository.status(event_id(0)).expect("explicit status");
    assert!(matches!(
        status.state(),
        OutboxDeliveryStateV1::Delivered { attempts, .. }
            if attempts.get() == expected_attempts
    ));
}

#[test]
fn accepted_delivery_lost_before_status_is_retried_with_stable_identity() {
    let repository = FakeRepository::with_events(1);
    let connector = SharedConnector::new(Vec::new(), ConnectorDisposition::Accepted);
    let clock = SharedClock::new(10);
    let policy = policy(3);
    let mut dispatcher = OutboxDispatcher::new(
        recovered(repository.clone(), &policy, &clock),
        connector.clone(),
        clock.clone(),
        OneShotFailpoint::new(OutboxFailpoint::DeliveryAfterConnectorAcceptedBeforeSuccess),
        NoOutboxTelemetry,
        policy.clone(),
    );

    assert_eq!(
        dispatcher.dispatch_once(),
        Err(OutboxWorkerError::Interrupted {
            failpoint: OutboxFailpoint::DeliveryAfterConnectorAcceptedBeforeSuccess
        })
    );
    assert_delivering(&repository, 1);
    assert_eq!(connector.observations().len(), 1);

    drop(dispatcher);
    clock.set(20);
    let recovered = match RecoveringOutbox::after_authoritative_readiness(repository.clone())
        .recover(
            &policy,
            &mut clock.clone(),
            &mut NoOutboxFailpoints,
            &mut NoOutboxTelemetry,
        ) {
        OutboxRecoveryResult::Ready { recovered, report } => {
            assert_eq!(report.normalized(), 1);
            recovered
        }
        OutboxRecoveryResult::Degraded { error, .. } => {
            panic!("unexpected recovery failure: {error}")
        }
    };
    let status_page = repository
        .read_outbox_status_page(OutboxStatusPageRequest::new(None, policy.scan_limit()))
        .expect("payload-free status");
    assert_eq!(
        status_page.items()[0].state(),
        OutboxStatusState::RetryScheduled
    );
    assert_eq!(status_page.items()[0].attempts(), 1);

    clock.set(21);
    let mut restarted = OutboxDispatcher::new(
        recovered,
        connector.clone(),
        clock.clone(),
        NoOutboxFailpoints,
        NoOutboxTelemetry,
        policy,
    );
    let report = restarted.dispatch_once().expect("retry dispatch");
    assert_eq!(report.delivered(), 1);
    assert_delivered(&repository, 2);

    let observations = connector.observations();
    assert_eq!(observations.len(), 2);
    assert_eq!(observations[0].event_id, observations[1].event_id);
    assert_eq!(observations[0].event_hash, observations[1].event_hash);
    assert_eq!(
        observations
            .iter()
            .map(|observation| observation.attempt)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(repository.authoritative_counts(), (1, 1));
}

#[test]
fn crash_after_claim_never_loses_the_committed_event() {
    let repository = FakeRepository::with_events(1);
    let connector = SharedConnector::new(Vec::new(), ConnectorDisposition::Accepted);
    let clock = SharedClock::new(10);
    let policy = policy(3);
    let mut dispatcher = OutboxDispatcher::new(
        recovered(repository.clone(), &policy, &clock),
        connector.clone(),
        clock.clone(),
        OneShotFailpoint::new(OutboxFailpoint::DeliveryAfterClaimBeforeConnector),
        NoOutboxTelemetry,
        policy.clone(),
    );

    assert!(matches!(
        dispatcher.dispatch_once(),
        Err(OutboxWorkerError::Interrupted {
            failpoint: OutboxFailpoint::DeliveryAfterClaimBeforeConnector
        })
    ));
    assert_delivering(&repository, 1);
    assert!(connector.observations().is_empty());

    drop(dispatcher);
    clock.set(20);
    let recovered = recovered(repository.clone(), &policy, &clock);
    clock.set(21);
    let mut restarted = OutboxDispatcher::new(
        recovered,
        connector.clone(),
        clock,
        NoOutboxFailpoints,
        NoOutboxTelemetry,
        policy,
    );
    restarted.dispatch_once().expect("recovered dispatch");

    assert_delivered(&repository, 2);
    assert_eq!(connector.observations().len(), 1);
    assert_eq!(repository.authoritative_counts(), (1, 1));
}

#[test]
fn retryable_failure_preserves_backoff_and_attempt_metadata() {
    let repository = FakeRepository::with_events(1);
    let connector = SharedConnector::new(
        vec![ConnectorDisposition::Retryable {
            safe_error: Some(
                OutboxSafeErrorV1::new("bounded retryable failure").expect("safe error"),
            ),
        }],
        ConnectorDisposition::Accepted,
    );
    let clock = SharedClock::new(10);
    let policy = policy(3);
    let mut dispatcher = OutboxDispatcher::new(
        recovered(repository.clone(), &policy, &clock),
        connector.clone(),
        clock.clone(),
        NoOutboxFailpoints,
        NoOutboxTelemetry,
        policy.clone(),
    );

    let first = dispatcher.dispatch_once().expect("first dispatch");
    assert_eq!(first.retries_scheduled(), 1);
    let pending = repository.status(event_id(0)).expect("pending status");
    let OutboxDeliveryStateV1::Pending(metadata) = pending.state() else {
        panic!("retry must be explicit pending")
    };
    assert_eq!(metadata.attempts().get(), 1);
    assert_eq!(metadata.next_attempt_at(), Some(timestamp(11)));
    assert_eq!(
        metadata.last_safe_error().expect("safe error").as_str(),
        "bounded retryable failure"
    );

    let deferred = dispatcher.dispatch_once().expect("deferred scan");
    assert_eq!(deferred.deferred(), 1);
    assert_eq!(connector.observations().len(), 1);

    clock.set(11);
    let completed = dispatcher.dispatch_once().expect("second attempt");
    assert_eq!(completed.delivered(), 1);
    assert_delivered(&repository, 2);
    assert_eq!(connector.observations().len(), 2);
}

#[test]
fn recovery_is_idempotent_after_crash_immediately_after_normalization() {
    let repository = FakeRepository::with_events(1);
    repository.set_status(StoredOutboxStatusV1::delivering(
        event_id(0),
        NonZeroU32::new(1).expect("attempt"),
        riffdb_storage_api::OutboxDestinationIdV1::new("test/deterministic").expect("destination"),
        timestamp(5),
        timestamp(15),
    ));
    let clock = SharedClock::new(20);
    let policy = policy(3);
    let result = RecoveringOutbox::after_authoritative_readiness(repository.clone()).recover(
        &policy,
        &mut clock.clone(),
        &mut OneShotFailpoint::new(OutboxFailpoint::RecoveryAfterNormalize),
        &mut NoOutboxTelemetry,
    );
    let recovering = match result {
        OutboxRecoveryResult::Degraded {
            recovering,
            report,
            error,
        } => {
            assert_eq!(report.normalized(), 1);
            assert!(matches!(
                error,
                OutboxWorkerError::Interrupted {
                    failpoint: OutboxFailpoint::RecoveryAfterNormalize
                }
            ));
            recovering
        }
        OutboxRecoveryResult::Ready { .. } => panic!("failpoint must withhold readiness"),
    };

    let second = recovering.recover(
        &policy,
        &mut clock.clone(),
        &mut NoOutboxFailpoints,
        &mut NoOutboxTelemetry,
    );
    match second {
        OutboxRecoveryResult::Ready { report, .. } => {
            assert_eq!(report.normalized(), 0);
        }
        OutboxRecoveryResult::Degraded { error, .. } => {
            panic!("second recovery must be idempotent: {error}")
        }
    }
    let status = repository.status(event_id(0)).expect("normalized status");
    let OutboxDeliveryStateV1::Pending(metadata) = status.state() else {
        panic!("normalized status must stay pending")
    };
    assert_eq!(metadata.attempts().get(), 1);
    assert_eq!(metadata.next_attempt_at(), Some(timestamp(21)));
}

#[test]
fn orphan_status_degrades_recovery_and_status_source() {
    let repository = FakeRepository::with_events(1);
    repository.insert_orphan_status(StoredOutboxStatusV1::dead_letter(
        event_id(1),
        0,
        riffdb_storage_api::OutboxDestinationIdV1::new("test/deterministic").expect("destination"),
        timestamp(1),
        None,
    ));
    let policy = policy(3);
    let result = RecoveringOutbox::after_authoritative_readiness(repository.clone()).recover(
        &policy,
        &mut SharedClock::new(10),
        &mut NoOutboxFailpoints,
        &mut NoOutboxTelemetry,
    );

    match result {
        OutboxRecoveryResult::Degraded { error, report, .. } => {
            assert!(matches!(
                error,
                OutboxWorkerError::Storage(ref error)
                    if error.kind() == StorageErrorKind::CorruptData
            ));
            assert_eq!(report.findings().len(), 1);
        }
        OutboxRecoveryResult::Ready { .. } => panic!("orphan status must fail closed"),
    }
    assert!(
        repository
            .read_outbox_status_page(OutboxStatusPageRequest::new(None, policy.scan_limit()))
            .is_err()
    );
    assert_eq!(repository.authoritative_counts(), (1, 1));
}

#[test]
fn retry_policy_exhaustion_is_visible_as_dead_letter() {
    let repository = FakeRepository::with_events(1);
    let connector = SharedConnector::new(
        vec![ConnectorDisposition::Retryable {
            safe_error: Some(OutboxSafeErrorV1::new("terminal retry").expect("safe error")),
        }],
        ConnectorDisposition::Accepted,
    );
    let clock = SharedClock::new(10);
    let policy = policy(1);
    let mut dispatcher = OutboxDispatcher::new(
        recovered(repository.clone(), &policy, &clock),
        connector.clone(),
        clock,
        NoOutboxFailpoints,
        NoOutboxTelemetry,
        policy,
    );

    let report = dispatcher.dispatch_once().expect("terminal attempt");
    assert_eq!(report.dead_lettered(), 1);
    let status = repository.status(event_id(0)).expect("dead-letter status");
    assert!(matches!(
        status.state(),
        OutboxDeliveryStateV1::DeadLetter { attempts: 1, .. }
    ));
    let page = repository
        .read_outbox_status_page(OutboxStatusPageRequest::new(
            None,
            OutboxPageLimit::new(NonZeroU16::new(50).expect("limit")).expect("bounded"),
        ))
        .expect("status page");
    assert_eq!(page.items()[0].state(), OutboxStatusState::DeadLetter);
    assert_eq!(page.items()[0].attempts(), 1);
    assert_eq!(connector.observations().len(), 1);
}

#[test]
fn explicit_pending_retry_metadata_survives_recovery_without_rewrite() {
    let repository = FakeRepository::with_events(1);
    let destination =
        riffdb_storage_api::OutboxDestinationIdV1::new("test/deterministic").expect("destination");
    let original = StoredOutboxStatusV1::pending(
        event_id(0),
        OutboxRetryMetadataV1::new(
            NonZeroU32::new(1).expect("attempt"),
            timestamp(5),
            Some(timestamp(100)),
            destination,
            Some(OutboxSafeErrorV1::new("preserved").expect("safe error")),
        ),
    );
    repository.set_status(original.clone());
    let policy = policy(3);
    let result = RecoveringOutbox::after_authoritative_readiness(repository.clone()).recover(
        &policy,
        &mut SharedClock::new(10),
        &mut NoOutboxFailpoints,
        &mut NoOutboxTelemetry,
    );

    match result {
        OutboxRecoveryResult::Ready { report, .. } => {
            assert_eq!(report.normalized(), 0);
        }
        OutboxRecoveryResult::Degraded { error, .. } => {
            panic!("pending status must recover cleanly: {error}")
        }
    }
    assert_eq!(repository.status(event_id(0)), Some(original));
}

#[test]
fn restart_recovery_scans_to_exact_end_across_pages() {
    let repository = FakeRepository::with_events(3);
    let destination =
        riffdb_storage_api::OutboxDestinationIdV1::new("test/deterministic").expect("destination");
    repository.set_status(StoredOutboxStatusV1::delivering(
        event_id(0),
        NonZeroU32::new(1).expect("attempt"),
        destination.clone(),
        timestamp(1),
        timestamp(10),
    ));
    let dead_letter =
        StoredOutboxStatusV1::dead_letter(event_id(1), 2, destination.clone(), timestamp(2), None);
    repository.set_status(dead_letter.clone());
    repository.set_status(StoredOutboxStatusV1::delivering(
        event_id(2),
        NonZeroU32::new(3).expect("attempt"),
        destination,
        timestamp(3),
        timestamp(12),
    ));

    let policy = policy_with_limit(4, 1);
    let result = RecoveringOutbox::after_authoritative_readiness(repository.clone()).recover(
        &policy,
        &mut SharedClock::new(20),
        &mut NoOutboxFailpoints,
        &mut NoOutboxTelemetry,
    );

    match result {
        OutboxRecoveryResult::Ready { report, .. } => {
            assert_eq!(report.scanned(), 3);
            assert_eq!(report.normalized(), 2);
            assert!(report.findings().is_empty());
        }
        OutboxRecoveryResult::Degraded { error, .. } => {
            panic!("complete restart scan must recover: {error}")
        }
    }
    assert!(matches!(
        repository
            .status(event_id(0))
            .expect("first normalized")
            .state(),
        OutboxDeliveryStateV1::Pending(_)
    ));
    assert_eq!(repository.status(event_id(1)), Some(dead_letter));
    assert!(matches!(
        repository
            .status(event_id(2))
            .expect("second normalized")
            .state(),
        OutboxDeliveryStateV1::Pending(_)
    ));
}

#[test]
fn recovery_continuation_retains_the_first_page_upper_fence() {
    let repository = FakeRepository::with_events(2);
    repository.set_status(StoredOutboxStatusV1::delivering(
        event_id(0),
        NonZeroU32::new(1).expect("attempt"),
        riffdb_storage_api::OutboxDestinationIdV1::new("test/deterministic").expect("destination"),
        timestamp(1),
        timestamp(10),
    ));
    let policy = policy_with_limit(3, 1);
    let mut telemetry = InsertAfterFirstNormalization::new(repository.clone());

    let result = RecoveringOutbox::after_authoritative_readiness(repository.clone()).recover(
        &policy,
        &mut SharedClock::new(20),
        &mut NoOutboxFailpoints,
        &mut telemetry,
    );

    match result {
        OutboxRecoveryResult::Ready { report, .. } => {
            assert_eq!(report.scanned(), 2);
            assert_eq!(report.normalized(), 1);
        }
        OutboxRecoveryResult::Degraded { error, .. } => {
            panic!("frozen continuation must recover: {error}")
        }
    }
    assert_eq!(repository.authoritative_counts(), (3, 3));

    let second = RecoveringOutbox::after_authoritative_readiness(repository).recover(
        &policy,
        &mut SharedClock::new(30),
        &mut NoOutboxFailpoints,
        &mut NoOutboxTelemetry,
    );
    match second {
        OutboxRecoveryResult::Ready { report, .. } => {
            assert_eq!(report.scanned(), 3);
            assert_eq!(report.normalized(), 0);
        }
        OutboxRecoveryResult::Degraded { error, .. } => {
            panic!("next recovery must capture the later event: {error}")
        }
    }
}

#[test]
fn status_scan_includes_dead_letter_and_excludes_delivered() {
    let repository = FakeRepository::with_events(3);
    let destination =
        riffdb_storage_api::OutboxDestinationIdV1::new("test/deterministic").expect("destination");
    repository.set_status(StoredOutboxStatusV1::dead_letter(
        event_id(0),
        2,
        destination.clone(),
        timestamp(2),
        None,
    ));
    repository.set_status(StoredOutboxStatusV1::delivered(
        event_id(1),
        NonZeroU32::new(1).expect("attempt"),
        destination,
        timestamp(3),
    ));

    let page = repository
        .read_outbox_status_page(OutboxStatusPageRequest::new(
            None,
            OutboxPageLimit::new(NonZeroU16::new(3).expect("limit")).expect("bounded"),
        ))
        .expect("status scan");

    assert_eq!(page.items().len(), 2);
    assert_eq!(page.items()[0].event_id(), event_id(0));
    assert_eq!(page.items()[0].state(), OutboxStatusState::DeadLetter);
    assert_eq!(page.items()[1].event_id(), event_id(2));
    assert_eq!(page.items()[1].state(), OutboxStatusState::Pending);
    assert_eq!(page.next_after(), None);
}

#[test]
fn recovery_state_change_fails_closed_without_overwrite() {
    let repository = FakeRepository::with_events(1);
    let destination =
        riffdb_storage_api::OutboxDestinationIdV1::new("test/deterministic").expect("destination");
    repository.set_status(StoredOutboxStatusV1::delivering(
        event_id(0),
        NonZeroU32::new(1).expect("attempt"),
        destination.clone(),
        timestamp(1),
        timestamp(10),
    ));
    let concurrent = StoredOutboxStatusV1::pending(
        event_id(0),
        OutboxRetryMetadataV1::new(
            NonZeroU32::new(1).expect("attempt"),
            timestamp(5),
            Some(timestamp(30)),
            destination,
            Some(OutboxSafeErrorV1::new("concurrent retry").expect("safe error")),
        ),
    );
    repository.change_during_next_retry(concurrent.clone());

    let result = RecoveringOutbox::after_authoritative_readiness(repository.clone()).recover(
        &policy(3),
        &mut SharedClock::new(20),
        &mut NoOutboxFailpoints,
        &mut NoOutboxTelemetry,
    );

    match result {
        OutboxRecoveryResult::Degraded { error, report, .. } => {
            assert!(matches!(
                error,
                OutboxWorkerError::StateChanged {
                    phase: riffdb_outbox::OutboxTransitionPhase::Recovery
                }
            ));
            assert_eq!(report.normalized(), 0);
            assert_eq!(report.findings().len(), 1);
            assert_eq!(
                report.findings()[0].code(),
                riffdb_outbox::OutboxRecoveryFindingCode::StateChanged
            );
        }
        OutboxRecoveryResult::Ready { .. } => {
            panic!("a recovery CAS race must withhold derived readiness")
        }
    }
    assert_eq!(repository.status(event_id(0)), Some(concurrent));
}

// req: EFF-002
#[test]
fn review_interrupted_page_retries_before_advancing_cursor() {
    let repository = FakeRepository::with_events(4);
    let connector = SharedConnector::new(Vec::new(), ConnectorDisposition::Accepted);
    let clock = SharedClock::new(10);
    let policy = policy_with_limit(3, 2);
    let mut dispatcher = OutboxDispatcher::new(
        recovered(repository.clone(), &policy, &clock),
        connector,
        clock,
        OneShotFailpoint::new(OutboxFailpoint::DeliveryAfterConnectorAcceptedBeforeSuccess),
        NoOutboxTelemetry,
        policy,
    );
    assert!(dispatcher.dispatch_once().is_err());
    let report = dispatcher.dispatch_once().unwrap();
    assert_eq!(report.delivered(), 2);
    assert!(matches!(
        repository.status(event_id(1)).unwrap().state(),
        OutboxDeliveryStateV1::Delivered { .. }
    ));
    assert_eq!(repository.status(event_id(3)), None);
}

// req: EFF-002
#[test]
fn review_completion_state_change_keeps_remaining_page_reachable() {
    let repository = FakeRepository::with_events(3);
    let connector = SharedConnector::new(
        vec![ConnectorDisposition::Retryable { safe_error: None }],
        ConnectorDisposition::Accepted,
    );
    let clock = SharedClock::new(10);
    let policy = policy_with_limit(3, 2);
    let mut dispatcher = OutboxDispatcher::new(
        recovered(repository.clone(), &policy, &clock),
        connector,
        clock,
        NoOutboxFailpoints,
        NoOutboxTelemetry,
        policy,
    );
    let concurrent = StoredOutboxStatusV1::pending(
        event_id(0),
        OutboxRetryMetadataV1::new(
            NonZeroU32::new(1).unwrap(),
            timestamp(10),
            Some(timestamp(30)),
            riffdb_storage_api::OutboxDestinationIdV1::new("test/deterministic").unwrap(),
            None,
        ),
    );
    repository.change_during_next_retry(concurrent.clone());
    assert!(matches!(
        dispatcher.dispatch_once(),
        Err(OutboxWorkerError::StateChanged {
            phase: riffdb_outbox::OutboxTransitionPhase::Retry
        })
    ));
    assert_eq!(repository.status(event_id(0)), Some(concurrent));
    let report = dispatcher.dispatch_once().unwrap();
    assert_eq!(report.deferred(), 1);
    assert_eq!(report.delivered(), 1);
    assert!(matches!(
        repository.status(event_id(1)).unwrap().state(),
        OutboxDeliveryStateV1::Delivered { .. }
    ));
    assert_eq!(repository.status(event_id(2)), None);
}

// req: EFF-002, EFF-003, REC-001, REC-002, STO-020
#[cfg(target_os = "linux")]
#[test]
fn durable_local_quota_pauses_before_claim_and_uncertain_completion_replays_exact_archive() {
    use riffdb_outbox::{DurableLocalArchive, DurableLocalConnector};
    use riffdb_types::DatabaseId;
    use std::num::NonZeroU64;
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::Builder::new()
        .permissions(std::fs::Permissions::from_mode(0o700))
        .tempdir()
        .unwrap();
    let binding = DatabaseId::from_unix_milliseconds_and_random(42, [1; 10]).unwrap();
    let open = |quota| {
        let mut archive = DurableLocalArchive::open(root.path(), binding, quota).unwrap();
        assert!(archive.validate_next_page().unwrap());
        DurableLocalConnector::new(archive, NonZeroU64::MIN)
    };
    let repository = FakeRepository::with_events(1);
    let clock = SharedClock::new(10);
    let delivery = policy(3);
    let mut paused = OutboxDispatcher::new(
        recovered(repository.clone(), &delivery, &clock),
        open(NonZeroU64::MIN),
        clock.clone(),
        NoOutboxFailpoints,
        NoOutboxTelemetry,
        delivery.clone(),
    );
    for _ in 0..40 {
        let report = paused.dispatch_once().unwrap();
        assert_eq!(report.claims(), 0);
        assert_eq!(report.deferred(), 1);
        assert!(repository.status(event_id(0)).is_none());
    }
    drop(paused);
    let mut uncertain = OutboxDispatcher::new(
        recovered(repository.clone(), &delivery, &clock),
        open(NonZeroU64::MAX),
        clock.clone(),
        OneShotFailpoint::new(OutboxFailpoint::DeliveryAfterConnectorAcceptedBeforeSuccess),
        NoOutboxTelemetry,
        delivery.clone(),
    );
    assert!(matches!(
        uncertain.dispatch_once(),
        Err(OutboxWorkerError::Interrupted { .. })
    ));
    assert_delivering(&repository, 1);
    let counts = uncertain.connector().archive().observation().unwrap();
    assert_eq!(counts.records(), 1);
    let key = riffdb_storage_api::LocalOutboxArchiveRecordV1::new(
        binding,
        NonZeroU64::MIN,
        make_event(0),
    )
    .key();
    let first = uncertain.connector().archive().read(&key).unwrap().unwrap();
    drop(uncertain);
    clock.set(20);
    let recovered = recovered(repository.clone(), &delivery, &clock);
    clock.set(21);
    // Equal replay is allowed even when the current quota is exactly full.
    let mut restarted = OutboxDispatcher::new(
        recovered,
        open(NonZeroU64::new(counts.encoded_bytes()).unwrap()),
        clock,
        NoOutboxFailpoints,
        NoOutboxTelemetry,
        delivery,
    );
    assert_eq!(restarted.dispatch_once().unwrap().delivered(), 1);
    assert_delivered(&repository, 2);
    assert_eq!(
        restarted.connector().archive().observation().unwrap(),
        counts
    );
    assert_eq!(
        restarted.connector().archive().read(&key).unwrap(),
        Some(first)
    );
    assert_eq!(repository.authoritative_counts(), (1, 1));
}

// req: REC-001
#[test]
fn recovery_page_admission_withdrawal_cannot_publish_dispatcher_readiness() {
    let repository = FakeRepository::with_events(3);
    let clock = SharedClock::new(10);
    let delivery = policy_with_limit(3, 1);
    let mut pages = 0;
    let result = RecoveringOutbox::after_authoritative_readiness(repository.clone())
        .recover_with_page_admission(
            &delivery,
            &mut clock.clone(),
            &mut NoOutboxFailpoints,
            &mut NoOutboxTelemetry,
            || {
                pages += 1;
                pages == 1
            },
        );
    assert_eq!(pages, 2);
    let OutboxRecoveryResult::Degraded {
        recovering, report, ..
    } = result
    else {
        panic!("withdrawal cannot grant readiness");
    };
    assert_eq!(report.scanned(), 1);
    assert_eq!(report.normalized(), 0);
    assert!(matches!(
        recovering.recover(
            &delivery,
            &mut clock.clone(),
            &mut NoOutboxFailpoints,
            &mut NoOutboxTelemetry
        ),
        OutboxRecoveryResult::Ready { .. }
    ));
    assert_eq!(repository.authoritative_counts(), (3, 3));
}
