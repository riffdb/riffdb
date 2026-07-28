//! Authoritative outbox reads and typed derived delivery-state transitions.

use std::fmt;
use std::num::{NonZeroU16, NonZeroU32};

use riffdb_types::{EventId, Timestamp};

use crate::{
    EncodedPageItem, MAX_SCAN_PAGE_BYTES, MAX_SCAN_PAGE_ENTRIES, StorageError, StorageValueError,
    StoredDurableEventV1, StoredOutboxIntentV1, checked_encoded_page_content,
};

#[cfg(test)]
use crate::canonical_record_bytes;

/// Maximum bytes in one configured destination identity retained in status.
pub const MAX_OUTBOX_DESTINATION_ID_BYTES: usize = 512;
/// Maximum bytes in one scrubbed safe connector error retained in status.
pub const MAX_OUTBOX_SAFE_ERROR_BYTES: usize = 1_024;

/// Checked nonzero pending-scan row limit under the storage page ceiling.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OutboxPageLimit(NonZeroU16);

impl OutboxPageLimit {
    /// Rejects caller-selected limits above the fixed storage page bound.
    pub fn new(value: NonZeroU16) -> Result<Self, StorageValueError> {
        if usize::from(value.get()) > MAX_SCAN_PAGE_ENTRIES {
            return Err(StorageValueError::LimitExceeded);
        }
        Ok(Self(value))
    }

    /// Returns the checked nonzero row limit.
    #[must_use]
    pub const fn get(self) -> NonZeroU16 {
        self.0
    }
}

/// A bounded exact destination configuration reference.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OutboxDestinationIdV1(String);

impl OutboxDestinationIdV1 {
    /// Validates nonempty visible ASCII without normalizing it.
    pub fn new(value: impl Into<String>) -> Result<Self, StorageValueError> {
        let value = value.into();
        if value.is_empty() {
            return Err(StorageValueError::Empty);
        }
        if value.len() > MAX_OUTBOX_DESTINATION_ID_BYTES {
            return Err(StorageValueError::LimitExceeded);
        }
        if value.bytes().any(|byte| !(0x21..=0x7e).contains(&byte)) {
            return Err(StorageValueError::InvalidShape);
        }
        Ok(Self(value))
    }

    /// Borrows the exact configured reference.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for OutboxDestinationIdV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("OutboxDestinationIdV1")
            .field(&self.0)
            .finish()
    }
}

/// One bounded, already-scrubbed connector failure summary.
#[derive(Clone, Eq, PartialEq)]
pub struct OutboxSafeErrorV1(String);

impl OutboxSafeErrorV1 {
    /// Retains a nonempty bounded safe summary without normalizing it.
    pub fn new(value: impl Into<String>) -> Result<Self, StorageValueError> {
        let value = value.into();
        if value.is_empty() {
            return Err(StorageValueError::Empty);
        }
        if value.len() > MAX_OUTBOX_SAFE_ERROR_BYTES {
            return Err(StorageValueError::LimitExceeded);
        }
        Ok(Self(value))
    }

    /// Borrows the policy-scrubbed safe summary.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for OutboxSafeErrorV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("OutboxSafeErrorV1([REDACTED])")
    }
}

/// Retry metadata retained only after at least one delivery attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutboxRetryMetadataV1 {
    attempts: NonZeroU32,
    last_attempt_at: Timestamp,
    next_attempt_at: Option<Timestamp>,
    destination_id: OutboxDestinationIdV1,
    last_safe_error: Option<OutboxSafeErrorV1>,
}

impl OutboxRetryMetadataV1 {
    /// Constructs worker-selected bounded retry metadata.
    #[must_use]
    pub const fn new(
        attempts: NonZeroU32,
        last_attempt_at: Timestamp,
        next_attempt_at: Option<Timestamp>,
        destination_id: OutboxDestinationIdV1,
        last_safe_error: Option<OutboxSafeErrorV1>,
    ) -> Self {
        Self {
            attempts,
            last_attempt_at,
            next_attempt_at,
            destination_id,
            last_safe_error,
        }
    }

    /// Returns the number of attempts already begun.
    #[must_use]
    pub const fn attempts(&self) -> NonZeroU32 {
        self.attempts
    }

    /// Returns the most recent worker-supplied attempt time.
    #[must_use]
    pub const fn last_attempt_at(&self) -> Timestamp {
        self.last_attempt_at
    }

    /// Returns the worker-selected next eligible time, if any.
    #[must_use]
    pub const fn next_attempt_at(&self) -> Option<Timestamp> {
        self.next_attempt_at
    }

    /// Borrows the selected destination identity.
    #[must_use]
    pub const fn destination_id(&self) -> &OutboxDestinationIdV1 {
        &self.destination_id
    }

    /// Borrows the last policy-scrubbed connector failure, if retained.
    #[must_use]
    pub const fn last_safe_error(&self) -> Option<&OutboxSafeErrorV1> {
        self.last_safe_error.as_ref()
    }
}

/// Closed derived delivery state stored after the canonical absent initial state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OutboxDeliveryStateV1 {
    /// A prior attempt may be retried according to worker policy.
    Pending(OutboxRetryMetadataV1),
    /// One attempt is currently leased by the single-node worker.
    Delivering {
        /// Nonzero attempt number, used to fence stale completions.
        attempt: NonZeroU32,
        /// Worker-selected destination configuration.
        destination_id: OutboxDestinationIdV1,
        /// Worker-supplied attempt start time.
        started_at: Timestamp,
        /// Worker-supplied lease deadline; duration policy is not storage-owned.
        lease_deadline: Timestamp,
    },
    /// Delivery completed for this destination.
    Delivered {
        /// Final nonzero attempt number.
        attempts: NonZeroU32,
        /// Destination that accepted the event.
        destination_id: OutboxDestinationIdV1,
        /// Worker-supplied completion time.
        delivered_at: Timestamp,
    },
    /// Worker policy selected a terminal delivery failure.
    DeadLetter {
        /// Number of attempts begun before the terminal decision.
        attempts: u32,
        /// Selected destination configuration.
        destination_id: OutboxDestinationIdV1,
        /// Worker-supplied terminal decision time.
        failed_at: Timestamp,
        /// Optional bounded policy-scrubbed failure summary.
        last_safe_error: Option<OutboxSafeErrorV1>,
    },
}

impl OutboxDeliveryStateV1 {
    /// Returns whether the state is eligible for the pending scanner.
    #[must_use]
    pub const fn is_pending(&self) -> bool {
        matches!(self, Self::Pending(_))
    }

    /// Returns attempts already begun.
    #[must_use]
    pub const fn attempts(&self) -> u32 {
        match self {
            Self::Pending(metadata) => metadata.attempts.get(),
            Self::Delivering { attempt, .. } => attempt.get(),
            Self::Delivered { attempts, .. } => attempts.get(),
            Self::DeadLetter { attempts, .. } => *attempts,
        }
    }

    #[cfg(test)]
    fn semantic_bytes(&self) -> Result<usize, StorageValueError> {
        match self {
            Self::Pending(metadata) => checked_outbox_sum([
                1,
                4,
                12,
                1 + metadata.next_attempt_at.map_or(0, |_| 12),
                bounded_text_semantic_bytes(metadata.destination_id.as_str())?,
                optional_text_semantic_bytes(metadata.last_safe_error.as_ref())?,
            ]),
            Self::Delivering { destination_id, .. } => checked_outbox_sum([
                1,
                4,
                bounded_text_semantic_bytes(destination_id.as_str())?,
                12,
                12,
            ]),
            Self::Delivered { destination_id, .. } => checked_outbox_sum([
                1,
                4,
                bounded_text_semantic_bytes(destination_id.as_str())?,
                12,
            ]),
            Self::DeadLetter {
                destination_id,
                last_safe_error,
                ..
            } => checked_outbox_sum([
                1,
                4,
                bounded_text_semantic_bytes(destination_id.as_str())?,
                12,
                optional_text_semantic_bytes(last_safe_error.as_ref())?,
            ]),
        }
    }
}

/// One present derived status row, always linked by the same event ID as its key.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredOutboxStatusV1 {
    event_id: EventId,
    state: OutboxDeliveryStateV1,
}

impl StoredOutboxStatusV1 {
    /// Constructs an explicit retryable pending row.
    #[must_use]
    pub const fn pending(event_id: EventId, metadata: OutboxRetryMetadataV1) -> Self {
        Self {
            event_id,
            state: OutboxDeliveryStateV1::Pending(metadata),
        }
    }

    /// Constructs an in-flight attempt row.
    #[must_use]
    pub const fn delivering(
        event_id: EventId,
        attempt: NonZeroU32,
        destination_id: OutboxDestinationIdV1,
        started_at: Timestamp,
        lease_deadline: Timestamp,
    ) -> Self {
        Self {
            event_id,
            state: OutboxDeliveryStateV1::Delivering {
                attempt,
                destination_id,
                started_at,
                lease_deadline,
            },
        }
    }

    /// Constructs a completed status row.
    #[must_use]
    pub const fn delivered(
        event_id: EventId,
        attempts: NonZeroU32,
        destination_id: OutboxDestinationIdV1,
        delivered_at: Timestamp,
    ) -> Self {
        Self {
            event_id,
            state: OutboxDeliveryStateV1::Delivered {
                attempts,
                destination_id,
                delivered_at,
            },
        }
    }

    /// Constructs a worker-selected terminal failure row.
    #[must_use]
    pub const fn dead_letter(
        event_id: EventId,
        attempts: u32,
        destination_id: OutboxDestinationIdV1,
        failed_at: Timestamp,
        last_safe_error: Option<OutboxSafeErrorV1>,
    ) -> Self {
        Self {
            event_id,
            state: OutboxDeliveryStateV1::DeadLetter {
                attempts,
                destination_id,
                failed_at,
                last_safe_error,
            },
        }
    }

    /// Returns the linked stable event identity.
    #[must_use]
    pub const fn event_id(&self) -> EventId {
        self.event_id
    }

    /// Borrows the complete closed delivery state.
    #[must_use]
    pub const fn state(&self) -> &OutboxDeliveryStateV1 {
        &self.state
    }
}

/// Exact stored observation used by compare-and-transition operations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OutboxStatusObservationV1 {
    /// No row: canonical never-attempted pending with zero attempts.
    AbsentInitialPending,
    /// One explicit status row.
    Present(StoredOutboxStatusV1),
}

impl OutboxStatusObservationV1 {
    /// Returns whether this is either canonical or explicit pending state.
    #[must_use]
    pub const fn is_pending(&self) -> bool {
        match self {
            Self::AbsentInitialPending => true,
            Self::Present(status) => status.state.is_pending(),
        }
    }

    /// Returns attempts already begun, with absent status canonically zero.
    #[must_use]
    pub const fn attempts(&self) -> u32 {
        match self {
            Self::AbsentInitialPending => 0,
            Self::Present(status) => status.state.attempts(),
        }
    }

    fn validate_event_id(&self, event_id: EventId) -> Result<(), StorageValueError> {
        if let Self::Present(status) = self
            && status.event_id != event_id
        {
            return Err(StorageValueError::IdentityMismatch);
        }
        Ok(())
    }

    #[cfg(test)]
    fn semantic_bytes(&self) -> Result<usize, StorageValueError> {
        match self {
            Self::AbsentInitialPending => Ok(1),
            Self::Present(status) => {
                let state_bytes = status.state.semantic_bytes()?;
                1usize
                    .checked_add(12)
                    .and_then(|value| value.checked_add(state_bytes))
                    .ok_or(StorageValueError::SizeOverflow)
            }
        }
    }
}

/// One pending authoritative event/intent tuple and its exact status observation.
#[derive(Clone, Eq, PartialEq)]
pub struct PendingOutboxItemV1 {
    event: StoredDurableEventV1,
    intent: StoredOutboxIntentV1,
    status: OutboxStatusObservationV1,
}

impl fmt::Debug for PendingOutboxItemV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PendingOutboxItemV1")
            .field("event_id", &self.event_id())
            .field("authoritative_event", &"[REDACTED]")
            .field("authoritative_intent", &"[REDACTED]")
            .field("status", &self.status)
            .finish()
    }
}

impl PendingOutboxItemV1 {
    /// Checks reciprocal authoritative content and pending status.
    pub fn new(
        event: StoredDurableEventV1,
        intent: StoredOutboxIntentV1,
        status: OutboxStatusObservationV1,
    ) -> Result<Self, StorageValueError> {
        if intent.event() != &event || !status.is_pending() {
            return Err(StorageValueError::IdentityMismatch);
        }
        status.validate_event_id(event.event_id())?;
        Ok(Self {
            event,
            intent,
            status,
        })
    }

    /// Returns the stable downstream deduplication identity.
    #[must_use]
    pub const fn event_id(&self) -> EventId {
        self.event.event_id()
    }

    /// Borrows the authoritative event.
    #[must_use]
    pub const fn event(&self) -> &StoredDurableEventV1 {
        &self.event
    }

    /// Borrows the reciprocal authoritative intent.
    #[must_use]
    pub const fn intent(&self) -> &StoredOutboxIntentV1 {
        &self.intent
    }

    /// Borrows the exact absent-or-present status observation.
    #[must_use]
    pub const fn status(&self) -> &OutboxStatusObservationV1 {
        &self.status
    }

    #[cfg(test)]
    fn semantic_bytes(&self) -> Result<usize, StorageValueError> {
        let authoritative_event = checked_outbox_sum([
            bounded_bytes_semantic_bytes(canonical_record_bytes(self.event.payload())?)?,
            12,
            self.event.event_type_id().to_be_bytes().len(),
            self.event.event_hash().as_bytes().len(),
        ])?;
        let status = self.status.semantic_bytes()?;
        authoritative_event
            .checked_mul(2)
            .and_then(|value| value.checked_add(status))
            .ok_or(StorageValueError::SizeOverflow)
    }
}

/// Exact-end bounded pending scan result in increasing `EventId` order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PendingOutboxScanV1 {
    /// A non-final page with its exclusive next lower bound.
    Page {
        /// Pending authoritative tuples in strict order.
        items: Vec<EncodedPageItem<PendingOutboxItemV1>>,
        /// Last returned event ID.
        next_after: EventId,
    },
    /// The supplied scan reached the exact end, including an empty result.
    ExactEnd {
        /// Final pending tuples in strict order.
        items: Vec<EncodedPageItem<PendingOutboxItemV1>>,
    },
}

impl PendingOutboxScanV1 {
    /// Validates count, bytes, strict order, and exact-end shape.
    pub fn page(
        items: Vec<EncodedPageItem<PendingOutboxItemV1>>,
        has_more: bool,
    ) -> Result<Self, StorageValueError> {
        if items.len() > MAX_SCAN_PAGE_ENTRIES {
            return Err(StorageValueError::LimitExceeded);
        }
        if items
            .windows(2)
            .any(|pair| pair[0].value().event_id() >= pair[1].value().event_id())
        {
            return Err(StorageValueError::NonCanonicalOrder);
        }
        checked_encoded_page_content(&items, MAX_SCAN_PAGE_BYTES)?;
        if has_more {
            let next_after = items
                .last()
                .ok_or(StorageValueError::InvalidShape)?
                .value()
                .event_id();
            Ok(Self::Page { items, next_after })
        } else {
            Ok(Self::ExactEnd { items })
        }
    }
}

/// One source-validated outbox status that has not reached delivered state.
///
/// Storage constructs this value only after proving the authoritative event,
/// outbox intent, and enclosing commit are reciprocal. Event and intent payloads
/// deliberately do not cross this recovery and administration boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UndeliveredOutboxStatusV1 {
    event_id: EventId,
    status: OutboxStatusObservationV1,
}

impl UndeliveredOutboxStatusV1 {
    /// Checks event identity and rejects already delivered status.
    pub fn new(
        event_id: EventId,
        status: OutboxStatusObservationV1,
    ) -> Result<Self, StorageValueError> {
        status.validate_event_id(event_id)?;
        if matches!(
            status,
            OutboxStatusObservationV1::Present(StoredOutboxStatusV1 {
                state: OutboxDeliveryStateV1::Delivered { .. },
                ..
            })
        ) {
            return Err(StorageValueError::InvalidShape);
        }
        Ok(Self { event_id, status })
    }

    /// Returns the stable event identity.
    #[must_use]
    pub const fn event_id(&self) -> EventId {
        self.event_id
    }

    /// Borrows the exact absent-or-present status observation.
    #[must_use]
    pub const fn status(&self) -> &OutboxStatusObservationV1 {
        &self.status
    }
}

/// Frozen upper fence for one complete undelivered-outbox recovery scan.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutboxRecoveryUpperFenceV1 {
    /// No reciprocal authoritative event and intent existed at the initial read.
    BeforeFirst,
    /// Greatest reciprocal authoritative event visible at the initial read.
    Inclusive(EventId),
}

/// Opaque continuation for one frozen undelivered-outbox recovery scan.
///
/// Only a checked non-final storage result can construct this value. Consumers
/// pass it back unchanged so a continuation cannot silently recapture a newer
/// upper fence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UndeliveredOutboxStatusContinuationV1 {
    after: EventId,
    inclusive_upper: EventId,
}

impl UndeliveredOutboxStatusContinuationV1 {
    /// Returns the exclusive lower event bound.
    #[must_use]
    pub const fn after(self) -> EventId {
        self.after
    }

    /// Returns the frozen inclusive upper event bound.
    #[must_use]
    pub const fn inclusive_upper(self) -> EventId {
        self.inclusive_upper
    }
}

/// Checked request for one page of a frozen undelivered-outbox recovery scan.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UndeliveredOutboxStatusScanRequestV1 {
    /// First page; storage captures the greatest reciprocal event identity.
    Initial {
        /// Optional exclusive lower event bound for a fresh bounded status page.
        after: Option<EventId>,
        /// Bounded page limit.
        limit: OutboxPageLimit,
    },
    /// Later page; the opaque continuation retains the original upper fence.
    Continue {
        /// Continuation returned by the preceding non-final page.
        continuation: UndeliveredOutboxStatusContinuationV1,
        /// Bounded page limit.
        limit: OutboxPageLimit,
    },
}

impl UndeliveredOutboxStatusScanRequestV1 {
    /// Constructs a first-page request.
    #[must_use]
    pub const fn initial(after: Option<EventId>, limit: OutboxPageLimit) -> Self {
        Self::Initial { after, limit }
    }

    /// Constructs a continuation request from the preceding checked page.
    #[must_use]
    pub const fn continuing(
        continuation: UndeliveredOutboxStatusContinuationV1,
        limit: OutboxPageLimit,
    ) -> Self {
        Self::Continue {
            continuation,
            limit,
        }
    }

    /// Returns the exclusive lower event bound, when continuing.
    #[must_use]
    pub const fn after(self) -> Option<EventId> {
        match self {
            Self::Initial { after, .. } => after,
            Self::Continue { continuation, .. } => Some(continuation.after),
        }
    }

    /// Returns the frozen inclusive upper event bound, when continuing.
    #[must_use]
    pub const fn inclusive_upper(self) -> Option<EventId> {
        match self {
            Self::Initial { .. } => None,
            Self::Continue { continuation, .. } => Some(continuation.inclusive_upper),
        }
    }

    /// Returns the checked requested row count.
    #[must_use]
    pub const fn limit(self) -> OutboxPageLimit {
        match self {
            Self::Initial { limit, .. } | Self::Continue { limit, .. } => limit,
        }
    }
}

/// Exact-end bounded undelivered scan in increasing `EventId` order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum UndeliveredOutboxStatusScanV1 {
    /// A non-final page bound to the first read's frozen upper fence.
    Page {
        /// Payload-free statuses with their complete source-record charge.
        items: Vec<EncodedPageItem<UndeliveredOutboxStatusV1>>,
        /// Opaque exclusive continuation retaining the frozen upper fence.
        continuation: UndeliveredOutboxStatusContinuationV1,
    },
    /// The frozen scan reached exact end, including an empty result.
    ExactEnd {
        /// Final payload-free statuses with their complete source-record charge.
        items: Vec<EncodedPageItem<UndeliveredOutboxStatusV1>>,
        /// Upper fence captured by the initial read.
        inclusive_upper: OutboxRecoveryUpperFenceV1,
    },
}

impl UndeliveredOutboxStatusScanV1 {
    /// Checks a non-final page and constructs its opaque continuation.
    pub fn page(
        request: UndeliveredOutboxStatusScanRequestV1,
        inclusive_upper: EventId,
        items: Vec<EncodedPageItem<UndeliveredOutboxStatusV1>>,
    ) -> Result<Self, StorageValueError> {
        validate_undelivered_scan_items(request, inclusive_upper, &items)?;
        let after = items
            .last()
            .ok_or(StorageValueError::InvalidShape)?
            .value()
            .event_id();
        if after >= inclusive_upper {
            return Err(StorageValueError::InvalidShape);
        }
        Ok(Self::Page {
            items,
            continuation: UndeliveredOutboxStatusContinuationV1 {
                after,
                inclusive_upper,
            },
        })
    }

    /// Checks a final page, including an empty before-first result.
    pub fn exact_end(
        request: UndeliveredOutboxStatusScanRequestV1,
        inclusive_upper: OutboxRecoveryUpperFenceV1,
        items: Vec<EncodedPageItem<UndeliveredOutboxStatusV1>>,
    ) -> Result<Self, StorageValueError> {
        match inclusive_upper {
            OutboxRecoveryUpperFenceV1::BeforeFirst => {
                if !matches!(
                    request,
                    UndeliveredOutboxStatusScanRequestV1::Initial { .. }
                ) || !items.is_empty()
                {
                    return Err(StorageValueError::InvalidShape);
                }
            }
            OutboxRecoveryUpperFenceV1::Inclusive(upper) => {
                validate_undelivered_scan_items(request, upper, &items)?;
            }
        }
        Ok(Self::ExactEnd {
            items,
            inclusive_upper,
        })
    }

    /// Borrows the statuses in this page.
    #[must_use]
    pub fn items(&self) -> &[EncodedPageItem<UndeliveredOutboxStatusV1>] {
        match self {
            Self::Page { items, .. } | Self::ExactEnd { items, .. } => items,
        }
    }

    /// Returns the opaque continuation only when more source rows remain.
    #[must_use]
    pub const fn continuation(&self) -> Option<UndeliveredOutboxStatusContinuationV1> {
        match self {
            Self::Page { continuation, .. } => Some(*continuation),
            Self::ExactEnd { .. } => None,
        }
    }

    /// Returns the frozen upper fence.
    #[must_use]
    pub const fn inclusive_upper(&self) -> OutboxRecoveryUpperFenceV1 {
        match self {
            Self::Page { continuation, .. } => {
                OutboxRecoveryUpperFenceV1::Inclusive(continuation.inclusive_upper)
            }
            Self::ExactEnd {
                inclusive_upper, ..
            } => *inclusive_upper,
        }
    }
}

fn validate_undelivered_scan_items(
    request: UndeliveredOutboxStatusScanRequestV1,
    inclusive_upper: EventId,
    items: &[EncodedPageItem<UndeliveredOutboxStatusV1>],
) -> Result<(), StorageValueError> {
    if request
        .inclusive_upper()
        .is_some_and(|expected| expected != inclusive_upper)
    {
        return Err(StorageValueError::IdentityMismatch);
    }
    if items.len() > usize::from(request.limit().get().get()) {
        return Err(StorageValueError::LimitExceeded);
    }
    if items
        .windows(2)
        .any(|pair| pair[0].value().event_id() >= pair[1].value().event_id())
    {
        return Err(StorageValueError::NonCanonicalOrder);
    }
    if items.iter().any(|item| {
        request
            .after()
            .is_some_and(|after| item.value().event_id() <= after)
            || item.value().event_id() > inclusive_upper
    }) {
        return Err(StorageValueError::InvalidShape);
    }
    checked_encoded_page_content(items, MAX_SCAN_PAGE_BYTES).map(|_| ())
}

/// Checked pending-to-delivering transition request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutboxClaimV1 {
    expected: OutboxStatusObservationV1,
    updated: StoredOutboxStatusV1,
}

impl OutboxClaimV1 {
    /// Increments the attempt number and constructs the exact in-flight row.
    pub fn new(
        event_id: EventId,
        expected: OutboxStatusObservationV1,
        destination_id: OutboxDestinationIdV1,
        started_at: Timestamp,
        lease_deadline: Timestamp,
    ) -> Result<Self, StorageValueError> {
        expected.validate_event_id(event_id)?;
        if !expected.is_pending() {
            return Err(StorageValueError::InvalidShape);
        }
        let attempt = expected
            .attempts()
            .checked_add(1)
            .and_then(NonZeroU32::new)
            .ok_or(StorageValueError::SizeOverflow)?;
        let updated = StoredOutboxStatusV1::delivering(
            event_id,
            attempt,
            destination_id,
            started_at,
            lease_deadline,
        );
        Ok(Self { expected, updated })
    }
}

/// Checked exact delivering-to-delivering lease renewal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutboxRenewV1 {
    expected: StoredOutboxStatusV1,
    updated: StoredOutboxStatusV1,
}

impl OutboxRenewV1 {
    /// Replaces only the worker-selected lease deadline.
    pub fn new(
        expected: StoredOutboxStatusV1,
        lease_deadline: Timestamp,
    ) -> Result<Self, StorageValueError> {
        let OutboxDeliveryStateV1::Delivering {
            attempt,
            destination_id,
            started_at,
            ..
        } = expected.state()
        else {
            return Err(StorageValueError::InvalidShape);
        };
        let updated = StoredOutboxStatusV1::delivering(
            expected.event_id,
            *attempt,
            destination_id.clone(),
            *started_at,
            lease_deadline,
        );
        Ok(Self { expected, updated })
    }
}

/// Checked delivering-to-delivered transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutboxSucceedV1 {
    expected: StoredOutboxStatusV1,
    updated: StoredOutboxStatusV1,
}

impl OutboxSucceedV1 {
    /// Preserves the attempt and destination in the terminal success row.
    pub fn new(
        expected: StoredOutboxStatusV1,
        delivered_at: Timestamp,
    ) -> Result<Self, StorageValueError> {
        let OutboxDeliveryStateV1::Delivering {
            attempt,
            destination_id,
            ..
        } = expected.state()
        else {
            return Err(StorageValueError::InvalidShape);
        };
        let updated = StoredOutboxStatusV1::delivered(
            expected.event_id,
            *attempt,
            destination_id.clone(),
            delivered_at,
        );
        Ok(Self { expected, updated })
    }
}

/// Checked delivering-to-explicit-pending retry transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutboxRetryV1 {
    expected: StoredOutboxStatusV1,
    updated: StoredOutboxStatusV1,
}

impl OutboxRetryV1 {
    /// Preserves attempt identity and worker-selected bounded retry metadata.
    pub fn new(
        expected: StoredOutboxStatusV1,
        next_attempt_at: Option<Timestamp>,
        last_safe_error: Option<OutboxSafeErrorV1>,
    ) -> Result<Self, StorageValueError> {
        let OutboxDeliveryStateV1::Delivering {
            attempt,
            destination_id,
            started_at,
            ..
        } = expected.state()
        else {
            return Err(StorageValueError::InvalidShape);
        };
        let metadata = OutboxRetryMetadataV1::new(
            *attempt,
            *started_at,
            next_attempt_at,
            destination_id.clone(),
            last_safe_error,
        );
        let updated = StoredOutboxStatusV1::pending(expected.event_id, metadata);
        Ok(Self { expected, updated })
    }
}

/// Checked pending-or-delivering to dead-letter transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutboxDeadLetterV1 {
    expected: OutboxStatusObservationV1,
    updated: StoredOutboxStatusV1,
}

impl OutboxDeadLetterV1 {
    /// Constructs a policy-selected terminal failure without choosing policy.
    pub fn new(
        event_id: EventId,
        expected: OutboxStatusObservationV1,
        initial_destination: Option<OutboxDestinationIdV1>,
        failed_at: Timestamp,
        last_safe_error: Option<OutboxSafeErrorV1>,
    ) -> Result<Self, StorageValueError> {
        expected.validate_event_id(event_id)?;
        let expected_is_absent =
            matches!(expected, OutboxStatusObservationV1::AbsentInitialPending);
        if expected_is_absent != initial_destination.is_some() {
            return Err(StorageValueError::InvalidShape);
        }
        let destination_id = match &expected {
            OutboxStatusObservationV1::AbsentInitialPending => {
                initial_destination.ok_or(StorageValueError::InvalidShape)?
            }
            OutboxStatusObservationV1::Present(status) => match status.state() {
                OutboxDeliveryStateV1::Pending(metadata) => metadata.destination_id.clone(),
                OutboxDeliveryStateV1::Delivering { destination_id, .. } => destination_id.clone(),
                OutboxDeliveryStateV1::Delivered { .. }
                | OutboxDeliveryStateV1::DeadLetter { .. } => {
                    return Err(StorageValueError::InvalidShape);
                }
            },
        };
        let updated = StoredOutboxStatusV1::dead_letter(
            event_id,
            expected.attempts(),
            destination_id,
            failed_at,
            last_safe_error,
        );
        Ok(Self { expected, updated })
    }
}

macro_rules! transition_accessors {
    ($type:ty, $expected:ty) => {
        impl $type {
            /// Returns the stable event identity.
            #[must_use]
            pub const fn event_id(&self) -> EventId {
                self.updated.event_id()
            }

            /// Borrows the exact required prior observation.
            #[must_use]
            pub const fn expected(&self) -> &$expected {
                &self.expected
            }

            /// Borrows the complete requested post-image.
            #[must_use]
            pub const fn updated(&self) -> &StoredOutboxStatusV1 {
                &self.updated
            }
        }
    };
}

transition_accessors!(OutboxClaimV1, OutboxStatusObservationV1);
transition_accessors!(OutboxRenewV1, StoredOutboxStatusV1);
transition_accessors!(OutboxSucceedV1, StoredOutboxStatusV1);
transition_accessors!(OutboxRetryV1, StoredOutboxStatusV1);
transition_accessors!(OutboxDeadLetterV1, OutboxStatusObservationV1);

/// Closed result shared by exact compare-and-transition operations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OutboxTransitionResultV1 {
    /// The requested complete status post-image committed.
    Applied(StoredOutboxStatusV1),
    /// The exact expected absent-or-present status no longer matched.
    StateChanged(OutboxStatusObservationV1),
    /// No reciprocal authoritative event/intent exists; no status was written.
    AuthoritativeIntentMissing,
}

/// Closed source-validated status lookup result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OutboxStatusReadResultV1 {
    /// A reciprocal authoritative event/intent exists with this effective status.
    Status(OutboxStatusObservationV1),
    /// No reciprocal authoritative event/intent exists for the supplied ID.
    AuthoritativeIntentMissing,
}

/// Specialized synchronous derived delivery-state repository.
pub trait OutboxRepository {
    /// Reads exact status only after proving the authoritative intent exists.
    fn read_outbox_status(
        &self,
        event_id: EventId,
    ) -> Result<OutboxStatusReadResultV1, StorageError>;

    /// Scans reciprocal authoritative event/intent tuples whose effective state
    /// is pending. Implementations must never synthesize a missing intent.
    fn scan_pending_outbox(
        &self,
        after: Option<EventId>,
        limit: OutboxPageLimit,
    ) -> Result<PendingOutboxScanV1, StorageError>;

    /// Scans every reciprocal authoritative tuple whose effective state has not
    /// reached `Delivered`, without exposing event or intent payloads.
    ///
    /// Implementations use strict increasing event order, an exclusive lower
    /// continuation, full source-record byte charging, and exact-end semantics.
    fn scan_undelivered_outbox_statuses(
        &self,
        request: UndeliveredOutboxStatusScanRequestV1,
    ) -> Result<UndeliveredOutboxStatusScanV1, StorageError>;

    /// Atomically claims the exact absent or explicit pending observation.
    fn claim_outbox(
        &mut self,
        transition: &OutboxClaimV1,
    ) -> Result<OutboxTransitionResultV1, StorageError>;

    /// Atomically renews the exact current in-flight attempt.
    fn renew_outbox(
        &mut self,
        transition: &OutboxRenewV1,
    ) -> Result<OutboxTransitionResultV1, StorageError>;

    /// Atomically marks the exact in-flight attempt delivered.
    fn succeed_outbox(
        &mut self,
        transition: &OutboxSucceedV1,
    ) -> Result<OutboxTransitionResultV1, StorageError>;

    /// Atomically returns the exact in-flight attempt to explicit pending.
    fn retry_outbox(
        &mut self,
        transition: &OutboxRetryV1,
    ) -> Result<OutboxTransitionResultV1, StorageError>;

    /// Atomically applies the worker-selected terminal failure transition.
    fn dead_letter_outbox(
        &mut self,
        transition: &OutboxDeadLetterV1,
    ) -> Result<OutboxTransitionResultV1, StorageError>;
}

#[cfg(test)]
fn bounded_text_semantic_bytes(value: &str) -> Result<usize, StorageValueError> {
    bounded_bytes_semantic_bytes(value.len())
}

#[cfg(test)]
fn bounded_bytes_semantic_bytes(value: usize) -> Result<usize, StorageValueError> {
    4usize
        .checked_add(value)
        .ok_or(StorageValueError::SizeOverflow)
}

#[cfg(test)]
fn optional_text_semantic_bytes(
    value: Option<&OutboxSafeErrorV1>,
) -> Result<usize, StorageValueError> {
    1usize
        .checked_add(match value {
            Some(value) => bounded_text_semantic_bytes(value.as_str())?,
            None => 0,
        })
        .ok_or(StorageValueError::SizeOverflow)
}

#[cfg(test)]
fn checked_outbox_sum(parts: impl IntoIterator<Item = usize>) -> Result<usize, StorageValueError> {
    parts.into_iter().try_fold(0usize, |total, part| {
        total
            .checked_add(part)
            .ok_or(StorageValueError::SizeOverflow)
    })
}

#[cfg(test)]
mod tests {
    use riffdb_types::{CanonicalRecord, CommitSequence, EventTypeId};

    use super::*;

    fn event_id() -> EventId {
        EventId::new(CommitSequence::first(), 0)
    }

    fn timestamp(seconds: i64) -> Timestamp {
        Timestamp::new(seconds, 0).expect("canonical timestamp")
    }

    fn destination() -> OutboxDestinationIdV1 {
        OutboxDestinationIdV1::new("stdout/default").expect("destination")
    }

    #[test]
    fn absent_status_is_exactly_zero_attempt_pending() {
        let status = OutboxStatusObservationV1::AbsentInitialPending;

        assert!(status.is_pending());
        assert_eq!(status.attempts(), 0);
    }

    #[test]
    fn pending_scan_limit_is_bounded_before_storage() {
        assert!(OutboxPageLimit::new(NonZeroU16::new(500).expect("nonzero")).is_ok());
        assert_eq!(
            OutboxPageLimit::new(NonZeroU16::new(501).expect("nonzero")),
            Err(StorageValueError::LimitExceeded)
        );
    }

    #[test]
    fn undelivered_status_rejects_delivered_and_scan_freezes_exact_order() {
        let delivered = StoredOutboxStatusV1::delivered(
            event_id(),
            NonZeroU32::MIN,
            destination(),
            timestamp(1),
        );
        assert_eq!(
            UndeliveredOutboxStatusV1::new(
                event_id(),
                OutboxStatusObservationV1::Present(delivered),
            ),
            Err(StorageValueError::InvalidShape)
        );

        let second_id = EventId::new(CommitSequence::first(), 1);
        let charge = crate::EncodedContentCharge::new(1).expect("nonzero charge");
        let first = EncodedPageItem::new(
            UndeliveredOutboxStatusV1::new(
                event_id(),
                OutboxStatusObservationV1::AbsentInitialPending,
            )
            .expect("initial pending"),
            charge,
        );
        let second = EncodedPageItem::new(
            UndeliveredOutboxStatusV1::new(
                second_id,
                OutboxStatusObservationV1::AbsentInitialPending,
            )
            .expect("initial pending"),
            charge,
        );
        let limit = OutboxPageLimit::new(NonZeroU16::new(2).expect("nonzero")).expect("limit");
        let upper = EventId::new(CommitSequence::new(2).expect("second commit"), 0);
        let request = UndeliveredOutboxStatusScanRequestV1::initial(None, limit);
        let page = UndeliveredOutboxStatusScanV1::page(
            request,
            upper,
            vec![first.clone(), second.clone()],
        )
        .expect("ordered page");
        let continuation = page.continuation().expect("continued page");
        assert_eq!(continuation.after(), second_id);
        assert_eq!(continuation.inclusive_upper(), upper);
        assert_eq!(
            UndeliveredOutboxStatusScanV1::exact_end(
                request,
                OutboxRecoveryUpperFenceV1::Inclusive(upper),
                vec![second, first],
            ),
            Err(StorageValueError::NonCanonicalOrder)
        );
        assert!(matches!(
            UndeliveredOutboxStatusScanV1::exact_end(
                request,
                OutboxRecoveryUpperFenceV1::BeforeFirst,
                Vec::new(),
            )
            .expect("empty exact end"),
            UndeliveredOutboxStatusScanV1::ExactEnd {
                inclusive_upper: OutboxRecoveryUpperFenceV1::BeforeFirst,
                ..
            }
        ));
    }

    #[test]
    fn pending_status_accounting_includes_maximum_bounded_text() {
        let status = OutboxStatusObservationV1::Present(StoredOutboxStatusV1::pending(
            event_id(),
            OutboxRetryMetadataV1::new(
                NonZeroU32::MIN,
                timestamp(1),
                Some(timestamp(2)),
                OutboxDestinationIdV1::new("d".repeat(MAX_OUTBOX_DESTINATION_ID_BYTES))
                    .expect("maximum destination"),
                Some(
                    OutboxSafeErrorV1::new("e".repeat(MAX_OUTBOX_SAFE_ERROR_BYTES))
                        .expect("maximum safe error"),
                ),
            ),
        ));

        assert!(
            status.semantic_bytes().expect("status size")
                > MAX_OUTBOX_DESTINATION_ID_BYTES + MAX_OUTBOX_SAFE_ERROR_BYTES
        );
    }

    #[test]
    fn pending_item_counts_both_complete_authoritative_event_copies() {
        let payload = CanonicalRecord::new(Vec::new()).expect("record");
        let payload_bytes = canonical_record_bytes(&payload).expect("payload size");
        let event_type_id = EventTypeId::try_from(1).expect("event type");
        let event_hash = crate::derive_event_hash_v1(event_id(), event_type_id, &payload)
            .expect("canonical event hash");
        let event = StoredDurableEventV1::new(event_id(), event_type_id, payload, event_hash)
            .expect("event");
        let intent = StoredOutboxIntentV1::new(event.clone());
        let item = PendingOutboxItemV1::new(
            event,
            intent,
            OutboxStatusObservationV1::AbsentInitialPending,
        )
        .expect("pending item");

        assert_eq!(item.semantic_bytes(), Ok(2 * (payload_bytes + 52) + 1));
    }

    #[test]
    fn retry_preserves_attempt_and_destination() {
        let claim = OutboxClaimV1::new(
            event_id(),
            OutboxStatusObservationV1::AbsentInitialPending,
            destination(),
            timestamp(10),
            timestamp(20),
        )
        .expect("valid claim");
        let retry = OutboxRetryV1::new(
            claim.updated().clone(),
            Some(timestamp(30)),
            Some(OutboxSafeErrorV1::new("temporary").expect("safe error")),
        )
        .expect("valid retry");

        let OutboxDeliveryStateV1::Pending(metadata) = retry.updated().state() else {
            panic!("retry must be pending");
        };
        assert_eq!(metadata.attempts().get(), 1);
        assert_eq!(metadata.destination_id(), &destination());
        assert_eq!(metadata.next_attempt_at(), Some(timestamp(30)));
    }

    #[test]
    fn delivered_cannot_be_claimed_again() {
        let delivered = StoredOutboxStatusV1::delivered(
            event_id(),
            NonZeroU32::MIN,
            destination(),
            timestamp(10),
        );

        assert_eq!(
            OutboxClaimV1::new(
                event_id(),
                OutboxStatusObservationV1::Present(delivered),
                destination(),
                timestamp(11),
                timestamp(12),
            ),
            Err(StorageValueError::InvalidShape)
        );
    }
}
