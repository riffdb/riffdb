//! Bounded undelivered-status scan and payload-free administration view.

use std::error::Error;
use std::fmt;

use riffdb_storage_api::{
    OutboxDeliveryStateV1, OutboxPageLimit, OutboxRepository, OutboxStatusObservationV1,
    StorageError, StoredOutboxStatusV1, UndeliveredOutboxStatusScanRequestV1,
    UndeliveredOutboxStatusScanV1, UndeliveredOutboxStatusV1,
};
use riffdb_types::{EventId, Timestamp};

/// Marker for repositories that expose the complete storage-owned worker API.
///
/// Storage owns the recovery scan values, page byte accounting, reciprocity
/// proof, continuation, and exact-end semantics. This trait adds no second
/// semantic interface.
pub trait OutboxWorkerRepository: OutboxRepository {}

impl<T> OutboxWorkerRepository for T where T: OutboxRepository + ?Sized {}

/// Payload-free lifecycle exposed to a later mechanical service adapter.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum OutboxStatusState {
    /// No attempt has begun, or an immediate retry is eligible.
    Pending,
    /// A prior attempt has an explicit future retry time.
    RetryScheduled,
    /// One attempt was durably claimed and has not reached terminal status.
    Delivering,
    /// Worker policy selected terminal delivery failure.
    DeadLetter,
}

/// One bounded administration-safe outbox status summary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OutboxStatusSummary {
    event_id: EventId,
    state: OutboxStatusState,
    attempts: u32,
    next_attempt_at: Option<Timestamp>,
}

impl OutboxStatusSummary {
    fn from_status(status: &UndeliveredOutboxStatusV1) -> Self {
        match status.status() {
            OutboxStatusObservationV1::AbsentInitialPending => Self {
                event_id: status.event_id(),
                state: OutboxStatusState::Pending,
                attempts: 0,
                next_attempt_at: None,
            },
            OutboxStatusObservationV1::Present(stored) => match stored.state() {
                OutboxDeliveryStateV1::Pending(metadata) => Self {
                    event_id: status.event_id(),
                    state: if metadata.next_attempt_at().is_some() {
                        OutboxStatusState::RetryScheduled
                    } else {
                        OutboxStatusState::Pending
                    },
                    attempts: metadata.attempts().get(),
                    next_attempt_at: metadata.next_attempt_at(),
                },
                OutboxDeliveryStateV1::Delivering { attempt, .. } => Self {
                    event_id: status.event_id(),
                    state: OutboxStatusState::Delivering,
                    attempts: attempt.get(),
                    next_attempt_at: None,
                },
                OutboxDeliveryStateV1::DeadLetter { attempts, .. } => Self {
                    event_id: status.event_id(),
                    state: OutboxStatusState::DeadLetter,
                    attempts: *attempts,
                    next_attempt_at: None,
                },
                OutboxDeliveryStateV1::Delivered { .. } => {
                    unreachable!("constructor rejects delivered status")
                }
            },
        }
    }

    /// Returns the stable event identity.
    #[must_use]
    pub const fn event_id(self) -> EventId {
        self.event_id
    }

    /// Returns the payload-free lifecycle.
    #[must_use]
    pub const fn state(self) -> OutboxStatusState {
        self.state
    }

    /// Returns delivery attempts already begun.
    #[must_use]
    pub const fn attempts(self) -> u32 {
        self.attempts
    }

    /// Returns the explicit retry instant, when scheduled.
    #[must_use]
    pub const fn next_attempt_at(self) -> Option<Timestamp> {
        self.next_attempt_at
    }
}

/// One payload-free status page request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OutboxStatusPageRequest {
    after: Option<EventId>,
    limit: OutboxPageLimit,
}

impl OutboxStatusPageRequest {
    /// Creates an exact bounded request.
    #[must_use]
    pub const fn new(after: Option<EventId>, limit: OutboxPageLimit) -> Self {
        Self { after, limit }
    }

    /// Returns the exclusive lower event bound.
    #[must_use]
    pub const fn after(self) -> Option<EventId> {
        self.after
    }

    /// Returns the checked row limit.
    #[must_use]
    pub const fn limit(self) -> OutboxPageLimit {
        self.limit
    }
}

/// One checked payload-free page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutboxStatusPage {
    items: Vec<OutboxStatusSummary>,
    next_after: Option<EventId>,
}

impl OutboxStatusPage {
    fn from_scan(scan: UndeliveredOutboxStatusScanV1) -> Self {
        let next_after = scan.continuation().map(|continuation| continuation.after());
        let items = scan
            .items()
            .iter()
            .map(|item| OutboxStatusSummary::from_status(item.value()))
            .collect();
        Self { items, next_after }
    }

    /// Borrows strictly ordered summaries.
    #[must_use]
    pub fn items(&self) -> &[OutboxStatusSummary] {
        &self.items
    }

    /// Returns the exclusive continuation only when more rows exist.
    #[must_use]
    pub const fn next_after(&self) -> Option<EventId> {
        self.next_after
    }
}

/// Closed failure from the payload-free source.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OutboxStatusSourceError {
    /// The specialized storage scan failed.
    Storage(StorageError),
}

impl fmt::Display for OutboxStatusSourceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Storage(error) => write!(formatter, "{error}"),
        }
    }
}

impl Error for OutboxStatusSourceError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Storage(error) => Some(error),
        }
    }
}

/// Consumer-owned payload-free status source.
///
/// WP-185 may mechanically map this view into service DTOs. It must not expose
/// the repository, connector configuration, safe-error text, or event payload.
pub trait OutboxStatusSource {
    /// Reads one bounded source-validated page.
    fn read_outbox_status_page(
        &self,
        request: OutboxStatusPageRequest,
    ) -> Result<OutboxStatusPage, OutboxStatusSourceError>;
}

impl<T> OutboxStatusSource for T
where
    T: OutboxWorkerRepository,
{
    fn read_outbox_status_page(
        &self,
        request: OutboxStatusPageRequest,
    ) -> Result<OutboxStatusPage, OutboxStatusSourceError> {
        self.scan_undelivered_outbox_statuses(UndeliveredOutboxStatusScanRequestV1::initial(
            request.after(),
            request.limit(),
        ))
        .map(OutboxStatusPage::from_scan)
        .map_err(OutboxStatusSourceError::Storage)
    }
}

/// Extracts a cloned in-flight status suitable for exact recovery CAS.
pub(crate) fn delivering_status(
    status: &UndeliveredOutboxStatusV1,
) -> Option<StoredOutboxStatusV1> {
    let OutboxStatusObservationV1::Present(stored) = status.status() else {
        return None;
    };
    matches!(stored.state(), OutboxDeliveryStateV1::Delivering { .. }).then(|| stored.clone())
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use riffdb_storage_api::{
        OutboxDestinationIdV1, OutboxRetryMetadataV1, StoredOutboxStatusV1,
        UndeliveredOutboxStatusV1,
    };
    use riffdb_types::CommitSequence;

    use super::*;

    fn event_id(ordinal: u32) -> EventId {
        EventId::new(CommitSequence::first(), ordinal)
    }

    fn timestamp(seconds: i64) -> Timestamp {
        Timestamp::new(seconds, 0).expect("timestamp")
    }

    fn destination() -> OutboxDestinationIdV1 {
        OutboxDestinationIdV1::new("test").expect("destination")
    }

    #[test]
    fn payload_free_mapping_preserves_retry_metadata_without_sensitive_fields() {
        let status = StoredOutboxStatusV1::pending(
            event_id(0),
            OutboxRetryMetadataV1::new(
                NonZeroU32::new(2).expect("attempt"),
                timestamp(10),
                Some(timestamp(20)),
                destination(),
                Some(
                    riffdb_storage_api::OutboxSafeErrorV1::new("secret-canary")
                        .expect("safe error"),
                ),
            ),
        );
        let item =
            UndeliveredOutboxStatusV1::new(event_id(0), OutboxStatusObservationV1::Present(status))
                .expect("status");
        let summary = OutboxStatusSummary::from_status(&item);

        assert_eq!(summary.state(), OutboxStatusState::RetryScheduled);
        assert_eq!(summary.attempts(), 2);
        assert_eq!(summary.next_attempt_at(), Some(timestamp(20)));
        assert!(!format!("{summary:?}").contains("secret-canary"));
    }
}
