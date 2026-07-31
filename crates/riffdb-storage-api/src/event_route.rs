//! Integrity-checked partition-local routing over authoritative durable events.

use std::num::NonZeroU16;

use riffdb_types::{EventHash, EventId, EventTypeId, PartitionKeyHash};

use crate::{
    EncodedPageItem, MAX_SCAN_PAGE_BYTES, MAX_SCAN_PAGE_ENTRIES, StorageError, StorageValueError,
    checked_encoded_page_content,
};

/// Checked nonzero event-route scan limit under the storage page ceiling.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct EventRoutePageLimit(NonZeroU16);

impl EventRoutePageLimit {
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

/// One payload-free route to an authoritative durable event.
///
/// Storage keys this value by `(PartitionKeyHash, EventId)`. The duplicated event
/// identity and immutable event hash make malformed or stale routing rows fail
/// closed before an event payload crosses the application boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StoredEventRouteV1 {
    event_id: EventId,
    event_type_id: EventTypeId,
    event_hash: EventHash,
}

impl StoredEventRouteV1 {
    /// Constructs the exact payload-free reference retained by the route index.
    #[must_use]
    pub const fn new(event_id: EventId, event_type_id: EventTypeId, event_hash: EventHash) -> Self {
        Self {
            event_id,
            event_type_id,
            event_hash,
        }
    }

    /// Returns the authoritative event identity.
    #[must_use]
    pub const fn event_id(self) -> EventId {
        self.event_id
    }

    /// Returns the stable contract event-type identity.
    #[must_use]
    pub const fn event_type_id(self) -> EventTypeId {
        self.event_type_id
    }

    /// Returns the immutable authoritative event hash.
    #[must_use]
    pub const fn event_hash(self) -> EventHash {
        self.event_hash
    }
}

/// Frozen upper fence for one complete partition event-route scan.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EventRouteUpperFenceV1 {
    /// No event route existed for the partition at the initial read.
    BeforeFirst,
    /// Greatest event identity in the partition at the initial read.
    Inclusive(EventId),
}

/// Opaque continuation for one frozen partition event-route scan.
///
/// Only a checked non-final storage result can construct this value. Callers
/// pass it back unchanged; they cannot switch partitions or recapture a newer
/// upper fence between pages.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EventRouteContinuationV1 {
    partition_hash: PartitionKeyHash,
    after: EventId,
    inclusive_upper: EventId,
}

impl EventRouteContinuationV1 {
    /// Returns the exact logical partition retained by the continuation.
    #[must_use]
    pub const fn partition_hash(self) -> PartitionKeyHash {
        self.partition_hash
    }

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

/// Checked request for one page of a frozen partition event-route scan.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EventRouteScanRequestV1 {
    /// First page; storage captures the partition's greatest event identity.
    Initial {
        /// Exact logical partition route.
        partition_hash: PartitionKeyHash,
        /// Optional exclusive lower event bound for replay.
        after: Option<EventId>,
        /// Bounded page limit.
        limit: EventRoutePageLimit,
    },
    /// Later page; the opaque continuation retains partition and upper fence.
    Continue {
        /// Continuation returned by the preceding non-final page.
        continuation: EventRouteContinuationV1,
        /// Bounded page limit.
        limit: EventRoutePageLimit,
    },
}

impl EventRouteScanRequestV1 {
    /// Constructs a first-page request.
    #[must_use]
    pub const fn initial(
        partition_hash: PartitionKeyHash,
        after: Option<EventId>,
        limit: EventRoutePageLimit,
    ) -> Self {
        Self::Initial {
            partition_hash,
            after,
            limit,
        }
    }

    /// Constructs a continuation request from the preceding checked page.
    #[must_use]
    pub const fn continuing(
        continuation: EventRouteContinuationV1,
        limit: EventRoutePageLimit,
    ) -> Self {
        Self::Continue {
            continuation,
            limit,
        }
    }

    /// Returns the exact logical partition route.
    #[must_use]
    pub const fn partition_hash(self) -> PartitionKeyHash {
        match self {
            Self::Initial { partition_hash, .. } => partition_hash,
            Self::Continue { continuation, .. } => continuation.partition_hash,
        }
    }

    /// Returns the exclusive lower event bound, when supplied.
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
    pub const fn limit(self) -> EventRoutePageLimit {
        match self {
            Self::Initial { limit, .. } | Self::Continue { limit, .. } => limit,
        }
    }
}

/// Exact-end bounded route scan in increasing partition-local `EventId` order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EventRouteScanV1 {
    /// A non-final page bound to the first read's partition and upper fence.
    Page {
        /// Payload-free routes in strict event order.
        items: Vec<EncodedPageItem<StoredEventRouteV1>>,
        /// Opaque exclusive continuation retaining partition and upper fence.
        continuation: EventRouteContinuationV1,
    },
    /// The frozen scan reached exact end, including an empty result.
    ExactEnd {
        /// Final payload-free routes in strict event order.
        items: Vec<EncodedPageItem<StoredEventRouteV1>>,
        /// Upper fence captured by the initial read.
        inclusive_upper: EventRouteUpperFenceV1,
    },
}

impl EventRouteScanV1 {
    /// Checks a non-final page and constructs its opaque continuation.
    pub fn page(
        request: EventRouteScanRequestV1,
        inclusive_upper: EventId,
        items: Vec<EncodedPageItem<StoredEventRouteV1>>,
    ) -> Result<Self, StorageValueError> {
        validate_event_route_scan_items(request, inclusive_upper, &items)?;
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
            continuation: EventRouteContinuationV1 {
                partition_hash: request.partition_hash(),
                after,
                inclusive_upper,
            },
        })
    }

    /// Checks a final page, including an empty before-first result.
    pub fn exact_end(
        request: EventRouteScanRequestV1,
        inclusive_upper: EventRouteUpperFenceV1,
        items: Vec<EncodedPageItem<StoredEventRouteV1>>,
    ) -> Result<Self, StorageValueError> {
        match inclusive_upper {
            EventRouteUpperFenceV1::BeforeFirst => {
                if !matches!(request, EventRouteScanRequestV1::Initial { .. }) || !items.is_empty()
                {
                    return Err(StorageValueError::InvalidShape);
                }
            }
            EventRouteUpperFenceV1::Inclusive(upper) => {
                validate_event_route_scan_items(request, upper, &items)?;
            }
        }
        Ok(Self::ExactEnd {
            items,
            inclusive_upper,
        })
    }

    /// Borrows the routes in this page.
    #[must_use]
    pub fn items(&self) -> &[EncodedPageItem<StoredEventRouteV1>] {
        match self {
            Self::Page { items, .. } | Self::ExactEnd { items, .. } => items,
        }
    }

    /// Returns the opaque continuation only when more routes remain.
    #[must_use]
    pub const fn continuation(&self) -> Option<EventRouteContinuationV1> {
        match self {
            Self::Page { continuation, .. } => Some(*continuation),
            Self::ExactEnd { .. } => None,
        }
    }

    /// Returns the frozen upper fence.
    #[must_use]
    pub const fn inclusive_upper(&self) -> EventRouteUpperFenceV1 {
        match self {
            Self::Page { continuation, .. } => {
                EventRouteUpperFenceV1::Inclusive(continuation.inclusive_upper)
            }
            Self::ExactEnd {
                inclusive_upper, ..
            } => *inclusive_upper,
        }
    }
}

fn validate_event_route_scan_items(
    request: EventRouteScanRequestV1,
    inclusive_upper: EventId,
    items: &[EncodedPageItem<StoredEventRouteV1>],
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

/// Narrow synchronous bounded reads over partition-local event routes.
pub trait PartitionEventRouteReader {
    /// Captures or reuses one partition upper fence and reads through that fence.
    fn scan_partition_event_routes(
        &self,
        request: EventRouteScanRequestV1,
    ) -> Result<EventRouteScanV1, StorageError>;
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU16;

    use riffdb_types::CommitSequence;

    use super::*;
    use crate::{EncodedContentCharge, MAX_SCAN_PAGE_BYTES};

    fn event_id(sequence: u64, ordinal: u32) -> EventId {
        EventId::new(CommitSequence::new(sequence).expect("sequence"), ordinal)
    }

    fn partition(byte: u8) -> PartitionKeyHash {
        PartitionKeyHash::from_bytes([byte; 32])
    }

    fn route(sequence: u64) -> EncodedPageItem<StoredEventRouteV1> {
        EncodedPageItem::new(
            StoredEventRouteV1::new(
                event_id(sequence, 0),
                EventTypeId::new(7).expect("event type"),
                EventHash::from_bytes([sequence as u8; 32]),
            ),
            EncodedContentCharge::new(80).expect("charge"),
        )
    }

    fn limit(value: u16) -> EventRoutePageLimit {
        EventRoutePageLimit::new(NonZeroU16::new(value).expect("nonzero")).expect("limit")
    }

    #[test]
    fn continuation_freezes_partition_and_upper_fence() {
        let first = EventRouteScanV1::page(
            EventRouteScanRequestV1::initial(partition(1), None, limit(2)),
            event_id(4, 0),
            vec![route(1), route(2)],
        )
        .expect("page");
        let continuation = first.continuation().expect("continuation");
        assert_eq!(continuation.partition_hash(), partition(1));
        assert_eq!(continuation.after(), event_id(2, 0));
        assert_eq!(continuation.inclusive_upper(), event_id(4, 0));

        let final_page = EventRouteScanV1::exact_end(
            EventRouteScanRequestV1::continuing(continuation, limit(2)),
            EventRouteUpperFenceV1::Inclusive(event_id(4, 0)),
            vec![route(3), route(4)],
        )
        .expect("exact end");
        assert!(final_page.continuation().is_none());
    }

    #[test]
    fn continuation_rejects_recaptured_or_out_of_order_pages() {
        let first = EventRouteScanV1::page(
            EventRouteScanRequestV1::initial(partition(1), None, limit(2)),
            event_id(4, 0),
            vec![route(1), route(2)],
        )
        .expect("page");
        let request = EventRouteScanRequestV1::continuing(
            first.continuation().expect("continuation"),
            limit(2),
        );

        assert_eq!(
            EventRouteScanV1::exact_end(
                request,
                EventRouteUpperFenceV1::Inclusive(event_id(5, 0)),
                vec![route(3)],
            ),
            Err(StorageValueError::IdentityMismatch)
        );
        assert_eq!(
            EventRouteScanV1::exact_end(
                request,
                EventRouteUpperFenceV1::Inclusive(event_id(4, 0)),
                vec![route(2)],
            ),
            Err(StorageValueError::InvalidShape)
        );
    }

    #[test]
    fn route_pages_enforce_count_and_encoded_byte_bounds() {
        let request = EventRouteScanRequestV1::initial(partition(1), None, limit(1));
        assert_eq!(
            EventRouteScanV1::exact_end(
                request,
                EventRouteUpperFenceV1::Inclusive(event_id(2, 0)),
                vec![route(1), route(2)],
            ),
            Err(StorageValueError::LimitExceeded)
        );

        let oversized = EncodedPageItem::new(
            *route(1).value(),
            EncodedContentCharge::new(MAX_SCAN_PAGE_BYTES + 1).expect("bounded envelope charge"),
        );
        assert_eq!(
            EventRouteScanV1::exact_end(
                EventRouteScanRequestV1::initial(partition(1), None, limit(1)),
                EventRouteUpperFenceV1::Inclusive(event_id(1, 0)),
                vec![oversized],
            ),
            Err(StorageValueError::LimitExceeded)
        );
    }

    #[test]
    fn before_first_is_only_valid_for_an_empty_initial_scan() {
        let initial = EventRouteScanRequestV1::initial(partition(1), None, limit(1));
        EventRouteScanV1::exact_end(initial, EventRouteUpperFenceV1::BeforeFirst, Vec::new())
            .expect("empty partition");

        assert_eq!(
            EventRouteScanV1::exact_end(
                initial,
                EventRouteUpperFenceV1::BeforeFirst,
                vec![route(1)],
            ),
            Err(StorageValueError::InvalidShape)
        );
    }
}
