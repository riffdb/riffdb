//! Narrow authoritative point reads and bounded ordered scans.

use std::fmt;
use std::num::NonZeroU16;

use riffdb_types::{CommitSequence, EventId, FrontierPosition, IndexEntryKey, ProvenanceId};

use crate::{
    EncodedPageItem, EntityTarget, IdempotencyIdentity, IndexEpochPosition, IndexRangeEntry,
    IndexRangeTarget, MAX_COMMIT_SCAN_PAGE_BYTES, MAX_SCAN_PAGE_BYTES, MAX_SCAN_PAGE_ENTRIES,
    StorageError, StorageValueError, StoredCommitRecordV1, StoredDurableEventV1,
    StoredEntityRecordV1, StoredOutcomeV1, StoredProvenanceRecordV1, checked_encoded_page_content,
};

/// A checked nonzero storage scan limit in `1..=500`.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct StorageScanLimit(NonZeroU16);

impl StorageScanLimit {
    /// Checks the fixed v1 row ceiling.
    #[must_use]
    pub const fn new(value: u16) -> Option<Self> {
        if value == 0 || value as usize > MAX_SCAN_PAGE_ENTRIES {
            return None;
        }
        match NonZeroU16::new(value) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    /// Returns the checked requested row count.
    #[must_use]
    pub const fn get(self) -> u16 {
        self.0.get()
    }
}

/// One bounded exact-prefix index scan after an optional complete key.
#[derive(Clone, Eq, PartialEq)]
pub struct AuthoritativeIndexScanRequest {
    target: IndexRangeTarget,
    after: Option<IndexEntryKey>,
    limit: StorageScanLimit,
}

impl AuthoritativeIndexScanRequest {
    /// Checks that an optional continuation belongs to and follows the target prefix.
    pub fn new(
        target: IndexRangeTarget,
        after: Option<IndexEntryKey>,
        limit: StorageScanLimit,
    ) -> Result<Self, StorageValueError> {
        if after.as_ref().is_some_and(|key| {
            key.index_id() != target.prefix().index_id()
                || !key.as_bytes().starts_with(target.prefix().as_bytes())
        }) {
            return Err(StorageValueError::IdentityMismatch);
        }
        Ok(Self {
            target,
            after,
            limit,
        })
    }

    /// Borrows the exact complete-component prefix target.
    #[must_use]
    pub const fn target(&self) -> &IndexRangeTarget {
        &self.target
    }

    /// Borrows the exclusive complete-key continuation.
    #[must_use]
    pub const fn after(&self) -> Option<&IndexEntryKey> {
        self.after.as_ref()
    }

    /// Returns the checked requested row count.
    #[must_use]
    pub const fn limit(&self) -> StorageScanLimit {
        self.limit
    }
}

/// Closed result for one authoritative index scan transaction.
#[derive(Clone, Eq, PartialEq)]
pub enum AuthoritativeIndexScanPage {
    /// A non-final nonempty page with an exclusive continuation.
    Page {
        /// Canonical rows in complete-key order.
        entries: Vec<EncodedPageItem<IndexRangeEntry>>,
        /// Exact last key returned on this page.
        next_after: IndexEntryKey,
        /// Exact range epoch observed in the same read view as the rows.
        epoch: IndexEpochPosition,
    },
    /// The exact range end, possibly with a final nonempty page.
    ExactEnd {
        /// Canonical final rows, or empty when no rows remain.
        entries: Vec<EncodedPageItem<IndexRangeEntry>>,
        /// Exact range epoch observed in the same read view as the rows.
        epoch: IndexEpochPosition,
    },
}

impl AuthoritativeIndexScanPage {
    /// Checks one non-final page against its exact request.
    pub fn page(
        request: &AuthoritativeIndexScanRequest,
        epoch: IndexEpochPosition,
        entries: Vec<EncodedPageItem<IndexRangeEntry>>,
        next_after: IndexEntryKey,
    ) -> Result<Self, StorageValueError> {
        validate_index_scan_entries(request, &entries)?;
        if entries.is_empty()
            || entries
                .last()
                .map(EncodedPageItem::value)
                .map(IndexRangeEntry::key)
                != Some(&next_after)
        {
            return Err(StorageValueError::InvalidShape);
        }
        Ok(Self::Page {
            entries,
            next_after,
            epoch,
        })
    }

    /// Checks a final page, including an empty exact-end result.
    pub fn exact_end(
        request: &AuthoritativeIndexScanRequest,
        epoch: IndexEpochPosition,
        entries: Vec<EncodedPageItem<IndexRangeEntry>>,
    ) -> Result<Self, StorageValueError> {
        validate_index_scan_entries(request, &entries)?;
        Ok(Self::ExactEnd { entries, epoch })
    }

    /// Borrows rows in canonical complete-key order.
    #[must_use]
    pub fn entries(&self) -> &[EncodedPageItem<IndexRangeEntry>] {
        match self {
            Self::Page { entries, .. } | Self::ExactEnd { entries, .. } => entries,
        }
    }

    /// Returns the exact range epoch observed atomically with this page.
    #[must_use]
    pub const fn epoch(&self) -> IndexEpochPosition {
        match self {
            Self::Page { epoch, .. } | Self::ExactEnd { epoch, .. } => *epoch,
        }
    }
}

/// One ordered application-commit scan that captures or reuses a frozen upper fence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommitScanRequest {
    /// First page; the storage read captures the current authoritative head.
    Initial {
        /// Bounded page limit.
        limit: StorageScanLimit,
    },
    /// Later page bound to the exact first-page authoritative head.
    Continue {
        /// Exclusive lower continuation.
        after: CommitSequence,
        /// Frozen inclusive upper sequence.
        inclusive_upper: CommitSequence,
        /// Bounded page limit.
        limit: StorageScanLimit,
    },
}

impl CommitScanRequest {
    /// Constructs a first-page request that atomically captures the current head.
    #[must_use]
    pub const fn initial(limit: StorageScanLimit) -> Self {
        Self::Initial { limit }
    }

    /// Constructs a continuation bound to the exact first-page upper sequence.
    pub fn continuing(
        after: CommitSequence,
        inclusive_upper: CommitSequence,
        limit: StorageScanLimit,
    ) -> Result<Self, StorageValueError> {
        if after > inclusive_upper {
            return Err(StorageValueError::InvalidShape);
        }
        Ok(Self::Continue {
            after,
            inclusive_upper,
            limit,
        })
    }

    /// Returns the exclusive lower sequence, or `None` for an initial scan.
    #[must_use]
    pub const fn after(self) -> Option<CommitSequence> {
        match self {
            Self::Initial { .. } => None,
            Self::Continue { after, .. } => Some(after),
        }
    }

    /// Returns the frozen upper sequence, or `None` when storage must capture it.
    #[must_use]
    pub const fn inclusive_upper(self) -> Option<CommitSequence> {
        match self {
            Self::Initial { .. } => None,
            Self::Continue {
                inclusive_upper, ..
            } => Some(inclusive_upper),
        }
    }

    /// Returns the checked requested row count.
    #[must_use]
    pub const fn limit(self) -> StorageScanLimit {
        match self {
            Self::Initial { limit } | Self::Continue { limit, .. } => limit,
        }
    }
}

/// Closed result for one ordered commit-log scan.
#[derive(Clone, Eq, PartialEq)]
pub enum CommitScanPageV1 {
    /// A non-final nonempty contiguous page.
    Page {
        /// Complete commit records in strictly contiguous sequence order.
        records: Vec<EncodedPageItem<StoredCommitRecordV1>>,
        /// Exact last sequence returned on this page.
        next_after: CommitSequence,
        /// Frozen inclusive upper frontier captured by the initial read.
        inclusive_upper: FrontierPosition,
    },
    /// Exact frozen log end, possibly with a final nonempty page.
    ExactEnd {
        /// Complete final records, or empty when no commits remain.
        records: Vec<EncodedPageItem<StoredCommitRecordV1>>,
        /// Frozen inclusive upper frontier captured by the initial read.
        inclusive_upper: FrontierPosition,
    },
}

impl CommitScanPageV1 {
    /// Checks a non-final page and its exclusive continuation.
    pub fn page(
        request: CommitScanRequest,
        inclusive_upper: FrontierPosition,
        records: Vec<EncodedPageItem<StoredCommitRecordV1>>,
        next_after: CommitSequence,
    ) -> Result<Self, StorageValueError> {
        validate_commit_scan_records(request, inclusive_upper, &records)?;
        if records.is_empty()
            || records
                .last()
                .map(EncodedPageItem::value)
                .map(StoredCommitRecordV1::commit_sequence)
                != Some(next_after)
            || !position_requires_continuation(inclusive_upper, next_after)
        {
            return Err(StorageValueError::InvalidShape);
        }
        Ok(Self::Page {
            records,
            next_after,
            inclusive_upper,
        })
    }

    /// Checks a final page, including an empty exact-end result.
    pub fn exact_end(
        request: CommitScanRequest,
        inclusive_upper: FrontierPosition,
        records: Vec<EncodedPageItem<StoredCommitRecordV1>>,
    ) -> Result<Self, StorageValueError> {
        validate_commit_scan_records(request, inclusive_upper, &records)?;
        let final_position = records
            .last()
            .map(EncodedPageItem::value)
            .map(StoredCommitRecordV1::commit_sequence)
            .or_else(|| request.after());
        if !position_reaches_fence(inclusive_upper, final_position) {
            return Err(StorageValueError::InvalidShape);
        }
        Ok(Self::ExactEnd {
            records,
            inclusive_upper,
        })
    }

    /// Borrows complete commit records in sequence order.
    #[must_use]
    pub fn records(&self) -> &[EncodedPageItem<StoredCommitRecordV1>] {
        match self {
            Self::Page { records, .. } | Self::ExactEnd { records, .. } => records,
        }
    }

    /// Returns the inclusive upper frontier frozen by the initial scan.
    #[must_use]
    pub const fn inclusive_upper(&self) -> FrontierPosition {
        match self {
            Self::Page {
                inclusive_upper, ..
            }
            | Self::ExactEnd {
                inclusive_upper, ..
            } => *inclusive_upper,
        }
    }
}

/// Narrow synchronous point reads over authoritative state.
pub trait AuthoritativePointReader {
    /// Gets one complete entity; absence is ordinary data.
    fn read_entity(
        &self,
        target: &EntityTarget,
    ) -> Result<Option<StoredEntityRecordV1>, StorageError>;

    /// Gets one terminal equal-input outcome; absence is ordinary data.
    fn read_stored_outcome(
        &self,
        identity: &IdempotencyIdentity,
    ) -> Result<Option<StoredOutcomeV1>, StorageError>;

    /// Gets one complete commit record; absence is ordinary data.
    fn read_commit(
        &self,
        sequence: CommitSequence,
    ) -> Result<Option<StoredCommitRecordV1>, StorageError>;

    /// Gets one immutable provenance record; absence is ordinary data.
    fn read_provenance(
        &self,
        provenance_id: ProvenanceId,
    ) -> Result<Option<StoredProvenanceRecordV1>, StorageError>;

    /// Gets one authoritative durable event; absence is ordinary data.
    fn read_durable_event(
        &self,
        event_id: EventId,
    ) -> Result<Option<StoredDurableEventV1>, StorageError>;
}

/// Narrow synchronous bounded scans over authoritative state.
pub trait AuthoritativeScanReader {
    /// Reads one exact-prefix index page and its epoch from one consistent read transaction.
    fn scan_index(
        &self,
        request: AuthoritativeIndexScanRequest,
    ) -> Result<AuthoritativeIndexScanPage, StorageError>;

    /// Captures or reuses one upper fence and reads a contiguous commit page through that fence.
    fn scan_commits(&self, request: CommitScanRequest) -> Result<CommitScanPageV1, StorageError>;
}

fn validate_index_scan_entries(
    request: &AuthoritativeIndexScanRequest,
    entries: &[EncodedPageItem<IndexRangeEntry>],
) -> Result<(), StorageValueError> {
    if entries.len() > usize::from(request.limit.get()) {
        return Err(StorageValueError::LimitExceeded);
    }
    checked_encoded_page_content(entries, MAX_SCAN_PAGE_BYTES)?;
    let mut prior = request.after.as_ref().map(IndexEntryKey::as_bytes);
    for charged in entries {
        let entry = charged.value();
        if entry.key().index_id() != request.target.prefix().index_id()
            || !entry
                .key()
                .as_bytes()
                .starts_with(request.target.prefix().as_bytes())
            || prior.is_some_and(|value| value >= entry.key().as_bytes())
        {
            return Err(StorageValueError::NonCanonicalOrder);
        }
        prior = Some(entry.key().as_bytes());
    }
    Ok(())
}

fn validate_commit_scan_records(
    request: CommitScanRequest,
    inclusive_upper: FrontierPosition,
    records: &[EncodedPageItem<StoredCommitRecordV1>],
) -> Result<(), StorageValueError> {
    if records.len() > usize::from(request.limit().get()) {
        return Err(StorageValueError::LimitExceeded);
    }
    if request
        .inclusive_upper()
        .is_some_and(|expected| inclusive_upper != FrontierPosition::AppliedThrough(expected))
    {
        return Err(StorageValueError::InvalidShape);
    }
    let mut expected = match request.after() {
        None => Some(CommitSequence::first()),
        Some(sequence) => sequence.checked_next(),
    };
    checked_encoded_page_content(records, MAX_COMMIT_SCAN_PAGE_BYTES)?;
    for charged in records {
        let record = charged.value();
        if expected != Some(record.commit_sequence())
            || !sequence_at_or_before(record.commit_sequence(), inclusive_upper)
        {
            return Err(StorageValueError::NonCanonicalOrder);
        }
        expected = record.commit_sequence().checked_next();
    }
    Ok(())
}

fn sequence_at_or_before(sequence: CommitSequence, frontier: FrontierPosition) -> bool {
    match frontier {
        FrontierPosition::BeforeFirst => false,
        FrontierPosition::AppliedThrough(upper) => sequence <= upper,
    }
}

fn position_requires_continuation(frontier: FrontierPosition, position: CommitSequence) -> bool {
    match frontier {
        FrontierPosition::BeforeFirst => false,
        FrontierPosition::AppliedThrough(upper) => position < upper,
    }
}

fn position_reaches_fence(frontier: FrontierPosition, position: Option<CommitSequence>) -> bool {
    match (frontier, position) {
        (FrontierPosition::BeforeFirst, None) => true,
        (FrontierPosition::AppliedThrough(upper), Some(position)) => position == upper,
        (FrontierPosition::BeforeFirst | FrontierPosition::AppliedThrough(_), _) => false,
    }
}

macro_rules! redacted_debug {
    ($($type:ty),+ $(,)?) => {
        $(
            impl fmt::Debug for $type {
                fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                    formatter.write_str(concat!(stringify!($type), "([REDACTED])"))
                }
            }
        )+
    };
}

redacted_debug!(
    AuthoritativeIndexScanRequest,
    AuthoritativeIndexScanPage,
    CommitScanPageV1,
);
