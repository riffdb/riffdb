//! Narrow authoritative point reads and bounded ordered scans.

use std::fmt;
use std::num::NonZeroU16;

use riffdb_types::{CommitSequence, EventId, IndexEntryKey, ProvenanceId};

use crate::{
    EncodedPageItem, EntityTarget, IdempotencyIdentity, IndexRangeEntry, IndexRangeTarget,
    MAX_COMMIT_SCAN_PAGE_BYTES, MAX_SCAN_PAGE_BYTES, MAX_SCAN_PAGE_ENTRIES, StorageError,
    StorageValueError, StoredCommitRecordV1, StoredDurableEventV1, StoredEntityRecordV1,
    StoredOutcomeV1, StoredProvenanceRecordV1, checked_encoded_page_content,
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
    },
    /// The exact range end, possibly with a final nonempty page.
    ExactEnd {
        /// Canonical final rows, or empty when no rows remain.
        entries: Vec<EncodedPageItem<IndexRangeEntry>>,
    },
}

impl AuthoritativeIndexScanPage {
    /// Checks one non-final page against its exact request.
    pub fn page(
        request: &AuthoritativeIndexScanRequest,
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
        })
    }

    /// Checks a final page, including an empty exact-end result.
    pub fn exact_end(
        request: &AuthoritativeIndexScanRequest,
        entries: Vec<EncodedPageItem<IndexRangeEntry>>,
    ) -> Result<Self, StorageValueError> {
        validate_index_scan_entries(request, &entries)?;
        Ok(Self::ExactEnd { entries })
    }

    /// Borrows rows in canonical complete-key order.
    #[must_use]
    pub fn entries(&self) -> &[EncodedPageItem<IndexRangeEntry>] {
        match self {
            Self::Page { entries, .. } | Self::ExactEnd { entries } => entries,
        }
    }
}

/// One ordered application-commit scan after an optional sequence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CommitScanRequest {
    after: Option<CommitSequence>,
    limit: StorageScanLimit,
}

impl CommitScanRequest {
    /// Constructs an exclusive sequence scan request.
    #[must_use]
    pub const fn new(after: Option<CommitSequence>, limit: StorageScanLimit) -> Self {
        Self { after, limit }
    }

    /// Returns the exclusive prior sequence, or `None` for sequence one.
    #[must_use]
    pub const fn after(self) -> Option<CommitSequence> {
        self.after
    }

    /// Returns the checked requested row count.
    #[must_use]
    pub const fn limit(self) -> StorageScanLimit {
        self.limit
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
    },
    /// Exact current log end, possibly with a final nonempty page.
    ExactEnd {
        /// Complete final records, or empty when no commits remain.
        records: Vec<EncodedPageItem<StoredCommitRecordV1>>,
    },
}

impl CommitScanPageV1 {
    /// Checks a non-final page and its exclusive continuation.
    pub fn page(
        request: CommitScanRequest,
        records: Vec<EncodedPageItem<StoredCommitRecordV1>>,
        next_after: CommitSequence,
    ) -> Result<Self, StorageValueError> {
        validate_commit_scan_records(request, &records)?;
        if records.is_empty()
            || records
                .last()
                .map(EncodedPageItem::value)
                .map(StoredCommitRecordV1::commit_sequence)
                != Some(next_after)
        {
            return Err(StorageValueError::InvalidShape);
        }
        Ok(Self::Page {
            records,
            next_after,
        })
    }

    /// Checks a final page, including an empty exact-end result.
    pub fn exact_end(
        request: CommitScanRequest,
        records: Vec<EncodedPageItem<StoredCommitRecordV1>>,
    ) -> Result<Self, StorageValueError> {
        validate_commit_scan_records(request, &records)?;
        Ok(Self::ExactEnd { records })
    }

    /// Borrows complete commit records in sequence order.
    #[must_use]
    pub fn records(&self) -> &[EncodedPageItem<StoredCommitRecordV1>] {
        match self {
            Self::Page { records, .. } | Self::ExactEnd { records } => records,
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
    /// Reads one exact-prefix index page from one consistent read transaction.
    fn scan_index(
        &self,
        request: AuthoritativeIndexScanRequest,
    ) -> Result<AuthoritativeIndexScanPage, StorageError>;

    /// Reads one contiguous ordered commit page after an optional sequence.
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
    records: &[EncodedPageItem<StoredCommitRecordV1>],
) -> Result<(), StorageValueError> {
    if records.len() > usize::from(request.limit.get()) {
        return Err(StorageValueError::LimitExceeded);
    }
    let mut expected = match request.after {
        None => Some(CommitSequence::first()),
        Some(sequence) => sequence.checked_next(),
    };
    checked_encoded_page_content(records, MAX_COMMIT_SCAN_PAGE_BYTES)?;
    for charged in records {
        let record = charged.value();
        if expected != Some(record.commit_sequence()) {
            return Err(StorageValueError::NonCanonicalOrder);
        }
        expected = record.commit_sequence().checked_next();
    }
    Ok(())
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
