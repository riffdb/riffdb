//! One-view, engine-neutral handles for bounded application reads.
//!
//! The handle pins one backend read view. Its vocabulary is deliberately
//! limited to storage identities, bounded physical ranges, semantic records,
//! and authoritative fences. Query programs, policy contexts, executor values,
//! and public cursors never cross this boundary.

use riffdb_types::{CommitSequence, IndexEntryKey, MAX_KEY_BYTES};

use crate::{
    AuthoritativePointReader, AuthoritativeScanReader, EncodedPageItem,
    FilteredAuthoritativeScanReader, MAX_SCAN_PAGE_BYTES, PartitionIndexTarget, SnapshotReader,
    StorageError, StorageScanLimit, StorageValueError, StoredIndexEntryV2,
    VectorEvidenceIndexRepository, VectorObservationRepository, checked_encoded_page_content,
};

/// Physical order for one bounded immutable index-range read.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SnapshotIndexDirectionV1 {
    /// Lowest complete key first.
    Forward,
    /// Highest complete key first.
    Reverse,
}

/// One checked engine-neutral range request against an owned snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotIndexRangeRequestV1 {
    lower: Vec<u8>,
    lower_inclusive: bool,
    upper: Vec<u8>,
    upper_inclusive: bool,
    direction: SnapshotIndexDirectionV1,
    after: Option<Vec<u8>>,
    limit: StorageScanLimit,
}

impl SnapshotIndexRangeRequestV1 {
    /// Checks bounded nonempty ordered endpoints and an in-range continuation.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        lower: Vec<u8>,
        lower_inclusive: bool,
        upper: Vec<u8>,
        upper_inclusive: bool,
        direction: SnapshotIndexDirectionV1,
        after: Option<Vec<u8>>,
        limit: StorageScanLimit,
    ) -> Result<Self, StorageValueError> {
        if lower.is_empty()
            || upper.is_empty()
            || lower.len() > MAX_KEY_BYTES
            || upper.len() > MAX_KEY_BYTES
            || lower >= upper
            || after.as_ref().is_some_and(|bytes| {
                bytes.as_slice() < lower.as_slice() || bytes.as_slice() > upper.as_slice()
            })
        {
            return Err(StorageValueError::IdentityMismatch);
        }
        Ok(Self {
            lower,
            lower_inclusive,
            upper,
            upper_inclusive,
            direction,
            after,
            limit,
        })
    }

    /// Lower physical bound.
    #[must_use]
    pub fn lower(&self) -> &[u8] {
        &self.lower
    }
    /// Whether the lower physical bound is included.
    #[must_use]
    pub const fn lower_inclusive(&self) -> bool {
        self.lower_inclusive
    }
    /// Upper physical bound.
    #[must_use]
    pub fn upper(&self) -> &[u8] {
        &self.upper
    }
    /// Whether the upper physical bound is included.
    #[must_use]
    pub const fn upper_inclusive(&self) -> bool {
        self.upper_inclusive
    }
    /// Stable physical direction.
    #[must_use]
    pub const fn direction(&self) -> SnapshotIndexDirectionV1 {
        self.direction
    }
    /// Exclusive prior complete-key position in the selected direction.
    #[must_use]
    pub fn after(&self) -> Option<&[u8]> {
        self.after.as_deref()
    }
    /// Maximum rows returned by this storage page.
    #[must_use]
    pub const fn limit(&self) -> StorageScanLimit {
        self.limit
    }
}

/// One bounded semantic page from an immutable index range.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotIndexRangePageV1 {
    entries: Vec<EncodedPageItem<StoredIndexEntryV2>>,
    next_after: Option<IndexEntryKey>,
}

impl SnapshotIndexRangePageV1 {
    /// Checks bounds, encoded charge, physical range, order, and continuation.
    pub fn new(
        request: &SnapshotIndexRangeRequestV1,
        entries: Vec<EncodedPageItem<StoredIndexEntryV2>>,
        has_more: bool,
    ) -> Result<Self, StorageValueError> {
        if entries.len() > usize::from(request.limit().get()) {
            return Err(StorageValueError::LimitExceeded);
        }
        checked_encoded_page_content(&entries, MAX_SCAN_PAGE_BYTES)?;
        let mut prior = request.after();
        for entry in &entries {
            let key = entry.value().key().as_bytes();
            let lower_ok =
                key > request.lower() || (request.lower_inclusive() && key == request.lower());
            let upper_ok =
                key < request.upper() || (request.upper_inclusive() && key == request.upper());
            let ordered = prior.is_none_or(|prior| match request.direction() {
                SnapshotIndexDirectionV1::Forward => prior < key,
                SnapshotIndexDirectionV1::Reverse => prior > key,
            });
            if !lower_ok || !upper_ok || !ordered {
                return Err(StorageValueError::NonCanonicalOrder);
            }
            prior = Some(key);
        }
        let next_after = if has_more {
            Some(
                entries
                    .last()
                    .ok_or(StorageValueError::InvalidShape)?
                    .value()
                    .key()
                    .clone(),
            )
        } else {
            None
        };
        Ok(Self {
            entries,
            next_after,
        })
    }

    /// Complete semantic current-index rows in selected physical order.
    #[must_use]
    pub fn entries(&self) -> &[EncodedPageItem<StoredIndexEntryV2>] {
        &self.entries
    }
    /// Exclusive continuation when more rows remain in this exact range.
    #[must_use]
    pub const fn next_after(&self) -> Option<&IndexEntryKey> {
        self.next_after.as_ref()
    }
}

/// Exact application and index fences captured by one owned snapshot.
pub trait SnapshotFenceReader {
    /// Application head pinned by the snapshot.
    fn application_frontier(&self) -> Result<Option<CommitSequence>, StorageError>;

    /// Exact generation fence for one partitioned index.
    fn index_epoch(&self, target: &PartitionIndexTarget) -> Result<u64, StorageError>;

    /// Reads one bounded physical range without interpreting its query meaning.
    fn scan_snapshot_index_range(
        &self,
        request: SnapshotIndexRangeRequestV1,
    ) -> Result<SnapshotIndexRangePageV1, StorageError>;
}

/// The complete API-neutral reader surface of one pinned backend view.
pub trait OwnedSnapshotHandle:
    AuthoritativePointReader
    + AuthoritativeScanReader
    + FilteredAuthoritativeScanReader
    + SnapshotReader
    + SnapshotFenceReader
    + VectorObservationRepository
    + VectorEvidenceIndexRepository
{
}

impl<T> OwnedSnapshotHandle for T where
    T: AuthoritativePointReader
        + AuthoritativeScanReader
        + FilteredAuthoritativeScanReader
        + SnapshotReader
        + SnapshotFenceReader
        + VectorObservationRepository
        + VectorEvidenceIndexRepository
{
}

/// Opens one owned immutable view whose handle cannot outlive its backend.
pub trait OwnedSnapshotReader {
    /// Backend-specific handle with only engine-neutral reader operations.
    type Snapshot<'a>: OwnedSnapshotHandle
    where
        Self: 'a;

    /// Pins one view for one complete executor page or bounded group.
    fn open_owned_snapshot(&self) -> Result<Self::Snapshot<'_>, StorageError>;
}

impl<T> OwnedSnapshotReader for &T
where
    T: OwnedSnapshotReader + ?Sized,
{
    type Snapshot<'a>
        = T::Snapshot<'a>
    where
        Self: 'a;

    fn open_owned_snapshot(&self) -> Result<Self::Snapshot<'_>, StorageError> {
        T::open_owned_snapshot(*self)
    }
}
