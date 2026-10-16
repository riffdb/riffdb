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

/// Shared canonical field map for one covered projection entry.
pub type CoveredProjectionFieldMapV1 =
    std::sync::Arc<std::collections::BTreeMap<std::sync::Arc<str>, riffdb_types::CanonicalValue>>;

/// Cached covered projection for one entry in a [`SnapshotIndexRangePageV1`].
pub type CachedCoveredProjectionV1 = (
    std::sync::Arc<[riffdb_types::FieldId]>,
    std::sync::Arc<[std::sync::Arc<str>]>,
    std::sync::Arc<[riffdb_types::CanonicalValue]>,
    CoveredProjectionFieldMapV1,
    u64,
    Option<(CoveredProjectionFieldMapV1, u64)>,
);

/// Borrowed view of a cached covered projection on [`SnapshotIndexRangePageV1`].
pub type CoveredProjectionBorrowV1<'a> = (
    &'a std::sync::Arc<[riffdb_types::CanonicalValue]>,
    &'a CoveredProjectionFieldMapV1,
    u64,
    &'a Option<(CoveredProjectionFieldMapV1, u64)>,
);

/// One bounded semantic page from an immutable index range.
#[derive(Clone)]
pub struct SnapshotIndexRangePageV1 {
    entries: std::sync::Arc<[EncodedPageItem<StoredIndexEntryV2>]>,
    resolved_entities: std::sync::Arc<[std::sync::OnceLock<crate::StoredEntityRecordV1>]>,
    covered_projections: std::sync::Arc<
        [(
            std::sync::OnceLock<CachedCoveredProjectionV1>,
            std::sync::OnceLock<CachedCoveredProjectionV1>,
        )],
    >,
    next_after: Option<IndexEntryKey>,
}

impl std::fmt::Debug for SnapshotIndexRangePageV1 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnapshotIndexRangePageV1")
            .field("entries", &self.entries)
            .field("next_after", &self.next_after)
            .finish()
    }
}

impl PartialEq for SnapshotIndexRangePageV1 {
    fn eq(&self, other: &Self) -> bool {
        self.entries == other.entries && self.next_after == other.next_after
    }
}

impl Eq for SnapshotIndexRangePageV1 {}

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
        let entry_len = entries.len();
        let resolved_entities: std::sync::Arc<[std::sync::OnceLock<crate::StoredEntityRecordV1>]> =
            (0..entry_len)
                .map(|_| std::sync::OnceLock::new())
                .collect::<Vec<_>>()
                .into();
        let covered_projections: std::sync::Arc<
            [(
                std::sync::OnceLock<CachedCoveredProjectionV1>,
                std::sync::OnceLock<CachedCoveredProjectionV1>,
            )],
        > = (0..entry_len)
            .map(|_| (std::sync::OnceLock::new(), std::sync::OnceLock::new()))
            .collect::<Vec<_>>()
            .into();
        Ok(Self {
            entries: std::sync::Arc::from(entries),
            resolved_entities,
            covered_projections,
            next_after,
        })
    }

    /// Creates a copy of this page with a detached entries slice and fresh, unshared cache slots.
    #[must_use]
    pub fn thread_local_detach(&self) -> Self {
        let entry_len = self.entries.len();
        let resolved_entities: std::sync::Arc<[std::sync::OnceLock<crate::StoredEntityRecordV1>]> =
            (0..entry_len)
                .map(|_| std::sync::OnceLock::new())
                .collect::<Vec<_>>()
                .into();
        let covered_projections: std::sync::Arc<
            [(
                std::sync::OnceLock<CachedCoveredProjectionV1>,
                std::sync::OnceLock<CachedCoveredProjectionV1>,
            )],
        > = (0..entry_len)
            .map(|_| (std::sync::OnceLock::new(), std::sync::OnceLock::new()))
            .collect::<Vec<_>>()
            .into();
        Self {
            entries: std::sync::Arc::from(self.entries.as_ref()),
            resolved_entities,
            covered_projections,
            next_after: self.next_after.clone(),
        }
    }

    /// Complete semantic current-index rows in selected physical order.
    #[must_use]
    pub fn entries(&self) -> &[EncodedPageItem<StoredIndexEntryV2>] {
        &self.entries
    }

    /// Returns a cached resolved entity record at `index`, if already hydrated on this snapshot page.
    #[must_use]
    pub fn resolved_entity(&self, index: usize) -> Option<&crate::StoredEntityRecordV1> {
        self.resolved_entities.get(index)?.get()
    }

    /// Caches a resolved entity record at `index` on this snapshot page and returns a reference to it.
    pub fn cache_resolved_entity(
        &self,
        index: usize,
        record: crate::StoredEntityRecordV1,
    ) -> &crate::StoredEntityRecordV1 {
        let slot = &self.resolved_entities[index];
        let _ = slot.set(record);
        #[expect(clippy::expect_used, reason = "OnceLock slot was just populated")]
        slot.get().expect("OnceLock slot populated")
    }

    /// Returns a cached covered projection at `index` if `cover_ids` and `field_names` match.
    #[must_use]
    pub fn covered_projection(
        &self,
        index: usize,
        cover_ids: &std::sync::Arc<[riffdb_types::FieldId]>,
        field_names: &std::sync::Arc<[std::sync::Arc<str>]>,
    ) -> Option<CoveredProjectionBorrowV1<'_>> {
        let (primary, secondary) = self.covered_projections.get(index)?;
        if let Some((cached_ids, cached_names, values, fields, fields_bytes, projected)) =
            primary.get()
            && std::sync::Arc::ptr_eq(cached_ids, cover_ids)
            && std::sync::Arc::ptr_eq(cached_names, field_names)
        {
            return Some((values, fields, *fields_bytes, projected));
        }
        if let Some((cached_ids, cached_names, values, fields, fields_bytes, projected)) =
            secondary.get()
            && std::sync::Arc::ptr_eq(cached_ids, cover_ids)
            && std::sync::Arc::ptr_eq(cached_names, field_names)
        {
            return Some((values, fields, *fields_bytes, projected));
        }
        if let Some((cached_ids, cached_names, values, fields, fields_bytes, projected)) =
            primary.get()
            && cached_ids.as_ref() == cover_ids.as_ref()
            && cached_names.as_ref() == field_names.as_ref()
        {
            return Some((values, fields, *fields_bytes, projected));
        }
        if let Some((cached_ids, cached_names, values, fields, fields_bytes, projected)) =
            secondary.get()
            && cached_ids.as_ref() == cover_ids.as_ref()
            && cached_names.as_ref() == field_names.as_ref()
        {
            return Some((values, fields, *fields_bytes, projected));
        }
        None
    }

    /// Caches a decoded covered projection at `index` on this snapshot page.
    pub fn cache_covered_projection(&self, index: usize, projection: CachedCoveredProjectionV1) {
        if let Some((primary, secondary)) = self.covered_projections.get(index)
            && let Err(projection) = primary.set(projection)
        {
            let _ = secondary.set(projection);
        }
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

/// First-party admitted reads from one captured provider source. Implementations
/// preserve ordinary point and range validation and never reopen a newer view.
#[doc(hidden)]
pub trait ProviderPolicySourceReaderV1 {
    /// Decode an entity only after admitting its input and native workspace.
    fn read_provider_entity_record_v1(
        &self,
        target: &crate::EntityTarget,
        budget: &riffdb_types::exact_provider_budget_v1::ProviderBudget,
    ) -> Result<Option<crate::AdmittedProviderEntityRecordV1>, StorageError>;

    /// Decode a forward relationship range under the ordinary page ceilings.
    fn read_provider_policy_index_range_v1(
        &self,
        request: &SnapshotIndexRangeRequestV1,
        budget: &riffdb_types::exact_provider_budget_v1::ProviderBudget,
    ) -> Result<
        std::sync::Arc<
            riffdb_types::exact_provider_budget_v1::ProviderAllocation<SnapshotIndexRangePageV1>,
        >,
        StorageError,
    >;
}
