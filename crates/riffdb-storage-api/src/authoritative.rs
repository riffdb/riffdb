//! Narrow authoritative point reads and bounded ordered scans.

use std::fmt;
use std::num::NonZeroU16;

use riffdb_types::{
    CommitSequence, ContractLineage, EventId, FrontierPosition, IndexEntryKey, PartitionKey,
    ProvenanceId,
};

use crate::{
    DurableKeySchemaBindingV1, EncodedPageItem, EntityTarget, IdempotencyIdentity,
    IndexEpochPosition, IndexRangeEntry, IndexRangeTarget, MAX_COMMIT_SCAN_PAGE_BYTES,
    MAX_INDEX_PARTITION_FILTER_BYTES, MAX_INDEX_PARTITION_FILTER_KEYS, MAX_SCAN_PAGE_BYTES,
    MAX_SCAN_PAGE_ENTRIES, StorageError, StorageValueError, StoredCommitRecordV1,
    StoredDurableEventV1, StoredEntityRecordV1, StoredIndexEntryV2, StoredOutcomeV1,
    StoredProvenanceRecordV1, checked_encoded_page_content, framed_bytes,
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

/// Policy-neutral exact partition scope for one lower index scan.
#[derive(Clone, Eq, PartialEq)]
pub enum IndexPartitionFilterScope {
    /// Every partition in the exact target lineage is eligible.
    All,
    /// No partition is eligible; storage must return exact end without inspecting rows.
    None,
    /// A nonempty bounded canonical set of exact partition keys.
    Explicit(Vec<PartitionKey>),
}

/// A checked policy-neutral partition filter supplied to authoritative storage.
#[derive(Clone, Eq, PartialEq)]
pub struct IndexPartitionFilter {
    target_lineage: ContractLineage,
    scope: IndexPartitionFilterScope,
}

impl IndexPartitionFilter {
    /// Checks the exact-lineage lower filter and its canonical explicit scope.
    pub fn new(
        target_lineage: ContractLineage,
        scope: IndexPartitionFilterScope,
    ) -> Result<Self, StorageValueError> {
        if let IndexPartitionFilterScope::Explicit(keys) = &scope {
            if keys.is_empty() {
                return Err(StorageValueError::Empty);
            }
            if keys.len() > MAX_INDEX_PARTITION_FILTER_KEYS {
                return Err(StorageValueError::LimitExceeded);
            }
            let mut total = 0usize;
            let mut prior: Option<&[u8]> = None;
            for key in keys {
                let bytes = key.as_bytes();
                if prior == Some(bytes) {
                    return Err(StorageValueError::Duplicate);
                }
                if prior.is_some_and(|prior| prior > bytes) {
                    return Err(StorageValueError::NonCanonicalOrder);
                }
                total = total
                    .checked_add(framed_bytes(bytes.len())?)
                    .ok_or(StorageValueError::SizeOverflow)?;
                if total > MAX_INDEX_PARTITION_FILTER_BYTES {
                    return Err(StorageValueError::LimitExceeded);
                }
                prior = Some(bytes);
            }
        }
        Ok(Self {
            target_lineage,
            scope,
        })
    }

    /// Borrows the exact contract lineage selected by the service request.
    #[must_use]
    pub const fn target_lineage(&self) -> &ContractLineage {
        &self.target_lineage
    }

    /// Borrows the checked lower scope.
    #[must_use]
    pub const fn scope(&self) -> &IndexPartitionFilterScope {
        &self.scope
    }

    /// Returns whether this filter proves that no physical inspection is needed.
    #[must_use]
    pub const fn is_none(&self) -> bool {
        matches!(self.scope, IndexPartitionFilterScope::None)
    }

    /// Tests one exact stored row identity without interpreting policy or contract IR.
    #[must_use]
    pub fn allows(
        &self,
        schema_binding: &DurableKeySchemaBindingV1,
        partition_key: &PartitionKey,
    ) -> bool {
        if schema_binding.lineage() != &self.target_lineage {
            return false;
        }
        match &self.scope {
            IndexPartitionFilterScope::All => true,
            IndexPartitionFilterScope::None => false,
            IndexPartitionFilterScope::Explicit(keys) => keys
                .binary_search_by(|candidate| candidate.as_bytes().cmp(partition_key.as_bytes()))
                .is_ok(),
        }
    }
}

/// Transitional strict ADR-0038 scan request used during the coordinated V1 cutover.
///
/// This type is intentionally separate from [`AuthoritativeIndexScanRequest`]
/// until the redb, shared-service, and transport owners move atomically to V2.
#[derive(Clone, Eq, PartialEq)]
pub struct FilteredAuthoritativeIndexScanRequest {
    target: IndexRangeTarget,
    partition_filter: IndexPartitionFilter,
    after: Option<IndexEntryKey>,
    limit: StorageScanLimit,
}

impl FilteredAuthoritativeIndexScanRequest {
    /// Constructs one checked filtered prefix scan after an optional physical key.
    pub fn new(
        target: IndexRangeTarget,
        partition_filter: IndexPartitionFilter,
        after: Option<IndexEntryKey>,
        limit: StorageScanLimit,
    ) -> Result<Self, StorageValueError> {
        validate_index_scan_continuation(&target, after.as_ref())?;
        match partition_filter.scope() {
            IndexPartitionFilterScope::Explicit(keys)
                if keys.as_slice() == [target.generation_target().partition_key().clone()] => {}
            IndexPartitionFilterScope::All
            | IndexPartitionFilterScope::None
            | IndexPartitionFilterScope::Explicit(_) => {
                return Err(StorageValueError::InvalidShape);
            }
        }
        Ok(Self {
            target,
            partition_filter,
            after,
            limit,
        })
    }

    /// Borrows the exact complete-component prefix target.
    #[must_use]
    pub const fn target(&self) -> &IndexRangeTarget {
        &self.target
    }

    /// Borrows the checked policy-neutral lower filter.
    #[must_use]
    pub const fn partition_filter(&self) -> &IndexPartitionFilter {
        &self.partition_filter
    }

    /// Borrows the exclusive complete physical-key continuation.
    #[must_use]
    pub const fn after(&self) -> Option<&IndexEntryKey> {
        self.after.as_ref()
    }

    /// Returns the checked requested matching-row count.
    #[must_use]
    pub const fn limit(&self) -> StorageScanLimit {
        self.limit
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
        validate_index_scan_continuation(&target, after.as_ref())?;
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

/// Strict ADR-0038 result for one filtered scan transaction.
///
/// Returned rows retain their exact stored binding and partition. A non-final
/// page may be empty, but always carries strict physical progress.
#[derive(Clone, Eq, PartialEq)]
pub enum FilteredAuthoritativeIndexScanPage {
    /// A non-final page with its last physically inspected complete key.
    Page {
        /// Matching current V2 rows in complete-key order.
        entries: Vec<EncodedPageItem<StoredIndexEntryV2>>,
        /// Exact last physical candidate inspected, including filtered-out rows.
        scanned_through: IndexEntryKey,
        /// Exact range epoch observed in the same read view as rows and progress.
        epoch: IndexEpochPosition,
    },
    /// The exact physical range end, possibly with matching final rows.
    ExactEnd {
        /// Matching final current V2 rows, or empty when no rows remain.
        entries: Vec<EncodedPageItem<StoredIndexEntryV2>>,
        /// Exact range epoch observed in the same read view as the rows.
        epoch: IndexEpochPosition,
    },
}

impl FilteredAuthoritativeIndexScanPage {
    /// Checks one non-final sparse page against its exact request.
    pub fn page(
        request: &FilteredAuthoritativeIndexScanRequest,
        epoch: IndexEpochPosition,
        entries: Vec<EncodedPageItem<StoredIndexEntryV2>>,
        scanned_through: IndexEntryKey,
    ) -> Result<Self, StorageValueError> {
        validate_filtered_index_scan_entries(request, &entries)?;
        validate_scanned_through(request, &entries, &scanned_through)?;
        Ok(Self::Page {
            entries,
            scanned_through,
            epoch,
        })
    }

    /// Checks a final sparse page, including an empty exact-end result.
    pub fn exact_end(
        request: &FilteredAuthoritativeIndexScanRequest,
        epoch: IndexEpochPosition,
        entries: Vec<EncodedPageItem<StoredIndexEntryV2>>,
    ) -> Result<Self, StorageValueError> {
        validate_filtered_index_scan_entries(request, &entries)?;
        Ok(Self::ExactEnd { entries, epoch })
    }

    /// Borrows matching current V2 rows in canonical complete-key order.
    #[must_use]
    pub fn entries(&self) -> &[EncodedPageItem<StoredIndexEntryV2>] {
        match self {
            Self::Page { entries, .. } | Self::ExactEnd { entries, .. } => entries,
        }
    }

    /// Borrows non-final physical progress, or returns `None` at exact end.
    #[must_use]
    pub const fn scanned_through(&self) -> Option<&IndexEntryKey> {
        match self {
            Self::Page {
                scanned_through, ..
            } => Some(scanned_through),
            Self::ExactEnd { .. } => None,
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

/// Transitional strict ADR-0038 scans over current partition-bearing index rows.
///
/// The coordinated cutover removes the legacy scan trait and promotes this
/// contract rather than retaining two production read paths.
pub trait FilteredAuthoritativeScanReader {
    /// Reads at most one bounded physical prefix page and its epoch atomically.
    fn scan_index_filtered(
        &self,
        request: FilteredAuthoritativeIndexScanRequest,
    ) -> Result<FilteredAuthoritativeIndexScanPage, StorageError>;
}

fn validate_index_scan_continuation(
    target: &IndexRangeTarget,
    after: Option<&IndexEntryKey>,
) -> Result<(), StorageValueError> {
    if after.is_some_and(|key| {
        key.index_id() != target.prefix().index_id()
            || !key.as_bytes().starts_with(target.prefix().as_bytes())
    }) {
        return Err(StorageValueError::IdentityMismatch);
    }
    Ok(())
}

fn validate_filtered_index_scan_entries(
    request: &FilteredAuthoritativeIndexScanRequest,
    entries: &[EncodedPageItem<StoredIndexEntryV2>],
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
        if !request
            .partition_filter
            .allows(entry.schema_binding(), entry.partition_key())
        {
            return Err(StorageValueError::IdentityMismatch);
        }
        prior = Some(entry.key().as_bytes());
    }
    Ok(())
}

fn validate_scanned_through(
    request: &FilteredAuthoritativeIndexScanRequest,
    entries: &[EncodedPageItem<StoredIndexEntryV2>],
    scanned_through: &IndexEntryKey,
) -> Result<(), StorageValueError> {
    if scanned_through.index_id() != request.target.prefix().index_id()
        || !scanned_through
            .as_bytes()
            .starts_with(request.target.prefix().as_bytes())
        || request
            .after
            .as_ref()
            .is_some_and(|after| after.as_bytes() >= scanned_through.as_bytes())
        || entries
            .last()
            .is_some_and(|entry| entry.value().key().as_bytes() > scanned_through.as_bytes())
    {
        return Err(StorageValueError::InvalidShape);
    }
    Ok(())
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
    IndexPartitionFilterScope,
    IndexPartitionFilter,
    FilteredAuthoritativeIndexScanRequest,
    FilteredAuthoritativeIndexScanPage,
    AuthoritativeIndexScanRequest,
    AuthoritativeIndexScanPage,
    CommitScanPageV1,
);

#[cfg(test)]
mod tests {
    use riffdb_types::{
        AggregateTypeId, CanonicalRecord, ContractBundleHash, ContractVersion, EntityKeyBuilder,
        EntityTypeId, IndexEntryKeyBuilder, IndexId, PartitionKeyBuilder,
    };

    use super::*;
    use crate::{EncodedContentCharge, IndexRangePrefixBuilder};

    fn lineage(value: &str) -> ContractLineage {
        ContractLineage::new(value).expect("lineage")
    }

    fn binding(value: &str) -> DurableKeySchemaBindingV1 {
        DurableKeySchemaBindingV1::new(
            lineage(value),
            ContractVersion::new(1).expect("version"),
            ContractBundleHash::from_bytes([0x11; 32]),
        )
    }

    fn partition(value: u64) -> PartitionKey {
        let mut builder = PartitionKeyBuilder::new(AggregateTypeId::new(1).expect("aggregate"));
        builder.push_u64(value).expect("partition component");
        builder.finish().expect("partition")
    }

    fn large_partition(length: usize, ordinal: u8) -> PartitionKey {
        let base = PartitionKeyBuilder::new(AggregateTypeId::new(1).expect("aggregate"))
            .finish()
            .expect("base partition");
        let mut bytes = vec![0; length];
        bytes[..base.as_bytes().len()].copy_from_slice(base.as_bytes());
        bytes[base.as_bytes().len()] = ordinal;
        PartitionKey::from_bytes(bytes).expect("bounded partition")
    }

    fn range() -> IndexRangeTarget {
        let mut builder = IndexRangePrefixBuilder::new(IndexId::new(1).expect("index"));
        builder.push_u64(7).expect("prefix component");
        IndexRangeTarget::new(partition(7), builder.finish())
    }

    fn index_key(value: u64) -> IndexEntryKey {
        let mut entity = EntityKeyBuilder::new(EntityTypeId::new(1).expect("entity type"));
        entity.push_u64(value).expect("entity component");
        let mut index = IndexEntryKeyBuilder::new(IndexId::new(1).expect("index"));
        index.push_u64(7).expect("index component");
        index
            .finish(entity.finish().expect("entity key"))
            .expect("index key")
    }

    fn row(value: u64, lineage: &str, partition_key: PartitionKey) -> StoredIndexEntryV2 {
        StoredIndexEntryV2::new(
            index_key(value),
            binding(lineage),
            CanonicalRecord::new(Vec::new()).expect("record"),
            partition_key,
        )
        .expect("V2 row")
    }

    #[test]
    fn explicit_filter_accepts_1024_keys_and_rejects_1025() {
        let exact = (0..MAX_INDEX_PARTITION_FILTER_KEYS)
            .map(|value| partition(u64::try_from(value).expect("value")))
            .collect::<Vec<_>>();
        assert!(
            IndexPartitionFilter::new(
                lineage("orders"),
                IndexPartitionFilterScope::Explicit(exact)
            )
            .is_ok()
        );

        let over = (0..=MAX_INDEX_PARTITION_FILTER_KEYS)
            .map(|value| partition(u64::try_from(value).expect("value")))
            .collect::<Vec<_>>();
        assert_eq!(
            IndexPartitionFilter::new(lineage("orders"), IndexPartitionFilterScope::Explicit(over)),
            Err(StorageValueError::LimitExceeded)
        );
    }

    #[test]
    fn explicit_filter_accepts_exact_mebibyte_and_rejects_one_more_byte() {
        let exact = (0_u8..=u8::MAX)
            .map(|ordinal| large_partition(4_092, ordinal))
            .collect::<Vec<_>>();
        assert!(
            IndexPartitionFilter::new(
                lineage("orders"),
                IndexPartitionFilterScope::Explicit(exact.clone())
            )
            .is_ok()
        );

        let mut over = exact;
        *over.last_mut().expect("last") = large_partition(4_093, u8::MAX);
        assert_eq!(
            IndexPartitionFilter::new(lineage("orders"), IndexPartitionFilterScope::Explicit(over)),
            Err(StorageValueError::LimitExceeded)
        );
    }

    #[test]
    fn explicit_filter_rejects_empty_duplicate_and_noncanonical_keys() {
        assert_eq!(
            IndexPartitionFilter::new(
                lineage("orders"),
                IndexPartitionFilterScope::Explicit(Vec::new())
            ),
            Err(StorageValueError::Empty)
        );
        let first = partition(1);
        let second = partition(2);
        assert_eq!(
            IndexPartitionFilter::new(
                lineage("orders"),
                IndexPartitionFilterScope::Explicit(vec![first.clone(), first])
            ),
            Err(StorageValueError::Duplicate)
        );
        assert_eq!(
            IndexPartitionFilter::new(
                lineage("orders"),
                IndexPartitionFilterScope::Explicit(vec![second, partition(1)])
            ),
            Err(StorageValueError::NonCanonicalOrder)
        );
    }

    #[test]
    fn sparse_page_requires_strict_physical_progress() {
        let filter = IndexPartitionFilter::new(
            lineage("orders"),
            IndexPartitionFilterScope::Explicit(vec![partition(7)]),
        )
        .expect("filter");
        let request = FilteredAuthoritativeIndexScanRequest::new(
            range(),
            filter.clone(),
            None,
            StorageScanLimit::new(1).expect("limit"),
        )
        .expect("request");
        let progress = index_key(2);
        assert!(
            FilteredAuthoritativeIndexScanPage::page(
                &request,
                IndexEpochPosition::BeforeFirst,
                Vec::new(),
                progress.clone()
            )
            .is_ok()
        );

        let continued = FilteredAuthoritativeIndexScanRequest::new(
            range(),
            filter,
            Some(progress.clone()),
            StorageScanLimit::new(1).expect("limit"),
        )
        .expect("request");
        assert_eq!(
            FilteredAuthoritativeIndexScanPage::page(
                &continued,
                IndexEpochPosition::BeforeFirst,
                Vec::new(),
                progress
            ),
            Err(StorageValueError::InvalidShape)
        );
    }

    #[test]
    fn filtered_page_rejects_wrong_lineage_and_enforces_returned_byte_boundary() {
        let filter = IndexPartitionFilter::new(
            lineage("orders"),
            IndexPartitionFilterScope::Explicit(vec![partition(7)]),
        )
        .expect("filter");
        let request = FilteredAuthoritativeIndexScanRequest::new(
            range(),
            filter,
            None,
            StorageScanLimit::new(1).expect("limit"),
        )
        .expect("request");
        let foreign = EncodedPageItem::new(
            row(1, "foreign", partition(7)),
            EncodedContentCharge::new(1).expect("charge"),
        );
        assert_eq!(
            FilteredAuthoritativeIndexScanPage::exact_end(
                &request,
                IndexEpochPosition::BeforeFirst,
                vec![foreign]
            ),
            Err(StorageValueError::IdentityMismatch)
        );

        let exact = EncodedPageItem::new(
            row(1, "orders", partition(7)),
            EncodedContentCharge::new(MAX_SCAN_PAGE_BYTES).expect("charge"),
        );
        assert!(
            FilteredAuthoritativeIndexScanPage::exact_end(
                &request,
                IndexEpochPosition::BeforeFirst,
                vec![exact]
            )
            .is_ok()
        );

        let oversized = EncodedPageItem::new(
            row(1, "orders", partition(7)),
            EncodedContentCharge::new(MAX_SCAN_PAGE_BYTES + 1).expect("charge"),
        );
        assert_eq!(
            FilteredAuthoritativeIndexScanPage::exact_end(
                &request,
                IndexEpochPosition::BeforeFirst,
                vec![oversized]
            ),
            Err(StorageValueError::LimitExceeded)
        );
    }
}
