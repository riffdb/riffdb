#![expect(
    clippy::expect_used,
    reason = "a resolved checkpoint root retains the table handles established by its immutable read transaction"
)]

//! One immutable redb read snapshot plus the table handles resolved from it.
//!
//! `redb::ReadTransaction::open_table` resolves a table by name on every call:
//! a `&str`-keyed lookup in the table tree, a UTF-8 decode, an
//! `InternalTableDefinition` decode, a `String` allocation for the returned
//! handle, and three refcount bumps on `Arc`s shared by every reader of the
//! database. The last of those is the expensive part under concurrency, because
//! the refcounts live on cache lines that every reader thread writes.
//!
//! None of that work can produce a different answer twice against the same
//! snapshot. A `ReadTransaction` observes one fixed committed root: the set of
//! tables it can see, their names, and their B-tree roots are all fixed for its
//! whole lifetime, and redb's `TransactionGuard` keeps the pages those roots
//! address from being reused while it lives. So the handle a name resolves to
//! is a pure function of (snapshot, name), and resolving it once per snapshot
//! is exactly equivalent to resolving it once per access.
//!
//! This type makes that equivalence structural. The cache is a private field of
//! the snapshot it was resolved from, populated lazily, and dropped with it. A
//! handle therefore cannot be observed against any snapshot but its own, and a
//! new checkpoint — which is a new `ReadTransaction` — necessarily gets a new
//! empty cache rather than inheriting one.
//!
//! Failures are deliberately not cached. A table that fails to open is
//! re-attempted on the next access and keeps returning the same error it does
//! today, so no lookup failure is converted into absence, and no validation is
//! skipped or made conditional.
//!
//! How long one of these lives depends on which read a caller holds, and the
//! saving scales with that lifetime rather than depending on it:
//!
//! - A published composite checkpoint or a durable read frontier keeps one
//!   snapshot across many queries, so each table is resolved once for all of
//!   them.
//! - `RedbReadAccess::Current` serves the newest snapshot a handle with no
//!   durable frontier may open. `SharedRedb::current_read_root` reuses one such
//!   snapshot for every access entitled to it — every access taken while
//!   the stable even `durable_root_publication` generation is unchanged — so
//!   the cache spans those accesses too. The intervening odd generation marks
//!   an engine commit whose successor root may already be visible but whose
//!   exact identity has not yet been published. A daemon that has not written
//!   since startup has neither a durable frontier nor a composite publication,
//!   so this is the only shape its reads take.
//!
//! The same equivalence licenses caching a derived *value* rather than a
//! handle, provided every input it reads is fixed by the snapshot. The
//! snapshot-visible application frontier is the one such value cached here.

use std::ops::Deref;
use std::sync::{Arc, OnceLock, Weak};
use std::time::Instant;

use redb::{ReadOnlyTable, ReadTransaction, TableDefinition, TableError};
use riffdb_storage_api::{
    EntityTarget, PartitionIndexTarget, SnapshotIndexRangePageV1, SnapshotIndexRangeRequestV1,
    StorageError, StoredEntityRecordV1,
};
use riffdb_types::CommitSequence;

use crate::journal::{JournalIoError, JournalTable};
use crate::layout::{CAPABILITIES, META};

/// Decoded snapshot access facade, retaining no decoded data between requests.
///
/// Memoized variable-size values require admitted byte ownership, including all
/// hydrated page aliases. Keep derivation direct until that ownership is provided.
#[derive(Default)]
pub(crate) struct SnapshotDecodedCache;

#[allow(
    clippy::unused_self,
    reason = "the access facade retains the shared call interface without decoded ownership"
)]
impl SnapshotDecodedCache {
    pub(crate) fn read_entity(
        &self,
        _target: &EntityTarget,
        derive: impl FnOnce() -> Result<Option<StoredEntityRecordV1>, StorageError>,
    ) -> Result<Option<StoredEntityRecordV1>, StorageError> {
        derive()
    }

    pub(crate) fn index_epoch(
        &self,
        _target: &PartitionIndexTarget,
        derive: impl FnOnce() -> Result<u64, StorageError>,
    ) -> Result<u64, StorageError> {
        derive()
    }

    pub(crate) fn scan_index_range(
        &self,
        request: SnapshotIndexRangeRequestV1,
        derive: impl FnOnce(
            &SnapshotIndexRangeRequestV1,
        ) -> Result<SnapshotIndexRangePageV1, StorageError>,
    ) -> Result<SnapshotIndexRangePageV1, StorageError> {
        derive(&request)
    }
}

/// Byte-keyed operational table handle.
type ByteTable = ReadOnlyTable<&'static [u8], &'static [u8]>;
/// String-keyed metadata table handle.
type MetaTable = ReadOnlyTable<&'static str, &'static [u8]>;

/// Cache slots for byte-keyed journal tables, indexed by discriminant minus one.
///
/// [`JournalTable::Meta`] occupies slot zero and is never stored there: its key
/// type differs, so it has its own field.
const JOURNAL_TABLE_SLOTS: usize = JournalTable::ALL.len();

/// The actual backend owner must outlive every escaped read transaction: redb
/// closes its backend when either owning database type is dropped, even while
/// a read transaction still retains transactional memory.
pub(crate) enum CheckpointEngineCustody {
    Writable {
        owner: std::sync::Arc<crate::engine_owner::EngineOwner<redb::Database>>,
    },
    ReadOnly {
        owner: std::sync::Arc<crate::engine_owner::EngineOwner<redb::ReadOnlyDatabase>>,
    },
}

impl From<std::sync::Arc<crate::engine_owner::EngineOwner<redb::Database>>>
    for CheckpointEngineCustody
{
    fn from(owner: std::sync::Arc<crate::engine_owner::EngineOwner<redb::Database>>) -> Self {
        Self::Writable { owner }
    }
}

impl From<std::sync::Arc<crate::engine_owner::EngineOwner<redb::ReadOnlyDatabase>>>
    for CheckpointEngineCustody
{
    fn from(
        owner: std::sync::Arc<crate::engine_owner::EngineOwner<redb::ReadOnlyDatabase>>,
    ) -> Self {
        Self::ReadOnly { owner }
    }
}

impl CheckpointEngineCustody {
    #[cfg(test)]
    pub(crate) fn provider_charge_bytes(&self) -> u64 {
        match self {
            Self::Writable { owner } => owner.provider_charge_bytes(),
            Self::ReadOnly { owner } => owner.provider_charge_bytes(),
        }
    }
    pub(crate) fn is_writable_owner(
        &self,
        other: &std::sync::Arc<crate::engine_owner::EngineOwner<redb::Database>>,
    ) -> bool {
        match self {
            Self::Writable { owner } => std::sync::Arc::ptr_eq(owner, other),
            Self::ReadOnly { .. } => false,
        }
    }
    pub(crate) fn provider_ownership_needed(
        &self,
        budget: &riffdb_types::exact_provider_budget_v1::ProviderBudget,
    ) -> Result<u64, riffdb_storage_api::StorageError> {
        match self {
            Self::Writable { owner } => owner.provider_ownership_needed(budget),
            Self::ReadOnly { owner } => owner.provider_ownership_needed(budget),
        }
    }

    pub(crate) fn prepare_provider_ownership_from(
        &self,
        work: &mut riffdb_types::exact_provider_budget_v1::Reservation,
        budget: &riffdb_types::exact_provider_budget_v1::ProviderBudget,
    ) -> Result<
        crate::engine_owner::PreparedEngineProviderOwnership<'_>,
        riffdb_storage_api::StorageError,
    > {
        match self {
            Self::Writable { owner } => owner.prepare_provider_ownership_from(work, budget),
            Self::ReadOnly { owner } => owner.prepare_provider_ownership_from(work, budget),
        }
    }
}

// This control contains only bounded accounting, never a root or engine. A
// tracking weak alias keeps it until after releasing the real Arc allocation.
struct RootAllocationControl {
    provider: riffdb_types::exact_provider_budget_v1::SharedPhysicalOwnershipCell,
}

pub(crate) const ROOT_ALLOCATION_CONTROL_BYTES: usize =
    std::mem::size_of::<RootAllocationControl>()
        + 4 * std::mem::size_of::<usize>()
        + std::mem::size_of::<riffdb_types::exact_provider_budget_v1::OwnershipCharge>()
        + 4 * std::mem::size_of::<usize>();

#[derive(Clone)]
pub(crate) struct TrackedRootWeak {
    // Release the actual residual allocation before its numerical owner.
    weak: Weak<CheckpointRoot>,
    _control: Arc<RootAllocationControl>,
}

impl TrackedRootWeak {
    pub(crate) fn upgrade(&self) -> Option<Arc<CheckpointRoot>> {
        self.weak.upgrade()
    }
    pub(crate) fn strong_count(&self) -> usize {
        self.weak.strong_count()
    }
    #[cfg(test)]
    pub(crate) fn ptr_eq(&self, other: &Weak<CheckpointRoot>) -> bool {
        self.weak.ptr_eq(other)
    }
}

/// One captured redb snapshot and the table handles already resolved from it.
///
/// The handles are declared before the snapshot so they drop first. Each holds
/// its own `Arc<TransactionGuard>` clone, so the snapshot would outlive them
/// either way; ordering it explicitly keeps the cache visibly subordinate to
/// the snapshot it was resolved from.
pub(crate) struct CheckpointRoot {
    identity: u64,
    meta: OnceLock<MetaTable>,
    capabilities: OnceLock<ByteTable>,
    journal_tables: [OnceLock<ByteTable>; JOURNAL_TABLE_SLOTS],
    snapshot_head: OnceLock<Option<CommitSequence>>,
    decoded_cache: OnceLock<SnapshotDecodedCache>,
    transaction: ReadTransaction,
    // Original retained extents remain resolvable after the live ports close.
    // A resolver holds no shared engine or later locator generations.
    history_resolver: Option<crate::history_stub::SnapshotHistoryResolverRef>,
    engine: Option<CheckpointEngineCustody>,
    // Cached handles, transaction and engine disappear before this owner. The
    // capability is last: its size-derived construction allowance covers the
    // plain control while strong, and tracking covers residual weak ownership.
    allocation_control: Arc<RootAllocationControl>,
    locator_capability: crate::locator_capability::LocatorCapability,
}

impl CheckpointRoot {
    pub(crate) fn downgrade_for_tracking(root: &Arc<Self>) -> TrackedRootWeak {
        TrackedRootWeak {
            weak: Arc::downgrade(root),
            _control: Arc::clone(&root.allocation_control),
        }
    }

    pub(crate) fn history_resolver_shared(
        &self,
    ) -> Option<&crate::history_stub::SnapshotHistoryResolverRef> {
        self.history_resolver.as_ref()
    }

    fn provider_metadata_bound(&self) -> Result<u64, riffdb_storage_api::StorageError> {
        // The existing maximum lazy-table allowance follows the one actual
        // immutable root, rather than being charged again for each source pin.
        let bytes = (32_usize * 1024)
            .checked_add(std::mem::size_of::<Self>())
            .and_then(|n| n.checked_add(4 * std::mem::size_of::<usize>()))
            .and_then(|n| n.checked_add(ROOT_ALLOCATION_CONTROL_BYTES))
            .and_then(|n| u64::try_from(n).ok())
            .ok_or_else(|| {
                crate::error::storage_error(riffdb_storage_api::StorageErrorKind::LimitExceeded)
            })?;
        Ok(bytes)
    }

    pub(crate) fn provider_metadata_needed(
        &self,
        budget: &riffdb_types::exact_provider_budget_v1::ProviderBudget,
    ) -> Result<u64, riffdb_storage_api::StorageError> {
        self.allocation_control
            .provider
            .provider_ownership_needed(self.provider_metadata_bound()?, budget)
            .map_err(|_| {
                crate::error::storage_error(riffdb_storage_api::StorageErrorKind::Unavailable)
            })
    }

    pub(crate) fn prepare_provider_metadata_from(
        &self,
        work: &mut riffdb_types::exact_provider_budget_v1::Reservation,
        budget: &riffdb_types::exact_provider_budget_v1::ProviderBudget,
    ) -> Result<
        riffdb_types::exact_provider_budget_v1::PreparedSharedPhysicalOwnership<'_>,
        riffdb_storage_api::StorageError,
    > {
        self.allocation_control
            .provider
            .prepare_provider_ownership_from(self.provider_metadata_bound()?, work, budget)
            .map_err(|_| {
                crate::error::storage_error(riffdb_storage_api::StorageErrorKind::Unavailable)
            })
    }

    #[cfg(test)]
    pub(crate) fn provider_metadata_charge_bytes(&self) -> u64 {
        self.allocation_control.provider.owned_bytes().unwrap()
    }

    #[cfg(test)]
    pub(crate) fn history_resolver_for_tests(
        &self,
    ) -> Option<&crate::history_stub::SnapshotHistoryResolverRef> {
        self.history_resolver.as_ref()
    }
    pub(crate) fn engine_custody(&self) -> Option<&CheckpointEngineCustody> {
        self.engine.as_ref()
    }
    /// Wraps one captured snapshot with an empty handle cache.
    pub(crate) fn new(
        transaction: ReadTransaction,
        identity: u64,
    ) -> Result<Self, riffdb_storage_api::StorageError> {
        let capability = crate::locator_capability::LocatorCapability::try_pending(
            crate::locator_memory::LocatorAllocationClass::Ordinary,
        )?;
        Self::new_with_locator_capability(transaction, identity, capability)
    }

    pub(crate) fn new_with_locator_capability(
        transaction: ReadTransaction,
        identity: u64,
        locator_capability: crate::locator_capability::LocatorCapability,
    ) -> Result<Self, riffdb_storage_api::StorageError> {
        bind_locator_source(&transaction, &locator_capability)?;
        Ok(Self {
            identity,
            locator_capability,
            meta: OnceLock::new(),
            capabilities: OnceLock::new(),
            journal_tables: std::array::from_fn(|_| OnceLock::new()),
            snapshot_head: OnceLock::new(),
            decoded_cache: OnceLock::new(),
            transaction,
            history_resolver: None,
            engine: None,
            // Native construction/class allowance was admitted by the source
            // capability before this actual control allocation is created.
            allocation_control: Arc::new(RootAllocationControl {
                provider: riffdb_types::exact_provider_budget_v1::SharedPhysicalOwnershipCell::new(
                ),
            }),
        })
    }

    pub(crate) fn retain_engine_and_history(
        mut self,
        engine: impl Into<CheckpointEngineCustody>,
        resolver: Option<crate::history_stub::SnapshotHistoryResolverRef>,
    ) -> Self {
        self.engine = Some(engine.into());
        self.history_resolver = resolver;
        self
    }

    pub(crate) fn resolve_command_segment_value<'a>(
        &self,
        bytes: &'a [u8],
    ) -> Result<std::borrow::Cow<'a, [u8]>, riffdb_storage_api::StorageError> {
        match &self.history_resolver {
            Some(resolver) => resolver.resolve_commits_value(bytes),
            None => {
                if crate::history_stub::HistoryStub::decode(bytes)?.is_some() {
                    return Err(crate::error::storage_error(
                        riffdb_storage_api::StorageErrorKind::Unavailable,
                    ));
                }
                Ok(std::borrow::Cow::Borrowed(bytes))
            }
        }
    }

    pub(crate) fn retained_history_custody_bound(&self) -> usize {
        self.history_resolver
            .as_ref()
            .map_or(0, |resolver| resolver.retained_charge_bound())
    }

    pub(crate) fn locator_capability(&self) -> &crate::locator_capability::LocatorCapability {
        &self.locator_capability
    }

    pub(crate) fn decoded_cache(&self) -> &SnapshotDecodedCache {
        self.decoded_cache
            .get_or_init(SnapshotDecodedCache::default)
    }

    pub(crate) const fn identity(&self) -> u64 {
        self.identity
    }

    /// Returns an exclusively owned immutable pin after its bounded walk.
    /// Cached handles are dropped; no newer transaction is substituted.
    pub(crate) fn into_transaction(self) -> ReadTransaction {
        self.transaction
    }

    /// Provider copying may use only an already validated frontier on this
    /// immutable root; it cannot start an unadmitted metadata decode.
    pub(crate) fn cached_provider_snapshot_head_v1(&self) -> Option<Option<CommitSequence>> {
        self.snapshot_head.get().copied()
    }

    /// Resolves the snapshot-visible application frontier once per snapshot.
    ///
    /// `derive` is the complete uncached derivation, authority-presence check
    /// included. Every input it reads — the application-sequence allocator row,
    /// whether `COMMITS` is empty, and the retention watermark — is fixed for
    /// this snapshot's whole life, so deriving once and reusing the answer is
    /// exactly equivalent to deriving it per access. No check is skipped, made
    /// conditional, or weakened: the check still runs, against the same
    /// snapshot, and still decides whether this frontier may be served at all.
    ///
    /// A failure is not cached, for the same reason a failed table open is not:
    /// the next access re-derives it and observes the same error, so a corrupt
    /// or absent allocator can never be converted into a frontier.
    pub(crate) fn snapshot_head<E>(
        &self,
        derive: impl FnOnce() -> Result<Option<CommitSequence>, E>,
    ) -> Result<Option<CommitSequence>, E> {
        derive_once(&self.snapshot_head, derive)
    }

    /// The captured snapshot itself, for reads of tables outside the cache.
    pub(crate) fn transaction(&self) -> &ReadTransaction {
        &self.transaction
    }

    /// Resolves the metadata table once per snapshot.
    pub(crate) fn meta_table(&self) -> Result<&MetaTable, TableError> {
        resolve(&self.meta, &self.transaction, META)
    }

    /// Resolves the capability table once per snapshot.
    pub(crate) fn capability_table(&self) -> Result<&ByteTable, TableError> {
        resolve(&self.capabilities, &self.transaction, CAPABILITIES)
    }

    /// Resolves one byte-keyed journal table once per snapshot.
    ///
    /// Returns `None` for [`JournalTable::Meta`], which is string-keyed and is
    /// reached through [`Self::meta_table`] instead. That mirrors
    /// [`crate::journal::byte_table_definition`] exactly.
    pub(crate) fn journal_byte_table(
        &self,
        table: JournalTable,
    ) -> Option<Result<&ByteTable, TableError>> {
        let definition = crate::journal::byte_table_definition(table)?;
        let slot = self
            .journal_tables
            .get(journal_table_slot(table))
            .expect("every journal table discriminant has a cache slot");
        Some(resolve(slot, &self.transaction, definition))
    }

    /// Reads one value through the cached handle for `table`.
    ///
    /// Byte-for-byte the same result as [`crate::journal::read_value`] against
    /// the same snapshot, including every error mapping: only a genuinely
    /// absent B-tree row yields `Ok(None)`.
    pub(crate) fn read_value(
        &self,
        table: JournalTable,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, JournalIoError> {
        if crate::query_diagnostics::query_execute_diagnostics_enabled()
            && crate::journal::byte_table_definition(table).is_some()
        {
            return self.read_value_profiled(table, key);
        }
        self.read_value_inner(table, key)
    }

    /// Splits one cached point read into the same `open_table`, `get` and
    /// value-copy segments [`crate::journal::read_value`] charges, so the
    /// census stays comparable across the caching change: the first segment
    /// now measures the cache lookup that replaced the name resolution.
    fn read_value_profiled(
        &self,
        table: JournalTable,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, JournalIoError> {
        let started = Instant::now();
        let opened = self
            .journal_byte_table(table)
            .expect("caller proved a byte-table definition")
            .map_err(|_| JournalIoError::Corrupt)?;
        crate::journal::charge_point_read_substage(0, started);
        let started = Instant::now();
        let found = opened.get(key).map_err(|_| JournalIoError::Corrupt)?;
        crate::journal::charge_point_read_substage(1, started);
        let started = Instant::now();
        let value = found.map(|value| value.value().to_vec());
        crate::journal::charge_point_read_substage(2, started);
        Ok(value)
    }

    fn read_value_inner(
        &self,
        table: JournalTable,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, JournalIoError> {
        let Some(opened) = self.journal_byte_table(table) else {
            let key = std::str::from_utf8(key).map_err(|_| JournalIoError::Corrupt)?;
            return self
                .meta_table()
                .map_err(|_| JournalIoError::Corrupt)?
                .get(key)
                .map_err(|_| JournalIoError::Corrupt)
                .map(|value| value.map(|value| value.value().to_vec()));
        };
        opened
            .map_err(|_| JournalIoError::Corrupt)?
            .get(key)
            .map_err(|_| JournalIoError::Corrupt)
            .map(|value| value.map(|value| value.value().to_vec()))
    }
}

pub(crate) fn bind_locator_source(
    transaction: &ReadTransaction,
    capability: &crate::locator_capability::LocatorCapability,
) -> Result<(), riffdb_storage_api::StorageError> {
    use crate::codec::{
        decode_database_identity_v1, decode_history_incarnation_v1, decode_record_registry_digest,
    };
    use crate::error::{precommit_storage_error, storage_error, table_error};
    use crate::layout::{META_DATABASE_ID, META_HISTORY_INCARNATION, META_RECORD_REGISTRY};
    let metadata = transaction.open_table(META).map_err(table_error)?;
    let missing = || storage_error(riffdb_storage_api::StorageErrorKind::CorruptData);
    let database = metadata
        .get(META_DATABASE_ID)
        .map_err(precommit_storage_error)?
        .ok_or_else(missing)?;
    let history = metadata
        .get(META_HISTORY_INCARNATION)
        .map_err(precommit_storage_error)?
        .ok_or_else(missing)?;
    let registry = metadata
        .get(META_RECORD_REGISTRY)
        .map_err(precommit_storage_error)?
        .ok_or_else(missing)?;
    capability.bind_source(
        *decode_database_identity_v1(database.value())?.value(),
        *decode_history_incarnation_v1(history.value())?.value(),
        *decode_record_registry_digest(registry.value())?.value(),
    )
}

impl Deref for CheckpointRoot {
    type Target = ReadTransaction;

    fn deref(&self) -> &Self::Target {
        &self.transaction
    }
}

/// Derives one value against an immutable read view exactly once.
///
/// Shared by every such cache so they cannot drift apart on the property that
/// makes them sound: a failure is never stored. A derivation that fails is
/// re-attempted on the next access and observes the same error, so no decode,
/// lookup, or verification failure is ever converted into a value.
///
/// A lost initialisation race drops this thread's answer and uses the winner's.
/// Both were derived against the same immutable view, so they are equal.
pub(crate) fn derive_once<T: Copy, E>(
    cell: &OnceLock<T>,
    derive: impl FnOnce() -> Result<T, E>,
) -> Result<T, E> {
    if let Some(value) = cell.get() {
        return Ok(*value);
    }
    let value = derive()?;
    Ok(*cell.get_or_init(|| value))
}

/// Cache slot for one journal table, derived from its stable discriminant.
const fn journal_table_slot(table: JournalTable) -> usize {
    (table as u8 as usize).saturating_sub(1)
}

/// Returns the cached handle, opening and storing it on first use.
///
/// A lost initialisation race simply drops this thread's handle and uses the
/// winner's. Both were resolved from the same snapshot, so they are
/// interchangeable. An open failure is returned without being stored, so the
/// next access retries and observes the same error it would today.
fn resolve<'cache, K, V>(
    cell: &'cache OnceLock<ReadOnlyTable<K, V>>,
    transaction: &ReadTransaction,
    definition: TableDefinition<'static, K, V>,
) -> Result<&'cache ReadOnlyTable<K, V>, TableError>
where
    K: redb::Key + 'static,
    V: redb::Value + 'static,
{
    if let Some(table) = cell.get() {
        return Ok(table);
    }
    let opened = transaction.open_table(definition)?;
    Ok(cell.get_or_init(|| opened))
}

/// A cached root is shared across reader threads by `Arc`, so the compiler must
/// agree that a resolved handle is safe to hold and read from any of them. That
/// is the whole basis for resolving once and reading many times, so it is
/// asserted here rather than left to the first caller that happens to need it.
const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<CheckpointRoot>();
    assert_send_sync::<ByteTable>();
    assert_send_sync::<MetaTable>();
};

#[cfg(test)]
mod tests {
    use super::*;
    use redb::ReadableDatabase;

    // req: TXN-042, STO-032
    #[test]
    fn readonly_checkpoint_root_retains_backend_until_its_final_clone_releases() {
        let directory = crate::test_path::ScopedDirectory::new("readonly-root-custody");
        let path = directory.join("source.redb");
        let database = redb::Database::create(&path).unwrap();
        let identity =
            riffdb_types::DatabaseId::from_unix_milliseconds_and_random(1, [11; 10]).unwrap();
        let encoded_identity = crate::codec::encode_database_identity_v1(identity).unwrap();
        let write = database.begin_write().unwrap();
        {
            let mut metadata = write.open_table(META).unwrap();
            metadata
                .insert(crate::layout::META_DATABASE_ID, encoded_identity.as_bytes())
                .unwrap();
            let incarnation = crate::codec::encode_history_incarnation_v1(1).unwrap();
            metadata
                .insert(
                    crate::layout::META_HISTORY_INCARNATION,
                    incarnation.as_bytes(),
                )
                .unwrap();
            let registry = crate::codec::encode_record_registry_v2(
                riffdb_types::SchemaHash::from_bytes([3; 32]),
            )
            .unwrap();
            metadata
                .insert(crate::layout::META_RECORD_REGISTRY, registry.as_bytes())
                .unwrap();
        }
        write.commit().unwrap();
        drop(database);

        let readonly = crate::engine_owner::EngineOwner::try_new(
            redb::Database::builder()
                .set_cache_size(crate::store::PRIMARY_REDB_CACHE_SIZE_BYTES)
                .open_read_only(&path)
                .unwrap(),
            crate::store::PRIMARY_REDB_CACHE_SIZE_BYTES,
            crate::locator_memory::LocatorAllocationClass::Ordinary,
        )
        .unwrap();
        let root = std::sync::Arc::new(
            CheckpointRoot::new(readonly.begin_read().unwrap(), 1)
                .unwrap()
                .retain_engine_and_history(std::sync::Arc::clone(&readonly), None),
        );
        let last = std::sync::Arc::clone(&root);
        drop(readonly);
        assert!(
            redb::Database::open(&path).is_err(),
            "a retained source excludes a writer"
        );
        assert_eq!(
            root.meta_table()
                .unwrap()
                .get(crate::layout::META_DATABASE_ID)
                .unwrap()
                .unwrap()
                .value(),
            encoded_identity.as_bytes()
        );
        drop(root);
        assert!(
            redb::Database::open(&path).is_err(),
            "the final source clone still owns the backend"
        );
        assert_eq!(
            last.meta_table()
                .unwrap()
                .get(crate::layout::META_DATABASE_ID)
                .unwrap()
                .unwrap()
                .value(),
            encoded_identity.as_bytes()
        );
        drop(last);
        drop(redb::Database::open(&path).unwrap());
    }

    /// `RedbReadAccess::Current` allocates one of these per read access, so its
    /// size is a per-read cost rather than a one-off. The bound is loose enough
    /// not to fail on a redb layout change and tight enough to catch a field
    /// that turns a cache into a bulk copy.
    #[test]
    fn the_cache_stays_small_enough_to_allocate_per_read_access() {
        let size = std::mem::size_of::<CheckpointRoot>();
        assert!(
            size <= 8 * 1024,
            "CheckpointRoot grew to {size} bytes and is built once per read access"
        );
    }

    #[test]
    fn every_journal_table_has_its_own_cache_slot() {
        let mut slots: Vec<usize> = JournalTable::ALL
            .iter()
            .copied()
            .map(journal_table_slot)
            .collect();
        slots.sort_unstable();
        slots.dedup();
        assert_eq!(
            slots.len(),
            JournalTable::ALL.len(),
            "two journal tables would otherwise share one cached handle"
        );
        assert!(
            slots.iter().all(|slot| *slot < JOURNAL_TABLE_SLOTS),
            "a journal table would otherwise index past the cache"
        );
    }

    #[test]
    fn meta_is_the_only_table_without_a_byte_definition() {
        for table in JournalTable::ALL {
            assert_eq!(
                crate::journal::byte_table_definition(table).is_none(),
                table == JournalTable::Meta,
                "{table:?} disagrees with the meta special case in `read_value`"
            );
        }
    }
}
