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
//!   `durable_commit_epoch` is unchanged — so the cache spans those accesses
//!   too. A daemon that has not written since startup has neither a durable
//!   frontier nor a composite publication, so this is the only shape its reads
//!   take.
//!
//! The same equivalence licenses caching a derived *value* rather than a
//! handle, provided every input it reads is fixed by the snapshot. The
//! snapshot-visible application frontier is the one such value cached here.

use std::ops::Deref;
use std::sync::OnceLock;
use std::time::Instant;

use redb::{ReadOnlyTable, ReadTransaction, TableDefinition, TableError};
use riffdb_types::CommitSequence;

use crate::journal::{JournalIoError, JournalTable};
use crate::layout::{CAPABILITIES, META};

/// Byte-keyed operational table handle.
type ByteTable = ReadOnlyTable<&'static [u8], &'static [u8]>;
/// String-keyed metadata table handle.
type MetaTable = ReadOnlyTable<&'static str, &'static [u8]>;

/// Cache slots for byte-keyed journal tables, indexed by discriminant minus one.
///
/// [`JournalTable::Meta`] occupies slot zero and is never stored there: its key
/// type differs, so it has its own field.
const JOURNAL_TABLE_SLOTS: usize = JournalTable::ALL.len();

/// One captured redb snapshot and the table handles already resolved from it.
///
/// The handles are declared before the snapshot so they drop first. Each holds
/// its own `Arc<TransactionGuard>` clone, so the snapshot would outlive them
/// either way; ordering it explicitly keeps the cache visibly subordinate to
/// the snapshot it was resolved from.
pub(crate) struct CheckpointRoot {
    meta: OnceLock<MetaTable>,
    capabilities: OnceLock<ByteTable>,
    journal_tables: [OnceLock<ByteTable>; JOURNAL_TABLE_SLOTS],
    snapshot_head: OnceLock<Option<CommitSequence>>,
    transaction: ReadTransaction,
}

impl CheckpointRoot {
    /// Wraps one captured snapshot with an empty handle cache.
    pub(crate) fn new(transaction: ReadTransaction) -> Self {
        Self {
            meta: OnceLock::new(),
            capabilities: OnceLock::new(),
            journal_tables: std::array::from_fn(|_| OnceLock::new()),
            snapshot_head: OnceLock::new(),
            transaction,
        }
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
        if crate::query::query_execute_diagnostics_enabled()
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
