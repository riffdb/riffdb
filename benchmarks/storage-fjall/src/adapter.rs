//! Bounded Fjall substrate adapter used only by the comparison workspace.

use std::collections::HashSet;
use std::fmt;
use std::ops::Bound;
use std::path::Path;

use fjall::{
    KeyspaceCreateOptions, PersistMode, Readable, SingleWriterTxDatabase, SingleWriterTxKeyspace,
};
use riffdb_storage_api::{OpenSessionId, StartupIndexMigrationPort};
use riffdb_types::DatabaseId;

const MAX_KEY_BYTES: usize = 4 * 1024;
const MAX_VALUE_BYTES: usize = 16 * 1024 * 1024;
const MAX_BATCH_MUTATIONS: usize = 4_096;
const MAX_BATCH_BYTES: usize = 16 * 1024 * 1024;
const MAX_PAGE_ROWS: usize = 500;
const MAX_PAGE_BYTES: usize = 4 * 1024 * 1024;

/// Closed RiffDB physical table inventory used by the comparison substrate.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(u8)]
pub enum RiffdbTable {
    /// Operational metadata.
    Meta = 0,
    /// Immutable contract bundles.
    ContractBundles,
    /// Active catalog pointer.
    CatalogActive,
    /// Authoritative entity rows.
    Entities,
    /// Authoritative secondary-index rows.
    SecondaryIndexes,
    /// Index validation epochs.
    IndexEpochs,
    /// Terminal idempotency outcomes.
    Idempotency,
    /// Pending idempotency admissions.
    IdempotencyPending,
    /// Authoritative application commits.
    Commits,
    /// Immutable provenance records.
    Provenance,
    /// Authoritative durable events.
    Events,
    /// Authoritative outbox intents.
    Outbox,
    /// Derived outbox delivery status.
    OutboxStatus,
    /// Derived projection group state.
    ProjectionState,
    /// Derived projection control/frontier state.
    ProjectionFrontier,
    /// Derived projection apply markers.
    ProjectionApplied,
    /// Capability records.
    Capabilities,
    /// Capability digest lookups.
    CapabilityTokens,
    /// Ordered administration audit records.
    Audit,
}

impl RiffdbTable {
    /// Exact accepted table inventory in stable physical order.
    pub const ALL: [Self; 19] = [
        Self::Meta,
        Self::ContractBundles,
        Self::CatalogActive,
        Self::Entities,
        Self::SecondaryIndexes,
        Self::IndexEpochs,
        Self::Idempotency,
        Self::IdempotencyPending,
        Self::Commits,
        Self::Provenance,
        Self::Events,
        Self::Outbox,
        Self::OutboxStatus,
        Self::ProjectionState,
        Self::ProjectionFrontier,
        Self::ProjectionApplied,
        Self::Capabilities,
        Self::CapabilityTokens,
        Self::Audit,
    ];

    /// Returns the accepted physical table name.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Meta => "meta",
            Self::ContractBundles => "contract_bundles",
            Self::CatalogActive => "catalog_active",
            Self::Entities => "entities",
            Self::SecondaryIndexes => "secondary_indexes",
            Self::IndexEpochs => "index_epochs",
            Self::Idempotency => "idempotency",
            Self::IdempotencyPending => "idempotency_pending",
            Self::Commits => "commits",
            Self::Provenance => "provenance",
            Self::Events => "events",
            Self::Outbox => "outbox",
            Self::OutboxStatus => "outbox_status",
            Self::ProjectionState => "projection_state",
            Self::ProjectionFrontier => "projection_frontier",
            Self::ProjectionApplied => "projection_applied",
            Self::Capabilities => "capabilities",
            Self::CapabilityTokens => "capability_tokens",
            Self::Audit => "audit",
        }
    }

    const fn index(self) -> usize {
        self as usize
    }
}

/// Durability used by one comparison transaction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ComparisonDurability {
    /// Commit and synchronize data plus metadata before returning.
    Sync,
    /// Commit to the process journal buffer; a later group flush is required.
    Buffered,
}

/// One bounded, canonically ordered physical comparison mutation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ComparisonMutation {
    table: RiffdbTable,
    key: Vec<u8>,
    value: Option<Vec<u8>>,
}

impl ComparisonMutation {
    /// Creates one put mutation.
    pub fn put(
        table: RiffdbTable,
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
    ) -> Result<Self, AdapterError> {
        Self::new(table, key.into(), Some(value.into()))
    }

    /// Creates one delete mutation.
    pub fn delete(table: RiffdbTable, key: impl Into<Vec<u8>>) -> Result<Self, AdapterError> {
        Self::new(table, key.into(), None)
    }

    fn new(table: RiffdbTable, key: Vec<u8>, value: Option<Vec<u8>>) -> Result<Self, AdapterError> {
        if key.is_empty() || key.len() > MAX_KEY_BYTES {
            return Err(AdapterError::new(AdapterErrorKind::BoundExceeded));
        }
        if value
            .as_ref()
            .is_some_and(|bytes| bytes.len() > MAX_VALUE_BYTES)
        {
            return Err(AdapterError::new(AdapterErrorKind::BoundExceeded));
        }
        Ok(Self { table, key, value })
    }

    fn order_key(&self) -> (RiffdbTable, &[u8]) {
        (self.table, &self.key)
    }

    fn charge(&self) -> Result<usize, AdapterError> {
        self.key
            .len()
            .checked_add(self.value.as_ref().map_or(0, Vec::len))
            .ok_or_else(|| AdapterError::new(AdapterErrorKind::BoundExceeded))
    }
}

/// Closed safe classification for the non-production adapter.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdapterErrorKind {
    /// Fjall could not complete the requested physical operation.
    Engine,
    /// A comparison hard bound was exceeded.
    BoundExceeded,
    /// The mutation or pagination request was noncanonical.
    NonCanonical,
}

/// Redacted comparison-adapter error.
pub struct AdapterError {
    kind: AdapterErrorKind,
    source: Option<fjall::Error>,
}

impl AdapterError {
    const fn new(kind: AdapterErrorKind) -> Self {
        Self { kind, source: None }
    }

    fn engine(source: fjall::Error) -> Self {
        Self {
            kind: AdapterErrorKind::Engine,
            source: Some(source),
        }
    }

    /// Returns the closed safe classification.
    #[must_use]
    pub const fn kind(&self) -> AdapterErrorKind {
        self.kind
    }
}

impl fmt::Debug for AdapterError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdapterError")
            .field("kind", &self.kind)
            .field("source", &self.source.as_ref().map(|_| "[REDACTED]"))
            .finish()
    }
}

impl fmt::Display for AdapterError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self.kind {
            AdapterErrorKind::Engine => "Fjall comparison operation failed",
            AdapterErrorKind::BoundExceeded => "comparison bound exceeded",
            AdapterErrorKind::NonCanonical => "comparison request is noncanonical",
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for AdapterError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_ref()
            .map(|source| source as &(dyn std::error::Error + 'static))
    }
}

impl From<fjall::Error> for AdapterError {
    fn from(value: fjall::Error) -> Self {
        Self::engine(value)
    }
}

/// Isolated physical Fjall substrate.
///
/// This type intentionally implements none of RiffDB's semantic persistence
/// traits. That missing surface is frozen as an explicit conformance failure.
pub struct FjallComparisonStore {
    database: SingleWriterTxDatabase,
    tables: Vec<SingleWriterTxKeyspace>,
}

/// Inaccessible identity-only probe for the accepted migration port shape.
///
/// It deliberately has no public constructor and no scan, bundle, apply, end,
/// or finish operation. A future conforming adapter must produce its real port
/// only by consuming an exclusive structural-evidence session.
pub struct FjallMigrationIdentityProbe {
    database_id: DatabaseId,
    open_session_id: OpenSessionId,
}

impl FjallMigrationIdentityProbe {
    #[cfg(test)]
    fn new(database_id: DatabaseId, open_session_id: OpenSessionId) -> Self {
        Self {
            database_id,
            open_session_id,
        }
    }
}

impl StartupIndexMigrationPort for FjallMigrationIdentityProbe {
    fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    fn open_session_id(&self) -> OpenSessionId {
        self.open_session_id
    }
}

impl FjallComparisonStore {
    /// Opens or creates the comparison database and its exact table inventory.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, AdapterError> {
        let database = SingleWriterTxDatabase::builder(path)
            .manual_journal_persist(true)
            .open()
            .map_err(AdapterError::engine)?;
        let mut tables = Vec::with_capacity(RiffdbTable::ALL.len());
        for table in RiffdbTable::ALL {
            tables.push(
                database
                    .keyspace(table.name(), KeyspaceCreateOptions::default)
                    .map_err(AdapterError::engine)?,
            );
        }
        Ok(Self { database, tables })
    }

    /// Atomically applies one bounded canonical batch across accepted tables.
    pub fn apply(
        &self,
        mutations: &[ComparisonMutation],
        durability: ComparisonDurability,
    ) -> Result<(), AdapterError> {
        validate_batch(mutations)?;
        let persist_mode = match durability {
            ComparisonDurability::Sync => PersistMode::SyncAll,
            ComparisonDurability::Buffered => PersistMode::Buffer,
        };
        let mut transaction = self.database.write_tx().durability(Some(persist_mode));
        for mutation in mutations {
            let keyspace = self.table(mutation.table);
            match &mutation.value {
                Some(value) => transaction.insert(keyspace, mutation.key.clone(), value.clone()),
                None => transaction.remove(keyspace, mutation.key.clone()),
            }
        }
        transaction.commit().map_err(AdapterError::engine)
    }

    /// Synchronizes all prior buffered comparison commits as one group flush.
    pub fn flush_group(&self) -> Result<(), AdapterError> {
        self.database
            .persist(PersistMode::SyncAll)
            .map_err(AdapterError::engine)
    }

    /// Opens one owned Fjall read snapshot.
    #[must_use]
    pub fn snapshot(&self) -> ComparisonSnapshot {
        ComparisonSnapshot {
            snapshot: self.database.read_tx(),
            tables: self.tables.clone(),
        }
    }

    /// Returns Fjall's current on-disk byte count for this comparison database.
    pub fn disk_space_bytes(&self) -> Result<u64, AdapterError> {
        self.database.disk_space().map_err(AdapterError::engine)
    }

    /// Returns the exact accepted table names observed by the engine.
    #[must_use]
    pub fn table_names(&self) -> Vec<String> {
        let mut names = self
            .database
            .list_keyspace_names()
            .into_iter()
            .map(|name| name.to_string())
            .collect::<Vec<_>>();
        names.sort();
        names
    }

    fn table(&self, table: RiffdbTable) -> &SingleWriterTxKeyspace {
        &self.tables[table.index()]
    }
}

/// One owned engine snapshot used only for bounded substrate probes.
pub struct ComparisonSnapshot {
    snapshot: fjall::Snapshot,
    tables: Vec<SingleWriterTxKeyspace>,
}

impl ComparisonSnapshot {
    /// Reads one exact key from this snapshot.
    pub fn get(&self, table: RiffdbTable, key: &[u8]) -> Result<Option<Vec<u8>>, AdapterError> {
        validate_key(key)?;
        self.snapshot
            .get(&self.tables[table.index()], key)
            .map(|value| value.map(|bytes| bytes.to_vec()))
            .map_err(AdapterError::engine)
    }

    /// Reads one bounded physical-order page strictly after `after`.
    pub fn scan(
        &self,
        table: RiffdbTable,
        after: Option<&[u8]>,
        row_limit: usize,
        byte_limit: usize,
    ) -> Result<ComparisonPage, AdapterError> {
        if row_limit == 0
            || row_limit > MAX_PAGE_ROWS
            || byte_limit == 0
            || byte_limit > MAX_PAGE_BYTES
        {
            return Err(AdapterError::new(AdapterErrorKind::BoundExceeded));
        }
        if let Some(key) = after {
            validate_key(key)?;
        }

        let lower = after.map_or(Bound::Unbounded, Bound::Excluded);
        let iterator = self.snapshot.range::<&[u8], _>(
            &self.tables[table.index()],
            (lower, Bound::<&[u8]>::Unbounded),
        );
        let mut rows = Vec::with_capacity(row_limit);
        let mut bytes = 0usize;
        let mut exact_end = true;

        for item in iterator {
            let (key, value) = item.into_inner().map_err(AdapterError::engine)?;
            let item_bytes = key
                .len()
                .checked_add(value.len())
                .ok_or_else(|| AdapterError::new(AdapterErrorKind::BoundExceeded))?;
            if rows.len() == row_limit
                || bytes
                    .checked_add(item_bytes)
                    .is_none_or(|next| next > byte_limit)
            {
                exact_end = false;
                break;
            }
            bytes += item_bytes;
            rows.push((key.to_vec(), value.to_vec()));
        }

        if rows.is_empty() && !exact_end {
            return Err(AdapterError::new(AdapterErrorKind::BoundExceeded));
        }
        let continuation = (!exact_end)
            .then(|| rows.last().map(|(key, _)| key.clone()))
            .flatten();
        Ok(ComparisonPage {
            rows,
            encoded_content_bytes: bytes,
            continuation,
            exact_end,
        })
    }
}

/// One bounded physical comparison page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ComparisonPage {
    rows: Vec<(Vec<u8>, Vec<u8>)>,
    encoded_content_bytes: usize,
    continuation: Option<Vec<u8>>,
    exact_end: bool,
}

impl ComparisonPage {
    /// Borrows the returned rows in physical key order.
    #[must_use]
    pub fn rows(&self) -> &[(Vec<u8>, Vec<u8>)] {
        &self.rows
    }

    /// Returns the charged key-plus-value bytes.
    #[must_use]
    pub const fn encoded_content_bytes(&self) -> usize {
        self.encoded_content_bytes
    }

    /// Returns the exclusive lower continuation when another page exists.
    #[must_use]
    pub fn continuation(&self) -> Option<&[u8]> {
        self.continuation.as_deref()
    }

    /// Returns whether the physical range reached exact end.
    #[must_use]
    pub const fn exact_end(&self) -> bool {
        self.exact_end
    }
}

fn validate_key(key: &[u8]) -> Result<(), AdapterError> {
    if key.is_empty() || key.len() > MAX_KEY_BYTES {
        Err(AdapterError::new(AdapterErrorKind::BoundExceeded))
    } else {
        Ok(())
    }
}

fn validate_batch(mutations: &[ComparisonMutation]) -> Result<(), AdapterError> {
    if mutations.is_empty() || mutations.len() > MAX_BATCH_MUTATIONS {
        return Err(AdapterError::new(AdapterErrorKind::BoundExceeded));
    }
    let mut charge = 0usize;
    let mut prior: Option<(RiffdbTable, &[u8])> = None;
    let mut seen = HashSet::with_capacity(mutations.len());
    for mutation in mutations {
        let order = mutation.order_key();
        if prior.is_some_and(|previous| previous >= order)
            || !seen.insert((mutation.table, mutation.key.as_slice()))
        {
            return Err(AdapterError::new(AdapterErrorKind::NonCanonical));
        }
        charge = charge
            .checked_add(mutation.charge()?)
            .ok_or_else(|| AdapterError::new(AdapterErrorKind::BoundExceeded))?;
        if charge > MAX_BATCH_BYTES {
            return Err(AdapterError::new(AdapterErrorKind::BoundExceeded));
        }
        prior = Some(order);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_inventory_matches_the_accepted_physical_names() {
        assert_eq!(RiffdbTable::ALL.len(), 19);
        assert_eq!(RiffdbTable::Meta.name(), "meta");
        assert_eq!(RiffdbTable::ProjectionApplied.name(), "projection_applied");
        assert_eq!(RiffdbTable::Audit.name(), "audit");
    }

    #[test]
    fn batch_requires_strict_canonical_target_order() {
        let first = ComparisonMutation::put(RiffdbTable::Meta, b"b", b"1")
            .expect("bounded comparison mutation");
        let second = ComparisonMutation::put(RiffdbTable::Meta, b"a", b"2")
            .expect("bounded comparison mutation");
        assert_eq!(
            validate_batch(&[first, second])
                .expect_err("decreasing targets must reject")
                .kind(),
            AdapterErrorKind::NonCanonical
        );
    }

    #[test]
    fn accepted_startup_migration_contract_is_identity_only() {
        let database_id = DatabaseId::from_unix_milliseconds_and_random(1, [1; 10])
            .expect("valid UUIDv7 fixture");
        let session = OpenSessionId::new(1).expect("nonzero session");
        let probe = FjallMigrationIdentityProbe::new(database_id, session);
        assert_eq!(probe.database_id(), database_id);
        assert_eq!(probe.open_session_id(), session);
    }
}
