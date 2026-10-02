//! Durable local destination with immutable records and atomic byte accounting.

use std::error::Error;
use std::fmt;
use std::num::NonZeroU64;
use std::ops::Bound;
use std::path::Path;

use crate::local_archive_custody::ArchiveCustody;
use redb::{
    Database, Durability, ReadableDatabase, ReadableTableMetadata, TableDefinition, TableHandle,
};
use riffdb_storage_api::{
    LocalOutboxArchiveRecordV1, decode_database_identity_v1, decode_local_outbox_archive_record_v1,
    encode_database_identity_v1, encode_local_outbox_archive_record_v1,
    local_outbox_archive_schema_hash_v1,
};
use riffdb_types::DatabaseId;

const RECORDS: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("local_outbox_archive_records_v1");
const META: TableDefinition<u8, &[u8]> = TableDefinition::new("local_outbox_archive_metadata_v1");
const COUNTERS: TableDefinition<u8, u64> = TableDefinition::new("local_outbox_archive_counters_v1");
const VALIDATION_ROWS: usize = 256;
const VALIDATION_BYTES: usize = 4 * 1024 * 1024;
const CACHE_BYTES: usize = 8 * 1024 * 1024;

/// Closed destination errors, with no path, event, or engine source text.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LocalArchiveError {
    /// Directory/file custody does not meet the protected ownership rule.
    UnsafePath,
    /// This implementation requires the accepted Linux custody primitives.
    UnsupportedPlatform,
    /// Existing archive belongs to a different database.
    Binding,
    /// A new encoded row would exceed the restart-applied quota.
    Quota,
    /// Format, identity, canonical content, or accounting is inconsistent.
    Integrity,
    /// Destination is unavailable or complete bounded validation is pending.
    Unavailable,
}
impl fmt::Display for LocalArchiveError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::UnsafePath => "local outbox archive custody is unsafe",
            Self::UnsupportedPlatform => "local outbox archive platform is unsupported",
            Self::Binding => "local outbox archive database binding differs",
            Self::Quota => "local outbox archive quota is exhausted",
            Self::Integrity => "local outbox archive integrity validation failed",
            Self::Unavailable => "local outbox archive is unavailable",
        })
    }
}
impl Error for LocalArchiveError {}

/// Exact durable accounting, excluding separately observed engine overhead.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LocalArchiveObservation {
    records: u64,
    encoded_bytes: u64,
}
impl LocalArchiveObservation {
    /// Immutable record count across all retained history incarnations.
    #[must_use]
    pub const fn records(self) -> u64 {
        self.records
    }
    /// Canonical key and complete envelope bytes charged to the quota.
    #[must_use]
    pub const fn encoded_bytes(self) -> u64 {
        self.encoded_bytes
    }
}

#[derive(Default)]
struct Validation {
    after: Option<[u8; 36]>,
    rows: u64,
    bytes: u64,
    ready: bool,
}

/// Sole durable owner of one private external destination archive.
pub struct DurableLocalArchive {
    database: Database,
    custody: ArchiveCustody,
    binding: DatabaseId,
    quota: NonZeroU64,
    validation: Validation,
    hooks: crate::local_archive_hooks::Hooks,
}
impl fmt::Debug for DurableLocalArchive {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("DurableLocalArchive([REDACTED])")
    }
}

impl DurableLocalArchive {
    /// Opens a private existing directory and recovers its sole archive engine.
    /// Empty engines initialize atomically; every other layout must match V1.
    /// Reopened records require bounded validation before new acceptance.
    pub fn open(
        path: &Path,
        binding: DatabaseId,
        quota: NonZeroU64,
    ) -> Result<Self, LocalArchiveError> {
        Self::open_inner(
            path,
            binding,
            quota,
            true,
            crate::local_archive_hooks::Hooks::default(),
        )
    }

    /// Opens an existing destination for offline operator inspection. This
    /// refuses absent/uninitialized archives and never creates a directory or record.
    pub fn open_existing(path: &Path, binding: DatabaseId) -> Result<Self, LocalArchiveError> {
        Self::open_inner(
            path,
            binding,
            NonZeroU64::MAX,
            false,
            crate::local_archive_hooks::Hooks::default(),
        )
    }

    /// Installs test-only deterministic process-kill boundaries.
    #[cfg(feature = "test-fixtures")]
    pub fn open_with_crash_hooks(
        path: &Path,
        binding: DatabaseId,
        quota: NonZeroU64,
        hook: std::sync::Arc<dyn crate::LocalArchiveCrashHooks>,
    ) -> Result<Self, LocalArchiveError> {
        Self::open_inner(
            path,
            binding,
            quota,
            true,
            crate::local_archive_hooks::Hooks { inner: Some(hook) },
        )
    }

    fn open_inner(
        path: &Path,
        binding: DatabaseId,
        quota: NonZeroU64,
        create: bool,
        hooks: crate::local_archive_hooks::Hooks,
    ) -> Result<Self, LocalArchiveError> {
        let custody = ArchiveCustody::open(path, create)?;
        let database = Database::builder()
            .set_cache_size(CACHE_BYTES)
            .create_file(custody.engine_file()?)
            .map_err(unavailable)?;
        let names = database
            .begin_read()
            .map_err(unavailable)?
            .list_tables()
            .map_err(integrity)?
            .take(4)
            .map(|entry| entry.name().to_owned())
            .collect::<Vec<_>>();
        if names.is_empty() {
            if !create {
                return Err(LocalArchiveError::Integrity);
            }
            let mut write = database.begin_write().map_err(unavailable)?;
            immediate(&mut write)?;
            write.open_table(RECORDS).map_err(integrity)?;
            {
                let mut meta = write.open_table(META).map_err(integrity)?;
                meta.insert(
                    0,
                    encode_database_identity_v1(binding)
                        .map_err(integrity)?
                        .as_bytes(),
                )
                .map_err(unavailable)?;
                meta.insert(
                    1,
                    local_outbox_archive_schema_hash_v1().as_bytes().as_slice(),
                )
                .map_err(unavailable)?;
            }
            {
                let mut counters = write.open_table(COUNTERS).map_err(integrity)?;
                counters.insert(0, 0).map_err(unavailable)?;
                counters.insert(1, 0).map_err(unavailable)?;
            }
            hooks.initialization_before_commit();
            write.commit().map_err(unavailable)?;
            hooks.initialization_after_commit();
        } else if names.len() != 3
            || names.iter().any(|name| {
                ![RECORDS.name(), META.name(), COUNTERS.name()].contains(&name.as_str())
            })
        {
            return Err(LocalArchiveError::Integrity);
        }
        // Also complete publication after a crash between engine commit and
        // first directory sync. No acceptance can precede this sync.
        custody.sync_directory()?;
        let value = Self {
            database,
            custody,
            binding,
            quota,
            validation: Validation::default(),
            hooks,
        };
        value.check_metadata()?;
        Ok(value)
    }

    /// Validates at most 256 records and 4 MiB per independent background step.
    /// Completion proves every key, event hash, format, binding, and counter.
    pub fn validate_next_page(&mut self) -> Result<bool, LocalArchiveError> {
        self.custody.verify()?;
        if self.validation.ready {
            return Ok(true);
        }
        let read = self.database.begin_read().map_err(unavailable)?;
        let rows = read.open_table(RECORDS).map_err(integrity)?;
        let low = self
            .validation
            .after
            .as_ref()
            .map_or(Bound::Unbounded, |key| Bound::Excluded(key.as_slice()));
        let iter = rows
            .range::<&[u8]>((low, Bound::Unbounded))
            .map_err(integrity)?;
        let mut page_bytes = 0;
        for (page_rows, item) in iter.enumerate() {
            let (key, encoded) = item.map_err(integrity)?;
            if encoded.value().len() > VALIDATION_BYTES {
                return Err(LocalArchiveError::Integrity);
            }
            if page_rows == VALIDATION_ROWS || page_bytes + encoded.value().len() > VALIDATION_BYTES
            {
                return Ok(false);
            }
            let record = decode_local_outbox_archive_record_v1(encoded.value())
                .map_err(integrity)?
                .into_parts()
                .0;
            if record.database_id() != self.binding || key.value() != record.key() {
                return Err(LocalArchiveError::Integrity);
            }
            let charge = row_charge(encoded.value().len())?;
            self.validation.rows = self
                .validation
                .rows
                .checked_add(1)
                .ok_or(LocalArchiveError::Integrity)?;
            self.validation.bytes = self
                .validation
                .bytes
                .checked_add(charge)
                .ok_or(LocalArchiveError::Integrity)?;
            self.validation.after = Some(record.key());
            page_bytes += encoded.value().len();
        }
        let counted = self.observation()?;
        if counted.records != self.validation.rows || counted.encoded_bytes != self.validation.bytes
        {
            return Err(LocalArchiveError::Integrity);
        }
        self.validation.ready = true;
        Ok(true)
    }

    /// Whether complete archive validation has finished.
    #[must_use]
    pub const fn validated(&self) -> bool {
        self.validation.ready
    }

    /// Exact durable counters; never a claim of full validation on their own.
    pub fn observation(&self) -> Result<LocalArchiveObservation, LocalArchiveError> {
        self.custody.verify()?;
        let read = self.database.begin_read().map_err(unavailable)?;
        let counters = read.open_table(COUNTERS).map_err(integrity)?;
        if counters.len().map_err(integrity)? != 2 {
            return Err(LocalArchiveError::Integrity);
        }
        let encoded_bytes = counters
            .get(0)
            .map_err(integrity)?
            .ok_or(LocalArchiveError::Integrity)?
            .value();
        let records = counters
            .get(1)
            .map_err(integrity)?
            .ok_or(LocalArchiveError::Integrity)?
            .value();
        Ok(LocalArchiveObservation {
            records,
            encoded_bytes,
        })
    }

    /// Physical allocated engine blocks, including retained redb overhead.
    pub fn physical_bytes(&self) -> Result<u64, LocalArchiveError> {
        self.custody.physical_bytes()
    }

    /// Reads and validates one exact record for independent operator reconciliation.
    pub fn read(
        &self,
        key: &[u8; 36],
    ) -> Result<Option<LocalOutboxArchiveRecordV1>, LocalArchiveError> {
        self.custody.verify()?;
        let read = self.database.begin_read().map_err(unavailable)?;
        let rows = read.open_table(RECORDS).map_err(integrity)?;
        let Some(encoded) = rows.get(key.as_slice()).map_err(integrity)? else {
            return Ok(None);
        };
        let record = decode_local_outbox_archive_record_v1(encoded.value())
            .map_err(integrity)?
            .into_parts()
            .0;
        if record.database_id() != self.binding || record.key() != *key {
            return Err(LocalArchiveError::Integrity);
        }
        Ok(Some(record))
    }

    /// Preflight before a delivery claim. Equal retries fit even at full quota.
    pub fn admits(&self, record: &LocalOutboxArchiveRecordV1) -> Result<bool, LocalArchiveError> {
        self.custody.verify()?;
        if !self.validation.ready {
            return Ok(false);
        }
        if record.database_id() != self.binding {
            return Err(LocalArchiveError::Binding);
        }
        let encoded = encode_local_outbox_archive_record_v1(record).map_err(integrity)?;
        let read = self.database.begin_read().map_err(unavailable)?;
        let rows = read.open_table(RECORDS).map_err(integrity)?;
        if let Some(existing) = rows.get(record.key().as_slice()).map_err(integrity)? {
            decode_local_outbox_archive_record_v1(existing.value()).map_err(integrity)?;
            if existing.value() != encoded.as_bytes() {
                return Err(LocalArchiveError::Integrity);
            }
            return Ok(true);
        }
        let used = self.observation()?.encoded_bytes;
        Ok(used
            .checked_add(row_charge(encoded.as_bytes().len())?)
            .is_some_and(|bytes| bytes <= self.quota.get()))
    }

    /// Acknowledges only after one two-phase Immediate engine commit and
    /// directory publication. Immutable equal retries never add rows or bytes.
    pub fn accept(&mut self, record: &LocalOutboxArchiveRecordV1) -> Result<(), LocalArchiveError> {
        if !self.validation.ready {
            return Err(LocalArchiveError::Unavailable);
        }
        if record.database_id() != self.binding {
            return Err(LocalArchiveError::Binding);
        }
        self.custody.verify()?;
        let encoded = encode_local_outbox_archive_record_v1(record).map_err(integrity)?;
        let key = record.key();
        if self.read(&key)?.is_some() {
            if !self.admits(record)? {
                return Err(LocalArchiveError::Integrity);
            }
            return Ok(());
        }
        let before = self.observation()?;
        let after_bytes = before
            .encoded_bytes
            .checked_add(row_charge(encoded.as_bytes().len())?)
            .ok_or(LocalArchiveError::Integrity)?;
        if after_bytes > self.quota.get() {
            return Err(LocalArchiveError::Quota);
        }
        let after_count = before
            .records
            .checked_add(1)
            .ok_or(LocalArchiveError::Integrity)?;
        let mut write = self.database.begin_write().map_err(unavailable)?;
        immediate(&mut write)?;
        {
            let mut rows = write.open_table(RECORDS).map_err(integrity)?;
            if rows
                .insert(key.as_slice(), encoded.as_bytes())
                .map_err(unavailable)?
                .is_some()
            {
                return Err(LocalArchiveError::Integrity);
            }
        }
        {
            let mut counters = write.open_table(COUNTERS).map_err(integrity)?;
            counters.insert(0, after_bytes).map_err(unavailable)?;
            counters.insert(1, after_count).map_err(unavailable)?;
        }
        self.hooks.record_before_commit();
        write.commit().map_err(unavailable)?;
        self.hooks.record_after_commit();
        self.custody.verify()?;
        Ok(())
    }

    fn check_metadata(&self) -> Result<(), LocalArchiveError> {
        let read = self.database.begin_read().map_err(unavailable)?;
        let meta = read.open_table(META).map_err(integrity)?;
        if meta.len().map_err(integrity)? != 2 {
            return Err(LocalArchiveError::Integrity);
        }
        let binding = meta
            .get(0)
            .map_err(integrity)?
            .ok_or(LocalArchiveError::Integrity)?;
        if *decode_database_identity_v1(binding.value())
            .map_err(integrity)?
            .value()
            != self.binding
        {
            return Err(LocalArchiveError::Binding);
        }
        let format = meta
            .get(1)
            .map_err(integrity)?
            .ok_or(LocalArchiveError::Integrity)?;
        if format.value() != local_outbox_archive_schema_hash_v1().as_bytes() {
            return Err(LocalArchiveError::Integrity);
        }
        self.observation()?;
        Ok(())
    }
}

fn row_charge(encoded_bytes: usize) -> Result<u64, LocalArchiveError> {
    u64::try_from(encoded_bytes)
        .map_err(integrity)?
        .checked_add(36)
        .ok_or(LocalArchiveError::Integrity)
}
fn immediate(write: &mut redb::WriteTransaction) -> Result<(), LocalArchiveError> {
    write
        .set_durability(Durability::Immediate)
        .map_err(unavailable)?;
    write.set_two_phase_commit(true);
    Ok(())
}
fn unavailable(_error: impl fmt::Display) -> LocalArchiveError {
    LocalArchiveError::Unavailable
}
fn integrity(_error: impl fmt::Display) -> LocalArchiveError {
    LocalArchiveError::Integrity
}

/// Existing dispatcher adapter for this one database's history incarnation.
pub struct DurableLocalConnector {
    archive: DurableLocalArchive,
    incarnation: NonZeroU64,
    last_error: Option<LocalArchiveError>,
}
impl DurableLocalConnector {
    /// Binds destination custody to the activated database-history incarnation.
    #[must_use]
    pub const fn new(archive: DurableLocalArchive, incarnation: NonZeroU64) -> Self {
        Self {
            archive,
            incarnation,
            last_error: None,
        }
    }
    /// Performs one bounded recovery page before dispatcher admission.
    pub fn validate_next_page(&mut self) -> Result<bool, LocalArchiveError> {
        self.archive.validate_next_page()
    }
    /// Closed pause/refusal cause; no destination or event values.
    #[must_use]
    pub const fn last_error(&self) -> Option<LocalArchiveError> {
        self.last_error
    }
    /// Validated operator reconciliation reader, never a source-state bypass.
    #[must_use]
    pub const fn archive(&self) -> &DurableLocalArchive {
        &self.archive
    }
}
impl fmt::Debug for DurableLocalConnector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("DurableLocalConnector([REDACTED])")
    }
}
impl crate::OutboxConnector for DurableLocalConnector {
    fn declaration(&self) -> crate::ConnectorDeclaration {
        crate::ConnectorDeclaration::new(
            crate::ConnectorDestinationKind::DurableLocal,
            crate::ConnectorIdempotency::EventId,
        )
    }
    fn ready_to_deliver(&mut self, event: &riffdb_storage_api::StoredDurableEventV1) -> bool {
        let record =
            LocalOutboxArchiveRecordV1::new(self.archive.binding, self.incarnation, event.clone());
        match self.archive.admits(&record) {
            Ok(true) => {
                self.last_error = None;
                true
            }
            Ok(false) => {
                self.last_error = Some(if self.archive.validated() {
                    LocalArchiveError::Quota
                } else {
                    LocalArchiveError::Unavailable
                });
                false
            }
            Err(error) => {
                self.last_error = Some(error);
                false
            }
        }
    }
    fn deliver(&mut self, attempt: crate::DeliveryAttempt<'_>) -> crate::ConnectorDisposition {
        let record = LocalOutboxArchiveRecordV1::new(
            self.archive.binding,
            self.incarnation,
            attempt.event().clone(),
        );
        match self.archive.accept(&record) {
            Ok(()) => {
                self.last_error = None;
                crate::ConnectorDisposition::Accepted
            }
            Err(error) => {
                self.last_error = Some(error);
                let safe_error = riffdb_storage_api::OutboxSafeErrorV1::new(
                    "durable local archive refused delivery",
                )
                .ok();
                match error {
                    LocalArchiveError::Quota
                    | LocalArchiveError::Unavailable
                    | LocalArchiveError::UnsafePath => {
                        crate::ConnectorDisposition::Retryable { safe_error }
                    }
                    LocalArchiveError::Binding
                    | LocalArchiveError::Integrity
                    | LocalArchiveError::UnsupportedPlatform => {
                        crate::ConnectorDisposition::PermanentFailure { safe_error }
                    }
                }
            }
        }
    }
}
