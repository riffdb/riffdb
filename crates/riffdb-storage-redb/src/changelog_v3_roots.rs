//! Bounded, read-only V3 checkpoint-root consistency. No history/population scan
//! and no independent latest-root lookup; every byte belongs to the supplied pin.
//! This is one input to clean eligibility, not a replacement for complete startup
//! validation or proof that an application/journal suffix has been published.

use redb::{
    ReadTransaction, ReadableTable, ReadableTableMetadata, TableError, TableHandle,
    WriteTransaction,
};
use riffdb_storage_api::{
    AdministrationSequenceAllocator, ApplicationSequenceAllocator, AuthoritativeNamespaceV1 as N,
    ChangelogHistoryStateV3, StorageError, StorageErrorKind, proto_codec::*,
};
use riffdb_types::{AdministrationSequence, CommitSequence, DualFrontier};

use crate::{
    changelog_v3_activation::{HISTORY, PRE_V3_REGISTRY, SOURCE_HOLDS},
    error::{codec_error, precommit_storage_error, storage_error, table_error},
    layout::{
        META, META_ADMINISTRATION_SEQUENCE, META_APPLICATION_SEQUENCE, META_DATABASE_ID,
        META_HISTORY_INCARNATION, META_RECORD_REGISTRY,
    },
};

#[cfg(test)]
std::thread_local! {
    pub(crate) static COMPLETE_HISTORY_VALIDATIONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Closed V3 metadata shape validation for shared identity probes. This neither
/// grants activation nor replaces the complete, cross-bound root check below.
pub(crate) fn validate_metadata_entry(key: &str, value: &[u8]) -> Result<bool, StorageError> {
    if riffdb_storage_api::AuthoritativeNamespaceV3::ProjectionSourceAllocator.metadata_key()
        == Some(key)
    {
        // Scalar, strict and canonical on the stack. Ordinary command paths
        // must never depend on the background source worker's heap ledger.
        decode_projection_checkpoint_ordinal_allocator_inline_v1(value).map_err(codec_error)?;
        return Ok(true);
    }
    if key == crate::primary_admission_roots::key()? {
        decode_replication_primary_admission(value).map_err(codec_error)?;
        return Ok(true);
    }
    let Some(namespace) = N::ALL.into_iter().find(|namespace| {
        namespace.requires_v3_activation() && namespace.metadata_key() == Some(key)
    }) else {
        return Ok(false);
    };
    if value.len() > 512 {
        return Err(storage_error(StorageErrorKind::LimitExceeded));
    }
    match namespace {
        N::AuthoritativeStateCatalog => {
            crate::primary_admission_roots::catalog_digest(value)?;
        }
        N::LeadershipEpoch => {
            decode_leadership_epoch_v1(value).map_err(codec_error)?;
        }
        N::ChangelogHistoryState => {
            decode_changelog_history_state_v3(value).map_err(codec_error)?;
        }
        N::NextChangelogTransaction => {
            decode_changelog_transaction_allocator_v3(value).map_err(codec_error)?;
        }
        N::ReplicationFollowerState => {
            decode_replication_follower_state_v3(value).map_err(codec_error)?;
        }
        _ => return Err(storage_error(StorageErrorKind::InvariantViolation)),
    }
    Ok(true)
}

/// Returns inactive only for the exact pre-V3 registry and complete absence of
/// every V3 root/table. Current-registry root erasure never becomes inactivity.
/// The caller must treat any failure as ineligible for a clean shortcut, retain
/// the failure for complete validation, and never repair or activate from it.
pub(crate) fn read_checkpoint_roots(
    transaction: &ReadTransaction,
) -> Result<Option<ChangelogHistoryStateV3>, StorageError> {
    read_checkpoint_roots_with_admission(transaction, CheckpointRegistryAdmissionV1::current())
}

pub(crate) fn read_checkpoint_roots_for_registry_completion(
    transaction: &ReadTransaction,
) -> Result<Option<ChangelogHistoryStateV3>, StorageError> {
    read_checkpoint_roots_with_admission(
        transaction,
        CheckpointRegistryAdmissionV1::RegistryCompletion,
    )
}

/// Only this closed entry can select the retained writer-4 marker registry.
/// Its read is opened internally from the immutable artifact owner.
#[cfg(unix)]
pub(crate) fn read_checkpoint_roots_owned_backup_v1(
    owner: &crate::backup::OwnedBackupReadOnlyV1,
) -> Result<Option<ChangelogHistoryStateV3>, StorageError> {
    let read = owner.begin_read()?;
    let history = read_checkpoint_roots_with_admission(
        &read,
        CheckpointRegistryAdmissionV1::OwnedBackup(owner),
    )?;
    owner.verify()?;
    Ok(history)
}

fn read_checkpoint_roots_with_admission(
    transaction: &ReadTransaction,
    admission: CheckpointRegistryAdmissionV1<'_>,
) -> Result<Option<ChangelogHistoryStateV3>, StorageError> {
    let meta = transaction.open_table(META).map_err(table_error)?;
    let history = match transaction.open_table(HISTORY) {
        Ok(table) => Some(table),
        Err(TableError::TableDoesNotExist(_)) => None,
        Err(error) => return Err(table_error(error)),
    };
    let source_holds = match transaction.open_table(SOURCE_HOLDS) {
        Ok(table) => Some(table),
        Err(TableError::TableDoesNotExist(_)) => None,
        Err(error) => return Err(table_error(error)),
    };
    let audit = match transaction.open_table(crate::layout::AUDIT) {
        Ok(table) => Some(table),
        Err(TableError::TableDoesNotExist(_)) => None,
        Err(error) => return Err(table_error(error)),
    };
    validate_roots(
        &meta,
        history.as_ref(),
        source_holds.as_ref(),
        audit.as_ref(),
        None,
        None,
        admission,
    )
}

pub(crate) fn read_checkpoint_roots_admitted_v1(
    transaction: &ReadTransaction,
    budget: &riffdb_types::exact_provider_budget_v1::ProviderBudget,
) -> Result<Option<ChangelogHistoryStateV3>, StorageError> {
    let _work = budget
        .reserve(512 * 1024)
        .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
    let meta = transaction.open_table(META).map_err(table_error)?;
    let history = match transaction.open_table(HISTORY) {
        Ok(table) => Some(table),
        Err(TableError::TableDoesNotExist(_)) => None,
        Err(error) => return Err(table_error(error)),
    };
    let source_holds = match transaction.open_table(SOURCE_HOLDS) {
        Ok(table) => Some(table),
        Err(TableError::TableDoesNotExist(_)) => None,
        Err(error) => return Err(table_error(error)),
    };
    let audit = match transaction.open_table(crate::layout::AUDIT) {
        Ok(table) => Some(table),
        Err(TableError::TableDoesNotExist(_)) => None,
        Err(error) => return Err(table_error(error)),
    };
    validate_roots(
        &meta,
        history.as_ref(),
        source_holds.as_ref(),
        audit.as_ref(),
        Some(budget),
        None,
        CheckpointRegistryAdmissionV1::current(),
    )
}

/// Source-only cold root validation. The caller already owns the fixed root
/// allowance, and all variable terminal work is charged by that same context.
#[cfg_attr(not(feature = "projection-source-driver"), allow(dead_code))]
pub(crate) fn read_checkpoint_roots_source_v1(
    transaction: &ReadTransaction,
    source: &crate::store::OriginalSourceReadContext<'_>,
) -> Result<Option<ChangelogHistoryStateV3>, StorageError> {
    let meta = transaction.open_table(META).map_err(table_error)?;
    let history = match transaction.open_table(HISTORY) {
        Ok(table) => Some(table),
        Err(TableError::TableDoesNotExist(_)) => None,
        Err(error) => return Err(table_error(error)),
    };
    let source_holds = match transaction.open_table(SOURCE_HOLDS) {
        Ok(table) => Some(table),
        Err(TableError::TableDoesNotExist(_)) => None,
        Err(error) => return Err(table_error(error)),
    };
    let audit = match transaction.open_table(crate::layout::AUDIT) {
        Ok(table) => Some(table),
        Err(TableError::TableDoesNotExist(_)) => None,
        Err(error) => return Err(table_error(error)),
    };
    validate_roots(
        &meta,
        history.as_ref(),
        source_holds.as_ref(),
        audit.as_ref(),
        None,
        Some(source),
        CheckpointRegistryAdmissionV1::current(),
    )
}

/// Complete retained receipt-chain validation for the full startup path. This
/// streams the exact retained interval from one pin, retaining only one decoded
/// bounded row and a constant-size running root. It is deliberately separate
/// from clean eligibility's bounded terminal-root check. Durable-record semantics
/// and the other authoritative namespaces still require their existing validators.
pub(crate) fn validate_retained_history(
    transaction: &ReadTransaction,
) -> Result<Option<ChangelogHistoryStateV3>, StorageError> {
    validate_retained_history_with_admission(transaction, CheckpointRegistryAdmissionV1::current())
}

#[cfg(unix)]
pub(crate) fn validate_retained_history_owned_backup_v1(
    owner: &crate::backup::OwnedBackupReadOnlyV1,
) -> Result<Option<ChangelogHistoryStateV3>, StorageError> {
    let read = owner.begin_read()?;
    let history = validate_retained_history_with_admission(
        &read,
        CheckpointRegistryAdmissionV1::OwnedBackup(owner),
    )?;
    owner.verify()?;
    Ok(history)
}

fn validate_retained_history_with_admission(
    transaction: &ReadTransaction,
    admission: CheckpointRegistryAdmissionV1<'_>,
) -> Result<Option<ChangelogHistoryStateV3>, StorageError> {
    #[cfg(test)]
    COMPLETE_HISTORY_VALIDATIONS.with(|count| count.set(count.get() + 1));
    let Some(history) = read_checkpoint_roots_with_admission(transaction, admission)? else {
        return Ok(None);
    };
    let table = transaction.open_table(HISTORY).map_err(table_error)?;
    let holds = transaction.open_table(SOURCE_HOLDS).map_err(table_error)?;
    // An empty history was admitted above only with the exact attached
    // follower progress and empty source holds. It has no source ancestry to
    // scan; all authoritative/catalog validators still run unchanged.
    if !table.is_empty().map_err(precommit_storage_error)? {
        validate_retained_rows(history, &table, &holds)?;
        let audit = transaction
            .open_table(crate::layout::AUDIT)
            .map_err(table_error)?;
        crate::replication_registration_links::validate(&holds, &audit, history, &table)?;
        let meta = transaction.open_table(META).map_err(table_error)?;
        crate::primary_admission_roots::validate_retained(&meta, history, &audit, &table)?;
        let indexes = transaction
            .open_table(crate::layout::AUDIT_BY_REQUEST)
            .map_err(table_error)?;
        crate::promotion_cutover::validate_retained(history, &audit, &indexes, &table)?;
    }
    Ok(Some(history))
}

/// Full preflight on the original exclusive restore transaction. The bounded
/// inventory check precedes opening write tables, so no missing table is created.
pub(crate) fn validate_retained_history_for_write(
    transaction: &WriteTransaction,
) -> Result<Option<ChangelogHistoryStateV3>, StorageError> {
    #[cfg(test)]
    COMPLETE_HISTORY_VALIDATIONS.with(|count| count.set(count.get() + 1));
    let Some(history) = read_checkpoint_roots_for_write(transaction)? else {
        return Ok(None);
    };
    let table = transaction.open_table(HISTORY).map_err(table_error)?;
    let holds = transaction.open_table(SOURCE_HOLDS).map_err(table_error)?;
    // An empty history was admitted above only with the exact attached
    // follower progress and empty source holds. It has no source ancestry to
    // scan; all authoritative/catalog validators still run unchanged.
    if !table.is_empty().map_err(precommit_storage_error)? {
        validate_retained_rows(history, &table, &holds)?;
        let audit = transaction
            .open_table(crate::layout::AUDIT)
            .map_err(table_error)?;
        crate::replication_registration_links::validate(&holds, &audit, history, &table)?;
        let meta = transaction.open_table(META).map_err(table_error)?;
        crate::primary_admission_roots::validate_retained(&meta, history, &audit, &table)?;
        let indexes = transaction
            .open_table(crate::layout::AUDIT_BY_REQUEST)
            .map_err(table_error)?;
        crate::promotion_cutover::validate_retained(history, &audit, &indexes, &table)?;
    }
    Ok(Some(history))
}

// Shared chain check; does not grant a V1 or V2 writer permit.
pub(crate) fn validate_retained_rows(
    history: ChangelogHistoryStateV3,
    table: &impl ReadableTable<&'static [u8], &'static [u8]>,
    holds: &impl ReadableTable<&'static [u8], &'static [u8]>,
) -> Result<(), StorageError> {
    let corrupt = || storage_error(StorageErrorKind::CorruptData);
    let mut covered = ChangelogHistoryStateV3::new(
        history.lineage(),
        history.anchor(),
        history.minimum_resume(),
        history.minimum_resume(),
    )
    .map_err(|_| corrupt())?;
    let mut first = true;
    #[cfg(all(unix, not(test)))]
    {
        let _fs_cache_guard = crate::journal::ScopedValidateRowsFsCache::enter();
        const BATCH_ROWS: usize = 2048;
        const BATCH_MAX_BYTES: usize = 16 * 1024 * 1024;
        struct BatchRow {
            key: [u8; 8],
            start: usize,
            end: usize,
            frame_hash: [u8; 32],
            expected_db: riffdb_types::DatabaseId,
            verified_sealed: bool,
            summary: Option<Result<crate::changelog_v3::ValidatedReceiptSummaryV3, StorageError>>,
        }
        let mut batch_bytes = Vec::with_capacity(8 * 1024 * 1024);
        let mut batch_rows: Vec<BatchRow> = Vec::with_capacity(BATCH_ROWS);
        let mut iter = table.iter().map_err(precommit_storage_error)?;
        loop {
            batch_bytes.clear();
            batch_rows.clear();
            let mut stub_count = 0usize;
            while batch_rows.len() < BATCH_ROWS && batch_bytes.len() < BATCH_MAX_BYTES {
                let Some(entry) = iter.next() else {
                    break;
                };
                let (key, value) = entry.map_err(precommit_storage_error)?;
                let Ok(key_arr) = <[u8; 8]>::try_from(key.value()) else {
                    return Err(corrupt());
                };
                let val_bytes = value.value();
                if let Some(stub) = crate::history_stub::HistoryStub::decode(val_bytes)? {
                    let start = batch_bytes.len();
                    let (expected_db, verified_sealed) =
                        crate::history_stub::copy_stub_logical_frame(&stub, &mut batch_bytes)?;
                    let end = batch_bytes.len();
                    stub_count += 1;
                    batch_rows.push(BatchRow {
                        key: key_arr,
                        start,
                        end,
                        frame_hash: stub.placement.frame_hash,
                        expected_db,
                        verified_sealed,
                        summary: None,
                    });
                } else {
                    let summary = crate::history_stub::resolve_history_summary(val_bytes);
                    batch_rows.push(BatchRow {
                        key: key_arr,
                        start: 0,
                        end: 0,
                        frame_hash: [0; 32],
                        expected_db: history.lineage().database_id(),
                        verified_sealed: false,
                        summary: Some(summary),
                    });
                }
            }
            if batch_rows.is_empty() {
                break;
            }
            if stub_count >= 64 {
                let bytes_ref = batch_bytes.as_slice();
                let chunk_size = batch_rows.len().div_ceil(8).max(16);
                std::thread::scope(|scope| {
                    for chunk in batch_rows.chunks_mut(chunk_size) {
                        scope.spawn(move || {
                            for row in chunk {
                                if row.summary.is_none() {
                                    let logical = &bytes_ref[row.start..row.end];
                                    let res = crate::journal::JournalFrame::decode_proven_history_summary_with_options(
                                        logical,
                                        &row.frame_hash,
                                        row.verified_sealed,
                                    )
                                    .ok()
                                    .filter(|(db, _)| *db == row.expected_db)
                                    .map(|(_, s)| s)
                                    .ok_or_else(corrupt);
                                    row.summary = Some(res);
                                }
                            }
                        });
                    }
                });
            } else {
                let bytes_ref = batch_bytes.as_slice();
                for row in &mut batch_rows {
                    if row.summary.is_none() {
                        let logical = &bytes_ref[row.start..row.end];
                        let res = crate::journal::JournalFrame::decode_proven_history_summary_with_options(
                            logical,
                            &row.frame_hash,
                            row.verified_sealed,
                        )
                        .ok()
                        .filter(|(db, _)| *db == row.expected_db)
                        .map(|(_, s)| s)
                        .ok_or_else(corrupt);
                        row.summary = Some(res);
                    }
                }
            }
            for row in &mut batch_rows {
                let summary = row.summary.take().unwrap_or_else(|| Err(corrupt()))?;
                if row.key != summary.binding().sequence.get().to_be_bytes()
                    || summary.binding().sequence > history.tail().sequence()
                {
                    return Err(corrupt());
                }
                if first {
                    summary
                        .validate_terminal_receipt(covered)
                        .map_err(|_| corrupt())?;
                    first = false;
                } else {
                    covered = summary.advance_history(covered).map_err(|_| corrupt())?;
                }
            }
        }
    }
    #[cfg(not(all(unix, not(test))))]
    for entry in table.iter().map_err(precommit_storage_error)? {
        let (key, value) = entry.map_err(precommit_storage_error)?;
        let summary = crate::history_stub::resolve_history_summary(value.value())?;
        if key.value() != summary.binding().sequence.get().to_be_bytes()
            || summary.binding().sequence > history.tail().sequence()
        {
            return Err(corrupt());
        }
        if first {
            summary
                .validate_terminal_receipt(covered)
                .map_err(|_| corrupt())?;
            first = false;
        } else {
            covered = summary.advance_history(covered).map_err(|_| corrupt())?;
        }
    }
    if first || covered != history {
        return Err(corrupt());
    }
    validate_source_holds(holds, history, table)
}

/// Full-validation-only, bounded source population from the SAME read pin as
/// the validated retained chain. Never upgrades a decoded value into permission
/// to prune or silently discards an unrecognized fence.
pub(crate) fn validate_source_holds(
    holds: &impl ReadableTable<&'static [u8], &'static [u8]>,
    history: ChangelogHistoryStateV3,
    receipts: &impl crate::source_validation_read::ReceiptRead,
) -> Result<(), StorageError> {
    use riffdb_storage_api::{
        FollowerRegistrationPhaseV1 as Phase, MAX_REPLICATION_SOURCE_HOLDS_V1,
        ReplicationSourceHoldStateV1 as State,
    };
    let corrupt = || storage_error(StorageErrorKind::CorruptData);
    if holds.len().map_err(precommit_storage_error)? > MAX_REPLICATION_SOURCE_HOLDS_V1 {
        return Err(storage_error(StorageErrorKind::LimitExceeded));
    }
    for row in holds.iter().map_err(precommit_storage_error)? {
        let (key, value) = row.map_err(precommit_storage_error)?;
        let state = *decode_replication_source_hold(value.value())
            .map_err(codec_error)?
            .value();
        let hold = match state {
            State::Legacy(hold) => hold,
            State::Registered(policy) => policy.hold(),
        };
        if key.value() != hold.storage_key() || hold.lineage() != history.lineage() {
            return Err(corrupt());
        }
        if let State::Registered(policy) = state {
            crate::replication_registration_links::validate_point(
                policy.registered_at(),
                history,
                receipts,
            )?;
            if let Some(point) = policy.degraded_at() {
                crate::replication_registration_links::validate_point(point, history, receipts)?;
            }
            if policy.phase() == Phase::Retired {
                // The full audit pass proves the exact release before any
                // caller may use the absence of this live retention fence.
                crate::replication_registration_links::validate_point(
                    hold.fence(),
                    history,
                    receipts,
                )?;
                continue;
            }
        }
        ChangelogHistoryStateV3::new(
            history.lineage(),
            history.minimum_resume(),
            history.tail(),
            hold.fence(),
        )
        .map_err(|_| corrupt())?;
        let receipt = receipts
            .receipt(hold.fence().sequence(), None)?
            .ok_or_else(corrupt)?;
        let summary = crate::changelog_v3::ValidatedReceiptSummaryV3::from_receipt(&receipt)
            .map_err(|_| corrupt())?;
        if summary.point() != hold.fence() {
            return Err(corrupt());
        }
    }
    Ok(())
}

#[cfg(test)]
thread_local! {
    /// Full transaction-current root validations on this thread, so tests can
    /// hold how often a multi-frame owner pays for one.
    pub(crate) static WRITE_ROOT_READS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Transaction-current form for receipt-producing Immediate writes. Opening a
/// redb write table creates it when absent, so check the bounded table-name
/// inventory first. This function never initializes missing roots or tables.
pub(crate) fn read_checkpoint_roots_for_write(
    transaction: &WriteTransaction,
) -> Result<Option<ChangelogHistoryStateV3>, StorageError> {
    #[cfg(test)]
    WRITE_ROOT_READS.with(|reads| reads.set(reads.get() + 1));
    let mut meta_present = false;
    let mut history_present = false;
    let mut holds_present = false;
    let mut audit_present = false;
    for (count, table) in transaction
        .list_tables()
        .map_err(precommit_storage_error)?
        .enumerate()
    {
        if count
            >= riffdb_storage_api::AuthoritativeStateCatalogV3
                .namespaces()
                .count()
        {
            return Err(storage_error(StorageErrorKind::LimitExceeded));
        }
        meta_present |= table.name() == META.name();
        history_present |= table.name() == HISTORY.name();
        holds_present |= table.name() == SOURCE_HOLDS.name();
        audit_present |= table.name() == crate::layout::AUDIT.name();
    }
    if !meta_present || (history_present && !audit_present) {
        return Err(storage_error(StorageErrorKind::CorruptData));
    }
    let meta = transaction.open_table(META).map_err(table_error)?;
    let history = history_present
        .then(|| transaction.open_table(HISTORY))
        .transpose()
        .map_err(table_error)?;
    let source_holds = holds_present
        .then(|| transaction.open_table(SOURCE_HOLDS))
        .transpose()
        .map_err(table_error)?;
    let audit = audit_present
        .then(|| transaction.open_table(crate::layout::AUDIT))
        .transpose()
        .map_err(table_error)?;
    validate_roots(
        &meta,
        history.as_ref(),
        source_holds.as_ref(),
        audit.as_ref(),
        None,
        None,
        CheckpointRegistryAdmissionV1::RegistryCompletion,
    )
}

/// Closed registry admission, with no arbitrary digest or read-owner selector.
enum CheckpointRegistryAdmissionV1<'owner> {
    Current(std::marker::PhantomData<&'owner ()>),
    RegistryCompletion,
    #[cfg(unix)]
    OwnedBackup(&'owner crate::backup::OwnedBackupReadOnlyV1),
}
impl CheckpointRegistryAdmissionV1<'_> {
    fn current() -> Self {
        Self::Current(std::marker::PhantomData)
    }
    fn permits_inactive(&self) -> bool {
        matches!(self, Self::Current(_) | Self::RegistryCompletion)
    }
    fn accepts_known(&self, registry: riffdb_types::SchemaHash) -> bool {
        self.accepts_active(registry) || (self.permits_inactive() && registry == PRE_V3_REGISTRY)
    }
    fn accepts_active(&self, registry: riffdb_types::SchemaHash) -> bool {
        match self {
            Self::Current(_) => {
                registry == current_record_registry_digest()
                    || registry == crate::store::PRE_COMPLETION_REGISTRY
            }
            Self::RegistryCompletion => {
                registry == current_record_registry_digest()
                    || registry == crate::store::PRE_COMPLETION_REGISTRY
                    || registry == crate::store::PRE_SOURCE_CUT_REGISTRY
            }
            #[cfg(unix)]
            Self::OwnedBackup(owner) => {
                owner.marker().identity().writer().get() == 4
                    && registry == owner.marker().registry_digest()
            }
        }
    }
}

fn validate_roots(
    meta: &impl ReadableTable<&'static str, &'static [u8]>,
    history_table: Option<&impl ReadableTable<&'static [u8], &'static [u8]>>,
    source_holds: Option<&impl ReadableTable<&'static [u8], &'static [u8]>>,
    audit: Option<&impl ReadableTable<&'static [u8], &'static [u8]>>,
    budget: Option<&riffdb_types::exact_provider_budget_v1::ProviderBudget>,
    source: Option<&crate::store::OriginalSourceReadContext<'_>>,
    admission: CheckpointRegistryAdmissionV1<'_>,
) -> Result<Option<ChangelogHistoryStateV3>, StorageError> {
    let mut reader = ordinary::OrdinaryRootRead {
        meta,
        history_table,
        source_holds,
        audit,
        budget,
        source,
    };
    validate_root_evidence(&mut reader, admission)
}

// This private trait is implemented only by the two typed transports below.
// It exposes no raw read, arbitrary callback, caller digest or root authority.
trait RootEvidenceRead {
    fn meta_value(&mut self, key: &str) -> Result<Option<RootValue>, StorageError>;
    fn meta_present(&mut self, key: &str) -> Result<bool, StorageError>;
    fn history_present(&self) -> bool;
    fn holds_present(&self) -> bool;
    fn history_empty(&mut self) -> Result<bool, StorageError>;
    fn holds_empty(&mut self) -> Result<bool, StorageError>;
    fn validate_source_allocator(
        &mut self,
        catalog: [u8; 32],
        incarnation: u64,
    ) -> Result<(), StorageError>;
    fn validate_admission(
        &mut self,
        history: ChangelogHistoryStateV3,
        attached: bool,
    ) -> Result<(), StorageError>;
    fn validate_terminal(&mut self, history: ChangelogHistoryStateV3) -> Result<(), StorageError>;
}
// Fixed scalar root transport is covered by the caller's pre-existing metadata
// work admission. Never copy a variable receipt/admission envelope into this.
struct RootValue {
    bytes: [u8; 512],
    len: usize,
}
impl RootValue {
    fn copy(bytes: &[u8]) -> Result<Self, StorageError> {
        if bytes.len() > 512 {
            return Err(storage_error(StorageErrorKind::LimitExceeded));
        }
        let mut value = Self {
            bytes: [0; 512],
            len: bytes.len(),
        };
        value.bytes[..bytes.len()].copy_from_slice(bytes);
        Ok(value)
    }
    fn value(&self) -> &[u8] {
        &self.bytes[..self.len]
    }
}
impl std::ops::Deref for RootValue {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        self.value()
    }
}
fn root_read(reader: &mut impl RootEvidenceRead, key: &str) -> Result<RootValue, StorageError> {
    reader
        .meta_value(key)?
        .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))
}
fn root_namespace(
    reader: &mut impl RootEvidenceRead,
    namespace: N,
) -> Result<RootValue, StorageError> {
    root_read(
        reader,
        namespace
            .metadata_key()
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?,
    )
}
#[path = "changelog_v3_roots/ordinary.rs"]
mod ordinary;
#[path = "changelog_v3_roots/source_native.rs"]
mod source_native;
pub(crate) use source_native::read_native_source_roots;

fn validate_root_evidence(
    reader: &mut impl RootEvidenceRead,
    admission: CheckpointRegistryAdmissionV1<'_>,
) -> Result<Option<ChangelogHistoryStateV3>, StorageError> {
    let registry = *decode_record_registry_digest(&root_read(reader, META_RECORD_REGISTRY)?)
        .map_err(codec_error)?
        .value();
    if !admission.accepts_known(registry) {
        return Err(storage_error(StorageErrorKind::IncompatibleFormat));
    }
    let mut present = 0;
    let mut required = 0;
    for namespace in N::ALL.into_iter().filter(|n| n.requires_v3_activation()) {
        if let Some(key) = namespace.metadata_key() {
            required += 1;
            present += usize::from(reader.meta_present(key)?);
        }
    }
    if admission.permits_inactive()
        && registry == PRE_V3_REGISTRY
        && present == 0
        && !reader.history_present()
        && !reader.holds_present()
    {
        let source_allocator_key =
            riffdb_storage_api::AuthoritativeNamespaceV3::ProjectionSourceAllocator
                .metadata_key()
                .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
        if reader.meta_present(crate::primary_admission_roots::key()?)?
            || reader.meta_present(source_allocator_key)?
        {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        return Ok(None);
    }
    if !admission.accepts_active(registry) || present != required || !reader.holds_present() {
        return Err(storage_error(StorageErrorKind::CorruptData));
    }
    if !reader.history_present() {
        return Err(storage_error(StorageErrorKind::CorruptData));
    }
    let catalog = crate::primary_admission_roots::catalog_digest(&root_namespace(
        reader,
        N::AuthoritativeStateCatalog,
    )?)?;
    let epoch = *decode_leadership_epoch_v1(&root_namespace(reader, N::LeadershipEpoch)?)
        .map_err(codec_error)?
        .value();
    let history =
        *decode_changelog_history_state_v3(&root_namespace(reader, N::ChangelogHistoryState)?)
            .map_err(codec_error)?
            .value();
    let allocator = *decode_changelog_transaction_allocator_v3(&root_namespace(
        reader,
        N::NextChangelogTransaction,
    )?)
    .map_err(codec_error)?
    .value();
    let follower = *decode_replication_follower_state_v3(&root_namespace(
        reader,
        N::ReplicationFollowerState,
    )?)
    .map_err(codec_error)?
    .value();
    let database = *decode_database_identity_v1(&root_read(reader, META_DATABASE_ID)?)
        .map_err(codec_error)?
        .value();
    let incarnation = *decode_history_incarnation_v1(&root_read(reader, META_HISTORY_INCARNATION)?)
        .map_err(codec_error)?
        .value();
    reader.validate_source_allocator(catalog, incarnation)?;
    let frontier = decode_physical_frontier(
        &root_read(reader, META_APPLICATION_SEQUENCE)?,
        &root_read(reader, META_ADMINISTRATION_SEQUENCE)?,
    )?;
    let lineage = history.lineage();
    if lineage.database_id() != database
        || lineage.history_incarnation() != incarnation
        || lineage.leadership_epoch() != epoch
        || lineage.catalog_digest() != catalog
        || history.tail().frontier() != frontier
        || follower
            .attached_state()
            .is_some_and(|(attached, _, _)| attached != lineage)
    {
        return Err(storage_error(StorageErrorKind::CorruptData));
    }
    history
        .validate_allocator(allocator)
        .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
    reader.validate_admission(history, follower.attached_state().is_some())?;
    // SourceOnly tables exist in the shared engine layout, but follower
    // bootstrap transfers no rows into them. Its existing durable applied
    // receipt hash binds the exact prefix; source receipt ancestry validation
    // remains mandatory for every source and physical source-history image.
    if let Some((attached, applied, _)) = follower.attached_state()
        && (reader.history_empty()?
            || catalog == riffdb_storage_api::AuthoritativeStateCatalogV2.digest()
            || catalog == riffdb_storage_api::AuthoritativeStateCatalogV3.digest())
    {
        if attached != lineage
            || applied != history.tail()
            || !reader.history_empty()?
            || !reader.holds_empty()?
            || reader.meta_present(crate::layout::META_CLEAN_CLOSE_LIFECYCLE)?
        {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        return Ok(Some(history));
    }
    reader.validate_terminal(history)?;
    Ok(Some(history))
}

pub(crate) fn validate_source_allocator_value(
    catalog: [u8; 32],
    incarnation: u64,
    value: Option<&[u8]>,
) -> Result<(), StorageError> {
    if catalog == riffdb_storage_api::AuthoritativeStateCatalogV3.digest() {
        let value = value.ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
        let decoded =
            decode_projection_checkpoint_ordinal_allocator_inline_v1(value).map_err(codec_error)?;
        if decoded.value().allocation_history_incarnation() != incarnation {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
    } else if value.is_some() {
        return Err(storage_error(StorageErrorKind::CorruptData));
    }
    Ok(())
}

/// Physical dual frontier from the two bounded current allocator envelopes.
/// This projection is not independently a validated root or operation permit.
pub(crate) fn decode_physical_frontier(
    application: &[u8],
    administration: &[u8],
) -> Result<DualFrontier, StorageError> {
    let application = match *decode_application_sequence_allocator_v1(application)
        .map_err(codec_error)?
        .value()
    {
        ApplicationSequenceAllocator::Next(next) => CommitSequence::new(next.get() - 1),
        ApplicationSequenceAllocator::Exhausted => CommitSequence::new(u64::MAX),
    };
    let administration = match *decode_administration_sequence_allocator_v1(administration)
        .map_err(codec_error)?
        .value()
    {
        AdministrationSequenceAllocator::Next(next) => AdministrationSequence::new(next.get() - 1),
        AdministrationSequenceAllocator::Exhausted => AdministrationSequence::new(u64::MAX),
    };
    Ok(DualFrontier::new(application, administration))
}
