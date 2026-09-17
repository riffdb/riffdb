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
    AuthoritativeTransactionV3, ChangelogHistoryStateV3, StorageError, StorageErrorKind,
    proto_codec::*,
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

/// Closed V3 metadata shape validation for shared identity probes. This neither
/// grants activation nor replaces the complete, cross-bound root check below.
pub(crate) fn validate_metadata_entry(key: &str, value: &[u8]) -> Result<bool, StorageError> {
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
            decode_authoritative_state_catalog_v1(value).map_err(codec_error)?;
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
    validate_roots(&meta, history.as_ref(), source_holds.as_ref())
}

/// Complete retained receipt-chain validation for the full startup path. This
/// streams the exact retained interval from one pin, retaining only one decoded
/// bounded row and a constant-size running root. It is deliberately separate
/// from clean eligibility's bounded terminal-root check. Durable-record semantics
/// and the other authoritative namespaces still require their existing validators.
pub(crate) fn validate_retained_history(
    transaction: &ReadTransaction,
) -> Result<Option<ChangelogHistoryStateV3>, StorageError> {
    let Some(history) = read_checkpoint_roots(transaction)? else {
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
    }
    Ok(Some(history))
}

/// Full preflight on the original exclusive restore transaction. The bounded
/// inventory check precedes opening write tables, so no missing table is created.
pub(crate) fn validate_retained_history_for_write(
    transaction: &WriteTransaction,
) -> Result<Option<ChangelogHistoryStateV3>, StorageError> {
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
    }
    Ok(Some(history))
}

fn validate_retained_rows(
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
    for entry in table.iter().map_err(precommit_storage_error)? {
        let (key, value) = entry.map_err(precommit_storage_error)?;
        let receipt = AuthoritativeTransactionV3::decode(value.value()).map_err(|_| corrupt())?;
        if key.value() != receipt.binding().sequence.get().to_be_bytes()
            || receipt.binding().sequence > history.tail().sequence()
        {
            return Err(corrupt());
        }
        if first {
            covered
                .validate_terminal_receipt(&receipt)
                .map_err(|_| corrupt())?;
            first = false;
        } else {
            covered = covered.advance(&receipt).map_err(|_| corrupt())?;
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
fn validate_source_holds(
    holds: &impl ReadableTable<&'static [u8], &'static [u8]>,
    history: ChangelogHistoryStateV3,
    receipts: &impl ReadableTable<&'static [u8], &'static [u8]>,
) -> Result<(), StorageError> {
    use riffdb_storage_api::{
        ChangelogHistoryPointV3, FollowerRegistrationPhaseV1 as Phase,
        MAX_REPLICATION_SOURCE_HOLDS_V1, ReplicationSourceHoldStateV1 as State,
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
            .get(hold.fence().sequence().get().to_be_bytes().as_slice())
            .map_err(precommit_storage_error)?
            .ok_or_else(corrupt)?;
        let receipt = AuthoritativeTransactionV3::decode(receipt.value()).map_err(|_| corrupt())?;
        if ChangelogHistoryPointV3::from_receipt(&receipt).map_err(|_| corrupt())? != hold.fence() {
            return Err(corrupt());
        }
    }
    Ok(())
}

/// Transaction-current form for receipt-producing Immediate writes. Opening a
/// redb write table creates it when absent, so check the bounded table-name
/// inventory first. This function never initializes missing roots or tables.
pub(crate) fn read_checkpoint_roots_for_write(
    transaction: &WriteTransaction,
) -> Result<Option<ChangelogHistoryStateV3>, StorageError> {
    let mut meta_present = false;
    let mut history_present = false;
    let mut holds_present = false;
    let mut audit_present = false;
    for (count, table) in transaction
        .list_tables()
        .map_err(precommit_storage_error)?
        .enumerate()
    {
        if count >= N::ALL.len() {
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
    validate_roots(&meta, history.as_ref(), source_holds.as_ref())
}

fn validate_roots(
    meta: &impl ReadableTable<&'static str, &'static [u8]>,
    history_table: Option<&impl ReadableTable<&'static [u8], &'static [u8]>>,
    source_holds: Option<&impl ReadableTable<&'static [u8], &'static [u8]>>,
) -> Result<Option<ChangelogHistoryStateV3>, StorageError> {
    let read = |key: &str| -> Result<Vec<u8>, StorageError> {
        let value = meta
            .get(key)
            .map_err(precommit_storage_error)?
            .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
        // Fixed-root envelopes are at most297 bytes today. This closed ceiling
        // also covers legacy envelopes while refusing large values before copy.
        if value.value().len() > 512 {
            return Err(storage_error(StorageErrorKind::LimitExceeded));
        }
        Ok(value.value().to_vec())
    };
    let read_namespace = |namespace: N| {
        read(
            namespace
                .metadata_key()
                .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?,
        )
    };
    let registry = *decode_record_registry_v2(&read(META_RECORD_REGISTRY)?)
        .map_err(codec_error)?
        .value();
    if registry != PRE_V3_REGISTRY && registry != current_record_registry_digest() {
        return Err(storage_error(StorageErrorKind::IncompatibleFormat));
    }
    let mut present = 0;
    let mut required = 0;
    for namespace in N::ALL.into_iter().filter(|n| n.requires_v3_activation()) {
        if let Some(key) = namespace.metadata_key() {
            required += 1;
            present += usize::from(meta.get(key).map_err(precommit_storage_error)?.is_some());
        }
    }
    if registry == PRE_V3_REGISTRY
        && present == 0
        && history_table.is_none()
        && source_holds.is_none()
    {
        return Ok(None);
    }
    if registry != current_record_registry_digest() || present != required || source_holds.is_none()
    {
        return Err(storage_error(StorageErrorKind::CorruptData));
    }
    let history_table =
        history_table.ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
    decode_authoritative_state_catalog_v1(&read_namespace(N::AuthoritativeStateCatalog)?)
        .map_err(codec_error)?;
    let epoch = *decode_leadership_epoch_v1(&read_namespace(N::LeadershipEpoch)?)
        .map_err(codec_error)?
        .value();
    let history = *decode_changelog_history_state_v3(&read_namespace(N::ChangelogHistoryState)?)
        .map_err(codec_error)?
        .value();
    let allocator =
        *decode_changelog_transaction_allocator_v3(&read_namespace(N::NextChangelogTransaction)?)
            .map_err(codec_error)?
            .value();
    let follower =
        *decode_replication_follower_state_v3(&read_namespace(N::ReplicationFollowerState)?)
            .map_err(codec_error)?
            .value();
    let database = *decode_database_identity_v1(&read(META_DATABASE_ID)?)
        .map_err(codec_error)?
        .value();
    let incarnation = *decode_history_incarnation_v1(&read(META_HISTORY_INCARNATION)?)
        .map_err(codec_error)?
        .value();
    let frontier = decode_physical_frontier(
        &read(META_APPLICATION_SEQUENCE)?,
        &read(META_ADMINISTRATION_SEQUENCE)?,
    )?;
    let lineage = history.lineage();
    if lineage.database_id() != database
        || lineage.history_incarnation() != incarnation
        || lineage.leadership_epoch() != epoch
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
    // SourceOnly tables exist in the shared engine layout, but follower
    // bootstrap transfers no rows into them. Its existing durable applied
    // receipt hash binds the exact prefix; source receipt ancestry validation
    // remains mandatory for every source and physical source-history image.
    if history_table.is_empty().map_err(precommit_storage_error)?
        && let Some((attached, applied, _)) = follower.attached_state()
    {
        if attached != lineage
            || applied != history.tail()
            || !source_holds
                .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?
                .is_empty()
                .map_err(precommit_storage_error)?
            || meta
                .get(crate::layout::META_CLEAN_CLOSE_LIFECYCLE)
                .map_err(precommit_storage_error)?
                .is_some()
        {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        return Ok(Some(history));
    }
    let receipt = history_table
        .get(history.tail().sequence().get().to_be_bytes().as_slice())
        .map_err(precommit_storage_error)?
        .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
    // The decoder checks its frame-adjusted hard bound before hashing/copying.
    let receipt = AuthoritativeTransactionV3::decode(receipt.value())
        .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
    history
        .validate_terminal_receipt(&receipt)
        .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
    Ok(Some(history))
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
