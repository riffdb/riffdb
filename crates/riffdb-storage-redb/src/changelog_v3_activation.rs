//! Atomic installation primitive for ADR-0186. Production startup must supply
//! complete structural/catalog validation under its exclusive session before
//! calling this primitive. It is intentionally not wired to readiness yet:
//! every post-activation writer and recovery lane must first carry V3 receipts.
#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "WP-772 production activation awaits complete writer and recovery integration"
    )
)]

use redb::{Durability, ReadableTable, TableDefinition, TableHandle, WriteTransaction};
use riffdb_storage_api::{
    AdministrationSequenceAllocator, ApplicationSequenceAllocator, AuthoritativeMutationV3,
    AuthoritativeNamespaceV1 as N, AuthoritativeStateCatalogV1, AuthoritativeTransactionBindingV3,
    AuthoritativeTransactionV3, ChangelogAttributionV3, ChangelogHistoryPointV3,
    ChangelogHistoryStateV3, ChangelogLineageV3, ChangelogTransactionAllocator,
    ReplicationFollowerStateV3, StorageError, StorageErrorKind, proto_codec::*,
};
use riffdb_types::{AdministrationSequence, CommitSequence, DualFrontier, SchemaHash};

use crate::{
    error::{codec_error, commit_error, precommit_storage_error, storage_error, table_error},
    layout::*,
};

// Exact merged tag-67 registry, independently frozen by the compatibility test.
const PRE_V3_REGISTRY: SchemaHash = SchemaHash::from_bytes([
    0x9c, 0x44, 0xee, 0x37, 0x89, 0x21, 0x10, 0x1a, 0x16, 0x32, 0x4e, 0xb2, 0x23, 0x26, 0xc2, 0x6c,
    0x04, 0x8e, 0xdd, 0xab, 0x0f, 0xcf, 0xe5, 0xcb, 0x9a, 0x14, 0x08, 0x89, 0x0a, 0x5f, 0x54, 0xed,
]);

pub(crate) const HISTORY: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new(N::ChangelogHistory.table());
pub(crate) const SOURCE_HOLDS: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new(N::ReplicationSourceHolds.table());

fn key(namespace: N) -> Result<&'static str, StorageError> {
    namespace
        .metadata_key()
        .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))
}

/// Consumes exactly one existing transaction and makes the complete activation
/// durable with one hardened commit. Every refusal before commit drops/aborts it;
/// a commit error remains unknown, never reported as rollback. No older receipt
/// is synthesized. The supplied fence is rechecked against transaction-current
/// retained metadata, not the latest independently opened read view.
pub(crate) fn activate_validated(
    mut transaction: WriteTransaction,
    lineage: ChangelogLineageV3,
    frontier: DualFrontier,
) -> Result<ChangelogHistoryStateV3, StorageError> {
    transaction.set_two_phase_commit(true);
    transaction
        .set_durability(Durability::Immediate)
        .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
    if transaction
        .list_tables()
        .map_err(precommit_storage_error)?
        .any(|table| {
            table.name() == N::ChangelogHistory.table()
                || table.name() == N::ReplicationSourceHolds.table()
        })
    {
        return Err(storage_error(StorageErrorKind::CorruptData));
    }
    let mut meta = transaction.open_table(META).map_err(table_error)?;
    for namespace in N::ALL
        .into_iter()
        .filter(|n| n.requires_v3_activation() && n.metadata_key().is_some())
    {
        if meta
            .get(key(namespace)?)
            .map_err(precommit_storage_error)?
            .is_some()
        {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
    }
    let read = |key: &str| -> Result<Vec<u8>, StorageError> {
        let value = meta
            .get(key)
            .map_err(precommit_storage_error)?
            .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
        if value.value().len() > 512 {
            return Err(storage_error(StorageErrorKind::LimitExceeded));
        }
        Ok(value.value().to_vec())
    };
    let database = decode_database_identity_v1(&read(META_DATABASE_ID)?)
        .map_err(codec_error)?
        .into_parts()
        .0;
    let incarnation = decode_history_incarnation_v1(&read(META_HISTORY_INCARNATION)?)
        .map_err(codec_error)?
        .into_parts()
        .0;
    let application = decode_application_sequence_allocator_v1(&read(META_APPLICATION_SEQUENCE)?)
        .map_err(codec_error)?
        .into_parts()
        .0;
    let administration =
        decode_administration_sequence_allocator_v1(&read(META_ADMINISTRATION_SEQUENCE)?)
            .map_err(codec_error)?
            .into_parts()
            .0;
    let application = match application {
        ApplicationSequenceAllocator::Next(next) => CommitSequence::new(next.get() - 1),
        ApplicationSequenceAllocator::Exhausted => CommitSequence::new(u64::MAX),
    };
    let administration = match administration {
        AdministrationSequenceAllocator::Next(next) => AdministrationSequence::new(next.get() - 1),
        AdministrationSequenceAllocator::Exhausted => AdministrationSequence::new(u64::MAX),
    };
    if database != lineage.database_id()
        || incarnation != lineage.history_incarnation()
        || DualFrontier::new(application, administration) != frontier
    {
        return Err(storage_error(StorageErrorKind::CorruptData));
    }
    let before_registry = read(META_RECORD_REGISTRY)?;
    let before_digest = decode_record_registry_v2(&before_registry)
        .map_err(codec_error)?
        .into_parts()
        .0;
    if before_digest != PRE_V3_REGISTRY && before_digest != current_record_registry_digest() {
        return Err(storage_error(StorageErrorKind::IncompatibleFormat));
    }
    let registry =
        encode_record_registry_v2(current_record_registry_digest()).map_err(codec_error)?;
    let mutations = if before_registry == registry.as_bytes() {
        vec![]
    } else {
        // The codec's expected-state helper owns SHA-256. No inventory or
        // metadata row is inferred from a caller-provided mutation list.
        vec![
            AuthoritativeMutationV3::replace(
                N::RecordRegistry,
                META_RECORD_REGISTRY.as_bytes(),
                &before_registry,
                registry.as_bytes(),
            )
            .map_err(|_| storage_error(StorageErrorKind::LimitExceeded))?,
        ]
    };
    let (sequence, allocator) = ChangelogTransactionAllocator::initial()
        .allocate_one()
        .map_err(|_| storage_error(StorageErrorKind::SequenceExhausted))?;
    let receipt = AuthoritativeTransactionV3::new(
        AuthoritativeTransactionBindingV3 {
            database_id: lineage.database_id(),
            history_incarnation: lineage.history_incarnation(),
            predecessor: None,
            sequence,
            predecessor_frontier: frontier,
            covered_frontier: frontier,
            prior_history_hash: [0; 32],
        },
        ChangelogAttributionV3::V3Activation,
        mutations,
    )
    .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
    let point = ChangelogHistoryPointV3::from_receipt(&receipt)
        .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
    let history = ChangelogHistoryStateV3::new(lineage, point, point, point)
        .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
    let roots = [
        (
            N::AuthoritativeStateCatalog,
            encode_authoritative_state_catalog_v1(AuthoritativeStateCatalogV1),
        ),
        (
            N::LeadershipEpoch,
            encode_leadership_epoch_v1(lineage.leadership_epoch()),
        ),
        (
            N::ChangelogHistoryState,
            encode_changelog_history_state_v3(history),
        ),
        (
            N::NextChangelogTransaction,
            encode_changelog_transaction_allocator_v3(allocator),
        ),
        (
            N::ReplicationFollowerState,
            encode_replication_follower_state_v3(ReplicationFollowerStateV3::detached()),
        ),
    ]
    .into_iter()
    .map(|(namespace, value)| value.map(|value| (namespace, value)).map_err(codec_error))
    .collect::<Result<Vec<_>, _>>()?;
    let receipt = receipt
        .encode()
        .map_err(|_| storage_error(StorageErrorKind::LimitExceeded))?;
    activation_edge("preflight");
    for (namespace, value) in roots {
        meta.insert(key(namespace)?, value.as_bytes())
            .map_err(precommit_storage_error)?;
    }
    meta.insert(META_RECORD_REGISTRY, registry.as_bytes())
        .map_err(precommit_storage_error)?;
    drop(meta);
    activation_edge("roots");
    transaction.open_table(SOURCE_HOLDS).map_err(table_error)?;
    transaction
        .open_table(HISTORY)
        .map_err(table_error)?
        .insert(sequence.get().to_be_bytes().as_slice(), receipt.as_slice())
        .map_err(precommit_storage_error)?;
    activation_edge("receipt");
    transaction.commit().map_err(commit_error)?;
    activation_edge("committed");
    Ok(history)
}

fn activation_edge(_edge: &str) {
    #[cfg(test)]
    if std::env::var("RIFFDB_WP772_ACTIVATION_CRASH").as_deref() == Ok(_edge) {
        std::process::exit(91);
    }
}

#[cfg(test)]
#[path = "changelog_v3_activation_tests.rs"]
mod tests;
