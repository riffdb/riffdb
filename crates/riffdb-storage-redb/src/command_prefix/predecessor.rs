//! Typed logical predecessor joins over the receipt's pinned physical state.
//! Earlier prefix post-images are borrowed, including deletions, so a physical
//! group can be checked without changing the transaction or copying its values.

use std::collections::{BTreeMap, BTreeSet};

use redb::{ReadableTable, TableDefinition, WriteTransaction};
use riffdb_storage_api::{
    AuthoritativeNamespaceV1 as N, EntityChainStateV1 as State, IndexEpochPosition, StorageError,
    StorageErrorKind, StoredCommandCapsuleV2,
};

use crate::error::{codec_error, precommit_storage_error, storage_error, table_error};

fn corrupt() -> StorageError {
    storage_error(StorageErrorKind::CorruptData)
}

type PhysicalPrior<'a> = Option<(&'a WriteTransaction, &'a BTreeSet<&'static str>)>;

type PriorImages<'a> = BTreeMap<(N, &'a [u8]), Option<&'a [u8]>>;

/// The caller has already bounded all prefixes, validated their row inventories
/// and net reduction, and checked each first observation's physical row hash.
pub(super) fn validate_commands<'a>(
    transaction: &WriteTransaction,
    tables: &BTreeSet<&'static str>,
    commands: impl IntoIterator<Item = &'a StoredCommandCapsuleV2>,
) -> Result<(), StorageError> {
    validate_sequence(Some((transaction, tables)), commands)
}

/// A retained segment may outlive its original receipt and physical prior rows.
/// Check every predecessor present in its own prefixes; the complete structural
/// history validators still own joins across retained segment boundaries.
pub(super) fn validate_retained_commands(
    commands: &[StoredCommandCapsuleV2],
) -> Result<(), StorageError> {
    if commands
        .first()
        .is_none_or(|command| command.prefix_evidence().is_none())
    {
        return Ok(());
    }
    validate_sequence(None, commands)
}

fn validate_sequence<'a>(
    physical: PhysicalPrior<'_>,
    commands: impl IntoIterator<Item = &'a StoredCommandCapsuleV2>,
) -> Result<(), StorageError> {
    let mut prior = PriorImages::new();
    for command in commands {
        let prefix = command.prefix_evidence().ok_or_else(corrupt)?;
        for transition in command.entity_transitions() {
            let key = crate::keys::encode_entity_key(transition.target().key());
            with_prior(physical, &prior, N::Entities, key, |bytes| {
                match (transition.prior_state(), bytes) {
                    (
                        State::Live {
                            version,
                            value_hash,
                        },
                        Some(bytes),
                    ) => {
                        let decoded = riffdb_storage_api::decode_entity_record_v1(bytes)
                            .map_err(codec_error)?;
                        let record = decoded.value();
                        if record.target() != transition.target()
                            || record.entity_version() != version
                            || riffdb_storage_api::derive_entity_record_hash_v1(record)
                                .map_err(|_| corrupt())?
                                != value_hash
                        {
                            return Err(corrupt());
                        }
                        Ok(())
                    }
                    (State::NeverExisted | State::Deleted, None) => Ok(()),
                    _ => Err(corrupt()),
                }
            })?;
            with_prior(physical, &prior, N::EntityChainHeads, key, |bytes| {
                match bytes {
                    Some(bytes) => riffdb_storage_api::decode_entity_chain_head_v1(bytes)
                        .map_err(codec_error)?
                        .value()
                        .apply(transition),
                    None => riffdb_storage_api::EntityChainHeadV1::from_genesis(transition),
                }
                .map(|_| ())
                .map_err(|_| corrupt())
            })?;
        }
        for advance in command.index_generation_transitions() {
            let key = crate::keys::encode_partition_index_key(advance.target());
            with_prior(physical, &prior, N::IndexEpochs, &key, |bytes| {
                match (advance.prior(), bytes) {
                    (IndexEpochPosition::BeforeFirst, None) => Ok(()),
                    (IndexEpochPosition::Value(epoch), Some(bytes)) => {
                        let decoded = riffdb_storage_api::decode_index_epoch_v1(bytes)
                            .map_err(codec_error)?;
                        if decoded.value().target() != advance.target()
                            || decoded.value().epoch() != epoch
                        {
                            return Err(corrupt());
                        }
                        Ok(())
                    }
                    _ => Err(corrupt()),
                }
            })?;
        }
        for row in prefix
            .mutations()
            .iter()
            .filter(|row| row.namespace() == N::SecondaryIndexes)
        {
            with_prior(physical, &prior, N::SecondaryIndexes, row.key(), |bytes| {
                super::secondary::validate_prior(command, row.key(), bytes).map_err(codec_error)
            })?;
        }
        // This map is bounded by the already checked segment/receipt-wide
        // item and byte ceilings. A present None shadows a deleted physical row.
        for row in prefix.mutations() {
            let key = (row.namespace(), row.key());
            if physical.is_none()
                && prior
                    .get(&key)
                    .is_some_and(|before| !row.matches_prior(*before))
            {
                return Err(corrupt());
            }
            // On receipt application the existing net reduction already proves
            // all sequential raw hashes, including keys with a zero net effect.
            prior.insert(key, row.value());
        }
    }
    Ok(())
}

fn with_prior(
    physical: PhysicalPrior<'_>,
    prior: &PriorImages<'_>,
    namespace: N,
    key: &[u8],
    check: impl FnOnce(Option<&[u8]>) -> Result<(), StorageError>,
) -> Result<(), StorageError> {
    if let Some(bytes) = prior.get(&(namespace, key)) {
        return check(*bytes);
    }
    let Some((transaction, tables)) = physical else {
        return Ok(());
    };
    if !tables.contains(namespace.table()) {
        // redb's write-transaction open_table would otherwise create a table.
        return Err(corrupt());
    }
    let definition: TableDefinition<&[u8], &[u8]> = TableDefinition::new(namespace.table());
    let table = transaction.open_table(definition).map_err(table_error)?;
    let row = table.get(key).map_err(precommit_storage_error)?;
    let bytes = row.as_ref().map(|row| row.value());
    if bytes.is_some_and(|bytes| bytes.len() > riffdb_storage_api::MAX_CHANGELOG_FRAME_BYTES) {
        return Err(storage_error(StorageErrorKind::LimitExceeded));
    }
    check(bytes)
}
