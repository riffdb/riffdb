//! Exact immutable V3 prefix binding for private staged migration witnesses.
//! Only allocator/tail progress is mutable; no durable identity or permit is added.

use super::*;
use riffdb_storage_api::{AuthoritativeTransactionV3, ChangelogHistoryStateV3};

pub(super) fn capture_v3_history(
    transaction: &redb::ReadTransaction,
) -> Result<Option<ChangelogHistoryStateV3>, StorageError> {
    crate::changelog_v3_roots::validate_retained_history(transaction)
}

pub(super) fn hash_v3_prefix(
    transaction: &redb::ReadTransaction,
    expected: Option<ChangelogHistoryStateV3>,
    digest: &mut Sha256,
) -> Result<(), StorageError> {
    let current = capture_v3_history(transaction)?;
    let Some(expected) = expected else {
        return if current.is_none() {
            Ok(())
        } else {
            Err(corrupt())
        };
    };
    let current = current.ok_or_else(corrupt)?;
    if current.lineage() != expected.lineage()
        || current.anchor() != expected.anchor()
        || current.minimum_resume() != expected.minimum_resume()
        || current.tail().sequence() < expected.tail().sequence()
    {
        return Err(corrupt());
    }
    // Complete streaming validation above links this exact predecessor checksum
    // through every newer receipt, including batches committed before a restart.
    // A rewritten or pruned predecessor cannot masquerade as permitted progress.
    let table = transaction
        .open_table(crate::changelog_v3_activation::HISTORY)
        .map_err(table_error)?;
    let row = table
        .get(expected.tail().sequence().get().to_be_bytes().as_slice())
        .map_err(precommit_storage_error)?
        .ok_or_else(corrupt)?;
    let receipt = AuthoritativeTransactionV3::decode(row.value()).map_err(|_| corrupt())?;
    expected
        .validate_terminal_receipt(&receipt)
        .map_err(|_| corrupt())?;
    let encoded = riffdb_storage_api::proto_codec::encode_changelog_history_state_v3(expected)
        .map_err(crate::error::codec_error)?;
    hash_component(digest, b"migration-immutable-v3-prefix")?;
    hash_component(digest, encoded.as_bytes())
}
