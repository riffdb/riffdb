//! Secondary-index row typing and command-owned generation buckets. Catalog
//! key/value derivation and complete mutation inventory are separate proofs.

use std::collections::BTreeSet;

use riffdb_storage_api::{
    AuthoritativeNamespaceV1 as N, DurableCodecError, DurableCodecErrorKind, PartitionIndexTarget,
    StoredCommandCapsuleV2, StoredIndexEntryV2,
};

fn corrupt() -> DurableCodecError {
    DurableCodecError::new(DurableCodecErrorKind::CorruptData)
}

pub(super) fn validate_post_images(
    command: &StoredCommandCapsuleV2,
) -> Result<(), DurableCodecError> {
    let Some(prefix) = command.prefix_evidence() else {
        return Ok(());
    };
    let indexes = command
        .index_generation_transitions()
        .iter()
        .map(|advance| advance.target().index_id())
        .collect::<BTreeSet<_>>();
    // AtomicCommandRecordSet requires every written epoch and index image to
    // retain the command's schema binding, including multi-aggregate commands.
    if command
        .index_generation_transitions()
        .iter()
        .any(|advance| {
            !advance
                .post_image()
                .schema_binding()
                .matches_plan(command.base().commit().plan())
        })
    {
        return Err(corrupt());
    }
    for mutation in prefix
        .mutations()
        .iter()
        .filter(|m| m.namespace() == N::SecondaryIndexes)
    {
        let key = crate::keys::decode_index_entry_key(mutation.key()).map_err(|_| corrupt())?;
        if !indexes.contains(&key.index_id()) {
            return Err(corrupt());
        }
        if let Some(bytes) = mutation.value() {
            let decoded = riffdb_storage_api::decode_index_entry_v2(bytes)?;
            let entry = decoded.value();
            if entry.key() != &key
                || !entry
                    .schema_binding()
                    .matches_plan(command.base().commit().plan())
            {
                return Err(corrupt());
            }
            require_bucket(command, entry)?;
        }
    }
    Ok(())
}

pub(super) fn validate_prior(
    command: &StoredCommandCapsuleV2,
    key: &[u8],
    bytes: Option<&[u8]>,
) -> Result<(), DurableCodecError> {
    let Some(bytes) = bytes else {
        return Ok(());
    };
    let decoded = riffdb_storage_api::decode_index_entry_v2(bytes)?;
    let entry = decoded.value();
    if entry.key().as_bytes() != key {
        return Err(corrupt());
    }
    // A deleted/replaced image may retain a historical schema binding. Its
    // actual owning partition/index bucket must still advance in this command.
    require_bucket(command, entry)
}

fn require_bucket(
    command: &StoredCommandCapsuleV2,
    entry: &StoredIndexEntryV2,
) -> Result<(), DurableCodecError> {
    let target = PartitionIndexTarget::new(entry.partition_key().clone(), entry.key().index_id());
    command
        .index_generation_transitions()
        .binary_search_by(|advance| advance.target().cmp(&target))
        .map(|_| ())
        .map_err(|_| corrupt())
}
