//! Streaming exact reciprocal locator inventory for retained command segments.
use super::*;
use crate::layout::{AUDIT_BY_REQUEST_LOCATORS, IDEMPOTENCY_LOCATORS, PROVENANCE_LOCATORS};
use riffdb_storage_api::{DurableCodecErrorKind, StoredCommandLocatorV1};

pub(super) fn validate(
    transaction: &ReadTransaction,
    cancellation: &AtomicBool,
) -> Result<(), StorageError> {
    let metadata = read_retained_metadata(transaction)?;
    let commits = transaction.open_table(COMMITS).map_err(table_error)?;
    let idempotency = transaction
        .open_table(IDEMPOTENCY_LOCATORS)
        .map_err(table_error)?;
    let provenance = transaction
        .open_table(PROVENANCE_LOCATORS)
        .map_err(table_error)?;
    let audits = transaction
        .open_table(AUDIT_BY_REQUEST_LOCATORS)
        .map_err(table_error)?;
    let mut counts = [0_u64; 3];
    let mut predecessor = None;
    for row in commits.iter().map_err(precommit_storage_error)? {
        if cancellation.load(Ordering::Acquire) {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        let (key, value) = row.map_err(precommit_storage_error)?;
        let decoded = match crate::command_prefix::decode_segment(value.value()) {
            Ok(decoded) => decoded,
            Err(error) if error.kind() == DurableCodecErrorKind::UnexpectedRecordType => continue,
            Err(error) => return Err(crate::error::codec_error(error)),
        };
        let segment = decoded.value();
        if segment.database_id() != metadata.database_id()
            || segment.history_incarnation() > metadata.history_incarnation()
            || key.value() != keys::encode_application_sequence_key(segment.first_commit_sequence())
            || predecessor
                .is_some_and(|digest| segment.predecessor_segment_digest() != Some(digest))
            || crate::application::build_command_segment_manifest(
                segment.commands(),
                segment.first_commit_sequence(),
            )? != *segment.manifest()
        {
            return Err(corrupt());
        }
        predecessor = Some(segment.segment_digest());
        for command in segment.commands() {
            let base = command.base();
            let locator = crate::codec::encode_command_locator_v1(StoredCommandLocatorV1::new(
                base.commit_sequence(),
            ))?;
            let identity = base
                .outcome()
                .identity()
                .storage_key()
                .map_err(|_| corrupt())?;
            let required = command.prefix_evidence().is_some();
            counts[0] = counts[0]
                .checked_add(u64::from(require(
                    &idempotency,
                    keys::encode_idempotency_key(&identity),
                    locator.as_bytes(),
                    required,
                )?))
                .ok_or_else(limit_exceeded)?;
            counts[1] = counts[1]
                .checked_add(u64::from(require(
                    &provenance,
                    &keys::encode_provenance_key(base.provenance().provenance_id()),
                    locator.as_bytes(),
                    required,
                )?))
                .ok_or_else(limit_exceeded)?;
            for audit in [base.started_audit(), base.terminal_audit()] {
                counts[2] = counts[2]
                    .checked_add(u64::from(require(
                        &audits,
                        &keys::encode_audit_by_request_key(
                            audit.request_id(),
                            audit.administration_sequence(),
                        ),
                        locator.as_bytes(),
                        required,
                    )?))
                    .ok_or_else(limit_exceeded)?;
            }
        }
    }
    if idempotency.len().map_err(precommit_storage_error)? != counts[0]
        || provenance.len().map_err(precommit_storage_error)? != counts[1]
        || audits.len().map_err(precommit_storage_error)? != counts[2]
    {
        return Err(corrupt());
    }
    Ok(())
}

fn require(
    table: &ReadOnlyTable<&[u8], &[u8]>,
    key: &[u8],
    expected: &[u8],
    required: bool,
) -> Result<bool, StorageError> {
    // ADR-0165 installs empty locator tables without a registry transition.
    // Old segment manifests remain complete logical authority without these
    // rows. V7 commands are written after that transition and require them.
    let Some(value) = table.get(key).map_err(precommit_storage_error)? else {
        return if required { Err(corrupt()) } else { Ok(false) };
    };
    if value.value() != expected {
        return Err(corrupt());
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    // req: REP-007, AFC-007
    #[test]
    fn private_graph_locator_presence_preserves_legacy_compatibility_and_requires_new_rows() {
        let scope = crate::test_path::ScopedDirectory::new("private-graph-locators");
        let database = redb::Database::create(scope.join("fixture.redb")).unwrap();
        let write = database.begin_write().unwrap();
        drop(write.open_table(IDEMPOTENCY_LOCATORS).unwrap());
        write.commit().unwrap();
        {
            let read = database.begin_read().unwrap();
            let table = read.open_table(IDEMPOTENCY_LOCATORS).unwrap();
            assert!(!require(&table, b"key", b"expected", false).unwrap());
            assert_eq!(
                require(&table, b"key", b"expected", true)
                    .unwrap_err()
                    .kind(),
                StorageErrorKind::CorruptData
            );
        }
        let write = database.begin_write().unwrap();
        write
            .open_table(IDEMPOTENCY_LOCATORS)
            .unwrap()
            .insert(b"key".as_slice(), b"wrong".as_slice())
            .unwrap();
        write.commit().unwrap();
        let read = database.begin_read().unwrap();
        let table = read.open_table(IDEMPOTENCY_LOCATORS).unwrap();
        for required in [false, true] {
            assert_eq!(
                require(&table, b"key", b"expected", required)
                    .unwrap_err()
                    .kind(),
                StorageErrorKind::CorruptData
            );
            assert!(require(&table, b"key", b"wrong", required).unwrap());
        }
    }
}
