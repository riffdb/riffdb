//! Streaming exact command-segment manifest and uniqueness validation for retained segments.
use super::*;
use riffdb_storage_api::DurableCodecErrorKind;

pub(super) fn validate(
    transaction: &ReadTransaction,
    cancellation: &AtomicBool,
) -> Result<(), StorageError> {
    let metadata = read_retained_metadata(transaction)?;
    let commits = transaction.open_table(COMMITS).map_err(table_error)?;
    let mut idempotency_keys = std::collections::BTreeSet::new();
    let mut provenance_keys = std::collections::BTreeSet::new();
    let mut audit_keys = std::collections::BTreeSet::new();
    let mut predecessor = None;
    for row in commits.iter().map_err(precommit_storage_error)? {
        if cancellation.load(Ordering::Acquire) {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        let (key, value) = row.map_err(precommit_storage_error)?;
        let decoded = match crate::history_stub::decode_retained_segment(value.value()) {
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
            let identity = base
                .outcome()
                .identity()
                .storage_key()
                .map_err(|_| corrupt())?;
            if !idempotency_keys.insert(keys::encode_idempotency_key(&identity).to_vec())
                || !provenance_keys
                    .insert(keys::encode_provenance_key(base.provenance().provenance_id()).to_vec())
            {
                return Err(corrupt());
            }
            for audit in [base.started_audit(), base.terminal_audit()] {
                if !audit_keys.insert(
                    keys::encode_audit_by_request_key(
                        audit.request_id(),
                        audit.administration_sequence(),
                    )
                    .to_vec(),
                ) {
                    return Err(corrupt());
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use riffdb_storage_api::DatabaseInitializationPort;

    // req: REP-007, AFC-007
    #[test]
    fn private_graph_locator_presence_preserves_legacy_compatibility_and_requires_new_rows() {
        let scope = crate::test_path::ScopedDirectory::new("private-graph-locators");
        let mut store = crate::RedbStore::open(scope.join("fixture.redb")).unwrap();
        let database_id =
            DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x41; 10]).unwrap();
        store.initialize_database(database_id).unwrap();
        let read = store.shared.database.begin_read().unwrap();
        validate(&read, &AtomicBool::new(false)).unwrap();
    }
}
