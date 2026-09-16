//! V7 independent mutation validation against a received original receipt.
//! This layer does not grant reconstruction/publication authority or replace
//! the command, entity/index, catalog and structural validators.

mod predecessor;
mod rows;

pub(crate) use rows::{decode_capsule, decode_segment};

use redb::WriteTransaction;
use riffdb_storage_api::{
    AuthoritativeNamespaceV1 as N, AuthoritativeTransactionV3, ChangelogAttributionV3,
    DurableCodecErrorKind, StorageError, StorageErrorKind, validate_command_prefix_mutations_v1,
};

use crate::{
    changelog_v3_write::{check_predecessor, value_error},
    error::{codec_error, storage_error},
    keys::encode_application_sequence_key,
};

/// One transaction-private command's bounded independent mutation fold. The
/// application owner scopes this around staging; graph and allocator writes
/// occur after it closes. An ignored capture error permanently poisons it.
#[derive(Default)]
pub(crate) struct CommandMutationCapture {
    active: Option<riffdb_storage_api::AuthoritativeMutationAccumulatorV3>,
    budget: usize,
    failure: Option<StorageErrorKind>,
}

impl CommandMutationCapture {
    pub(crate) fn begin(&mut self, budget: usize) -> Result<(), StorageError> {
        if self.failure.is_some()
            || self.active.is_some()
            || !(40..=riffdb_storage_api::MAX_STAGED_WRITE_BYTES).contains(&budget)
        {
            return self.refuse(StorageErrorKind::InvariantViolation);
        }
        self.budget = budget;
        self.active = Some(Default::default());
        Ok(())
    }

    fn refuse<T>(&mut self, kind: StorageErrorKind) -> Result<T, StorageError> {
        self.failure.get_or_insert(kind);
        Err(storage_error(kind))
    }

    pub(crate) fn record(
        &mut self,
        mutation: &crate::journal::JournalMutation,
    ) -> Result<(), StorageError> {
        if let Some(kind) = self.failure {
            return Err(storage_error(kind));
        }
        let Some(active) = self.active.as_mut() else {
            return Ok(());
        };
        let result = (|| {
            use riffdb_storage_api::{AuthoritativeMutationV3 as M, ChangelogV3Error as E};
            let namespace = riffdb_storage_api::AuthoritativeStateCatalogV1
                .lookup(mutation.table().label(), mutation.key())
                .ok_or(E::InvalidNamespace)?;
            if !riffdb_storage_api::CommandPrefixEvidenceV1::supports_namespace(namespace) {
                return Err(E::InvalidNamespace);
            }
            let captured = match mutation.value() {
                Some(value) => M::put(namespace, mutation.key(), mutation.expected_hash(), value)?,
                None => M::delete(
                    namespace,
                    mutation.key(),
                    mutation.expected_hash().ok_or(E::InvalidEncoding)?,
                )?,
            };
            active.record(captured)
        })();
        match result {
            Ok(()) => Ok(()),
            Err(error) => self.refuse(value_error(error).kind()),
        }
    }

    pub(crate) fn finish(
        &mut self,
    ) -> Result<Vec<riffdb_storage_api::AuthoritativeMutationV3>, StorageError> {
        if let Some(kind) = self.failure {
            return Err(storage_error(kind));
        }
        let Some(active) = self.active.take() else {
            return self.refuse(StorageErrorKind::InvariantViolation);
        };
        let mutations = match active.finish() {
            Ok(value) => value,
            Err(error) => return self.refuse(value_error(error).kind()),
        };
        let bytes = mutations.iter().try_fold(40_usize, |bytes, mutation| {
            bytes.checked_add(mutation.encoded_len())
        });
        if bytes.is_none_or(|bytes| bytes > self.budget) {
            // The presequence reservation already charged this complete copy.
            // Exceeding it is a broken internal proof, never a late capacity
            // refusal or permission to publish the partly staged command.
            return self.refuse(StorageErrorKind::InvariantViolation);
        }
        Ok(mutations)
    }
}

/// Runs before applying the original receipt. The transaction is pinned to that
/// receipt's predecessor, including earlier receipts in the same private frame.
/// Legacy command groups remain readable; they cannot supply exact-stop evidence.
pub(crate) fn validate_received_prefixes(
    transaction: &WriteTransaction,
    tables: &std::collections::BTreeSet<&'static str>,
    receipt: &AuthoritativeTransactionV3,
) -> Result<(), StorageError> {
    // Retention can re-encode an old segment without advancing its commands.
    // Its retained prefix frontiers belong to the original command receipt,
    // not the administrative transaction that rewrites the segment.
    if !matches!(
        receipt.attribution(),
        ChangelogAttributionV3::JournaledApplicationGroup
            | ChangelogAttributionV3::DirectApplicationOrServiceAuditGroup
    ) {
        return Ok(());
    }
    let corrupt = || storage_error(StorageErrorKind::CorruptData);
    let mut segments = Vec::new();
    let mut command_count = 0_usize;
    let mut legacy = false;
    for mutation in receipt
        .mutations()
        .iter()
        .filter(|m| m.namespace() == N::Commits)
    {
        let Some(value) = mutation.value() else {
            legacy = true;
            continue;
        };
        let segment = match rows::decode_segment_images(value) {
            Ok(decoded) => decoded.into_parts().0,
            Err(error) if error.kind() == DurableCodecErrorKind::UnexpectedRecordType => {
                legacy = true;
                continue;
            }
            Err(error) => return Err(codec_error(error)),
        };
        if segment.commands()[0].prefix_evidence().is_none() {
            legacy = true;
            continue;
        }
        if encode_application_sequence_key(segment.first_commit_sequence()) != mutation.key()
            || segment.database_id() != receipt.binding().database_id
            || segment.history_incarnation() != receipt.binding().history_incarnation
            || mutation.expected_hash().is_some()
        {
            return Err(corrupt());
        }
        command_count = command_count
            .checked_add(segment.commands().len())
            .filter(|count| *count <= riffdb_storage_api::MAX_STAGED_COMMANDS)
            .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
        segments.push(segment);
    }
    if segments.is_empty() {
        return Ok(());
    }
    if legacy {
        return Err(corrupt());
    }
    // The receipt's bounded canonical segment bytes own every payload. Retain
    // borrowed evidence here; neither this list nor first-observation lookup
    // copies independent post-images from the decoded segments.
    let evidence = segments
        .iter()
        .flat_map(|segment| segment.commands())
        .map(|command| command.prefix_evidence().ok_or_else(corrupt))
        .collect::<Result<Vec<_>, _>>()?;
    validate_independent_prefixes(transaction, tables, &evidence, receipt)?;
    predecessor::validate_commands(
        transaction,
        tables,
        segments.iter().flat_map(|segment| segment.commands()),
    )
}

fn validate_independent_prefixes<
    P: std::borrow::Borrow<riffdb_storage_api::CommandPrefixEvidenceV1>,
>(
    transaction: &WriteTransaction,
    tables: &std::collections::BTreeSet<&'static str>,
    evidence: &[P],
    receipt: &AuthoritativeTransactionV3,
) -> Result<(), StorageError> {
    for first in validate_command_prefix_mutations_v1(evidence, receipt).map_err(value_error)? {
        if !tables.contains(first.namespace().table()) {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        // Includes keys with no net mutation in the receipt. Merely checking
        // the receipt's preconditions would leave these false histories hidden.
        check_predecessor(transaction, first)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use redb::ReadableTable;
    use riffdb_storage_api::{
        AuthoritativeMutationV3 as M, AuthoritativeTransactionBindingV3,
        ChangelogTransactionSequence, CommandPrefixEvidenceV1 as Prefix,
    };
    use riffdb_types::{AdministrationSequence, CommitSequence, DatabaseId, DualFrontier};

    fn frontier(sequence: u64) -> DualFrontier {
        DualFrontier::new(
            CommitSequence::new(sequence),
            AdministrationSequence::new(sequence * 2),
        )
    }

    #[test]
    // req: REP-007
    fn command_capture_keeps_command_boundaries_and_poisoned_refusals() {
        use crate::journal::{JournalMutation as J, JournalTable as T};
        let mut capture = CommandMutationCapture::default();
        capture
            .record(&J::put(T::Commits, b"graph".as_slice(), b"opaque".as_slice()).unwrap())
            .unwrap();
        capture.begin(100).unwrap();
        capture
            .record(&J::put(T::Entities, b"key".as_slice(), b"first".as_slice()).unwrap())
            .unwrap();
        let first = capture.finish().unwrap();
        capture.begin(100).unwrap();
        capture
            .record(
                &J::replace(T::Entities, b"key".as_slice(), b"first", b"last".as_slice()).unwrap(),
            )
            .unwrap();
        let last = capture.finish().unwrap();
        assert_eq!(first[0].value(), Some(b"first".as_slice()));
        assert_eq!(last[0].value(), Some(b"last".as_slice()));
        assert!(last[0].matches_prior(first[0].value()));

        for bad in [
            J::put(T::Commits, b"graph".as_slice(), b"opaque".as_slice()).unwrap(),
            J::replace(T::Entities, b"key".as_slice(), b"wrong", b"bad".as_slice()).unwrap(),
        ] {
            let mut refused = CommandMutationCapture::default();
            refused.begin(100).unwrap();
            refused
                .record(&J::put(T::Entities, b"key".as_slice(), b"first".as_slice()).unwrap())
                .unwrap();
            assert!(refused.record(&bad).is_err());
            assert!(refused.finish().is_err());
            assert!(refused.begin(100).is_err());
        }
        let mut underestimated = CommandMutationCapture::default();
        underestimated.begin(40).unwrap();
        underestimated
            .record(&J::put(T::Entities, b"key".as_slice(), b"first".as_slice()).unwrap())
            .unwrap();
        assert!(underestimated.finish().is_err());
        assert!(underestimated.begin(100).is_err());
    }

    #[test]
    // req: REP-007, REP-003
    fn net_zero_prefixes_check_actual_predecessor_without_mutating_it() {
        let scope = crate::test_path::ScopedDirectory::new("prefix-predecessor");
        let database = redb::Database::create(scope.join("db.redb")).unwrap();
        let transaction = database.begin_write().unwrap();
        let receipt = AuthoritativeTransactionV3::new(
            AuthoritativeTransactionBindingV3 {
                database_id: DatabaseId::from_unix_milliseconds_and_random(1, [7; 10]).unwrap(),
                history_incarnation: 1,
                predecessor: None,
                sequence: ChangelogTransactionSequence::new(1).unwrap(),
                predecessor_frontier: frontier(0),
                covered_frontier: frontier(2),
                prior_history_hash: [0; 32],
            },
            ChangelogAttributionV3::JournaledApplicationGroup,
            vec![M::put(N::Commits, b"opaque-graph", None, b"graph").unwrap()],
        )
        .unwrap();
        // This checks only the independent predecessor layer. Opaque graph
        // bytes are never passed to a decoder or given startup/publication proof.
        for initial in [None, Some(b"initial".as_slice())] {
            let first = match initial {
                None => M::put(N::Entities, b"key", None, b"temporary"),
                Some(before) => M::replace(N::Entities, b"key", before, b"temporary"),
            }
            .unwrap();
            let last = match initial {
                None => M::delete_matching(N::Entities, b"key", b"temporary"),
                Some(after) => M::replace(N::Entities, b"key", b"temporary", after),
            }
            .unwrap();
            let evidence = [
                Prefix::new(frontier(0), frontier(1), vec![first]).unwrap(),
                Prefix::new(frontier(1), frontier(2), vec![last]).unwrap(),
            ];
            let tables = [N::Entities.table()].into_iter().collect();
            for physical in [
                None,
                Some(b"initial".as_slice()),
                Some(b"unrelated".as_slice()),
            ] {
                let mut table = transaction.open_table(crate::layout::ENTITIES).unwrap();
                match physical {
                    Some(value) => {
                        table.insert(b"key".as_slice(), value).unwrap();
                    }
                    None => {
                        table.remove(b"key".as_slice()).unwrap();
                    }
                }
                drop(table);
                let result =
                    validate_independent_prefixes(&transaction, &tables, &evidence, &receipt);
                assert_eq!(result.is_ok(), physical == initial);
                let table = transaction.open_table(crate::layout::ENTITIES).unwrap();
                let retained = table.get(b"key".as_slice()).unwrap();
                assert_eq!(retained.as_ref().map(|row| row.value()), physical);
            }
            transaction.delete_table(crate::layout::ENTITIES).unwrap();
            let absent = crate::changelog_v3_write::table_inventory(&transaction).unwrap();
            assert!(absent.is_empty());
            assert!(
                validate_independent_prefixes(&transaction, &absent, &evidence, &receipt).is_err()
            );
            assert_eq!(transaction.list_tables().unwrap().count(), 0);
        }
        transaction.abort().unwrap();
    }
}
