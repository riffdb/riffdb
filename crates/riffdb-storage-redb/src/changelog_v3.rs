//! V3 receipt formation from validated journal mutations, never latest-row reads.

use riffdb_storage_api::proto_codec::{
    decode_changelog_transaction_allocator_v3, encode_changelog_transaction_allocator_v3,
};
use riffdb_storage_api::{
    AuthoritativeMutationAccumulatorV3, AuthoritativeMutationV3, AuthoritativeNamespaceV1,
    AuthoritativeStateCatalogV1, AuthoritativeTransactionBindingV3, AuthoritativeTransactionV3,
    ChangelogAttributionV3, ChangelogTransactionAllocator, ChangelogTransactionSequence,
    ChangelogV3Error, CompositeMutationV1, ReplicationAuthorityClassV1,
};
use riffdb_types::DualFrontier;
use sha2::{Digest, Sha256};

use crate::journal::{JournalFrame, JournalFrameKind, JournalMutation, JournalTable};

/// Checks the one-step physical allocation carried by a journal mutation.
/// This is shape/source validation, not permission to apply it: replay still
/// requires the retained, fully validated V3 activation roots and predecessor.
/// Position one is reserved for Immediate activation and cannot be journaled.
pub(crate) fn journal_allocator_assignment(
    mutation: &JournalMutation,
) -> Result<ChangelogTransactionSequence, ChangelogV3Error> {
    allocator_assignment(
        mutation.table(),
        mutation.key(),
        mutation.expected_hash(),
        mutation.value(),
    )
}

fn allocator_assignment(
    table: JournalTable,
    key: &[u8],
    expected: Option<[u8; 32]>,
    value: Option<&[u8]>,
) -> Result<ChangelogTransactionSequence, ChangelogV3Error> {
    let (Some(expected), Some(value)) = (expected, value) else {
        return Err(ChangelogV3Error::InvalidEncoding);
    };
    if table != JournalTable::Meta
        || AuthoritativeStateCatalogV1.lookup(table.label(), key)
            != Some(AuthoritativeNamespaceV1::NextChangelogTransaction)
    {
        return Err(ChangelogV3Error::InvalidNamespace);
    }
    let decoded = decode_changelog_transaction_allocator_v3(value)
        .map_err(|_| ChangelogV3Error::InvalidEncoding)?;
    let assigned = match *decoded.value() {
        ChangelogTransactionAllocator::Next(next) => next
            .get()
            .checked_sub(1)
            .and_then(ChangelogTransactionSequence::new)
            .ok_or(ChangelogV3Error::PredecessorMismatch)?,
        ChangelogTransactionAllocator::Exhausted => ChangelogTransactionSequence::new(u64::MAX)
            .ok_or(ChangelogV3Error::SequenceExhausted)?,
    };
    if assigned.get() == 1 {
        return Err(ChangelogV3Error::PredecessorMismatch);
    }
    let before = ChangelogTransactionAllocator::Next(assigned);
    let after = encode_changelog_transaction_allocator_v3(before.allocate_one()?.1)
        .map_err(|_| ChangelogV3Error::InvalidEncoding)?;
    let before = encode_changelog_transaction_allocator_v3(before)
        .map_err(|_| ChangelogV3Error::InvalidEncoding)?;
    let expected_hash: [u8; 32] = Sha256::digest(before.as_bytes()).into();
    if expected != expected_hash || value != after.as_bytes() {
        return Err(ChangelogV3Error::PredecessorMismatch);
    }
    Ok(assigned)
}

/// Forms one net receipt from the exact admitted/recovered frame. The caller
/// binds its predecessor to the known-durable activation/checkpoint or prior
/// validated suffix receipt, and owns the durable-record semantic validation.
/// No source may be synthesized for a pre-activation frame without its allocator.
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "WP-772 cursor/writer wiring follows the independently tested source conversion"
    )
)]
pub(crate) fn receipt_from_journal(
    frame: &JournalFrame,
    binding: AuthoritativeTransactionBindingV3,
) -> Result<AuthoritativeTransactionV3, ChangelogV3Error> {
    if binding.predecessor.is_none()
        || binding.database_id != frame.database_id()
        || binding.predecessor_frontier
            != DualFrontier::new(
                frame.predecessor_sequence(),
                frame.predecessor_administration_sequence(),
            )
        || binding.covered_frontier
            != DualFrontier::new(
                frame.covered_sequence(),
                frame.covered_administration_sequence(),
            )
    {
        return Err(ChangelogV3Error::PredecessorMismatch);
    }
    let source = match frame.kind() {
        JournalFrameKind::Command => ChangelogAttributionV3::JournaledApplicationGroup,
        JournalFrameKind::ServiceAudit => ChangelogAttributionV3::JournaledServiceAudit,
    };
    receipt_from_sources(
        binding,
        source,
        frame.mutations().iter().map(|mutation| {
            Ok((
                mutation.table(),
                mutation.key(),
                mutation.expected_hash(),
                mutation.value(),
            ))
        }),
    )
}

/// Same receipt fold for the live checkpoint's retained validated mutations.
/// It borrows the original source, not latest overlay rows, and does not decode
/// a journal frame or duplicate its mutation payloads before coalescing.
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "WP-772 live checkpoint integration follows")
)]
pub(crate) fn receipt_from_validated_mutations(
    binding: AuthoritativeTransactionBindingV3,
    source: ChangelogAttributionV3,
    mutations: &[CompositeMutationV1],
) -> Result<AuthoritativeTransactionV3, ChangelogV3Error> {
    receipt_from_sources(
        binding,
        source,
        mutations.iter().map(|mutation| {
            // Invert the existing exhaustive journal-to-composite mapping instead
            // of maintaining a second physical table inventory.
            let table = JournalTable::ALL
                .into_iter()
                .find(|table| table.composite() == mutation.table())
                .ok_or(ChangelogV3Error::InvalidNamespace)?;
            Ok((
                table,
                mutation.key(),
                mutation.expected_hash(),
                mutation.value(),
            ))
        }),
    )
}

type MutationSource<'a> = (JournalTable, &'a [u8], Option<[u8; 32]>, Option<&'a [u8]>);

fn receipt_from_sources<'a>(
    binding: AuthoritativeTransactionBindingV3,
    source: ChangelogAttributionV3,
    mutations: impl IntoIterator<Item = Result<MutationSource<'a>, ChangelogV3Error>>,
) -> Result<AuthoritativeTransactionV3, ChangelogV3Error> {
    if binding.predecessor.is_none()
        || !matches!(
            source,
            ChangelogAttributionV3::JournaledApplicationGroup
                | ChangelogAttributionV3::JournaledServiceAudit
        )
    {
        return Err(ChangelogV3Error::PredecessorMismatch);
    }
    let mut allocator_seen = false;
    let mut changes = AuthoritativeMutationAccumulatorV3::default();
    for mutation in mutations {
        let (table, key, expected_hash, value) = mutation?;
        let namespace = AuthoritativeStateCatalogV1
            .lookup(table.label(), key)
            .ok_or(ChangelogV3Error::InvalidNamespace)?;
        if namespace == AuthoritativeNamespaceV1::NextChangelogTransaction {
            if allocator_seen {
                return Err(ChangelogV3Error::InvalidEncoding);
            }
            allocator_seen = true;
            if allocator_assignment(table, key, expected_hash, value)? != binding.sequence {
                return Err(ChangelogV3Error::PredecessorMismatch);
            }
            continue;
        }
        if namespace.class() != ReplicationAuthorityClassV1::ReplicatedAuthoritative {
            // The closed journal table set has no derived projection payloads.
            // Its only V3 control mutation is the checked allocator above.
            return Err(ChangelogV3Error::InvalidNamespace);
        }
        changes.record(match value {
            Some(value) => AuthoritativeMutationV3::put(namespace, key, expected_hash, value)?,
            None => AuthoritativeMutationV3::delete(
                namespace,
                key,
                expected_hash.ok_or(ChangelogV3Error::InvalidEncoding)?,
            )?,
        })?;
    }
    if !allocator_seen {
        return Err(ChangelogV3Error::InvalidEncoding);
    }
    AuthoritativeTransactionV3::new(binding, source, changes.finish()?)
}

#[cfg(test)]
mod tests {
    use riffdb_storage_api::proto_codec::encode_changelog_transaction_allocator_v3;
    use riffdb_storage_api::{
        AuthoritativeNamespaceV1 as N, AuthoritativeTransactionBindingV3,
        ChangelogTransactionAllocator, ChangelogTransactionSequence, ChangelogV3Error,
    };
    use riffdb_types::{CommitSequence, DatabaseId, DualFrontier};

    use crate::journal::{JournalFrame, JournalMutation, JournalTable};

    fn binding() -> AuthoritativeTransactionBindingV3 {
        AuthoritativeTransactionBindingV3 {
            database_id: DatabaseId::from_unix_milliseconds_and_random(
                1_700_000_000_000,
                [0x71; 10],
            )
            .unwrap(),
            history_incarnation: 1,
            predecessor: ChangelogTransactionSequence::new(1),
            sequence: ChangelogTransactionSequence::new(2).unwrap(),
            predecessor_frontier: DualFrontier::INITIAL,
            covered_frontier: DualFrontier::new(CommitSequence::new(1), None),
            prior_history_hash: [0x71; 32],
        }
    }

    fn allocator_mutation() -> JournalMutation {
        let before = ChangelogTransactionAllocator::Next(binding().sequence);
        let after = before.allocate_one().unwrap().1;
        JournalMutation::replace(
            JournalTable::Meta,
            N::NextChangelogTransaction
                .metadata_key()
                .unwrap()
                .as_bytes(),
            encode_changelog_transaction_allocator_v3(before)
                .unwrap()
                .as_bytes(),
            encode_changelog_transaction_allocator_v3(after)
                .unwrap()
                .into_bytes(),
        )
        .unwrap()
    }

    fn frame(extra: Vec<JournalMutation>) -> JournalFrame {
        let mut mutations = vec![
            JournalMutation::put(
                JournalTable::Commits,
                vec![1],
                b"opaque-codec-vector".to_vec(),
            )
            .unwrap(),
            JournalMutation::replace(
                JournalTable::Entities,
                b"entity".to_vec(),
                b"original",
                b"overwritten".to_vec(),
            )
            .unwrap(),
            JournalMutation::delete_matching(
                JournalTable::Entities,
                b"entity".to_vec(),
                b"overwritten",
            )
            .unwrap(),
        ];
        mutations.extend(extra);
        JournalFrame::command(
            binding().database_id,
            None,
            CommitSequence::new(1),
            None,
            None,
            1,
            [0x44; 32],
            mutations,
        )
        .unwrap()
    }

    #[test]
    // req: REP-003, REC-001, STO-012
    fn journal_receipt_formation_preserves_original_delete_precondition_without_row_reads() {
        let source = frame(vec![allocator_mutation()]);
        let encoded = source.encode().unwrap();
        let (decoded, _) = JournalFrame::decode(encoded.as_bytes()).unwrap();
        let receipt = super::receipt_from_journal(&decoded, binding()).unwrap();
        assert_eq!(receipt.binding(), binding());
        assert_eq!(receipt.mutations().len(), 2);
        let entity = &receipt.mutations()[0];
        assert_eq!(entity.namespace(), N::Entities);
        assert_eq!(entity.value(), None);
        assert!(entity.matches_prior(Some(b"original")));
        assert!(!entity.matches_prior(Some(b"overwritten")));
        assert_eq!(
            receipt.encode().unwrap(),
            super::receipt_from_journal(&source, binding())
                .unwrap()
                .encode()
                .unwrap()
        );
        assert_eq!(source.encode().unwrap().as_bytes(), encoded.as_bytes());
    }

    #[test]
    // req: REP-003, REC-001, STO-012
    fn retained_checkpoint_mutations_form_the_exact_recovered_receipt() {
        let source = frame(vec![allocator_mutation()]);
        let encoded = source.encode().unwrap();
        let (decoded, _) = JournalFrame::decode(encoded.as_bytes()).unwrap();
        let retained: Vec<_> = source
            .mutations()
            .iter()
            .map(|mutation| mutation.composite().unwrap())
            .collect();
        let receipt = super::receipt_from_validated_mutations(
            binding(),
            riffdb_storage_api::ChangelogAttributionV3::JournaledApplicationGroup,
            &retained,
        )
        .unwrap();
        assert_eq!(
            receipt.encode().unwrap(),
            super::receipt_from_journal(&decoded, binding())
                .unwrap()
                .encode()
                .unwrap()
        );
        assert!(receipt.mutations()[0].matches_prior(Some(b"original")));
        assert_eq!(receipt.mutations()[0].value(), None);
        for attribution in [
            riffdb_storage_api::ChangelogAttributionV3::DirectApplicationOrServiceAuditGroup,
            riffdb_storage_api::ChangelogAttributionV3::V3Activation,
        ] {
            assert!(
                super::receipt_from_validated_mutations(binding(), attribution, &retained).is_err()
            );
        }
        let mut missing = retained.clone();
        missing.pop();
        assert!(
            super::receipt_from_validated_mutations(
                binding(),
                riffdb_storage_api::ChangelogAttributionV3::JournaledApplicationGroup,
                &missing,
            )
            .is_err()
        );
        let mut duplicate = retained.clone();
        duplicate.push(allocator_mutation().composite().unwrap());
        assert!(
            super::receipt_from_validated_mutations(
                binding(),
                riffdb_storage_api::ChangelogAttributionV3::JournaledApplicationGroup,
                &duplicate,
            )
            .is_err()
        );
    }

    #[test]
    // req: REP-003, REC-001, STO-012
    fn standalone_audit_receipt_carries_the_checked_physical_allocator_without_application_advance()
    {
        let mut audit_binding = binding();
        audit_binding.covered_frontier =
            riffdb_types::DualFrontier::new(None, riffdb_types::AdministrationSequence::new(1));
        let frame = JournalFrame::service_audit(
            audit_binding.database_id,
            None,
            None,
            riffdb_types::AdministrationSequence::new(1),
            1,
            [0x44; 32],
            vec![
                JournalMutation::put(
                    JournalTable::Audit,
                    vec![1],
                    b"opaque-audit-codec-vector".to_vec(),
                )
                .unwrap(),
                allocator_mutation(),
            ],
        )
        .expect("an attributed standalone audit must carry its V3 allocator source");
        let encoded = frame.encode().unwrap();
        let (decoded, _) = JournalFrame::decode(encoded.as_bytes()).unwrap();
        let receipt = super::receipt_from_journal(&decoded, audit_binding).unwrap();
        assert_eq!(receipt.binding(), audit_binding);
        assert_eq!(
            receipt.attribution(),
            riffdb_storage_api::ChangelogAttributionV3::JournaledServiceAudit
        );
        assert_eq!(receipt.mutations().len(), 1);
        assert_eq!(receipt.mutations()[0].namespace(), N::Audit);
        assert_eq!(decoded.encode().unwrap().as_bytes(), encoded.as_bytes());
    }

    #[test]
    // req: REP-003, REC-001, STO-012
    fn journal_allocator_requires_exact_current_envelopes_and_one_non_activation_step() {
        fn advance(before: u64, after: ChangelogTransactionAllocator) -> JournalMutation {
            JournalMutation::replace(
                JournalTable::Meta,
                N::NextChangelogTransaction
                    .metadata_key()
                    .unwrap()
                    .as_bytes(),
                encode_changelog_transaction_allocator_v3(ChangelogTransactionAllocator::Next(
                    ChangelogTransactionSequence::new(before).unwrap(),
                ))
                .unwrap()
                .as_bytes(),
                encode_changelog_transaction_allocator_v3(after)
                    .unwrap()
                    .into_bytes(),
            )
            .unwrap()
        }
        for assigned in [2, 42, u64::MAX] {
            let before = ChangelogTransactionAllocator::Next(
                ChangelogTransactionSequence::new(assigned).unwrap(),
            );
            let mutation = advance(assigned, before.allocate_one().unwrap().1);
            assert_eq!(
                super::journal_allocator_assignment(&mutation)
                    .unwrap()
                    .get(),
                assigned
            );
        }
        let next = |value| {
            ChangelogTransactionAllocator::Next(ChangelogTransactionSequence::new(value).unwrap())
        };
        for mutation in [
            advance(1, next(2)), // Activation is Immediate, never a journal source.
            advance(2, next(4)), // Skipped assignment.
            advance(2, next(2)), // No advance.
            advance(2, next(1)), // Rollback.
            advance(2, ChangelogTransactionAllocator::Exhausted),
            JournalMutation::put(
                JournalTable::Meta,
                N::NextChangelogTransaction
                    .metadata_key()
                    .unwrap()
                    .as_bytes(),
                encode_changelog_transaction_allocator_v3(next(3))
                    .unwrap()
                    .into_bytes(),
            )
            .unwrap(),
            JournalMutation::replace(
                JournalTable::Meta,
                N::NextChangelogTransaction
                    .metadata_key()
                    .unwrap()
                    .as_bytes(),
                b"wrong-preimage",
                encode_changelog_transaction_allocator_v3(next(3))
                    .unwrap()
                    .into_bytes(),
            )
            .unwrap(),
            JournalMutation::delete_matching(
                JournalTable::Meta,
                N::NextChangelogTransaction
                    .metadata_key()
                    .unwrap()
                    .as_bytes(),
                b"old",
            )
            .unwrap(),
        ] {
            assert!(super::journal_allocator_assignment(&mutation).is_err());
            assert!(
                JournalFrame::service_audit(
                    binding().database_id,
                    None,
                    None,
                    riffdb_types::AdministrationSequence::new(1),
                    1,
                    [0; 32],
                    vec![
                        JournalMutation::put(JournalTable::Audit, vec![1], vec![1]).unwrap(),
                        mutation
                    ],
                )
                .is_err()
            );
        }
    }

    #[test]
    // req: REP-003, REC-001, STO-012
    fn journal_allocator_shape_does_not_authorize_replay_into_inactive_or_partial_v3_state() {
        use redb::ReadableDatabase;
        let scope = crate::test_path::ScopedDirectory::new("v3-inactive-refusal");
        let database = redb::Database::create(scope.join("db.redb")).unwrap();
        let key = N::NextChangelogTransaction.metadata_key().unwrap();
        let before = encode_changelog_transaction_allocator_v3(
            ChangelogTransactionAllocator::Next(binding().sequence),
        )
        .unwrap();
        for installed in [false, true] {
            let transaction = database.begin_write().unwrap();
            {
                let mut meta = transaction.open_table(crate::layout::META).unwrap();
                if installed {
                    meta.insert(key, before.as_bytes()).unwrap();
                }
            }
            transaction.commit().unwrap();
            let transaction = database.begin_write().unwrap();
            assert!(super::journal_allocator_assignment(&allocator_mutation()).is_ok());
            assert!(crate::journal::apply_mutation(&transaction, &allocator_mutation()).is_err());
            // Even if the private caller ignores the refusal and commits, the
            // allocator itself must neither appear nor advance without roots.
            transaction.commit().unwrap();
            let snapshot = database.begin_read().unwrap();
            let meta = snapshot.open_table(crate::layout::META).unwrap();
            let observed = meta.get(key).unwrap();
            assert_eq!(
                observed.as_ref().map(|value| value.value()),
                installed.then_some(before.as_bytes())
            );
        }
    }

    #[test]
    // req: REP-003, REC-001, STO-012
    fn journal_receipt_refuses_missing_duplicate_and_misattributed_allocator_state() {
        assert_eq!(
            super::receipt_from_journal(&frame(vec![]), binding()),
            Err(ChangelogV3Error::InvalidEncoding)
        );
        assert!(
            super::receipt_from_journal(
                &frame(vec![allocator_mutation(), allocator_mutation()]),
                binding()
            )
            .is_err()
        );
        let source = frame(vec![allocator_mutation()]);
        let mut wrong = binding();
        wrong.covered_frontier = wrong.predecessor_frontier;
        assert!(super::receipt_from_journal(&source, wrong).is_err());
        wrong = binding();
        wrong.sequence = ChangelogTransactionSequence::new(3).unwrap();
        assert!(super::receipt_from_journal(&source, wrong).is_err());
        wrong = binding();
        wrong.predecessor = None;
        assert!(super::receipt_from_journal(&source, wrong).is_err());
        let foreign =
            JournalMutation::put(JournalTable::Meta, b"foreign/v1".to_vec(), vec![1]).unwrap();
        assert!(
            super::receipt_from_journal(&frame(vec![allocator_mutation(), foreign]), binding())
                .is_err()
        );
    }
}
