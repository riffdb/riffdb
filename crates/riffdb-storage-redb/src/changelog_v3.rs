//! V3 receipt formation from validated journal mutations, never latest-row reads.

use riffdb_storage_api::proto_codec::encode_changelog_transaction_allocator_v3;
use riffdb_storage_api::{
    AuthoritativeMutationAccumulatorV3, AuthoritativeMutationV3, AuthoritativeNamespaceV1,
    AuthoritativeStateCatalogV1, AuthoritativeTransactionBindingV3, AuthoritativeTransactionV3,
    ChangelogAttributionV3, ChangelogTransactionAllocator, ChangelogV3Error,
    ReplicationAuthorityClassV1,
};
use riffdb_types::DualFrontier;
use sha2::{Digest, Sha256};

use crate::journal::{JournalFrame, JournalFrameKind, JournalMutation};

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
    let before = ChangelogTransactionAllocator::Next(binding.sequence);
    let after = before.allocate_one()?.1;
    let before = encode_changelog_transaction_allocator_v3(before)
        .map_err(|_| ChangelogV3Error::InvalidEncoding)?;
    let after = encode_changelog_transaction_allocator_v3(after)
        .map_err(|_| ChangelogV3Error::InvalidEncoding)?;
    let expected_allocator_hash: [u8; 32] = Sha256::digest(before.as_bytes()).into();
    let mut allocator_seen = false;
    let mut changes = AuthoritativeMutationAccumulatorV3::default();
    for mutation in frame.mutations() {
        let namespace = AuthoritativeStateCatalogV1
            .lookup(mutation.table().label(), mutation.key())
            .ok_or(ChangelogV3Error::InvalidNamespace)?;
        if namespace == AuthoritativeNamespaceV1::NextChangelogTransaction {
            if allocator_seen {
                return Err(ChangelogV3Error::InvalidEncoding);
            }
            allocator_seen = true;
            match mutation {
                JournalMutation::Put {
                    expected_hash: Some(expected),
                    value,
                    ..
                } if *expected == expected_allocator_hash && value.as_ref() == after.as_bytes() => {
                }
                _ => return Err(ChangelogV3Error::PredecessorMismatch),
            }
            continue;
        }
        if namespace.class() != ReplicationAuthorityClassV1::ReplicatedAuthoritative {
            // The closed journal table set has no derived projection payloads.
            // Its only V3 control mutation is the checked allocator above.
            return Err(ChangelogV3Error::InvalidNamespace);
        }
        changes.record(match mutation {
            JournalMutation::Put {
                key,
                expected_hash,
                value,
                ..
            } => AuthoritativeMutationV3::put(namespace, key, *expected_hash, value)?,
            JournalMutation::Delete {
                key, expected_hash, ..
            } => AuthoritativeMutationV3::delete(namespace, key, *expected_hash)?,
        })?;
    }
    if !allocator_seen {
        return Err(ChangelogV3Error::InvalidEncoding);
    }
    let source = match frame.kind() {
        JournalFrameKind::Command => ChangelogAttributionV3::JournaledApplicationGroup,
        JournalFrameKind::ServiceAudit => ChangelogAttributionV3::JournaledServiceAudit,
    };
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
