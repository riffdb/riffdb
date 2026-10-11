//! Atomic materialization of the closed V5 source cut; no live certificates.

use crate::{
    changelog_v3_activation::HISTORY,
    error::{precommit_storage_error, storage_error, table_error},
    journal::SourceReclamationProof,
};
use redb::{ReadableTable, WriteTransaction};
use riffdb_storage_api::{
    AuthoritativeNamespaceV1 as N, ChangelogHistoryStateV3 as History, StorageError,
    StorageErrorKind, proto_codec::encode_changelog_history_state_v3,
};

pub(crate) fn verify(
    transaction: &WriteTransaction,
    proof: &SourceReclamationProof,
) -> Result<(), StorageError> {
    if crate::changelog_v3_roots::read_checkpoint_roots_for_write(transaction)?
        != Some(proof.predecessor())
    {
        return Err(corrupt());
    }
    let history = proof.predecessor();
    let mut covered = History::new(
        history.lineage(),
        history.anchor(),
        history.minimum_resume(),
        history.minimum_resume(),
    )
    .map_err(|_| corrupt())?;
    let table = transaction.open_table(HISTORY).map_err(table_error)?;
    let first = history.minimum_resume().sequence().get();
    for index in 0..proof.removed().len() {
        let sequence = first
            .checked_add(u64::try_from(index).map_err(|_| corrupt())?)
            .ok_or_else(corrupt)?;
        let row = table
            .get(sequence.to_be_bytes().as_slice())
            .map_err(precommit_storage_error)?
            .ok_or_else(corrupt)?;
        let canonical = crate::history_stub::resolve_history_value(row.value())?;
        covered = proof.validate_removed(index, &canonical, covered)?;
    }
    let row = table
        .get(
            proof
                .retained()
                .minimum_resume()
                .sequence()
                .get()
                .to_be_bytes()
                .as_slice(),
        )
        .map_err(precommit_storage_error)?
        .ok_or_else(corrupt)?;
    proof.validate_resume(
        &crate::history_stub::resolve_history_value(row.value())?,
        covered,
    )
}

/// Caller verified the complete bounded cut before its first mutation; the
/// sole write transaction still owns the exact same preimages.
pub(crate) fn stage(
    transaction: &WriteTransaction,
    proof: &SourceReclamationProof,
    successor: History,
) -> Result<(), StorageError> {
    if successor.lineage() != proof.predecessor().lineage()
        || successor.anchor() != proof.predecessor().anchor()
        || proof.predecessor().tail().sequence().checked_next() != Some(successor.tail().sequence())
        || successor.minimum_resume() != proof.predecessor().minimum_resume()
    {
        return Err(corrupt());
    }
    {
        let mut table = transaction.open_table(HISTORY).map_err(table_error)?;
        for sequence in proof.predecessor().minimum_resume().sequence().get()
            ..proof.retained().minimum_resume().sequence().get()
        {
            table
                .remove(sequence.to_be_bytes().as_slice())
                .map_err(precommit_storage_error)?
                .ok_or_else(corrupt)?;
        }
    }
    let retained = History::new(
        successor.lineage(),
        successor.anchor(),
        successor.tail(),
        proof.retained().minimum_resume(),
    )
    .map_err(|_| corrupt())?;
    let encoded = encode_changelog_history_state_v3(retained).map_err(crate::error::codec_error)?;
    transaction
        .open_table(crate::layout::META)
        .map_err(table_error)?
        .insert(
            N::ChangelogHistoryState
                .metadata_key()
                .ok_or_else(corrupt)?,
            encoded.as_bytes(),
        )
        .map_err(precommit_storage_error)?;
    Ok(())
}

fn corrupt() -> StorageError {
    storage_error(StorageErrorKind::CorruptData)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        RedbOperationalPorts,
        journal::{JournalFrame, SourceReceiptPreimage},
    };
    use redb::ReadableDatabase;
    use riffdb_storage_api::{AuthoritativeTransactionV3, ChangelogHistoryPointV3 as Point};

    fn frame(ports: &RedbOperationalPorts) -> JournalFrame {
        let root = ports.begin_read().unwrap();
        let history = crate::changelog_v3_roots::read_checkpoint_roots(&root)
            .unwrap()
            .unwrap();
        let table = root.open_table(HISTORY).unwrap();
        let first = history.minimum_resume().sequence().get();
        let receipt = |sequence: u64| {
            let row = table
                .get(sequence.to_be_bytes().as_slice())
                .unwrap()
                .unwrap();
            crate::history_stub::resolve_history_value(row.value())
                .unwrap()
                .to_vec()
        };
        let floor =
            Point::from_receipt(&AuthoritativeTransactionV3::decode(&receipt(first + 1)).unwrap())
                .unwrap();
        let retained =
            History::new(history.lineage(), history.anchor(), history.tail(), floor).unwrap();
        JournalFrame::source_reclamation(
            SourceReclamationProof::new(
                history,
                retained,
                vec![SourceReceiptPreimage::capture(&receipt(first)).unwrap()],
            )
            .unwrap(),
            [5; 32],
        )
        .unwrap()
    }

    // req: STO-030, STO-031, STO-032, REC-001
    #[test]
    fn source_cut_materialization_atomically_preserves_anchor_and_empty_control_receipt() {
        let (_scope, ports, _) = crate::changelog_v3_control_tests::fixture();
        let frame = frame(&ports);
        let proof = frame.source_reclamation_proof().unwrap();
        let first = proof.predecessor().minimum_resume().sequence().get();
        let tx = ports.shared.database.begin_write().unwrap();
        let receipt =
            crate::changelog_v3_journal::materialize_recovered_frame(&tx, &frame).unwrap();
        assert!(receipt.mutations().is_empty());
        tx.commit().unwrap();
        let root = ports.shared.database.begin_read().unwrap();
        let history = crate::changelog_v3_roots::validate_retained_history(&root)
            .unwrap()
            .unwrap();
        assert_eq!(history.minimum_resume(), proof.retained().minimum_resume());
        assert_eq!(history.tail().sequence(), receipt.binding().sequence);
        assert_eq!(
            history.tail().frontier(),
            proof.predecessor().tail().frontier()
        );
        let rows = root.open_table(HISTORY).unwrap();
        assert!(rows.get(first.to_be_bytes().as_slice()).unwrap().is_none());
        assert!(
            rows.get((first + 1).to_be_bytes().as_slice())
                .unwrap()
                .is_some()
        );
        assert_eq!(
            crate::changelog_v3_journal::verify_materialized_frame(&root, &frame).unwrap(),
            receipt
        );
    }

    // req: STO-030, STO-032, REC-001
    #[test]
    fn source_cut_abort_preserves_floor_allocator_and_all_original_receipts() {
        let (_scope, ports, _) = crate::changelog_v3_control_tests::fixture();
        let frame = frame(&ports);
        let before = frame.source_reclamation_proof().unwrap().predecessor();
        let tx = ports.shared.database.begin_write().unwrap();
        crate::changelog_v3_journal::materialize_recovered_frame(&tx, &frame).unwrap();
        tx.abort().unwrap();
        let root = ports.shared.database.begin_read().unwrap();
        assert_eq!(
            crate::changelog_v3_roots::validate_retained_history(&root).unwrap(),
            Some(before)
        );
    }

    // req: STO-030, STO-032, REC-001
    #[test]
    fn source_cut_refuses_missing_and_substituted_preimages_before_its_first_write() {
        for missing in [false, true] {
            let (_scope, ports, _) = crate::changelog_v3_control_tests::fixture();
            let frame = frame(&ports);
            let before = frame.source_reclamation_proof().unwrap().predecessor();
            let first = before.minimum_resume().sequence().get().to_be_bytes();
            let tx = ports.shared.database.begin_write().unwrap();
            {
                let mut table = tx.open_table(HISTORY).unwrap();
                if missing {
                    table.remove(first.as_slice()).unwrap();
                } else {
                    table
                        .insert(first.as_slice(), b"substituted".as_slice())
                        .unwrap();
                }
            }
            assert_eq!(
                crate::changelog_v3_journal::materialize_recovered_frame(&tx, &frame)
                    .unwrap_err()
                    .kind(),
                StorageErrorKind::CorruptData
            );
            assert_eq!(
                crate::changelog_v3_roots::read_checkpoint_roots_for_write(&tx).unwrap(),
                Some(before)
            );
            tx.abort().unwrap();
            assert_eq!(
                crate::changelog_v3_roots::validate_retained_history(
                    &ports.shared.database.begin_read().unwrap()
                )
                .unwrap(),
                Some(before)
            );
        }
    }
    // req: STO-030, STO-032, REC-001
    #[test]
    fn source_cut_recovery_plan_preserves_each_virtual_floor_and_refuses_a_changed_predecessor() {
        let (_scope, ports, _) = crate::changelog_v3_control_tests::fixture();
        let first = frame(&ports);
        let before = first.source_reclamation_proof().unwrap().predecessor();
        let tx = ports.shared.database.begin_write().unwrap();
        crate::changelog_v3_journal::materialize_recovered_frame(&tx, &first).unwrap();
        let predecessor = crate::changelog_v3_roots::read_checkpoint_roots_for_write(&tx)
            .unwrap()
            .unwrap();
        let table = tx.open_table(HISTORY).unwrap();
        let sequence = predecessor.minimum_resume().sequence().get();
        let removed = crate::history_stub::resolve_history_value(
            table
                .get(sequence.to_be_bytes().as_slice())
                .unwrap()
                .unwrap()
                .value(),
        )
        .unwrap()
        .to_vec();
        let floor = Point::from_receipt(
            &AuthoritativeTransactionV3::decode(
                &crate::history_stub::resolve_history_value(
                    table
                        .get((sequence + 1).to_be_bytes().as_slice())
                        .unwrap()
                        .unwrap()
                        .value(),
                )
                .unwrap(),
            )
            .unwrap(),
        )
        .unwrap();
        let retained = History::new(
            predecessor.lineage(),
            predecessor.anchor(),
            predecessor.tail(),
            floor,
        )
        .unwrap();
        let second = JournalFrame::source_reclamation(
            SourceReclamationProof::new(
                predecessor,
                retained,
                vec![SourceReceiptPreimage::capture(&removed).unwrap()],
            )
            .unwrap(),
            first.encode().unwrap().frame_hash(),
        )
        .unwrap();
        drop(table);
        tx.abort().unwrap();
        let read = ports.shared.database.begin_read().unwrap();
        let frames = [first.clone(), second.clone()];
        let plan =
            crate::changelog_v3_journal::plan_recovery(&read, &frames, before.tail().frontier())
                .unwrap();
        assert_eq!(plan.replay_from, 0);
        assert_eq!(plan.covered, before.tail().frontier());
        let changed = History::new(
            before.lineage(),
            before.anchor(),
            before.tail(),
            predecessor.minimum_resume(),
        )
        .unwrap();
        let bad_retained =
            History::new(before.lineage(), before.anchor(), before.tail(), floor).unwrap();
        let bad = JournalFrame::source_reclamation(
            SourceReclamationProof::new(
                changed,
                bad_retained,
                vec![SourceReceiptPreimage::capture(&removed).unwrap()],
            )
            .unwrap(),
            [5; 32],
        )
        .unwrap();
        assert_eq!(
            crate::changelog_v3_journal::plan_recovery(&read, &[bad], before.tail().frontier())
                .err()
                .unwrap()
                .kind(),
            StorageErrorKind::CorruptData
        );
        assert_eq!(
            crate::changelog_v3_roots::read_checkpoint_roots(&read).unwrap(),
            Some(before)
        );
        drop(read);
        let tx = ports.shared.database.begin_write().unwrap();
        for frame in &frames {
            crate::changelog_v3_journal::materialize_recovered_frame(&tx, frame).unwrap();
        }
        tx.commit().unwrap();
        let read = ports.shared.database.begin_read().unwrap();
        let actual = crate::changelog_v3_roots::validate_retained_history(&read)
            .unwrap()
            .unwrap();
        assert_eq!(actual.minimum_resume(), floor);
        assert_eq!(
            actual.tail().sequence().get(),
            before.tail().sequence().get() + 2
        );
        assert_eq!(
            crate::changelog_v3_journal::plan_recovery(&read, &frames, actual.tail().frontier())
                .unwrap()
                .replay_from,
            2
        );
    }

    // req: STO-030, STO-032, REC-001, REC-005
    #[test]
    fn source_cut_materializes_an_actual_maximum_sized_canonical_preimage() {
        use riffdb_storage_api::{
            AuthoritativeMutationV3, AuthoritativeStateCatalogV1,
            AuthoritativeTransactionBindingV3, ChangelogAttributionV3, ChangelogLineageV3,
            LeadershipEpochV1, MAX_CHANGELOG_FRAME_BYTES,
        };
        use riffdb_types::DualFrontier;
        let (_scope, ports, _) = crate::changelog_v3_control_tests::fixture();
        let original = crate::changelog_v3_roots::read_checkpoint_roots(
            &ports.shared.database.begin_read().unwrap(),
        )
        .unwrap()
        .unwrap();
        let lineage = ChangelogLineageV3::new_with_catalog(
            original.lineage().database_id(),
            1,
            LeadershipEpochV1::initial(),
            AuthoritativeStateCatalogV1.digest(),
        )
        .unwrap();
        let binding = AuthoritativeTransactionBindingV3 {
            database_id: lineage.database_id(),
            history_incarnation: 1,
            predecessor: None,
            sequence: riffdb_storage_api::ChangelogTransactionSequence::new(1).unwrap(),
            predecessor_frontier: DualFrontier::INITIAL,
            covered_frontier: DualFrontier::INITIAL,
            prior_history_hash: [0; 32],
        };
        let maximum = MAX_CHANGELOG_FRAME_BYTES - (166 + 4 * 33 + 48 + 4);
        let receipt = AuthoritativeTransactionV3::new_for_catalog(
            binding,
            ChangelogAttributionV3::V3Activation,
            vec![
                AuthoritativeMutationV3::put(
                    N::Entities,
                    b"a",
                    None,
                    &vec![0x71; maximum - 160 - 44 - 1],
                )
                .unwrap(),
            ],
            lineage.catalog_digest(),
        )
        .unwrap();
        let anchor = Point::from_receipt(&receipt).unwrap();
        let canonical = receipt.encode().unwrap();
        drop(receipt);
        let successor = AuthoritativeTransactionV3::new(
            AuthoritativeTransactionBindingV3 {
                predecessor: Some(anchor.sequence()),
                sequence: anchor.sequence().checked_next().unwrap(),
                prior_history_hash: anchor.history_hash(),
                ..binding
            },
            ChangelogAttributionV3::HistoryReclamation,
            vec![],
        )
        .unwrap();
        let tail = Point::from_receipt(&successor).unwrap();
        let history = History::new(lineage, anchor, tail, anchor).unwrap();
        let retained = History::new(lineage, anchor, tail, tail).unwrap();
        let proof = SourceReclamationProof::new(
            history,
            retained,
            vec![SourceReceiptPreimage::capture(&canonical).unwrap()],
        )
        .unwrap();
        assert_eq!(canonical.len(), maximum);
        assert!(proof.encode().unwrap().len() < 64 * 1024);
        let tx = ports.shared.database.begin_write().unwrap();
        {
            let mut rows = tx.open_table(HISTORY).unwrap();
            rows.retain(|_, _| false).unwrap();
            rows.insert(1_u64.to_be_bytes().as_slice(), canonical.as_slice())
                .unwrap();
            rows.insert(
                2_u64.to_be_bytes().as_slice(),
                successor.encode().unwrap().as_slice(),
            )
            .unwrap();
        }
        {
            let mut meta = tx.open_table(crate::layout::META).unwrap();
            meta.insert(
                N::AuthoritativeStateCatalog.metadata_key().unwrap(),
                riffdb_storage_api::proto_codec::encode_authoritative_state_catalog_v1(
                    AuthoritativeStateCatalogV1,
                )
                .unwrap()
                .as_bytes(),
            )
            .unwrap();
            meta.insert(
                N::ChangelogHistoryState.metadata_key().unwrap(),
                encode_changelog_history_state_v3(history)
                    .unwrap()
                    .as_bytes(),
            )
            .unwrap();
            meta.insert(
                N::NextChangelogTransaction.metadata_key().unwrap(),
                riffdb_storage_api::proto_codec::encode_changelog_transaction_allocator_v3(
                    history.expected_allocator(),
                )
                .unwrap()
                .as_bytes(),
            )
            .unwrap();
        }
        tx.commit().unwrap();
        drop(canonical);
        let frame = JournalFrame::source_reclamation(proof, [5; 32]).unwrap();
        let tx = ports.shared.database.begin_write().unwrap();
        let control =
            crate::changelog_v3_journal::materialize_recovered_frame(&tx, &frame).unwrap();
        assert!(control.mutations().is_empty());
        tx.commit().unwrap();
        let pin = ports.shared.database.begin_read().unwrap();
        let actual = crate::changelog_v3_roots::read_checkpoint_roots(&pin)
            .unwrap()
            .unwrap();
        assert_eq!(actual.minimum_resume(), tail);
        let rows = pin.open_table(HISTORY).unwrap();
        assert!(rows.get(1_u64.to_be_bytes().as_slice()).unwrap().is_none());
        assert!(rows.get(2_u64.to_be_bytes().as_slice()).unwrap().is_some());
    }
}
