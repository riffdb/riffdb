//! Closed V5 source-prefix proof. Receipt payloads are streamed, never retained.

use super::*;
use riffdb_storage_api::{
    ChangelogHistoryStateV3 as History, MAX_CHANGELOG_FRAME_BYTES,
    proto_codec::{decode_changelog_history_state_v3, encode_changelog_history_state_v3},
};

pub(crate) const MAX_SOURCE_CUT_PROOF_BYTES: usize = 64 * 1024;
const MAX_REMOVED: usize = 256;
const PREIMAGE_BYTES: usize = 4 + HASH_BYTES;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SourceReceiptPreimage {
    canonical_bytes: u32,
    canonical_hash: [u8; HASH_BYTES],
}

impl SourceReceiptPreimage {
    pub(crate) fn capture(canonical: &[u8]) -> Result<Self, JournalCodecError> {
        if canonical.is_empty() || canonical.len() > MAX_CHANGELOG_FRAME_BYTES {
            return Err(JournalCodecError::LimitExceeded);
        }
        Ok(Self {
            canonical_bytes: u32::try_from(canonical.len())
                .map_err(|_| JournalCodecError::LimitExceeded)?,
            canonical_hash: digest(canonical),
        })
    }

    pub(crate) fn canonical_bytes(&self) -> u32 {
        self.canonical_bytes
    }

    pub(crate) fn matches(&self, canonical: &[u8]) -> bool {
        usize::try_from(self.canonical_bytes).ok() == Some(canonical.len())
            && self.canonical_hash == digest(canonical)
    }
}

/// `retained` has the predecessor tail and the new minimum resume point.
/// The frame's separately checked empty control receipt advances that tail.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SourceReclamationProof {
    predecessor: History,
    retained: History,
    removed: Box<[SourceReceiptPreimage]>,
}

impl SourceReclamationProof {
    pub(crate) fn new(
        predecessor: History,
        retained: History,
        removed: Vec<SourceReceiptPreimage>,
    ) -> Result<Self, JournalCodecError> {
        let count = retained
            .minimum_resume()
            .sequence()
            .get()
            .checked_sub(predecessor.minimum_resume().sequence().get());
        if predecessor.lineage() != retained.lineage()
            || predecessor.anchor() != retained.anchor()
            || predecessor.tail() != retained.tail()
            || !(1..=MAX_REMOVED).contains(&removed.len())
            || count != u64::try_from(removed.len()).ok()
        {
            return Err(JournalCodecError::InvalidValue);
        }
        let bytes = removed.iter().try_fold(0_usize, |total, row| {
            let size = usize::try_from(row.canonical_bytes)
                .map_err(|_| JournalCodecError::LimitExceeded)?;
            if size == 0 || size > MAX_CHANGELOG_FRAME_BYTES {
                return Err(JournalCodecError::LimitExceeded);
            }
            total
                .checked_add(size)
                .ok_or(JournalCodecError::LimitExceeded)
        })?;
        if bytes > MAX_CHANGELOG_FRAME_BYTES {
            return Err(JournalCodecError::LimitExceeded);
        }
        Ok(Self {
            predecessor,
            retained,
            removed: removed.into_boxed_slice(),
        })
    }

    pub(crate) fn predecessor(&self) -> History {
        self.predecessor
    }
    pub(crate) fn retained(&self) -> History {
        self.retained
    }
    pub(crate) fn removed(&self) -> &[SourceReceiptPreimage] {
        &self.removed
    }

    pub(crate) fn rebind_checked_prefix(
        &self,
        predecessor: History,
    ) -> Result<Self, JournalCodecError> {
        if predecessor.lineage() != self.predecessor.lineage()
            || predecessor.anchor() != self.predecessor.anchor()
            || predecessor.minimum_resume() != self.predecessor.minimum_resume()
            || predecessor.tail().sequence() < self.predecessor.tail().sequence()
        {
            return Err(JournalCodecError::InvalidValue);
        }
        let retained = History::new(
            predecessor.lineage(),
            predecessor.anchor(),
            predecessor.tail(),
            self.retained.minimum_resume(),
        )
        .map_err(|_| JournalCodecError::InvalidValue)?;
        Self::new(predecessor, retained, self.removed.to_vec())
    }

    pub(crate) fn validate_removed(
        &self,
        index: usize,
        canonical: &[u8],
        covered: History,
    ) -> Result<History, riffdb_storage_api::StorageError> {
        use riffdb_storage_api::StorageErrorKind;
        let corrupt = || crate::error::storage_error(StorageErrorKind::CorruptData);
        let preimage = self.removed.get(index).ok_or_else(corrupt)?;
        if !preimage.matches(canonical) {
            return Err(corrupt());
        }
        if index == 0 {
            covered
                .validate_terminal_canonical_v1(canonical)
                .map_err(|_| corrupt())?;
            Ok(covered)
        } else {
            let after = covered
                .advance_canonical_v1(canonical)
                .map_err(|_| corrupt())?;
            if self
                .predecessor
                .minimum_resume()
                .sequence()
                .get()
                .checked_add(u64::try_from(index).map_err(|_| corrupt())?)
                != Some(after.tail().sequence().get())
            {
                return Err(corrupt());
            }
            Ok(after)
        }
    }

    pub(crate) fn validate_resume(
        &self,
        canonical: &[u8],
        covered: History,
    ) -> Result<(), riffdb_storage_api::StorageError> {
        let corrupt =
            || crate::error::storage_error(riffdb_storage_api::StorageErrorKind::CorruptData);
        let after = covered
            .advance_canonical_v1(canonical)
            .map_err(|_| corrupt())?;
        if after.tail() != self.retained.minimum_resume() {
            return Err(corrupt());
        }
        Ok(())
    }

    pub(crate) fn encode(&self) -> Result<Vec<u8>, JournalCodecError> {
        let predecessor = encode_changelog_history_state_v3(self.predecessor)
            .map_err(|_| JournalCodecError::InvalidValue)?;
        let retained = encode_changelog_history_state_v3(self.retained)
            .map_err(|_| JournalCodecError::InvalidValue)?;
        let total = 4_usize
            .checked_add(predecessor.as_bytes().len())
            .and_then(|n| n.checked_add(4 + retained.as_bytes().len()))
            .and_then(|n| n.checked_add(2 + self.removed.len() * PREIMAGE_BYTES))
            .filter(|n| *n <= MAX_SOURCE_CUT_PROOF_BYTES)
            .ok_or(JournalCodecError::LimitExceeded)?;
        let mut bytes = Vec::with_capacity(total);
        for state in [predecessor.as_bytes(), retained.as_bytes()] {
            bytes.extend_from_slice(
                &u32::try_from(state.len())
                    .map_err(|_| JournalCodecError::LimitExceeded)?
                    .to_be_bytes(),
            );
            bytes.extend_from_slice(state);
        }
        bytes.extend_from_slice(
            &u16::try_from(self.removed.len())
                .map_err(|_| JournalCodecError::LimitExceeded)?
                .to_be_bytes(),
        );
        for row in &self.removed {
            bytes.extend_from_slice(&row.canonical_bytes.to_be_bytes());
            bytes.extend_from_slice(&row.canonical_hash);
        }
        Ok(bytes)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, JournalCodecError> {
        if bytes.len() > MAX_SOURCE_CUT_PROOF_BYTES {
            return Err(JournalCodecError::LimitExceeded);
        }
        let mut cursor = 0_usize;
        let mut history = || {
            let len = usize::try_from(read_u32(bytes, cursor)?)
                .map_err(|_| JournalCodecError::LimitExceeded)?;
            cursor = cursor
                .checked_add(4)
                .ok_or(JournalCodecError::LimitExceeded)?;
            let end = cursor
                .checked_add(len)
                .ok_or(JournalCodecError::LimitExceeded)?;
            let value = decode_changelog_history_state_v3(
                bytes.get(cursor..end).ok_or(JournalCodecError::Truncated)?,
            )
            .map_err(|_| JournalCodecError::InvalidValue)?;
            cursor = end;
            Ok::<_, JournalCodecError>(*value.value())
        };
        let predecessor = history()?;
        let retained = history()?;
        let count = usize::from(read_u16(bytes, cursor)?);
        cursor = cursor
            .checked_add(2)
            .ok_or(JournalCodecError::LimitExceeded)?;
        if !(1..=MAX_REMOVED).contains(&count)
            || cursor.checked_add(count * PREIMAGE_BYTES) != Some(bytes.len())
        {
            return Err(JournalCodecError::InvalidValue);
        }
        let mut removed = Vec::with_capacity(count);
        for _ in 0..count {
            removed.push(SourceReceiptPreimage {
                canonical_bytes: read_u32(bytes, cursor)?,
                canonical_hash: read_array::<HASH_BYTES>(bytes, cursor + 4)?,
            });
            cursor += PREIMAGE_BYTES;
        }
        let proof = Self::new(predecessor, retained, removed)?;
        if proof.encode()?.as_slice() != bytes {
            return Err(JournalCodecError::InvalidValue);
        }
        Ok(proof)
    }
}

impl JournalFrame {
    pub(crate) fn source_reclamation(
        proof: SourceReclamationProof,
        previous_hash: [u8; HASH_BYTES],
    ) -> Result<Self, JournalCodecError> {
        use riffdb_storage_api::{
            AuthoritativeNamespaceV1 as N, AuthoritativeTransactionBindingV3,
            proto_codec::encode_changelog_transaction_allocator_v3,
        };
        let history = proof.predecessor();
        let before = history.expected_allocator();
        let (sequence, after) = before
            .allocate_one()
            .map_err(|_| JournalCodecError::InvalidValue)?;
        let mutation = JournalMutation::replace(
            JournalTable::Meta,
            N::NextChangelogTransaction
                .metadata_key()
                .ok_or(JournalCodecError::InvalidValue)?
                .as_bytes(),
            encode_changelog_transaction_allocator_v3(before)
                .map_err(|_| JournalCodecError::InvalidValue)?
                .as_bytes(),
            encode_changelog_transaction_allocator_v3(after)
                .map_err(|_| JournalCodecError::InvalidValue)?
                .as_bytes()
                .to_vec(),
        )?;
        let frontier = history.tail().frontier();
        let binding = AuthoritativeTransactionBindingV3 {
            database_id: history.lineage().database_id(),
            history_incarnation: history.lineage().history_incarnation(),
            predecessor: Some(history.tail().sequence()),
            sequence,
            predecessor_frontier: frontier,
            covered_frontier: frontier,
            prior_history_hash: history.tail().history_hash(),
        };
        let frame = Self {
            format_version: FRAME_VERSION_V5,
            changelog: Some(FrameChangelogBinding::new(
                &binding,
                history.lineage().catalog_digest(),
            )),
            source_reclamation: Some(proof),
            ..Self::checked(
                history.lineage().database_id(),
                JournalFrameKind::SourceReclamation,
                frontier.application(),
                frontier.application(),
                frontier.administration(),
                frontier.administration(),
                1,
                0,
                0,
                previous_hash,
                vec![mutation],
            )?
        };
        frame.validate_source_reclamation()?;
        Ok(frame)
    }

    pub(crate) fn source_reclamation_proof(&self) -> Option<&SourceReclamationProof> {
        self.source_reclamation.as_ref()
    }

    pub(super) fn validate_source_reclamation(&self) -> Result<(), JournalCodecError> {
        if self.kind != JournalFrameKind::SourceReclamation {
            return if self.source_reclamation.is_none() {
                Ok(())
            } else {
                Err(JournalCodecError::InvalidValue)
            };
        }
        let proof = self
            .source_reclamation
            .as_ref()
            .ok_or(JournalCodecError::InvalidValue)?;
        let history = proof.predecessor();
        let recorded = self
            .changelog
            .as_ref()
            .ok_or(JournalCodecError::InvalidValue)?;
        let binding = recorded.binding(self);
        if self.format_version != FRAME_VERSION_V5
            || binding.database_id != history.lineage().database_id()
            || binding.history_incarnation != history.lineage().history_incarnation()
            || binding.predecessor != Some(history.tail().sequence())
            || history.tail().sequence().checked_next() != Some(binding.sequence)
            || binding.prior_history_hash != history.tail().history_hash()
            || binding.predecessor_frontier != history.tail().frontier()
            || binding.covered_frontier != history.tail().frontier()
            || recorded.catalog_digest() != history.lineage().catalog_digest()
        {
            return Err(JournalCodecError::InvalidValue);
        }
        let receipt = crate::changelog_v3::receipt_from_journal_for_catalog(
            self,
            binding,
            history.lineage().catalog_digest(),
        )
        .map_err(|_| JournalCodecError::InvalidValue)?;
        if !receipt.mutations().is_empty() {
            return Err(JournalCodecError::InvalidValue);
        }
        history
            .advance(&receipt)
            .map_err(|_| JournalCodecError::InvalidValue)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::changelog_v3_activation::HISTORY;
    use riffdb_storage_api::{AuthoritativeTransactionV3, ChangelogHistoryPointV3};

    fn fixture() -> (History, History, Vec<u8>) {
        let (_scope, ports, _) = crate::changelog_v3_control_tests::fixture();
        let root = ports.begin_read().unwrap().into_shared().unwrap();
        let predecessor = crate::changelog_v3_roots::read_checkpoint_roots(&root)
            .unwrap()
            .unwrap();
        let table = root.open_table(HISTORY).unwrap();
        let first = predecessor.minimum_resume().sequence().get();
        let bytes = |sequence: u64| {
            let row = table
                .get(sequence.to_be_bytes().as_slice())
                .unwrap()
                .unwrap();
            crate::history_stub::resolve_history_value(row.value())
                .unwrap()
                .to_vec()
        };
        let floor = ChangelogHistoryPointV3::from_receipt(
            &AuthoritativeTransactionV3::decode(&bytes(first + 1)).unwrap(),
        )
        .unwrap();
        let retained = History::new(
            predecessor.lineage(),
            predecessor.anchor(),
            predecessor.tail(),
            floor,
        )
        .unwrap();
        (predecessor, retained, bytes(first))
    }

    // req: STO-030, STO-031, STO-032, REC-001
    #[test]
    fn source_cut_proof_binds_exact_preimages_and_preserves_surviving_history() {
        let (predecessor, retained, bytes) = fixture();
        let row = SourceReceiptPreimage::capture(&bytes).unwrap();
        assert!(row.matches(&bytes));
        let mut substitution = bytes.clone();
        substitution[0] ^= 1;
        assert!(!row.matches(&substitution));
        assert!(!row.matches(&bytes[..bytes.len() - 1]));
        let proof = SourceReclamationProof::new(predecessor, retained, vec![row.clone()]).unwrap();
        let encoded = proof.encode().unwrap();
        assert!(encoded.len() <= MAX_SOURCE_CUT_PROOF_BYTES);
        assert_eq!(SourceReclamationProof::decode(&encoded).unwrap(), proof);
        assert!(SourceReclamationProof::new(predecessor, predecessor, vec![row.clone()]).is_err());
        assert!(SourceReclamationProof::new(predecessor, retained, vec![]).is_err());
        assert!(SourceReclamationProof::new(predecessor, retained, vec![row; 257]).is_err());
        let mut trailing = encoded.clone();
        trailing.push(0);
        assert!(SourceReclamationProof::decode(&trailing).is_err());
        for length in 0..encoded.len() {
            assert!(SourceReclamationProof::decode(&encoded[..length]).is_err());
        }
    }

    // req: AFC-001, STO-030, STO-032, REC-001
    #[test]
    fn source_cut_v5_frame_refuses_old_versions_business_writes_and_frontier_changes() {
        let (predecessor, retained, bytes) = fixture();
        let proof = SourceReclamationProof::new(
            predecessor,
            retained,
            vec![SourceReceiptPreimage::capture(&bytes).unwrap()],
        )
        .unwrap();
        let frame = JournalFrame::source_reclamation(proof, [5; HASH_BYTES]).unwrap();
        let encoded = frame.encode().unwrap();
        let (decoded, hash) = JournalFrame::decode(encoded.as_bytes()).unwrap();
        assert_eq!(decoded, frame);
        assert_eq!(
            JournalFrame::decode_proven(encoded.as_bytes(), &hash).unwrap(),
            frame
        );
        assert_eq!(decoded.predecessor_sequence(), decoded.covered_sequence());
        assert_eq!(
            decoded.predecessor_administration_sequence(),
            decoded.covered_administration_sequence()
        );
        assert_eq!(
            frame.composite().unwrap().kind(),
            riffdb_storage_api::CompositeFrameKindV1::SourceReclamation
        );
        let receipt = crate::changelog_v3::receipt_from_journal_for_catalog(
            &frame,
            frame.changelog().unwrap().binding(&frame),
            predecessor.lineage().catalog_digest(),
        )
        .unwrap();
        assert_eq!(
            receipt.attribution(),
            riffdb_storage_api::ChangelogAttributionV3::HistoryReclamation
        );
        assert!(receipt.mutations().is_empty());
        for version in [FRAME_VERSION_V2, FRAME_VERSION_V3, FRAME_VERSION_V4] {
            assert!(frame.encode_as(version).is_err());
            let mut old = encoded.as_bytes().to_vec();
            old[8..10].copy_from_slice(&version.to_be_bytes());
            let footer = old.len() - FRAME_FOOTER_BYTES;
            let hash = digest(&old[..footer]);
            old[footer + 16..].copy_from_slice(&hash);
            assert!(JournalFrame::decode(&old).is_err());
        }
        let mut business = frame.clone();
        business
            .mutations
            .push(JournalMutation::put(JournalTable::Audit, vec![1], vec![2]).unwrap());
        assert!(business.encode().is_err());
        let mut changed = frame;
        changed.covered_sequence = CommitSequence::new(100);
        assert!(changed.encode().is_err());
    }

    // req: STO-030, STO-031, END-008
    #[test]
    fn source_cut_proof_admits_one_maximum_receipt_but_not_a_byte_over_budget() {
        let (predecessor, retained, _) = fixture();
        let row = SourceReceiptPreimage {
            canonical_bytes: u32::try_from(MAX_CHANGELOG_FRAME_BYTES).unwrap(),
            canonical_hash: [1; HASH_BYTES],
        };
        assert!(SourceReclamationProof::new(predecessor, retained, vec![row.clone()]).is_ok());
        let mut oversized = row.clone();
        oversized.canonical_bytes += 1;
        assert!(SourceReclamationProof::new(predecessor, retained, vec![oversized]).is_err());
        let two = History::new(
            predecessor.lineage(),
            predecessor.anchor(),
            predecessor.tail(),
            predecessor.tail(),
        )
        .unwrap();
        let count = usize::try_from(
            two.minimum_resume().sequence().get() - predecessor.minimum_resume().sequence().get(),
        )
        .unwrap();
        assert!(count > 1);
        assert!(SourceReclamationProof::new(predecessor, two, vec![row; count]).is_err());
    }
    // req: AFC-001, STO-030, STO-032, REC-001
    #[test]
    fn source_reclamation_v5_encoding_matches_its_frozen_fixture() {
        let (predecessor, retained, bytes) = fixture();
        let proof = SourceReclamationProof::new(
            predecessor,
            retained,
            vec![SourceReceiptPreimage::capture(&bytes).unwrap()],
        )
        .unwrap();
        let frame = JournalFrame::source_reclamation(proof, [5; HASH_BYTES]).unwrap();
        let encoded = frame.encode().unwrap();
        let text: String = encoded
            .as_bytes()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        if std::env::var_os("RIFFDB_REGENERATE_SOURCE_RECLAMATION_FIXTURE").is_some() {
            std::fs::write(
                std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("../../fixtures/compatibility/journal-frame-v5-source-reclamation.hex"),
                text + "\n",
            )
            .unwrap();
            return;
        }
        assert_eq!(
            text,
            include_str!(
                "../../../../fixtures/compatibility/journal-frame-v5-source-reclamation.hex"
            )
            .trim()
        );
        let (decoded, hash) = JournalFrame::decode(encoded.as_bytes()).unwrap();
        assert_eq!(decoded.encode().unwrap().as_bytes(), encoded.as_bytes());
        assert_eq!(hash, encoded.frame_hash());
    }
}
