//! Page-driven form of the complete retained-chain and source-evidence pass.
//! An unfinished or failed walk never creates a reclamation proof.

use std::ops::Bound;
use std::sync::Arc;

use redb::ReadableTableMetadata;
use riffdb_storage_api::{
    AuthoritativeStateCatalogV2, AuthoritativeTransactionV3, ChangelogAttributionV3,
    ChangelogHistoryStateV3, EvidencePageLimit, MAX_CHANGELOG_FRAME_BYTES, StorageError,
    StorageErrorKind,
};

use crate::changelog_v3_activation::{HISTORY, SOURCE_HOLDS};
use crate::checkpoint_root::CheckpointRoot;
use crate::error::{precommit_storage_error, storage_error, table_error};
use crate::layout::{AUDIT, AUDIT_BY_REQUEST, META};
use crate::primary_fence_write::state::FenceValidation;
use crate::promotion_cutover::PromotionValidation;
use crate::replication_registration_links::RegistrationLinksValidation;

const MAX_ROWS_PER_STEP: usize = 256;
const AUDIT_PAGE_BYTES: usize = riffdb_proto::envelope::MAX_STORED_ENVELOPE_BYTES;

#[derive(Clone, Copy, Eq, PartialEq)]
enum Phase {
    Receipts,
    Audit,
    Done,
}

pub(crate) struct RetainedHistoryValidation {
    root: Arc<CheckpointRoot>,
    history: ChangelogHistoryStateV3,
    covered: ChangelogHistoryStateV3,
    first: bool,
    after_receipt: Option<[u8; 8]>,
    after_audit: Option<Vec<u8>>,
    independent_sequence: u64,
    registrations: RegistrationLinksValidation,
    fence: Option<FenceValidation>,
    promotion: PromotionValidation,
    phase: Phase,
}

/// Sealed result of the exhaustive exact-end pass over this one immutable pin.
pub(crate) struct ValidatedRetainedHistory {
    root: Arc<CheckpointRoot>,
    history: ChangelogHistoryStateV3,
    independent_sequence: u64,
}

impl ValidatedRetainedHistory {
    pub(crate) fn root(&self) -> &Arc<CheckpointRoot> {
        &self.root
    }

    pub(crate) fn history(&self) -> ChangelogHistoryStateV3 {
        self.history
    }

    /// Excludes both reclamation and prefix-refresh receipts, preventing an
    /// idle maintenance feedback loop from manufacturing independent progress.
    pub(crate) fn independent_sequence(&self) -> u64 {
        self.independent_sequence
    }
}

impl RetainedHistoryValidation {
    pub(crate) fn new(root: Arc<CheckpointRoot>) -> Result<Option<Self>, StorageError> {
        let Some(history) = crate::changelog_v3_roots::read_checkpoint_roots(&root)? else {
            return Ok(None);
        };
        let receipts = root.open_table(HISTORY).map_err(table_error)?;
        let empty = receipts.is_empty().map_err(precommit_storage_error)?;
        let holds = root.open_table(SOURCE_HOLDS).map_err(table_error)?;
        let registrations = RegistrationLinksValidation::new(&holds, history)?;
        let fence = if !empty
            && history.lineage().catalog_digest() == AuthoritativeStateCatalogV2.digest()
        {
            let meta = root.open_table(META).map_err(table_error)?;
            let admission = crate::primary_admission_roots::read(&meta)?.ok_or_else(corrupt)?;
            Some(FenceValidation::new(history, admission))
        } else {
            None
        };
        let covered = ChangelogHistoryStateV3::new(
            history.lineage(),
            history.anchor(),
            history.minimum_resume(),
            history.minimum_resume(),
        )
        .map_err(|_| corrupt())?;
        Ok(Some(Self {
            root,
            history,
            covered,
            first: true,
            after_receipt: None,
            after_audit: None,
            independent_sequence: 0,
            registrations,
            fence,
            promotion: PromotionValidation::default(),
            // Existing full validation skips ancillary source checks for the
            // exact attached empty history admitted by read_checkpoint_roots.
            phase: if empty { Phase::Done } else { Phase::Receipts },
        }))
    }

    pub(crate) fn advance(&mut self, limit: EvidencePageLimit) -> Result<bool, StorageError> {
        let rows = usize::try_from(limit.get())
            .map_err(|_| corrupt())?
            .min(MAX_ROWS_PER_STEP);
        match self.phase {
            Phase::Receipts => self.receipts(rows),
            Phase::Audit => self.audit(rows),
            Phase::Done => Ok(true),
        }
    }

    pub(crate) fn complete(self) -> Result<ValidatedRetainedHistory, StorageError> {
        if self.phase != Phase::Done {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        Ok(ValidatedRetainedHistory {
            root: self.root,
            history: self.history,
            independent_sequence: self.independent_sequence,
        })
    }

    fn receipts(&mut self, limit: usize) -> Result<bool, StorageError> {
        let table = self.root.open_table(HISTORY).map_err(table_error)?;
        let low = self
            .after_receipt
            .as_ref()
            .map_or(Bound::Unbounded, |key| Bound::Excluded(key.as_slice()));
        let mut bytes = 0_usize;
        for (rows, entry) in table
            .range::<&[u8]>((low, Bound::Unbounded))
            .map_err(precommit_storage_error)?
            .enumerate()
        {
            if rows == limit {
                return Ok(false);
            }
            let (key, value) = entry.map_err(precommit_storage_error)?;
            let encoded = crate::history_stub::resolve_history_value(value.value())?;
            if encoded.len() > MAX_CHANGELOG_FRAME_BYTES {
                return Err(corrupt());
            }
            let next = bytes.checked_add(encoded.len()).ok_or_else(corrupt)?;
            if next > MAX_CHANGELOG_FRAME_BYTES {
                return Ok(false);
            }
            let receipt = AuthoritativeTransactionV3::decode(&encoded).map_err(|_| corrupt())?;
            if key.value() != receipt.binding().sequence.get().to_be_bytes()
                || receipt.binding().sequence > self.history.tail().sequence()
            {
                return Err(corrupt());
            }
            if self.first {
                self.covered
                    .validate_terminal_receipt(&receipt)
                    .map_err(|_| corrupt())?;
                self.first = false;
            } else {
                self.covered = self.covered.advance(&receipt).map_err(|_| corrupt())?;
            }
            if !matches!(
                receipt.attribution(),
                ChangelogAttributionV3::HistoryReclamation
                    | ChangelogAttributionV3::ValidatedPrefixCheckpoint
            ) {
                self.independent_sequence = receipt.binding().sequence.get();
            }
            self.after_receipt = Some(key.value().try_into().map_err(|_| corrupt())?);
            bytes = next;
        }
        if self.first || self.covered != self.history {
            return Err(corrupt());
        }
        let holds = self.root.open_table(SOURCE_HOLDS).map_err(table_error)?;
        crate::changelog_v3_roots::validate_source_holds(&holds, self.history, &table)?;
        self.phase = Phase::Audit;
        Ok(false)
    }

    fn audit(&mut self, limit: usize) -> Result<bool, StorageError> {
        let audit = self.root.open_table(AUDIT).map_err(table_error)?;
        let indexes = self
            .root
            .open_table(AUDIT_BY_REQUEST)
            .map_err(table_error)?;
        let receipts = self.root.open_table(HISTORY).map_err(table_error)?;
        let low = self
            .after_audit
            .as_ref()
            .map_or(Bound::Unbounded, |key| Bound::Excluded(key.as_slice()));
        let mut bytes = 0_usize;
        for (rows, entry) in audit
            .range::<&[u8]>((low, Bound::Unbounded))
            .map_err(precommit_storage_error)?
            .enumerate()
        {
            if rows == limit {
                return Ok(false);
            }
            let (key, encoded) = entry.map_err(precommit_storage_error)?;
            if key.value().len() > riffdb_types::MAX_KEY_BYTES
                || encoded.value().len() > AUDIT_PAGE_BYTES
            {
                return Err(storage_error(StorageErrorKind::LimitExceeded));
            }
            let next = bytes
                .checked_add(encoded.value().len())
                .ok_or_else(corrupt)?;
            if next > AUDIT_PAGE_BYTES {
                return Ok(false);
            }
            self.registrations
                .observe(key.value(), encoded.value(), &receipts)?;
            if let Some(fence) = &mut self.fence {
                fence.observe(key.value(), encoded.value(), &receipts)?;
            }
            self.promotion.observe(
                (key.value(), encoded.value()),
                self.history,
                &audit,
                &indexes,
                &receipts,
            )?;
            self.after_audit = Some(key.value().to_vec());
            bytes = next;
        }
        self.registrations.finish()?;
        if let Some(fence) = &self.fence {
            fence.finish()?;
        }
        self.promotion.finish(self.history, &receipts)?;
        self.phase = Phase::Done;
        Ok(true)
    }
}

fn corrupt() -> StorageError {
    storage_error(StorageErrorKind::CorruptData)
}

#[cfg(test)]
mod tests {
    use super::*;
    use redb::ReadableDatabase;

    // req: STO-030, STO-031, STO-032
    #[test]
    fn partial_history_pages_never_prove_reclamation_and_exact_end_matches_full_validation() {
        let (_scope, ports, _) = crate::changelog_v3_control_tests::fixture();
        let root = Arc::new(CheckpointRoot::new(
            ports.shared.database.begin_read().unwrap(),
            0,
        ));
        let expected = crate::changelog_v3_roots::validate_retained_history(&root)
            .unwrap()
            .unwrap();
        let mut walk = RetainedHistoryValidation::new(root.clone())
            .unwrap()
            .unwrap();
        assert!(!walk.advance(EvidencePageLimit::new(1).unwrap()).unwrap());
        assert_eq!(
            walk.complete().err().unwrap().kind(),
            StorageErrorKind::Unavailable
        );
        let mut walk = RetainedHistoryValidation::new(root).unwrap().unwrap();
        let mut pages = 0;
        while !walk.advance(EvidencePageLimit::new(1).unwrap()).unwrap() {
            pages += 1;
        }
        assert!(pages >= 7);
        let proof = walk.complete().unwrap();
        assert_eq!(proof.history(), expected);
        assert_eq!(
            proof.independent_sequence(),
            expected.tail().sequence().get()
        );
    }
}
