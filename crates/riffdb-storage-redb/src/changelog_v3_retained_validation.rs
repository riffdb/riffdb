//! Page-driven form of the complete retained-chain and source-evidence pass.
//! An unfinished or failed walk never creates a reclamation proof.

use std::ops::Bound;
use std::sync::Arc;

use riffdb_storage_api::{
    AuthoritativeStateCatalogV2, ChangelogAttributionV3, ChangelogHistoryStateV3,
    EvidencePageLimit, MAX_CHANGELOG_FRAME_BYTES, StorageError, StorageErrorKind,
};

use crate::changelog_v3_activation::{HISTORY, SOURCE_HOLDS};
use crate::checkpoint_root::CheckpointRoot;
use crate::error::{precommit_storage_error, storage_error, table_error};
use crate::layout::{AUDIT, AUDIT_BY_REQUEST, META};
use crate::primary_fence_write::state::FenceValidation;
use crate::promotion_cutover::PromotionValidation;
use crate::source_validation_pin::{SourceIdentity, ValidationSource};
use crate::source_validation_read::{CompositePoints, ReceiptRead};

#[path = "changelog_v3_retained_composite.rs"]
mod composite;
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
    root: Option<ValidationSource>,
    source_identity: SourceIdentity,
    root_identity: u64,
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
    qualification: Option<u64>,
    initial_pass_complete: bool,
    step_rows: [u64; 2],
    #[cfg(test)]
    rows_read: [u64; 2],
}

/// Sealed result of the exhaustive exact-end pass over this one immutable pin.
pub(crate) struct ValidatedRetainedHistory {
    root: ValidationSource,
    history: ChangelogHistoryStateV3,
    independent_sequence: u64,
}

impl ValidatedRetainedHistory {
    pub(crate) fn root(&self) -> &Arc<CheckpointRoot> {
        self.root.checkpoint_pin()
    }

    pub(crate) fn source(&self) -> &ValidationSource {
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
        Self::new_source(ValidationSource::Checkpoint(root))
    }

    pub(crate) fn new_source(root: ValidationSource) -> Result<Option<Self>, StorageError> {
        let Some(history) = root.history()? else {
            return Ok(None);
        };
        let empty = root.empty()?;
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
            root_identity: root.identity(),
            source_identity: root.publication(),
            root: Some(root),
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
            qualification: None,
            initial_pass_complete: empty,
            step_rows: [0; 2],
            #[cfg(test)]
            rows_read: [0; 2],
        }))
    }

    #[cfg(test)]
    pub(crate) fn validation_pin(&self) -> Option<std::sync::Weak<CheckpointRoot>> {
        self.root
            .as_ref()
            .map(|root| Arc::downgrade(root.checkpoint_pin()))
    }

    #[cfg(test)]
    pub(crate) fn rows_read(&self) -> [u64; 2] {
        self.rows_read
    }

    pub(crate) fn clear_step_rows(&mut self) {
        self.step_rows = [0; 2];
    }
    pub(crate) fn step_rows(&self) -> [u64; 2] {
        self.step_rows
    }

    pub(crate) fn root_identity(&self) -> u64 {
        self.root_identity
    }

    /// A completed certificate consists of checked native state and an exact
    /// qualification binding. It needs no obsolete reader pin between turns.
    pub(crate) fn release_completed_pin(&mut self) {
        if self.is_complete() {
            self.root = None;
        }
    }

    pub(crate) fn independent_sequence(&self) -> u64 {
        self.independent_sequence
    }

    pub(crate) fn bind_qualification(&mut self, epoch: u64) {
        self.qualification = (epoch != u64::MAX).then_some(epoch);
    }

    pub(crate) fn qualifies(&self, epoch: u64) -> bool {
        epoch != u64::MAX && self.qualification == Some(epoch)
    }

    pub(crate) fn is_complete(&self) -> bool {
        self.phase == Phase::Done
    }

    pub(crate) fn proof(&self) -> Result<ValidatedRetainedHistory, StorageError> {
        if !self.is_complete() {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        Ok(ValidatedRetainedHistory {
            root: Clone::clone(
                self.root
                    .as_ref()
                    .ok_or_else(|| storage_error(StorageErrorKind::Unavailable))?,
            ),
            history: self.history,
            independent_sequence: self.independent_sequence,
        })
    }

    /// Previously visited rows are reusable only through the native mutation
    /// owner's closed qualification. Canonical metadata and every appended row
    /// still have to agree with the exact successor pin.
    pub(crate) fn continue_on(
        &mut self,
        root: Arc<CheckpointRoot>,
        epoch: u64,
    ) -> Result<bool, StorageError> {
        self.continue_source(ValidationSource::Checkpoint(root), epoch)
    }

    pub(crate) fn continue_source(
        &mut self,
        root: ValidationSource,
        epoch: u64,
    ) -> Result<bool, StorageError> {
        if !self.qualifies(epoch) || !self.is_complete() {
            return Ok(false);
        }
        if root.publication() == self.source_identity {
            self.root = Some(root);
            return Ok(true);
        }
        let history = root.history()?.ok_or_else(corrupt)?;
        if history.lineage() != self.history.lineage()
            || history.anchor() != self.history.anchor()
            || history.minimum_resume() != self.history.minimum_resume()
            || history.tail().sequence() < self.history.tail().sequence()
        {
            return Ok(false);
        }
        self.rebind_native_evidence(&root, history)?;
        self.root_identity = root.identity();
        self.source_identity = root.publication();
        self.root = Some(root);
        self.history = history;
        self.phase = if self.covered == history {
            Phase::Done
        } else {
            Phase::Receipts
        };
        Ok(true)
    }

    fn rebind_native_evidence(
        &mut self,
        root: &ValidationSource,
        history: ChangelogHistoryStateV3,
    ) -> Result<(), StorageError> {
        self.registrations.rebind_unchanged(history)?;
        if let Some(fence) = &mut self.fence {
            let meta = root.open_table(META).map_err(table_error)?;
            let admission = crate::primary_admission_roots::read(&meta)?.ok_or_else(corrupt)?;
            fence.rebind_unchanged(history, admission)?;
        }
        Ok(())
    }

    /// At most one native receipt page and one native audit page under the
    /// drained gate. An unfinished suffix yields; never walk population here.
    pub(crate) fn close_suffix_page(&mut self) -> Result<bool, StorageError> {
        if self.phase == Phase::Receipts {
            self.receipts(MAX_ROWS_PER_STEP)?;
            if self.phase == Phase::Receipts {
                return Ok(false);
            }
        }
        if self.phase == Phase::Audit {
            return self.audit(MAX_ROWS_PER_STEP);
        }
        Ok(self.is_complete())
    }

    /// Only the single owner's sealed, predecessor-checked deletion may supply
    /// this root. Deleted evidence is dropped; the new receipt is validated
    /// canonically before the rebased certificate is published.
    pub(crate) fn rebase_own_deletion(
        &mut self,
        root: Arc<CheckpointRoot>,
        epoch: u64,
    ) -> Result<(), StorageError> {
        self.rebase_own_source_deletion(ValidationSource::Checkpoint(root), epoch)
    }

    pub(crate) fn rebase_own_source_deletion(
        &mut self,
        root: ValidationSource,
        epoch: u64,
    ) -> Result<(), StorageError> {
        if !self.is_complete() || epoch == u64::MAX {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        let history = root.history()?.ok_or_else(corrupt)?;
        if history.lineage() != self.history.lineage()
            || history.anchor() != self.history.anchor()
            || history.minimum_resume().sequence() <= self.history.minimum_resume().sequence()
            || history.minimum_resume().sequence().get()
                - self.history.minimum_resume().sequence().get()
                > MAX_ROWS_PER_STEP as u64
            || self.history.tail().sequence().get().checked_add(1)
                != Some(history.tail().sequence().get())
        {
            return Err(corrupt());
        }
        self.rebind_native_evidence(&root, history)?;
        self.covered = ChangelogHistoryStateV3::new(
            history.lineage(),
            history.anchor(),
            self.covered.tail(),
            history.minimum_resume(),
        )
        .map_err(|_| corrupt())?;
        self.root_identity = root.identity();
        self.source_identity = root.publication();
        self.root = Some(root);
        self.history = history;
        self.phase = Phase::Receipts;
        self.bind_qualification(epoch);
        if !self.close_suffix_page()? {
            return Err(corrupt());
        }
        Ok(())
    }

    /// Only the sealed native acknowledgement publication may rebind changed
    /// hold evidence. Its same-generation policy delta was checked against the
    /// complete predecessor proof; validate the actual new receipt and audit
    /// end again before retaining the successor certificate.
    pub(crate) fn rebase_own_acknowledgement(
        &mut self,
        publication: &crate::store::AcknowledgementPublication,
    ) -> Result<(), StorageError> {
        let Some((predecessor, successor, root, epoch)) = publication.successor() else {
            return Ok(());
        };
        if !self.is_complete()
            || self.history != predecessor
            || self.qualification.and_then(|prior| prior.checked_add(1)) != Some(epoch)
            || epoch == u64::MAX
            || successor.lineage() != predecessor.lineage()
            || successor.anchor() != predecessor.anchor()
            || successor.minimum_resume() != predecessor.minimum_resume()
            || predecessor.tail().sequence().checked_next() != Some(successor.tail().sequence())
        {
            return Err(corrupt());
        }
        let root = ValidationSource::Checkpoint(Arc::clone(root));
        if root.history()? != Some(successor) {
            return Err(corrupt());
        }
        let independent_sequence = self.independent_sequence;
        self.rebind_native_evidence(&root, successor)?;
        self.root_identity = root.identity();
        self.source_identity = root.publication();
        self.root = Some(root);
        self.history = successor;
        self.phase = Phase::Receipts;
        self.bind_qualification(epoch);
        if !self.close_suffix_page()? {
            return Err(corrupt());
        }
        // This owner's acknowledgement changes the floor, never its original
        // independently checkpointed license or independent-progress census.
        self.independent_sequence = independent_sequence;
        Ok(())
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
            root: self
                .root
                .ok_or_else(|| storage_error(StorageErrorKind::Unavailable))?,
            history: self.history,
            independent_sequence: self.independent_sequence,
        })
    }

    fn receipts(&mut self, limit: usize) -> Result<bool, StorageError> {
        if matches!(self.root.as_ref(), Some(ValidationSource::Composite(_))) {
            return self.composite_receipts(limit);
        }
        let root = self
            .root
            .as_ref()
            .ok_or_else(|| storage_error(StorageErrorKind::Unavailable))?;
        let table = root.open_table(HISTORY).map_err(table_error)?;
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
            let summary = crate::history_stub::resolve_history_summary(value.value())?;
            if summary.encoded_len() > MAX_CHANGELOG_FRAME_BYTES {
                return Err(corrupt());
            }
            let next = bytes
                .checked_add(summary.encoded_len())
                .ok_or_else(corrupt)?;
            if next > MAX_CHANGELOG_FRAME_BYTES {
                return Ok(false);
            }
            self.step_rows[usize::from(self.initial_pass_complete)] =
                self.step_rows[usize::from(self.initial_pass_complete)].saturating_add(1);
            #[cfg(test)]
            {
                self.rows_read[0] += 1;
            }
            if key.value() != summary.binding().sequence.get().to_be_bytes()
                || summary.binding().sequence > self.history.tail().sequence()
            {
                return Err(corrupt());
            }
            if self.first {
                summary
                    .validate_terminal_receipt(self.covered)
                    .map_err(|_| corrupt())?;
                self.first = false;
            } else {
                self.covered = summary
                    .advance_history(self.covered)
                    .map_err(|_| corrupt())?;
            }
            if !matches!(
                summary.attribution(),
                ChangelogAttributionV3::HistoryReclamation
                    | ChangelogAttributionV3::ValidatedPrefixCheckpoint
            ) {
                self.independent_sequence = summary.binding().sequence.get();
            }
            self.after_receipt = Some(key.value().try_into().map_err(|_| corrupt())?);
            bytes = next;
        }
        if self.first || self.covered != self.history {
            return Err(corrupt());
        }
        let holds = root.open_table(SOURCE_HOLDS).map_err(table_error)?;
        crate::changelog_v3_roots::validate_source_holds(&holds, self.history, &table)?;
        self.phase = Phase::Audit;
        Ok(false)
    }

    fn audit(&mut self, limit: usize) -> Result<bool, StorageError> {
        if matches!(self.root.as_ref(), Some(ValidationSource::Composite(_))) {
            return self.composite_audit(limit);
        }
        let root = self
            .root
            .as_ref()
            .ok_or_else(|| storage_error(StorageErrorKind::Unavailable))?;
        let audit = root.open_table(AUDIT).map_err(table_error)?;
        let indexes = root.open_table(AUDIT_BY_REQUEST).map_err(table_error)?;
        let holds = root.open_table(SOURCE_HOLDS).map_err(table_error)?;
        let receipts = root.open_table(HISTORY).map_err(table_error)?;
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
            self.step_rows[usize::from(self.initial_pass_complete)] =
                self.step_rows[usize::from(self.initial_pass_complete)].saturating_add(1);
            #[cfg(test)]
            {
                self.rows_read[1] += 1;
            }
            self.registrations
                .observe(key.value(), encoded.value(), &holds, &receipts)?;
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
        self.initial_pass_complete = true;
        Ok(true)
    }
}

fn corrupt() -> StorageError {
    storage_error(StorageErrorKind::CorruptData)
}

#[cfg(test)]
pub(crate) fn assert_full_pinned_oracle(read: redb::ReadTransaction) {
    let root = Arc::new(CheckpointRoot::new(read, 0));
    let expected = crate::changelog_v3_roots::validate_retained_history(&root)
        .unwrap()
        .unwrap();
    let mut walk = RetainedHistoryValidation::new(root).unwrap().unwrap();
    while !walk.advance(EvidencePageLimit::new(1).unwrap()).unwrap() {}
    assert_eq!(walk.complete().unwrap().history(), expected);
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
