//! Closed, fully validated maintenance refresh. All population work is off gate.

use super::*;
use riffdb_storage_api::{EvidencePageLimit, StartupValidationInputs};
use std::sync::atomic::AtomicBool;

impl RedbOnlineRetentionLane {
    /// Reestablishes optional startup evidence after retention invalidated it.
    /// The maintenance owner supplies the same readable inventory as startup.
    /// Complete validation is mandatory; cancellation is checked before every
    /// bounded page. Changed state discards the candidate, leaving evidence
    /// absent. This never forces journal materialization or waits for a writer.
    pub fn refresh_validated_prefix(
        &self,
        inputs: StartupValidationInputs,
        cancellation: Arc<AtomicBool>,
    ) -> Result<u64, StorageError> {
        let (scrub, captured_us) = crate::store::PendingPrefixRefresh::begin_if_needed(
            &self.shared,
            inputs,
            Arc::clone(&cancellation),
        )?;
        let Some(mut scrub) = scrub else {
            return Ok(captured_us);
        };
        let limit = EvidencePageLimit::new(64)
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
        scrub.validate_retained_history_pages(limit, || {
            std::thread::yield_now();
            !cancellation.load(std::sync::atomic::Ordering::Acquire)
        })?;
        let structural = loop {
            std::thread::yield_now();
            match scrub.read_structural_page(limit)? {
                crate::startup::BackgroundScrubPage::Page { findings, .. } => {
                    if !findings.is_empty() {
                        return Err(storage_error(StorageErrorKind::CorruptData));
                    }
                }
                crate::startup::BackgroundScrubPage::ExactEnd(end) => break end,
            }
        };
        let historical = scrub.validate_catalog_history(limit, || {
            std::thread::yield_now();
            !cancellation.load(std::sync::atomic::Ordering::Acquire)
        })?;
        let proof = scrub.finish_for_prefix_refresh(structural, historical)?;
        let candidate = crate::store::PendingPrefixRefresh::prepare(proof)?;
        let (publication, publication_us) = candidate.publish()?;
        if publication == crate::store::PrefixRefreshPublication::Cancelled {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        Ok(captured_us.saturating_add(publication_us))
    }
}
