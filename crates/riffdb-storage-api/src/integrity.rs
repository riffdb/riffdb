//! Bounded structural-integrity summaries with separate component ownership.
//!
//! [`crate::StructuralEvidenceSession`] is the sole executable complete-scan
//! interface. These values only aggregate its bounded pages after that session
//! proves exact end; they neither open a second validation path nor produce an
//! operational-readiness capability. Repairs remain outside this API.

use crate::{MAX_INTEGRITY_FINDINGS, StorageValueError, StructuralFinding, StructuralFindingScope};

/// Closed structural result for one component; this is not operational readiness.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StructuralComponentStatusV1 {
    /// The complete inspection observed no finding in this scope.
    Clear,
    /// The complete inspection observed one or more findings in this scope.
    FindingsPresent {
        /// Exact number observed even when diagnostic materialization truncated.
        count: u64,
    },
}

impl StructuralComponentStatusV1 {
    fn from_count(count: u64) -> Self {
        if count == 0 {
            Self::Clear
        } else {
            Self::FindingsPresent { count }
        }
    }
}

/// Complete-scan counts for authoritative and rebuildable structural scopes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StructuralFindingCountsV1 {
    authoritative: u64,
    outbox_delivery: u64,
    projection: u64,
}

impl StructuralFindingCountsV1 {
    /// Constructs exact complete-scan counts.
    #[must_use]
    pub const fn new(authoritative: u64, outbox_delivery: u64, projection: u64) -> Self {
        Self {
            authoritative,
            outbox_delivery,
            projection,
        }
    }

    /// Returns authoritative finding count.
    #[must_use]
    pub const fn authoritative(self) -> u64 {
        self.authoritative
    }

    /// Returns outbox-delivery finding count.
    #[must_use]
    pub const fn outbox_delivery(self) -> u64 {
        self.outbox_delivery
    }

    /// Returns projection finding count.
    #[must_use]
    pub const fn projection(self) -> u64 {
        self.projection
    }

    /// Returns the exact total, rejecting an impossible counter overflow.
    pub const fn checked_total(self) -> Option<u64> {
        match self.authoritative.checked_add(self.outbox_delivery) {
            Some(total) => total.checked_add(self.projection),
            None => None,
        }
    }
}

/// Bounded redacted report produced only after a complete structural scan.
///
/// The report distinguishes authoritative defects from derived outbox and
/// projection findings. It does not grant readiness and contains no repair API.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IntegrityReportV1 {
    findings: Vec<StructuralFinding>,
    counts: StructuralFindingCountsV1,
    truncated: bool,
}

impl IntegrityReportV1 {
    /// Validates bounded materialization and exact per-scope counts.
    pub fn new(
        findings: Vec<StructuralFinding>,
        counts: StructuralFindingCountsV1,
    ) -> Result<Self, StorageValueError> {
        if findings.len() > MAX_INTEGRITY_FINDINGS {
            return Err(StorageValueError::LimitExceeded);
        }
        let total = counts
            .checked_total()
            .ok_or(StorageValueError::SizeOverflow)?;
        let retained_count =
            u64::try_from(findings.len()).map_err(|_| StorageValueError::SizeOverflow)?;
        if total < retained_count {
            return Err(StorageValueError::InvalidShape);
        }

        let retained = retained_counts(&findings)?;
        if retained.authoritative > counts.authoritative
            || retained.outbox_delivery > counts.outbox_delivery
            || retained.projection > counts.projection
        {
            return Err(StorageValueError::InvalidShape);
        }

        Ok(Self {
            findings,
            counts,
            truncated: total > retained_count,
        })
    }

    /// Borrows at most 256 redacted findings.
    #[must_use]
    pub fn findings(&self) -> &[StructuralFinding] {
        &self.findings
    }

    /// Returns exact per-scope counts from the complete scan.
    #[must_use]
    pub const fn counts(&self) -> StructuralFindingCountsV1 {
        self.counts
    }

    /// Returns whether additional findings were omitted from diagnostics.
    #[must_use]
    pub const fn truncated(&self) -> bool {
        self.truncated
    }

    /// Returns at least the number of retained findings.
    #[must_use]
    pub const fn total_findings_at_least(&self) -> u64 {
        match self.counts.checked_total() {
            Some(total) => total,
            None => u64::MAX,
        }
    }

    /// Returns the authoritative structural status without claiming readiness.
    #[must_use]
    pub fn authoritative_status(&self) -> StructuralComponentStatusV1 {
        StructuralComponentStatusV1::from_count(self.counts.authoritative)
    }

    /// Returns the rebuildable outbox-overlay structural status.
    #[must_use]
    pub fn outbox_status(&self) -> StructuralComponentStatusV1 {
        StructuralComponentStatusV1::from_count(self.counts.outbox_delivery)
    }

    /// Returns the rebuildable projection structural status.
    #[must_use]
    pub fn projection_status(&self) -> StructuralComponentStatusV1 {
        StructuralComponentStatusV1::from_count(self.counts.projection)
    }
}

/// Bounded accumulator that keeps counting after diagnostic truncation.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct IntegrityReportAccumulatorV1 {
    findings: Vec<StructuralFinding>,
    authoritative: u64,
    outbox_delivery: u64,
    projection: u64,
}

impl IntegrityReportAccumulatorV1 {
    /// Observes one safe finding while retaining at most the hard report bound.
    pub fn observe(&mut self, finding: StructuralFinding) -> Result<(), StorageValueError> {
        let count = match finding.scope() {
            StructuralFindingScope::Authoritative => &mut self.authoritative,
            StructuralFindingScope::OutboxDelivery => &mut self.outbox_delivery,
            StructuralFindingScope::Projection => &mut self.projection,
        };
        *count = count
            .checked_add(1)
            .ok_or(StorageValueError::SizeOverflow)?;
        if self.findings.len() < MAX_INTEGRITY_FINDINGS {
            self.findings.push(finding);
        }
        Ok(())
    }

    /// Finishes a report after `StructuralEvidenceSession` establishes exact end.
    ///
    /// A clear report is diagnostic evidence only. The startup coordinator must
    /// still complete catalog validation before releasing operational storage.
    pub fn finish(self) -> Result<IntegrityReportV1, StorageValueError> {
        IntegrityReportV1::new(
            self.findings,
            StructuralFindingCountsV1::new(
                self.authoritative,
                self.outbox_delivery,
                self.projection,
            ),
        )
    }
}

fn retained_counts(
    findings: &[StructuralFinding],
) -> Result<StructuralFindingCountsV1, StorageValueError> {
    let mut accumulator = StructuralFindingCountsV1::new(0, 0, 0);
    for finding in findings {
        let count = match finding.scope() {
            StructuralFindingScope::Authoritative => &mut accumulator.authoritative,
            StructuralFindingScope::OutboxDelivery => &mut accumulator.outbox_delivery,
            StructuralFindingScope::Projection => &mut accumulator.projection,
        };
        *count = count
            .checked_add(1)
            .ok_or(StorageValueError::SizeOverflow)?;
    }
    Ok(accumulator)
}

#[cfg(test)]
mod tests {
    use crate::StructuralFindingCode;

    use super::*;

    #[test]
    fn authoritative_and_derived_statuses_remain_separate() {
        let mut accumulator = IntegrityReportAccumulatorV1::default();
        accumulator
            .observe(StructuralFinding::new(
                StructuralFindingScope::OutboxDelivery,
                StructuralFindingCode::OrphanedOutboxStatus,
            ))
            .expect("count finding");
        let report = accumulator.finish().expect("finish report");

        assert_eq!(
            report.authoritative_status(),
            StructuralComponentStatusV1::Clear
        );
        assert_eq!(
            report.outbox_status(),
            StructuralComponentStatusV1::FindingsPresent { count: 1 }
        );
    }

    #[test]
    fn accumulator_counts_beyond_materialized_bound() {
        let finding = StructuralFinding::new(
            StructuralFindingScope::Projection,
            StructuralFindingCode::ProjectionStateMismatch,
        );
        let mut accumulator = IntegrityReportAccumulatorV1::default();
        for _ in 0..=MAX_INTEGRITY_FINDINGS {
            accumulator.observe(finding).expect("count finding");
        }
        let report = accumulator.finish().expect("finish report");

        assert_eq!(report.findings().len(), MAX_INTEGRITY_FINDINGS);
        assert!(report.truncated());
        assert_eq!(
            report.projection_status(),
            StructuralComponentStatusV1::FindingsPresent { count: 257 }
        );
    }

    #[test]
    fn clear_report_remains_component_diagnostics_only() {
        let report = IntegrityReportAccumulatorV1::default()
            .finish()
            .expect("finish clear report");

        assert_eq!(
            report.authoritative_status(),
            StructuralComponentStatusV1::Clear
        );
        assert_eq!(report.outbox_status(), StructuralComponentStatusV1::Clear);
        assert_eq!(
            report.projection_status(),
            StructuralComponentStatusV1::Clear
        );
        assert_eq!(report.total_findings_at_least(), 0);
    }
}
