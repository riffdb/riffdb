//! Frozen unchanged-suite status for the isolated comparison.

use std::fmt::Write as _;

/// Conformance evidence class.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConformanceClass {
    /// The accepted RiffDB semantic storage contract.
    Semantic,
    /// Engine-substrate behavior that is useful but insufficient for conformance.
    Substrate,
    /// Architecture and dependency isolation.
    Architecture,
}

impl ConformanceClass {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Semantic => "semantic",
            Self::Substrate => "substrate",
            Self::Architecture => "architecture",
        }
    }
}

/// Closed result for one frozen comparison case.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConformanceStatus {
    /// The unchanged case executed and passed.
    Passed,
    /// The unchanged case did not pass and no semantic exception was introduced.
    Failed,
}

impl ConformanceStatus {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Passed => "passed",
            Self::Failed => "failed",
        }
    }
}

/// One bounded frozen conformance result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConformanceCase {
    /// Stable case identifier.
    pub id: &'static str,
    /// Evidence class.
    pub class: ConformanceClass,
    /// Exact result.
    pub status: ConformanceStatus,
    /// Bounded static classification or evidence note.
    pub detail: &'static str,
}

/// Exact WP-075 unchanged-suite inventory.
///
/// Passing substrate cases never compensate for a failed semantic case.
pub const CONFORMANCE_CASES: &[ConformanceCase] = &[
    ConformanceCase {
        id: "architecture.nested-workspace-isolation",
        class: ConformanceClass::Architecture,
        status: ConformanceStatus::Passed,
        detail: "separate workspace and lockfile; no root workspace member",
    },
    ConformanceCase {
        id: "architecture.fjall-pin-and-features",
        class: ConformanceClass::Architecture,
        status: ConformanceStatus::Passed,
        detail: "fjall exactly 3.1.8 with default features disabled",
    },
    ConformanceCase {
        id: "architecture.v2-registry-shape",
        class: ConformanceClass::Architecture,
        status: ConformanceStatus::Passed,
        detail: "27 readable; 26 writable; V1 decode-only; V2 writable",
    },
    ConformanceCase {
        id: "architecture.identity-only-migration-contract",
        class: ConformanceClass::Architecture,
        status: ConformanceStatus::Passed,
        detail: "accepted StartupIndexMigrationPort exposes identity only",
    },
    ConformanceCase {
        id: "substrate.cross-keyspace-atomic-commit",
        class: ConformanceClass::Substrate,
        status: ConformanceStatus::Passed,
        detail: "bounded canonical multi-keyspace commit and reopen",
    },
    ConformanceCase {
        id: "substrate.owned-read-snapshot",
        class: ConformanceClass::Substrate,
        status: ConformanceStatus::Passed,
        detail: "snapshot retains one consistent pre-write view",
    },
    ConformanceCase {
        id: "substrate.bounded-physical-range-page",
        class: ConformanceClass::Substrate,
        status: ConformanceStatus::Passed,
        detail: "strict physical order, lower continuation, explicit exact end",
    },
    ConformanceCase {
        id: "substrate.migration-dual-ledger-pagination",
        class: ConformanceClass::Substrate,
        status: ConformanceStatus::Passed,
        detail: "500 rows and independent 4 MiB evidence/instruction ledgers",
    },
    ConformanceCase {
        id: "semantic.complete-storage-port-set",
        class: ConformanceClass::Semantic,
        status: ConformanceStatus::Failed,
        detail: "adapter does not implement the unchanged semantic persistence traits",
    },
    ConformanceCase {
        id: "semantic.command-candidate-type-state-and-atomic-graph",
        class: ConformanceClass::Semantic,
        status: ConformanceStatus::Failed,
        detail: "not implemented; no sequence or atomic-command conformance claim",
    },
    ConformanceCase {
        id: "semantic.snapshot-observations-and-dependencies",
        class: ConformanceClass::Semantic,
        status: ConformanceStatus::Failed,
        detail: "not implemented; physical snapshots are not ReadSnapshot evidence",
    },
    ConformanceCase {
        id: "semantic.idempotency-outcome-commit-event-provenance-reciprocity",
        class: ConformanceClass::Semantic,
        status: ConformanceStatus::Failed,
        detail: "not implemented; substrate transactions are not semantic records",
    },
    ConformanceCase {
        id: "semantic.catalog-capability-audit-and-bootstrap",
        class: ConformanceClass::Semantic,
        status: ConformanceStatus::Failed,
        detail: "not implemented",
    },
    ConformanceCase {
        id: "semantic.outbox-transitions-and-recovery-scan",
        class: ConformanceClass::Semantic,
        status: ConformanceStatus::Failed,
        detail: "not implemented",
    },
    ConformanceCase {
        id: "semantic.projection-identity-generation-lifecycle-frontier",
        class: ConformanceClass::Semantic,
        status: ConformanceStatus::Failed,
        detail: "PRJ-001 through PRJ-004 unchanged suite not implemented",
    },
    ConformanceCase {
        id: "semantic.projection-marker-hash-and-atomic-apply",
        class: ConformanceClass::Semantic,
        status: ConformanceStatus::Failed,
        detail: "not implemented; no frontier monotonicity claim",
    },
    ConformanceCase {
        id: "semantic.structural-and-historical-exact-end-session",
        class: ConformanceClass::Semantic,
        status: ConformanceStatus::Failed,
        detail: "StructuralEvidenceOpen and session-bound exact-end scan not implemented",
    },
    ConformanceCase {
        id: "semantic.sealed-catalog-migration-backend",
        class: ConformanceClass::Semantic,
        status: ConformanceStatus::Failed,
        detail: "CatalogIndexMigrationBackend and fresh post-migration validation not implemented",
    },
    ConformanceCase {
        id: "semantic.v1-v2-compare-rewrite-and-restart",
        class: ConformanceClass::Semantic,
        status: ConformanceStatus::Failed,
        detail: "no V1-to-V2 rewrite or mixed-state recovery claim",
    },
    ConformanceCase {
        id: "semantic.process-crash-failpoint-matrix",
        class: ConformanceClass::Semantic,
        status: ConformanceStatus::Failed,
        detail: "named semantic failpoints not implemented",
    },
];

/// Returns the aggregate result without allowing substrate passes to mask failure.
#[must_use]
pub fn overall_conformance_status() -> ConformanceStatus {
    if CONFORMANCE_CASES
        .iter()
        .filter(|case| case.class == ConformanceClass::Semantic)
        .all(|case| case.status == ConformanceStatus::Passed)
    {
        ConformanceStatus::Passed
    } else {
        ConformanceStatus::Failed
    }
}

/// Renders the deterministic machine-readable WP-075 TSV report.
#[must_use]
pub fn render_conformance_report() -> String {
    let mut report = String::from(
        "schema\triffdb.storage-fjall-conformance/v1\n\
         engine\tfjall\n\
         engine_version\t3.1.8\n\
         default_features\tfalse\n\
         semantic_contract\tunchanged\n\
         overall\tconformance_failure\n\
         performance_eligibility\tnot_run_due_to_conformance_failure\n\
         case_id\tclass\tstatus\tdetail\n",
    );
    for case in CONFORMANCE_CASES {
        writeln!(
            report,
            "{}\t{}\t{}\t{}",
            case.id,
            case.class.as_str(),
            case.status.as_str(),
            case.detail
        )
        .expect("writing a String cannot fail");
    }
    report
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn semantic_failure_cannot_be_hidden_by_substrate_passes() {
        assert_eq!(overall_conformance_status(), ConformanceStatus::Failed);
        assert!(
            CONFORMANCE_CASES
                .iter()
                .any(|case| case.class == ConformanceClass::Semantic
                    && case.status == ConformanceStatus::Failed)
        );
    }
}
