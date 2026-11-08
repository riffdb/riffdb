//! Fixed, pre-redacted evidence owners. No variant accepts a rendered string.

use crate::process_graph::{ProductionGraphShutdownError, ProductionGraphShutdownStageEvidence};

pub(crate) enum Diagnostic<'a> {
    ProcessBaseline,
    Shutdown,
    Extents,
    Receipts,
    SourceValidation,
    SourceAdmission,
    SourceTurn,
    SourceReclamation,
    DurableFlushGroups,
    Startup,
    GraphStageLabels,
    Checkpoint(ProductionGraphShutdownStageEvidence),
    FailedCheckpoint(&'a ProductionGraphShutdownError),
    RetentionReclamation(&'a riffdb_storage_redb::OnlineRetentionPhases),
    RetentionAdmission([u64; 8]),
    RetentionDrain(Option<[u64; 11]>),
    DebugProfile,
    PromotionRetryReady,
}

impl Diagnostic<'_> {
    pub(super) fn render(&self) -> String {
        match self {
            Self::ProcessBaseline => "riffdb-process-memory-baseline-v1".to_owned(),
            Self::Shutdown => crate::shutdown_census::format_v1_line(),
            Self::Extents => crate::shutdown_census::format_extent_line(),
            Self::Receipts => crate::shutdown_census::format_receipt_line(),
            Self::SourceValidation => crate::shutdown_census::format_source_validation_line(),
            Self::SourceAdmission => crate::shutdown_census::format_source_admission_line(),
            Self::SourceTurn => crate::shutdown_census::format_source_turn_line(),
            Self::SourceReclamation => crate::shutdown_census::format_source_reclamation_line(),
            Self::DurableFlushGroups => crate::shutdown_census::format_durable_flush_group_line(),
            Self::Startup => crate::startup_census::format_v1_line(),
            Self::GraphStageLabels => ProductionGraphShutdownStageEvidence::format_labels_v1_line(),
            Self::Checkpoint(evidence) => evidence.format_checkpoint_close_v1_line(),
            Self::FailedCheckpoint(error) => error.checkpoint_close_receipt()
                .map(|receipt| receipt.format_v1_line())
                .unwrap_or_else(|| "riffdb-graceful-checkpoint-close-v1\tavailable=0".to_owned()),
            Self::RetentionReclamation(phases) => crate::shutdown_census::format_source_reclamation_sample(phases),
            Self::RetentionAdmission(values) => crate::shutdown_census::format_source_admission_census(*values),
            Self::RetentionDrain(None) => "riffdb-retention-source-drain-v1\tavailable=0".to_owned(),
            Self::RetentionDrain(Some([unpublished,unaccounted,oversized,transitions,bytes,unpublished_transitions,unpublished_bytes,sequence_matches,administration_matches,hash_matches,frames])) => format!("riffdb-retention-source-drain-v1\tavailable=1\tunpublished={unpublished}\tunaccounted={unaccounted}\toversized={oversized}\tsuffix_transitions={transitions}\tsuffix_bytes={bytes}\tunpublished_transitions={unpublished_transitions}\tunpublished_bytes={unpublished_bytes}\tsequence_matches={sequence_matches}\tadministration_matches={administration_matches}\thash_matches={hash_matches}\tsuffix_frames={frames}"),
            Self::PromotionRetryReady => "riffdb-promotion-retry-ready-v1".to_owned(),
            Self::DebugProfile => "riffdb-census-profile-warning-v1\tdebug build: stage costs are inflated unevenly against I/O; use a release build for performance evidence".to_owned(),
        }
    }
}

pub(crate) fn emit(diagnostic: Diagnostic<'_>) {
    super::emit(super::OperationalEvent::Diagnostic(diagnostic));
}
