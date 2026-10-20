//! Projection-owned consuming replay-source protocol. Native only supplies custody.
//! Complete offline input and checkpoint orchestration share the sealed protocol.
//! Generic backends cannot convert their invariant instructions to the actual
//! native backend, obtain its private instance brand, or mint its confirmation.
use crate::ProjectionEvaluationError;
use riffdb_storage_api::StorageError;

mod complete_input;
mod driver;
mod input;
mod maintenance;
mod plan;
mod seal;
mod staging;
pub use complete_input::*;
pub use driver::*;
pub use input::{
    SourceCommandEvidence, SourceCommandResponse, SourceExactEndEvidence, SourceReadInstruction,
    SourceRewindEvidence, SourceRewindInstruction,
};
pub use maintenance::*;
pub use plan::SourceDriverPlan;
pub use staging::*;

/// Backend type identity and private live-instance identities. This trait alone
/// provides no read, staging, evaluation, maintenance or completion operation.
/// The concrete native implementation and brand constructors remain private.
#[doc(hidden)]
pub trait SourceCheckpointBackend: Sized {
    /// Non-cloneable actual captured native session; reference never escapes.
    type Brand: ?Sized;
    /// Actual nonzero stage instance, distinct from the session and numeric IDs.
    type StageBrand: ?Sized;
}

/// Bounded, value-free refusal. A failed driver never fabricates a candidate,
/// exact end, native publication result, rollback or durable reconciliation.
#[derive(Debug)]
pub enum SourceDriverError {
    /// Actual captured plan/input/source capacity or eligibility is unavailable.
    SourceUnavailable,
    /// A response belongs to another engine/session/stage or wrapper backend.
    ForeignCustody,
    /// The complete command or captured immutable input changed unexpectedly.
    InputChanged,
    /// Exact end was asserted before the complete pinned application prefix.
    IncompleteInput,
    /// Native bounded physical operation failed.
    Storage(StorageError),
    /// Existing complete-command projection interpretation refused.
    Evaluation(ProjectionEvaluationError),
}
impl std::fmt::Display for SourceDriverError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::SourceUnavailable => "source unavailable",
            Self::ForeignCustody => "source custody mismatch",
            Self::InputChanged => "source input changed",
            Self::IncompleteInput => "source input incomplete",
            Self::Storage(_) => "source storage unavailable",
            Self::Evaluation(_) => "source projection evaluation refused",
        })
    }
}
impl std::error::Error for SourceDriverError {}
impl From<StorageError> for SourceDriverError {
    fn from(error: StorageError) -> Self {
        Self::Storage(error)
    }
}
impl From<ProjectionEvaluationError> for SourceDriverError {
    fn from(error: ProjectionEvaluationError) -> Self {
        Self::Evaluation(error)
    }
}

/// Fixed original-read operations. Input is separate from stage custody so a
/// charged complete command can stay borrowed while opaque stage writes run.
/// An offline complete-input backend implements only this capability; that
/// provides no checkpoint staging, candidate or publication operation.
#[doc(hidden)]
pub trait SourceReplayInputBackend: SourceCheckpointBackend {
    /// Actual admitted original cursor/pin context, with constructor private to
    /// its concrete native engine owner. No caller-supplied values or EOF.
    type Input;
    /// Rewinds the same genuine pinned input; no new current root is captured.
    fn rewind<'session>(
        input: &mut Self::Input,
        actual: &Self::Brand,
        request: SourceRewindInstruction<'session, Self>,
    ) -> Result<SourceRewindEvidence<'session, Self>, SourceDriverError>;
    /// Supplies only the requested exact complete command or actual exact end.
    fn read_command<'command, 'session>(
        input: &'command mut Self::Input,
        actual: &Self::Brand,
        request: SourceReadInstruction<'session, Self>,
    ) -> Result<SourceCommandResponse<'command, 'session, Self>, SourceDriverError>;
}

mod catalog_input;
pub use catalog_input::*;
