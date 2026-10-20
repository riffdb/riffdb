//! Consuming exact-command instructions; raw input never constitutes completion.
use super::{SourceCheckpointBackend, SourceDriverError, seal::SourceBrand};
use riffdb_storage_api::StoredCommitRecordV1;
use riffdb_types::CommitSequence;

/// One projection-minted read of the exact next original command or exact end.
pub struct SourceReadInstruction<'session, B: SourceCheckpointBackend> {
    brand: SourceBrand<'session, B>,
    expected: Option<CommitSequence>,
    frontier: Option<CommitSequence>,
}
impl<'session, B: SourceCheckpointBackend> SourceReadInstruction<'session, B> {
    pub(super) fn new(
        brand: SourceBrand<'session, B>,
        expected: Option<CommitSequence>,
        frontier: Option<CommitSequence>,
    ) -> Self {
        Self {
            brand,
            expected,
            frontier,
        }
    }
    /// Checks exact private instance before reading or allocating a command.
    pub fn check_before_read(&self, actual: &B::Brand) -> Result<(), SourceDriverError> {
        self.brand.check(actual)
    }
    /// Read-only sequence expectation, never caller-selected source authority.
    #[must_use]
    pub const fn expected_sequence(&self) -> Option<CommitSequence> {
        self.expected
    }
    /// The one captured application frontier, unchanged across both passes.
    #[must_use]
    pub const fn captured_through(&self) -> Option<CommitSequence> {
        self.frontier
    }
    /// Attaches the actual borrowed admitted command to this consumed request.
    /// The private native brand reference cannot be obtained by application code.
    pub fn command<'command>(
        self,
        actual: &B::Brand,
        command: &'command StoredCommitRecordV1,
    ) -> Result<SourceCommandResponse<'command, 'session, B>, SourceDriverError> {
        self.brand.check(actual)?;
        if self.expected != Some(command.commit_sequence())
            || self
                .frontier
                .is_none_or(|frontier| command.commit_sequence() > frontier)
        {
            return Err(SourceDriverError::InputChanged);
        }
        Ok(SourceCommandResponse::Command(SourceCommandEvidence {
            command,
            brand: self.brand,
        }))
    }
    /// Attaches exact physical end only at the complete requested frontier.
    pub fn exact_end<'command>(
        self,
        actual: &B::Brand,
    ) -> Result<SourceCommandResponse<'command, 'session, B>, SourceDriverError> {
        self.brand.check(actual)?;
        if self
            .expected
            .is_some_and(|sequence| self.frontier.is_some_and(|frontier| sequence <= frontier))
        {
            return Err(SourceDriverError::IncompleteInput);
        }
        Ok(SourceCommandResponse::ExactEnd(SourceExactEndEvidence {
            brand: self.brand,
        }))
    }
}

/// Closed consuming response. Its payload constructors and brand stay private.
pub enum SourceCommandResponse<'command, 'session, B: SourceCheckpointBackend> {
    /// The exact next complete, admitted authoritative command.
    Command(SourceCommandEvidence<'command, 'session, B>),
    /// Actual exact end of this one pinned prefix.
    ExactEnd(SourceExactEndEvidence<'session, B>),
}
/// A borrowed full command cannot outlive its actual admitted native graph.
pub struct SourceCommandEvidence<'command, 'session, B: SourceCheckpointBackend> {
    command: &'command StoredCommitRecordV1,
    brand: SourceBrand<'session, B>,
}
impl<B: SourceCheckpointBackend> SourceCommandEvidence<'_, '_, B> {
    /// Complete existing semantic command, borrowed until physical evidence drop.
    #[must_use]
    pub fn command(&self) -> &StoredCommitRecordV1 {
        self.command
    }
    pub(super) fn check(&self, expected: &SourceBrand<'_, B>) -> Result<(), SourceDriverError> {
        // The composition owner receives no brand getter; the driver alone can
        // compare the response with its own originating session.
        expected.check_same(&self.brand)
    }
}
impl<B: SourceCheckpointBackend> Drop for SourceCommandEvidence<'_, '_, B> {
    fn drop(&mut self) {}
}
/// Non-reusable exact-end evidence for the consumed original read request.
pub struct SourceExactEndEvidence<'session, B: SourceCheckpointBackend> {
    brand: SourceBrand<'session, B>,
}
impl<B: SourceCheckpointBackend> SourceExactEndEvidence<'_, B> {
    pub(super) fn check(&self, expected: &SourceBrand<'_, B>) -> Result<(), SourceDriverError> {
        expected.check_same(&self.brand)
    }
}
impl<B: SourceCheckpointBackend> Drop for SourceExactEndEvidence<'_, B> {
    fn drop(&mut self) {}
}

/// Projection-minted rewind of the exact retained original input for one pass.
pub struct SourceRewindInstruction<'session, B: SourceCheckpointBackend> {
    pub(super) brand: SourceBrand<'session, B>,
}
impl<'session, B: SourceCheckpointBackend> SourceRewindInstruction<'session, B> {
    /// Checks the actual native instance before destroying/reopening its cursor.
    pub fn check_before_rewind(&self, actual: &B::Brand) -> Result<(), SourceDriverError> {
        self.brand.check(actual)
    }
    /// Attaches actual fresh cursor custody while consuming this exact rewind.
    pub fn rewound(
        self,
        actual: &B::Brand,
    ) -> Result<SourceRewindEvidence<'session, B>, SourceDriverError> {
        self.brand.check(actual)?;
        Ok(SourceRewindEvidence { brand: self.brand })
    }
}
/// Non-detachable confirmation of the same pinned input rewind role.
pub struct SourceRewindEvidence<'session, B: SourceCheckpointBackend> {
    pub(super) brand: SourceBrand<'session, B>,
}
impl<B: SourceCheckpointBackend> Drop for SourceRewindEvidence<'_, B> {
    fn drop(&mut self) {}
}
