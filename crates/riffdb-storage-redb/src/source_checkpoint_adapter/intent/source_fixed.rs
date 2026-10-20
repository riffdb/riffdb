//! Preencoded history controls for the closed actual Source intent owner.
//! No begin_write, allocation grant, Source authority or successful outcome is
//! constructed here. The fixed native write owner must consume the full quote.
use super::super::maintenance::NativeSourceCheckpointRequest;
use super::NativeSourceStepIntent;
use crate::RedbOperationalPorts;
use riffdb_storage_api::{
    AuthoritativeMutationV3, ChangelogHistoryStateV3, StorageError, StorageErrorKind,
    proto_codec::{
        CanonicalStoredEnvelopeV1, encode_changelog_history_state_source_admitted_v3,
        encode_changelog_transaction_allocator_source_admitted_v3,
    },
};
use riffdb_types::{ProjectionSourceWorkAllocationV1, ProjectionSourceWorkReservationV1};

type ControlImage = ProjectionSourceWorkAllocationV1<CanonicalStoredEnvelopeV1>;

pub(crate) struct SourceFixedPhysicalRecipe<'work> {
    // All retained data precedes its fixed inline construction allowance.
    intent: NativeSourceStepIntent,
    before_history: ControlImage,
    before_allocator: ControlImage,
    after_history: ControlImage,
    after_allocator: ControlImage,
    successor: ChangelogHistoryStateV3,
    history_key: [u8; 8],
    actual: &'work RedbOperationalPorts,
    _control: ProjectionSourceWorkReservationV1,
}
impl<'work> SourceFixedPhysicalRecipe<'work> {
    pub(crate) fn prepare<'request, 'candidate, 'stage, 'slot, 'session>(
        request: &NativeSourceCheckpointRequest<
            'request,
            'candidate,
            'stage,
            'slot,
            'session,
            'work,
        >,
        intent: NativeSourceStepIntent,
    ) -> Result<Self, StorageError>
    where
        'work: 'session,
        'session: 'slot,
        'slot: 'stage,
        'stage: 'candidate,
    {
        let actual = request.actual_ports();
        let budget = actual.shared.database.source_working_budget();
        if !intent.belongs_to(budget)
            || intent.locator() != request.captured_locator()
            || intent.operation() != request.operation()
            || intent.predecessor_history().lineage() != request.captured_history().lineage()
            || intent.predecessor_history().tail().sequence()
                < request.captured_history().tail().sequence()
        {
            return Err(corrupt());
        }
        // All dynamic encoding and canonical receipt validation happens offgate.
        // The input intent was admitted by its sole builder and stays borrowed.
        let control = budget
            .reserve(core::mem::size_of::<Self>() as u64)
            .map_err(|_| unavailable())?;
        let predecessor = intent.predecessor_history();
        let (assigned, allocator) = predecessor
            .expected_allocator()
            .allocate_one()
            .map_err(crate::changelog_v3_write::value_error)?;
        if assigned != intent.receipt().binding().sequence {
            return Err(corrupt());
        }
        let successor = predecessor
            .advance_canonical_v1(intent.canonical_receipt())
            .map_err(crate::changelog_v3_write::value_error)?;
        let before_history = encode_changelog_history_state_source_admitted_v3(predecessor, budget)
            .map_err(source_encode_error)?;
        let before_allocator = encode_changelog_transaction_allocator_source_admitted_v3(
            predecessor.expected_allocator(),
            budget,
        )
        .map_err(source_encode_error)?;
        let after_history = encode_changelog_history_state_source_admitted_v3(successor, budget)
            .map_err(source_encode_error)?;
        let after_allocator =
            encode_changelog_transaction_allocator_source_admitted_v3(allocator, budget)
                .map_err(source_encode_error)?;
        Ok(Self {
            intent,
            before_history,
            before_allocator,
            after_history,
            after_allocator,
            successor,
            history_key: assigned.get().to_be_bytes(),
            actual,
            _control: control,
        })
    }
    pub(crate) fn authorize<'request, 'candidate, 'stage, 'slot, 'session>(
        self,
        request: &NativeSourceCheckpointRequest<
            'request,
            'candidate,
            'stage,
            'slot,
            'session,
            'work,
        >,
    ) -> Result<CommitAssignedSourceFixedRecipe<'work>, StorageError>
    where
        'work: 'session,
        'session: 'slot,
        'slot: 'stage,
        'stage: 'candidate,
    {
        // Only Commit's genuine consuming instruction carries an assignment.
        // Actual native prepared-step/epoch identity is checked separately by
        // the closed native apply owner before any mutation or allocation.
        if !std::ptr::eq(self.actual, request.actual_ports())
            || self.intent.operation() != request.operation()
            || self.intent.locator() != request.captured_locator()
            || request.transaction_binding_assignment() != Some(self.intent.receipt().binding())
        {
            return Err(unavailable());
        }
        Ok(CommitAssignedSourceFixedRecipe { recipe: self })
    }
    pub(crate) fn actual_ports(&self) -> &'work RedbOperationalPorts {
        self.actual
    }
    pub(crate) fn intent(&self) -> &NativeSourceStepIntent {
        &self.intent
    }
    pub(crate) fn predecessor(&self) -> ChangelogHistoryStateV3 {
        self.intent.predecessor_history()
    }
    pub(crate) fn successor(&self) -> ChangelogHistoryStateV3 {
        self.successor
    }
    pub(crate) fn mutations(&self) -> &[AuthoritativeMutationV3] {
        self.intent.receipt().mutations()
    }
    pub(crate) fn history_key(&self) -> &[u8; 8] {
        &self.history_key
    }
    pub(crate) fn receipt_bytes(&self) -> &[u8] {
        self.intent.canonical_receipt()
    }
    pub(crate) fn before_history(&self) -> &[u8] {
        self.before_history.as_bytes()
    }
    pub(crate) fn before_allocator(&self) -> &[u8] {
        self.before_allocator.as_bytes()
    }
    pub(crate) fn after_history(&self) -> &[u8] {
        self.after_history.as_bytes()
    }
    pub(crate) fn after_allocator(&self) -> &[u8] {
        self.after_allocator.as_bytes()
    }
    pub(crate) fn normal_operation_count(&self) -> Result<usize, StorageError> {
        self.mutations().len().checked_add(3).ok_or_else(limit)
    }
    // This indexed view borrows the immutable admitted intent/history images;
    // it allocates no vector and cannot authorize execution. The native owning
    // recipe copies these exact bytes only AFTER its own full preadmission.
    pub(crate) fn physical_point(
        &self,
        index: usize,
    ) -> Result<Option<crate::changelog_v3_write::FixedPhysicalPoint<'_>>, StorageError> {
        if let Some(mutation) = self.mutations().get(index) {
            return crate::changelog_v3_write::physical_mutation_view(mutation).map(Some);
        }
        let history_index = index
            .checked_sub(self.mutations().len())
            .ok_or_else(limit)?;
        if history_index >= 3 {
            return Ok(None);
        }
        let points = crate::changelog_v3_write::source_fixed_history_points(
            self.history_key(),
            self.receipt_bytes(),
            self.after_allocator(),
            self.after_history(),
        );
        points.point(history_index)
    }
}
pub(crate) struct CommitAssignedSourceFixedRecipe<'work> {
    recipe: SourceFixedPhysicalRecipe<'work>,
}
impl<'work> CommitAssignedSourceFixedRecipe<'work> {
    pub(crate) fn recipe(&self) -> &SourceFixedPhysicalRecipe<'work> {
        &self.recipe
    }

    // Only the future sealed complete native fixed-write owner may supply the
    // actual transaction, after native admission/root claim and Commit's exact
    // binding authorization. This method opens no writer and mints no proof.
    // Each sole existing mutation helper returns only (), so native returned
    // old-value guards physically drop before the next operation starts.
    pub(crate) fn stage_normal_and_history(
        &self,
        transaction: &redb::WriteTransaction,
    ) -> Result<(), StorageError> {
        for mutation in self.recipe.mutations() {
            crate::changelog_v3_write::apply_mutation(transaction, mutation)?;
        }
        crate::changelog_v3_write::PreparedHistoryAdvance::stage_source_fixed_history(
            transaction,
            self,
        )
    }
}
impl Drop for CommitAssignedSourceFixedRecipe<'_> {
    fn drop(&mut self) {}
}

// Preserve the actual ports borrow through physical destruction of all images.
impl Drop for SourceFixedPhysicalRecipe<'_> {
    fn drop(&mut self) {}
}
fn source_encode_error(
    error: riffdb_storage_api::proto_codec::ProjectionSourceRecordDecodeErrorV1,
) -> StorageError {
    use riffdb_storage_api::proto_codec::ProjectionSourceRecordDecodeErrorV1 as E;
    match error {
        E::Codec(error) => crate::error::codec_error(error),
        E::AdmissionUnavailable => unavailable(),
    }
}
fn corrupt() -> StorageError {
    crate::error::storage_error(StorageErrorKind::CorruptData)
}
fn unavailable() -> StorageError {
    crate::error::storage_error(StorageErrorKind::Unavailable)
}
fn limit() -> StorageError {
    crate::error::storage_error(StorageErrorKind::LimitExceeded)
}
