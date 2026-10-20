//! The actual classified-Empty transaction is the sole Fresh usage producer.
//! Unknown outcomes preserve a prepared before/after pair, never an empty proof.
use super::*;
use crate::{
    engine_owner::EngineOwner,
    error::{precommit_storage_error, table_error},
    layout::META,
    store::ProvenEmptyInitializationV1,
};
use redb::{Database, ReadableTable, TableDefinition};
use riffdb_storage_api::{
    AuthoritativeNamespaceV3 as N, ProjectionCheckpointOrdinalAllocatorV1,
    proto_codec::decode_projection_checkpoint_ordinal_allocator_inline_v1,
};
use riffdb_types::ProjectionSourceWorkReservationV1;

pub(crate) struct PreparedFreshSourceUsage<'engine> {
    mutation: Option<SourceUsageMutation<'engine>>,
    owner: &'engine EngineOwner<Database>,
    // Last: actual mutation custody and its engine borrow die before admission.
    _control: ProjectionSourceWorkReservationV1,
}

impl PreparedFreshSourceUsage<'_> {
    /// Sole caller: StagedEmptyV3's consuming definite-commit installation.
    /// Neither an Err nor dropping this owner calls this method.
    pub(crate) fn install_after_durable_commit(mut self) -> Result<(), StorageError> {
        let mutation = self.mutation.take().ok_or_else(integrity)?;
        if !std::ptr::eq(mutation.domain, self.owner.source_usage_domain()) {
            return Err(integrity());
        }
        mutation.resolve_exact(VerifiedPhysicalOutcome::ExactAfter)
    }
}
impl Drop for PreparedFreshSourceUsage<'_> {
    fn drop(&mut self) {
        // Explicit physical engine-borrow custody even for otherwise-unused values.
        // Mutation Drop keeps unknown quota and refuses future availability.
    }
}

/// Called before applying any Source/allocator post-image in the actual Empty
/// transaction. No caller-selected domain, encoded key or byte count is accepted.
pub(crate) fn prepare_fresh_source_usage<'engine>(
    empty: &ProvenEmptyInitializationV1<'engine, '_>,
    actual_allocator_bytes: &[u8],
) -> Result<PreparedFreshSourceUsage<'engine>, StorageError> {
    if actual_allocator_bytes.len() > riffdb_storage_api::MAX_PROJECTION_SOURCE_CONTROL_BYTES {
        return Err(limit());
    }
    let allocator =
        decode_projection_checkpoint_ordinal_allocator_inline_v1(actual_allocator_bytes)
            .map_err(crate::error::codec_error)?;
    if *allocator.value()
        != ProjectionCheckpointOrdinalAllocatorV1::initial(1).map_err(|_| integrity())?
    {
        return Err(integrity());
    }
    let owner = empty.owner();
    let domain = owner.source_usage_domain();
    let control_bytes =
        u64::try_from(std::mem::size_of::<PreparedFreshSourceUsage<'_>>()).map_err(|_| limit())?;
    let control = owner
        .source_working_budget()
        .reserve(control_bytes)
        .map_err(|_| unavailable())?;
    for namespace in [
        N::ProjectionSourceRows,
        N::ProjectionSourceObjects,
        N::ProjectionSourceRoots,
        N::ProjectionSourceMaintenance,
    ] {
        let definition: TableDefinition<&[u8], &[u8]> = TableDefinition::new(namespace.table());
        let table = empty
            .transaction()
            .open_table(definition)
            .map_err(table_error)?;
        // One bounded first-entry check, never a full table population count.
        if table
            .iter()
            .map_err(precommit_storage_error)?
            .next()
            .transpose()
            .map_err(precommit_storage_error)?
            .is_some()
        {
            return Err(integrity());
        }
    }
    let allocator_key = N::ProjectionSourceAllocator
        .metadata_key()
        .ok_or_else(integrity)?;
    let metadata = empty.transaction().open_table(META).map_err(table_error)?;
    if metadata
        .get(allocator_key)
        .map_err(precommit_storage_error)?
        .is_some()
    {
        return Err(integrity());
    }
    drop(metadata);
    let after_bytes = allocator_key
        .len()
        .checked_add(actual_allocator_bytes.len())
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or_else(limit)?;
    let after = Usage::new(0, 0, after_bytes, None).map_err(|_| limit())?;
    let mut state = domain.state.lock().map_err(|_| unavailable())?;
    if state.input_obligation.is_some()
        || state.complete_candidate.is_some()
        || state.complete.is_some()
        || state.publication.is_some()
        || state.pending.is_some_and(|pending| {
            pending.in_flight || pending.kind != PendingKind::FreshInitialization
        })
    {
        return Err(unavailable());
    }
    // A retry may clear only a released uncertain FRESH transition, and only
    // after the new actual same-owner Empty proof above established exact-before
    // Source/allocator physical absence. No DTO/readonly epoch can do this.
    let version = state.version.checked_add(1).ok_or_else(limit)?;
    state.version = version;
    state.pending = Some(Pending {
        kind: PendingKind::FreshInitialization,
        in_flight: true,
        mutation_version: version,
        before: Usage::empty(),
        after,
        before_candidate: None,
        after_candidate: None,
    });
    // Completeness stays unavailable until the real original commit succeeds.
    state.complete = None;
    drop(state);
    Ok(PreparedFreshSourceUsage {
        mutation: Some(SourceUsageMutation {
            domain,
            version,
            _native_control: None,
        }),
        owner,
        _control: control,
    })
}
