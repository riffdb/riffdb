//! Closed readonly allocation observation. Nothing here assigns an ordinal.
use super::maintenance::NativeSourceCheckpointRequest;
use super::*;
use crate::{
    error::{precommit_storage_error, table_error},
    layout::{CATALOG_ACTIVE, CATALOG_ACTIVE_KEY, META},
    source_usage_domain::SourceUsageEpoch,
    store::{RedbReadAccess, SourceReadWork},
};
use redb::{ReadableTableMetadata, TableDefinition};
use riffdb_projection::source_driver::{
    SourceCheckpointOperationKind, SourceNativePreparedAllocationEvidence,
};
use riffdb_storage_api::proto_codec::{
    ProjectionSourceActiveCatalogDecodeAdmissionV1, ProjectionSourceRecordDecodeErrorV1,
    decode_projection_checkpoint_ordinal_allocator_inline_v1,
    decode_projection_source_object_admitted_v1, decode_projection_source_root_admitted_v1,
};
use riffdb_storage_api::{
    AuthoritativeNamespaceV3 as N, BorrowedOverlayPointV1, ChangelogHistoryStateV3,
    CompositeTableV1, MAX_PROJECTION_SOURCE_CONTROL_BYTES, MAX_PROJECTION_SOURCE_ROOTS,
    ProjectionCheckpointOrdinalAllocatorV1, ProjectionSourceLifecycleV1, ProjectionSourceObjectV1,
    ProjectionSourceRootV1, StorageError, StorageErrorKind,
};
use riffdb_types::{
    MAX_KEY_BYTES, ProjectionSourceWorkAllocationV1, ProjectionSourceWorkReservationV1,
    owned_allocation_capacity_bound_v1,
};
use std::mem::size_of;

/// Actual-engine/source-epoch observation with newly admitted semantic owners.
/// All fields and constructors are private; these scalar views grant no apply.
#[doc(hidden)]
pub struct NativeSourceAllocationObservation<'work> {
    allocator: ProjectionCheckpointOrdinalAllocatorV1,
    prior_root: Option<ProjectionSourceWorkAllocationV1<ProjectionSourceRootV1>>,
    prior_object: Option<ProjectionSourceWorkAllocationV1<ProjectionSourceObjectV1>>,
    allocator_bytes: Vec<u8>,
    prior_root_bytes: Vec<u8>,
    prior_object_bytes: Vec<u8>,
    history: ChangelogHistoryStateV3,
    epoch: SourceUsageEpoch<'work>,
    // Last: all copied canonical bytes/inline controls physically die first.
    _controls: ProjectionSourceWorkReservationV1,
}
impl SourceNativePreparedAllocationEvidence for NativeSourceAllocationObservation<'_> {
    fn allocator(&self) -> ProjectionCheckpointOrdinalAllocatorV1 {
        self.allocator
    }
    fn prior_root(&self) -> Option<&ProjectionSourceRootV1> {
        self.prior_root.as_deref()
    }
}
impl<'work> NativeSourceAllocationObservation<'work> {
    pub(super) fn source_epoch(&self) -> &SourceUsageEpoch<'work> {
        &self.epoch
    }
    pub(super) fn observed_history(&self) -> ChangelogHistoryStateV3 {
        self.history
    }
    pub(super) fn prior_object(&self) -> Option<&ProjectionSourceObjectV1> {
        self.prior_object.as_deref()
    }
    pub(super) fn allocator_bytes(&self) -> &[u8] {
        &self.allocator_bytes
    }
    pub(super) fn prior_root_bytes(&self) -> Option<&[u8]> {
        self.prior_root
            .as_ref()
            .map(|_| self.prior_root_bytes.as_slice())
    }
    pub(super) fn prior_object_bytes(&self) -> Option<&[u8]> {
        self.prior_object
            .as_ref()
            .map(|_| self.prior_object_bytes.as_slice())
    }
}
impl Drop for NativeSourceAllocationObservation<'_> {
    fn drop(&mut self) {}
}

struct AllocationRead<'work> {
    access: Option<RedbReadAccess>,
    actual: &'work RedbOperationalPorts,
    _scratch: SourceReadWork,
}
impl Drop for AllocationRead<'_> {
    fn drop(&mut self) {
        drop(self.access.take());
    }
}
impl<'work> AllocationRead<'work> {
    fn capture(actual: &'work RedbOperationalPorts) -> Result<Self, StorageError> {
        if !riffdb_proto::envelope::release_locator_decode_scratch_v1() {
            return Err(unavailable());
        }
        let scratch =
            SourceReadWork::admit(actual.shared.database.source_working_budget(), 512 * 1024)?;
        Ok(Self {
            access: Some(actual.begin_composite_read()?),
            actual,
            _scratch: scratch,
        })
    }
    fn access(&self) -> Result<&RedbReadAccess, StorageError> {
        self.access.as_ref().ok_or_else(integrity)
    }
    fn checkpoint(&self) -> Result<&redb::ReadTransaction, StorageError> {
        Ok(match self.access()? {
            RedbReadAccess::Current(root) | RedbReadAccess::Durable(root) => root,
            RedbReadAccess::Composite(view) => view.checkpoint_root(),
        })
    }
    fn checked_origin(
        &self,
    ) -> Result<riffdb_storage_api::NativeCatalogOriginReferenceV1, StorageError> {
        crate::native_source_fresh::validate_same_owner_cold_origin(
            &self.actual.shared.database,
            self.access()?,
        )?;
        crate::native_source_fresh::checked_captured_source_origin_v1(
            &self.actual.shared.database,
            self.checkpoint()?,
        )
    }
    fn budget(&self) -> &riffdb_types::ProjectionSourceWorkingBudgetV1 {
        self.actual.shared.database.source_working_budget()
    }
    fn allocator(
        &self,
        output: &mut Vec<u8>,
    ) -> Result<ProjectionCheckpointOrdinalAllocatorV1, StorageError> {
        let key = N::ProjectionSourceAllocator
            .metadata_key()
            .ok_or_else(integrity)?;
        if let RedbReadAccess::Composite(view) = self.access()? {
            match view
                .overlay()
                .borrowed_provider_point_v1(CompositeTableV1::Meta, key.as_bytes())
            {
                BorrowedOverlayPointV1::Tombstone => return Err(integrity()),
                BorrowedOverlayPointV1::Value(bytes) => return decode_allocator(bytes, output),
                BorrowedOverlayPointV1::Checkpoint => {}
            }
        }
        let meta = self.checkpoint()?.open_table(META).map_err(table_error)?;
        let row = meta
            .get(key)
            .map_err(precommit_storage_error)?
            .ok_or_else(integrity)?;
        decode_allocator(row.value(), output)
    }
}

/// Called only after the fixed Native port has minted this same-engine request.
/// Reads bounded controls off-gate and drops the actual access before returning.
pub(super) fn observe<'request, 'candidate, 'stage, 'slot, 'session, 'work>(
    request: &NativeSourceCheckpointRequest<'request, 'candidate, 'stage, 'slot, 'session, 'work>,
) -> Result<NativeSourceAllocationObservation<'work>, SourceDriverError>
where
    'work: 'session,
    'session: 'slot,
    'slot: 'stage,
    'stage: 'candidate,
{
    if request.operation() != SourceCheckpointOperationKind::PrepareAllocation
        || request.descriptor().is_some()
        || request.ordinal_assignment().is_some()
    {
        return Err(SourceDriverError::InputChanged);
    }
    let actual = request.actual_ports();
    let budget = actual.shared.database.source_working_budget();
    let epoch = actual
        .shared
        .database
        .source_usage_domain()
        .capture_epoch()?;
    epoch.verify_for_native_allocation_observation(actual)?;
    // Admit every exact original envelope buffer and lookup prefix BEFORE Vec.
    let buffer = owned_allocation_capacity_bound_v1(MAX_PROJECTION_SOURCE_CONTROL_BYTES, 1)
        .map_err(|_| SourceDriverError::SourceUnavailable)?;
    let prefix_work = owned_allocation_capacity_bound_v1(MAX_KEY_BYTES, 1)
        .map_err(|_| SourceDriverError::SourceUnavailable)?;
    let controls = buffer
        .checked_mul(3)
        .and_then(|bytes| bytes.checked_add(prefix_work))
        .and_then(|bytes| {
            bytes.checked_add(size_of::<NativeSourceAllocationObservation<'work>>() as u64)
        })
        .ok_or(SourceDriverError::SourceUnavailable)?;
    let controls = budget
        .reserve(controls)
        .map_err(|_| SourceDriverError::SourceUnavailable)?;
    let mut allocator_bytes = Vec::with_capacity(MAX_PROJECTION_SOURCE_CONTROL_BYTES);
    let mut prior_root_bytes = Vec::with_capacity(MAX_PROJECTION_SOURCE_CONTROL_BYTES);
    let mut prior_object_bytes = Vec::with_capacity(MAX_PROJECTION_SOURCE_CONTROL_BYTES);
    let mut prefix = Vec::with_capacity(MAX_KEY_BYTES);
    let pointer_admission =
        ProjectionSourceActiveCatalogDecodeAdmissionV1::new(budget).map_err(record_error)?;
    let read = AllocationRead::capture(actual)?;
    let history = crate::changelog_v3_cursor::capture_history_position_v1(read.access()?)?;
    if history.lineage() != request.captured_history().lineage()
        || history.tail().frontier().application()
            < request.captured_history().tail().frontier().application()
    {
        return Err(SourceDriverError::SourceUnavailable);
    }
    // Source eligibility/root authority is checked on THIS access and owner.
    let origin = read.checked_origin()?;
    if Some(origin) != request.captured_native_origin() {
        return Err(SourceDriverError::InputChanged);
    }
    {
        let table = read
            .checkpoint()?
            .open_table(CATALOG_ACTIVE)
            .map_err(table_error)?;
        let row = table
            .get(CATALOG_ACTIVE_KEY.as_slice())
            .map_err(precommit_storage_error)?
            .ok_or_else(integrity)?;
        let pointer = pointer_admission
            .decode(row.value())
            .map_err(record_error)?
            .into_parts()
            .0;
        if &*pointer != request.captured_catalog_pointer() {
            return Err(SourceDriverError::InputChanged);
        }
    }
    let allocator = read.allocator(&mut allocator_bytes)?;
    let locator = request.captured_locator();
    if allocator.allocation_history_incarnation() != history.lineage().history_incarnation()
        || allocator
            .allocate_one(
                locator.database_id(),
                locator.allocation_history_incarnation(),
            )
            .map_err(|_| SourceDriverError::SourceUnavailable)?
            .locator()
            != locator
    {
        return Err(SourceDriverError::SourceUnavailable);
    }
    crate::source_keys::encode_source_root_identity_prefix_into(
        &mut prefix,
        request.projection_identity(),
    )
    .map_err(|_| SourceDriverError::InputChanged)?;
    let roots: TableDefinition<&[u8], &[u8]> =
        TableDefinition::new(N::ProjectionSourceRoots.table());
    let objects: TableDefinition<&[u8], &[u8]> =
        TableDefinition::new(N::ProjectionSourceObjects.table());
    let (prior_root, prior_object) = {
        let table = read.checkpoint()?.open_table(roots).map_err(table_error)?;
        if table.len().map_err(precommit_storage_error)? > u64::from(MAX_PROJECTION_SOURCE_ROOTS) {
            return Err(SourceDriverError::InputChanged);
        }
        let mut range = table
            .range::<&[u8]>(prefix.as_slice()..)
            .map_err(precommit_storage_error)?;
        let first = range.next().transpose().map_err(precommit_storage_error)?;
        match first {
            None => (None, None),
            Some((key, _value)) if !key.value().starts_with(&prefix) => (None, None),
            Some((key, value)) => {
                copy_control(&mut prior_root_bytes, value.value())?;
                let root = decode_projection_source_root_admitted_v1(value.value(), read.budget())
                    .map_err(record_error)?
                    .into_parts()
                    .0;
                if root.reference().identity() != request.projection_identity() {
                    return Err(SourceDriverError::InputChanged);
                }
                crate::source_keys::check_source_root_key(
                    key.value(),
                    request.projection_identity(),
                    root.reference().locator(),
                )
                .map_err(|_| SourceDriverError::InputChanged)?;
                if let Some((next_key, _)) =
                    range.next().transpose().map_err(precommit_storage_error)?
                    && next_key.value().starts_with(&prefix)
                {
                    return Err(SourceDriverError::InputChanged);
                }
                let object_table = read
                    .checkpoint()?
                    .open_table(objects)
                    .map_err(table_error)?;
                let object_key = crate::source_keys::source_object_key(root.reference().locator());
                let object_value = object_table
                    .get(object_key.as_slice())
                    .map_err(precommit_storage_error)?
                    .ok_or_else(integrity)?;
                copy_control(&mut prior_object_bytes, object_value.value())?;
                let object = decode_projection_source_object_admitted_v1(
                    object_value.value(),
                    read.budget(),
                )
                .map_err(record_error)?
                .into_parts()
                .0;
                let descriptor = object.descriptor();
                let prior_locator = descriptor.reference().locator();
                if object.lifecycle() != ProjectionSourceLifecycleV1::Published
                    || !root.matches_published_object(&object)
                    || descriptor.reference().identity() != request.projection_identity()
                    || prior_locator.database_id() != locator.database_id()
                    || descriptor.applied_through()
                        > request
                            .captured_history()
                            .tail()
                            .frontier()
                            .application()
                            .ok_or(SourceDriverError::IncompleteInput)?
                {
                    return Err(SourceDriverError::InputChanged);
                }
                if prior_locator.allocation_history_incarnation()
                    != locator.allocation_history_incarnation()
                    || descriptor.captured_source().lineage() != history.lineage()
                {
                    return Err(SourceDriverError::SourceUnavailable);
                }
                if prior_locator.ordinal() >= locator.ordinal() {
                    return Err(SourceDriverError::InputChanged);
                }
                let publication = root.publication_binding();
                let observed = history.tail();
                if publication.sequence > observed.sequence()
                    || publication.covered_frontier.application()
                        > observed.frontier().application()
                    || publication.covered_frontier.administration()
                        > observed.frontier().administration()
                {
                    return Err(SourceDriverError::InputChanged);
                }
                // This first live producer supports the genuine Fresh origin.
                // Inherited/cut checkpoints require their actual catalog-owner
                // ancestry resolver; no old locator is silently reinterpreted.
                let expected_origin = riffdb_storage_api::projection_source_origin_commitment_v1(
                    prior_locator.database_id(),
                    prior_locator.allocation_history_incarnation(),
                    origin,
                )
                .map_err(|_| SourceDriverError::InputChanged)?;
                if descriptor.reference().origin() != expected_origin {
                    return Err(SourceDriverError::InputChanged);
                }
                (Some(root), Some(object))
            }
        }
    };
    epoch.verify_for_native_allocation_observation(actual)?;
    drop(read);
    drop(prefix);
    Ok(NativeSourceAllocationObservation {
        allocator,
        prior_root,
        prior_object,
        allocator_bytes,
        prior_root_bytes,
        prior_object_bytes,
        history,
        epoch,
        _controls: controls,
    })
}

fn decode_allocator(
    bytes: &[u8],
    output: &mut Vec<u8>,
) -> Result<ProjectionCheckpointOrdinalAllocatorV1, StorageError> {
    copy_control(output, bytes)?;
    decode_projection_checkpoint_ordinal_allocator_inline_v1(bytes)
        .map(|value| *value.value())
        .map_err(crate::error::codec_error)
}
fn copy_control(output: &mut Vec<u8>, bytes: &[u8]) -> Result<(), StorageError> {
    if bytes.is_empty()
        || bytes.len() > MAX_PROJECTION_SOURCE_CONTROL_BYTES
        || output.capacity() < bytes.len()
    {
        return Err(integrity());
    }
    output.clear();
    output.extend_from_slice(bytes);
    Ok(())
}
fn record_error(error: ProjectionSourceRecordDecodeErrorV1) -> SourceDriverError {
    match error {
        ProjectionSourceRecordDecodeErrorV1::AdmissionUnavailable => {
            SourceDriverError::SourceUnavailable
        }
        ProjectionSourceRecordDecodeErrorV1::Codec(_) => SourceDriverError::Storage(integrity()),
    }
}
fn unavailable() -> StorageError {
    crate::error::storage_error(StorageErrorKind::Unavailable)
}
fn integrity() -> StorageError {
    crate::error::storage_error(StorageErrorKind::CorruptData)
}

mod current;
pub(super) use current::CurrentSourceStepRead;
