//! Same-actual-owner bounded off-gate Source control observation.
use super::*;
use crate::store::OriginalSourceReadContext;
use std::marker::PhantomData;

/// Owns only newly admitted original canonical bytes. This is neither semantic
/// reconstruction nor permission to apply any Source mutation.
pub(in crate::source_checkpoint_adapter) struct NativeSourceControlImage {
    bytes: Vec<u8>,
    _work: ProjectionSourceWorkReservationV1,
}
impl NativeSourceControlImage {
    pub(in crate::source_checkpoint_adapter) fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
}

/// The read never escapes this closed native helper. Its request borrow remains
/// live even when the helper is unused, until the access and scratch die.
pub(in crate::source_checkpoint_adapter) struct CurrentSourceStepRead<'request, 'work> {
    access: Option<RedbReadAccess>,
    actual: &'work RedbOperationalPorts,
    history: ChangelogHistoryStateV3,
    epoch: SourceUsageEpoch<'work>,
    locator: riffdb_types::ProjectionSourceLocatorV1,
    identity: &'request riffdb_types::ProjectionIdentity,
    operation: SourceCheckpointOperationKind,
    context: OriginalSourceReadContext<'work>,
    _request: PhantomData<&'request ()>,
    _control_work: ProjectionSourceWorkReservationV1,
}
impl Drop for CurrentSourceStepRead<'_, '_> {
    fn drop(&mut self) {
        drop(self.access.take());
    }
}
impl<'request, 'work> CurrentSourceStepRead<'request, 'work> {
    pub(in crate::source_checkpoint_adapter) fn capture<'candidate, 'stage, 'slot, 'session>(
        request: &'request NativeSourceCheckpointRequest<
            '_,
            'candidate,
            'stage,
            'slot,
            'session,
            'work,
        >,
    ) -> Result<Self, StorageError>
    where
        'work: 'session,
        'session: 'slot,
        'slot: 'stage,
        'stage: 'candidate,
    {
        let operation = request.operation();
        if !matches!(
            operation,
            SourceCheckpointOperationKind::Allocate
                | SourceCheckpointOperationKind::Copy
                | SourceCheckpointOperationKind::Publish
        ) || request.descriptor().is_none()
        {
            return Err(integrity());
        }
        // A transaction proposal is prepared before Commit grants its binding;
        // no assignment is required or manufactured by this readonly owner.
        let actual = request.actual_ports();
        let locator = request.captured_locator();
        let epoch = actual
            .shared
            .database
            .source_usage_domain()
            .capture_epoch()?;
        verify_epoch(&epoch, actual, locator, operation)?;
        let control_work = actual
            .shared
            .database
            .source_working_budget()
            .reserve(size_of::<Self>() as u64)
            .map_err(|_| unavailable())?;
        let context = OriginalSourceReadContext::admit(actual)?;
        let pointer_admission = ProjectionSourceActiveCatalogDecodeAdmissionV1::new(
            context.budget(),
        )
        .map_err(|error| match record_error(error) {
            SourceDriverError::Storage(error) => error,
            _ => unavailable(),
        })?;
        let access = actual.begin_composite_read()?;
        // Complete canonical terminal/extent checks use the existing admitted
        // Source owner. The cheap capture-position hint is not used as proof.
        let history = crate::changelog_v3_cursor::history_source_admitted_v1(&access, &context)?;
        if history.lineage() != request.captured_history().lineage()
            || history.tail().sequence() < request.captured_history().tail().sequence()
            || history.tail().frontier().application()
                < request.captured_history().tail().frontier().application()
            || history.tail().frontier().administration()
                < request
                    .captured_history()
                    .tail()
                    .frontier()
                    .administration()
        {
            return Err(integrity());
        }
        crate::native_source_fresh::validate_same_owner_cold_origin(
            &actual.shared.database,
            &access,
        )?;
        let root = checkpoint(&access);
        let origin = crate::native_source_fresh::checked_captured_source_origin_v1(
            &actual.shared.database,
            root,
        )?;
        if Some(origin) != request.captured_native_origin() {
            return Err(integrity());
        }
        {
            let table = root.open_table(CATALOG_ACTIVE).map_err(table_error)?;
            let row = table
                .get(CATALOG_ACTIVE_KEY.as_slice())
                .map_err(precommit_storage_error)?
                .ok_or_else(integrity)?;
            let pointer = pointer_admission
                .decode(row.value())
                .map_err(|error| match record_error(error) {
                    SourceDriverError::Storage(error) => error,
                    _ => unavailable(),
                })?
                .into_parts()
                .0;
            if &*pointer != request.captured_catalog_pointer() {
                return Err(integrity());
            }
        }
        verify_epoch(&epoch, actual, locator, operation)?;
        Ok(Self {
            access: Some(access),
            actual,
            history,
            epoch,
            locator,
            identity: request.projection_identity(),
            operation,
            context,
            _request: PhantomData,
            _control_work: control_work,
        })
    }
    pub(in crate::source_checkpoint_adapter) fn history(&self) -> ChangelogHistoryStateV3 {
        self.history
    }
    /// Recheck the original selected Root/PublishedObject canonical preimages.
    /// A fresh Source epoch does not imply equality to an old allocation view.
    pub(in crate::source_checkpoint_adapter) fn validate_expected_selection(
        &self,
        observation: &NativeSourceAllocationObservation<'_>,
    ) -> Result<(), StorageError> {
        let work = owned_allocation_capacity_bound_v1(MAX_KEY_BYTES, std::mem::align_of::<u8>())
            .map_err(|_| unavailable())?;
        let _work = self
            .context
            .budget()
            .reserve(work)
            .map_err(|_| unavailable())?;
        let mut prefix = Vec::with_capacity(MAX_KEY_BYTES);
        crate::source_keys::encode_source_root_identity_prefix_into(&mut prefix, self.identity)
            .map_err(|_| integrity())?;
        let access = self.access.as_ref().ok_or_else(integrity)?;
        let definition: TableDefinition<&[u8], &[u8]> =
            TableDefinition::new(N::ProjectionSourceRoots.table());
        let table = checkpoint(access)
            .open_table(definition)
            .map_err(table_error)?;
        if table.len().map_err(precommit_storage_error)? > u64::from(MAX_PROJECTION_SOURCE_ROOTS) {
            return Err(integrity());
        }
        let mut range = table
            .range::<&[u8]>(prefix.as_slice()..)
            .map_err(precommit_storage_error)?;
        let first = range.next().transpose().map_err(precommit_storage_error)?;
        let selected = first
            .as_ref()
            .filter(|(key, _)| key.value().starts_with(&prefix));
        match (selected, observation.prior_root()) {
            (None, None) => {
                if observation.prior_root_bytes().is_some()
                    || observation.prior_object_bytes().is_some()
                    || observation.prior_object().is_some()
                {
                    return Err(integrity());
                }
            }
            (Some((key, value)), Some(root)) => {
                if root.reference().identity() != self.identity
                    || value.value().is_empty()
                    || value.value().len() > MAX_PROJECTION_SOURCE_CONTROL_BYTES
                    || Some(value.value()) != observation.prior_root_bytes()
                {
                    return Err(unavailable());
                }
                crate::source_keys::check_source_root_key(
                    key.value(),
                    self.identity,
                    root.reference().locator(),
                )
                .map_err(|_| integrity())?;
                if let Some((next, _)) =
                    range.next().transpose().map_err(precommit_storage_error)?
                    && next.value().starts_with(&prefix)
                {
                    return Err(integrity());
                }
                let object_key = crate::source_keys::source_object_key(root.reference().locator());
                let current = self
                    .point(N::ProjectionSourceObjects, &object_key)?
                    .ok_or_else(integrity)?;
                if Some(current.as_bytes()) != observation.prior_object_bytes() {
                    return Err(unavailable());
                }
            }
            _ => return Err(unavailable()),
        }
        Ok(())
    }
    pub(in crate::source_checkpoint_adapter) fn point(
        &self,
        namespace: N,
        key: &[u8],
    ) -> Result<Option<NativeSourceControlImage>, StorageError> {
        if key.is_empty() || key.len() > MAX_KEY_BYTES {
            return Err(integrity());
        }
        let access = self.access.as_ref().ok_or_else(integrity)?;
        match namespace {
            N::ProjectionSourceAllocator => {
                let expected = namespace.metadata_key().ok_or_else(integrity)?;
                if key != expected.as_bytes() {
                    return Err(integrity());
                }
                if let RedbReadAccess::Composite(view) = access {
                    match view
                        .overlay()
                        .borrowed_provider_point_v1(CompositeTableV1::Meta, key)
                    {
                        BorrowedOverlayPointV1::Tombstone => return Ok(None),
                        BorrowedOverlayPointV1::Value(bytes) => {
                            return self.copy_image(bytes).map(Some);
                        }
                        BorrowedOverlayPointV1::Checkpoint => {}
                    }
                }
                let table = checkpoint(access).open_table(META).map_err(table_error)?;
                let row = table.get(expected).map_err(precommit_storage_error)?;
                row.map(|row| self.copy_image(row.value())).transpose()
            }
            N::ProjectionSourceObjects
            | N::ProjectionSourceRoots
            | N::ProjectionSourceMaintenance => {
                let definition: TableDefinition<&[u8], &[u8]> =
                    TableDefinition::new(namespace.table());
                let table = checkpoint(access)
                    .open_table(definition)
                    .map_err(table_error)?;
                let row = table.get(key).map_err(precommit_storage_error)?;
                row.map(|row| self.copy_image(row.value())).transpose()
            }
            // Row absence/census belongs to the quota producer; this helper is
            // not an arbitrary table reader or a reduced row decoder.
            _ => Err(integrity()),
        }
    }
    /// One freshly owned bounded page of this request's actual Source rows.
    /// The primary range/transaction remain inside this short read owner.
    pub(in crate::source_checkpoint_adapter) fn source_rows_page(
        &self,
        after: Option<&[u8]>,
    ) -> Result<NativeSourceRowsPage, StorageError> {
        const MAX_ROWS: usize = riffdb_storage_api::MAX_PROJECTION_SOURCE_STEP_ROWS as usize;
        let prefix = crate::source_keys::source_row_prefix(self.locator);
        if after.is_some_and(|key| key.len() > MAX_KEY_BYTES || !key.starts_with(&prefix)) {
            return Err(integrity());
        }
        let work = owned_allocation_capacity_bound_v1(
            MAX_ROWS
                .checked_mul(size_of::<NativeSourceRowImage>())
                .ok_or_else(unavailable)?,
            std::mem::align_of::<NativeSourceRowImage>(),
        )
        .map_err(|_| unavailable())?
        .checked_add(size_of::<NativeSourceRowsPage>() as u64)
        .ok_or_else(unavailable)?;
        let work = self
            .context
            .budget()
            .reserve(work)
            .map_err(|_| unavailable())?;
        let mut rows = Vec::with_capacity(MAX_ROWS);
        let mut bytes = 0_u64;
        let mut complete = false;
        let definition: TableDefinition<&[u8], &[u8]> =
            TableDefinition::new(N::ProjectionSourceRows.table());
        let access = self.access.as_ref().ok_or_else(integrity)?;
        let table = checkpoint(access)
            .open_table(definition)
            .map_err(table_error)?;
        let mut range = match after {
            None => table.range::<&[u8]>(prefix.as_slice()..),
            Some(key) => {
                table.range::<&[u8]>((std::ops::Bound::Excluded(key), std::ops::Bound::Unbounded))
            }
        }
        .map_err(precommit_storage_error)?;
        while rows.len() < MAX_ROWS {
            let Some(next) = range.next() else {
                complete = true;
                break;
            };
            let (key, value) = next.map_err(precommit_storage_error)?;
            if !key.value().starts_with(&prefix) {
                complete = true;
                break;
            }
            if key.value().len() > MAX_KEY_BYTES || value.value().is_empty() {
                return Err(integrity());
            }
            let pair_bytes = key
                .value()
                .len()
                .checked_add(value.value().len())
                .and_then(|bytes| u64::try_from(bytes).ok())
                .ok_or_else(unavailable)?;
            if pair_bytes > riffdb_storage_api::MAX_PROJECTION_SOURCE_STEP_BYTES {
                return Err(integrity());
            }
            let total = bytes.checked_add(pair_bytes).ok_or_else(unavailable)?;
            if total > riffdb_storage_api::MAX_PROJECTION_SOURCE_STEP_BYTES {
                break;
            }
            let row_work =
                owned_allocation_capacity_bound_v1(key.value().len(), std::mem::align_of::<u8>())
                    .map_err(|_| unavailable())?
                    .checked_add(
                        owned_allocation_capacity_bound_v1(
                            value.value().len(),
                            std::mem::align_of::<u8>(),
                        )
                        .map_err(|_| unavailable())?,
                    )
                    .ok_or_else(unavailable)?;
            let row_work = self
                .context
                .budget()
                .reserve(row_work)
                .map_err(|_| unavailable())?;
            let mut owned_key = Vec::with_capacity(key.value().len());
            let mut owned_value = Vec::with_capacity(value.value().len());
            owned_key.extend_from_slice(key.value());
            owned_value.extend_from_slice(value.value());
            rows.push(NativeSourceRowImage {
                key: owned_key,
                value: owned_value,
                _work: row_work,
            });
            bytes = total;
        }
        Ok(NativeSourceRowsPage {
            rows,
            complete,
            _work: work,
        })
    }
    fn copy_image(&self, bytes: &[u8]) -> Result<NativeSourceControlImage, StorageError> {
        if bytes.is_empty() || bytes.len() > MAX_PROJECTION_SOURCE_CONTROL_BYTES {
            return Err(integrity());
        }
        let work = owned_allocation_capacity_bound_v1(bytes.len(), std::mem::align_of::<u8>())
            .map_err(|_| unavailable())?
            .checked_add(size_of::<NativeSourceControlImage>() as u64)
            .ok_or_else(unavailable)?;
        let work = self
            .context
            .budget()
            .reserve(work)
            .map_err(|_| unavailable())?;
        let mut owned = Vec::with_capacity(bytes.len());
        owned.extend_from_slice(bytes);
        Ok(NativeSourceControlImage {
            bytes: owned,
            _work: work,
        })
    }
    pub(in crate::source_checkpoint_adapter) fn finish(self) -> Result<(), StorageError> {
        verify_epoch(&self.epoch, self.actual, self.locator, self.operation)
        // Explicit Drop destroys actual read before Source scratch/reservations.
    }
}
fn checkpoint(access: &RedbReadAccess) -> &redb::ReadTransaction {
    match access {
        RedbReadAccess::Current(root) | RedbReadAccess::Durable(root) => root,
        RedbReadAccess::Composite(view) => view.checkpoint_root(),
    }
}
fn verify_epoch(
    epoch: &SourceUsageEpoch<'_>,
    actual: &RedbOperationalPorts,
    locator: riffdb_types::ProjectionSourceLocatorV1,
    operation: SourceCheckpointOperationKind,
) -> Result<(), StorageError> {
    match operation {
        SourceCheckpointOperationKind::Allocate => {
            epoch.verify_for_native_allocation_observation(actual)
        }
        SourceCheckpointOperationKind::Copy | SourceCheckpointOperationKind::Publish => {
            epoch.verify_for_native_step_observation(actual, locator)
        }
        _ => Err(integrity()),
    }
}

/// Raw original bytes, owned only under the actual Source ledger.
pub(in crate::source_checkpoint_adapter) struct NativeSourceRowImage {
    key: Vec<u8>,
    value: Vec<u8>,
    _work: ProjectionSourceWorkReservationV1,
}
impl NativeSourceRowImage {
    pub(in crate::source_checkpoint_adapter) fn key(&self) -> &[u8] {
        &self.key
    }
    pub(in crate::source_checkpoint_adapter) fn value(&self) -> &[u8] {
        &self.value
    }
}
pub(in crate::source_checkpoint_adapter) struct NativeSourceRowsPage {
    rows: Vec<NativeSourceRowImage>,
    complete: bool,
    _work: ProjectionSourceWorkReservationV1,
}
impl NativeSourceRowsPage {
    pub(in crate::source_checkpoint_adapter) fn rows(&self) -> &[NativeSourceRowImage] {
        &self.rows
    }
    pub(in crate::source_checkpoint_adapter) fn complete(&self) -> bool {
        self.complete
    }
}
