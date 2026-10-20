//! Exact capture read owned by its actual originating, admitted native context.
//! No caller-selected owner/raw-read pair or transferable origin proof exists.
#![allow(dead_code)]
use super::*;
use crate::layout::{CATALOG_ACTIVE, CATALOG_ACTIVE_KEY};
use riffdb_storage_api::proto_codec::{
    ProjectionSourceActiveCatalogDecodeAdmissionV1,
    decode_projection_checkpoint_ordinal_allocator_inline_v1,
};
use riffdb_storage_api::{
    ActiveCatalogPointerV1, AuthoritativeNamespaceV3, BorrowedOverlayPointV1,
    ChangelogHistoryStateV3, NativeCatalogOriginReferenceV1, ProjectionCheckpointOrdinalPositionV1,
};
use riffdb_types::{ProjectionSourceLocatorV1, ProjectionSourceWorkAllocationV1};

pub(crate) struct OriginalCapturedRead<'context, 'ports> {
    // The actual read/access physically drops before its originating context.
    access: RedbReadAccess,
    context: &'context OriginalSourceReadContext<'ports>,
    // True only on the no-new-registration selector below.
    native_existing_base: bool,
    // Strict clone-only control is prepaid and drops after the actual access.
    _native_control: Option<riffdb_types::ProjectionSourceWorkReservationV1>,
}
impl Drop for OriginalCapturedRead<'_, '_> {
    fn drop(&mut self) {}
}
impl<'ports> OriginalSourceReadContext<'ports> {
    pub(crate) fn begin_existing_native_captured_read(
        &self,
    ) -> Result<OriginalCapturedRead<'_, 'ports>, StorageError> {
        let bytes = u64::try_from(std::mem::size_of::<OriginalCapturedRead<'_, '_>>())
            .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
        let control = self
            .budget()
            .reserve(bytes)
            .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
        let access = self
            .ports
            .shared
            .try_clone_existing_source_base()?
            .ok_or_else(|| storage_error(StorageErrorKind::Unavailable))?;
        Ok(OriginalCapturedRead {
            access,
            context: self,
            native_existing_base: true,
            _native_control: Some(control),
        })
    }
    pub(super) fn begin_captured_read(
        &self,
    ) -> Result<OriginalCapturedRead<'_, 'ports>, StorageError> {
        Ok(OriginalCapturedRead {
            access: self.ports.begin_composite_read()?,
            context: self,
            native_existing_base: false,
            _native_control: None,
        })
    }
}
impl OriginalCapturedRead<'_, '_> {
    pub(in crate::store) fn checked_existing_native_access(
        &self,
        prefix: &OriginalPrefixRetentionLease,
    ) -> Result<&RedbReadAccess, StorageError> {
        if !prefix.belongs_to_ports(self.context.ports)
            || !self.native_existing_base
            || !self.context.ports.shared.source_native_file_backend
        {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        Ok(&self.access)
    }
    #[cfg(test)]
    pub(crate) fn native_existing_composite_for_test(&self) -> bool {
        self.native_existing_base && matches!(&self.access, RedbReadAccess::Composite(_))
    }
    pub(super) fn access(&self) -> &RedbReadAccess {
        &self.access
    }
    fn checkpoint(&self) -> &redb::ReadTransaction {
        match &self.access {
            RedbReadAccess::Current(root) | RedbReadAccess::Durable(root) => root,
            RedbReadAccess::Composite(view) => view.checkpoint_root(),
        }
    }
    pub(super) fn checked_origin(&self) -> Result<NativeCatalogOriginReferenceV1, StorageError> {
        // Both sides of the pair are chosen by this fields-private capture.
        // The accessor checks existing actual-owner proof; it never mints it.
        crate::native_source_fresh::checked_captured_source_origin_v1(
            &self.context.ports.shared.database,
            self.checkpoint(),
        )
    }
    pub(super) fn pointer(
        &self,
        admitted: ProjectionSourceActiveCatalogDecodeAdmissionV1,
    ) -> Result<ProjectionSourceWorkAllocationV1<ActiveCatalogPointerV1>, StorageError> {
        // CatalogActive has no JournalTable slot: its dedicated activation
        // owner uses a direct barrier. This is THIS access's checkpoint only.
        let table = self
            .checkpoint()
            .open_table(CATALOG_ACTIVE)
            .map_err(table_error)?;
        let row = table
            .get(CATALOG_ACTIVE_KEY.as_slice())
            .map_err(precommit_storage_error)?
            .ok_or_else(|| storage_error(StorageErrorKind::Unavailable))?;
        admitted
            .decode(row.value())
            .map(|decoded| decoded.into_parts().0)
            .map_err(source_record_error)
    }
    pub(super) fn tentative_locator(
        &self,
        history: ChangelogHistoryStateV3,
    ) -> Result<ProjectionSourceLocatorV1, StorageError> {
        let key = AuthoritativeNamespaceV3::ProjectionSourceAllocator
            .metadata_key()
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
        let decode = |bytes: &[u8]| {
            let allocator = *decode_projection_checkpoint_ordinal_allocator_inline_v1(bytes)
                .map_err(crate::error::codec_error)?
                .value();
            let incarnation = history.lineage().history_incarnation();
            if allocator.allocation_history_incarnation() != incarnation {
                return Err(storage_error(StorageErrorKind::Unavailable));
            }
            let ProjectionCheckpointOrdinalPositionV1::Next(ordinal) = allocator.position() else {
                return Err(storage_error(StorageErrorKind::Unavailable));
            };
            // An expected Next locator only. This read neither assigns it nor
            // constructs the allocator postimage or a publication capability.
            ProjectionSourceLocatorV1::new(history.lineage().database_id(), incarnation, ordinal)
                .ok_or_else(|| storage_error(StorageErrorKind::Unavailable))
        };
        if let RedbReadAccess::Composite(view) = &self.access {
            match view.overlay().borrowed_provider_point_v1(
                riffdb_storage_api::CompositeTableV1::Meta,
                key.as_bytes(),
            ) {
                BorrowedOverlayPointV1::Tombstone => {
                    return Err(storage_error(StorageErrorKind::Unavailable));
                }
                BorrowedOverlayPointV1::Value(bytes) => return decode(bytes),
                BorrowedOverlayPointV1::Checkpoint => {}
            }
        }
        let meta = self.checkpoint().open_table(META).map_err(table_error)?;
        let row = meta
            .get(key)
            .map_err(precommit_storage_error)?
            .ok_or_else(|| storage_error(StorageErrorKind::Unavailable))?;
        decode(row.value())
    }
}
pub(super) fn source_record_error(
    error: riffdb_storage_api::ProjectionSourceRecordDecodeErrorV1,
) -> StorageError {
    match error {
        riffdb_storage_api::ProjectionSourceRecordDecodeErrorV1::AdmissionUnavailable => {
            storage_error(StorageErrorKind::Unavailable)
        }
        _ => storage_error(StorageErrorKind::CorruptData),
    }
}
