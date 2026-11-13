//! Exact capture read owned by its actual originating, admitted native context.
//! No caller-selected owner/raw-read pair or transferable origin proof exists.
use super::*;

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
