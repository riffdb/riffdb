//! Fresh bounded reads of an exact engine's pinned Original1..F prefix.
#![cfg_attr(not(feature = "projection-source-driver"), allow(dead_code))]
use super::source_command_prefix::{SourceCommandPrefixV1, SourceCommandReadV1};
use super::*;
use riffdb_storage_api::StoredCommitRecordV1;
use riffdb_types::ProjectionSourceWorkReservationV1;

pub(crate) struct OriginalSourceCursor<'source> {
    prefix_state: SourceCommandPrefixV1,
    ports: &'source RedbOperationalPorts,
    prefix: &'source OriginalPrefixRetentionLease,
    _control: ProjectionSourceWorkReservationV1,
}
impl Drop for OriginalSourceCursor<'_> {
    fn drop(&mut self) {}
}
impl<'source> OriginalSourceCursor<'source> {
    pub(crate) fn open(
        ports: &'source RedbOperationalPorts,
        prefix: &'source OriginalPrefixRetentionLease,
    ) -> Result<Self, StorageError> {
        if !prefix.belongs_to_ports(ports) {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        let bytes = u64::try_from(std::mem::size_of::<Self>())
            .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
        let control = prefix
            .originating_budget()
            .reserve(bytes)
            .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
        Ok(Self {
            prefix_state: SourceCommandPrefixV1::new(Some(prefix.captured_through())),
            ports,
            prefix,
            _control: control,
        })
    }
    pub(crate) fn next_commit(&mut self) -> Result<Option<&StoredCommitRecordV1>, StorageError> {
        // Drop an exhausted prior graph BEFORE capture work, and preserve EOF.
        if self.prefix_state.prepare_next().is_none() {
            return Ok(None);
        }
        // Preserve the same actual-origin check for every primary command read.
        let context = OriginalSourceReadContext::admit(self.ports)?;
        let base = context.begin_native_base()?;
        let mut access = base.logical()?;
        access.validate_cursor_origin(self.prefix)?;
        self.prefix_state
            .next_commit(&mut SourceCommandReadV1::Primary(&mut access))
    }
}
