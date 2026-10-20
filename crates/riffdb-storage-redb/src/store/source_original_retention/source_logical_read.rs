//! Sealed logical Source transport over one actual admitted native observation.
//! Overlay precedence is used only by logical application/meta reads. Checkpoint
//! root validation always sees the unchanged checkpoint, never mixed frontiers.
#![allow(clippy::let_and_return)]
use super::*;
use crate::store::source_native_read::{OriginalNativeBase, SourceNativeReadTransaction};
use redb::TableHandle;
use riffdb_storage_api::{
    BorrowedOverlayPointV1, ChangelogHistoryStateV3, NativeCatalogOriginReferenceV1,
    ProjectionSourceCommandReadV1,
};

pub(crate) struct ScalarBytes {
    bytes: [u8; 512],
    len: usize,
}
impl ScalarBytes {
    fn copy(bytes: &[u8]) -> Result<Self, StorageError> {
        if bytes.len() > 512 {
            return Err(storage_error(StorageErrorKind::LimitExceeded));
        }
        let mut result = Self {
            bytes: [0; 512],
            len: bytes.len(),
        };
        result.bytes[..bytes.len()].copy_from_slice(bytes);
        Ok(result)
    }
    pub(crate) fn value(&self) -> &[u8] {
        &self.bytes[..self.len]
    }
}
pub(crate) struct OriginalLogicalRead<'read, 'context, 'ports> {
    native: SourceNativeReadTransaction<'read>,
    base: &'read OriginalNativeBase<'context, 'ports>,
    visits: u32,
}
impl Drop for OriginalLogicalRead<'_, '_, '_> {
    fn drop(&mut self) {}
}
impl<'context, 'ports> OriginalNativeBase<'context, 'ports> {
    pub(in crate::store) fn logical(
        &self,
    ) -> Result<OriginalLogicalRead<'_, 'context, 'ports>, StorageError> {
        Ok(OriginalLogicalRead {
            native: self.open()?,
            base: self,
            visits: 0,
        })
    }
}
impl OriginalLogicalRead<'_, '_, '_> {
    fn visit(&mut self) -> Result<(), StorageError> {
        // All loops outside this reader remain bounded by their existing role
        // inventories. This counts physical point/range probes, not DTOs.
        self.visits = self.visits.checked_add(1).ok_or_else(corrupt)?;
        Ok(())
    }
    pub(crate) fn context(&self) -> &OriginalSourceReadContext<'_> {
        self.base.context()
    }
    pub(crate) fn checkpoint_meta(
        &mut self,
        key: &str,
    ) -> Result<Option<ScalarBytes>, StorageError> {
        self.visit()?;
        let mut table = self.native.open_str_table(META.name())?;
        let result = table
            .get(key)?
            .map(|row| ScalarBytes::copy(row.value()))
            .transpose();
        result
    }
    pub(crate) fn checkpoint_meta_present(&mut self, key: &str) -> Result<bool, StorageError> {
        self.visit()?;
        let mut table = self.native.open_str_table(META.name())?;
        Ok(table.get(key)?.is_some())
    }
    pub(crate) fn checkpoint_table_present(&mut self, name: &str) -> Result<bool, StorageError> {
        self.visit()?;
        Ok(self.native.optional_bytes_table(name)?.is_some())
    }
    pub(crate) fn checkpoint_table_empty(&mut self, name: &str) -> Result<bool, StorageError> {
        self.visit()?;
        let mut table = self.native.open_bytes_table(name)?;
        let mut range = table.range_from(None)?;
        Ok(range.next()?.is_none())
    }
    fn required_meta(&mut self, key: &str) -> Result<ScalarBytes, StorageError> {
        self.checkpoint_meta(key)?.ok_or_else(corrupt)
    }
    fn logical_meta(&mut self, key: &str) -> Result<Option<ScalarBytes>, StorageError> {
        if let Some(view) = self.base.composite() {
            match view.overlay().borrowed_provider_point_v1(
                riffdb_storage_api::CompositeTableV1::Meta,
                key.as_bytes(),
            ) {
                BorrowedOverlayPointV1::Tombstone => return Ok(None),
                BorrowedOverlayPointV1::Value(value) => return ScalarBytes::copy(value).map(Some),
                BorrowedOverlayPointV1::Checkpoint => {}
            }
        }
        self.checkpoint_meta(key)
    }
    pub(in crate::store) fn watermark(&mut self) -> Result<u64, StorageError> {
        self.logical_meta(crate::layout::META_RETENTION_WATERMARK)?
            .map(|row| {
                riffdb_storage_api::proto_codec::decode_retention_watermark_v1(row.value())
                    .map(|d| d.value().watermark_sequence())
                    .map_err(crate::error::codec_error)
            })
            .transpose()
            .map(|v| v.unwrap_or(0))
    }
    pub(in crate::store) fn history_position(
        &mut self,
    ) -> Result<ChangelogHistoryStateV3, StorageError> {
        if let Some(view) = self.base.composite() {
            return crate::changelog_v3_cursor::capture_history_position_v1_from_composite(view);
        }
        let key = riffdb_storage_api::AuthoritativeNamespaceV1::ChangelogHistoryState
            .metadata_key()
            .ok_or_else(corrupt)?;
        riffdb_storage_api::proto_codec::decode_changelog_history_state_v3(
            self.required_meta(key)?.value(),
        )
        .map(|d| *d.value())
        .map_err(crate::error::codec_error)
    }
    pub(in crate::store) fn verified_history(
        &mut self,
    ) -> Result<ChangelogHistoryStateV3, StorageError> {
        let checkpoint =
            crate::changelog_v3_roots::read_native_source_roots(self)?.ok_or_else(corrupt)?;
        if let Some(view) = self.base.composite() {
            crate::changelog_v3_cursor::validate_source_composite_suffix_v1(
                view,
                checkpoint,
                self.context(),
            )
        } else {
            Ok(checkpoint)
        }
    }
    pub(in crate::store) fn tentative_locator(
        &mut self,
        history: ChangelogHistoryStateV3,
    ) -> Result<riffdb_types::ProjectionSourceLocatorV1, StorageError> {
        let key = riffdb_storage_api::AuthoritativeNamespaceV3::ProjectionSourceAllocator
            .metadata_key()
            .ok_or_else(corrupt)?;
        let row = self.logical_meta(key)?.ok_or_else(unavailable)?;
        let value=*riffdb_storage_api::proto_codec::decode_projection_checkpoint_ordinal_allocator_inline_v1(row.value())
            .map_err(crate::error::codec_error)?.value();
        let incarnation = history.lineage().history_incarnation();
        if value.allocation_history_incarnation() != incarnation {
            return Err(unavailable());
        }
        let riffdb_storage_api::ProjectionCheckpointOrdinalPositionV1::Next(ordinal) =
            value.position()
        else {
            return Err(unavailable());
        };
        riffdb_types::ProjectionSourceLocatorV1::new(
            history.lineage().database_id(),
            incarnation,
            ordinal,
        )
        .ok_or_else(unavailable)
    }
    pub(in crate::store) fn pointer(
        &mut self,
        admitted: riffdb_storage_api::proto_codec::ProjectionSourceActiveCatalogDecodeAdmissionV1,
    ) -> Result<
        riffdb_types::ProjectionSourceWorkAllocationV1<riffdb_storage_api::ActiveCatalogPointerV1>,
        StorageError,
    > {
        self.visit()?;
        let mut table = self
            .native
            .open_bytes_table(crate::layout::CATALOG_ACTIVE.name())?;
        let row = table
            .get(crate::layout::CATALOG_ACTIVE_KEY.as_slice())?
            .ok_or_else(unavailable)?;
        admitted
            .decode(row.value())
            .map(|d| d.into_parts().0)
            .map_err(super::source_original_metadata::source_record_error)
    }
    pub(in crate::store) fn checked_origin(
        &mut self,
    ) -> Result<NativeCatalogOriginReferenceV1, StorageError> {
        // The owner is selected only from this context; never from IDs/arguments.
        let base = self.base;
        let owner = &base.context().ports().shared.database;
        let key = crate::native_source_fresh::source_origin_anchor_key_v1(owner)?;
        let catalog = self.required_meta(
            riffdb_storage_api::AuthoritativeNamespaceV1::AuthoritativeStateCatalog
                .metadata_key()
                .ok_or_else(corrupt)?,
        )?;
        let incarnation = self.required_meta(META_HISTORY_INCARNATION)?;
        let epoch = self.required_meta(
            riffdb_storage_api::AuthoritativeNamespaceV1::LeadershipEpoch
                .metadata_key()
                .ok_or_else(corrupt)?,
        )?;
        self.visit()?;
        let mut table = self.native.open_bytes_table(crate::layout::AUDIT.name())?;
        let row = table.get(&key)?.ok_or_else(corrupt)?;
        // Existing context verifies exact canonical original artifact/stamps.
        // This read cannot install a context or transfer one to another engine.
        crate::native_source_fresh::checked_source_origin_controls_v1(
            owner,
            &key,
            row.value(),
            catalog.value(),
            incarnation.value(),
            epoch.value(),
        )
    }
    pub(crate) fn validate_checkpoint_source_allocator(
        &mut self,
        catalog: [u8; 32],
        incarnation: u64,
    ) -> Result<(), StorageError> {
        let key = riffdb_storage_api::AuthoritativeNamespaceV3::ProjectionSourceAllocator
            .metadata_key()
            .ok_or_else(corrupt)?;
        self.visit()?;
        let mut table = self.native.open_str_table(META.name())?;
        let row = table.get(key)?;
        crate::changelog_v3_roots::validate_source_allocator_value(
            catalog,
            incarnation,
            row.as_ref().map(|v| v.value()),
        )
    }
    pub(crate) fn checkpoint_admission(
        &mut self,
    ) -> Result<Option<riffdb_storage_api::ReplicationPrimaryAdmissionV1>, StorageError> {
        self.visit()?;
        let mut table = self.native.open_str_table(META.name())?;
        let result = table
            .get(crate::primary_admission_roots::key()?)?
            .map(|row| {
                riffdb_storage_api::proto_codec::decode_replication_primary_admission(row.value())
                    .map(|d| d.value().clone())
                    .map_err(crate::error::codec_error)
            })
            .transpose();
        result
    }
    pub(crate) fn validate_checkpoint_terminal(
        &mut self,
        history: ChangelogHistoryStateV3,
    ) -> Result<(), StorageError> {
        self.visit()?;
        let source = self.base.context();
        let mut table = self
            .native
            .open_bytes_table(crate::changelog_v3_activation::HISTORY.name())?;
        let row = table
            .get(history.tail().sequence().get().to_be_bytes().as_slice())?
            .ok_or_else(corrupt)?;
        source.validate_terminal(row.value(), history)
    }
    pub(in crate::store) fn validate_cursor_origin(
        &mut self,
        prefix: &OriginalPrefixRetentionLease,
    ) -> Result<(), StorageError> {
        let ports = self.base.context().ports();
        if !prefix.belongs_to_ports(ports)
            || !ports.shared.is_source_mode()
            || ports.shared.write_fenced.load(Ordering::Acquire)
        {
            return Err(unavailable());
        }
        let lineage = prefix.captured_history().lineage();
        let database = *crate::codec::decode_database_identity_v1(
            self.required_meta(META_DATABASE_ID)?.value(),
        )?
        .value();
        let incarnation = *crate::codec::decode_history_incarnation_v1(
            self.required_meta(META_HISTORY_INCARNATION)?.value(),
        )?
        .value();
        let epoch = *riffdb_storage_api::proto_codec::decode_leadership_epoch_v1(
            self.required_meta(
                riffdb_storage_api::AuthoritativeNamespaceV1::LeadershipEpoch
                    .metadata_key()
                    .ok_or_else(corrupt)?,
            )?
            .value(),
        )
        .map_err(crate::error::codec_error)?
        .value();
        let catalog = crate::primary_admission_roots::catalog_digest(
            self.required_meta(
                riffdb_storage_api::AuthoritativeNamespaceV1::AuthoritativeStateCatalog
                    .metadata_key()
                    .ok_or_else(corrupt)?,
            )?
            .value(),
        )?;
        if database != lineage.database_id()
            || incarnation != lineage.history_incarnation()
            || epoch != lineage.leadership_epoch()
            || catalog != lineage.catalog_digest()
        {
            return Err(unavailable());
        }
        if self.watermark()? != 0 {
            return Err(storage_error(StorageErrorKind::HistoryPruned));
        }
        Ok(())
    }
    pub(in crate::store) fn command(
        &mut self,
        physical: CommitSequence,
    ) -> Result<ProjectionSourceCommandReadV1, StorageError> {
        let key = crate::keys::encode_application_sequence_key(physical);
        if let Some(view) = self.base.composite() {
            match view
                .overlay()
                .borrowed_provider_point_v1(riffdb_storage_api::CompositeTableV1::Commits, &key)
            {
                BorrowedOverlayPointV1::Tombstone => return Err(corrupt()),
                BorrowedOverlayPointV1::Value(value) => {
                    return self.context().decode_command_at(value, physical);
                }
                BorrowedOverlayPointV1::Checkpoint => {}
            }
        }
        self.visit()?;
        let mut table = self
            .native
            .open_bytes_table(crate::layout::COMMITS.name())?;
        let mut range = table.range_from(Some(&key))?;
        let row = range.next()?.ok_or_else(corrupt)?;
        if row.key() != key {
            return Err(corrupt());
        }
        self.base.context().decode_command_at(row.value(), physical)
    }
    pub(in crate::store) fn join_event(
        &mut self,
        graph: &mut ProjectionSourceCommandReadV1,
        index: usize,
    ) -> Result<(), StorageError> {
        let key = crate::keys::encode_event_key(graph.required_events()[index].event_id());
        if let Some(view) = self.base.composite() {
            match view
                .overlay()
                .borrowed_provider_point_v1(riffdb_storage_api::CompositeTableV1::Events, &key)
            {
                BorrowedOverlayPointV1::Tombstone => return Err(corrupt()),
                BorrowedOverlayPointV1::Value(value) => {
                    return graph.accept_event_canonical(value).map_err(source_error);
                }
                BorrowedOverlayPointV1::Checkpoint => {}
            }
        }
        self.visit()?;
        let mut table = self.native.open_bytes_table(crate::layout::EVENTS.name())?;
        let row = table.get(&key)?.ok_or_else(corrupt)?;
        graph
            .accept_event_canonical(row.value())
            .map_err(source_error)
    }
}
fn corrupt() -> StorageError {
    storage_error(StorageErrorKind::CorruptData)
}
fn unavailable() -> StorageError {
    storage_error(StorageErrorKind::Unavailable)
}
fn source_error(e: riffdb_storage_api::ProjectionSourceRecordDecodeErrorV1) -> StorageError {
    if matches!(
        e,
        riffdb_storage_api::ProjectionSourceRecordDecodeErrorV1::AdmissionUnavailable
    ) {
        unavailable()
    } else {
        corrupt()
    }
}
