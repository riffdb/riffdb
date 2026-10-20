//! Read-only engine construction from the original retained backup descriptor.
use super::*;
use redb::ReadableDatabase;

const READ_ONLY_CACHE_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) struct ImmutableBackupObservationV1 {
    manifest_digest: [u8; 32],
}

/// Read ownership only. Neither this value nor its engine can become ordinary
/// startup, an origin proof, an operational port, or a catalog-cut completion.
pub(crate) struct OwnedBackupReadOnlyV1 {
    // The actual engine and physical cache die before their artifact custody.
    engine: riffdb_types::OwnedCapacitySharedV1<
        crate::engine_owner::EngineOwner<redb::ReadOnlyDatabase>,
    >,
    backup: Arc<OwnedBackupHistory>,
    marker: riffdb_storage_api::DurableFormatMarker,
}

impl OwnedBackupReadOnlyV1 {
    pub(crate) fn open(backup: Arc<OwnedBackupHistory>) -> Result<Self, StorageError> {
        Self::open_with_source_domain(backup, None)
    }
    pub(crate) fn open_configured(
        backup: Arc<OwnedBackupHistory>,
        domain: crate::source_working_domain::SourceWorkingDomain,
    ) -> Result<Self, StorageError> {
        Self::open_with_source_domain(backup, Some(domain))
    }
    fn open_with_source_domain(
        backup: Arc<OwnedBackupHistory>,
        domain: Option<crate::source_working_domain::SourceWorkingDomain>,
    ) -> Result<Self, StorageError> {
        backup.verify_full()?;
        let artifact = backup.artifacts.first().ok_or_else(corrupt)?;
        if artifact.name != DATABASE_ARTIFACT_FILE_NAME || artifact.generation.is_some() {
            return Err(corrupt());
        }
        let database = open_exact_descriptor(&artifact.file)?;
        let engine = match domain {
            Some(domain) => crate::engine_owner::EngineOwner::try_new_configured(
                database,
                READ_ONLY_CACHE_BYTES,
                crate::locator_memory::LocatorAllocationClass::Maintenance,
                domain,
            )?,
            None => crate::engine_owner::EngineOwner::try_new(
                database,
                READ_ONLY_CACHE_BYTES,
                crate::locator_memory::LocatorAllocationClass::Maintenance,
            )?,
        };
        let mut marker_bytes = [0; riffdb_storage_api::DURABLE_FORMAT_MARKER_BYTES];
        let marker = backup.artifacts.get(2).ok_or_else(corrupt)?;
        PositionalReader {
            file: &marker.file,
            offset: 0,
        }
        .read_exact(&mut marker_bytes)
        .map_err(io_unavailable)?;
        let marker = riffdb_storage_api::decode_durable_format_marker(&marker_bytes)
            .map_err(|_| corrupt())?;
        backup.verify()?;
        Ok(Self {
            engine,
            backup,
            marker,
        })
    }

    pub(crate) fn observe(&self) -> Result<ImmutableBackupObservationV1, StorageError> {
        self.backup.verify()?;
        let manifest_digest = self
            .backup
            .manifest_digest
            .as_bytes()
            .try_into()
            .map_err(|_| corrupt())?;
        Ok(ImmutableBackupObservationV1 { manifest_digest })
    }
    pub(crate) fn observation_matches(
        &self,
        observation: ImmutableBackupObservationV1,
    ) -> Result<bool, StorageError> {
        Ok(self.observe()? == observation)
    }
    pub(crate) fn invalidate(&self) {
        self.backup.invalidate();
    }
    pub(crate) fn marker(&self) -> riffdb_storage_api::DurableFormatMarker {
        self.marker
    }
    pub(crate) fn begin_read(&self) -> Result<redb::ReadTransaction, StorageError> {
        self.backup.verify()?;
        let read = self
            .engine
            .begin_read()
            .map_err(crate::error::transaction_error)?;
        self.backup.verify()?;
        Ok(read)
    }
    pub(crate) fn verify(&self) -> Result<(), StorageError> {
        self.backup.verify()
    }
    pub(crate) fn verify_full(&self) -> Result<(), StorageError> {
        self.backup.verify_full()
    }
    pub(crate) fn engine(
        &self,
    ) -> &riffdb_types::OwnedCapacitySharedV1<
        crate::engine_owner::EngineOwner<redb::ReadOnlyDatabase>,
    > {
        &self.engine
    }
    #[cfg(test)]
    pub(crate) fn engine_has_no_strong_or_weak_aliases_for_test(&mut self) -> bool {
        self.engine.get_mut().is_some()
    }

    pub(crate) fn history_database_id(&self) -> Result<DatabaseId, StorageError> {
        crate::store::read_identity_from_owned_backup_v1(self)
    }
    pub(crate) fn history_directory(&self) -> &Path {
        &self.backup.path
    }
    // Shared exact owner formula: the ordinary interval quote remains identical.
    fn history_media_allocation_bytes() -> Result<u64, StorageError> {
        #[repr(C)]
        struct MediaAllocationLayoutV1 {
            counts: [usize; 2],
            value: OriginalMedia,
        }
        riffdb_types::owned_allocation_capacity_bound_v1(
            std::mem::size_of::<MediaAllocationLayoutV1>(),
            std::mem::align_of::<MediaAllocationLayoutV1>(),
        )
        .map_err(|_| storage_error(StorageErrorKind::Unavailable))
    }
    pub(crate) fn history_interval_allocation_bytes() -> Result<u64, StorageError> {
        let interval = riffdb_types::owned_allocation_capacity_bound_v1(
            std::mem::size_of::<OriginalBackupReadsV1>(),
            std::mem::align_of::<OriginalBackupReadsV1>(),
        )
        .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
        interval
            .checked_add(Self::history_media_allocation_bytes()?)
            .ok_or_else(|| storage_error(StorageErrorKind::Unavailable))
    }

    pub(crate) fn begin_history_interval(
        &self,
    ) -> Result<Box<dyn crate::media::OwnedRestoreReads>, StorageError> {
        self.backup.verify()?;
        Ok(Box::new(OriginalBackupReadsV1 {
            backup: Arc::clone(&self.backup),
            finished: false,
        }))
    }
}

#[cfg(target_os = "linux")]
fn open_exact_descriptor(file: &File) -> Result<redb::ReadOnlyDatabase, StorageError> {
    let file = file.try_clone().map_err(io_unavailable)?;
    redb::Database::builder()
        .set_cache_size(READ_ONLY_CACHE_BYTES)
        .open_read_only_maintenance_file(file)
        .map_err(crate::store::native_read_error)
}
#[cfg(not(target_os = "linux"))]
fn open_exact_descriptor(_: &File) -> Result<redb::ReadOnlyDatabase, StorageError> {
    Err(storage_error(StorageErrorKind::IncompatibleFormat))
}

/// The existing full canonical maintenance decoders use this operation-local
/// read interval. It carries no live resolver registration or write capability.
struct OriginalBackupReadsV1 {
    backup: Arc<OwnedBackupHistory>,
    finished: bool,
}
impl crate::media::OwnedRestoreReads for OriginalBackupReadsV1 {
    fn media(&self) -> Arc<dyn JournalMedia> {
        Arc::new(OriginalMedia(Arc::clone(&self.backup)))
    }
    fn finish(&mut self) -> io::Result<()> {
        if self.finished {
            return Err(refused());
        }
        self.backup.verify().map_err(unavailable)?;
        self.finished = true;
        Ok(())
    }
    fn verify_finished(&self) -> io::Result<()> {
        if !self.finished {
            return Err(refused());
        }
        self.backup.verify().map_err(unavailable)
    }
}
impl Drop for OriginalBackupReadsV1 {
    fn drop(&mut self) {
        if !self.finished {
            self.backup.invalidate();
        }
    }
}

/// Complete original descriptor graph, never an engine/read or Source proof.
/// All Arc fields remain private: Source media/interval aliases must retain the
/// final fixed kernel data owner before any such alias can be exposed.
pub(crate) struct RetainedPredecessorSourceDescriptorsV1 {
    backup: Arc<OwnedBackupHistory>,
    marker: riffdb_storage_api::DurableFormatMarker,
    outer: crate::startup::RetainedPredecessorOuterV1,
}
impl RetainedPredecessorSourceDescriptorsV1 {
    pub(crate) fn source_original_history_allocation_bytes(&self) -> Result<u64, StorageError> {
        let path = riffdb_types::owned_allocation_capacity_bound_v1(
            self.backup.path.as_os_str().len(),
            std::mem::align_of::<u8>(),
        )
        .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
        path.checked_add(OwnedBackupReadOnlyV1::history_media_allocation_bytes()?)
            .ok_or_else(|| storage_error(StorageErrorKind::Unavailable))
    }
    pub(crate) fn source_original_history(
        &self,
        construction: crate::store::SourceOriginalHistoryConstructionV1,
    ) -> Result<crate::history_stub::SourceOriginalHistoryV1, StorageError> {
        self.check_cancellation()?;
        self.backup.verify()?;
        let history = self.outer.original_history();
        Ok(
            crate::history_stub::SourceOriginalHistoryV1::from_retained_backup(
                construction,
                history,
                &self.backup.path,
                Arc::new(OriginalMedia(Arc::clone(&self.backup))),
            ),
        )
    }

    pub(crate) fn check_cancellation(&self) -> Result<(), StorageError> {
        self.outer.check_cancellation()
    }
    pub(crate) fn native_quote_capture_bytes(&self) -> Result<u64, StorageError> {
        self.outer.native_quote_capture_bytes()
    }
    pub(crate) fn native_retained_resource_bytes(&self) -> Result<u64, StorageError> {
        self.outer.native_retained_resource_bytes()
    }
    pub(crate) fn inspect_source_descriptor_capacity(
        &self,
        inspection: &mut riffdb_types::OwnedCapacityInspectionV1<'_>,
    ) -> Result<(), riffdb_types::OwnedCapacityInspectionErrorV1> {
        let Self {
            backup,
            marker,
            outer,
        } = self;
        crate::source_descriptor_capacity::arc_block(inspection, backup)?;
        backup.inspect_source_descriptor_capacity(inspection)?;
        inspection.inline(marker)?;
        outer.inspect_source_descriptor_capacity(inspection)
    }
}
impl OwnedBackupReadOnlyV1 {
    // The unique permit is minted only while consuming the ACTUAL completely
    // validated predecessor. Its native engine and backup were acquired in this
    // single fields-private owner; no caller-selected read is accepted.
    pub(crate) fn into_source_read_phase(
        self,
        outer: crate::startup::RetainedPredecessorOuterV1,
        construction: crate::engine_owner::source_reopen::PredecessorSourceConstructionV1,
    ) -> Result<crate::engine_owner::source_reopen::OpenedPredecessorSourceReadV1, StorageError>
    {
        // Preserve native-before-primary retirement on EVERY verification or
        // alias refusal, rather than relying on loose argument/local order.
        struct PendingBackupBindingV1 {
            owner: OwnedBackupReadOnlyV1,
            outer: crate::startup::RetainedPredecessorOuterV1,
            construction: crate::engine_owner::source_reopen::PredecessorSourceConstructionV1,
        }
        let mut pending = PendingBackupBindingV1 {
            owner: self,
            outer,
            construction,
        };
        pending
            .outer
            .verify_backup_binding(&pending.owner.backup, pending.owner.marker)?;
        pending.owner.backup.verify_full()?;
        // Actual old interval/media values and ALL scoped TLS weak backing must
        // be retired/joined before moving descriptors into Source ownership.
        // This checks real Arc uniqueness, never IDs or a caller-supplied bool.
        if Arc::get_mut(&mut pending.owner.backup).is_none() {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        let PendingBackupBindingV1 {
            owner,
            outer,
            construction,
        } = pending;
        let Self {
            engine,
            backup,
            marker,
        } = owner;
        let retained = RetainedPredecessorSourceDescriptorsV1 {
            backup,
            marker,
            outer,
        };
        construction.bind(engine, retained)
    }
}
