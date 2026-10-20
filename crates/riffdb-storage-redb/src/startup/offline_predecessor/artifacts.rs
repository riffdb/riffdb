//! Actual primary exclusion plus exact immutable backup custody, never an opener.
use super::*;
use riffdb_storage_api::AuthoritativeNamespaceV3 as N;
use std::fs::OpenOptions;

pub(super) struct OfflinePredecessorArtifactsV1 {
    // Read engine and original backup descriptors die before primary exclusion.
    pub(super) backup: crate::backup::OwnedBackupReadOnlyV1,
    source_database: PathBuf,
    backup_directory: PathBuf,
    manifest: riffdb_storage_api::OfflineBackupManifestV1,
    _primary: crate::native_file_custody::NativeFileCustody,
    cancellation: Arc<AtomicBool>,
}

impl OfflinePredecessorArtifactsV1 {
    pub(super) fn acquire(
        source: &Path,
        backup: &Path,
        cancellation: Arc<AtomicBool>,
    ) -> Result<Self, StorageError> {
        Self::acquire_with_source_domain(source, backup, cancellation, None)
    }
    pub(super) fn acquire_configured(
        source: &Path,
        backup: &Path,
        cancellation: Arc<AtomicBool>,
        domain: crate::source_working_domain::SourceWorkingDomain,
    ) -> Result<Self, StorageError> {
        Self::acquire_with_source_domain(source, backup, cancellation, Some(domain))
    }
    fn acquire_with_source_domain(
        source: &Path,
        backup: &Path,
        cancellation: Arc<AtomicBool>,
        domain: Option<crate::source_working_domain::SourceWorkingDomain>,
    ) -> Result<Self, StorageError> {
        const MAX_PATH_BYTES: usize = 4096;
        if source.as_os_str().len() > MAX_PATH_BYTES || backup.as_os_str().len() > MAX_PATH_BYTES {
            return Err(limit_exceeded());
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(source)
            .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
        let primary = readonly_phase(
            "primary_work_exclusion",
            crate::native_file_custody::NativeFileCustody::acquire(file),
        )?;
        let observed = crate::format_preflight::preflight_durable_format_path(source)
            .map_err(|_| storage_error(StorageErrorKind::IncompatibleFormat))?;
        let crate::format_preflight::RedbDurableFormatPreflight::OfflineUpgradeRequired {
            current,
            binary,
            ..
        } = observed
        else {
            return Err(storage_error(StorageErrorKind::IncompatibleFormat));
        };
        if current.writer().get() != 4
            || binary.writer().get() != 5
            || current.epoch() != binary.epoch()
        {
            return Err(storage_error(StorageErrorKind::IncompatibleFormat));
        }
        let (manifest, _) = crate::backup::validate_immutable_backup(backup)?;
        crate::backup::validate_catalog_cut_source_artifacts(source, backup, &manifest)?;
        let owned = crate::backup::OwnedBackupHistory::acquire(
            backup,
            &manifest,
            Arc::clone(&cancellation),
        )?;
        let reader = match domain {
            Some(domain) => crate::backup::OwnedBackupReadOnlyV1::open_configured(owned, domain)?,
            None => crate::backup::OwnedBackupReadOnlyV1::open(owned)?,
        };
        if reader.marker().identity() != current {
            return Err(corrupt());
        }
        crate::backup::validate_readable_database_semantics(reader.engine().database(), &manifest)?;
        let result = Self {
            backup: reader,
            source_database: source.to_path_buf(),
            backup_directory: backup.to_path_buf(),
            manifest,
            _primary: primary,
            cancellation,
        };
        // This is only actual layout/read custody. No complete validation,
        // active-plan input closure, drain or Accepted receipt has been minted.
        result.verify()?;
        Ok(result)
    }

    pub(super) fn verify(&self) -> Result<(), StorageError> {
        self._primary.verify_path_binding(&self.source_database)?;
        self.backup.verify()?;
        crate::backup::validate_catalog_cut_source_artifacts(
            &self.source_database,
            &self.backup_directory,
            &self.manifest,
        )?;
        self._primary.verify_path_binding(&self.source_database)
    }
    pub(super) fn validate_source_absence(read: &ReadTransaction) -> Result<(), StorageError> {
        let source_tables = [
            N::ProjectionSourceRows,
            N::ProjectionSourceObjects,
            N::ProjectionSourceRoots,
            N::ProjectionSourceMaintenance,
        ];
        for table in read.list_tables().map_err(precommit_storage_error)? {
            if let Some(namespace) = source_tables
                .iter()
                .find(|namespace| namespace.table() == table.name())
            {
                let definition = TableDefinition::<&[u8], &[u8]>::new(namespace.table());
                if read
                    .open_table(definition)
                    .map_err(table_error)?
                    .len()
                    .map_err(precommit_storage_error)?
                    != 0
                {
                    return Err(corrupt());
                }
            }
        }
        for table in read
            .list_multimap_tables()
            .map_err(precommit_storage_error)?
        {
            if source_tables
                .iter()
                .any(|namespace| namespace.table() == table.name())
            {
                return Err(corrupt());
            }
        }
        let meta = read.open_table(META).map_err(table_error)?;
        let key = N::ProjectionSourceAllocator
            .metadata_key()
            .ok_or_else(invariant)?;
        if meta.get(key).map_err(precommit_storage_error)?.is_some() {
            return Err(corrupt());
        }
        Ok(())
    }
    pub(super) fn cancellation(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.cancellation)
    }
    pub(super) fn validate_v2_layout(read: &ReadTransaction) -> Result<(), StorageError> {
        Self::validate_source_absence(read)?;
        let mut tables = BTreeSet::new();
        let source_tables = [
            N::ProjectionSourceRows,
            N::ProjectionSourceObjects,
            N::ProjectionSourceRoots,
            N::ProjectionSourceMaintenance,
        ];
        let catalog = riffdb_storage_api::AuthoritativeStateCatalogV2.digest();
        for table in read.list_tables().map_err(precommit_storage_error)? {
            let name = table.name();
            if source_tables
                .iter()
                .any(|namespace| namespace.table() == name)
            {
                continue;
            }
            let known = crate::catalog_dispatch::namespaces(catalog)?
                .any(|namespace| namespace.table() == name)
                || matches!(
                    name,
                    "idempotency_locators" | "provenance_locators" | "audit_by_request_locators"
                );
            if !known {
                return Err(storage_error(StorageErrorKind::IncompatibleFormat));
            }
            tables.insert(name.to_owned());
        }
        if read
            .list_multimap_tables()
            .map_err(precommit_storage_error)?
            .next()
            .is_some()
        {
            return Err(storage_error(StorageErrorKind::IncompatibleFormat));
        }
        crate::store::v3_layout::exact_current_tables(&tables, &BTreeSet::new(), catalog)
    }
    pub(super) fn original_registry(&self) -> riffdb_types::SchemaHash {
        self.backup.marker().registry_digest()
    }
    pub(super) fn manifest(&self) -> &riffdb_storage_api::OfflineBackupManifestV1 {
        &self.manifest
    }
}

impl OfflinePredecessorArtifactsV1 {
    pub(super) fn into_source_read_phase(
        self,
        observation: FixedMaintenanceObservationV1,
        construction: crate::engine_owner::source_reopen::PredecessorSourceConstructionV1,
    ) -> Result<crate::engine_owner::source_reopen::OpenedPredecessorSourceReadV1, StorageError>
    {
        self.verify()?;
        self.backup.verify_full()?;
        let Self {
            backup,
            source_database,
            backup_directory,
            manifest,
            _primary,
            cancellation,
        } = self;
        let outer = source_phase::RetainedPredecessorOuterV1::from_artifacts(
            source_database,
            backup_directory,
            manifest,
            _primary,
            cancellation,
            observation,
        );
        // The same original backup owner consumes its own actual engine and
        // descriptor together; no getter returns a loose engine/budget pair.
        backup.into_source_read_phase(outer, construction)
    }
}
