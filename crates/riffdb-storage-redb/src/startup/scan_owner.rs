//! Closed native read ownership for one structural/historical evidence scan.
use super::*;

mod sealed {
    pub(crate) trait AuditOwner {}
    pub(crate) trait ScanOwner: AuditOwner {}

    impl AuditOwner for crate::engine_owner::EngineOwner<redb::Database> {}
    impl AuditOwner for std::sync::Arc<crate::store::SharedRedb> {}
    impl ScanOwner for std::sync::Arc<crate::store::SharedRedb> {}
    #[cfg(unix)]
    impl AuditOwner for super::super::offline_predecessor::OfflineBackupScanOwnerV1<'_> {}
    #[cfg(unix)]
    impl ScanOwner for super::super::offline_predecessor::OfflineBackupScanOwnerV1<'_> {}
}

/// This private dispatch never admits a caller-defined audit resolver or read.
/// Its only live implementations retain the actual existing native engine.
pub(super) trait EvidenceAuditReadOwnerV1: sealed::AuditOwner + Sync {
    fn begin_evidence_read_scope(&self) -> Result<EvidenceReadScopeV1<'_>, StorageError>;
    fn checkpoint_roots(
        &self,
        transaction: &ReadTransaction,
    ) -> Result<Option<riffdb_storage_api::ChangelogHistoryStateV3>, StorageError>;
    fn checked_native_origin_row(
        &self,
        transaction: &ReadTransaction,
        key: &[u8],
        value: &[u8],
    ) -> Result<bool, StorageError>;
}

/// The scan core can read only through its exact closed native owner. Normal
/// startup keeps its concrete live specialization and all existing completion
/// methods; a readonly specialization does not implement those capabilities.
pub(super) trait EvidenceScanReadOwnerV1:
    EvidenceAuditReadOwnerV1 + sealed::ScanOwner
{
    type Observation: Copy;
    fn admit_source_maintenance_decode(
        &self,
        bytes: usize,
    ) -> Result<crate::locator_memory::LocatorCharge, StorageError>;
    fn observe_source_origin_control(
        &self,
        transaction: &ReadTransaction,
        observation: &mut crate::native_source_fresh::SourceOriginObservationV1,
    ) -> Result<(), StorageError>;
    fn scan_epoch_matches(&self, observation: Self::Observation) -> Result<bool, StorageError>;
    fn open_scan_read(&self) -> Result<ReadTransaction, StorageError>;
    fn validate_scan_binding(&self, transaction: &ReadTransaction) -> Result<(), StorageError>;
    fn scan_retained_metadata(
        &self,
        transaction: &ReadTransaction,
    ) -> Result<RetainedMetadataV1, StorageError>;
    fn inspect_scan_meta_row(
        &self,
        key: &str,
        value: &[u8],
        database_id: DatabaseId,
    ) -> Option<StructuralFinding>;
}

impl EvidenceAuditReadOwnerV1 for crate::engine_owner::EngineOwner<redb::Database> {
    fn begin_evidence_read_scope(&self) -> Result<EvidenceReadScopeV1<'_>, StorageError> {
        Ok(EvidenceReadScopeV1::Live)
    }
    fn checkpoint_roots(
        &self,
        transaction: &ReadTransaction,
    ) -> Result<Option<riffdb_storage_api::ChangelogHistoryStateV3>, StorageError> {
        crate::changelog_v3_roots::read_checkpoint_roots(transaction)
    }
    fn checked_native_origin_row(
        &self,
        transaction: &ReadTransaction,
        key: &[u8],
        value: &[u8],
    ) -> Result<bool, StorageError> {
        crate::native_source_fresh::checked_origin_row(self, transaction, key, value)
    }
}

impl EvidenceAuditReadOwnerV1 for Arc<SharedRedb> {
    fn begin_evidence_read_scope(&self) -> Result<EvidenceReadScopeV1<'_>, StorageError> {
        Ok(EvidenceReadScopeV1::Live)
    }
    fn checkpoint_roots(
        &self,
        transaction: &ReadTransaction,
    ) -> Result<Option<riffdb_storage_api::ChangelogHistoryStateV3>, StorageError> {
        crate::changelog_v3_roots::read_checkpoint_roots(transaction)
    }
    fn checked_native_origin_row(
        &self,
        transaction: &ReadTransaction,
        key: &[u8],
        value: &[u8],
    ) -> Result<bool, StorageError> {
        self.database
            .checked_native_origin_row(transaction, key, value)
    }
}

impl EvidenceScanReadOwnerV1 for Arc<SharedRedb> {
    type Observation = u64;
    fn admit_source_maintenance_decode(
        &self,
        bytes: usize,
    ) -> Result<crate::locator_memory::LocatorCharge, StorageError> {
        self.database.admit_maintenance_decode(bytes)
    }
    fn observe_source_origin_control(
        &self,
        transaction: &ReadTransaction,
        observation: &mut crate::native_source_fresh::SourceOriginObservationV1,
    ) -> Result<(), StorageError> {
        observation.step(&self.database, transaction)
    }
    fn scan_epoch_matches(&self, observation: u64) -> Result<bool, StorageError> {
        Ok(self.durable_commit_epoch() == observation)
    }
    fn open_scan_read(&self) -> Result<ReadTransaction, StorageError> {
        self.database.begin_read().map_err(transaction_error)
    }
    fn validate_scan_binding(&self, transaction: &ReadTransaction) -> Result<(), StorageError> {
        if let Some(binding) = self.private_restore_binding() {
            binding.validate(transaction)?;
        }
        Ok(())
    }
    fn scan_retained_metadata(
        &self,
        transaction: &ReadTransaction,
    ) -> Result<RetainedMetadataV1, StorageError> {
        read_retained_metadata(transaction)
    }
    fn inspect_scan_meta_row(
        &self,
        key: &str,
        value: &[u8],
        database_id: DatabaseId,
    ) -> Option<StructuralFinding> {
        inspect_meta_row(key, value, database_id)
    }
}

/// A closed maintenance scope, not a caller-supplied callback or resolver.
pub(super) enum EvidenceReadScopeV1<'owner> {
    Live,
    #[cfg(unix)]
    Immutable(crate::history_stub::ImmutableBackupReadsV1<'owner>),
    #[cfg(not(unix))]
    _Unavailable(std::marker::PhantomData<&'owner ()>),
}
impl EvidenceReadScopeV1<'_> {
    pub(super) fn finish(&mut self) -> Result<(), StorageError> {
        match self {
            Self::Live => Ok(()),
            #[cfg(unix)]
            Self::Immutable(scope) => scope.finish(),
            #[cfg(not(unix))]
            Self::_Unavailable(_) => Err(storage_error(StorageErrorKind::IncompatibleFormat)),
        }
    }
}

/// Closed historical-registry dispatch. No normal opener accepts another digest.
pub(super) enum RetainedRegistryAdmissionV1<'owner> {
    Startup(std::marker::PhantomData<&'owner ()>),
    #[cfg(unix)]
    OfflineWriter4(&'owner super::offline_predecessor::OfflineBackupScanOwnerV1<'owner>),
}
impl RetainedRegistryAdmissionV1<'_> {
    pub(super) fn startup() -> Self {
        Self::Startup(std::marker::PhantomData)
    }
    pub(super) fn accepts(&self, registry: riffdb_types::SchemaHash) -> bool {
        match self {
            Self::Startup(_) => startup_registry_is_supported(registry),
            #[cfg(unix)]
            Self::OfflineWriter4(owner) => registry == owner.original_registry(),
        }
    }
    pub(super) fn validate_inventory(
        &self,
        read: &ReadTransaction,
        private: Option<crate::maintenance::PrivateArchiveValidationBinding>,
    ) -> Result<(), StorageError> {
        match self {
            Self::Startup(_) => validate_table_inventory(read, private),
            #[cfg(unix)]
            Self::OfflineWriter4(owner) => {
                if private.is_some() {
                    return Err(invariant());
                }
                owner.validate_layout(read)
            }
        }
    }
}

#[cfg(unix)]
impl EvidenceAuditReadOwnerV1 for super::offline_predecessor::OfflineBackupScanOwnerV1<'_> {
    fn begin_evidence_read_scope(&self) -> Result<EvidenceReadScopeV1<'_>, StorageError> {
        self.begin_history_scope()
            .map(EvidenceReadScopeV1::Immutable)
    }
    fn checkpoint_roots(
        &self,
        transaction: &ReadTransaction,
    ) -> Result<Option<riffdb_storage_api::ChangelogHistoryStateV3>, StorageError> {
        self.verify_read_binding(transaction)?;
        self.read_checkpoint_roots()
    }
    fn checked_native_origin_row(
        &self,
        transaction: &ReadTransaction,
        _key: &[u8],
        _value: &[u8],
    ) -> Result<bool, StorageError> {
        // The actual constructor proved V2 and source absence. Returning false
        // preserves the ordinary legacy decoder, which rejects native 85/86;
        // this branch never supplies an origin bypass or live control proof.
        self.verify_read_binding(transaction)?;
        Ok(false)
    }
}
#[cfg(unix)]
impl EvidenceScanReadOwnerV1 for super::offline_predecessor::OfflineBackupScanOwnerV1<'_> {
    type Observation = crate::backup::ImmutableBackupObservationV1;
    fn admit_source_maintenance_decode(
        &self,
        bytes: usize,
    ) -> Result<crate::locator_memory::LocatorCharge, StorageError> {
        self.admit_source_decode(bytes)
    }
    fn observe_source_origin_control(
        &self,
        transaction: &ReadTransaction,
        _observation: &mut crate::native_source_fresh::SourceOriginObservationV1,
    ) -> Result<(), StorageError> {
        // The sealed writer4 constructor proved these domains empty. It cannot
        // supply a Fresh/current native context for subsequently found Source.
        self.verify_read_binding(transaction)?;
        Err(storage_error(StorageErrorKind::CorruptData))
    }
    fn scan_epoch_matches(&self, observation: Self::Observation) -> Result<bool, StorageError> {
        self.observation_matches(observation)
    }
    fn open_scan_read(&self) -> Result<ReadTransaction, StorageError> {
        self.begin_read()
    }
    fn validate_scan_binding(&self, transaction: &ReadTransaction) -> Result<(), StorageError> {
        self.verify_read_binding(transaction)
    }
    fn scan_retained_metadata(
        &self,
        transaction: &ReadTransaction,
    ) -> Result<RetainedMetadataV1, StorageError> {
        read_retained_metadata_with_admission(
            transaction,
            RetainedRegistryAdmissionV1::OfflineWriter4(self),
        )
    }
    fn inspect_scan_meta_row(
        &self,
        key: &str,
        value: &[u8],
        database_id: DatabaseId,
    ) -> Option<StructuralFinding> {
        inspect_meta_row_with_admission(
            key,
            value,
            database_id,
            RetainedRegistryAdmissionV1::OfflineWriter4(self),
        )
    }
}
