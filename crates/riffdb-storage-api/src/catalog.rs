//! Immutable contract-bundle and active-catalog persistence semantics.

use std::fmt;

use riffdb_types::{
    AdministrationSequence, ApprovalId, ContractBundleHash, ContractLineage, ContractVersion,
    RequestId, Timestamp,
};

use crate::{
    AuditPrincipalV1, MAX_CATALOG_BUNDLE_BYTES, StorageError, StorageValueError,
    StoredContractMigrationEdgeV1,
};

/// Storage-structural identity and canonical bytes for one immutable bundle.
#[derive(Clone, Eq, PartialEq)]
pub struct StoredContractBundleV1 {
    lineage: ContractLineage,
    contract_version: ContractVersion,
    bundle_hash: ContractBundleHash,
    canonical_bytes: Vec<u8>,
}

impl StoredContractBundleV1 {
    /// Constructs a bounded opaque bundle record.
    ///
    /// Storage preserves these bytes and identity but never decodes contract IR.
    pub fn new(
        lineage: ContractLineage,
        contract_version: ContractVersion,
        bundle_hash: ContractBundleHash,
        canonical_bytes: Vec<u8>,
    ) -> Result<Self, StorageValueError> {
        if canonical_bytes.is_empty() {
            return Err(StorageValueError::Empty);
        }
        if canonical_bytes.len() > MAX_CATALOG_BUNDLE_BYTES {
            return Err(StorageValueError::LimitExceeded);
        }
        Ok(Self {
            lineage,
            contract_version,
            bundle_hash,
            canonical_bytes,
        })
    }

    /// Returns the exact contract lineage.
    #[must_use]
    pub const fn lineage(&self) -> &ContractLineage {
        &self.lineage
    }

    /// Returns the immutable application contract version.
    #[must_use]
    pub const fn contract_version(&self) -> ContractVersion {
        self.contract_version
    }

    /// Returns the hash of the exact canonical bytes.
    #[must_use]
    pub const fn bundle_hash(&self) -> ContractBundleHash {
        self.bundle_hash
    }

    /// Borrows the complete compiler-produced canonical bundle bytes.
    #[must_use]
    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical_bytes
    }
}

impl fmt::Debug for StoredContractBundleV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StoredContractBundleV1")
            .field("lineage", &self.lineage)
            .field("contract_version", &self.contract_version)
            .field("bundle_hash", &self.bundle_hash)
            .field("canonical_bytes", &"[REDACTED]")
            .field("canonical_length", &self.canonical_bytes.len())
            .finish()
    }
}

/// Exact durable pointer to the contract accepted for new invocations.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ActiveCatalogPointerV1 {
    lineage: ContractLineage,
    contract_version: ContractVersion,
    bundle_hash: ContractBundleHash,
}

impl ActiveCatalogPointerV1 {
    /// Constructs an exact immutable bundle pointer.
    #[must_use]
    pub const fn new(
        lineage: ContractLineage,
        contract_version: ContractVersion,
        bundle_hash: ContractBundleHash,
    ) -> Self {
        Self {
            lineage,
            contract_version,
            bundle_hash,
        }
    }

    /// Constructs a pointer from one stored bundle.
    #[must_use]
    pub fn from_bundle(bundle: &StoredContractBundleV1) -> Self {
        Self::new(
            bundle.lineage.clone(),
            bundle.contract_version,
            bundle.bundle_hash,
        )
    }

    /// Returns the exact contract lineage.
    #[must_use]
    pub const fn lineage(&self) -> &ContractLineage {
        &self.lineage
    }

    /// Returns the active application contract version.
    #[must_use]
    pub const fn contract_version(&self) -> ContractVersion {
        self.contract_version
    }

    /// Returns the exact active bundle hash.
    #[must_use]
    pub const fn bundle_hash(&self) -> ContractBundleHash {
        self.bundle_hash
    }

    /// Returns whether this pointer names the supplied immutable bundle.
    #[must_use]
    pub fn matches_bundle(&self, bundle: &StoredContractBundleV1) -> bool {
        self.lineage == bundle.lineage
            && self.contract_version == bundle.contract_version
            && self.bundle_hash == bundle.bundle_hash
    }
}

/// Pre-sequence typed deployment transition prepared for the coordinator.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogActivationIntentV1 {
    expected_active_version: Option<ContractVersion>,
    bundle: StoredContractBundleV1,
    request_id: RequestId,
    principal: AuditPrincipalV1,
    timestamp: Timestamp,
    approval_id: Option<ApprovalId>,
}

impl CatalogActivationIntentV1 {
    /// Constructs a complete bounded expected-version activation request.
    #[must_use]
    pub const fn new(
        expected_active_version: Option<ContractVersion>,
        bundle: StoredContractBundleV1,
        request_id: RequestId,
        principal: AuditPrincipalV1,
        timestamp: Timestamp,
        approval_id: Option<ApprovalId>,
    ) -> Self {
        Self {
            expected_active_version,
            bundle,
            request_id,
            principal,
            timestamp,
            approval_id,
        }
    }

    /// Returns the expected prior active version, including expected absence.
    #[must_use]
    pub const fn expected_active_version(&self) -> Option<ContractVersion> {
        self.expected_active_version
    }

    /// Returns the immutable bundle to persist and activate.
    #[must_use]
    pub const fn bundle(&self) -> &StoredContractBundleV1 {
        &self.bundle
    }

    /// Returns the administration invocation identity.
    #[must_use]
    pub const fn request_id(&self) -> RequestId {
        self.request_id
    }

    /// Returns the authenticated administration principal.
    #[must_use]
    pub const fn principal(&self) -> &AuditPrincipalV1 {
        &self.principal
    }

    /// Returns the coordinator-supplied administration timestamp.
    #[must_use]
    pub const fn timestamp(&self) -> Timestamp {
        self.timestamp
    }

    /// Returns the optional validated approval identity.
    #[must_use]
    pub const fn approval_id(&self) -> Option<&ApprovalId> {
        self.approval_id.as_ref()
    }

    /// Returns the exact requested post-image pointer.
    #[must_use]
    pub fn requested_active(&self) -> ActiveCatalogPointerV1 {
        ActiveCatalogPointerV1::from_bundle(&self.bundle)
    }
}

/// Durable catalog-administration record in the shared sequence space.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredCatalogAdministrationV1 {
    administration_sequence: AdministrationSequence,
    request_id: RequestId,
    timestamp: Timestamp,
    principal: AuditPrincipalV1,
    previous_active: Option<ActiveCatalogPointerV1>,
    activated: ActiveCatalogPointerV1,
    approval_id: Option<ApprovalId>,
}

impl StoredCatalogAdministrationV1 {
    /// Reconstructs an exact durable administration record without fabricating
    /// the separately stored contract bundle.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub const fn from_stored_parts(
        administration_sequence: AdministrationSequence,
        request_id: RequestId,
        timestamp: Timestamp,
        principal: AuditPrincipalV1,
        previous_active: Option<ActiveCatalogPointerV1>,
        activated: ActiveCatalogPointerV1,
        approval_id: Option<ApprovalId>,
    ) -> Self {
        Self {
            administration_sequence,
            request_id,
            timestamp,
            principal,
            previous_active,
            activated,
            approval_id,
        }
    }

    /// Lowers one successful CAS using the transaction-current prior pointer.
    pub fn from_committed_intent(
        administration_sequence: AdministrationSequence,
        intent: &CatalogActivationIntentV1,
        previous_active: Option<ActiveCatalogPointerV1>,
    ) -> Result<Self, StorageValueError> {
        if previous_active
            .as_ref()
            .map(ActiveCatalogPointerV1::contract_version)
            != intent.expected_active_version
        {
            return Err(StorageValueError::IdentityMismatch);
        }
        Ok(Self::from_stored_parts(
            administration_sequence,
            intent.request_id,
            intent.timestamp,
            intent.principal.clone(),
            previous_active,
            intent.requested_active(),
            intent.approval_id.clone(),
        ))
    }

    /// Returns the assigned total administration order.
    #[must_use]
    pub const fn administration_sequence(&self) -> AdministrationSequence {
        self.administration_sequence
    }

    /// Returns the catalog administration request identity.
    #[must_use]
    pub const fn request_id(&self) -> RequestId {
        self.request_id
    }

    /// Returns the coordinator-observed activation timestamp.
    #[must_use]
    pub const fn timestamp(&self) -> Timestamp {
        self.timestamp
    }

    /// Returns the authenticated activation principal.
    #[must_use]
    pub const fn principal(&self) -> &AuditPrincipalV1 {
        &self.principal
    }

    /// Returns the active pointer installed by this record.
    #[must_use]
    pub const fn activated(&self) -> &ActiveCatalogPointerV1 {
        &self.activated
    }

    /// Returns the transaction-current prior pointer replaced by the operation.
    #[must_use]
    pub const fn previous_active(&self) -> Option<&ActiveCatalogPointerV1> {
        self.previous_active.as_ref()
    }

    /// Returns the optional policy-validated approval identity.
    #[must_use]
    pub const fn approval_id(&self) -> Option<&ApprovalId> {
        self.approval_id.as_ref()
    }
}

/// Closed atomic catalog activation result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CatalogActivationResult {
    /// Bundle, active pointer, audit, and sequence committed together.
    Activated {
        /// Newly active pointer.
        active: ActiveCatalogPointerV1,
        /// Assigned administration sequence.
        administration_sequence: AdministrationSequence,
    },
    /// The exact requested pointer was already active; no write occurred.
    AlreadyActive {
        /// Existing exact active pointer.
        active: ActiveCatalogPointerV1,
        /// Original administration sequence that activated it.
        administration_sequence: AdministrationSequence,
    },
    /// Transaction-current active version/absence differed from the expectation.
    ExpectedActiveVersionMismatch {
        /// Actual current version, or absence before first deployment.
        actual: Option<ContractVersion>,
    },
    /// The immutable identity already exists with different bundle bytes.
    BundleConflict,
}

/// Bounded immutable-bundle and active-pointer reads.
pub trait CatalogRepository {
    /// Reads the current active pointer, with normal absence before deployment.
    fn read_active_catalog(&self) -> Result<Option<ActiveCatalogPointerV1>, StorageError>;

    /// Reads one immutable bundle by lineage and application version.
    fn read_contract_bundle(
        &self,
        lineage: &ContractLineage,
        contract_version: ContractVersion,
    ) -> Result<Option<StoredContractBundleV1>, StorageError>;

    /// Resolves exact permanent evidence for a migrated successor edge.
    ///
    /// Repositories predating contract migration have no such evidence. The
    /// default preserves that fail-closed absence without burdening read-only
    /// test repositories or non-migrating backends.
    fn read_contract_migration_edge(
        &self,
        _predecessor: ContractBundleHash,
    ) -> Result<Option<StoredContractMigrationEdgeV1>, StorageError> {
        Ok(None)
    }
}

/// Coordinator-only typed catalog activation transition.
pub trait CatalogAdministrationRepository {
    /// Atomically persists the immutable bundle, compares the expected active
    /// version/absence, switches the pointer, appends its administration record,
    /// and advances the allocator.
    ///
    /// Implementations apply this fixed precedence without allocating a sequence:
    /// conflicting bytes for an existing immutable identity return
    /// [`CatalogActivationResult::BundleConflict`]; an exact already-active
    /// requested pointer then returns its original administration sequence even
    /// when the retry's expected version is stale; only then is expected version
    /// compared with transaction-current state. A new activation records that
    /// actual prior pointer, never a caller-supplied reconstruction.
    fn activate_catalog(
        &mut self,
        intent: &CatalogActivationIntentV1,
    ) -> Result<CatalogActivationResult, StorageError>;
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use riffdb_types::{ActorId, ActorKind, CapabilityId};

    use super::*;

    fn lineage() -> ContractLineage {
        ContractLineage::new("budget").expect("lineage")
    }

    fn uuid_v7_bytes() -> [u8; 16] {
        [
            0x01, 0x8f, 0x00, 0x00, 0x00, 0x00, 0x70, 0x01, 0x80, 0x02, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x03,
        ]
    }

    fn principal() -> AuditPrincipalV1 {
        AuditPrincipalV1::new(
            ActorId::new("maintainer").expect("actor"),
            ActorKind::Human,
            CapabilityId::from_bytes(uuid_v7_bytes()).expect("capability ID"),
            NonZeroU64::MIN,
        )
    }

    #[test]
    fn active_pointer_repeats_the_exact_bundle_identity() {
        let bundle = StoredContractBundleV1::new(
            lineage(),
            ContractVersion::new(1).expect("nonzero"),
            ContractBundleHash::from_bytes([7; 32]),
            vec![1, 2, 3],
        )
        .expect("bundle");
        let pointer = ActiveCatalogPointerV1::from_bundle(&bundle);
        assert!(pointer.matches_bundle(&bundle));
    }

    #[test]
    fn bundle_bytes_are_bounded_and_redacted() {
        let bundle = StoredContractBundleV1::new(
            lineage(),
            ContractVersion::new(1).expect("nonzero"),
            ContractBundleHash::from_bytes([7; 32]),
            b"secret source bytes".to_vec(),
        )
        .expect("bundle");
        assert!(!format!("{bundle:?}").contains("secret source bytes"));
        assert_eq!(
            StoredContractBundleV1::new(
                lineage(),
                ContractVersion::new(1).expect("nonzero"),
                ContractBundleHash::from_bytes([7; 32]),
                Vec::new(),
            ),
            Err(StorageValueError::Empty)
        );
    }

    #[test]
    fn committed_catalog_audit_uses_transaction_current_prior_pointer() {
        let previous = ActiveCatalogPointerV1::new(
            lineage(),
            ContractVersion::new(1).expect("version"),
            ContractBundleHash::from_bytes([1; 32]),
        );
        let next = StoredContractBundleV1::new(
            lineage(),
            ContractVersion::new(2).expect("version"),
            ContractBundleHash::from_bytes([2; 32]),
            vec![2],
        )
        .expect("bundle");
        let intent = CatalogActivationIntentV1::new(
            Some(ContractVersion::new(1).expect("version")),
            next,
            RequestId::from_bytes(uuid_v7_bytes()).expect("request ID"),
            principal(),
            Timestamp::new(1, 0).expect("timestamp"),
            None,
        );

        let stored = StoredCatalogAdministrationV1::from_committed_intent(
            AdministrationSequence::first(),
            &intent,
            Some(previous.clone()),
        )
        .expect("matching CAS");
        assert_eq!(stored.previous_active(), Some(&previous));

        let reconstructed = StoredCatalogAdministrationV1::from_stored_parts(
            stored.administration_sequence(),
            stored.request_id(),
            stored.timestamp(),
            stored.principal().clone(),
            stored.previous_active().cloned(),
            stored.activated().clone(),
            stored.approval_id().cloned(),
        );
        assert_eq!(reconstructed, stored);
        assert_eq!(
            StoredCatalogAdministrationV1::from_committed_intent(
                AdministrationSequence::first(),
                &intent,
                None,
            ),
            Err(StorageValueError::IdentityMismatch)
        );
    }
}
