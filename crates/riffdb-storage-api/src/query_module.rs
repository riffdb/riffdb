//! IR-blind immutable query-module and active-pointer persistence semantics.

use std::fmt;

use riffdb_types::{
    AdministrationSequence, ApprovalId, ContractBundleHash, ContractLineage, ContractVersion,
    QueryModuleHash, QueryModuleName, QueryModuleVersion, RequestId, Timestamp,
};

use crate::{AuditPrincipalV1, MAX_QUERY_MODULE_BYTES, StorageError, StorageValueError};

/// Opaque canonical bytes and exact identity for one immutable query module.
#[derive(Clone, Eq, PartialEq)]
pub struct StoredQueryModuleV1 {
    module_name: QueryModuleName,
    module_version: QueryModuleVersion,
    module_hash: QueryModuleHash,
    contract_lineage: ContractLineage,
    contract_version: ContractVersion,
    contract_bundle_hash: ContractBundleHash,
    canonical_bytes: Vec<u8>,
}

impl StoredQueryModuleV1 {
    /// Constructs one bounded IR-opaque module record.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        module_name: QueryModuleName,
        module_version: QueryModuleVersion,
        module_hash: QueryModuleHash,
        contract_lineage: ContractLineage,
        contract_version: ContractVersion,
        contract_bundle_hash: ContractBundleHash,
        canonical_bytes: Vec<u8>,
    ) -> Result<Self, StorageValueError> {
        if canonical_bytes.is_empty() {
            return Err(StorageValueError::Empty);
        }
        if canonical_bytes.len() > MAX_QUERY_MODULE_BYTES {
            return Err(StorageValueError::LimitExceeded);
        }
        Ok(Self {
            module_name,
            module_version,
            module_hash,
            contract_lineage,
            contract_version,
            contract_bundle_hash,
            canonical_bytes,
        })
    }

    /// Exact module name.
    #[must_use]
    pub const fn module_name(&self) -> &QueryModuleName {
        &self.module_name
    }

    /// Positive module version.
    #[must_use]
    pub const fn module_version(&self) -> QueryModuleVersion {
        self.module_version
    }

    /// Immutable content identity.
    #[must_use]
    pub const fn module_hash(&self) -> QueryModuleHash {
        self.module_hash
    }

    /// Exact contract lineage.
    #[must_use]
    pub const fn contract_lineage(&self) -> &ContractLineage {
        &self.contract_lineage
    }

    /// Exact contract version.
    #[must_use]
    pub const fn contract_version(&self) -> ContractVersion {
        self.contract_version
    }

    /// Exact contract bundle hash.
    #[must_use]
    pub const fn contract_bundle_hash(&self) -> ContractBundleHash {
        self.contract_bundle_hash
    }

    /// Complete canonical query-module bytes.
    #[must_use]
    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical_bytes
    }
}

impl fmt::Debug for StoredQueryModuleV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StoredQueryModuleV1")
            .field("module_name", &self.module_name)
            .field("module_version", &self.module_version)
            .field("module_hash", &self.module_hash)
            .field("contract_lineage", &self.contract_lineage)
            .field("contract_version", &self.contract_version)
            .field("contract_bundle_hash", &self.contract_bundle_hash)
            .field("canonical_bytes", &"[REDACTED]")
            .field("canonical_length", &self.canonical_bytes.len())
            .finish()
    }
}

/// Exact durable pointer selected for named execution.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ActiveQueryModulePointerV1 {
    module_name: QueryModuleName,
    module_version: QueryModuleVersion,
    module_hash: QueryModuleHash,
    contract_lineage: ContractLineage,
    contract_version: ContractVersion,
    contract_bundle_hash: ContractBundleHash,
}

impl ActiveQueryModulePointerV1 {
    /// Constructs one exact active pointer.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub const fn new(
        module_name: QueryModuleName,
        module_version: QueryModuleVersion,
        module_hash: QueryModuleHash,
        contract_lineage: ContractLineage,
        contract_version: ContractVersion,
        contract_bundle_hash: ContractBundleHash,
    ) -> Self {
        Self {
            module_name,
            module_version,
            module_hash,
            contract_lineage,
            contract_version,
            contract_bundle_hash,
        }
    }

    /// Constructs the exact pointer for a stored module.
    #[must_use]
    pub fn from_module(module: &StoredQueryModuleV1) -> Self {
        Self::new(
            module.module_name.clone(),
            module.module_version,
            module.module_hash,
            module.contract_lineage.clone(),
            module.contract_version,
            module.contract_bundle_hash,
        )
    }

    /// Exact module name.
    #[must_use]
    pub const fn module_name(&self) -> &QueryModuleName {
        &self.module_name
    }

    /// Positive module version.
    #[must_use]
    pub const fn module_version(&self) -> QueryModuleVersion {
        self.module_version
    }

    /// Immutable module identity.
    #[must_use]
    pub const fn module_hash(&self) -> QueryModuleHash {
        self.module_hash
    }

    /// Exact contract lineage.
    #[must_use]
    pub const fn contract_lineage(&self) -> &ContractLineage {
        &self.contract_lineage
    }

    /// Exact contract version.
    #[must_use]
    pub const fn contract_version(&self) -> ContractVersion {
        self.contract_version
    }

    /// Exact contract bundle hash.
    #[must_use]
    pub const fn contract_bundle_hash(&self) -> ContractBundleHash {
        self.contract_bundle_hash
    }

    /// Whether this pointer names the complete supplied immutable record.
    #[must_use]
    pub fn matches_module(&self, module: &StoredQueryModuleV1) -> bool {
        self == &Self::from_module(module)
    }
}

/// Transaction-current active-pointer expectation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryModuleActiveExpectationV1 {
    /// No comparison; atomically replace any current pointer.
    Any,
    /// The exact contract has no active module.
    Absent,
    /// The exact contract has this active module identity.
    Exact(QueryModuleHash),
}

/// Checked query-module activation input submitted only by the coordinator.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryModuleActivationIntentV1 {
    expectation: QueryModuleActiveExpectationV1,
    module: StoredQueryModuleV1,
    request_id: RequestId,
    principal: AuditPrincipalV1,
    timestamp: Timestamp,
    approval_id: Option<ApprovalId>,
}

impl QueryModuleActivationIntentV1 {
    /// Groups a complete expected-pointer activation request.
    #[must_use]
    pub const fn new(
        expectation: QueryModuleActiveExpectationV1,
        module: StoredQueryModuleV1,
        request_id: RequestId,
        principal: AuditPrincipalV1,
        timestamp: Timestamp,
        approval_id: Option<ApprovalId>,
    ) -> Self {
        Self {
            expectation,
            module,
            request_id,
            principal,
            timestamp,
            approval_id,
        }
    }

    /// Transaction-current pointer expectation.
    #[must_use]
    pub const fn expectation(&self) -> QueryModuleActiveExpectationV1 {
        self.expectation
    }

    /// Immutable candidate module.
    #[must_use]
    pub const fn module(&self) -> &StoredQueryModuleV1 {
        &self.module
    }

    /// Invocation identity.
    #[must_use]
    pub const fn request_id(&self) -> RequestId {
        self.request_id
    }

    /// Authenticated administration principal.
    #[must_use]
    pub const fn principal(&self) -> &AuditPrincipalV1 {
        &self.principal
    }

    /// Coordinator-observed timestamp.
    #[must_use]
    pub const fn timestamp(&self) -> Timestamp {
        self.timestamp
    }

    /// Optional validated approval.
    #[must_use]
    pub const fn approval_id(&self) -> Option<&ApprovalId> {
        self.approval_id.as_ref()
    }

    /// Exact requested post-image.
    #[must_use]
    pub fn requested_active(&self) -> ActiveQueryModulePointerV1 {
        ActiveQueryModulePointerV1::from_module(&self.module)
    }
}

/// Durable query-module activation administration record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredQueryModuleAdministrationV1 {
    administration_sequence: AdministrationSequence,
    request_id: RequestId,
    timestamp: Timestamp,
    principal: AuditPrincipalV1,
    previous_active: Option<ActiveQueryModulePointerV1>,
    activated: ActiveQueryModulePointerV1,
    approval_id: Option<ApprovalId>,
}

impl StoredQueryModuleAdministrationV1 {
    /// Reconstructs a checked durable record from independently decoded parts.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub const fn from_stored_parts(
        administration_sequence: AdministrationSequence,
        request_id: RequestId,
        timestamp: Timestamp,
        principal: AuditPrincipalV1,
        previous_active: Option<ActiveQueryModulePointerV1>,
        activated: ActiveQueryModulePointerV1,
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

    /// Lowers an atomic transition using its transaction-current pre-image.
    #[must_use]
    pub fn from_committed_intent(
        administration_sequence: AdministrationSequence,
        intent: &QueryModuleActivationIntentV1,
        previous_active: Option<ActiveQueryModulePointerV1>,
    ) -> Self {
        Self::from_stored_parts(
            administration_sequence,
            intent.request_id,
            intent.timestamp,
            intent.principal.clone(),
            previous_active,
            intent.requested_active(),
            intent.approval_id.clone(),
        )
    }

    /// Assigned shared administration sequence.
    #[must_use]
    pub const fn administration_sequence(&self) -> AdministrationSequence {
        self.administration_sequence
    }

    /// Invocation identity.
    #[must_use]
    pub const fn request_id(&self) -> RequestId {
        self.request_id
    }

    /// Coordinator-observed timestamp.
    #[must_use]
    pub const fn timestamp(&self) -> Timestamp {
        self.timestamp
    }

    /// Authenticated principal.
    #[must_use]
    pub const fn principal(&self) -> &AuditPrincipalV1 {
        &self.principal
    }

    /// Transaction-current prior pointer.
    #[must_use]
    pub const fn previous_active(&self) -> Option<&ActiveQueryModulePointerV1> {
        self.previous_active.as_ref()
    }

    /// Installed exact pointer.
    #[must_use]
    pub const fn activated(&self) -> &ActiveQueryModulePointerV1 {
        &self.activated
    }

    /// Optional validated approval.
    #[must_use]
    pub const fn approval_id(&self) -> Option<&ApprovalId> {
        self.approval_id.as_ref()
    }
}

/// Closed atomic activation result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryModuleActivationResult {
    /// Module, pointer, audit, and allocator committed together.
    Activated {
        /// Newly active pointer.
        active: ActiveQueryModulePointerV1,
        /// Assigned shared administration sequence.
        administration_sequence: AdministrationSequence,
    },
    /// Exact module is already active and no write occurred.
    AlreadyActive {
        /// Existing exact active pointer.
        active: ActiveQueryModulePointerV1,
        /// Original activation sequence.
        administration_sequence: AdministrationSequence,
    },
    /// Transaction-current active pointer differs from the expectation.
    ExpectedActiveMismatch {
        /// Actual module identity, or absence.
        actual: Option<QueryModuleHash>,
    },
    /// Same module name/version is retained with different content.
    ModuleVersionConflict,
    /// Exact contract bundle is not retained hash-equal.
    ContractUnavailable,
}

/// Read-only immutable module and active-pointer access.
pub trait QueryModuleRepository {
    /// Reads one module by immutable identity.
    fn read_query_module(
        &self,
        module_hash: QueryModuleHash,
    ) -> Result<Option<StoredQueryModuleV1>, StorageError>;

    /// Reads the active module for one exact contract identity.
    fn read_active_query_module(
        &self,
        lineage: &ContractLineage,
        contract_version: ContractVersion,
        contract_bundle_hash: ContractBundleHash,
    ) -> Result<Option<ActiveQueryModulePointerV1>, StorageError>;
}

/// Coordinator-only atomic query-module activation transition.
pub trait QueryModuleAdministrationRepository {
    /// Persists an immutable module and atomically updates its exact-contract
    /// active pointer, shared administration audit, and allocator.
    fn activate_query_module(
        &mut self,
        intent: &QueryModuleActivationIntentV1,
    ) -> Result<QueryModuleActivationResult, StorageError>;
}
