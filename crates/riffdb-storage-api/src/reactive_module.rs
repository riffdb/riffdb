//! IR-blind immutable reactive-module publication semantics.

use std::fmt;

use riffdb_types::{
    AdministrationSequence, ApprovalId, ContractBundleHash, ContractLineage, ContractVersion,
    QueryModuleHash, ReactiveModuleHash, ReactiveSourceHash, RequestId, Timestamp,
};

use crate::{
    AuditPrincipalV1, MAX_REACTIVE_MODULE_BYTES, MAX_REACTIVE_MODULE_SOURCE_BYTES,
    MAX_REACTIVE_QUERY_MODULE_DEPENDENCIES, StorageError, StorageValueError,
};

/// Opaque canonical source and artifact bytes for one exact reactive module.
#[derive(Clone, Eq, PartialEq)]
pub struct StoredReactiveModuleV1 {
    module_name: String,
    module_version: u64,
    module_hash: ReactiveModuleHash,
    contract_lineage: ContractLineage,
    contract_version: ContractVersion,
    contract_bundle_hash: ContractBundleHash,
    source_hash: ReactiveSourceHash,
    query_module_hashes: Vec<QueryModuleHash>,
    canonical_source: Vec<u8>,
    canonical_module: Vec<u8>,
}

impl StoredReactiveModuleV1 {
    /// Constructs one bounded, dependency-complete, IR-opaque module record.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        module_name: String,
        module_version: u64,
        module_hash: ReactiveModuleHash,
        contract_lineage: ContractLineage,
        contract_version: ContractVersion,
        contract_bundle_hash: ContractBundleHash,
        source_hash: ReactiveSourceHash,
        mut query_module_hashes: Vec<QueryModuleHash>,
        canonical_source: Vec<u8>,
        canonical_module: Vec<u8>,
    ) -> Result<Self, StorageValueError> {
        if !valid_symbolic_name(&module_name)
            || module_version == 0
            || canonical_source.is_empty()
            || canonical_source.len() > MAX_REACTIVE_MODULE_SOURCE_BYTES
            || canonical_module.is_empty()
            || canonical_module.len() > MAX_REACTIVE_MODULE_BYTES
            || query_module_hashes.len() > MAX_REACTIVE_QUERY_MODULE_DEPENDENCIES
        {
            return Err(StorageValueError::LimitExceeded);
        }
        query_module_hashes.sort_unstable();
        if query_module_hashes
            .windows(2)
            .any(|pair| pair[0] == pair[1])
        {
            return Err(StorageValueError::Duplicate);
        }
        Ok(Self {
            module_name,
            module_version,
            module_hash,
            contract_lineage,
            contract_version,
            contract_bundle_hash,
            source_hash,
            query_module_hashes,
            canonical_source,
            canonical_module,
        })
    }

    /// Symbolic module name.
    #[must_use]
    pub fn module_name(&self) -> &str {
        &self.module_name
    }
    /// Positive source-declared version.
    #[must_use]
    pub const fn module_version(&self) -> u64 {
        self.module_version
    }
    /// Immutable artifact identity.
    #[must_use]
    pub const fn module_hash(&self) -> ReactiveModuleHash {
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
    /// Exact contract artifact identity.
    #[must_use]
    pub const fn contract_bundle_hash(&self) -> ContractBundleHash {
        self.contract_bundle_hash
    }
    /// Canonical source identity.
    #[must_use]
    pub const fn source_hash(&self) -> ReactiveSourceHash {
        self.source_hash
    }
    /// Exact query-module dependencies in hash order.
    #[must_use]
    pub fn query_module_hashes(&self) -> &[QueryModuleHash] {
        &self.query_module_hashes
    }
    /// Canonical UTF-8 source bytes.
    #[must_use]
    pub fn canonical_source(&self) -> &[u8] {
        &self.canonical_source
    }
    /// Canonical compiler artifact bytes.
    #[must_use]
    pub fn canonical_module(&self) -> &[u8] {
        &self.canonical_module
    }
}

impl fmt::Debug for StoredReactiveModuleV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StoredReactiveModuleV1")
            .field("module_name", &self.module_name)
            .field("module_version", &self.module_version)
            .field("module_hash", &self.module_hash)
            .field("contract_lineage", &self.contract_lineage)
            .field("contract_version", &self.contract_version)
            .field("contract_bundle_hash", &self.contract_bundle_hash)
            .field("source_hash", &self.source_hash)
            .field("query_module_hashes", &self.query_module_hashes)
            .field("canonical_source", &"[REDACTED]")
            .field("canonical_module", &"[REDACTED]")
            .finish()
    }
}

/// Checked publication submitted only by the commit-owned control plane.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReactiveModulePublicationIntentV1 {
    module: StoredReactiveModuleV1,
    request_id: RequestId,
    principal: AuditPrincipalV1,
    timestamp: Timestamp,
    approval_id: Option<ApprovalId>,
}

impl ReactiveModulePublicationIntentV1 {
    /// Groups one immutable publication and its coordinator-owned metadata.
    #[must_use]
    pub const fn new(
        module: StoredReactiveModuleV1,
        request_id: RequestId,
        principal: AuditPrincipalV1,
        timestamp: Timestamp,
        approval_id: Option<ApprovalId>,
    ) -> Self {
        Self {
            module,
            request_id,
            principal,
            timestamp,
            approval_id,
        }
    }
    /// Borrows the immutable module to publish.
    #[must_use]
    pub const fn module(&self) -> &StoredReactiveModuleV1 {
        &self.module
    }
    /// Returns the retry-stable administrative request identity.
    #[must_use]
    pub const fn request_id(&self) -> RequestId {
        self.request_id
    }
    /// Borrows the authenticated audit principal.
    #[must_use]
    pub const fn principal(&self) -> &AuditPrincipalV1 {
        &self.principal
    }
    /// Returns the coordinator-supplied administrative instant.
    #[must_use]
    pub const fn timestamp(&self) -> Timestamp {
        self.timestamp
    }
    /// Borrows the optional human approval identity.
    #[must_use]
    pub const fn approval_id(&self) -> Option<&ApprovalId> {
        self.approval_id.as_ref()
    }
}

/// Durable shared-administration proof for immutable publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredReactiveModuleAdministrationV1 {
    administration_sequence: AdministrationSequence,
    request_id: RequestId,
    timestamp: Timestamp,
    principal: AuditPrincipalV1,
    module_hash: ReactiveModuleHash,
    approval_id: Option<ApprovalId>,
}

impl StoredReactiveModuleAdministrationV1 {
    /// Reconstructs one decoded durable publication audit record.
    #[must_use]
    pub const fn from_stored_parts(
        administration_sequence: AdministrationSequence,
        request_id: RequestId,
        timestamp: Timestamp,
        principal: AuditPrincipalV1,
        module_hash: ReactiveModuleHash,
        approval_id: Option<ApprovalId>,
    ) -> Self {
        Self {
            administration_sequence,
            request_id,
            timestamp,
            principal,
            module_hash,
            approval_id,
        }
    }
    /// Builds the durable audit post-image for an assigned sequence.
    #[must_use]
    pub fn from_committed_intent(
        administration_sequence: AdministrationSequence,
        intent: &ReactiveModulePublicationIntentV1,
    ) -> Self {
        Self::from_stored_parts(
            administration_sequence,
            intent.request_id,
            intent.timestamp,
            intent.principal.clone(),
            intent.module.module_hash,
            intent.approval_id.clone(),
        )
    }
    /// Returns the shared administration sequence.
    #[must_use]
    pub const fn administration_sequence(&self) -> AdministrationSequence {
        self.administration_sequence
    }
    /// Returns the retry-stable request identity.
    #[must_use]
    pub const fn request_id(&self) -> RequestId {
        self.request_id
    }
    /// Returns the recorded administrative instant.
    #[must_use]
    pub const fn timestamp(&self) -> Timestamp {
        self.timestamp
    }
    /// Borrows the authenticated principal.
    #[must_use]
    pub const fn principal(&self) -> &AuditPrincipalV1 {
        &self.principal
    }
    /// Returns the published immutable module identity.
    #[must_use]
    pub const fn module_hash(&self) -> ReactiveModuleHash {
        self.module_hash
    }
    /// Borrows the optional human approval identity.
    #[must_use]
    pub const fn approval_id(&self) -> Option<&ApprovalId> {
        self.approval_id.as_ref()
    }
}

/// Closed atomic publication result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReactiveModulePublicationResult {
    /// A new immutable module and its audit record committed atomically.
    Published {
        /// Published module identity.
        module_hash: ReactiveModuleHash,
        /// Assigned shared administration sequence.
        administration_sequence: AdministrationSequence,
    },
    /// The exact immutable module already exists; no write occurred.
    ///
    /// The original publication's shared administration sequence is returned so
    /// an idempotent republish records a linked success naming that publication,
    /// exactly as the catalog and query-module already-active results do. A
    /// retained module always has exactly one publication record: the
    /// administration stream is append-only and contiguous, never pruned.
    AlreadyPublished {
        /// Existing module identity.
        module_hash: ReactiveModuleHash,
        /// Shared administration sequence of the original publication.
        administration_sequence: AdministrationSequence,
    },
    /// Another artifact already owns the declared name and version.
    ModuleVersionConflict,
    /// The exact contract dependency is not retained.
    ContractUnavailable,
    /// One exact query-module dependency is not retained.
    QueryModuleUnavailable {
        /// Missing query-module identity.
        module_hash: QueryModuleHash,
    },
}

/// Immutable reactive-module lookup.
pub trait ReactiveModuleRepository {
    /// Reads one exact immutable module without interpreting compiler IR.
    fn read_reactive_module(
        &self,
        module_hash: ReactiveModuleHash,
    ) -> Result<Option<StoredReactiveModuleV1>, StorageError>;
}

/// Coordinator-only immutable publication transition.
pub trait ReactiveModuleAdministrationRepository {
    /// Publishes one checked immutable module and shared audit record atomically.
    fn publish_reactive_module(
        &mut self,
        intent: &ReactiveModulePublicationIntentV1,
    ) -> Result<ReactiveModulePublicationResult, StorageError>;
}

fn valid_symbolic_name(value: &str) -> bool {
    let mut chars = value.chars();
    matches!(chars.next(), Some(first) if first.is_ascii_alphabetic())
        && value.len() <= 128
        && chars.all(|character| character.is_ascii_alphanumeric() || character == '_')
}
