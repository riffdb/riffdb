//! Catalog-owned query-module validation and activation preparation.

use std::fmt;

use riffdb_query_module::{QueryModule, QueryModuleCandidate, QueryModuleErrorKind};
use riffdb_storage_api::{
    ActiveQueryModulePointerV1, AuditPrincipalV1, QueryModuleActivationIntentV1,
    QueryModuleActiveExpectationV1, StoredQueryModuleV1,
};
use riffdb_types::{ApprovalId, QueryModuleHash, RequestId, Timestamp};

use crate::ValidatedContractBundle;

/// Closed redaction-safe query-module catalog failure.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum QueryModuleCatalogError {
    /// Query source or its compiled module is invalid.
    InvalidModule,
    /// Module content is not pinned to the supplied exact contract.
    ContractMismatch,
    /// Canonical bytes and their asserted identity disagree.
    IdentityMismatch,
}

impl QueryModuleCatalogError {
    /// Fixed safe diagnostic text.
    #[must_use]
    pub const fn safe_message(self) -> &'static str {
        match self {
            Self::InvalidModule => "query module is invalid",
            Self::ContractMismatch => "query module contract does not match",
            Self::IdentityMismatch => "query module identity does not match",
        }
    }
}

impl fmt::Display for QueryModuleCatalogError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.safe_message())
    }
}

impl std::error::Error for QueryModuleCatalogError {}

/// An immutable module recompiled at the catalog trust boundary.
#[derive(Clone)]
pub struct ValidatedQueryModule(QueryModule);

impl ValidatedQueryModule {
    /// Compiles a source candidate against one exact validated contract.
    pub fn compile(
        candidate: QueryModuleCandidate,
        contract: &ValidatedContractBundle,
    ) -> Result<Self, QueryModuleCatalogError> {
        QueryModule::compile(candidate, contract.bundle())
            .map(Self)
            .map_err(map_module_error)
    }

    /// Recompiles opaque durable bytes and checks every repeated identity.
    pub fn from_stored(
        stored: &StoredQueryModuleV1,
        contract: &ValidatedContractBundle,
    ) -> Result<Self, QueryModuleCatalogError> {
        let module = QueryModule::decode_and_validate(stored.canonical_bytes(), contract.bundle())
            .map_err(map_module_error)?;
        if module.name() != stored.module_name()
            || module.version() != stored.module_version()
            || module.identity() != stored.module_hash()
            || module.contract_lineage() != stored.contract_lineage()
            || module.contract_version() != stored.contract_version()
            || module.contract_hash() != stored.contract_bundle_hash()
        {
            return Err(QueryModuleCatalogError::IdentityMismatch);
        }
        Ok(Self(module))
    }

    /// Complete executable checked module.
    #[must_use]
    pub const fn module(&self) -> &QueryModule {
        &self.0
    }

    /// Immutable content identity.
    #[must_use]
    pub const fn identity(&self) -> QueryModuleHash {
        self.0.identity()
    }

    /// IR-opaque durable representation.
    pub fn to_stored(&self) -> Result<StoredQueryModuleV1, QueryModuleCatalogError> {
        StoredQueryModuleV1::new(
            self.0.name().clone(),
            self.0.version(),
            self.0.identity(),
            self.0.contract_lineage().clone(),
            self.0.contract_version(),
            self.0.contract_hash(),
            self.0.canonical_bytes().to_vec(),
        )
        .map_err(|_| QueryModuleCatalogError::InvalidModule)
    }
}

impl fmt::Debug for ValidatedQueryModule {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ValidatedQueryModule")
            .field("identity", &self.identity())
            .field("module", &"[CHECKED]")
            .finish()
    }
}

/// Move-ready catalog preparation for one atomic module activation.
#[derive(Clone, Debug)]
pub struct PreparedQueryModuleActivation {
    module: ValidatedQueryModule,
    expectation: QueryModuleActiveExpectationV1,
}

impl PreparedQueryModuleActivation {
    /// Prepares a candidate after comparing the caller's active-pointer CAS.
    #[must_use]
    pub const fn new(
        module: ValidatedQueryModule,
        expectation: QueryModuleActiveExpectationV1,
    ) -> Self {
        Self {
            module,
            expectation,
        }
    }

    /// Checked module.
    #[must_use]
    pub const fn module(&self) -> &ValidatedQueryModule {
        &self.module
    }

    /// Transaction-current storage expectation.
    #[must_use]
    pub const fn expectation(&self) -> QueryModuleActiveExpectationV1 {
        self.expectation
    }

    /// Exact requested post-image.
    #[must_use]
    pub fn requested_active(&self) -> ActiveQueryModulePointerV1 {
        let module = self.module.module();
        ActiveQueryModulePointerV1::new(
            module.name().clone(),
            module.version(),
            module.identity(),
            module.contract_lineage().clone(),
            module.contract_version(),
            module.contract_hash(),
        )
    }

    /// Adds coordinator-owned operational metadata and lowers to storage.
    pub fn into_storage_intent(
        self,
        request_id: RequestId,
        principal: AuditPrincipalV1,
        timestamp: Timestamp,
        approval_id: Option<ApprovalId>,
    ) -> Result<QueryModuleActivationIntentV1, QueryModuleCatalogError> {
        Ok(QueryModuleActivationIntentV1::new(
            self.expectation,
            self.module.to_stored()?,
            request_id,
            principal,
            timestamp,
            approval_id,
        ))
    }
}

fn map_module_error(error: riffdb_query_module::QueryModuleError) -> QueryModuleCatalogError {
    match error.kind() {
        QueryModuleErrorKind::ContractMismatch => QueryModuleCatalogError::ContractMismatch,
        QueryModuleErrorKind::IdentityMismatch => QueryModuleCatalogError::IdentityMismatch,
        QueryModuleErrorKind::InvalidName
        | QueryModuleErrorKind::LimitExceeded
        | QueryModuleErrorKind::DuplicateQuery
        | QueryModuleErrorKind::QueryNameMismatch
        | QueryModuleErrorKind::InvalidQuery
        | QueryModuleErrorKind::InvalidContract
        | QueryModuleErrorKind::InvalidEncoding
        | QueryModuleErrorKind::UnsupportedVersion => QueryModuleCatalogError::InvalidModule,
    }
}
