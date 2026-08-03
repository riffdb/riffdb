//! Catalog-owned reactive-module compilation and publication preparation.

use std::fmt;
use std::sync::Arc;

use riffdb_query_module::{
    ReactiveModuleCompilationError, ReactiveModulePlanV1, canonicalize_reactive_source,
    compile_reactive_source, decode_and_validate_reactive_module,
    reactive_module_query_dependencies,
};
use riffdb_storage_api::{
    AuditPrincipalV1, ReactiveModulePublicationIntentV1, StoredReactiveModuleV1,
};
use riffdb_types::{ApprovalId, QueryModuleHash, ReactiveModuleHash, RequestId, Timestamp};

use crate::{ValidatedContractBundle, ValidatedQueryModule};

/// Closed redaction-safe reactive-module catalog failure.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ReactiveModuleCatalogError {
    /// Source or compiled reactive IR is invalid.
    InvalidModule,
    /// The candidate does not bind the supplied exact contract and query modules.
    DependencyMismatch,
    /// Canonical source, artifact, and asserted identities disagree.
    IdentityMismatch,
}

impl ReactiveModuleCatalogError {
    /// Fixed public-safe diagnostic text.
    #[must_use]
    pub const fn safe_message(self) -> &'static str {
        match self {
            Self::InvalidModule => "reactive module is invalid",
            Self::DependencyMismatch => "reactive module dependencies do not match",
            Self::IdentityMismatch => "reactive module identity does not match",
        }
    }
}

impl fmt::Display for ReactiveModuleCatalogError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.safe_message())
    }
}

impl std::error::Error for ReactiveModuleCatalogError {}

/// An immutable reactive module compiled at the catalog trust boundary.
#[derive(Clone)]
pub struct ValidatedReactiveModule {
    plan: Arc<ReactiveModulePlanV1>,
    canonical_source: Arc<str>,
    query_module_hashes: Arc<[QueryModuleHash]>,
}

impl ValidatedReactiveModule {
    /// Compiles source against one exact contract and checked query catalog.
    pub fn compile(
        source: &str,
        contract: &ValidatedContractBundle,
        query_modules: &[ValidatedQueryModule],
    ) -> Result<Self, ReactiveModuleCatalogError> {
        let canonical_source = canonicalize_reactive_source(source).map_err(map_error)?;
        let modules = query_modules
            .iter()
            .map(|module| module.module().clone())
            .collect::<Vec<_>>();
        let plan = compile_reactive_source(&canonical_source, contract.bundle(), &modules)
            .map_err(map_error)?;
        let dependencies = reactive_module_query_dependencies(&plan);
        Ok(Self {
            plan: Arc::new(plan),
            canonical_source: Arc::from(canonical_source),
            query_module_hashes: Arc::from(dependencies),
        })
    }

    /// Recompiles durable source and verifies every repeated identity and dependency.
    pub fn from_stored(
        stored: &StoredReactiveModuleV1,
        contract: &ValidatedContractBundle,
        query_modules: &[ValidatedQueryModule],
    ) -> Result<Self, ReactiveModuleCatalogError> {
        let source = std::str::from_utf8(stored.canonical_source())
            .map_err(|_| ReactiveModuleCatalogError::InvalidModule)?;
        let canonical_source = canonicalize_reactive_source(source).map_err(map_error)?;
        if canonical_source.as_bytes() != stored.canonical_source() {
            return Err(ReactiveModuleCatalogError::IdentityMismatch);
        }
        let modules = query_modules
            .iter()
            .map(|module| module.module().clone())
            .collect::<Vec<_>>();
        let plan = decode_and_validate_reactive_module(
            stored.canonical_module(),
            &canonical_source,
            contract.bundle(),
            &modules,
        )
        .map_err(map_error)?;
        let dependencies = reactive_module_query_dependencies(&plan);
        if plan.name() != stored.module_name()
            || plan.version() != stored.module_version()
            || plan.identity() != stored.module_hash()
            || plan.contract_lineage() != stored.contract_lineage()
            || plan.contract_version() != stored.contract_version()
            || plan.contract_hash() != stored.contract_bundle_hash()
            || plan.source_hash() != stored.source_hash()
            || dependencies != stored.query_module_hashes()
        {
            return Err(ReactiveModuleCatalogError::IdentityMismatch);
        }
        Ok(Self {
            plan: Arc::new(plan),
            canonical_source: Arc::from(canonical_source),
            query_module_hashes: Arc::from(dependencies),
        })
    }

    /// Complete checked executable plan.
    #[must_use]
    pub fn plan(&self) -> &ReactiveModulePlanV1 {
        &self.plan
    }

    /// Immutable module identity.
    #[must_use]
    pub fn identity(&self) -> ReactiveModuleHash {
        self.plan.identity()
    }

    /// Lowers the checked module to its IR-opaque durable representation.
    pub fn to_stored(&self) -> Result<StoredReactiveModuleV1, ReactiveModuleCatalogError> {
        StoredReactiveModuleV1::new(
            self.plan.name().to_owned(),
            self.plan.version(),
            self.plan.identity(),
            self.plan.contract_lineage().clone(),
            self.plan.contract_version(),
            self.plan.contract_hash(),
            self.plan.source_hash(),
            self.query_module_hashes.to_vec(),
            self.canonical_source.as_bytes().to_vec(),
            self.plan.canonical_bytes().to_vec(),
        )
        .map_err(|_| ReactiveModuleCatalogError::InvalidModule)
    }
}

impl fmt::Debug for ValidatedReactiveModule {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ValidatedReactiveModule")
            .field("identity", &self.identity())
            .field("plan", &"[CHECKED]")
            .finish()
    }
}

/// Move-ready catalog preparation for immutable publication.
#[derive(Clone, Debug)]
pub struct PreparedReactiveModulePublication {
    module: ValidatedReactiveModule,
}

impl PreparedReactiveModulePublication {
    /// Prepares one already checked immutable module.
    #[must_use]
    pub const fn new(module: ValidatedReactiveModule) -> Self {
        Self { module }
    }

    /// Borrows the checked module.
    #[must_use]
    pub const fn module(&self) -> &ValidatedReactiveModule {
        &self.module
    }

    /// Adds coordinator-owned operational metadata and lowers to storage.
    pub fn into_storage_intent(
        self,
        request_id: RequestId,
        principal: AuditPrincipalV1,
        timestamp: Timestamp,
        approval_id: Option<ApprovalId>,
    ) -> Result<ReactiveModulePublicationIntentV1, ReactiveModuleCatalogError> {
        Ok(ReactiveModulePublicationIntentV1::new(
            self.module.to_stored()?,
            request_id,
            principal,
            timestamp,
            approval_id,
        ))
    }
}

fn map_error(error: ReactiveModuleCompilationError) -> ReactiveModuleCatalogError {
    match error {
        ReactiveModuleCompilationError::IdentityMismatch => {
            ReactiveModuleCatalogError::IdentityMismatch
        }
        ReactiveModuleCompilationError::InvalidQueryCatalog => {
            ReactiveModuleCatalogError::DependencyMismatch
        }
        ReactiveModuleCompilationError::Syntax(_)
        | ReactiveModuleCompilationError::Semantic(_)
        | ReactiveModuleCompilationError::ArtifactLimit => {
            ReactiveModuleCatalogError::InvalidModule
        }
    }
}
