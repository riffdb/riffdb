//! Checked immutable bundles and exact executable-plan resolution.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

use riffdb_contract_ir::{
    BUNDLE_FORMAT_VERSION_V1, CommandPlan, ContractBundle, EXECUTABLE_IR_VERSION_V1,
    GRAMMAR_VERSION_V1, MCP_COMMAND_NAME_REGISTRY_VERSION_V1, McpCommandToolNameV1,
};
use riffdb_storage_api::{
    ActiveCatalogPointerV1, CatalogRepository, ExecutablePlanRef, StoredContractBundleV1,
};
use riffdb_types::{ContractBundleHash, ContractLineage, ContractVersion};

use crate::{CatalogError, CatalogErrorKind};

/// A canonical bundle that has passed catalog-owned activation revalidation.
#[derive(Clone)]
pub struct ValidatedContractBundle(Arc<ContractBundle>);

impl ValidatedContractBundle {
    /// Re-decodes a checked compiler result at the catalog trust boundary.
    pub fn from_compiler_bundle(bundle: ContractBundle) -> Result<Self, CatalogError> {
        Self::decode(bundle.canonical_bytes())
    }

    /// Decodes exact canonical bytes and revalidates catalog-owned metadata.
    pub fn decode(bytes: &[u8]) -> Result<Self, CatalogError> {
        let bundle =
            ContractBundle::decode(bytes).map_err(|error| CatalogError::from_ir(&error))?;
        validate_supported_versions(&bundle)?;
        validate_command_registry(&bundle)?;
        Ok(Self(Arc::new(bundle)))
    }

    /// Validates storage's opaque identity against decoded canonical content.
    pub fn from_stored(stored: &StoredContractBundleV1) -> Result<Self, CatalogError> {
        let bundle = Self::decode(stored.canonical_bytes())?;
        if bundle.lineage() != stored.lineage()
            || bundle.contract_version() != stored.contract_version()
            || bundle.bundle_hash() != stored.bundle_hash()
        {
            return Err(CatalogError::new(CatalogErrorKind::BundleIdentityConflict));
        }
        Ok(bundle)
    }

    /// Borrows the complete checked immutable IR bundle.
    #[must_use]
    pub fn bundle(&self) -> &ContractBundle {
        &self.0
    }

    /// Exact contract lineage.
    #[must_use]
    pub fn lineage(&self) -> &ContractLineage {
        self.0.lineage()
    }

    /// Application contract version.
    #[must_use]
    pub fn contract_version(&self) -> ContractVersion {
        self.0.contract_version()
    }

    /// Hash of the canonical immutable bytes.
    #[must_use]
    pub fn bundle_hash(&self) -> ContractBundleHash {
        self.0.bundle_hash()
    }

    /// Constructs the IR-opaque storage representation without operational metadata.
    pub fn to_stored(&self) -> Result<StoredContractBundleV1, CatalogError> {
        StoredContractBundleV1::new(
            self.lineage().clone(),
            self.contract_version(),
            self.bundle_hash(),
            self.0.canonical_bytes().to_vec(),
        )
        .map_err(|_| CatalogError::new(CatalogErrorKind::InvalidBundle))
    }

    /// Resolves one command and verifies the complete historical plan identity.
    pub fn resolve_plan(
        &self,
        reference: &ExecutablePlanRef,
    ) -> Result<ResolvedExecutablePlan, CatalogError> {
        if self.lineage() != reference.contract_lineage()
            || self.contract_version() != reference.contract_version()
            || self.bundle_hash() != reference.contract_bundle_hash()
        {
            return Err(CatalogError::new(CatalogErrorKind::UnknownExecutablePlan));
        }
        let plan = self
            .0
            .command(reference.command_id())
            .filter(|plan| plan.plan_hash() == reference.command_plan_hash())
            .ok_or_else(|| CatalogError::new(CatalogErrorKind::UnknownExecutablePlan))?;
        Ok(ResolvedExecutablePlan {
            reference: reference.clone(),
            plan: plan.clone(),
            bundle: self.clone(),
        })
    }
}

impl fmt::Debug for ValidatedContractBundle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ValidatedContractBundle")
            .field("lineage", self.lineage())
            .field("contract_version", &self.contract_version())
            .field("bundle_hash", &self.bundle_hash())
            .field("canonical_bytes", &"[REDACTED]")
            .finish()
    }
}

/// One exact checked historical executable command plan.
#[derive(Clone)]
pub struct ResolvedExecutablePlan {
    reference: ExecutablePlanRef,
    plan: CommandPlan,
    bundle: ValidatedContractBundle,
}

impl ResolvedExecutablePlan {
    /// Complete immutable durable plan reference.
    #[must_use]
    pub const fn reference(&self) -> &ExecutablePlanRef {
        &self.reference
    }

    /// Checked executable command plan.
    #[must_use]
    pub const fn plan(&self) -> &CommandPlan {
        &self.plan
    }

    /// Bundle whose canonical bytes own this plan.
    #[must_use]
    pub const fn bundle(&self) -> &ValidatedContractBundle {
        &self.bundle
    }
}

impl fmt::Debug for ResolvedExecutablePlan {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResolvedExecutablePlan")
            .field("reference", &self.reference)
            .field("plan", &"[CHECKED]")
            .finish()
    }
}

/// Current active pointer paired with its exact checked immutable bundle.
#[derive(Clone, Debug)]
pub struct ActiveCatalogSnapshot {
    pointer: ActiveCatalogPointerV1,
    bundle: ValidatedContractBundle,
}

impl ActiveCatalogSnapshot {
    /// Reads and validates the current active pointer and immutable bundle.
    pub fn read<R: CatalogRepository>(repository: &R) -> Result<Option<Self>, CatalogError> {
        let Some(pointer) = repository.read_active_catalog()? else {
            return Ok(None);
        };
        let stored = repository
            .read_contract_bundle(pointer.lineage(), pointer.contract_version())?
            .ok_or_else(|| CatalogError::new(CatalogErrorKind::ActiveCatalogMismatch))?;
        let bundle = ValidatedContractBundle::from_stored(&stored)?;
        if !pointer.matches_bundle(&stored) {
            return Err(CatalogError::new(CatalogErrorKind::ActiveCatalogMismatch));
        }
        Ok(Some(Self { pointer, bundle }))
    }

    /// Exact durable active pointer.
    #[must_use]
    pub const fn pointer(&self) -> &ActiveCatalogPointerV1 {
        &self.pointer
    }

    /// Checked active bundle.
    #[must_use]
    pub const fn bundle(&self) -> &ValidatedContractBundle {
        &self.bundle
    }
}

/// Resolves a complete historical plan through the bounded catalog read port.
pub fn resolve_executable_plan<R: CatalogRepository>(
    repository: &R,
    reference: &ExecutablePlanRef,
) -> Result<ResolvedExecutablePlan, CatalogError> {
    let stored = repository
        .read_contract_bundle(reference.contract_lineage(), reference.contract_version())?
        .ok_or_else(|| CatalogError::new(CatalogErrorKind::UnknownExecutablePlan))?;
    ValidatedContractBundle::from_stored(&stored)?.resolve_plan(reference)
}

fn validate_supported_versions(bundle: &ContractBundle) -> Result<(), CatalogError> {
    if bundle.format_version() != BUNDLE_FORMAT_VERSION_V1
        || bundle.grammar_version() != GRAMMAR_VERSION_V1
        || bundle.ir_version() != EXECUTABLE_IR_VERSION_V1
    {
        return Err(CatalogError::new(
            CatalogErrorKind::UnsupportedBundleVersion,
        ));
    }
    Ok(())
}

fn validate_command_registry(bundle: &ContractBundle) -> Result<(), CatalogError> {
    let registry = bundle.mcp_command_names();
    if registry.version() != MCP_COMMAND_NAME_REGISTRY_VERSION_V1
        || registry.lineage() != bundle.lineage()
        || registry.source_contract_name() != bundle.lineage().as_str()
        || registry.entries().len() != bundle.commands().len()
    {
        return Err(CatalogError::new(CatalogErrorKind::InvalidCommandRegistry));
    }

    let mut names = BTreeSet::new();
    for (entry, command) in registry.entries().iter().zip(bundle.commands()) {
        if entry.command_id() != command.command_id()
            || entry.source_command_name() != command.name()
            || McpCommandToolNameV1::new_checked(
                registry.source_contract_name(),
                command.name(),
                entry.tool_name().as_str(),
            )
            .is_err()
            || !names.insert(entry.tool_name().as_str())
        {
            return Err(CatalogError::new(CatalogErrorKind::InvalidCommandRegistry));
        }
    }
    Ok(())
}
