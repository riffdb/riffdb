//! Checked immutable bundles and exact executable-plan resolution.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;

use riffdb_contract_ir::{
    BUNDLE_FORMAT_VERSION_V1, BUNDLE_FORMAT_VERSION_V2, BUNDLE_FORMAT_VERSION_V3,
    BUNDLE_FORMAT_VERSION_V4, BUNDLE_FORMAT_VERSION_V5, BUNDLE_FORMAT_VERSION_V6,
    BUNDLE_FORMAT_VERSION_V7, BUNDLE_FORMAT_VERSION_V8, BUNDLE_FORMAT_VERSION_V9,
    BUNDLE_FORMAT_VERSION_V10, CommandPlan, ContractBundle, EXECUTABLE_IR_VERSION_V1,
    EXECUTABLE_IR_VERSION_V2, EXECUTABLE_IR_VERSION_V3, EXECUTABLE_IR_VERSION_V4,
    EXECUTABLE_IR_VERSION_V5, EXECUTABLE_IR_VERSION_V6, EXECUTABLE_IR_VERSION_V7,
    EXECUTABLE_IR_VERSION_V8, EXECUTABLE_IR_VERSION_V9, EXECUTABLE_IR_VERSION_V10,
    GRAMMAR_VERSION_V1, GRAMMAR_VERSION_V2, GRAMMAR_VERSION_V3, GRAMMAR_VERSION_V4,
    GRAMMAR_VERSION_V5, GRAMMAR_VERSION_V6, GRAMMAR_VERSION_V7, GRAMMAR_VERSION_V8,
    GRAMMAR_VERSION_V9, GRAMMAR_VERSION_V10, MCP_COMMAND_NAME_REGISTRY_VERSION_V2,
    McpCommandToolNameV2,
};
use riffdb_storage_api::{
    ActiveCatalogPointerV1, CatalogRepository, ExecutablePlanRef, StoredContractBundleV1,
};
use riffdb_types::{CommandId, ContractBundleHash, ContractLineage, ContractVersion, PlanHash};

use crate::lineage::LineageMaterializationProof;
use crate::{CatalogError, CatalogErrorKind};

/// Shared enum display-name table for one published contract schema.
///
/// Built once when the validated bundle is decoded and shared for the lifetime
/// of that catalog publication (bounded by the catalog's existing caches).
pub type ContractEnumVariantNames = Arc<BTreeMap<(u32, u32), String>>;

struct ValidatedContractBundleInner {
    bundle: ContractBundle,
    enum_variant_names: ContractEnumVariantNames,
}

/// A canonical bundle that has passed catalog-owned activation revalidation.
#[derive(Clone)]
pub struct ValidatedContractBundle(Arc<ValidatedContractBundleInner>);

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
        let enum_variant_names = Arc::new(build_enum_variant_names(&bundle));
        Ok(Self(Arc::new(ValidatedContractBundleInner {
            bundle,
            enum_variant_names,
        })))
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
        &self.0.bundle
    }

    /// Exact contract lineage.
    #[must_use]
    pub fn lineage(&self) -> &ContractLineage {
        self.0.bundle.lineage()
    }

    /// Application contract version.
    #[must_use]
    pub fn contract_version(&self) -> ContractVersion {
        self.0.bundle.contract_version()
    }

    /// Hash of the canonical immutable bytes.
    #[must_use]
    pub fn bundle_hash(&self) -> ContractBundleHash {
        self.0.bundle.bundle_hash()
    }

    /// Shared enum display names for this publication (pointer-stable while held).
    #[must_use]
    pub fn enum_variant_names(&self) -> &ContractEnumVariantNames {
        &self.0.enum_variant_names
    }

    /// Constructs the IR-opaque storage representation without operational metadata.
    pub fn to_stored(&self) -> Result<StoredContractBundleV1, CatalogError> {
        StoredContractBundleV1::new(
            self.lineage().clone(),
            self.contract_version(),
            self.bundle_hash(),
            self.0.bundle.canonical_bytes().to_vec(),
        )
        .map_err(|_| CatalogError::new(CatalogErrorKind::InvalidBundle))
    }

    pub(crate) fn resolve_plan_with_proof(
        &self,
        reference: &ExecutablePlanRef,
        lineage_proof: Arc<LineageMaterializationProof>,
        executing_ordinal: u16,
    ) -> Result<ResolvedExecutablePlan, CatalogError> {
        let ordinal_names_self = lineage_proof
            .exact_member(self.contract_version(), self.bundle_hash())
            .is_some_and(|(ordinal, bundle)| {
                ordinal == executing_ordinal
                    && bundle.lineage() == self.lineage()
                    && bundle.bundle().canonical_bytes() == self.bundle().canonical_bytes()
            });
        if self.lineage() != reference.contract_lineage()
            || self.contract_version() != reference.contract_version()
            || self.bundle_hash() != reference.contract_bundle_hash()
            || !ordinal_names_self
        {
            return Err(CatalogError::new(CatalogErrorKind::UnknownExecutablePlan));
        }
        let plan = self
            .0
            .bundle
            .command(reference.command_id())
            .filter(|plan| plan.plan_hash() == reference.command_plan_hash())
            .ok_or_else(|| CatalogError::new(CatalogErrorKind::UnknownExecutablePlan))?;
        Ok(ResolvedExecutablePlan {
            reference: reference.clone(),
            plan: Arc::new(plan.clone()),
            bundle: self.clone(),
            lineage_proof,
            executing_ordinal,
        })
    }
}

fn build_enum_variant_names(bundle: &ContractBundle) -> BTreeMap<(u32, u32), String> {
    bundle
        .schema()
        .enums()
        .iter()
        .flat_map(|enumeration| {
            enumeration.variants().iter().map(move |variant| {
                (
                    (enumeration.id().get(), variant.id().get()),
                    variant.name().to_owned(),
                )
            })
        })
        .collect()
}

impl fmt::Debug for ValidatedContractBundle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ValidatedContractBundle")
            .field("lineage", &"[REDACTED]")
            .field("contract_version", &self.contract_version())
            .field("bundle_hash", &"[REDACTED]")
            .field("canonical_bytes", &"[REDACTED]")
            .finish()
    }
}

/// One exact checked historical executable command plan.
#[derive(Clone)]
pub struct ResolvedExecutablePlan {
    reference: ExecutablePlanRef,
    plan: Arc<CommandPlan>,
    bundle: ValidatedContractBundle,
    lineage_proof: Arc<LineageMaterializationProof>,
    executing_ordinal: u16,
}

impl ResolvedExecutablePlan {
    /// Complete immutable durable plan reference.
    #[must_use]
    pub const fn reference(&self) -> &ExecutablePlanRef {
        &self.reference
    }

    /// Checked executable command plan.
    #[must_use]
    pub fn plan(&self) -> &CommandPlan {
        self.plan.as_ref()
    }

    /// Bundle whose canonical bytes own this plan.
    #[must_use]
    pub const fn bundle(&self) -> &ValidatedContractBundle {
        &self.bundle
    }

    #[allow(dead_code)] // Consumed by catalog-owned snapshot normalization in the next slice.
    pub(crate) fn lineage_proof(&self) -> &Arc<LineageMaterializationProof> {
        &self.lineage_proof
    }

    #[allow(dead_code)] // Consumed by catalog-owned snapshot normalization in the next slice.
    pub(crate) const fn executing_ordinal(&self) -> u16 {
        self.executing_ordinal
    }
}

impl fmt::Debug for ResolvedExecutablePlan {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResolvedExecutablePlan")
            .field("reference", &"[REDACTED]")
            .field("plan", &"[CHECKED]")
            .field("lineage_proof", &"[CHECKED]")
            .field("lineage_bundle_count", &self.lineage_proof.bundle_count())
            .field("executing_ordinal", &self.executing_ordinal)
            .finish()
    }
}

/// Published active-catalog contents. Immutable after construction; shared via
/// [`ActiveCatalogSnapshot`]'s outer `Arc`.
struct ActiveCatalogPublished {
    pointer: ActiveCatalogPointerV1,
    bundle: ValidatedContractBundle,
    lineage_proof: Arc<LineageMaterializationProof>,
    active_plans: Arc<BTreeMap<CommandId, Arc<CommandPlan>>>,
}

/// Current active pointer paired with its exact checked immutable bundle.
///
/// Clone is an `Arc` increment: publication replaces the Arc; readers share one
/// snapshot until the next activation.
#[derive(Clone)]
pub struct ActiveCatalogSnapshot {
    published: Arc<ActiveCatalogPublished>,
}

impl ActiveCatalogSnapshot {
    /// Reads and validates the current active pointer and immutable bundle.
    pub fn read<R: CatalogRepository>(repository: &R) -> Result<Option<Self>, CatalogError> {
        let Some(pointer) = repository.read_active_catalog()? else {
            return Ok(None);
        };
        let lineage_proof = LineageMaterializationProof::load_active(repository, &pointer)
            .map_err(map_stored_lineage_error)?;
        let bundle = lineage_proof.terminal().clone();
        if bundle.lineage() != pointer.lineage()
            || bundle.contract_version() != pointer.contract_version()
            || bundle.bundle_hash() != pointer.bundle_hash()
        {
            return Err(CatalogError::new(
                CatalogErrorKind::InvalidHistoricalEvidence,
            ));
        }
        let active_plans = bundle
            .bundle()
            .commands()
            .iter()
            .map(|plan| (plan.command_id(), Arc::new(plan.clone())))
            .collect();
        Ok(Some(Self {
            published: Arc::new(ActiveCatalogPublished {
                pointer,
                bundle,
                lineage_proof,
                // Architecture/source checks keep this field shape visible.
                active_plans: Arc::new(active_plans),
            }),
        }))
    }

    /// True when both values share one published snapshot (Arc identity).
    #[must_use]
    pub fn same_publication_as(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.published, &other.published)
    }

    /// Exact durable active pointer.
    #[must_use]
    pub fn pointer(&self) -> &ActiveCatalogPointerV1 {
        &self.published.pointer
    }

    /// Checked active bundle.
    #[must_use]
    pub fn bundle(&self) -> &ValidatedContractBundle {
        &self.published.bundle
    }

    pub(crate) fn lineage_proof(&self) -> &Arc<LineageMaterializationProof> {
        &self.published.lineage_proof
    }

    /// Resolves one exact plan through this already validated active lineage
    /// proof without rereading or redecoding the catalog.
    pub(crate) fn resolve_plan(
        &self,
        reference: &ExecutablePlanRef,
    ) -> Result<ResolvedExecutablePlan, CatalogError> {
        let (ordinal, bundle) = self
            .published
            .lineage_proof
            .exact_member(
                reference.contract_version(),
                reference.contract_bundle_hash(),
            )
            .ok_or_else(|| CatalogError::new(CatalogErrorKind::UnknownExecutablePlan))?;
        if usize::from(ordinal) + 1 == self.published.lineage_proof.bundle_count()
            && let Some(plan) = self.published.active_plans.get(&reference.command_id())
            && plan.plan_hash() == reference.command_plan_hash()
        {
            return Ok(ResolvedExecutablePlan {
                reference: reference.clone(),
                plan: Arc::clone(plan),
                bundle: bundle.clone(),
                lineage_proof: Arc::clone(&self.published.lineage_proof),
                executing_ordinal: ordinal,
            });
        }
        bundle.resolve_plan_with_proof(
            reference,
            Arc::clone(&self.published.lineage_proof),
            ordinal,
        )
    }

    /// Resolves a command owned by this exact active bundle.
    pub fn resolve_active_command(
        &self,
        command_id: CommandId,
        command_plan_hash: PlanHash,
    ) -> Result<ResolvedExecutablePlan, CatalogError> {
        self.resolve_plan(&ExecutablePlanRef::new(
            self.pointer().lineage().clone(),
            self.pointer().contract_version(),
            self.pointer().bundle_hash(),
            command_id,
            command_plan_hash,
        ))
    }
}

fn map_stored_lineage_error(error: CatalogError) -> CatalogError {
    match error.kind() {
        CatalogErrorKind::ActiveCatalogMismatch
        | CatalogErrorKind::InvalidHistoricalEvidence
        | CatalogErrorKind::LineageBundleCountLimit
        | CatalogErrorKind::LineageCanonicalBytesLimit
        | CatalogErrorKind::LineageMaterializationProofLimit => {
            CatalogError::new(CatalogErrorKind::InvalidHistoricalEvidence)
        }
        CatalogErrorKind::InvalidBundle
        | CatalogErrorKind::UnsupportedBundleVersion
        | CatalogErrorKind::InvalidCommandRegistry
        | CatalogErrorKind::IncompatibleContract
        | CatalogErrorKind::BundleIdentityConflict
        | CatalogErrorKind::UnknownExecutablePlan
        | CatalogErrorKind::InvalidHistoricalKey
        | CatalogErrorKind::Storage => error,
    }
}

impl fmt::Debug for ActiveCatalogSnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ActiveCatalogSnapshot")
            .field("pointer", &"[REDACTED]")
            .field("bundle", &"[CHECKED]")
            .field("lineage_proof", &"[CHECKED]")
            .finish()
    }
}

// Keep the field name visible for architecture boundary tests.
const _: () = {
    let _ = |published: &ActiveCatalogPublished| {
        let _: &Arc<LineageMaterializationProof> = &published.lineage_proof;
    };
};

/// Resolves a complete historical plan through the bounded catalog read port.
pub fn resolve_executable_plan<R: CatalogRepository>(
    repository: &R,
    reference: &ExecutablePlanRef,
) -> Result<ResolvedExecutablePlan, CatalogError> {
    let active = ActiveCatalogSnapshot::read(repository)?
        .ok_or_else(|| CatalogError::new(CatalogErrorKind::UnknownExecutablePlan))?;
    if active.pointer().lineage() != reference.contract_lineage() {
        return Err(CatalogError::new(CatalogErrorKind::UnknownExecutablePlan));
    }
    active.resolve_plan(reference)
}

fn validate_supported_versions(bundle: &ContractBundle) -> Result<(), CatalogError> {
    let versions = (
        bundle.format_version(),
        bundle.grammar_version(),
        bundle.ir_version(),
    );
    if !matches!(
        versions,
        (
            BUNDLE_FORMAT_VERSION_V1,
            GRAMMAR_VERSION_V1,
            EXECUTABLE_IR_VERSION_V1
        ) | (
            BUNDLE_FORMAT_VERSION_V2,
            GRAMMAR_VERSION_V2,
            EXECUTABLE_IR_VERSION_V2
        ) | (
            BUNDLE_FORMAT_VERSION_V3,
            GRAMMAR_VERSION_V3,
            EXECUTABLE_IR_VERSION_V3
        ) | (
            BUNDLE_FORMAT_VERSION_V4,
            GRAMMAR_VERSION_V4,
            EXECUTABLE_IR_VERSION_V4
        ) | (
            BUNDLE_FORMAT_VERSION_V5,
            GRAMMAR_VERSION_V5,
            EXECUTABLE_IR_VERSION_V5
        ) | (
            BUNDLE_FORMAT_VERSION_V6,
            GRAMMAR_VERSION_V6,
            EXECUTABLE_IR_VERSION_V6
        ) | (
            BUNDLE_FORMAT_VERSION_V7,
            GRAMMAR_VERSION_V7,
            EXECUTABLE_IR_VERSION_V7
        ) | (
            BUNDLE_FORMAT_VERSION_V8,
            GRAMMAR_VERSION_V8,
            EXECUTABLE_IR_VERSION_V8
        ) | (
            BUNDLE_FORMAT_VERSION_V9,
            GRAMMAR_VERSION_V9,
            EXECUTABLE_IR_VERSION_V9
        ) | (
            BUNDLE_FORMAT_VERSION_V10,
            GRAMMAR_VERSION_V10,
            EXECUTABLE_IR_VERSION_V10
        )
    ) {
        return Err(CatalogError::new(
            CatalogErrorKind::UnsupportedBundleVersion,
        ));
    }
    Ok(())
}

fn validate_command_registry(bundle: &ContractBundle) -> Result<(), CatalogError> {
    let registry = bundle.mcp_command_names();
    let application_commands = bundle
        .commands()
        .iter()
        .filter(|command| !command.is_reimport())
        .collect::<Vec<_>>();
    if registry.version() != MCP_COMMAND_NAME_REGISTRY_VERSION_V2
        || registry.lineage() != bundle.lineage()
        || registry.source_contract_name() != bundle.lineage().as_str()
        || registry.entries().len() != application_commands.len()
    {
        return Err(CatalogError::new(CatalogErrorKind::InvalidCommandRegistry));
    }

    let mut names = BTreeSet::new();
    for (entry, command) in registry.entries().iter().zip(application_commands) {
        if entry.command_id() != command.command_id()
            || entry.source_command_name() != command.name()
            || McpCommandToolNameV2::new_checked(
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn startup_reclassifies_only_broken_or_over_limit_active_lineage() {
        for kind in [
            CatalogErrorKind::ActiveCatalogMismatch,
            CatalogErrorKind::InvalidHistoricalEvidence,
            CatalogErrorKind::LineageBundleCountLimit,
            CatalogErrorKind::LineageCanonicalBytesLimit,
            CatalogErrorKind::LineageMaterializationProofLimit,
        ] {
            assert_eq!(
                map_stored_lineage_error(CatalogError::new(kind)).kind(),
                CatalogErrorKind::InvalidHistoricalEvidence
            );
        }

        for kind in [
            CatalogErrorKind::InvalidBundle,
            CatalogErrorKind::UnsupportedBundleVersion,
            CatalogErrorKind::InvalidCommandRegistry,
            CatalogErrorKind::IncompatibleContract,
            CatalogErrorKind::BundleIdentityConflict,
            CatalogErrorKind::UnknownExecutablePlan,
            CatalogErrorKind::InvalidHistoricalKey,
            CatalogErrorKind::Storage,
        ] {
            assert_eq!(
                map_stored_lineage_error(CatalogError::new(kind)).kind(),
                kind
            );
        }
    }
}
