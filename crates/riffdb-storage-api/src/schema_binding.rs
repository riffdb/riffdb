//! IR-opaque durable ownership of persisted key schemas.

use riffdb_types::{ContractBundleHash, ContractLineage, ContractVersion};

use crate::{ExecutablePlanRef, StorageValueError};

/// Exact immutable bundle whose key schema owns one persisted key post-image.
///
/// The entity or index owner ID is retained by the surrounding record. This
/// value selects the exact retained bundle without importing or interpreting IR.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DurableKeySchemaBindingV1 {
    lineage: ContractLineage,
    contract_version: ContractVersion,
    bundle_hash: ContractBundleHash,
}

impl DurableKeySchemaBindingV1 {
    /// Constructs an exact IR-opaque retained-bundle reference.
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

    /// Derives the only binding accepted for post-images from this plan.
    #[must_use]
    pub fn from_plan(plan: &ExecutablePlanRef) -> Self {
        Self::new(
            plan.contract_lineage().clone(),
            plan.contract_version(),
            plan.contract_bundle_hash(),
        )
    }

    /// Borrows the exact contract lineage.
    #[must_use]
    pub const fn lineage(&self) -> &ContractLineage {
        &self.lineage
    }

    /// Returns the exact contract version.
    #[must_use]
    pub const fn contract_version(&self) -> ContractVersion {
        self.contract_version
    }

    /// Returns the immutable bundle hash.
    #[must_use]
    pub const fn bundle_hash(&self) -> ContractBundleHash {
        self.bundle_hash
    }

    /// Returns whether this binding is exactly the executing plan's bundle.
    #[must_use]
    pub fn matches_plan(&self, plan: &ExecutablePlanRef) -> bool {
        self.lineage == *plan.contract_lineage()
            && self.contract_version == plan.contract_version()
            && self.bundle_hash == plan.contract_bundle_hash()
    }

    pub(crate) fn semantic_bytes(&self) -> Result<usize, StorageValueError> {
        self.lineage
            .as_bytes()
            .len()
            .checked_add(4 + 8 + 32)
            .ok_or(StorageValueError::SizeOverflow)
    }
}
