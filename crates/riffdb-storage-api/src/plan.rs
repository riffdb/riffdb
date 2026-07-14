//! Exact historical executable-plan identity used at every durable boundary.

use riffdb_types::{CommandId, ContractBundleHash, ContractLineage, ContractVersion, PlanHash};

/// The complete immutable identity of one executable command plan.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ExecutablePlanRef {
    contract_lineage: ContractLineage,
    contract_version: ContractVersion,
    contract_bundle_hash: ContractBundleHash,
    command_id: CommandId,
    command_plan_hash: PlanHash,
}

impl ExecutablePlanRef {
    /// Constructs an exact five-field plan reference.
    #[must_use]
    pub const fn new(
        contract_lineage: ContractLineage,
        contract_version: ContractVersion,
        contract_bundle_hash: ContractBundleHash,
        command_id: CommandId,
        command_plan_hash: PlanHash,
    ) -> Self {
        Self {
            contract_lineage,
            contract_version,
            contract_bundle_hash,
            command_id,
            command_plan_hash,
        }
    }

    /// Borrows the exact contract lineage.
    #[must_use]
    pub const fn contract_lineage(&self) -> &ContractLineage {
        &self.contract_lineage
    }

    /// Returns the exact application contract version.
    #[must_use]
    pub const fn contract_version(&self) -> ContractVersion {
        self.contract_version
    }

    /// Returns the exact immutable bundle hash.
    #[must_use]
    pub const fn contract_bundle_hash(&self) -> ContractBundleHash {
        self.contract_bundle_hash
    }

    /// Returns the stable command identity.
    #[must_use]
    pub const fn command_id(&self) -> CommandId {
        self.command_id
    }

    /// Returns the exact command-plan hash.
    #[must_use]
    pub const fn command_plan_hash(&self) -> PlanHash {
        self.command_plan_hash
    }

    pub(crate) fn semantic_bytes(&self) -> Option<usize> {
        self.contract_lineage
            .as_bytes()
            .len()
            .checked_add(4 + 8 + 32 + 4 + 32)
    }
}
