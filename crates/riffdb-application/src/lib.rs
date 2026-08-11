#![forbid(unsafe_code)]

//! Exact, resumable application installation campaigns.
//!
//! This crate owns the API-neutral plan, stage, observation, and receipt
//! vocabulary for application installation. It deliberately owns no storage,
//! transport, authority, filesystem access, or application callbacks.

use riffdb_types::ApplicationInstallationPlanHash;

/// Canonical schema version for exact application installation plans.
pub const APPLICATION_INSTALLATION_PLAN_SCHEMA_V1: u16 = 1;

/// Closed, dependency-ordered stages in one application installation campaign.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum InstallationStage {
    /// Validate exact local inputs without remote mutation.
    Preflight,
    /// Deploy or verify the exact contract successor.
    Contract,
    /// Run or verify the explicitly approved migration, when present.
    Migration,
    /// Deploy or verify exact named-query modules.
    QueryModules,
    /// Deploy or verify exact reactive modules.
    ReactiveModules,
    /// Provision or reconcile exact application roles.
    Roles,
    /// Rotate or verify application credentials without exposing their bytes.
    Credentials,
    /// Prove every required public driver against the installed identity.
    DriverProof,
    /// Run bounded command-based seed batches and retain only their receipts.
    Seeds,
    /// Seal the terminal redacted installation receipt.
    Receipt,
}

impl InstallationStage {
    /// Every stage in the only valid execution order.
    pub const ALL: [Self; 10] = [
        Self::Preflight,
        Self::Contract,
        Self::Migration,
        Self::QueryModules,
        Self::ReactiveModules,
        Self::Roles,
        Self::Credentials,
        Self::DriverProof,
        Self::Seeds,
        Self::Receipt,
    ];
}

/// Exact immutable identity attached to every campaign observation.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct InstallationPlanIdentity(ApplicationInstallationPlanHash);

impl InstallationPlanIdentity {
    /// Wraps the compiler-derived plan hash.
    #[must_use]
    pub const fn new(hash: ApplicationInstallationPlanHash) -> Self {
        Self(hash)
    }

    /// Returns the exact compiler-derived plan hash.
    #[must_use]
    pub const fn hash(self) -> ApplicationInstallationPlanHash {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::{APPLICATION_INSTALLATION_PLAN_SCHEMA_V1, InstallationStage};

    #[test]
    fn installation_stages_are_closed_and_dependency_ordered() {
        assert_eq!(APPLICATION_INSTALLATION_PLAN_SCHEMA_V1, 1);
        assert_eq!(InstallationStage::ALL.len(), 10);
        assert_eq!(InstallationStage::ALL[0], InstallationStage::Preflight);
        assert_eq!(InstallationStage::ALL[9], InstallationStage::Receipt);
    }
}
