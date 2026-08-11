//! Engine-neutral durable ownership for exact application-installation campaigns.

use std::fmt;

pub use riffdb_types::MAX_APPLICATION_INSTALLATION_CAMPAIGN_STATE_BYTES;
use riffdb_types::{
    ApplicationInstallationCampaignId, ApplicationInstallationPlanHash, ContractLineage,
};

use crate::{StorageError, StorageValueError};

/// Result of one exact durable campaign compare-and-swap.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplicationInstallationCampaignWriteResultV1 {
    /// The exact replacement became durable.
    Applied,
    /// The exact replacement was already durable; no write was needed.
    Unchanged,
    /// Retained state did not match the caller's exact predecessor.
    CompareMismatch,
}

/// Opaque canonical application-owned state bound to one immutable campaign identity.
#[derive(Clone, Eq, PartialEq)]
pub struct StoredApplicationInstallationCampaignV1 {
    campaign_id: ApplicationInstallationCampaignId,
    contract_lineage: ContractLineage,
    plan_hash: ApplicationInstallationPlanHash,
    canonical_state: Vec<u8>,
}

impl StoredApplicationInstallationCampaignV1 {
    /// Constructs one bounded durable campaign record.
    pub fn new(
        campaign_id: ApplicationInstallationCampaignId,
        contract_lineage: ContractLineage,
        plan_hash: ApplicationInstallationPlanHash,
        canonical_state: Vec<u8>,
    ) -> Result<Self, StorageValueError> {
        if canonical_state.is_empty() {
            return Err(StorageValueError::Empty);
        }
        if canonical_state.len() > MAX_APPLICATION_INSTALLATION_CAMPAIGN_STATE_BYTES {
            return Err(StorageValueError::LimitExceeded);
        }
        Ok(Self {
            campaign_id,
            contract_lineage,
            plan_hash,
            canonical_state,
        })
    }

    /// Caller-stable campaign identity.
    #[must_use]
    pub const fn campaign_id(&self) -> ApplicationInstallationCampaignId {
        self.campaign_id
    }

    /// Protected contract lineage used for observation authorization.
    #[must_use]
    pub const fn contract_lineage(&self) -> &ContractLineage {
        &self.contract_lineage
    }

    /// Immutable exact installation-plan identity.
    #[must_use]
    pub const fn plan_hash(&self) -> ApplicationInstallationPlanHash {
        self.plan_hash
    }

    /// Canonical application-owned plan-plus-progress bytes.
    #[must_use]
    pub fn canonical_state(&self) -> &[u8] {
        &self.canonical_state
    }
}

impl fmt::Debug for StoredApplicationInstallationCampaignV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StoredApplicationInstallationCampaignV1")
            .field("campaign_id", &self.campaign_id)
            .field("contract_lineage", &self.contract_lineage)
            .field("plan_hash", &self.plan_hash)
            .field("canonical_state", &"[REDACTED]")
            .finish()
    }
}

/// Narrow engine-neutral repository for resumable installation campaigns.
///
/// The repository accepts only opaque, bounded, identity-bound records and an
/// exact predecessor. It deliberately exposes no transaction or callback.
pub trait ApplicationInstallationCampaignRepository {
    /// Reads one exact campaign by caller-stable identity.
    fn read_application_installation_campaign(
        &self,
        campaign_id: ApplicationInstallationCampaignId,
    ) -> Result<Option<StoredApplicationInstallationCampaignV1>, StorageError>;

    /// Atomically replaces the exact predecessor, recovering duplicate retries.
    fn compare_and_swap_application_installation_campaign(
        &mut self,
        expected: Option<&StoredApplicationInstallationCampaignV1>,
        replacement: &StoredApplicationInstallationCampaignV1,
    ) -> Result<ApplicationInstallationCampaignWriteResultV1, StorageError>;
}
