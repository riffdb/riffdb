//! Immutable exact installation submissions for uncertainty-safe retry.

use std::fmt;

use riffdb_application::ApplicationInstallationPlan;
use riffdb_proto::v1;
use riffdb_types::{ApplicationInstallationCampaignId, RequestId};

/// One immutable exact application-installation start or resume submission.
#[derive(Clone)]
pub struct StartApplicationInstallation {
    campaign_id: ApplicationInstallationCampaignId,
    plan: ApplicationInstallationPlan,
}

impl StartApplicationInstallation {
    /// Retains the canonical plan under one caller-stable campaign identity.
    #[must_use]
    pub const fn new(
        campaign_id: ApplicationInstallationCampaignId,
        plan: ApplicationInstallationPlan,
    ) -> Self {
        Self { campaign_id, plan }
    }

    /// Caller-stable campaign identity used across retries and observation.
    #[must_use]
    pub const fn campaign_id(&self) -> ApplicationInstallationCampaignId {
        self.campaign_id
    }

    /// Exact content-addressed canonical plan.
    #[must_use]
    pub const fn plan(&self) -> &ApplicationInstallationPlan {
        &self.plan
    }

    pub(crate) fn request(&self, request_id: RequestId) -> v1::StartApplicationInstallationRequest {
        v1::StartApplicationInstallationRequest {
            request_id: request_id.into_bytes().to_vec(),
            campaign_id: self.campaign_id.into_bytes().to_vec(),
            canonical_plan: self.plan.canonical_bytes().to_vec(),
        }
    }
}

impl fmt::Debug for StartApplicationInstallation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StartApplicationInstallation")
            .field("campaign_id", &self.campaign_id)
            .field("plan_hash", &self.plan.identity())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn uuid(seed: u8) -> [u8; 16] {
        let mut bytes = [seed; 16];
        bytes[6] = 0x70 | (seed & 0x0f);
        bytes[8] = 0x80 | (seed & 0x3f);
        bytes
    }

    fn plan() -> ApplicationInstallationPlan {
        ApplicationInstallationPlan::decode_canonical(include_bytes!(
            "../../../fixtures/installation/application-installation-plan-v1.json"
        ))
        .expect("plan fixture")
    }

    #[test]
    fn retries_change_only_the_transport_request_id() {
        let start = StartApplicationInstallation::new(
            ApplicationInstallationCampaignId::from_bytes(uuid(1)).expect("campaign"),
            plan(),
        );
        let first = start.request(RequestId::from_bytes(uuid(2)).expect("request"));
        let second = start.request(RequestId::from_bytes(uuid(3)).expect("request"));
        assert_ne!(first.request_id, second.request_id);
        assert_eq!(first.campaign_id, second.campaign_id);
        assert_eq!(first.canonical_plan, second.canonical_plan);
    }
}
