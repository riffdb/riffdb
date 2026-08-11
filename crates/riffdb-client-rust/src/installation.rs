//! Immutable exact installation submissions for uncertainty-safe retry.

use std::fmt;

use riffdb_application::{
    ApplicationInstallationPlan, InstallationCampaignError, InstallationDriver,
    InstallationStageEvidence, InstalledSeedEvidence,
};
use riffdb_proto::v1;
use riffdb_types::{ApplicationInstallationCampaignId, RequestId};

/// One immutable exact application-installation start or resume submission.
#[derive(Clone)]
pub struct StartApplicationInstallation {
    campaign_id: ApplicationInstallationCampaignId,
    plan: ApplicationInstallationPlan,
    external_completion: Option<ExternalInstallationCompletion>,
}

#[derive(Clone)]
enum ExternalInstallationCompletion {
    DriverProof(Vec<InstallationDriver>),
    Seeds(Vec<InstalledSeedEvidence>),
}

impl StartApplicationInstallation {
    /// Retains the canonical plan under one caller-stable campaign identity.
    #[must_use]
    pub const fn new(
        campaign_id: ApplicationInstallationCampaignId,
        plan: ApplicationInstallationPlan,
    ) -> Self {
        Self {
            campaign_id,
            plan,
            external_completion: None,
        }
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

    /// Attaches proof that every plan-declared public driver observed the
    /// installed identity through its first-party path.
    pub fn with_driver_proof(
        mut self,
        drivers: Vec<InstallationDriver>,
    ) -> Result<Self, InstallationCampaignError> {
        let completion = InstallationStageEvidence::DriverProof(drivers.clone());
        completion.validate_for(&self.plan)?;
        self.external_completion = Some(ExternalInstallationCompletion::DriverProof(drivers));
        Ok(self)
    }

    /// Attaches bounded terminal counters from the plan-declared ordinary
    /// command seed batches.
    pub fn with_seed_receipts(
        mut self,
        seeds: Vec<InstalledSeedEvidence>,
    ) -> Result<Self, InstallationCampaignError> {
        let completion = InstallationStageEvidence::Seeds(seeds.clone());
        completion.validate_for(&self.plan)?;
        self.external_completion = Some(ExternalInstallationCompletion::Seeds(seeds));
        Ok(self)
    }

    pub(crate) fn request(&self, request_id: RequestId) -> v1::StartApplicationInstallationRequest {
        v1::StartApplicationInstallationRequest {
            request_id: request_id.into_bytes().to_vec(),
            campaign_id: self.campaign_id.into_bytes().to_vec(),
            canonical_plan: self.plan.canonical_bytes().to_vec(),
            external_completion: self
                .external_completion
                .as_ref()
                .map(external_completion_to_proto),
        }
    }
}

fn external_completion_to_proto(
    completion: &ExternalInstallationCompletion,
) -> v1::ApplicationInstallationExternalCompletion {
    use v1::application_installation_external_completion::Completion;

    let completion = match completion {
        ExternalInstallationCompletion::DriverProof(drivers) => {
            Completion::DriverProof(v1::ApplicationInstallationDriverProof {
                drivers: drivers
                    .iter()
                    .map(|driver| match driver {
                        InstallationDriver::Rust => v1::ApplicationInstallationDriver::Rust,
                        InstallationDriver::TypeScript => {
                            v1::ApplicationInstallationDriver::Typescript
                        }
                        InstallationDriver::Go => v1::ApplicationInstallationDriver::Go,
                        InstallationDriver::Python => v1::ApplicationInstallationDriver::Python,
                    } as i32)
                    .collect(),
            })
        }
        ExternalInstallationCompletion::Seeds(seeds) => {
            Completion::SeedReceipts(v1::ApplicationInstallationSeedReceipts {
                seeds: seeds
                    .iter()
                    .map(|seed| v1::ApplicationInstallationSeedReceipt {
                        name: seed.name().as_str().to_owned(),
                        content_hash: seed.content_hash().as_bytes().to_vec(),
                        succeeded: seed.succeeded(),
                        replayed: seed.replayed(),
                    })
                    .collect(),
            })
        }
    };
    v1::ApplicationInstallationExternalCompletion {
        completion: Some(completion),
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

    #[test]
    fn external_completion_is_limited_to_exact_driver_or_seed_evidence() {
        let start = StartApplicationInstallation::new(
            ApplicationInstallationCampaignId::from_bytes(uuid(4)).expect("campaign"),
            plan(),
        )
        .with_driver_proof(vec![
            InstallationDriver::Rust,
            InstallationDriver::TypeScript,
        ])
        .expect("exact driver proof");
        let request = start.request(RequestId::from_bytes(uuid(5)).expect("request"));
        assert!(matches!(
            request.external_completion,
            Some(v1::ApplicationInstallationExternalCompletion {
                completion: Some(
                    v1::application_installation_external_completion::Completion::DriverProof(_)
                )
            })
        ));
    }
}
