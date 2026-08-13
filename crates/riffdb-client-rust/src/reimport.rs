//! Immutable operator-only reimport submissions for uncertainty-safe retry.

use std::fmt;

use riffdb_application::ApplicationPortabilityManifest;
use riffdb_proto::{PublicWireError, v1, validate_public_message};
use riffdb_types::{
    ApplicationInstallationCampaignId, CapabilityApplicationReimportScopeV1, ContractLineage,
    RequestId,
};

/// One immutable, exact application-reimport start submission.
#[derive(Clone, Eq, PartialEq)]
pub struct StartApplicationReimport {
    campaign_id: ApplicationInstallationCampaignId,
    lineage: ContractLineage,
    scope: CapabilityApplicationReimportScopeV1,
    portability_manifest: ApplicationPortabilityManifest,
    canonical_export_manifest_json: Vec<u8>,
    canonical_export_receipt_json: Vec<u8>,
}

impl StartApplicationReimport {
    /// Retains one exact portability source under a caller-stable campaign.
    ///
    /// The context-free public-message validator rejects malformed or
    /// oversized canonical documents here. The server remains authoritative
    /// for source hashes, database identity, scope, and portability semantics.
    pub fn new(
        campaign_id: ApplicationInstallationCampaignId,
        lineage: ContractLineage,
        scope: CapabilityApplicationReimportScopeV1,
        portability_manifest: ApplicationPortabilityManifest,
        canonical_export_manifest_json: Vec<u8>,
        canonical_export_receipt_json: Vec<u8>,
    ) -> Result<Self, PublicWireError> {
        let submission = Self {
            campaign_id,
            lineage,
            scope,
            portability_manifest,
            canonical_export_manifest_json,
            canonical_export_receipt_json,
        };
        validate_public_message(
            &submission.request(
                RequestId::from_bytes(campaign_id.into_bytes())
                    .map_err(|_| PublicWireError::InvalidUuidV7)?,
            ),
        )?;
        Ok(submission)
    }

    /// Caller-stable reimport campaign identity reused across retries.
    #[must_use]
    pub const fn campaign_id(&self) -> ApplicationInstallationCampaignId {
        self.campaign_id
    }

    /// Exact protected application lineage.
    #[must_use]
    pub const fn lineage(&self) -> &ContractLineage {
        &self.lineage
    }

    /// Exact requested Capability V7 reimport scope.
    #[must_use]
    pub const fn scope(&self) -> CapabilityApplicationReimportScopeV1 {
        self.scope
    }

    /// Exact adapter-owned portability manifest.
    #[must_use]
    pub const fn portability_manifest(&self) -> &ApplicationPortabilityManifest {
        &self.portability_manifest
    }

    pub(crate) fn request(&self, request_id: RequestId) -> v1::StartApplicationReimportRequest {
        let scope = match self.scope {
            CapabilityApplicationReimportScopeV1::PrincipalFiltered => {
                v1::CapabilityApplicationReimportScope::PrincipalFiltered
            }
            CapabilityApplicationReimportScopeV1::WholeApplication => {
                v1::CapabilityApplicationReimportScope::WholeApplication
            }
        };
        v1::StartApplicationReimportRequest {
            request_id: request_id.into_bytes().to_vec(),
            campaign_id: self.campaign_id.into_bytes().to_vec(),
            contract_lineage: self.lineage.as_str().to_owned(),
            scope: scope as i32,
            canonical_portability_manifest_json: self
                .portability_manifest
                .canonical_bytes()
                .to_vec(),
            canonical_export_manifest_json: self.canonical_export_manifest_json.clone(),
            canonical_export_receipt_json: self.canonical_export_receipt_json.clone(),
        }
    }
}

impl fmt::Debug for StartApplicationReimport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StartApplicationReimport")
            .field("campaign_id", &self.campaign_id)
            .field("lineage", &self.lineage)
            .field("scope", &self.scope)
            .field(
                "portability_manifest_hash",
                &self.portability_manifest.identity(),
            )
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

    #[test]
    fn retries_change_only_outer_request_identity() {
        let portability = ApplicationPortabilityManifest::decode_canonical(include_bytes!(
            "../../../fixtures/export/openfga/portability-manifest-v2.json"
        ))
        .expect("portability manifest");
        let start = StartApplicationReimport::new(
            ApplicationInstallationCampaignId::from_bytes(uuid(1)).expect("campaign"),
            portability.input().contract_lineage.clone(),
            CapabilityApplicationReimportScopeV1::WholeApplication,
            portability,
            b"{\"manifest\":\"exact\"}".to_vec(),
            b"{\"receipt\":\"exact\"}".to_vec(),
        )
        .expect("structural submission");
        let first = start.request(RequestId::from_bytes(uuid(2)).expect("request"));
        let second = start.request(RequestId::from_bytes(uuid(3)).expect("request"));
        assert_ne!(first.request_id, second.request_id);
        assert_eq!(first.campaign_id, second.campaign_id);
        assert_eq!(first.contract_lineage, second.contract_lineage);
        assert_eq!(first.scope, second.scope);
        assert_eq!(
            first.canonical_portability_manifest_json,
            second.canonical_portability_manifest_json
        );
        assert_eq!(
            first.canonical_export_manifest_json,
            second.canonical_export_manifest_json
        );
        assert_eq!(
            first.canonical_export_receipt_json,
            second.canonical_export_receipt_json
        );
    }

    #[test]
    fn malformed_source_documents_fail_before_transport() {
        let portability = ApplicationPortabilityManifest::decode_canonical(include_bytes!(
            "../../../fixtures/export/openfga/portability-manifest-v2.json"
        ))
        .expect("portability manifest");
        assert_eq!(
            StartApplicationReimport::new(
                ApplicationInstallationCampaignId::from_bytes(uuid(1)).expect("campaign"),
                portability.input().contract_lineage.clone(),
                CapabilityApplicationReimportScopeV1::WholeApplication,
                portability,
                b"not-json".to_vec(),
                b"{}".to_vec(),
            )
            .expect_err("malformed source must fail"),
            PublicWireError::InvalidBytes
        );
    }
}
